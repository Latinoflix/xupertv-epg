#!/usr/bin/env python3
"""XuperTV - Servidor EPG (Gestión EPG).

Descarga la guía XMLTV (GZIP) de México, la procesa y la guarda en SQLite.
Sin dependencias externas (solo biblioteca estándar de Python 3.9+).

Ejecutar:  python3 epg_server.py        (puerto 8787 por defecto)
"""
import calendar
import gzip
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("EPG_DATA_DIR", os.path.join(BASE, "data"))
DB_PATH = os.path.join(DATA_DIR, "epg.db")
HOST = os.environ.get("EPG_HOST", "0.0.0.0")
PORT = int(os.environ.get("EPG_PORT", "8787"))
DEFAULT_SOURCE = os.environ.get("EPG_SOURCE_URL", "https://epg.lat/files/mx.xml.gz")
MAX_BYTES = 400 * 1024 * 1024
RETRY_AFTER_ERROR = 30 * 60        # reintento automático tras un fallo
PUBLIC_REFRESH_MIN_GAP = 300       # el botón de la app puede disparar import cada 5 min como máximo

IMPORT_LOCK = threading.Lock()
STATE = {"running": False, "phase": "", "trigger": "", "started": 0}
CACHE = {"t": 0, "keymap": None, "json": None}
CACHE_LOCK = threading.Lock()
_last_public_refresh = 0.0


class EpgError(Exception):
    """Error legible para mostrar al usuario."""


# ----------------------------------------------------------------- base de datos
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS epg_channels(
        id TEXT PRIMARY KEY, name TEXT, names TEXT, icon TEXT);
    CREATE TABLE IF NOT EXISTS epg_programmes(
        channel_id TEXT NOT NULL, start INTEGER NOT NULL, stop INTEGER NOT NULL,
        title TEXT, descr TEXT, category TEXT);
    CREATE INDEX IF NOT EXISTS ix_prog_ch ON epg_programmes(channel_id, start);
    CREATE INDEX IF NOT EXISTS ix_prog_stop ON epg_programmes(stop);
    CREATE TABLE IF NOT EXISTS epg_links(
        name_key TEXT PRIMARY KEY, name TEXT, tvg_id TEXT, source TEXT, created INTEGER);
    CREATE TABLE IF NOT EXISTS epg_sync(
        id INTEGER PRIMARY KEY AUTOINCREMENT, trigger TEXT, started INTEGER, finished INTEGER,
        status TEXT, channels INTEGER DEFAULT 0, programmes INTEGER DEFAULT 0,
        bytes INTEGER DEFAULT 0, error TEXT);
    CREATE TABLE IF NOT EXISTS epg_settings(k TEXT PRIMARY KEY, v TEXT);
    """)
    # Una sincronización que quedó "running" por un cierre brusco se marca como interrumpida.
    c.execute("UPDATE epg_sync SET status='error', finished=?, error='Interrumpida (el servidor se reinició)' "
              "WHERE status='running'", (int(time.time()),))
    c.commit()
    c.close()


def get_setting(c, k, default=None):
    r = c.execute("SELECT v FROM epg_settings WHERE k=?", (k,)).fetchone()
    return r["v"] if r else default


def set_setting(c, k, v):
    c.execute("INSERT INTO epg_settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


# ---------------------------------------------------------------- utilidades
_ACC = str.maketrans({"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n"})


def name_key(name):
    """Misma normalización que EpgKey() en la app Roku (epg01.brs). No cambiar una sin la otra."""
    t = (name or "").lower().translate(_ACC)
    t = re.sub(r"\.(mx|us|ca|uk|es|ar|co|cl|pe|br)$", "", t)
    t = re.sub(r"\b(hd|sd|fhd|uhd|4k|tv|canal|channel)\b", " ", t)
    return re.sub(r"[^a-z0-9]", "", t)


_XT = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})?\s*([+-]\d{4})?")


def xmltv_time(s):
    m = _XT.match((s or "").strip())
    if not m:
        return None
    y, mo, d, h, mi, se, tz = m.groups()
    try:
        ts = calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(se or 0)))
    except (ValueError, OverflowError):
        return None
    if tz:
        off = (int(tz[1:3]) * 3600 + int(tz[3:5]) * 60) * (1 if tz[0] == "+" else -1)
        ts -= off
    return ts


def invalidate_cache():
    with CACHE_LOCK:
        CACHE.update(t=0, keymap=None, json=None)


# --------------------------------------------------------------- importación
def download(source):
    req = urllib.request.Request(source, headers={"User-Agent": "XuperTV-EPG/1.0", "Accept-Encoding": "identity"})
    try:
        resp = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as e:
        raise EpgError("La fuente respondió HTTP %s (%s)" % (e.code, e.reason))
    except urllib.error.URLError as e:
        raise EpgError("No se pudo conectar con la fuente: %s" % e.reason)
    except (socket.timeout, TimeoutError):
        raise EpgError("Tiempo de espera agotado al conectar con la fuente")
    except ValueError:
        raise EpgError("La URL de la fuente no es válida")
    fd, tmp = tempfile.mkstemp(suffix=".xml.gz", dir=DATA_DIR)
    size = 0
    try:
        with os.fdopen(fd, "wb") as out, resp:
            while True:
                chunk = resp.read(262144)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_BYTES:
                    raise EpgError("El archivo supera el límite de %d MB" % (MAX_BYTES // 1048576))
                out.write(chunk)
    except (socket.timeout, TimeoutError):
        os.unlink(tmp)
        raise EpgError("La descarga se interrumpió por tiempo de espera")
    except (OSError, urllib.error.URLError) as e:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise EpgError("La descarga falló: %s" % e)
    except EpgError:
        os.unlink(tmp)
        raise
    if size == 0:
        os.unlink(tmp)
        raise EpgError("La fuente devolvió un archivo vacío")
    with open(tmp, "rb") as f:
        if f.read(2) != b"\x1f\x8b":
            os.unlink(tmp)
            raise EpgError("El archivo descargado no es GZIP válido (¿la fuente devolvió una página de error?)")
    return tmp, size


def load_xmltv(c, path):
    """Procesa el XMLTV en streaming y reemplaza la guía en UNA transacción:
    si algo falla, se hace rollback y la guía anterior se conserva."""
    n_ch = n_pr = 0
    batch = []
    try:
        c.execute("DELETE FROM epg_programmes")
        c.execute("DELETE FROM epg_channels")
        root = None
        with gzip.open(path, "rb") as f:
            for ev, el in ET.iterparse(f, events=("start", "end")):
                if ev == "start":
                    if root is None:
                        root = el
                    continue
                tag = el.tag
                if tag == "channel":
                    cid = (el.get("id") or "").strip()
                    if cid:
                        names = [(d.text or "").strip() for d in el.findall("display-name") if (d.text or "").strip()]
                        icon_el = el.find("icon")
                        icon = icon_el.get("src") if icon_el is not None else None
                        c.execute("INSERT OR REPLACE INTO epg_channels(id,name,names,icon) VALUES(?,?,?,?)",
                                  (cid, names[0] if names else cid, json.dumps(names, ensure_ascii=False), icon))
                        n_ch += 1
                    root.clear()
                elif tag == "programme":
                    cid = (el.get("channel") or "").strip()
                    s, e = xmltv_time(el.get("start")), xmltv_time(el.get("stop"))
                    if cid and s is not None and e is not None and e > s:
                        batch.append((cid, s, e, (el.findtext("title") or "").strip(),
                                      (el.findtext("desc") or "").strip(), (el.findtext("category") or "").strip()))
                        n_pr += 1
                        if len(batch) >= 2000:
                            c.executemany("INSERT INTO epg_programmes VALUES(?,?,?,?,?,?)", batch)
                            batch = []
                    root.clear()
        if batch:
            c.executemany("INSERT INTO epg_programmes VALUES(?,?,?,?,?,?)", batch)
        if n_ch == 0 or n_pr == 0:
            raise EpgError("El archivo no contiene canales/programas XMLTV válidos (canales=%d, programas=%d)" % (n_ch, n_pr))
        return n_ch, n_pr
    except ET.ParseError as e:
        raise EpgError("El XML de la guía está dañado o incompleto: %s" % e)
    except (gzip.BadGzipFile, EOFError, zlib.error) as e:
        raise EpgError("El GZIP está corrupto o incompleto: %s" % e)


def do_import(trigger):
    c = db()
    source = get_setting(c, "source_url", DEFAULT_SOURCE)
    now = int(time.time())
    sid = c.execute("INSERT INTO epg_sync(trigger,started,status) VALUES(?,?,'running')", (trigger, now)).lastrowid
    c.commit()
    STATE.update(running=True, phase="Descargando…", trigger=trigger, started=now)
    tmp, size = None, 0
    try:
        tmp, size = download(source)
        STATE["phase"] = "Procesando y guardando…"
        n_ch, n_pr = load_xmltv(c, tmp)
        c.execute("UPDATE epg_sync SET finished=?, status='ok', channels=?, programmes=?, bytes=? WHERE id=?",
                  (int(time.time()), n_ch, n_pr, size, sid))
        c.commit()
        invalidate_cache()
        log("Importación OK (%s): %d canales, %d programas" % (trigger, n_ch, n_pr))
    except Exception as e:  # cualquier fallo: rollback y registrar
        c.rollback()
        msg = str(e) if isinstance(e, EpgError) else "Error inesperado: %r" % e
        c.execute("UPDATE epg_sync SET finished=?, status='error', bytes=?, error=? WHERE id=?",
                  (int(time.time()), size, msg, sid))
        c.commit()
        log("Importación con error (%s): %s" % (trigger, msg))
    finally:
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
        c.close()
        STATE.update(running=False, phase="")


def start_import(trigger):
    """Lanza la importación en segundo plano. Devuelve False si ya hay una en curso."""
    if not IMPORT_LOCK.acquire(blocking=False):
        return False

    def worker():
        try:
            do_import(trigger)
        finally:
            IMPORT_LOCK.release()
    STATE.update(running=True, phase="Iniciando…", trigger=trigger, started=int(time.time()))
    threading.Thread(target=worker, daemon=True).start()
    return True


def scheduler():
    time.sleep(3)
    while True:
        try:
            c = db()
            if get_setting(c, "auto_enabled", "1") == "1" and not STATE["running"]:
                interval = float(get_setting(c, "interval_hours", "12")) * 3600
                last = c.execute("SELECT * FROM epg_sync WHERE status!='running' ORDER BY id DESC LIMIT 1").fetchone()
                gap = interval if (last and last["status"] == "ok") else RETRY_AFTER_ERROR
                if last is None or (last["finished"] or 0) + gap <= time.time():
                    start_import("auto")
            c.close()
        except Exception as e:
            log("Error en el programador: %r" % e)
        time.sleep(30)


# ------------------------------------------------------------ consultas / API
def status_dict(c):
    last = c.execute("SELECT * FROM epg_sync WHERE status!='running' ORDER BY id DESC LIMIT 1").fetchone()
    ok = c.execute("SELECT * FROM epg_sync WHERE status='ok' ORDER BY id DESC LIMIT 1").fetchone()
    interval = float(get_setting(c, "interval_hours", "12"))
    auto = get_setting(c, "auto_enabled", "1") == "1"
    if STATE["running"]:
        state = "running"
    elif last is None:
        state = "never"
    else:
        state = last["status"]
    next_run = None
    if auto and last is not None:
        next_run = (last["finished"] or 0) + (interval * 3600 if last["status"] == "ok" else RETRY_AFTER_ERROR)
    return {
        "state": state,
        "phase": STATE["phase"] if STATE["running"] else "",
        "trigger": STATE["trigger"] if STATE["running"] else (last["trigger"] if last else ""),
        "last_success": ok["finished"] if ok else None,
        "last_attempt": last["finished"] if last else None,
        "last_error": last["error"] if (last and last["status"] == "error") else None,
        "channels": c.execute("SELECT COUNT(*) n FROM epg_channels").fetchone()["n"],
        "programmes": c.execute("SELECT COUNT(*) n FROM epg_programmes").fetchone()["n"],
        "source": get_setting(c, "source_url", DEFAULT_SOURCE),
        "interval_hours": interval,
        "auto_enabled": auto,
        "next_run": next_run,
        "now": int(time.time()),
    }


def build_keymap(c):
    """clave normalizada -> id de canal EPG.  Prioridad (menor a mayor):
    display-name  <  tvg-id  <  vínculos manuales/M3U."""
    km = {}
    rows = c.execute("SELECT id,names FROM epg_channels").fetchall()
    for r in rows:
        for n in json.loads(r["names"] or "[]"):
            k = name_key(n)
            if k:
                km[k] = r["id"]
    for r in rows:
        k = name_key(r["id"])
        if k:
            km[k] = r["id"]
    ids = {r["id"] for r in rows}
    for r in c.execute("SELECT name_key,tvg_id FROM epg_links"):
        if r["tvg_id"] in ids:
            km[r["name_key"]] = r["tvg_id"]
    return km


def keymap(c):
    with CACHE_LOCK:
        if CACHE["keymap"] is not None and time.time() - CACHE["t"] < 60:
            return CACHE["keymap"]
    km = build_keymap(c)
    with CACHE_LOCK:
        CACHE.update(keymap=km, t=time.time(), json=None)
    return km


def resolve_channel(c, tvg_id="", name=""):
    km = keymap(c)
    for cand in (tvg_id, name):
        k = name_key(cand)
        if k and k in km:
            return km[k]
    return None


def now_next(c, channel_id, now=None):
    now = now or int(time.time())
    rows = c.execute("SELECT start,stop,title,descr,category FROM epg_programmes WHERE channel_id=? AND stop>? "
                     "ORDER BY start LIMIT 2", (channel_id, now)).fetchall()
    cur = nxt = None
    for r in rows:
        d = {"start": r["start"], "stop": r["stop"], "title": r["title"], "desc": r["descr"], "category": r["category"]}
        if r["start"] <= now and cur is None:
            cur = d
        elif nxt is None:
            nxt = d
    return cur, nxt


def build_epg_json(c, per=4):
    """Formato compacto que ya consume la app Roku: {channels:{clave:[[inicio,fin,titulo],..]}}"""
    with CACHE_LOCK:
        if CACHE["json"] is not None and time.time() - CACHE["t"] < 60:
            return CACHE["json"]
    now = int(time.time())
    km = keymap(c)
    progs = {}
    # The import counters include the whole feed. Do not restrict the public
    # export to a 24-hour window: a feed with a later schedule would otherwise
    # report thousands of imported programmes but serialize no channels.
    for r in c.execute("SELECT channel_id,start,stop,title FROM epg_programmes WHERE stop>? "
                       "ORDER BY channel_id,start", (now,)):
        lst = progs.setdefault(r["channel_id"], [])
        if len(lst) < per:
            lst.append([r["start"], r["stop"], r["title"]])
    channels = {k: progs[cid] for k, cid in km.items() if cid in progs}
    if not channels:
        raise EpgError("No se puede generar epg.json: no hay canales con programas vigentes o futuros. "
                       "Comprueba las fechas de la guía y la hora del sistema.")
    ok = c.execute("SELECT finished FROM epg_sync WHERE status='ok' ORDER BY id DESC LIMIT 1").fetchone()
    body = json.dumps({"updated": ok["finished"] if ok else None, "generated": now, "channels": channels},
                      ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with CACHE_LOCK:
        CACHE["json"] = body
    return body


def import_m3u(c, text):
    ids = {r["id"].lower(): r["id"] for r in c.execute("SELECT id FROM epg_channels")}
    linked, unmatched, seen = 0, [], 0
    now = int(time.time())
    for line in text.splitlines():
        if not line.startswith("#EXTINF"):
            continue
        seen += 1
        m = re.search(r'tvg-id="([^"]*)"', line)
        tid = (m.group(1).strip() if m else "")
        name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
        tn = re.search(r'tvg-name="([^"]*)"', line)
        epg_id = ids.get(tid.lower()) if tid else None
        if not epg_id:
            if len(unmatched) < 100:
                unmatched.append({"name": name, "tvg_id": tid})
            continue
        for nm in {name, tn.group(1).strip() if tn else ""}:
            k = name_key(nm)
            if k:
                c.execute("INSERT INTO epg_links(name_key,name,tvg_id,source,created) VALUES(?,?,?,'m3u',?) "
                          "ON CONFLICT(name_key) DO UPDATE SET name=excluded.name, tvg_id=excluded.tvg_id, "
                          "created=excluded.created WHERE epg_links.source!='manual'", (k, nm, epg_id, now))
        linked += 1
    c.commit()
    invalidate_cache()
    return {"entries": seen, "linked": linked, "unmatched_count": seen - linked, "unmatched": unmatched}


def log(msg):
    print(time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg, flush=True)


# --------------------------------------------------------------------- HTTP
def load_admin_token():
    tok = os.environ.get("EPG_ADMIN_TOKEN")
    if tok:
        return tok
    p = os.path.join(DATA_DIR, "admin_token.txt")
    if os.path.exists(p):
        return open(p).read().strip()
    tok = secrets.token_urlsafe(18)
    with open(p, "w") as f:
        f.write(tok)
    os.chmod(p, 0o600)
    return tok


ADMIN_TOKEN = ""


class Handler(BaseHTTPRequestHandler):
    server_version = "XuperEPG/1.0"

    def log_message(self, fmt, *args):
        pass

    # --- helpers
    def send(self, code, body, ctype="application/json; charset=utf-8", extra=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Admin-Token")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def body_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 5 * 1024 * 1024:
            raise ValueError("cuerpo demasiado grande")
        raw = self.rfile.read(n) if n else b""
        return json.loads(raw.decode("utf-8")) if raw else {}

    def is_admin(self):
        tok = self.headers.get("X-Admin-Token", "")
        return bool(tok) and secrets.compare_digest(tok, ADMIN_TOKEN)

    def do_OPTIONS(self):
        self.send(204, b"")

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_DELETE(self):
        self.route("DELETE")

    def route(self, method):
        global _last_public_refresh
        u = urllib.parse.urlparse(self.path)
        path, q = u.path.rstrip("/") or "/", {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        try:
            if method == "GET" and path in ("/", "/admin"):
                with open(os.path.join(BASE, "admin.html"), "rb") as f:
                    return self.send(200, f.read(), "text/html; charset=utf-8")
            c = db()
            try:
                # ---------- público (app Roku)
                if method == "GET" and path == "/epg.json":
                    return self.send(200, build_epg_json(c), extra={"Cache-Control": "max-age=60"})
                if method == "GET" and path == "/api/epg/status":
                    s = status_dict(c)
                    s.pop("source", None)
                    return self.send(200, s)
                if method == "GET" and path == "/api/epg/refresh":
                    if STATE["running"]:
                        return self.send(200, {"started": False, "reason": "busy", **status_dict(c)})
                    wait = PUBLIC_REFRESH_MIN_GAP - (time.time() - _last_public_refresh)
                    if wait > 0:
                        return self.send(200, {"started": False, "reason": "throttled", "retry_in": int(wait), **status_dict(c)})
                    _last_public_refresh = time.time()
                    started = start_import("app")
                    return self.send(200, {"started": started, "reason": "" if started else "busy", **status_dict(c)})
                if method == "GET" and path == "/api/epg/now":
                    ids = [x for x in q.get("ids", "").split(",") if x] or [q.get("id", "") or q.get("name", "")]
                    out = {}
                    for ident in ids:
                        cid = resolve_channel(c, tvg_id=ident, name=ident)
                        if cid:
                            cur, nxt = now_next(c, cid)
                            out[ident] = {"channel": cid, "now": cur, "next": nxt}
                        else:
                            out[ident] = None
                    return self.send(200, out)
                # ---------- administración
                if path.startswith("/api/admin"):
                    if not self.is_admin():
                        return self.send(401, {"error": "Token de administración inválido"})
                    return self.admin(c, method, path, q)
                return self.send(404, {"error": "No encontrado"})
            finally:
                c.close()
        except (ValueError, json.JSONDecodeError) as e:
            return self.send(400, {"error": "Solicitud inválida: %s" % e})
        except Exception as e:
            log("Error HTTP %s %s: %r" % (method, self.path, e))
            return self.send(500, {"error": "Error interno del servidor"})

    def admin(self, c, method, path, q):
        if method == "GET" and path == "/api/admin/status":
            return self.send(200, status_dict(c))
        if method == "POST" and path == "/api/admin/import":
            started = start_import("manual")
            return self.send(200, {"started": started, "reason": "" if started else "Ya hay una importación en curso"})
        if method == "GET" and path == "/api/admin/history":
            rows = c.execute("SELECT * FROM epg_sync ORDER BY id DESC LIMIT 30").fetchall()
            return self.send(200, [dict(r) for r in rows])
        if path == "/api/admin/settings":
            if method == "GET":
                return self.send(200, {"source_url": get_setting(c, "source_url", DEFAULT_SOURCE),
                                       "interval_hours": float(get_setting(c, "interval_hours", "12")),
                                       "auto_enabled": get_setting(c, "auto_enabled", "1") == "1"})
            d = self.body_json()
            src = str(d.get("source_url", "")).strip()
            if not re.match(r"^https?://", src, re.I):
                raise ValueError("La URL debe empezar con http:// o https://")
            hrs = float(d.get("interval_hours", 12))
            if not 1 <= hrs <= 168:
                raise ValueError("El intervalo debe estar entre 1 y 168 horas")
            set_setting(c, "source_url", src)
            set_setting(c, "interval_hours", hrs)
            set_setting(c, "auto_enabled", "1" if d.get("auto_enabled", True) else "0")
            c.commit()
            return self.send(200, {"ok": True})
        if method == "GET" and path == "/api/admin/channels":
            term = name_key(q.get("q", ""))
            page, size = max(int(q.get("page", 1)), 1), 50
            rows = [r for r in c.execute("SELECT id,name,icon FROM epg_channels ORDER BY name COLLATE NOCASE")
                    if not term or term in name_key(r["id"]) or term in name_key(r["name"])]
            total = len(rows)
            now = int(time.time())
            out = []
            for r in rows[(page - 1) * size: page * size]:
                cur, nxt = now_next(c, r["id"], now)
                cnt = c.execute("SELECT COUNT(*) n FROM epg_programmes WHERE channel_id=?", (r["id"],)).fetchone()["n"]
                out.append({"id": r["id"], "name": r["name"], "icon": r["icon"], "programmes": cnt,
                            "now": cur["title"] if cur else None, "next": nxt["title"] if nxt else None})
            return self.send(200, {"total": total, "page": page, "page_size": size, "channels": out})
        if method == "GET" and path == "/api/admin/links":
            return self.send(200, [dict(r) for r in c.execute("SELECT * FROM epg_links ORDER BY created DESC LIMIT 500")])
        if method == "POST" and path == "/api/admin/links":
            d = self.body_json()
            name, tid = str(d.get("name", "")).strip(), str(d.get("tvg_id", "")).strip()
            if not c.execute("SELECT 1 FROM epg_channels WHERE id=?", (tid,)).fetchone():
                raise ValueError("El tvg-id '%s' no existe en la guía importada" % tid)
            k = name_key(name)
            if not k:
                raise ValueError("Nombre de canal vacío o inválido")
            c.execute("INSERT OR REPLACE INTO epg_links(name_key,name,tvg_id,source,created) VALUES(?,?,?,'manual',?)",
                      (k, name, tid, int(time.time())))
            c.commit()
            invalidate_cache()
            return self.send(200, {"ok": True, "name_key": k})
        if method == "DELETE" and path == "/api/admin/links":
            c.execute("DELETE FROM epg_links WHERE name_key=?", (q.get("key", ""),))
            c.commit()
            invalidate_cache()
            return self.send(200, {"ok": True})
        if method == "POST" and path == "/api/admin/links/m3u":
            d = self.body_json()
            text = str(d.get("text", ""))
            if d.get("url"):
                if not re.match(r"^https?://", d["url"], re.I):
                    raise ValueError("La URL de la lista M3U debe ser http(s)")
                try:
                    req = urllib.request.Request(d["url"], headers={"User-Agent": "XuperTV-EPG/1.0"})
                    with urllib.request.urlopen(req, timeout=30) as r:
                        text = r.read(30 * 1024 * 1024).decode("utf-8", "replace")
                except Exception as e:
                    raise ValueError("No se pudo descargar la lista M3U: %s" % e)
            if "#EXTINF" not in text:
                raise ValueError("No se encontró ninguna entrada #EXTINF en la lista")
            return self.send(200, import_m3u(c, text))
        if method == "GET" and path == "/api/admin/channel":
            cid = q.get("id", "")
            now = int(time.time())
            rows = c.execute("SELECT start,stop,title,category FROM epg_programmes WHERE channel_id=? AND stop>? "
                             "ORDER BY start LIMIT 30", (cid, now)).fetchall()
            return self.send(200, [dict(r) for r in rows])
        return self.send(404, {"error": "No encontrado"})


def main():
    global ADMIN_TOKEN
    init_db()
    ADMIN_TOKEN = load_admin_token()
    threading.Thread(target=scheduler, daemon=True).start()
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    log("XuperTV EPG escuchando en http://%s:%d  (panel: /admin)" % (HOST, PORT))
    log("Token de administración: %s" % ADMIN_TOKEN)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
