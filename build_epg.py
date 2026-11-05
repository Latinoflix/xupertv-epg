#!/usr/bin/env python3
"""Build static epg.json for XuperTV using the existing epg_server parser."""
import os, json, tempfile

# epg_server reads EPG_DATA_DIR when imported, so set it first. Use the
# repository location rather than the caller's current directory.
REPO_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("EPG_DATA_DIR", os.path.join(REPO_DIR, "data"))
import epg_server as epg

def main():
    epg.init_db()
    epg.do_import("github-actions")
    conn = epg.db()
    try:
        row = conn.execute("SELECT status,error,channels,programmes FROM epg_sync ORDER BY id DESC LIMIT 1").fetchone()
        if not row or row["status"] != "ok":
            raise SystemExit("EPG import failed: " + (row["error"] if row else "no status"))
        db_channels = conn.execute("SELECT COUNT(*) FROM epg_channels").fetchone()[0]
        db_programmes = conn.execute("SELECT COUNT(*) FROM epg_programmes").fetchone()[0]
        if not db_channels or not db_programmes:
            raise SystemExit("EPG import reported success but database is empty "
                             "(channels=%d, programmes=%d)" % (db_channels, db_programmes))
        body = epg.build_epg_json(conn, per=4)
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SystemExit("Generated EPG is not valid UTF-8 JSON: %s" % exc)
        exported_channels = payload.get("channels") if isinstance(payload, dict) else None
        if not isinstance(exported_channels, dict) or not exported_channels:
            raise SystemExit("Refusing to publish an empty or invalid channels object")

        docs_dir = os.path.join(REPO_DIR, "docs")
        os.makedirs(docs_dir, exist_ok=True)
        target = os.path.join(docs_dir, "epg.json")
        fd, tmp = tempfile.mkstemp(prefix=".epg.json.", suffix=".tmp", dir=docs_dir)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
        except Exception:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
        print("Generated docs/epg.json: %d exported channels (%d imported), %d imported programmes" %
              (len(exported_channels), db_channels, db_programmes))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
