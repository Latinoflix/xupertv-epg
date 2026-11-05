#!/usr/bin/env python3
"""Build static epg.json for XuperTV using the existing epg_server parser."""
import os, json
import epg_server as epg

def main():
    os.environ.setdefault("EPG_DATA_DIR", os.path.join(os.getcwd(), "data"))
    # epg_server computes DATA_DIR at import time; workflow runs from repository root.
    epg.init_db()
    epg.do_import("github-actions")
    conn = epg.db()
    try:
        row = conn.execute("SELECT status,error,channels,programmes FROM epg_sync ORDER BY id DESC LIMIT 1").fetchone()
        if not row or row["status"] != "ok":
            raise SystemExit("EPG import failed: " + (row["error"] if row else "no status"))
        body = epg.build_epg_json(conn, per=4)
        os.makedirs("docs", exist_ok=True)
        with open("docs/epg.json", "wb") as f:
            f.write(body)
        print("Generated docs/epg.json: %s channels, %s programmes" % (row["channels"], row["programmes"]))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
