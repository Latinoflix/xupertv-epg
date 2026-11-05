import json
import sqlite3
import time
import unittest

import epg_server as epg


def make_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE epg_channels(id TEXT PRIMARY KEY, name TEXT, names TEXT, icon TEXT);
        CREATE TABLE epg_programmes(channel_id TEXT, start INTEGER, stop INTEGER,
                                    title TEXT, descr TEXT, category TEXT);
        CREATE TABLE epg_links(name_key TEXT PRIMARY KEY, name TEXT, tvg_id TEXT,
                               source TEXT, created INTEGER);
        CREATE TABLE epg_sync(id INTEGER PRIMARY KEY, status TEXT, finished INTEGER);
    """)
    conn.execute("INSERT INTO epg_channels VALUES(?,?,?,?)",
                 ("canal-1", "Canal Uno", '["Canal Uno"]', None))
    conn.execute("INSERT INTO epg_sync VALUES(1,'ok',?)", (int(time.time()),))
    return conn


class BuildEpgJsonTests(unittest.TestCase):
    def setUp(self):
        epg.invalidate_cache()

    def test_includes_programmes_beyond_24_hour_window(self):
        conn = make_db()
        now = int(time.time())
        conn.execute("INSERT INTO epg_programmes VALUES(?,?,?,?,?,?)",
                     ("canal-1", now + 2 * 86400, now + 2 * 86400 + 3600,
                      "Programa futuro", "", ""))

        result = json.loads(epg.build_epg_json(conn))

        self.assertEqual(result["channels"]["canaluno"][0][2], "Programa futuro")
        conn.close()

    def test_refuses_export_when_no_programme_is_current_or_future(self):
        conn = make_db()
        now = int(time.time())
        conn.execute("INSERT INTO epg_programmes VALUES(?,?,?,?,?,?)",
                     ("canal-1", now - 7200, now - 3600, "Programa vencido", "", ""))

        with self.assertRaisesRegex(epg.EpgError, "no hay canales"):
            epg.build_epg_json(conn)
        conn.close()


if __name__ == "__main__":
    unittest.main()
