"""Runs against a real ADoor server when ADOOR_TEST_URL and ADOOR_TEST_TOKEN are set.

The token needs select, insert, update, delete, create_table and alter in ADOOR_TEST_SPACE, plus
manage_connections and recover for the sandbox test (recover lists the sandboxes' changes).
"""

import datetime as dt
import math
import os
import sys
import unittest
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import adoor  # noqa: E402

URL = os.environ.get("ADOOR_TEST_URL")
TOKEN = os.environ.get("ADOOR_TEST_TOKEN")
SPACE = os.environ.get("ADOOR_TEST_SPACE")


def table_name(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


@unittest.skipUnless(URL and TOKEN, "set ADOOR_TEST_URL and ADOOR_TEST_TOKEN")
class LiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = adoor.connect(URL, TOKEN, space=SPACE)
        cls.me = cls.db.whoami()

    def test_notebook_round_trip_with_types(self):
        table = table_name("plate_reads")
        self.db.create_table(table, {"well": "text", "od600": "numeric", "read_at": "timestamptz",
                                     "replicate": "integer", "meta": "jsonb", "series": "numeric[]"})
        at = dt.datetime(2026, 11, 3, 9, 30, tzinfo=dt.timezone.utc)
        rows = [{"well": f"A{index}", "od600": index / 10, "read_at": at, "replicate": index,
                 "meta": {"strain": "K-12"}, "series": [0.1, 0.2, float("nan")]} for index in range(1, 4)]
        inserted = self.db.insert(table, rows)
        self.assertEqual(inserted["rowCount"], 3)
        self.assertEqual(len(inserted["eventIds"]), 1)
        result = self.db.query(f'SELECT well, od600, read_at, replicate, meta, series FROM "{table}" ORDER BY well')
        self.assertEqual([row["well"] for row in result], ["A1", "A2", "A3"])
        first = result[0]
        self.assertEqual(first["od600"], 0.1)
        self.assertEqual(first["read_at"], at)
        self.assertEqual(first["replicate"], 1)
        self.assertEqual(first["meta"], {"strain": "K-12"})
        # The server sends a numeric[] as JSON numbers, and NaN inside an array arrives as null.
        self.assertEqual(first["series"][:2], [0.1, 0.2])
        self.assertIsNone(first["series"][2])
        # A NaN cell is missing data, stored as NULL, so avg and count skip it as pandas does.
        self.db.insert(table, [{"well": "B1", "od600": float("nan")}, {"well": "B2", "od600": 0.3}])
        self.assertIsNone(self.db.query(f'SELECT od600 FROM "{table}" WHERE well = $1', ["B1"])[0]["od600"])
        stats = self.db.query(f'SELECT avg(od600) AS mean, count(od600) AS n FROM "{table}" WHERE well IN ($1, $2)', ["B1", "B2"])[0]
        self.assertEqual((stats["mean"], stats["n"]), (0.3, 1))
        # An explicit NaN parameter is PostgreSQL's NaN number.
        self.db.execute(f'INSERT INTO "{table}" (well, od600) VALUES ($1, $2)', ["B3", float("nan")])
        self.assertTrue(math.isnan(self.db.query(f'SELECT od600 FROM "{table}" WHERE well = $1', ["B3"])[0]["od600"]))

    def test_dataframe_insert_is_idempotent_and_pages(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas is not installed")
        table = table_name("runs")
        frame = pd.DataFrame({"step": range(2500), "loss": [1 / (index + 1) for index in range(2500)],
                              "run": ["r1"] * 2500})
        self.db.create_table(table, frame)
        key = f"test-{uuid.uuid4()}"
        first = self.db.insert(table, frame, key=key, chunk_rows=1000)
        self.assertEqual((first["rowCount"], first["chunks"]), (2500, 3))
        self.db.insert(table, frame, key=key, chunk_rows=1000)
        count = self.db.query(f'SELECT count(*) AS n FROM "{table}"')[0]["n"]
        self.assertEqual(count, 2500, "a rerun with the same key must not insert twice")
        back = self.db.df(f'SELECT step, loss FROM "{table}" ORDER BY step')
        self.assertEqual(len(back), 2500)
        self.assertEqual(back["step"].tolist(), list(range(2500)))
        self.assertAlmostEqual(back["loss"].iloc[9], 0.1)

    def test_history_names_the_identity_and_keeps_row_images(self):
        table = table_name("samples")
        self.db.create_table(table, {"name": "text", "od600": "numeric"})
        self.db.insert(table, [{"name": "S1", "od600": 0.4}])
        changed = self.db.execute(f'UPDATE "{table}" SET od600 = $1 WHERE name = $2', [0.45, "S1"],
                                  expect_row_count=1, idempotency_key=str(uuid.uuid4()))
        self.assertTrue(changed.event_id)
        events = self.db.history(table=table, limit=10)
        self.assertEqual([event["event"] for event in events], ["update", "insert", "create"])
        connection = self.me["connection"] if "connection" in self.me else self.me
        connection_id = connection.get("id") or connection.get("connectionId")
        for event in events:
            self.assertEqual(event["connectionId"], connection_id)
            self.assertTrue(event["actorLabel"])
            self.assertTrue(event["createdAt"])
        detail = self.db.inspect(changed.event_id)
        change = detail["changes"][0]
        self.assertEqual(float(change["before"]["od600"]), 0.4)
        self.assertEqual(float(change["after"]["od600"]), 0.45)
        self.db.undo(changed.event_id)
        self.assertEqual(self.db.query(f'SELECT od600 FROM "{table}"')[0]["od600"], 0.4)

    def test_logger_batches_a_training_loop(self):
        table = table_name("metrics")
        self.db.create_table(table, {"run": "text", "step": "integer", "loss": "numeric"})
        with self.db.logger(table, flush_rows=100, flush_seconds=3600, run="r1") as log:
            for step in range(250):
                log.log(step=step, loss=1 / (step + 1))
            self.assertEqual(log.written, 200)
        self.assertEqual(log.written, 250)
        self.assertEqual(len(log.event_ids), 3)
        rows = self.db.query(f'SELECT run, count(*) AS n, max(step) AS last FROM "{table}" GROUP BY run')
        self.assertEqual(rows.rows, [{"run": "r1", "n": 250, "last": 249}])

    def test_import_frame_round_trips_types_and_nulls(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas is not installed")
        table = table_name("bulk")
        n = 5000
        frame = pd.DataFrame({
            "well": [f"W{i % 384}" for i in range(n)],
            "od600": [None if i % 1000 == 7 else i / 1000 for i in range(n)],
            "ok": [i % 2 == 0 for i in range(n)],
            "note": ["" if i % 2 else 'a "quoted", note' for i in range(n)],
            "read_at": pd.Timestamp("2026-11-03T09:00:00Z") + pd.to_timedelta(range(n), unit="s"),
        })
        result = self.db.import_frame(table, frame, create=True)
        self.assertEqual((result["rowCount"], result["rowsWritten"]), (n, n))
        back = self.db.df(f'SELECT well, od600, ok, note, read_at FROM "{table}" ORDER BY read_at')
        pd.testing.assert_frame_equal(back, frame, check_dtype=False)
        sql = f"SELECT count(*) FILTER (WHERE od600 IS NULL) AS n, count(*) FILTER (WHERE note = '') AS e FROM \"{table}\""
        nulls = self.db.query(sql)[0]
        self.assertEqual((nulls["n"], nulls["e"]), (5, n // 2))

    def test_import_csv_maps_header_names_and_resumes_after_disconnects(self):
        table = table_name("resume")
        self.db.create_table(table, {"step": "integer", "loss": "numeric", "run": "text"})
        n = 8_000
        lines = ["run,loss,step"] + [f"r1,{1 / (i + 1)!r},{i}" for i in range(n)]
        data = ("\n".join(lines) + "\n").encode()
        # A timeout shorter than the load: a request disconnects mid-COPY, the server keeps the committed chunks,
        # and the next attempt continues after them.
        result = self.db.import_csv(table, data, request_timeout=1.0, attempts=50)
        self.assertGreater(result["attempts"], 1, "the load should have needed more than one COPY attempt")
        self.assertEqual(result["rowCount"], n)
        sql = f'SELECT count(*) AS n, count(DISTINCT step) AS d, min(step) AS lo, max(step) AS hi FROM "{table}"'
        check = self.db.query(sql)[0]
        self.assertEqual((check["n"], check["d"], check["lo"], check["hi"]), (n, n, 0, n - 1))

    def test_guarded_write_and_dry_run(self):
        table = table_name("guarded")
        self.db.create_table(table, {"name": "text"})
        self.db.insert(table, {"name": "x"})
        with self.assertRaises(adoor.AdoorError) as caught:
            self.db.execute(f'UPDATE "{table}" SET name = $1 WHERE name = $2', ["y", "missing"], expect_row_count=1)
        self.assertEqual(caught.exception.code, "CONFLICT")
        self.db.execute(f'DELETE FROM "{table}"', dry_run=True)
        self.assertEqual(self.db.query(f'SELECT count(*) AS n FROM "{table}"')[0]["n"], 1)

    def test_sandboxes_write_under_their_own_names_and_revoke(self):
        table = table_name("eval_results")
        self.db.create_table(table, {"task": "text", "score": "numeric"})
        grant = [{"table": table, "actions": ["select", "insert"]}]
        if SPACE:
            grant[0]["space"] = SPACE
        boxes = self.db.sandboxes(3, prefix="eval-box", permissions=grant, expires_in=600)
        self.assertEqual([box.name for box in boxes], ["eval-box-1", "eval-box-2", "eval-box-3"])
        for index, box in enumerate(boxes):
            self.assertEqual(set(box.env()), {"ADOOR_URL", "ADOOR_TOKEN"})
            box.client(space=SPACE).insert(table, {"task": f"t{index}", "score": index / 2})
        names = {event["connectionName"] for event in self.db.history(table=table) if event["event"] == "insert"}
        self.assertEqual(names, {"eval-box-1", "eval-box-2", "eval-box-3"})
        with self.assertRaises(adoor.AdoorError) as caught:
            boxes[0].client(space=SPACE).execute(f'DELETE FROM "{table}"')
        self.assertEqual(caught.exception.status, 403)
        self.db.revoke(boxes[0].id)
        with self.assertRaises(adoor.AdoorError) as caught:
            boxes[0].client(space=SPACE).query(f'SELECT * FROM "{table}"')
        self.assertTrue(caught.exception.authority)
        self.assertEqual(len(boxes[1].client(space=SPACE).query(f'SELECT * FROM "{table}"')), 3)


if __name__ == "__main__":
    unittest.main()
