import datetime as dt
import math
import os
import sys
import unittest
import uuid
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from adoor import AdoorError, Client, Json, quote_ident  # noqa: E402
from adoor.client import _column_types, _records  # noqa: E402
from adoor.errors import error_from_body  # noqa: E402
from adoor.values import decode_rows, encode_cell, encode_param, parse_timestamp  # noqa: E402


class EncodeTest(unittest.TestCase):
    def test_scalars(self):
        self.assertIsNone(encode_param(None))
        self.assertIs(encode_param(True), True)
        self.assertEqual(encode_param(3), 3)
        self.assertEqual(encode_param(2**60), str(2**60))
        self.assertEqual(encode_param(0.25), 0.25)
        self.assertEqual(encode_param(float("nan")), "NaN")
        self.assertEqual(encode_param(float("inf")), "Infinity")
        self.assertEqual(encode_param(float("-inf")), "-Infinity")
        self.assertEqual(encode_param(Decimal("1.10")), "1.10")
        value = uuid.uuid4()
        self.assertEqual(encode_param(value), str(value))

    def test_nan_cell_is_missing_but_nan_param_is_a_number(self):
        self.assertIsNone(encode_cell(float("nan")))
        self.assertEqual(encode_cell(0.5), 0.5)
        self.assertEqual(encode_param(float("nan")), "NaN")
        try:
            import numpy as np
        except ImportError:
            return
        self.assertIsNone(encode_cell(np.float64("nan")))
        self.assertEqual(encode_cell(np.float64(0.25)), 0.25)

    def test_times(self):
        aware = dt.datetime(2026, 11, 3, 9, 30, tzinfo=dt.timezone.utc)
        self.assertEqual(encode_param(aware), "2026-11-03T09:30:00+00:00")
        self.assertEqual(encode_param(dt.date(2026, 11, 3)), "2026-11-03")
        with self.assertRaisesRegex(ValueError, "time zone"):
            encode_param(dt.datetime(2026, 11, 3))

    def test_arrays_and_json(self):
        self.assertEqual(encode_param([1, 2, None]), "{\"1\",\"2\",NULL}")
        self.assertEqual(encode_param(["a b", 'c"d']), '{"a b","c\\"d"}')
        self.assertEqual(encode_param({"lr": 0.001, "at": dt.date(2026, 1, 2)}), '{"lr": 0.001, "at": "2026-01-02"}')
        self.assertEqual(encode_param(Json([1, 2])), "[1, 2]")
        with self.assertRaises(TypeError):
            encode_param(object())

    def test_identifiers(self):
        self.assertEqual(quote_ident('od"600'), '"od""600"')
        with self.assertRaises(ValueError):
            quote_ident("")


class DecodeTest(unittest.TestCase):
    columns = [{"name": "b", "type": "20"}, {"name": "n", "type": "1700"}, {"name": "t", "type": "1184"},
               {"name": "d", "type": "1082"}, {"name": "a", "type": "1231"}, {"name": "s", "type": "25"}]

    def test_rows(self):
        rows = decode_rows([{"b": "9007199254740993", "n": "NaN", "t": "2026-11-03 20:40:15.105433+00",
                             "d": "2026-11-03", "a": [0.1, None], "s": "x"}], self.columns)
        row = rows[0]
        self.assertEqual(row["b"], 9007199254740993)
        self.assertTrue(math.isnan(row["n"]))
        self.assertEqual(row["t"], dt.datetime(2026, 11, 3, 20, 40, 15, 105433, tzinfo=dt.timezone.utc))
        self.assertEqual(row["d"], dt.date(2026, 11, 3))
        self.assertEqual(row["a"], [0.1, None])
        self.assertEqual(row["s"], "x")

    def test_decimal(self):
        rows = decode_rows([{"n": "0.000000150000000000000000"}], [{"name": "n", "type": "1700"}], as_decimal=True)
        self.assertEqual(rows[0]["n"], Decimal("0.000000150000000000000000"))

    def test_timestamps(self):
        self.assertEqual(parse_timestamp("2026-11-03 20:40:15+05:30").utcoffset(), dt.timedelta(hours=5, minutes=30))
        self.assertEqual(parse_timestamp("2026-11-03 20:40:15.1-08").utcoffset(), dt.timedelta(hours=-8))


class ShapeTest(unittest.TestCase):
    def test_records_and_types(self):
        self.assertEqual(_records({"a": 1}), [{"a": 1}])
        self.assertEqual(_records([{"a": 1}, {"b": 2}]), [{"a": 1}, {"b": 2}])
        self.assertEqual(_column_types({"a": "text"}), {"a": "text"})

    def test_pandas_types(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas is not installed")
        frame = pd.DataFrame({"well": ["A1"], "n": [1], "od": [0.5], "ok": [True],
                              "at": pd.to_datetime(["2026-11-03"], utc=True)})
        self.assertEqual(_column_types(frame), {"well": "text", "n": "bigint", "od": "numeric", "ok": "boolean",
                                                "at": "timestamptz"})
        record = _records(frame)[0]
        self.assertEqual(encode_param(record["n"]), 1)
        self.assertEqual(encode_param(record["at"]), "2026-11-03T00:00:00+00:00")
        self.assertIsNone(encode_param(pd.NA))
        self.assertIsNone(encode_param(pd.NaT))


class CsvTest(unittest.TestCase):
    def test_null_empty_quotes_and_types(self):
        import io
        from adoor.bulk import write_csv
        out = io.BytesIO()
        count = write_csv([{"a": "x,y", "b": None, "c": True, "d": 1.5, "e": float("nan"), "f": ""},
                           {"a": 'say "hi"', "b": 2, "c": False, "d": 10**20, "e": dt.date(2026, 11, 3), "f": None}],
                          ["a", "b", "c", "d", "e", "f"], out)
        self.assertEqual(count, 2)
        expected = ('"a","b","c","d","e","f"\n'
                    '"x,y",,true,1.5,,""\n'
                    '"say ""hi""",2,false,"100000000000000000000","2026-11-03",\n')
        self.assertEqual(out.getvalue().decode(), expected)


class ClientTest(unittest.TestCase):
    def test_refuses_plain_http_off_loopback(self):
        with self.assertRaises(AdoorError):
            Client("http://example.com", "t")
        with self.assertRaises(AdoorError):
            Client("https://user:pass@example.com", "t")
        self.assertEqual(Client("http://127.0.0.1:4337/", "t").url, "http://127.0.0.1:4337")

    def test_requires_token(self):
        with self.assertRaises(AdoorError) as caught:
            Client("https://example.com", "")
        self.assertEqual(caught.exception.code, "TOKEN_REQUIRED")

    def test_rejects_bad_operation(self):
        with self.assertRaises(AdoorError) as caught:
            Client("https://example.com", "t").call("not an operation")
        self.assertEqual(caught.exception.code, "INVALID_OPERATION")

    def test_retries_only_refusals_that_ran_nothing(self):
        class Flaky(Client):
            def __init__(self, failures):
                super().__init__("https://example.com", "t", retries=0)
                self.failures, self.calls = list(failures), 0

            def _request(self, method, path, body=None, timeout=None):
                self.calls += 1
                if self.failures:
                    raise self.failures.pop(0)
                return {"rows": [], "columns": [], "rowCount": 1, "command": "INSERT"}

        busy = AdoorError("QUERY_BUSY", "queue full", 429, retry_after=0.01)
        limited = AdoorError("RATE_LIMITED", "slow down", 429, retry_after=0.01)
        client = Flaky([busy, limited])
        self.assertEqual(client.execute("INSERT INTO t VALUES (1)").row_count, 1)
        self.assertEqual(client.calls, 3)
        client = Flaky([AdoorError("INTERNAL_ERROR", "boom", 500)])
        with self.assertRaises(AdoorError):
            client.execute("INSERT INTO t VALUES (1)")
        self.assertEqual(client.calls, 1, "an unkeyed write that may have run is not retried")

    def test_error_envelope(self):
        error = error_from_body(409, b'{"error":{"code":"CONFLICT","message":"m","retryable":true,"retryAfter":2}}')
        self.assertEqual((error.code, error.status, error.retryable, error.retry_after), ("CONFLICT", 409, True, 2.0))
        self.assertFalse(error.authority)
        self.assertTrue(error_from_body(403, b'{"error":{"code":"FORBIDDEN","message":"no"}}').authority)
        self.assertEqual(error_from_body(502, b"<html>").code, "HTTP_502")


if __name__ == "__main__":
    unittest.main()
