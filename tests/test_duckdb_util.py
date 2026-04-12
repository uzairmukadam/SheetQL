"""Unit tests for sheetql.duckdb_util (no SheetQL engine required)."""

import unittest

import duckdb

from sheetql.duckdb_util import fetch_columns_by_table, quote_duckdb_identifier


class TestDuckdbUtil(unittest.TestCase):
    def test_quote_identifier_basic(self) -> None:
        self.assertEqual(quote_duckdb_identifier("my_table"), '"my_table"')

    def test_quote_identifier_escapes_embedded_quotes(self) -> None:
        self.assertEqual(quote_duckdb_identifier('a"b'), '"a""b"')

    def test_quote_identifier_empty_raises(self) -> None:
        with self.assertRaises(ValueError):
            quote_duckdb_identifier("")

    def test_fetch_columns_by_table_single_view(self) -> None:
        con = duckdb.connect(":memory:")
        con.execute("CREATE VIEW myv AS SELECT 1 AS col_a, 2 AS col_b")
        got = fetch_columns_by_table(con, ["myv"])
        self.assertEqual(got["myv"], ["col_a", "col_b"])

    def test_fetch_columns_by_table_multiple(self) -> None:
        con = duckdb.connect(":memory:")
        con.execute("CREATE VIEW v1 AS SELECT 1 AS x")
        con.execute("CREATE VIEW v2 AS SELECT 'a' AS y, 'b' AS z")
        got = fetch_columns_by_table(con, ["v1", "v2"])
        self.assertEqual(got["v1"], ["x"])
        self.assertEqual(got["v2"], ["y", "z"])


if __name__ == "__main__":
    unittest.main()
