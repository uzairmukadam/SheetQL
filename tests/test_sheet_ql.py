import unittest
import os
import shutil
import logging
import pandas as pd
from rich.tree import Tree as RichTree
from unittest.mock import patch, MagicMock

try:
    from sheetql.engine import SheetQL

    ENGINE_AVAILABLE = True
except ModuleNotFoundError as e:
    # Allow running validation-only tests in minimal environments.
    ENGINE_AVAILABLE = False
    _engine_import_error = e

from sheetql.scripting import parse_script_config, ScriptConfigError
from sheetql.naming import normalize_name

try:
    import yaml  # type: ignore

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class TestSheetQL(unittest.TestCase):
    """
    Test suite for SheetQL functionality including Zero-Copy loading,
    Session Recording, and CLI meta-commands.
    """

    def setUp(self):
        """Initializes the test environment with sandbox directory and dummy files."""
        if not ENGINE_AVAILABLE:
            self.skipTest(f"Engine dependencies missing: {_engine_import_error}")
        self.test_dir = "test_env_sandbox"
        os.makedirs(self.test_dir, exist_ok=True)

        self.csv_path = os.path.join(self.test_dir, "sales_2023.csv")
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "sales_rep": ["Alice", "Bob", "Charlie"],
                "amount": [100, 200, 150],
            }
        ).to_csv(self.csv_path, index=False)

        self.excel_path = os.path.join(self.test_dir, "targets.xlsx")
        with pd.ExcelWriter(self.excel_path) as writer:
            pd.DataFrame({"city": ["NY", "LA"], "target": [1000, 500]}).to_excel(
                writer, sheet_name="Q1_Targets", index=False
            )

        # Initialize with a dummy logger to suppress console output during tests
        self.test_logger = logging.getLogger("TestLogger")
        self.test_logger.setLevel(logging.CRITICAL)

        self.tool = SheetQL(self.test_logger)
        self.tool._init_db()

    def tearDown(self):
        """
        Aggressively cleans up resources to prevent file lock issues on Windows.
        """
        # 1. Close Database Connection (Releases locks on CSV/Parquet files)
        if self.tool.db_connection:
            try:
                self.tool.db_connection.close()
            except Exception:
                pass

        # 2. Close Logger Handlers (Releases locks on log files)
        if hasattr(self.tool, "logger"):
            for handler in self.tool.logger.handlers:
                try:
                    handler.close()
                except Exception:
                    pass

        # 3. Remove Sandbox Directory (With retry logic for lingering locks)
        if os.path.exists(self.test_dir):

            def on_rm_error(func, path, exc_info):
                # Attempt to change permission and retry
                try:
                    os.chmod(path, 0o777)
                    func(path)
                except Exception:
                    pass

            shutil.rmtree(self.test_dir, onerror=on_rm_error)

    def test_01_zero_copy_loading(self):
        """Verifies that files are correctly registered as DuckDB views."""
        loaded = self.tool._load_data([self.csv_path, self.excel_path])

        self.assertIn("sales_2023_csv", loaded)
        self.assertIn("targets_q1_targets", loaded)

        tables = (
            self.tool.db_connection.execute("SHOW TABLES").fetchdf()["name"].tolist()
        )
        self.assertIn("sales_2023_csv", tables)
        self.assertIn("targets_q1_targets", tables)

    def test_02_schema_caching(self):
        """Verifies that the autocomplete schema cache populates on load."""
        self.tool._load_data([self.csv_path])

        table_name = "sales_2023_csv"
        self.assertIn(table_name, self.tool.schema_cache)

        columns = self.tool.schema_cache[table_name]
        self.assertIn("sales_rep", columns)
        self.assertIn("amount", columns)

    def test_03_query_execution(self):
        """Verifies SQL execution logic and data retrieval."""
        self.tool._load_data([self.csv_path])

        # Suppress UI interactions
        self.tool.console = MagicMock()

        query = "SELECT sales_rep FROM sales_2023_csv WHERE amount > 150"
        self.tool._execute_query(query)

        res = self.tool.db_connection.execute(query).fetchdf()
        self.assertEqual(res.iloc[0]["sales_rep"], "Bob")

    def test_04_session_recorder(self):
        """Verifies that loading and querying actions are recorded."""
        self.tool._load_data([self.csv_path])
        self.assertEqual(len(self.tool.recorder.inputs), 1)
        self.assertEqual(self.tool.recorder.inputs[0]["path"], self.csv_path)

        df = pd.DataFrame({"a": [1]})
        self.tool.console.input = MagicMock(side_effect=["y", "my_export"])

        self.tool._prompt_to_stage_results(df, "SELECT * FROM sales")

        self.assertEqual(len(self.tool.recorder.transformations), 1)
        self.assertEqual(
            self.tool.recorder.transformations[0]["sql"], "SELECT * FROM sales"
        )

    def test_05_yaml_script_generation(self):
        """Verifies that the .dump command produces valid YAML."""
        if not YAML_AVAILABLE:
            self.skipTest("PyYAML not installed")
        self.tool.recorder.record_load("data.csv", "raw_data")
        self.tool.recorder.record_query("clean_data", "SELECT * FROM raw_data")

        yaml_out = self.tool.recorder.generate_yaml()
        parsed = yaml.safe_load(yaml_out)

        self.assertEqual(parsed["inputs"][0]["alias"], "raw_data")
        self.assertEqual(parsed["tasks"][0]["name"], "clean_data")

    def test_06_yaml_aliasing_logic(self):
        """Verifies that batch processing respects YAML aliases."""
        if not YAML_AVAILABLE:
            self.skipTest("PyYAML not installed")
        config = {"inputs": [{"path": self.csv_path, "alias": "revenue_data"}]}

        self.tool._execute_yaml_script(config)

        tables = (
            self.tool.db_connection.execute("SHOW TABLES").fetchdf()["name"].tolist()
        )
        self.assertIn("revenue_data", tables)

    def test_06e_yaml_alias_excel_registered_table(self):
        """YAML aliases must rename BASE TABLE (Excel) objects, not only VIEWs."""
        if not YAML_AVAILABLE:
            self.skipTest("PyYAML not installed")
        config = {"inputs": [{"path": self.excel_path, "alias": "targets_alias"}]}
        self.tool._execute_yaml_script(config)
        tables = (
            self.tool.db_connection.execute("SHOW TABLES").fetchdf()["name"].tolist()
        )
        self.assertIn("targets_alias", tables)

    def test_06b_yaml_validation_errors(self):
        """Verifies scripting validation catches malformed configs."""
        with self.assertRaises(ScriptConfigError):
            parse_script_config({"inputs": "not-a-list"})

    def test_06f_memory_limit_rejects_unsafe_values(self):
        """options.memory_limit must not allow string breakouts in SET ..."""
        with self.assertRaises(ScriptConfigError):
            parse_script_config(
                {"options": {"memory_limit": "1GB'; ATTACH 'x'; --"}, "inputs": []}
            )

    def test_06c_task_level_exports_parse(self):
        cfg = {
            "tasks": [
                {"name": "t1", "sql": "select 1", "export": {"path": "out/t1.csv"}},
                {
                    "name": "t2",
                    "sql": "select 2",
                    "export": {"path": "out/t2.xlsx", "sheet": "sheet2"},
                },
            ]
        }
        _, tasks, export, _ = parse_script_config(cfg)
        self.assertIsNone(export)
        self.assertEqual(tasks[0].export.path, "out/t1.csv")
        self.assertEqual(tasks[1].export.sheet, "sheet2")

    def test_06d_normalize_name_numeric_leading(self):
        """normalize_name should handle numeric-leading and weird characters."""
        self.assertEqual(normalize_name("2026 Jan&Feb Report"), "t_2026_jan_feb_report")
        self.assertEqual(normalize_name("__My-Table__"), "my_table")
        self.assertEqual(normalize_name("   123"), "t_123")

    @patch("sheetql.engine.SheetQL._export_results")
    def test_07_exit_with_staging_prompt(self, mock_export):
        """Verifies that quitting triggers an export prompt if data is staged."""
        self.tool.results_to_save["test_sheet"] = pd.DataFrame()
        self.tool.console.input = MagicMock(return_value="y")

        should_exit = self.tool._handle_meta_command(".exit")

        self.assertTrue(should_exit)
        mock_export.assert_called_once()

    def test_08_rename_command(self):
        """Verifies the .rename meta-command updates DB and Cache."""
        self.tool._load_data([self.csv_path])
        self.tool._handle_meta_command(".rename sales_2023_csv old_sales")

        tables = (
            self.tool.db_connection.execute("SHOW TABLES").fetchdf()["name"].tolist()
        )
        self.assertIn("old_sales", tables)
        self.assertNotIn("sales_2023_csv", tables)

        self.assertIn("old_sales", self.tool.schema_cache)

    def test_09_peek_skips_staging_prompt(self) -> None:
        """`.peek` runs a LIMIT query without prompting to stage for export."""
        self.tool._load_data([self.csv_path])
        self.tool.console = MagicMock()
        self.tool._peek_table([".peek", "sales_2023_csv", "2"])
        self.tool.console.input.assert_not_called()

    def test_10_count_table_rows(self) -> None:
        """`.count` reports row count for a loaded table."""
        self.tool._load_data([self.csv_path])
        self.tool.console = MagicMock()
        self.tool._count_table_rows([".count", "sales_2023_csv"])
        self.tool.console.print.assert_called()
        printed = " ".join(
            str(call.args[0]) for call in self.tool.console.print.call_args_list
        )
        self.assertIn("3", printed.replace(",", ""))

    def test_11_list_loaded_files(self) -> None:
        """`.files` prints a Rich tree of loaded paths and table names."""
        self.tool._load_data([self.csv_path])
        self.tool.console = MagicMock()
        self.tool._list_loaded_files()
        self.tool.console.print.assert_called_once()
        arg0 = self.tool.console.print.call_args[0][0]
        self.assertIsInstance(arg0, RichTree)
        self.assertGreater(len(arg0.children), 0)

    def test_12_unknown_meta_command(self) -> None:
        self.assertFalse(self.tool._handle_meta_command(".nosuchcommand"))


if __name__ == "__main__":
    unittest.main()
