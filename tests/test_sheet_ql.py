import unittest
import os
import sys
import io
import pandas as pd
import shutil
from unittest.mock import patch
import duckdb

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sheet_ql import SheetQL

class TestSheetQL(unittest.TestCase):
    """Test suite for the SheetQL application."""

    def setUp(self):
        """Set up a clean environment for each test."""
        self.test_dir = 'test_temp_data'
        os.makedirs(self.test_dir, exist_ok=True)
        
        # --- Create Sample Data Files ---
        # 1. CSV
        self.csv_path = os.path.join(self.test_dir, 'sample.csv')
        pd.DataFrame({
            'ID': [1, 2, 3], 'Name': ['Alice', 'Bob', 'Charlie'], 'Value': [100, 200, 150]
        }).to_csv(self.csv_path, index=False)

        # 2. Excel
        self.excel_path = os.path.join(self.test_dir, 'sample.xlsx')
        with pd.ExcelWriter(self.excel_path) as writer:
            pd.DataFrame({'City': ['NY', 'LA'], 'Population': [8.4, 3.9]}).to_excel(writer, sheet_name='Cities', index=False)

        # 3. JSON
        self.json_path = os.path.join(self.test_dir, 'sample.json')
        pd.DataFrame({'Product': ['Widget', 'Gadget']}).to_json(self.json_path, orient='records')

        # 4. YAML Script
        self.script_path = os.path.join(self.test_dir, 'script.yml')
        with open(self.script_path, 'w') as f:
            f.write(f"""
inputs:
  - path: '{self.csv_path}'
    alias: my_data
tasks:
  - name: 'filtered_data'
    sql: >
      SELECT * FROM my_data WHERE Value > 120;
""")

        self.tool = SheetQL()
        self.tool.console = unittest.mock.MagicMock()
        self.tool.db_connection = duckdb.connect(database=':memory:')

    def tearDown(self):
        """Clean up files and directories after each test."""
        self.tool.db_connection.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_01_file_loading(self):
        """Test that CSV, Excel, and JSON files are loaded correctly."""
        all_files = [self.csv_path, self.excel_path, self.json_path]
        loaded_dfs = self.tool._load_data(all_files)
        self.tool._register_dataframes(loaded_dfs)
        
        tables_df = self.tool.db_connection.execute("SHOW TABLES;").fetchdf()
        table_names = tables_df['name'].tolist()
        
        self.assertIn('sample_csv', table_names)
        self.assertIn('sample_xlsx_Cities', table_names)
        self.assertIn('sample_json', table_names)

    def test_02_sql_query_execution(self):
        """Test that a basic SQL query runs and returns correct data."""
        loaded_dfs = self.tool._load_data([self.csv_path])
        self.tool._register_dataframes(loaded_dfs)
        
        result_df = self.tool.db_connection.execute("SELECT Name FROM sample_csv WHERE Value = 200;").fetchdf()
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df['Name'][0], 'Bob')

    def test_03_schema_command(self):
        """Test the '.schema' meta-command."""
        loaded_dfs = self.tool._load_data([self.csv_path])
        self.tool._register_dataframes(loaded_dfs)
        
        schema_df = self.tool.db_connection.execute("DESCRIBE sample_csv;").fetchdf()
        column_names = schema_df['column_name'].tolist()
        
        self.assertListEqual(column_names, ['ID', 'Name', 'Value'])

    def test_04_rename_command(self):
        """Test the '.rename' meta-command."""
        loaded_dfs = self.tool._load_data([self.csv_path])
        self.tool._register_dataframes(loaded_dfs)
        
        self.tool._rename_table(['.rename', 'sample_csv', 'renamed_csv'])
        
        tables_df = self.tool.db_connection.execute("SHOW TABLES;").fetchdf()
        table_names = tables_df['name'].tolist()
        
        self.assertIn('renamed_csv', table_names)
        self.assertNotIn('sample_csv', table_names)

    def test_05_history_feature(self):
        """Test the query history functionality."""
        self.tool.history.append("SELECT 1;")
        self.tool.history.append("SELECT * FROM foo;")
        
        self.assertEqual(len(self.tool.history), 2)
        self.assertEqual(list(self.tool.history)[0], "SELECT 1;")

    @patch('builtins.input', return_value='n')
    def test_06_yaml_script_interactive_run(self, mock_input):
        """Test running a partial YAML script with the '.runscript' command."""
        self.tool._run_script_interactive(['.runscript', self.script_path])
        
        self.assertIn('filtered_data', self.tool.results_to_save)
        result_df = self.tool.results_to_save['filtered_data']
        
        self.assertEqual(len(result_df), 2)
        self.assertSetEqual(set(result_df['Name']), {'Bob', 'Charlie'})
        
if __name__ == '__main__':
    unittest.main()