# SheetQL: Interactive SQL Query Tool for Data Files

**Query, analyze, and report on your local data files using the power of SQL, right from your terminal.**

SheetQL is a powerful command-line tool that transforms your local data files (Excel, CSV, JSON, and Parquet) into a relational database, allowing you to run complex SQL queries without cumbersome manual steps. It's designed for data analysts, software engineers, and anyone who needs to quickly analyze data with the speed and precision of SQL.


---

## 🚀 Key Features

* **Broad File Support**: Natively query **Excel** (`.xlsx`, `.xls`), **CSV** (`.csv`), **JSON** (`.json`, `.jsonl`) and **Apache Parquet** (`.parquet`) files.
* **Powerful Automation**: Execute complex workflows non-interactively with **YAML scripts** for reproducible analysis and reporting.
* **Interactive SQL Console**: Run standard SQL queries in a live, multi-line terminal session with command history.
* **Live Session Introspection**: Check table structures with the `.schema` command and review past queries with `.history`.
* **Dynamic File Loading**: Load additional files into your session at any time with the `.load` command without restarting.
* **Interactive SQL Console**: Run standard SQL queries in a live, interactive terminal session.
* **GUI & CLI File Selection**: Uses a graphical file picker if available, but gracefully falls back to a command-line interface on headless systems.
* **Custom Table Names**: Rename the default long table names to shorter, more convenient aliases using the `.rename` command.
* **Professional Excel Reports**: Save multiple query results to a single, beautifully formatted Excel file with styled headers, auto-fitted columns, and filters.
* **Fast & Efficient**: Leverages the high-performance DuckDB analytical engine for near-instant query results.

---

## 📋 System Requirements

* **Python**: Version 3.9 or newer.
* **Operating System**: Windows, macOS, or Linux.
* **Tkinter (Optional)**: For the graphical file dialogs. If not present, the tool will use a command-line fallback.

---

## 🛠️ Installation Guide

### 1. Clone the Repository (Optional)

If you have Git installed, this is the recommended way to get the project files.

```bash
git clone [https://github.com/uzairmukadam/sheetql.git](https://github.com/uzairmukadam/sheetql.git)
cd sheetql
```

Alternatively, you can just download the `sheet_ql.py` and `requirements.txt` files into the same folder.

### 2. Set Up a Virtual Environment (Recommended)

Using a virtual environment keeps your project dependencies isolated from your system's Python installation.

```bash
# Create the virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Required Libraries

Install all the necessary Python libraries using the provided `requirements.txt` file.

```bash
pip install -r requirements.txt
```

## ▶️ How to Run

### Interactive Mode

Start the application with a single command to enter the interactive SQL shell.

```bash
python sheet_ql.py
```

### Batch Mode (via YAML)

Execute a predefined script non-interactively. This is perfect for automated reporting.

```bash
python sheet_ql.py --run your_script.yml
```

## 📖 Usage Instructions

### Step 1: Select Your Data Files

* **With GUI**: A file dialog will pop up. Select one or more data files.
* **Without GUI**: If `tkinter` is not installed, you will be prompted to enter the full file paths directly in the terminal.

### Step 2: Identify and Rename Your Tables

After loading, the tool will print a list of all available tables. You can rename any table for convenience using the `.rename` command.

### Step 3: Write SQL Queries

You can now type your SQL queries directly into the terminal.
* **End with a Semicolon**: Every SQL query must end with a semicolon (`;`).
* **Column Names**: If a column name contains spaces, you **must** wrap it in double quotes (e.g., `"Total Amount"`).

### Step 4: Use Meta-Commands

Instead of a SQL query, you can type special commands (starting with a dot):

* `.help`: Displays a list of all available commands.
* `.tables`: Displays a list all available tables.
* `.schema <table_name>`: Shows the columns and data types for a table.
* `.history`: Displays the last 50 queries.
* `.load`: Opens the file selection dialog to add more files to the current session.
* `.rename <old_name> <new_name>`: Renames a table (view).
* `.runscript [path]`: Executes a YAML script in the current session.
* `.export`: Exports all the staged results to an Excel file.
* `.exit` or `.quit`: Exits the application and proceeds to the final save step.

### Step 5: Rerun from History

Type `!N` (e.g., `!5`) to re-execute the Nth query from your `.history`.

### Step 6: Save Your Results

After a query runs successfully, you will be prompted to stage the results for saving. You can stage multiple results and then use the `.export` command to save them all to a single, formatted Excel report.

## ⚙️ Automated Execution with YAML

You can automate a sequence of actions by creating a `.yml` script file. This allows you to load files, apply aliases, run queries, and export a report with a single command.

Example `script.yml`:

```bash
inputs:
  - path: './data/sales.xlsx'
    alias: sales
  - path: './data/employees.csv'
    alias: emps

tasks:
  - name: 'High_Value_Sales'
    sql: >
      SELECT * FROM sales_xlsx_Sheet1
      WHERE Amount > 5000;
  
  - name: 'Sales_by_Employee'
    sql: >
      SELECT e.Name, COUNT(s.ID) as TotalSales
      FROM sales_xlsx_Sheet1 s JOIN emps_csv e ON s.EmpID = e.ID
      GROUP BY e.Name;

export:
  path: './reports/final_report.xlsx'
```

## 💡 Troubleshooting

* **No GUI File Dialog Appears**: The tool automatically falls back to a command-line interface if the `tkinter` library is not found. To enable the GUI, install `tkinter` using your system's package manager (e.g., `sudo apt-get install python3-tk`).
* **SQL Error: "Referenced column not found"**:
    * Check that you have wrapped column names with spaces in double quotes (e.g., `"First Name"`).
    * Ensure you are using single quotes for string values in your `WHERE` clause (e.g., `WHERE Region = 'North'`).

## 🤝 Contributing

Contributions are welcome! If you have ideas for new features, bug fixes, or improvements, please feel free to:

1.  Open an issue to discuss what you would like to change.
2.  Fork the repository and create a pull request with your changes.

## 🚀 Future Features

* **Persistent Sessions**: Save and load your entire session, including loaded tables and renames, so you can pick up where you left off.
* **Saved Queries**: An option to save your favorite or most-used queries with an alias for quick execution.
* **Additional Export Formats**: Support for exporting query results to CSV, JSON, and Markdown.
* **Basic Charting**: A command to generate simple text-based charts in the terminal or save graphical charts to an image file.

## 📄 License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
