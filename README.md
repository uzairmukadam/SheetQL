# SheetQL: Interactive SQL Query Tool for Data Files

**Query, analyze, and report on your local data files (Excel, CSV, Parquet) using the power of SQL, right from your terminal.**

This tool provides a simple yet powerful command-line interface to load data from multiple spreadsheet files directly into an in-memory database, allowing you to run complex SQL queries without cumbersome manual steps. It's designed for data analysts, business professionals, and anyone who needs to quickly analyze spreadsheet and data files.


---

## 🚀 Key Features

* **Broad File Support**: Natively query **Excel** (`.xlsx`, `.xls`), **CSV** (`.csv`), and **Apache Parquet** (`.parquet`) files.
* **Dynamic File Loading**: Load additional files into your session at any time with the `.load` command without restarting.
* **Interactive SQL Console**: Run standard SQL queries in a live, interactive terminal session.
* **GUI & CLI File Selection**: Uses a graphical file picker if available, but gracefully falls back to a command-line interface on headless systems.
* **Custom Table Names**: Rename the default long table names to shorter, more convenient aliases using the `.rename` command.
* **Professional Excel Reports**: Save multiple query results to a single, beautifully formatted Excel file with styled headers, auto-fitted columns, and filters.
* **Fast & Efficient**: Leverages the high-performance DuckDB analytical engine for near-instant query results.

---

## 📋 System Requirements

* **Python**: Version 3.7 or newer.
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

Alternatively, you can just download the ```sheet_ql.py``` and ```requirements.txt``` files into the same folder.

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

Install all the necessary Python libraries using the provided ```requirements.txt``` file.

```bash
pip install -r requirements.txt
```

## ▶️ How to Run

Once the installation is complete, you can start the application with a single command from your terminal:

```bash
python sheet_ql.py
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
* **Column Names**: If a column name contains spaces, you **must** wrap it in double quotes (e.g., `"Award Amount"`).

### Step 4: Use Meta-Commands

Instead of a SQL query, you can type special commands (starting with a dot):

* `.help`: Displays a list of all available commands.
* `.load`: Opens the file selection dialog to add more files to the current session.
* `.rename <old_name> <new_name>`: Renames a table (view).
* `.exit` or `.quit`: Exits the application and proceeds to the final save step.

### Step 5: Save Your Results

After a query runs successfully, you will be prompted to save the results. You can stage multiple results, and they will all be saved into a single, formatted Excel report when you exit.

## 💡 Troubleshooting

* **No GUI File Dialog Appears**: The tool automatically falls back to a command-line interface if the `tkinter` library is not found. To enable the GUI, install `tkinter` using your system's package manager (e.g., `sudo apt-get install python3-tk`).
* **SQL Error: "Referenced column not found"**:
    * Check that you have wrapped column names with spaces in double quotes (e.g., `"First Name"`).
    * Ensure you are using single quotes for string values in your `WHERE` clause (e.g., `WHERE Region = 'North'`).

## 🤝 Contributing

Contributions are welcome! If you have ideas for new features, bug fixes, or improvements, please feel free to:

1.  Open an issue to discuss what you would like to change.
2.  Fork the repository and create a pull request with your changes.

Please make sure to update tests as appropriate.

## 🚀 Future Features

I have a number of exciting features planned for future releases:

* **Persistent Sessions**: Save and load your entire session, including loaded tables and renames, so you can pick up where you left off.
* **Query History**: A `.history` command to view and re-run previous queries.
* **Saved Queries**: An option to save your favorite or most-used queries with an alias for quick execution.
* **Additional Export Formats**: Support for exporting query results to CSV, JSON, and Markdown.
* **Basic Charting**: A command to generate simple text-based charts in the terminal or save graphical charts to an image file.

## 📄 License

This project is licensed under the MIT License. See the `LICENSE` file for more details.