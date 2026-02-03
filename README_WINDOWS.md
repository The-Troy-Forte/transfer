# Wisconsin Provider Medicaid - Windows + DuckDB Edition

## 🎯 Quick Start for Windows

This project has been configured specifically for **Windows** with **DuckDB** as the database.

### Prerequisites

1. **Python 3.8+** - Download from https://www.python.org/downloads/
2. **Windows 10/11** - With PowerShell
3. **40MB disk space** - For DuckDB database and dbt

### 5-Minute Setup

```powershell
# 1. Run the setup script
.\setup_windows.bat

# 2. Place your data file
# Copy your file to: C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv

# 3. Run the pipeline
.\run.ps1 run

# Done! 🎉
```

---

## 📁 Windows Directory Structure

```
C:\data\
├── duckdb\
│   ├── wi_prov_medicaid_dev.duckdb    # Development database
│   ├── wi_prov_medicaid_test.duckdb   # Test database
│   └── wi_prov_medicaid_prod.duckdb   # Production database
│
└── wi_medicaid\
    └── WI_PROV_FILE_EXTRACT.psv       # Your source file goes here

%USERPROFILE%\.dbt\
└── profiles.yml                        # Database connection config
```

---

## 🚀 Usage

### Using PowerShell Helper (Recommended)

```powershell
# View all available commands
.\run.ps1 help

# Run the complete pipeline
.\run.ps1 run

# Run inbound processing only
.\run.ps1 run-inbound

# Run outbound processing only
.\run.ps1 run-outbound

# Test everything
.\run.ps1 test

# Generate documentation
.\run.ps1 docs
```

### Using dbt Directly

```powershell
# Run all models
dbt run

# Run specific tags
dbt run --select tag:inbound
dbt run --select tag:outbound

# Run specific model
dbt run --select stg_provider

# Test
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

---

## 🗄️ DuckDB Benefits

### Why DuckDB?

- ✅ **No server required** - Single file database
- ✅ **Fast** - Optimized for analytics
- ✅ **Portable** - Move database by copying file
- ✅ **SQL-based** - Standard SQL syntax
- ✅ **Free** - Open source
- ✅ **Python integration** - Easy to extend

### DuckDB File Location

Your data is stored in: `C:\data\duckdb\wi_prov_medicaid_dev.duckdb`

You can:
- **Backup**: Copy this file
- **Share**: Send to colleagues
- **Query**: Use DuckDB CLI or any DuckDB client

### Accessing DuckDB Directly

```powershell
# Install DuckDB CLI (optional)
# Download from: https://duckdb.org/docs/installation/

# Open DuckDB database
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb

# Query your data
SELECT * FROM stg_provider LIMIT 10;
```

---

## 📊 Data Flow

### Step 1: Load Raw File
```
C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv
    ↓ (dbt reads directly)
stg_wi_ma_inbound_raw (table in DuckDB)
```

### Step 2: Parse Records
```
stg_wi_ma_inbound_raw
    ↓ (split by pipe delimiter)
int_wi_ma_parsed_records (view)
```

### Step 3: Split by Type
```
int_wi_ma_parsed_records
    ↓ (filter by record_type)
stg_provider (Type 01)
stg_provider_address (Type 02)
stg_provider_tax (Type 03)
... 15 tables total
```

### Step 4: Create Outputs
```
EDS source data (if available)
    ↓ (transform)
out_wi_ma_provider
out_wi_ma_facility
out_wi_ma_control
```

---

## ⚙️ Configuration

### File Paths (dbt_project.yml)

```yaml
vars:
  source_file_path: 'C:\data\wi_medicaid'
  source_file_name: 'WI_PROV_FILE_EXTRACT.psv'
  duckdb_path: 'C:\data\duckdb\wi_prov_medicaid.duckdb'
```

### Database Connection (%USERPROFILE%\.dbt\profiles.yml)

```yaml
wi_prov_medicaid:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'C:\data\duckdb\wi_prov_medicaid_dev.duckdb'
      threads: 4
```

**Note**: On Windows, you can use either:
- Backslashes: `C:\data\duckdb\file.duckdb`
- Forward slashes: `C:/data/duckdb/file.duckdb`
- Double backslashes in YAML: `C:\\data\\duckdb\\file.duckdb`

---

## 🔧 Troubleshooting

### Issue: "dbt command not found"

**Solution**:
```powershell
# Add Python Scripts to PATH
# 1. Find your Python installation
python -c "import sys; print(sys.executable)"

# 2. Add this directory and Scripts folder to PATH:
# Example: C:\Users\YourName\AppData\Local\Programs\Python\Python311
#          C:\Users\YourName\AppData\Local\Programs\Python\Python311\Scripts

# 3. Or run setup again
.\setup_windows.bat
```

### Issue: "Cannot read file"

**Solution**:
```powershell
# Check file exists
Test-Path C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv

# Check permissions
Get-Acl C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv

# Verify path in dbt_project.yml matches
```

### Issue: "Database locked"

**Solution**:
```powershell
# DuckDB is single-writer
# Close any other connections to the database
# Check Task Manager for duckdb.exe processes
```

### Issue: PowerShell Execution Policy

**Solution**:
```powershell
# Allow scripts to run
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Then try again
.\run.ps1 help
```

---

## 📝 Sample Data

### Create Sample PSV File

If you don't have the real file yet, create a test file:

```powershell
# Create directory
New-Item -ItemType Directory -Path "C:\data\wi_medicaid" -Force

# Create sample file
@"
00|TEST_FILE|2024-01-15|5
01|PROV001|ABC Medical Center|ABC Clinic|A|2024-01-01||HOSP|MED001|Active
02|PROV001|MAILING|123 Main St|Suite 100|Madison|WI|53703|Dane|US|2024-01-01|
03|PROV001|12-3456789|EIN
01|PROV002|XYZ Health|XYZ Health|A|2024-01-01||CLINIC|MED002|Active
"@ | Out-File -FilePath "C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv" -Encoding ASCII
```

---

## 🎓 Learning Path

### Day 1: Setup and First Run
1. Run `.\setup_windows.bat`
2. Create or copy sample data file
3. Run `.\run.ps1 run`
4. View results: `.\run.ps1 docs`

### Day 2: Understand the Models
1. Open `models\staging\stg_wi_ma_inbound_raw.sql`
2. See how DuckDB reads the PSV file directly
3. Explore `models\intermediate\int_wi_ma_parsed_records.sql`
4. Check how records are split by type

### Day 3: Query Your Data
```powershell
# Install DuckDB CLI
# Download from https://duckdb.org/

# Query the database
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb

# Run queries
SELECT * FROM stg_provider;
SELECT record_type, count(*) FROM int_wi_ma_parsed_records GROUP BY record_type;
```

### Day 4: Customize
1. Adjust field mappings in type models
2. Add new transformations
3. Create custom tests

---

## 📊 Viewing Results

### Option 1: dbt Docs (Recommended)
```powershell
.\run.ps1 docs
# Opens browser at http://localhost:8080
```

### Option 2: DuckDB CLI
```powershell
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb
```

### Option 3: Python
```python
import duckdb

conn = duckdb.connect('C:/data/duckdb/wi_prov_medicaid_dev.duckdb')
df = conn.execute("SELECT * FROM stg_provider").df()
print(df)
```

### Option 4: Excel/Power BI
- Install ODBC driver from https://duckdb.org/docs/api/odbc
- Connect to: `C:\data\duckdb\wi_prov_medicaid_dev.duckdb`

---

## 🔄 Daily Operations

### Morning: Process New File
```powershell
# 1. Copy new file to C:\data\wi_medicaid\
# 2. Run inbound processing
.\run.ps1 run-inbound

# 3. Verify
.\run.ps1 test
```

### Afternoon: Create Reports
```powershell
# Run outbound models
.\run.ps1 run-outbound

# Export data
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb
COPY (SELECT * FROM out_wi_ma_provider) TO 'C:\data\exports\provider.csv';
```

---

## 📦 Exporting Data from DuckDB

### Export to CSV
```sql
-- In DuckDB CLI
COPY stg_provider TO 'C:\data\exports\provider.csv' (HEADER, DELIMITER ',');
```

### Export to Excel
```python
import duckdb
import pandas as pd

conn = duckdb.connect('C:/data/duckdb/wi_prov_medicaid_dev.duckdb')
df = conn.execute("SELECT * FROM stg_provider").df()
df.to_excel('C:/data/exports/provider.xlsx', index=False)
```

### Export to Parquet
```sql
-- In DuckDB CLI
COPY stg_provider TO 'C:\data\exports\provider.parquet' (FORMAT PARQUET);
```

---

## 🆚 DuckDB vs. Traditional Databases

| Feature | DuckDB | Oracle/SQL Server |
|---------|--------|-------------------|
| Setup | 30 seconds | Hours/Days |
| Cost | Free | $$$ |
| Installation | Single file | Complex installer |
| Maintenance | None | DBA required |
| Performance (Analytics) | Excellent | Good |
| Portability | Copy file | Backup/Restore |
| Learning Curve | Low | Medium-High |

---

## 💡 Tips & Tricks

### Speed Up Development
```powershell
# Only run models you're working on
dbt run --select stg_provider

# Preview without full run
dbt show --select stg_provider --limit 10
```

### Debug Issues
```powershell
# See compiled SQL
dbt compile
# Then check: target\compiled\wi_prov_medicaid\models\...

# Run with debug output
dbt run --select stg_provider --debug
```

### Backup Your Data
```powershell
# Simple backup
Copy-Item "C:\data\duckdb\wi_prov_medicaid_dev.duckdb" "C:\backups\backup_$(Get-Date -Format 'yyyyMMdd').duckdb"

# Or use DuckDB's export
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb
EXPORT DATABASE 'C:\backups\export_20240130';
```

---

## 📚 Additional Resources

- **DuckDB Documentation**: https://duckdb.org/docs/
- **dbt Documentation**: https://docs.getdbt.com/
- **dbt-duckdb Adapter**: https://github.com/duckdb/dbt-duckdb

---

## 🎯 Next Steps

1. ✅ Complete setup: `.\setup_windows.bat`
2. ✅ Add your data file
3. ✅ Run pipeline: `.\run.ps1 run`
4. ✅ View docs: `.\run.ps1 docs`
5. 📖 Read MIGRATION_GUIDE.md for details
6. 🔧 Customize models for your needs
7. 📊 Export results to Excel/CSV
8. 🚀 Schedule with Windows Task Scheduler

---

**Need Help?**

1. Check this README
2. Review README.md (main documentation)
3. Consult MIGRATION_GUIDE.md
4. Check error messages in `logs\dbt.log`

**Ready to go?**
```powershell
.\setup_windows.bat
.\run.ps1 run
```

Good luck! 🚀
