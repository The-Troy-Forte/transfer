# Wisconsin Provider Medicaid - Windows Quick Reference

## 📋 **Cheat Sheet for Windows Users**

### First Time Setup (Run Once)
```cmd
setup_windows.bat
```

### Common Commands

| Task | PowerShell Command | Direct dbt Command |
|------|-------------------|-------------------|
| Run everything | `.\run.ps1 run` | `dbt run` |
| Test | `.\run.ps1 test` | `dbt test` |
| View docs | `.\run.ps1 docs` | `dbt docs generate && dbt docs serve` |
| Run inbound only | `.\run.ps1 run-inbound` | `dbt run --select tag:inbound` |
| Clean up | `.\run.ps1 clean` | `rm -r target dbt_packages logs` |
| Debug connection | `.\run.ps1 debug` | `dbt debug` |

### File Locations

```
C:\data\
├── duckdb\
│   └── wi_prov_medicaid_dev.duckdb    ← Your database
└── wi_medicaid\
    └── WI_PROV_FILE_EXTRACT.psv       ← Your source file

%USERPROFILE%\.dbt\
└── profiles.yml                        ← Database config

[Project folder]\
├── README_WINDOWS.md                   ← START HERE
├── setup_windows.bat                   ← Run me first
├── run.ps1                             ← Use me often
└── generate_sample_data.py             ← Create test data
```

### Troubleshooting

| Problem | Solution |
|---------|----------|
| "dbt not found" | `pip install dbt-core dbt-duckdb` |
| "Cannot find file" | Check `C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv` exists |
| "Permission denied" | Run PowerShell as Administrator |
| "Execution policy" | `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` |
| Need sample data | `python generate_sample_data.py` |

### Database Access

```powershell
# Option 1: Using Python
python
>>> import duckdb
>>> conn = duckdb.connect('C:/data/duckdb/wi_prov_medicaid_dev.duckdb')
>>> conn.execute("SELECT * FROM stg_provider LIMIT 5").df()

# Option 2: Using DuckDB CLI (download from duckdb.org)
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb
SELECT * FROM stg_provider;
```

### Export Data

```sql
-- In DuckDB CLI
COPY stg_provider TO 'C:\exports\provider.csv' (HEADER, DELIMITER ',');
COPY stg_provider TO 'C:\exports\provider.xlsx' WITH (FORMAT GDAL, DRIVER 'XLSX');
```

### Workflow

```
1. Place file → C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv
2. Run        → .\run.ps1 run
3. Test       → .\run.ps1 test
4. Export     → Query DuckDB or use dbt docs
```

### Daily Operation

```powershell
# Morning: Process new file
Copy-Item "\\network\share\newfile.psv" "C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv"
.\run.ps1 run-inbound
.\run.ps1 test

# Afternoon: Create outputs
.\run.ps1 run-outbound

# Export results
duckdb C:\data\duckdb\wi_prov_medicaid_dev.duckdb
COPY out_wi_ma_provider TO 'C:\exports\provider_$(date +%Y%m%d).csv';
```

### Backup

```powershell
# Backup database
Copy-Item "C:\data\duckdb\wi_prov_medicaid_dev.duckdb" `
          "C:\backups\backup_$(Get-Date -Format 'yyyyMMdd').duckdb"

# Backup project
Compress-Archive -Path "C:\projects\wi_prov_medicaid" `
                 -DestinationPath "C:\backups\project_$(Get-Date -Format 'yyyyMMdd').zip"
```

### Key Differences from Linux/Oracle Version

| Aspect | This Version (Windows/DuckDB) | Original (Linux/Oracle) |
|--------|------------------------------|------------------------|
| OS | Windows | Linux |
| Database | DuckDB (file-based) | Oracle (server) |
| Path separator | `\` (backslash) | `/` (forward slash) |
| Setup | `setup_windows.bat` | `make setup` |
| Commands | `.\run.ps1` or `dbt` | `make` or `dbt` |
| Profile location | `%USERPROFILE%\.dbt\` | `~/.dbt/` |
| File reading | DuckDB native | External load |

### Performance Tips

```yaml
# In profiles_duckdb_windows.yml
settings:
  memory_limit: '8GB'    # Increase if you have RAM
  threads: 8             # Match your CPU cores
```

### Next Steps

1. ✅ Read `README_WINDOWS.md` (detailed guide)
2. ✅ Run `setup_windows.bat`
3. ✅ Run `python generate_sample_data.py` (if no real data)
4. ✅ Run `.\run.ps1 run`
5. ✅ Run `.\run.ps1 docs` to see results

### Help Resources

- **Windows Guide**: README_WINDOWS.md
- **Full Documentation**: README.md
- **Migration Guide**: MIGRATION_GUIDE.md
- **Command Help**: `.\run.ps1 help`
- **dbt Help**: `dbt --help`

---

**Quick Start**
```cmd
setup_windows.bat
python generate_sample_data.py
.\run.ps1 run
.\run.ps1 docs
```

Done! 🎉
