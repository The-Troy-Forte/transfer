# Dagster Orchestration Guide - Windows

## 🎯 What is Dagster?

Dagster is a modern data orchestrator that provides:
- **Asset-based orchestration**: Track data products (tables, files) not just tasks
- **Built-in lineage**: Automatic dependency tracking between dbt models
- **Rich UI**: Web interface for monitoring, debugging, and managing pipelines
- **Scheduling**: Run pipelines on schedules or based on events
- **Data quality**: Test and validate data quality
- **Observability**: Track performance and data freshness

## 🚀 Quick Start

### 1. Install Dagster
```powershell
# Install all dependencies including Dagster
pip install -r requirements.txt

# Or install Dagster separately
.\run.ps1 install-dagster
```

### 2. Start Dagster UI
```powershell
# Option 1: Using run.ps1
.\run.ps1 dagster-dev

# Option 2: Direct command
cd dagster_project
dagster dev

# Opens UI at: http://localhost:3000
```

### 3. Explore the UI
1. Open browser to **http://localhost:3000**
2. Navigate to **Assets** to see all dbt models
3. Click **Materialize all** to run the pipeline
4. View lineage graph to see dependencies

## 📊 Understanding Dagster Concepts

### Assets
Each dbt model becomes a Dagster asset:
- `stg_wi_ma_inbound_raw` → Asset
- `int_wi_ma_parsed_records` → Asset
- `stg_provider` → Asset
- etc.

Dagster automatically tracks dependencies between them.

### Jobs
Pre-configured pipelines:
- **wi_medicaid_inbound_job**: Process inbound files only
- **wi_medicaid_outbound_job**: Create outbound extracts only
- **wi_medicaid_full_pipeline**: Run everything

### Schedules
Automated runs at specific times:
- **daily_wi_medicaid_pipeline**: Runs at 2 AM daily
- **business_days_wi_medicaid**: Monday-Friday only
- **inbound_every_6_hours**: Check for new files every 6 hours

### Sensors
Event-driven triggers:
- **wi_medicaid_file_sensor**: Triggers when new file appears
- Monitors: `C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv`

## 🎮 Using Dagster

### Running Pipelines

#### Via UI (Recommended for Beginners)
1. Open http://localhost:3000
2. Go to **Assets**
3. Click **Materialize all** or select specific assets
4. Watch progress in real-time

#### Via PowerShell
```powershell
# Start Dagster UI
.\run.ps1 dagster-dev

# Materialize all assets
.\run.ps1 dagster-materialize

# Run inbound job
.\run.ps1 dagster-inbound

# Run outbound job
.\run.ps1 dagster-outbound
```

#### Via Dagster CLI
```powershell
cd dagster_project

# Materialize specific assets
dagster asset materialize --select stg_wi_ma_inbound_raw

# Run a job
dagster job execute -j wi_medicaid_full_pipeline

# List all assets
dagster asset list
```

### Enabling Schedules

1. Go to **Overview** → **Schedules**
2. Find `daily_wi_medicaid_pipeline`
3. Toggle **ON**
4. Pipeline will now run automatically at 2 AM daily

Or via CLI:
```powershell
cd dagster_project
dagster schedule start daily_wi_medicaid_pipeline
```

### Enabling Sensors

1. Go to **Overview** → **Sensors**
2. Find `wi_medicaid_file_sensor`
3. Toggle **ON**
4. Sensor will check for new files every 30 seconds

Or via CLI:
```powershell
cd dagster_project
dagster sensor start wi_medicaid_file_sensor
```

## 📁 Project Structure

```
dagster_project/
├── __init__.py                          # Main Definitions
├── workspace.yaml                       # Workspace config
├── README.md                           # This file
│
├── assets/
│   ├── dbt_assets.py                   # dbt model assets
│   └── __init__.py
│
├── resources/
│   ├── dbt_resource.py                 # dbt configuration
│   └── __init__.py
│
├── sensors/
│   ├── file_sensor.py                  # File arrival sensors
│   └── __init__.py
│
├── schedules/
│   ├── daily_schedule.py               # Time-based schedules
│   └── __init__.py
│
└── ops/
    └── __init__.py                     # Custom operations
```

## 🔄 Workflows

### Daily Automated Workflow

1. **Enable schedule** (one-time):
   ```powershell
   cd dagster_project
   dagster schedule start daily_wi_medicaid_pipeline
   ```

2. **Keep Dagster running**:
   ```powershell
   # Run as a service or in a dedicated PowerShell window
   .\run.ps1 dagster-dev
   ```

3. Pipeline runs automatically at 2 AM every day

### Event-Driven Workflow

1. **Enable file sensor** (one-time):
   ```powershell
   cd dagster_project
   dagster sensor start wi_medicaid_file_sensor
   ```

2. **Drop file**:
   ```powershell
   Copy-Item "\\network\share\newfile.psv" `
             "C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv"
   ```

3. Sensor detects file and triggers pipeline automatically

### Manual Workflow

```powershell
# 1. Start Dagster UI
.\run.ps1 dagster-dev

# 2. In browser at http://localhost:3000:
#    - Go to Assets
#    - Click "Materialize all"
#    - Or select specific assets

# 3. Monitor progress in UI
```

## 📈 Monitoring and Observability

### View Run History
1. Go to **Runs** tab
2. See all pipeline executions
3. Filter by status (Success, Failed, Running)
4. Click any run to see details

### Asset Lineage
1. Go to **Assets**
2. Click any asset (e.g., `stg_provider`)
3. View **Lineage** graph showing upstream/downstream dependencies
4. See which assets need to run before/after

### Logs
1. Click any asset or run
2. View **Logs** tab
3. See detailed execution logs
4. Filter by log level (INFO, WARNING, ERROR)

### Data Quality
Dagster automatically runs dbt tests:
1. Go to **Assets**
2. Click an asset
3. View **Checks** to see test results
4. Failed tests show in red

## 🎨 Customization

### Adding Custom Schedules

Edit `dagster_project/schedules/daily_schedule.py`:

```python
from dagster import ScheduleDefinition

custom_schedule = ScheduleDefinition(
    name="custom_schedule",
    job_name="wi_medicaid_full_pipeline",
    cron_schedule="0 8 * * *",  # 8 AM daily
    description="Custom schedule",
)
```

Then add to `__init__.py` schedules list.

### Adding Custom Sensors

Create new sensor in `dagster_project/sensors/`:

```python
from dagster import sensor, RunRequest

@sensor(job_name="wi_medicaid_inbound_job")
def my_sensor(context):
    # Your logic here
    if condition:
        return RunRequest()
    return SkipReason("Not ready")
```

### Creating Alerts

```python
# In dagster_project/__init__.py
from dagster import make_email_on_run_failure_sensor

email_on_failure = make_email_on_run_failure_sensor(
    email_from="dagster@company.com",
    email_to=["team@company.com"],
)

# Add to defs:
defs = Definitions(
    # ... other definitions
    sensors=[
        wi_medicaid_file_sensor,
        email_on_failure,
    ],
)
```

## 🔧 Advanced Features

### Partitioned Assets

For processing historical data:

```python
from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01"
)

@asset(partitions_def=daily_partitions)
def daily_provider_snapshot():
    # Process one day at a time
    pass
```

### Asset Checks

Add custom data quality checks:

```python
from dagster import asset_check, AssetCheckResult

@asset_check(asset="stg_provider")
def check_provider_count():
    # Run custom validation
    count = query_provider_count()
    return AssetCheckResult(
        passed=count > 0,
        description=f"Found {count} providers"
    )
```

### Resource Configuration

Configure different environments:

```python
# dagster_project/resources/dbt_resource.py
from dagster import EnvVar

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    target=EnvVar("DBT_TARGET"),  # From environment
)
```

Then set environment variable:
```powershell
$env:DBT_TARGET = "prod"
.\run.ps1 dagster-dev
```

## 🐛 Troubleshooting

### Issue: "Module not found"
```powershell
# Ensure you're in the right directory
cd dagster_project
dagster dev

# Or use full path
dagster dev -f __init__.py
```

### Issue: "Port 3000 already in use"
```powershell
# Change port
dagster dev -p 3001

# Or kill existing process
Get-Process | Where-Object {$_.ProcessName -like "*dagster*"} | Stop-Process
```

### Issue: "Cannot find dbt project"
```powershell
# Check paths in dagster_project/resources/dbt_resource.py
# Should point to parent directory containing dbt_project.yml
```

### Issue: Sensor not triggering
1. Check sensor is enabled in UI
2. Verify file path is correct
3. Check Dagster logs for errors
4. Ensure file modification time is newer

### Issue: Schedule not running
1. Verify schedule is enabled
2. Check cron expression is correct
3. Ensure Dagster daemon is running
4. Check timezone settings

## 📊 Performance Tips

### Optimize for Large Pipelines
```python
# Use parallelization
@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(
            enable_asset_checks=True
        )
    ),
)
```

### Resource Configuration
```yaml
# In profiles.yml, increase DuckDB performance
settings:
  memory_limit: '8GB'
  threads: 8
  max_memory: '8GB'
```

## 🌐 Production Deployment

### Option 1: Windows Service

Use NSSM to run as Windows service:
```powershell
# Download NSSM from nssm.cc
nssm install DagsterWI "C:\Python\Scripts\dagster.exe" "dev -w dagster_project\workspace.yaml"
nssm start DagsterWI
```

### Option 2: Task Scheduler

1. Open Task Scheduler
2. Create Task: "Start Dagster"
3. Trigger: At system startup
4. Action: Run PowerShell script
5. Script: `.\run.ps1 dagster-dev`

### Option 3: Cloud Deployment

Consider Dagster Cloud for production:
- Managed infrastructure
- Built-in monitoring
- Easy scaling
- Free tier available

## 📚 Additional Resources

- **Dagster Docs**: https://docs.dagster.io
- **dbt-Dagster Integration**: https://docs.dagster.io/integrations/dbt
- **University**: https://dagster.io/university
- **Community Slack**: https://dagster.io/slack

## 🎯 Next Steps

1. ✅ Install Dagster: `.\run.ps1 install-dagster`
2. ✅ Start UI: `.\run.ps1 dagster-dev`
3. ✅ Explore assets in browser
4. ✅ Run pipeline manually
5. ✅ Enable a schedule
6. ✅ Test file sensor
7. 📖 Read Dagster docs for advanced features

---

**Quick Reference**

```powershell
# Start Dagster
.\run.ps1 dagster-dev

# Run pipeline
.\run.ps1 dagster-materialize

# View in browser
http://localhost:3000
```

Happy orchestrating! 🚀
