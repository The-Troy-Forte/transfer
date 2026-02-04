# Dagster Orchestration for Wisconsin Provider Medicaid

This directory contains Dagster orchestration for the dbt project.

## Setup

```powershell
# Install Dagster and dbt integration
pip install dagster dagster-webserver dagster-dbt

# Or install all at once
pip install -r requirements.txt
```

## Running Dagster

### Option 1: Using run.ps1
```powershell
.\run.ps1 dagster-dev     # Start Dagster UI
.\run.ps1 dagster-materialize  # Run all assets
```

### Option 2: Using Dagster CLI
```powershell
# Start Dagster UI
dagster dev -f dagster_project/wi_prov_medicaid_pipeline.py

# Or use workspace file
dagster dev -w dagster_project/workspace.yaml
```

### Option 3: Using Dagit (Web UI)
```powershell
# Navigate to dagster_project folder
cd dagster_project

# Start Dagster
dagster dev

# Open browser to: http://localhost:3000
```

## Directory Structure

```
dagster_project/
├── __init__.py
├── workspace.yaml                      # Dagster workspace config
├── wi_prov_medicaid_pipeline.py       # Main pipeline definition
├── assets/
│   ├── __init__.py
│   ├── dbt_assets.py                  # dbt model assets
│   └── file_assets.py                 # File sensor assets
├── resources/
│   ├── __init__.py
│   └── dbt_resource.py                # dbt resource configuration
├── sensors/
│   ├── __init__.py
│   └── file_sensor.py                 # File arrival sensor
├── schedules/
│   ├── __init__.py
│   └── daily_schedule.py              # Daily schedule
└── ops/
    ├── __init__.py
    └── file_ops.py                    # File operations
```

## Features

- **Asset-based orchestration**: Each dbt model is a Dagster asset
- **Automatic lineage**: Dagster tracks dependencies between models
- **File sensors**: Trigger pipeline when new file arrives
- **Schedules**: Run daily at configured time
- **Monitoring**: Built-in UI for monitoring runs
- **Testing**: Validate data quality with Dagster checks
