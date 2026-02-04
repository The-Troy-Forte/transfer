# Dagster Quick Start - 5 Minutes

## 🎯 What You Get

- **Web UI** at http://localhost:3000
- **Visual pipeline** with dependency graph
- **Automatic scheduling** (daily at 2 AM)
- **File sensors** (trigger on new file)
- **Monitoring & logs** built-in

## 🚀 Setup (3 Steps)

```powershell
# 1. Install Dagster
.\run.ps1 install-dagster

# 2. Start Dagster UI
.\run.ps1 dagster-dev

# 3. Open browser
# Navigate to: http://localhost:3000
```

## 🎮 Using Dagster

### Run Pipeline via UI
1. Open http://localhost:3000
2. Click **Assets** tab
3. Click **Materialize all**
4. Watch it run! 🎉

### Run Pipeline via Command
```powershell
.\run.ps1 dagster-materialize
```

## ⏰ Enable Daily Schedule

### Via UI (Recommended)
1. Go to **Overview** → **Schedules**
2. Find `daily_wi_medicaid_pipeline`
3. Click toggle to **ON**
4. Pipeline runs at 2 AM every day

### Via Command
```powershell
cd dagster_project
dagster schedule start daily_wi_medicaid_pipeline
```

## 📁 Enable File Sensor

### Via UI
1. Go to **Overview** → **Sensors**
2. Find `wi_medicaid_file_sensor`
3. Click toggle to **ON**
4. Monitors: `C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv`

### Via Command
```powershell
cd dagster_project
dagster sensor start wi_medicaid_file_sensor
```

## 🎨 What Dagster Shows You

### Asset Lineage Graph
```
WI_PROV_FILE_EXTRACT.psv
    ↓
stg_wi_ma_inbound_raw
    ↓
int_wi_ma_parsed_records
    ├→ stg_provider (Type 01)
    ├→ stg_provider_address (Type 02)
    ├→ stg_provider_tax (Type 03)
    └→ ... (12 more types)
```

### Run History
- See all past runs
- Success/failure status
- Execution time
- Logs and errors

### Asset Details
- Last materialized time
- Data freshness
- Test results
- Dependencies

## 🔄 Workflows

### Manual Run
```powershell
# Start UI
.\run.ps1 dagster-dev

# In browser: Click "Materialize all"
```

### Scheduled Run
```powershell
# Enable schedule (one-time)
cd dagster_project
dagster schedule start daily_wi_medicaid_pipeline

# Keep Dagster running
.\run.ps1 dagster-dev

# Runs automatically at 2 AM
```

### Event-Driven
```powershell
# Enable sensor (one-time)
cd dagster_project
dagster sensor start wi_medicaid_file_sensor

# Drop new file
Copy-Item "\\source\file.psv" "C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv"

# Pipeline triggers automatically
```

## 📊 Jobs Available

| Job | What It Does | Command |
|-----|--------------|---------|
| **wi_medicaid_full_pipeline** | Run everything | `.\run.ps1 dagster-materialize` |
| **wi_medicaid_inbound_job** | Inbound only | `.\run.ps1 dagster-inbound` |
| **wi_medicaid_outbound_job** | Outbound only | `.\run.ps1 dagster-outbound` |

## 🎓 Learning Path

### Day 1: Setup
```powershell
.\run.ps1 install-dagster
.\run.ps1 dagster-dev
```
Open http://localhost:3000 and explore!

### Day 2: Run Manually
1. Click **Assets**
2. Select a few assets
3. Click **Materialize selected**
4. Watch logs in real-time

### Day 3: Enable Schedule
1. Go to **Schedules**
2. Enable daily schedule
3. Let it run automatically

### Day 4: Enable Sensor
1. Go to **Sensors**
2. Enable file sensor
3. Drop a file and watch it trigger

## 🐛 Troubleshooting

### Can't access http://localhost:3000
```powershell
# Check if Dagster is running
Get-Process | Where-Object {$_.ProcessName -like "*dagster*"}

# Restart Dagster
.\run.ps1 dagster-dev
```

### Assets not showing
```powershell
# Reload definitions
# In UI: Click "Reload definitions" button
# Or restart: Ctrl+C and run again
```

### Schedule not running
1. Ensure schedule is **ON** in UI
2. Keep Dagster daemon running
3. Check logs for errors

## 📚 Full Documentation

- **Complete guide**: `DAGSTER_GUIDE.md`
- **Windows setup**: `README_WINDOWS.md`
- **Project docs**: `README.md`

## 💡 Tips

✅ **Keep Dagster running** for schedules to work
✅ **Use the UI** - it's easier than CLI
✅ **Check logs** when things fail
✅ **Enable sensors** for automation
✅ **View lineage** to understand dependencies

## 🎯 Quick Commands

```powershell
# Start Dagster
.\run.ps1 dagster-dev

# Run everything
.\run.ps1 dagster-materialize

# Run inbound only
.\run.ps1 dagster-inbound

# Run outbound only
.\run.ps1 dagster-outbound
```

## 🌟 Why Use Dagster?

| Feature | Without Dagster | With Dagster |
|---------|----------------|--------------|
| Run pipeline | Manual command | Click button in UI |
| See progress | Read logs | Visual progress bar |
| Dependencies | Remember order | Automatic graph |
| Scheduling | Windows Task Scheduler | Built-in schedules |
| Monitoring | Check files manually | Dashboard with history |
| Debugging | Read log files | Interactive logs in UI |

---

**Get Started Now**

```powershell
.\run.ps1 install-dagster
.\run.ps1 dagster-dev
```

Then open http://localhost:3000 🚀
