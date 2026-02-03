@echo off
REM Wisconsin Provider Medicaid - Windows Setup Script
REM Run this script to set up the project on Windows

echo ========================================
echo Wisconsin Provider Medicaid Setup
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/6] Creating directories...
if not exist "C:\data" mkdir C:\data
if not exist "C:\data\duckdb" mkdir C:\data\duckdb
if not exist "C:\data\wi_medicaid" mkdir C:\data\wi_medicaid
if not exist "%USERPROFILE%\.dbt" mkdir "%USERPROFILE%\.dbt"
echo     Directories created successfully

echo.
echo [2/6] Installing dbt and DuckDB adapter...
pip install --upgrade pip
pip install dbt-core dbt-duckdb
if %errorlevel% neq 0 (
    echo ERROR: Failed to install dbt packages
    pause
    exit /b 1
)
echo     dbt installed successfully

echo.
echo [3/6] Copying profile configuration...
if not exist "%USERPROFILE%\.dbt\profiles.yml" (
    copy profiles_duckdb_windows.yml "%USERPROFILE%\.dbt\profiles.yml"
    echo     Profile copied to %USERPROFILE%\.dbt\profiles.yml
) else (
    echo     Profile already exists at %USERPROFILE%\.dbt\profiles.yml
    echo     Please merge profiles_duckdb_windows.yml manually if needed
)

echo.
echo [4/6] Installing dbt packages...
call dbt deps
if %errorlevel% neq 0 (
    echo WARNING: Failed to install dbt packages
    echo This is optional, continuing...
)

echo.
echo [5/6] Testing dbt connection...
call dbt debug
if %errorlevel% neq 0 (
    echo WARNING: Connection test failed
    echo Please check your profiles.yml configuration
)

echo.
echo [6/6] Setup complete!
echo.
echo ========================================
echo Next Steps:
echo ========================================
echo 1. Place your source file at: C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv
echo 2. Review the configuration in: %USERPROFILE%\.dbt\profiles.yml
echo 3. Run: dbt run
echo 4. Test: dbt test
echo 5. View docs: dbt docs generate ^&^& dbt docs serve
echo.
echo For help, see: README_WINDOWS.md
echo ========================================
pause
