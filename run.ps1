# Wisconsin Provider Medicaid - PowerShell Commands
# Windows alternative to Makefile
# Usage: .\run.ps1 <command>
# Example: .\run.ps1 run
# Or: .\run.ps1 help

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

function Show-Help {
    Write-Host "Wisconsin Provider Medicaid - Available Commands" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Setup & Installation:" -ForegroundColor Yellow
    Write-Host "  setup          - Initial project setup"
    Write-Host "  install        - Install dbt and dependencies"
    Write-Host "  deps           - Install dbt package dependencies"
    Write-Host "  debug          - Debug dbt connection"
    Write-Host ""
    Write-Host "Running Models:" -ForegroundColor Yellow
    Write-Host "  run            - Run all dbt models"
    Write-Host "  run-inbound    - Run only inbound staging models"
    Write-Host "  run-outbound   - Run outbound models"
    Write-Host "  compile        - Compile models without running"
    Write-Host ""
    Write-Host "Testing:" -ForegroundColor Yellow
    Write-Host "  test           - Run all dbt tests"
    Write-Host "  test-staging   - Test staging models"
    Write-Host "  test-outbound  - Test outbound models"
    Write-Host ""
    Write-Host "Documentation:" -ForegroundColor Yellow
    Write-Host "  docs           - Generate and serve documentation"
    Write-Host "  docs-generate  - Generate documentation only"
    Write-Host ""
    Write-Host "Specific Tasks:" -ForegroundColor Yellow
    Write-Host "  load-raw       - Load raw inbound file"
    Write-Host "  parse-records  - Parse records by type"
    Write-Host "  full-pipeline  - Run complete pipeline"
    Write-Host ""
    Write-Host "Maintenance:" -ForegroundColor Yellow
    Write-Host "  clean          - Clean dbt artifacts"
    Write-Host "  fresh          - Check source freshness"
    Write-Host ""
}

function Run-Command {
    param([string]$Cmd)
    
    switch ($Cmd) {
        "help" { Show-Help }
        
        # Setup
        "setup" { 
            Write-Host "Running setup..." -ForegroundColor Green
            .\setup_windows.bat
        }
        "install" {
            Write-Host "Installing dbt and DuckDB..." -ForegroundColor Green
            pip install --upgrade pip
            pip install dbt-core dbt-duckdb
        }
        "deps" {
            Write-Host "Installing dbt packages..." -ForegroundColor Green
            dbt deps
        }
        "debug" {
            Write-Host "Testing dbt connection..." -ForegroundColor Green
            dbt debug
        }
        
        # Clean
        "clean" {
            Write-Host "Cleaning dbt artifacts..." -ForegroundColor Green
            Remove-Item -Recurse -Force target -ErrorAction SilentlyContinue
            Remove-Item -Recurse -Force dbt_packages -ErrorAction SilentlyContinue
            Remove-Item -Recurse -Force logs -ErrorAction SilentlyContinue
            Write-Host "Cleaned successfully" -ForegroundColor Green
        }
        
        # Compile
        "compile" {
            Write-Host "Compiling dbt models..." -ForegroundColor Green
            dbt compile
        }
        
        # Run
        "run" {
            Write-Host "Running all dbt models..." -ForegroundColor Green
            dbt run
        }
        "run-inbound" {
            Write-Host "Running inbound models..." -ForegroundColor Green
            dbt run --select tag:inbound
        }
        "run-staging" {
            Write-Host "Running staging models..." -ForegroundColor Green
            dbt run --select tag:staging
        }
        "run-intermediate" {
            Write-Host "Running intermediate models..." -ForegroundColor Green
            dbt run --select tag:intermediate
        }
        "run-outbound" {
            Write-Host "Running outbound models..." -ForegroundColor Green
            dbt run --select tag:outbound
        }
        
        # Test
        "test" {
            Write-Host "Running all tests..." -ForegroundColor Green
            dbt test
        }
        "test-source" {
            Write-Host "Testing sources..." -ForegroundColor Green
            dbt test --select source:*
        }
        "test-staging" {
            Write-Host "Testing staging models..." -ForegroundColor Green
            dbt test --select tag:staging
        }
        "test-outbound" {
            Write-Host "Testing outbound models..." -ForegroundColor Green
            dbt test --select tag:outbound
        }
        
        # Documentation
        "docs" {
            Write-Host "Generating and serving documentation..." -ForegroundColor Green
            dbt docs generate
            dbt docs serve
        }
        "docs-generate" {
            Write-Host "Generating documentation..." -ForegroundColor Green
            dbt docs generate
        }
        
        # Specific workflows
        "load-raw" {
            Write-Host "Loading raw inbound file..." -ForegroundColor Green
            dbt run --select stg_wi_ma_inbound_raw
        }
        "parse-records" {
            Write-Host "Parsing records..." -ForegroundColor Green
            dbt run --select int_wi_ma_parsed_records
        }
        "load-types" {
            Write-Host "Loading type-specific tables..." -ForegroundColor Green
            dbt run --select staging.by_type.*
        }
        "full-inbound" {
            Write-Host "Running complete inbound flow..." -ForegroundColor Green
            dbt run --select tag:inbound
        }
        "full-outbound" {
            Write-Host "Running complete outbound flow..." -ForegroundColor Green
            dbt run --select tag:outbound
        }
        "full-pipeline" {
            Write-Host "Running complete pipeline..." -ForegroundColor Green
            dbt run
        }
        
        # Helpers
        "list-models" {
            Write-Host "Listing all models..." -ForegroundColor Green
            dbt list --resource-type model
        }
        "list-sources" {
            Write-Host "Listing all sources..." -ForegroundColor Green
            dbt list --resource-type source
        }
        "fresh" {
            Write-Host "Checking source freshness..." -ForegroundColor Green
            dbt source freshness
        }
        
        default {
            Write-Host "Unknown command: $Cmd" -ForegroundColor Red
            Write-Host "Run '.\run.ps1 help' for available commands" -ForegroundColor Yellow
        }
    }
}

# Main execution
Run-Command -Cmd $Command
