$ErrorActionPreference = "Stop"

$BackendRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Database = "G:\TTE_Local_Intelligence\data\tte_intelligence.duckdb"
$Python = Join-Path $BackendRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    throw "Backend environment not installed. Run setup_backend.ps1 first."
}

if (-not (Test-Path $Database)) {
    throw "TTE database was not found: $Database"
}

$env:TTE_DB_PATH = $Database

$env:TTE_DATA_MODE = "PRODUCTION"
$env:TTE_PRODUCTION_DATA_ROOT = "G:\TTE_Local_Intelligence\production_data"
$env:TTE_PRODUCTION_DB = "G:\TTE_Local_Intelligence\production_data\tte_production.duckdb"

Set-Location $BackendRoot

Write-Host "Starting TTE Local Intelligence API..." -ForegroundColor Green
Write-Host "API:  http://127.0.0.1:8080"
Write-Host "Docs: http://127.0.0.1:8080/docs"

& $Python -m uvicorn app.main:app `
    --host 127.0.0.1 `
    --port 8080

