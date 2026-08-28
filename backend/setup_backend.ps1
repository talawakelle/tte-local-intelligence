$ErrorActionPreference = "Stop"

$BackendRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Set-Location $BackendRoot

if (-not (Test-Path ".\.venv")) {
    py -3.12 -m venv .venv
}

.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host ""
Write-Host "Backend setup completed successfully." -ForegroundColor Green
Write-Host "Run: .\start_backend.ps1"
