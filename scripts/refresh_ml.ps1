$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
python (Join-Path $ProjectRoot "src\ml\forecast_daily_orders.py")
Write-Host "Refresh the Power BI forecast page after this script finishes."
