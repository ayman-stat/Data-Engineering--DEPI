param(
    [int]$Limit = 100,
    [double]$DelaySeconds = 0.25,
    [int]$StartRow = 0,
    [int]$ConsumerWarmupSeconds = 45,
    [int]$PostProduceWaitSeconds = 25,
    [switch]$CleanRun,
    [switch]$SkipML
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$LogsDir = Join-Path $ProjectRoot "logs"
$PidFile = Join-Path $LogsDir "spark_consumer.pid"
$ConsumerOut = Join-Path $LogsDir "spark_consumer.out.log"
$ConsumerErr = Join-Path $LogsDir "spark_consumer.err.log"

function Remove-GeneratedPath {
    param([string]$RelativePath)

    $target = Join-Path $ProjectRoot $RelativePath
    if (-not (Test-Path $target)) {
        return
    }

    $resolvedProject = [System.IO.Path]::GetFullPath($ProjectRoot)
    $resolvedTarget = [System.IO.Path]::GetFullPath($target)
    if (-not $resolvedTarget.StartsWith($resolvedProject)) {
        throw "Refusing to remove path outside project: $resolvedTarget"
    }

    Remove-Item -LiteralPath $resolvedTarget -Recurse -Force
}

New-Item -ItemType Directory -Force $LogsDir | Out-Null

Write-Host "Starting Kafka and MongoDB..."
docker compose -f (Join-Path $ProjectRoot "docker\docker-compose.yml") up -d

$ordersCsv = Join-Path $ProjectRoot "data\olist_orders_dataset.csv"
if (-not (Test-Path $ordersCsv)) {
    Write-Host "Dataset not found. Downloading Olist data..."
    python (Join-Path $ProjectRoot "src\producer\ingest_data.py")
}

if ($CleanRun) {
    Write-Host "Cleaning generated lake/checkpoint/output folders for a fresh demo run..."
    Remove-GeneratedPath "data_lake\bronze\live_orders"
    Remove-GeneratedPath "data_lake\silver\live_orders"
    Remove-GeneratedPath "data_lake\gold\orders_per_minute"
    Remove-GeneratedPath "data_lake\gold\order_status_counts"
    Remove-GeneratedPath "data_lake\gold\revenue_by_state"
    Remove-GeneratedPath "data_lake\gold\top_products"
    Remove-GeneratedPath "checkpoints\bronze_live_orders"
    Remove-GeneratedPath "checkpoints\silver_live_orders"
    Remove-GeneratedPath "checkpoints\gold_orders_per_minute"
    Remove-GeneratedPath "checkpoints\gold_order_status_counts"
    Remove-GeneratedPath "checkpoints\gold_revenue_by_state"
    Remove-GeneratedPath "checkpoints\gold_top_products"
    Remove-GeneratedPath "checkpoints\mongo_live_orders"
    Remove-Item -LiteralPath (Join-Path $ProjectRoot "ml_outputs\daily_orders_forecast.csv") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $ProjectRoot "ml_outputs\daily_orders_metrics.json") -Force -ErrorAction SilentlyContinue
}

$existingPid = $null
if (Test-Path $PidFile) {
    $existingPid = Get-Content $PidFile -ErrorAction SilentlyContinue
}

if ($existingPid -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
    Write-Host "Spark consumer already appears to be running as PID $existingPid."
} else {
    Remove-Item $ConsumerOut, $ConsumerErr -Force -ErrorAction SilentlyContinue
    Write-Host "Starting Spark consumer..."
    $consumer = Start-Process `
        -FilePath "python" `
        -ArgumentList @("src\consumer\spark_consumer.py") `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $ConsumerOut `
        -RedirectStandardError $ConsumerErr `
        -PassThru `
        -WindowStyle Hidden
    $consumer.Id | Set-Content $PidFile
    Write-Host "Spark consumer PID: $($consumer.Id)"
}

Write-Host "Waiting $ConsumerWarmupSeconds seconds for Spark to initialize..."
Start-Sleep -Seconds $ConsumerWarmupSeconds

Write-Host "Producing $Limit live orders to Kafka..."
python (Join-Path $ProjectRoot "src\producer\live_producer.py") --limit $Limit --delay-seconds $DelaySeconds --start-row $StartRow

Write-Host "Waiting $PostProduceWaitSeconds seconds for Spark micro-batches to finish..."
Start-Sleep -Seconds $PostProduceWaitSeconds

if (-not $SkipML) {
    Write-Host "Refreshing ML forecast outputs..."
    python (Join-Path $ProjectRoot "src\ml\forecast_daily_orders.py")
}

Write-Host ""
Write-Host "Demo pipeline run finished."
Write-Host "Refresh Power BI now: Home -> Refresh."
Write-Host "Useful folders:"
Write-Host "  data_lake\gold\orders_per_minute"
Write-Host "  data_lake\gold\order_status_counts"
Write-Host "  data_lake\gold\revenue_by_state"
Write-Host "  data_lake\gold\top_products"
Write-Host "  ml_outputs\daily_orders_forecast.csv"
Write-Host ""
Write-Host "Consumer logs:"
Write-Host "  $ConsumerOut"
Write-Host "  $ConsumerErr"
Write-Host ""
Write-Host "To stop the Spark consumer:"
Write-Host "  .\scripts\stop_demo_pipeline.ps1"
