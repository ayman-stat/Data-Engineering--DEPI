param(
    [switch]$StopDocker
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$LogsDir = Join-Path $ProjectRoot "logs"
$PidFile = Join-Path $LogsDir "spark_consumer.pid"

if (Test-Path $PidFile) {
    $sparkPid = Get-Content $PidFile -ErrorAction SilentlyContinue
    if ($sparkPid -and (Get-Process -Id $sparkPid -ErrorAction SilentlyContinue)) {
        Write-Host "Stopping Spark consumer PID $sparkPid..."
        Stop-Process -Id $sparkPid -Force
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "No Spark consumer PID file found."
}

if ($StopDocker) {
    Write-Host "Stopping Docker services..."
    docker compose -f (Join-Path $ProjectRoot "docker\docker-compose.yml") stop
}

Write-Host "Done."
