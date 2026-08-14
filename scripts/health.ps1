[CmdletBinding()]
param(
    [switch]$Dashboard,
    [switch]$Monitoring
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $projectRoot "compose.yaml"
$envFile = Join-Path $projectRoot ".env"
if (-not (Test-Path $envFile)) {
    throw ".env is missing. Run scripts/bootstrap.ps1 first."
}

$composeArguments = @("compose", "--env-file", $envFile, "--file", $composeFile)
$services = @(
    "postgres",
    "kafka",
    "connect",
    "spark-master",
    "spark-worker",
    "minio",
    "airflow-api-server",
    "airflow-scheduler",
    "airflow-dag-processor"
)
if ($Dashboard) {
    $services += "dashboard"
}
if ($Monitoring) {
    $services += @("prometheus", "pushgateway", "grafana")
}

$failures = [System.Collections.Generic.List[string]]::new()
foreach ($service in $services) {
    $containerIds = @(& docker @composeArguments ps --quiet $service)
    if ($LASTEXITCODE -ne 0 -or $containerIds.Count -eq 0) {
        $failures.Add("${service}: no container")
        continue
    }

    $stateJson = & docker inspect --format "{{json .State}}" $containerIds[0]
    if ($LASTEXITCODE -ne 0) {
        $failures.Add("${service}: inspection failed")
        continue
    }
    $state = $stateJson | ConvertFrom-Json
    $health = "not-configured"
    if ($state.PSObject.Properties.Name -contains "Health") {
        $health = $state.Health.Status
    }

    Write-Host ("{0,-24} state={1,-10} health={2}" -f $service, $state.Status, $health)
    if ($state.Status -ne "running" -or $health -notin @("healthy", "not-configured")) {
        $failures.Add("${service}: state=$($state.Status), health=$health")
    }
}

$connectorLine = Get-Content -LiteralPath $envFile |
    Where-Object { $_ -match "^DEBEZIUM_CONNECTOR_NAME=" } |
    Select-Object -First 1
if (-not $connectorLine) {
    throw "DEBEZIUM_CONNECTOR_NAME is missing from .env."
}
$connectorName = ($connectorLine -split "=", 2)[1]
$statusJson = & docker @composeArguments exec -T connect `
    curl --fail --silent "http://localhost:8083/connectors/$connectorName/status"
if ($LASTEXITCODE -ne 0) {
    $failures.Add("${connectorName}: connector status unavailable")
} else {
    $status = $statusJson | ConvertFrom-Json
    $taskStates = @($status.tasks | ForEach-Object { $_.state })
    if ($status.connector.state -ne "RUNNING" -or $taskStates -contains "FAILED") {
        $failures.Add("${connectorName}: connector or task is not running")
    }
    Write-Host ("{0,-24} connector={1} tasks={2}" -f
        $connectorName,
        $status.connector.state,
        ($taskStates -join ","))
}

if ($failures.Count -gt 0) {
    throw "Health check failed: $($failures -join '; ')"
}

Write-Host "Millrace services are healthy."
