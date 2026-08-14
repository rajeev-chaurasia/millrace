[CmdletBinding()]
param(
    [switch]$Dashboard,
    [switch]$Monitoring,
    [ValidateRange(30, 3600)]
    [int]$TimeoutSeconds = 1200
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $projectRoot "compose.yaml"
$envFile = Join-Path $projectRoot ".env"
$envExample = Join-Path $projectRoot ".env.example"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker is not available on PATH."
}

& docker compose version | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose v2 is required."
}

if (-not (Test-Path $envFile)) {
    Copy-Item -LiteralPath $envExample -Destination $envFile
    Write-Host "Created .env from local-only example values."
} else {
    $configuredNames = [System.Collections.Generic.HashSet[string]]::new(
        [StringComparer]::Ordinal
    )
    Get-Content -LiteralPath $envFile | ForEach-Object {
        if ($_ -match "^([A-Za-z_][A-Za-z0-9_]*)=") {
            [void]$configuredNames.Add($Matches[1])
        }
    }
    $missingDefaults = @(
        Get-Content -LiteralPath $envExample | Where-Object {
            $_ -match "^([A-Za-z_][A-Za-z0-9_]*)=" -and
            -not $configuredNames.Contains($Matches[1])
        }
    )
    if ($missingDefaults.Count -gt 0) {
        Add-Content -LiteralPath $envFile -Value $missingDefaults
        Write-Host "Added missing local-only defaults to .env."
    }
}

$composeArguments = @("compose", "--env-file", $envFile, "--file", $composeFile)
if ($Dashboard) {
    $composeArguments += @("--profile", "dashboard")
}
if ($Monitoring) {
    $composeArguments += @("--profile", "monitoring")
}
$startArguments = $composeArguments + @(
    "up",
    "--detach",
    "--build"
)

& docker @startArguments
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose failed to start the Millrace stack."
}

$waitServices = @(
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
$healthArguments = @()
if ($Dashboard) {
    $waitServices += "dashboard"
    $healthArguments += "-Dashboard"
}
if ($Monitoring) {
    $waitServices += @("prometheus", "pushgateway", "grafana")
    $healthArguments += "-Monitoring"
}

$waitArguments = $composeArguments + @(
    "up",
    "--detach",
    "--no-deps",
    "--wait",
    "--wait-timeout",
    $TimeoutSeconds.ToString()
) + $waitServices
& docker @waitArguments
if ($LASTEXITCODE -ne 0) {
    throw "The Millrace stack did not become healthy within $TimeoutSeconds seconds."
}

& (Join-Path $PSScriptRoot "health.ps1") @healthArguments
