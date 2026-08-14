[CmdletBinding()]
param(
    [switch]$Volumes
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $projectRoot "compose.yaml"
$envFile = Join-Path $projectRoot ".env"
if (-not (Test-Path $envFile)) {
    throw ".env is missing. Nothing was changed."
}

$arguments = @(
    "compose",
    "--env-file",
    $envFile,
    "--file",
    $composeFile,
    "--profile",
    "dashboard",
    "--profile",
    "monitoring",
    "down",
    "--remove-orphans"
)
if ($Volumes) {
    $arguments += "--volumes"
}

& docker @arguments
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose teardown failed."
}

if ($Volumes) {
    Write-Host "Millrace containers and local volumes were removed."
} else {
    Write-Host "Millrace containers were removed; local volumes were retained."
}
