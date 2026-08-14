[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $projectRoot "compose.yaml"
$envFile = Join-Path $projectRoot ".env"
if (-not (Test-Path $envFile)) {
    throw ".env is missing. Run scripts/bootstrap.ps1 first."
}

function Get-EnvValue {
    param([Parameter(Mandatory)][string]$Name)

    $line = Get-Content -LiteralPath $envFile |
        Where-Object { $_ -match "^$([regex]::Escape($Name))=" } |
        Select-Object -First 1
    if (-not $line) {
        throw "$Name is missing from .env."
    }
    return ($line -split "=", 2)[1]
}

& (Join-Path $PSScriptRoot "health.ps1")

$postgresUser = Get-EnvValue -Name "POSTGRES_USER"
$postgresDatabase = Get-EnvValue -Name "POSTGRES_DB"
$composeArguments = @("compose", "--env-file", $envFile, "--file", $composeFile)
$sql = @'
\set ON_ERROR_STOP on
SELECT batch_id, control.apply_demo_batch(batch_id) AS applied
FROM generate_series(1, 3) AS batches(batch_id);

DO $$
BEGIN
    IF (SELECT count(*) FROM retail.customers) <> 3
        OR (SELECT count(*) FROM retail.products) <> 4
        OR (SELECT count(*) FROM retail.orders) <> 3
        OR (SELECT count(*) FROM retail.order_items) <> 6
        OR (SELECT count(*) FROM control.source_batch WHERE state = 'completed') <> 3
    THEN
        RAISE EXCEPTION 'deterministic demo validation failed';
    END IF;
END;
$$;

SELECT batch_id, state, row_count, checksum
FROM control.source_batch
ORDER BY batch_id;

SELECT 'customers' AS entity, count(*) AS current_rows FROM retail.customers
UNION ALL SELECT 'products', count(*) FROM retail.products
UNION ALL SELECT 'orders', count(*) FROM retail.orders
UNION ALL SELECT 'order_items', count(*) FROM retail.order_items
ORDER BY entity;
'@

$sql | & docker @composeArguments exec -T postgres `
    psql --username $postgresUser --dbname $postgresDatabase
if ($LASTEXITCODE -ne 0) {
    throw "The deterministic demo batches failed."
}

Write-Host "Deterministic demo batches are loaded and validated."
