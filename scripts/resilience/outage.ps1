<#
.SYNOPSIS
  Prueba de resiliencia: detiene una dependencia durante la carga y verifica que no se pierde
  ningún evento aceptado (k6 scripts/load/e2e.js con reintentos de cliente).

.EXAMPLE
  ./scripts/resilience/outage.ps1 -Service nats -ApiKey $env:API_KEY
  ./scripts/resilience/outage.ps1 -Service postgres -DownSeconds 10 -Container mi-contenedor-pg

  -Service   nats | redis | postgres (nombre del servicio en docker-compose.yml)
  -Container Opcional: nombre de contenedor concreto si no usas Compose.
  -ProjectName Opcional: proyecto Compose (docker compose -p) si no es el del directorio actual.
  Requiere k6 y Docker en el PATH. Sale con el código de k6 (0 = no se perdió nada).
#>
param(
  [Parameter(Mandatory)][ValidateSet("nats", "redis", "postgres")][string]$Service,
  [string]$BaseUrl = "http://localhost:8080",
  [string]$ApiKey = $env:API_KEY,
  [int]$Total = 300,
  [int]$DownAfterSeconds = 3,
  [int]$DownSeconds = 8,
  [string]$Container,
  [string]$ProjectName
)
$ErrorActionPreference = "Stop"
$k6 = (Get-Command k6 -ErrorAction SilentlyContinue).Source
if (-not $k6) { throw "k6 no encontrado en el PATH" }
if (-not $Container) {
  $composeArgs = @()
  if ($ProjectName) { $composeArgs += @("-p", $ProjectName) }
  $Container = (docker compose @composeArgs ps -q $Service | Select-Object -First 1)
  if (-not $Container) { throw "No hay contenedor Compose para '$Service'. Usa -Container." }
}

$env:BASE_URL = $BaseUrl
$env:API_KEY = $ApiKey
$env:PROFILE = "smoke"
$env:TOTAL = "$Total"
$env:E2E_RETRY = "true"
$env:E2E_PACE = "0.25"
$env:E2E_TIMEOUT = "120"

$job = Start-Job -ArgumentList $Container, $DownAfterSeconds, $DownSeconds -ScriptBlock {
  param($c, $after, $down)
  Start-Sleep $after
  Write-Output "[outage] docker stop $c"
  docker stop $c | Out-Null
  Start-Sleep $down
  Write-Output "[outage] docker start $c"
  docker start $c | Out-Null
}
try {
  # k6 escribe warnings en stderr; en Windows PowerShell 5.1 con "Stop" abortarían la prueba.
  $ErrorActionPreference = "Continue"
  & $k6 run (Join-Path $PSScriptRoot "..\load\e2e.js") 2>&1 | ForEach-Object { "$_" }
  $code = $LASTEXITCODE
} finally {
  Receive-Job $job -Wait | Write-Host
  Remove-Job $job -Force
}
exit $code
