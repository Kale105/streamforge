[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
  # Managed Windows shells sometimes inherit an unset or non-writable GOCACHE.
  # Keep Go's build artifacts outside the repository and use a deterministic
  # per-user temp location only when the inherited cache cannot be used.
  $cacheUsable = $false
  if (-not [string]::IsNullOrWhiteSpace($env:GOCACHE)) {
    try {
      New-Item -ItemType Directory -Force -Path $env:GOCACHE | Out-Null
      $probe = Join-Path $env:GOCACHE '.streamforge-write-probe'
      [System.IO.File]::WriteAllText($probe, 'ok')
      Remove-Item -LiteralPath $probe -Force
      $cacheUsable = $true
    } catch {
      $cacheUsable = $false
    }
  }
  if (-not $cacheUsable) {
    $tempRoot = if ([string]::IsNullOrWhiteSpace($env:TEMP)) { [System.IO.Path]::GetTempPath() } else { $env:TEMP }
    $env:GOCACHE = Join-Path $tempRoot 'streamforge-go-build'
    New-Item -ItemType Directory -Force -Path $env:GOCACHE | Out-Null
    Write-Host "Using writable GOCACHE: $env:GOCACHE"
  }

  go test -p=1 ./internal/integration
  go vet ./internal/integration

  $docker = Get-Command docker -ErrorAction SilentlyContinue
  if ($null -eq $docker) {
    Write-Host 'SKIP docker compose config: docker is not installed'
  } else {
    docker compose -f compose.provider.yaml config
  }

  $helm = Get-Command helm -ErrorAction SilentlyContinue
  if ($null -eq $helm) {
    Write-Host 'SKIP helm lint/template: helm is not installed'
  } else {
    helm lint deploy/helm/streamforge
    helm template streamforge deploy/helm/streamforge | Out-Null
  }
} finally {
  Pop-Location
}
