param([int]$Port = 8000, [switch]$Live)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (Test-Path -LiteralPath '.env.local') {
    foreach ($line in Get-Content -LiteralPath '.env.local') {
        if ($line.Trim() -and -not $line.Trim().StartsWith('#')) {
            $pair = $line.Split('=', 2)
            if ($pair.Length -eq 2) {
                [Environment]::SetEnvironmentVariable($pair[0].Trim(), $pair[1].Trim(), 'Process')
            }
        }
    }
}
if ($Live) { $env:CHICAGO_DATA_ROOT = 'data' }
python -m uvicorn src.api.main:app --host 127.0.0.1 --port $Port
