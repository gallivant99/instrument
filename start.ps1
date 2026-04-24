$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot
$env:UV_CACHE_DIR = ".uv-cache"

Write-Host "Starting medical device traceability system..."
Write-Host "Open http://127.0.0.1:8000 after the server starts."
uv run python -m app.server
