param(
    [switch]$IncludeData
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$archive = Join-Path $root "medical-device-traceability-portable.zip"
$stage = Join-Path $root ".portable_stage"

if (Test-Path $stage) {
    Remove-Item -LiteralPath $stage -Recurse -Force
}
New-Item -ItemType Directory -Path $stage | Out-Null

$items = @(
    "app",
    "docs",
    "static",
    "tests",
    ".gitignore",
    "pyproject.toml",
    "README.md",
    "start.bat",
    "start.ps1",
    "uv.lock"
)

foreach ($item in $items) {
    $source = Join-Path $root $item
    if (Test-Path $source) {
        Copy-Item -LiteralPath $source -Destination $stage -Recurse -Force
    }
}

if ($IncludeData) {
    $dataSource = Join-Path $root "data\traceability.db"
    if (Test-Path $dataSource) {
        $dataTarget = Join-Path $stage "data"
        New-Item -ItemType Directory -Path $dataTarget -Force | Out-Null
        Copy-Item -LiteralPath $dataSource -Destination $dataTarget -Force
    }
}

Get-ChildItem -Path $stage -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Path $stage -Recurse -File -Include "*.pyc" | Remove-Item -Force

if (Test-Path $archive) {
    Remove-Item -LiteralPath $archive -Force
}

Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $archive -Force
Remove-Item -LiteralPath $stage -Recurse -Force

Write-Host "Created: $archive"
