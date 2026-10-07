# Thin wrapper so `.\scripts\dkv.ps1 up` works before anything is installed.
# Prefers the release build, falls back to debug, and finally to `cargo run`.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

if ($PSVersionTable.PSVersion.Major -lt 6) {
    # Windows Terminal handles ANSI fine; the old conhost may not, and the
    # output of this project is mostly colour.
    Write-Host "note: on legacy consoles colours may not render. Use Windows Terminal, or pass --no-color." -ForegroundColor DarkGray
}

$release = Join-Path $root "target\release\dkv.exe"
$debug = Join-Path $root "target\debug\dkv.exe"

if (Test-Path $release) {
    & $release @args
} elseif (Test-Path $debug) {
    & $debug @args
} else {
    Write-Host "building first..." -ForegroundColor DarkGray
    Push-Location $root
    try {
        & cargo run --quiet --bin dkv -- @args
    } finally {
        Pop-Location
    }
}
exit $LASTEXITCODE
