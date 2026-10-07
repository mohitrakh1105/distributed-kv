# Opens the two panes you actually want side by side: the cluster's logs, and
# the live dashboard. Everything else is already one short subcommand, so this
# is the only wrapper worth having.
#
# Requires Windows Terminal (wt). Without it, run `dkv up` in one terminal and
# `dkv watch` in another.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dkv = Join-Path $root "target\release\dkv.exe"
if (-not (Test-Path $dkv)) { $dkv = Join-Path $root "target\debug\dkv.exe" }
if (-not (Test-Path $dkv)) {
    Write-Host "build first: cargo build" -ForegroundColor Red
    exit 1
}

if (-not (Get-Command wt -ErrorAction SilentlyContinue)) {
    Write-Host "Windows Terminal (wt) not found." -ForegroundColor Yellow
    Write-Host "Run 'dkv up' in one terminal and 'dkv watch' in another."
    exit 1
}

# Split horizontally: cluster logs on top, dashboard below.
wt -w 0 nt --title "dkv cluster" $dkv up `; sp -H --title "dkv watch" $dkv watch
