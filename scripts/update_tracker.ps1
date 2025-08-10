[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$tracker = "PROJECT_TRACKER.md"
if (-not (Test-Path $tracker)) { throw "Tracker not found: $tracker" }

# Read robustly (avoid $null); join with LF
$contentLines = Get-Content -LiteralPath $tracker -ErrorAction Stop
$content = ($contentLines -join "`n")
if ($null -eq $content) { $content = "" }

# Lines to tick
$labels = @(
  "Data/Processed/holy_grail_static_221_dataset.parquet",
  "Data/Processed/holy_grail_static_221_windows_long.parquet",
  "Data/Processed/labeled_holy_grail_static_221_windows_long.parquet",
  "Data/Processed/holy_grail_static_221_long_only.parquet",
  "Data/Processed/static_breakouts.csv"
)

foreach ($lab in $labels) {
  $esc = [regex]::Escape($lab)
  # change "- [ ] <label>" -> "- [x] <label>"
  $content = $content -replace "(?m)^\s*-\s*\[\s\]\s*($esc)", "- [x] `$1"
}

# Artifacts (hashes)
$csv = "Data/Processed/static_breakouts.csv"
if (Test-Path $csv) {
  $hash = (Get-FileHash $csv -Algorithm SHA256).Hash
  if ($content -notmatch "(?m)^\s*##\s*Artifacts\s*\(hashes\)") {
    $content += "`n## Artifacts (hashes)`n"
  }
  # Update existing line OR append
  if ($content -match "(?m)^\s*-\s*\[\w\]\s*static_breakouts\.csv — sha256: .+$") {
    $content = $content -replace "(?m)^\s*-\s*\[\w\]\s*static_breakouts\.csv — sha256: .+$",
      ("- [x] static_breakouts.csv — sha256: {0}" -f $hash)
  } else {
    $content += "- [x] static_breakouts.csv — sha256: $hash`n"
  }
}

Set-Content -Encoding UTF8 -LiteralPath $tracker -Value $content
Write-Host "Updated PROJECT_TRACKER.md"
