param(
  [string]$TrackerPath = "PROJECT_TRACKER.md"
)

if (!(Test-Path $TrackerPath)) {
  Write-Error "Tracker file not found: $TrackerPath"
  exit 1
}

# Load tracker
$content = Get-Content -Raw $TrackerPath

function Set-Checkbox {
  param(
    [string]$label,   # text exactly as it appears after the checkbox in the MD
    [bool]$checked
  )
  $esc = [regex]::Escape($label)

  # two patterns: unchecked and checked
  $patternUnchecked = "(- \[\s\]\s)$esc"
  $patternChecked   = "(- \[x\]\s)$esc"

  if ($checked) {
    # turn unchecked -> checked
    $content = [regex]::Replace($content, $patternUnchecked, "`$1$label" -replace "\[\s\]","[x]")
  } else {
    # turn checked -> unchecked (rare, but keeps it accurate)
    $content = [regex]::Replace($content, $patternChecked, "`$1$label" -replace "\[x\]","[ ]")
  }
  return $content
}

# Predicates (what to consider "done")
$done = @{
  "Data/Processed/holy_grail_static_221_dataset.parquet"            = Test-Path "Data/Processed/holy_grail_static_221_dataset.parquet"
  "Data/Processed/holy_grail_static_221_windows_long.parquet"       = Test-Path "Data/Processed/holy_grail_static_221_windows_long.parquet"
  "Data/Processed/labeled_holy_grail_static_221_windows_long.parquet" = Test-Path "Data/Processed/labeled_holy_grail_static_221_windows_long.parquet"
  "Data/Processed/holy_grail_static_221_long_only.parquet"          = Test-Path "Data/Processed/holy_grail_static_221_long_only.parquet"

  # Inputs present
  "`Data/Filtered_OHLCV/` complete for current universe"            = Test-Path "Data/Filtered_OHLCV"
  "`Data/Raw/macro_regime_data.csv` (or built via `build_macro_regime_data.py`)" = Test-Path "Data/Raw/macro_regime_data.csv"

  # Per-bar indicators
  "Run `scripts/generate_indicators.py` as needed → confirm `per_bar_indicators_core.csv` matches current schema" = Test-Path "Data/Processed/per_bar_indicators_core.csv"

  # Final static breakouts (exists only; schema validation is a separate step)
  "`Data/Processed/static_breakouts.csv` (schema-validated)"        = Test-Path "Data/Processed/static_breakouts.csv"
}

# Map labels as they appear in PROJECT_TRACKER.md
$labels = @(
  # Status at a glance section
  "Data/Processed/holy_grail_static_221_dataset.parquet",
  "Data/Processed/holy_grail_static_221_windows_long.parquet",
  "Data/Processed/labeled_holy_grail_static_221_windows_long.parquet",
  "Data/Processed/holy_grail_static_221_long_only.parquet",
  "`Data/Processed/static_breakouts.csv` (schema-validated)",

  # Task checklist — Archive salvage & commit (same strings as above)
  "Add `holy_grail_static_221_dataset.parquet` (LFS)",
  "Add `holy_grail_static_221_windows_long.parquet` (LFS)",
  "Add `labeled_holy_grail_static_221_windows_long.parquet` (LFS)",
  "Add `holy_grail_static_221_long_only.parquet` (LFS)",

  # Inputs present
  "`Data/Filtered_OHLCV/` complete for current universe",
  "`Data/Raw/macro_regime_data.csv` (or built via `build_macro_regime_data.py`)",

  # Per‑bar indicators
  "Run `scripts/generate_indicators.py` as needed → confirm `per_bar_indicators_core.csv` matches current schema"
)

# Tie the “Add … (LFS)” checklist items to the underlying file existence
$aliasMap = @{
  "Add `holy_grail_static_221_dataset.parquet` (LFS)"                     = "Data/Processed/holy_grail_static_221_dataset.parquet"
  "Add `holy_grail_static_221_windows_long.parquet` (LFS)"                = "Data/Processed/holy_grail_static_221_windows_long.parquet"
  "Add `labeled_holy_grail_static_221_windows_long.parquet` (LFS)"        = "Data/Processed/labeled_holy_grail_static_221_windows_long.parquet"
  "Add `holy_grail_static_221_long_only.parquet` (LFS)"                   = "Data/Processed/holy_grail_static_221_long_only.parquet"
}

foreach ($label in $labels) {
  $key = if ($aliasMap.ContainsKey($label)) { $aliasMap[$label] } else { $label }
  $isDone = $false
  if ($done.ContainsKey($key)) { $isDone = [bool]$done[$key] }
  $content = Set-Checkbox -label $label -checked $isDone
}

# Save back
Set-Content -Path $TrackerPath -Value $content -Encoding UTF8

Write-Host "Updated $TrackerPath"
