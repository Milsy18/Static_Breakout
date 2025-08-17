<# =========================
  TP% scan by regime (hold_days fixed) with progress
  Save as: C:\Users\milla\Static_Breakout\scripts\tp_scan_by_level.ps1
========================= #>

# --- CONFIG ---
$MergedCsv = "C:\Users\milla\Static_Breakout\holy_grail_static_dataset.csv"
$PerBarCsv = "C:\Users\milla\Static_Breakout\Data\Processed\per_bar_indicators_core.csv"
$HoldRec   = "C:\Users\milla\Static_Breakout\hold_days_recommendations_tp33.csv"
$OutDir    = "C:\Users\milla\Static_Breakout"
$Tracker   = "C:\Users\milla\Static_Breakout\docs\M18_v18_Build_Tracker.md"

# Outputs
$FullOut = Join-Path $OutDir "tp_scan_by_level_full.csv"
$BestOut = Join-Path $OutDir "tp_recommendations_by_level.csv"

# TP candidates: 10%–100% (5% steps) + 33%
$TpGrid = @(for ($x = 0.10; $x -le 1.00; $x += 0.05) { [math]::Round($x,2) })
$TpGrid += 0.33
$TpGrid = $TpGrid | Sort-Object -Unique

# Optional outlier control on TIMED exits (NOT TP exits)
$Winsorize = $false       # enable/disable winsorization of *timed* returns
$CapUp     = 5.0          # +500% cap on timed returns
$CapDn     = -0.80        # -80% floor on timed returns

# --- LOAD ---
if (!(Test-Path $MergedCsv)) { throw "Missing merged dataset: $MergedCsv" }
if (!(Test-Path $PerBarCsv)) { throw "Missing per-bar CSV: $PerBarCsv" }
if (!(Test-Path $HoldRec))   { throw "Missing hold-days recommendations CSV: $HoldRec" }

$rows = Import-Csv -LiteralPath $MergedCsv
$per  = Import-Csv -LiteralPath $PerBarCsv

$holdMap = @{}
(Import-Csv -LiteralPath $HoldRec) | ForEach-Object {
  $holdMap[[int]$_.market_level] = [int]$_.hold_days
}

# --- HELPERS ---
function AsDec([string]$x) {
  if ([string]::IsNullOrWhiteSpace($x)) { return $null }
  $v = [double]$x
  if ($v -gt 1.5) { return $v/100.0 } else { return $v }
}
function Median($arr) {
  $a = $arr | Sort-Object
  $n = $a.Count
  if ($n -eq 0) { return $null }
  if ($n % 2 -eq 1) { return $a[[int][math]::Floor($n/2)] }
  else { return ($a[$n/2-1] + $a[$n/2]) / 2.0 }
}
function Winsor([double]$r) {
  if (-not $Winsorize) { return $r }
  if ($r -gt $CapUp) { return $CapUp }
  if ($r -lt $CapDn) { return $CapDn }
  return $r
}
function End-Progress { param([int]$Id, [string]$Label="done")
  try { Write-Progress -Id $Id -Activity $Label -Completed } catch {}
}

# Close lookup: "SYM|YYYY-MM-DD" -> close
$closeLkp = @{}
foreach ($r in $per) {
  try {
    $d = [datetime]$r.date
    $k = "$($r.symbol)|$($d.ToString('yyyy-MM-dd'))"
    $closeLkp[$k] = [double]$r.close
  } catch { continue }
}

# --- SCAN with progress ---
$grid   = New-Object System.Collections.Generic.List[object]
$levels = ($rows | Select-Object -ExpandProperty market_level -Unique | Sort-Object)

$totalCombos = $levels.Count * $TpGrid.Count
$comboIdx = 0
$sw = [System.Diagnostics.Stopwatch]::StartNew()

foreach ($lvl in $levels) {
  $grp  = $rows | Where-Object { [int]$_.market_level -eq [int]$lvl }
  $hold = $(if ($holdMap.ContainsKey([int]$lvl)) { $holdMap[[int]$lvl] } else { 10 })
  $nEv  = $grp.Count

  foreach ($tp in $TpGrid) {
    $comboIdx++
    $rets = New-Object System.Collections.Generic.List[double]
    $tpCount = 0; $timCount = 0; $valid = 0

    $evIdx = 0
    foreach ($ev in $grp) {
      $evIdx++

      if ($evIdx % 250 -eq 0) {
        $pctOuter = [int](100 * $comboIdx / $totalCombos)
        $eta = ""
        if ($comboIdx -gt 0) {
          $avgPerCombo = $sw.Elapsed.TotalSeconds / $comboIdx
          $remain = ($totalCombos - $comboIdx) * $avgPerCombo
          $eta = " | ETA ~ {0:N0}s" -f $remain
        }
        Write-Progress -Id 1 -Activity "TP% scan (regime x TP grid)" -Status "Level $lvl, TP=$tp  ($comboIdx/$totalCombos)$eta" -PercentComplete $pctOuter
        Write-Progress -Id 2 -ParentId 1 -Activity "Events" -Status "$evIdx / $nEv" -PercentComplete ([int](100 * $evIdx / [math]::Max(1,$nEv)))
      }

      try {
        $sym = $ev.symbol
        $bd  = [datetime]$ev.breakout_date
        $sp  = [double]$ev.start_price
        if (-not $sp -or $sp -le 0) { continue }

        $mg  = AsDec $ev.max_gain
        $dur = 0; if ($ev.PSObject.Properties.Name -contains 'duration_days') { $dur = [int]$ev.duration_days }

        # TP within holding window
        if ($mg -ne $null -and $mg -ge [double]$tp -and $dur -le $hold) {
          $rets.Add([double]$tp) | Out-Null
          $tpCount++; $valid++
          continue
        }

        # Timed exit
        $t = $bd.AddDays($hold)
        $k = "$sym|$($t.ToString('yyyy-MM-dd'))"
        if ($closeLkp.ContainsKey($k)) {
          $cd = $closeLkp[$k]
          $ret = ($cd / $sp) - 1.0
          $rets.Add( (Winsor([double]$ret)) ) | Out-Null
          $timCount++; $valid++
        }
      } catch { continue }
    } # events

    $avg=$null; $med=$null; $pos=$null
    if ($valid -gt 0) {
      $avg = ($rets | Measure-Object -Average).Average
      $med = Median $rets
      $pos = (($rets | Where-Object { $_ -gt 0 }).Count) / $valid
    }

    $grid.Add([pscustomobject]@{
      market_level = [int]$lvl
      hold_days    = [int]$hold
      tp_pct       = [double]$tp
      n_valid      = [int]$valid
      tp_count     = [int]$tpCount
      timed_count  = [int]$timCount
      avg_ret      = $(if ($avg -ne $null){ [math]::Round($avg,6) } else { $null })
      median_ret   = $(if ($med -ne $null){ [math]::Round($med,6) } else { $null })
      pct_pos      = $(if ($pos -ne $null){ [math]::Round($pos,4) } else { $null })
    }) | Out-Null
  } # tp grid
} # levels

End-Progress -Id 1 -Label "TP% scan"
End-Progress -Id 2 -Label "Events"

# --- SAVE FULL GRID ---
$grid | Sort-Object market_level, tp_pct | Export-Csv -NoTypeInformation -LiteralPath $FullOut
Write-Host "Saved full TP scan -> $FullOut"

# --- PICK BEST (avg_ret, then pct_pos, then n_valid) ---
$best = $grid |
  Group-Object market_level | ForEach-Object {
    $_.Group |
    Where-Object { $_.avg_ret -ne $null } |
    Sort-Object @{e='avg_ret';Descending=$true}, @{e='pct_pos';Descending=$true}, @{e='n_valid';Descending=$true} |
    Select-Object -First 1
  }

$best | Sort-Object market_level | Export-Csv -NoTypeInformation -LiteralPath $BestOut

# --- CONSOLE table ---
$best | Sort-Object market_level | Format-Table -AutoSize
Write-Host "Saved recommendations -> $BestOut"

# --- TRACKER update (replace-or-append) ---
New-Item -ItemType Directory -Path (Split-Path $Tracker -Parent) -Force | Out-Null
$tblRows = $best | Sort-Object market_level | ForEach-Object {
  "| $($_.market_level) | $($_.hold_days) | $($_.tp_pct) | $($_.n_valid) | $($_.tp_count) | $($_.timed_count) | $($_.avg_ret) | $($_.median_ret) | $($_.pct_pos) |"
}
$stamp  = (Get-Date).ToString("yyyy-MM-dd HH:mm")
$header = "| market_level | hold_days | tp_pct | n_valid | tp_count | timed_count | avg_ret | median_ret | pct_pos |`n|---|---:|---:|---:|---:|---:|---:|---:|---:|"
$body   = ($tblRows -join "`n")
$section = "`r`n## TP% recommendations (hold_days fixed) — appended $stamp`r`n$header`r`n$body`r`n"

$md = if (Test-Path $Tracker) { Get-Content -LiteralPath $Tracker -Raw } else { "# M18 Entry/Exit Model v18.0 — Build Tracker`r`n" }
$pattern = '(?ms)^\#\# TP% recommendations \(hold_days fixed\).*?(?=^\#\# |\Z)'

if ([regex]::IsMatch($md, $pattern)) {
  $m = [regex]::Match($md, $pattern)
  $newMd = $md.Substring(0, $m.Index) + $section + $md.Substring($m.Index + $m.Length)
} else {
  $newMd = $md + $section
}
Set-Content -LiteralPath $Tracker -Value $newMd -Encoding UTF8
Write-Host "Tracker section updated:" $Tracker
