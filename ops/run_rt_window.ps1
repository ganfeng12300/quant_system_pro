param()
$ErrorActionPreference='Stop'
$OutputEncoding = [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$env:PYTHONIOENCODING='utf-8'; chcp 65001 | Out-Null

$ROOT = 'D:\quant_system_pro (3)\quant_system_pro'
$DB   = 'D:\quant_system_v2\data\market_data.db'
$PY   = 'C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python.exe'
$CONF = Join-Path $ROOT 'ops\rt_window_conf.json'
$LOGDIR = 'D:\SQuant_Pro\logs\service'
$SYMS = Join-Path $ROOT 'results\keep_symbols_hot.txt'
$LOG  = Join-Path $LOGDIR 'rt_window_stdout.log'

function Get-RTProc {
  $pat = "*rt_updater_with_banner.py* --db $DB*"
  Get-CimInstance Win32_Process |
    Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like $pat }
}
if (Get-RTProc) { Write-Host '[RUNNER] already running.'; exit }

while ($true) {
  if (-not (Test-Path $CONF)) { '{"max_workers":6,"interval":45}' | Set-Content -Encoding ASCII $CONF }
  $cfg = Get-Content $CONF | ConvertFrom-Json
  $mw = [int]$cfg.max_workers; if(-not $mw){ $mw = 6 }
  $it = [int]$cfg.interval;    if(-not $it){ $it = 45 }

  Set-Location $ROOT
  Write-Host ('=' * 70)
  Write-Host ("[RUNNER] args: max_workers={0}  interval={1}s  {2}" -f $mw, $it, (Get-Date -Format 'u'))
  Write-Host ('=' * 70)

  $args = @(
    "tools\rt_updater_with_banner.py",
    "--db", $DB, "--symbols-file", $SYMS,
    "--backfill-days", "365",
    "--max-workers", $mw,
    "--interval", $it
  )
  & $PY @args 2>&1 | Tee-Object -FilePath $LOG -Append
  Write-Host ("[RUNNER] exited. relaunch in 2s... {0}" -f (Get-Date -Format 'u'))
  Start-Sleep 2
}
