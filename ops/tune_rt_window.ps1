param()
$CONF = "D:\quant_system_pro (3)\quant_system_pro\ops\rt_window_conf.json"
$LOG  = "D:\SQuant_Pro\logs\service\rt_window_stdout.log"

if(-not (Test-Path $CONF)){ '{"max_workers":6,"interval":45}' | Set-Content -Encoding ASCII $CONF }
$cfg = Get-Content $CONF | ConvertFrom-Json
$mw = [int]$cfg.max_workers; if(-not $mw){ $mw=6 }
$it = [int]$cfg.interval;    if(-not $it){ $it=45 }

$locked = 0
if(Test-Path $LOG){
  $locked = (Get-Content -Tail 600 -Path $LOG | Select-String -SimpleMatch 'database is locked').Count
}

$minW=2; $maxW=12; $decStep=2; $incStep=1
$minI=30; $maxI=90; $incI=15; $decI=5

$newW=$mw; $newI=$it
if($locked -ge 3){
  $newW = [math]::Max($minW, $mw - $decStep)
  $newI = [math]::Min($maxI, $it + $incI)
}elseif($locked -eq 0){
  $newW = [math]::Min($maxW, $mw + $incStep)
  $newI = [math]::Max($minI, $it - $decI)
}

if($newW -ne $mw -or $newI -ne $it){
  @{max_workers=$newW; interval=$newI} | ConvertTo-Json -Compress | Set-Content -Encoding ASCII $CONF

  Get-CimInstance Win32_Process |
    Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like "*rt_updater_with_banner.py*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

  "$(Get-Date -f u) tuned: locked=$locked  workers $mw->$newW  interval $it->$newI" |
    Add-Content (Join-Path (Split-Path $LOG) 'tuner.log')
}else{
  "$(Get-Date -f u) steady: locked=$locked  workers=$mw interval=$it" |
    Add-Content (Join-Path (Split-Path $LOG) 'tuner.log')
}
