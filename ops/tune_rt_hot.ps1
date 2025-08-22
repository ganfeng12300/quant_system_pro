param(
  [string]\D:\SQuant_Pro\ops\bin\nssm.exe = 'D:\SQuant_Pro\ops\bin\nssm.exe',
  [string]\D:\quant_system_pro (3)\quant_system_pro = 'D:\quant_system_pro (3)\quant_system_pro',
  [string]\D:\quant_system_v2\data\market_data.db   = 'D:\quant_system_v2\data\market_data.db',
  [string]\D:\SQuant_Pro\logs\service\SQuant-RT-Hot_stdout.log  = 'D:\SQuant_Pro\logs\service\SQuant-RT-Hot_stdout.log'
)

# 目标范围
\=2; \=12; \=2; \=1
\=30; \=90; \=15; \=5
\=600

function Get-Params() {
  \ = & \D:\SQuant_Pro\ops\bin\nssm.exe get 'SQuant-RT-Hot' AppParameters
  if(-not \){ return \ }
  \ = [int]([regex]::Match(\, '--max-workers\s+(\d+)').Groups[1].Value)
  \ = [int]([regex]::Match(\, '--interval\s+(\d+)').Groups[1].Value)
  return @{raw=\; workers=(if(\){\}else{6}); interval=(if(\){\}else{45})}
}

function Set-Params(\,\){
  \ = "tools\rt_updater_with_banner.py --db "\D:\quant_system_v2\data\market_data.db" --symbols-file "\D:\quant_system_pro (3)\quant_system_pro\results\keep_symbols_hot.txt" --backfill-days 365 --max-workers \ --interval \"
  & \D:\SQuant_Pro\ops\bin\nssm.exe set 'SQuant-RT-Hot' AppParameters \ | Out-Null
}

# 1) 统计最近日志中的锁错误
\=0
if(Test-Path \D:\SQuant_Pro\logs\service\SQuant-RT-Hot_stdout.log){
  \ = (Get-Content -Tail \ -Path \D:\SQuant_Pro\logs\service\SQuant-RT-Hot_stdout.log | Select-String -SimpleMatch 'database is locked').Count
}

# 2) 当前参数
\ = Get-Params
if(-not \){ exit 0 }
\=\.workers; \=\.interval
\=\; \=\

if(\ -ge 3){
  \ = [math]::Max(\, \ - \)
  \ = [math]::Min(\, \ + \)
} elseif(\ -eq 0) {
  \ = [math]::Min(\, \ + \)
  \ = [math]::Max(\, \ - \)
}

# 3) 如变化，应用并重启服务
if(\ -ne \ -or \ -ne \){
  Set-Params -mw \ -it \
  & \D:\SQuant_Pro\ops\bin\nssm.exe restart 'SQuant-RT-Hot' | Out-Null
  "$(Get-Date -f 'u') tuned: locked=\  workers \->\  interval \->\" | Add-Content (Join-Path (Split-Path \D:\SQuant_Pro\logs\service\SQuant-RT-Hot_stdout.log) 'tuner.log')
} else {
  "$(Get-Date -f 'u') steady: locked=\  workers=\  interval=\" | Add-Content (Join-Path (Split-Path \D:\SQuant_Pro\logs\service\SQuant-RT-Hot_stdout.log) 'tuner.log')
}
