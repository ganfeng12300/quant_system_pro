# tools\HealthCheck-All-SGrade.ps1
# 全系统只读体检（不中断、报全错、出双报告）—— S级机构版
# 覆盖：环境 → 库 → 采集 → 回测 → 策略 → 选优产出供实盘 → 实盘 → 日志

param(
  [string]$DB = "D:\quant_system_v2\data\market_data.db",
  [string]$SymbolsFile = "results\keep_symbols.txt",
  [string[]]$TFs = @("5m","15m","30m","1h","2h","4h","1d"),
  [int]$FreshThresholdMin = 5,     # 数据库最新K线距当前 UTC 的分钟阈值
  [int]$Days = 365,                # 目标覆盖天数（用于报告提示）
  [int]$LogScanMins = 60,          # 日志扫描最近分钟
  [int]$BestParamsFreshDays = 7    # 选优产出（best_params/JSON）的新鲜度阈值（天）
)

$ErrorActionPreference = "Continue"
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
[Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8

# 输出
$ReportDir = "results"
[IO.Directory]::CreateDirectory($ReportDir) | Out-Null
$ReportFile  = Join-Path $ReportDir ("syscheck_{0:yyyyMMdd_HHmm}.md" -f (Get-Date))
$SummaryJson = Join-Path $ReportDir ("syscheck_summary.json")

# 内部状态
$AllFindings = @()
$Stopwatch = [Diagnostics.Stopwatch]::StartNew()

function Add-Finding([string]$Level,[string]$Section,[string]$Msg){
  $rec = [PSCustomObject]@{ Level=$Level; Section=$Section; Message=$Msg }
  $script:AllFindings += $rec
  $color = switch ($Level) { 'ERROR' {'Red'} 'WARN' {'Yellow'} 'INFO' {'Gray'} 'OK' {'Green'} default {'White'} }
  Write-Host ("[{0}] {1}: {2}" -f $Level,$Section,$Msg) -ForegroundColor $color
}

function New-Section([string]$Title){
  Write-Host ""
  Write-Host ("=== {0} ===" -f $Title) -ForegroundColor Cyan
}

function Get-PythonPath {
  $py = (Get-Command python -ErrorAction SilentlyContinue).Path
  if(!$py){ $py = (Get-Command python3 -ErrorAction SilentlyContinue).Path }
  return $py
}

function Invoke-Python([string]$code, [string[]]$args=@()){
  $py = Get-PythonPath
  if(!$py){ Add-Finding "ERROR" "Env" "未找到 Python 解释器"; return $null }
  try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $py
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false
    $psi.Arguments = @("- <<'PYEOF'") + $args -join ' '
    # 为了兼容，这里不走 -c 多段，改为写入临时文件执行
    $tmp = New-TemporaryFile
    Set-Content -Path $tmp -Value $code -Encoding UTF8
    $psi.Arguments = "`"$tmp`""
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi
    $null = $p.Start()
    $out = $p.StandardOutput.ReadToEnd()
    $err = $p.StandardError.ReadToEnd()
    $p.WaitForExit()
    Remove-Item $tmp -ErrorAction SilentlyContinue
    if($err){ Add-Finding "WARN" "Python" ("stderr: " + ($err.Trim() -replace "`r","" -replace "`n"," | ")) }
    return $out
  } catch {
    Add-Finding "ERROR" "Python" "调用失败：$_"
    return $null
  }
}

function Invoke-SqliteJson([string]$DbPath,[string]$Sql){
  # 用 python sqlite3 执行 SQL，并把结果转 JSON 行
  $code = @"
import sys, json, sqlite3, os, time, datetime as dt
db = r'''$DbPath'''
sql = r'''$Sql'''
out = []
try:
    con = sqlite3.connect(db, timeout=5)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(sql).fetchall()
    for r in rows:
        out.append({k:r[k] for k in r.keys()})
    print(json.dumps(out, ensure_ascii=False))
except Exception as e:
    print(json.dumps({"__error__": str(e)}))
"@
  $ret = Invoke-Python $code
  try { return $ret | ConvertFrom-Json } catch { return @(@{__error__="json_parse"; raw=$ret}) }
}

function Get-Symbols {
  if(Test-Path $SymbolsFile){
    $syms = Get-Content $SymbolsFile | Where-Object { $_ -and -not $_.StartsWith("#") } | ForEach-Object { $_.Trim().ToUpper() }
    $syms = $syms | Where-Object { $_ -ne "" } | Select-Object -Unique
    if($syms.Count -gt 0){ return $syms }
  }
  # 若无 symbols 文件，则从表名推导
  $rows = Invoke-SqliteJson $DB "SELECT name FROM sqlite_master WHERE type='table';"
  if($rows -and -not $rows.__error__){
    $set = New-Object System.Collections.Generic.HashSet[string]
    foreach($r in $rows){
      $n = $r.name
      if($n -match '^([A-Z0-9]+)_(\d+[mh]|1d)$'){ $null = $set.Add($matches[1].ToUpper()) }
    }
    return $set.ToArray() | Sort-Object
  }
  return @()
}

# ---------------- 1) 环境检查 ----------------
New-Section "环境检查"
$py = Get-PythonPath
if($py){ Add-Finding "OK" "Env" "Python=$(& $py --version 2>&1).Trim()" } else { Add-Finding "ERROR" "Env" "未找到Python" }
try {
  $mem = Get-CimInstance Win32_OperatingSystem
  Add-Finding "INFO" "Env" ("内存可用={0} GB / 总计={1} GB" -f ([math]::Round($mem.FreePhysicalMemory/1MB,1)), [math]::Round($mem.TotalVisibleMemorySize/1MB,1))
} catch {}
try {
  $sys = Get-CimInstance Win32_ComputerSystem
  Add-Finding "INFO" "Env" ("CPU={0}  物理内核≈{1}" -f $sys.Model, $sys.NumberOfProcessors)
} catch {}

# ---------------- 2) 数据库检查 ----------------
New-Section "数据库检查"
if(Test-Path $DB){
  $fi = Get-Item $DB
  Add-Finding "OK" "DB" ("存在, 大小={0} GB, 修改时间={1}" -f ([math]::Round($fi.Length/1GB,2)), $fi.LastWriteTime)
  # PRAGMA quick check via python
  $chk = Invoke-SqliteJson $DB "PRAGMA quick_check;"
  if($chk -and -not $chk.__error__){
    if($chk[0].'quick_check' -ne 'ok'){ Add-Finding "ERROR" "DB" ("quick_check 非 ok -> {0}" -f ($chk | ConvertTo-Json -Compress)) }
    else { Add-Finding "OK" "DB" "quick_check = ok" }
  } else { Add-Finding "WARN" "DB" "无法执行 quick_check" }
} else {
  Add-Finding "ERROR" "DB" "数据库不存在：$DB"
}

# 表命名、最新K线与新鲜度（抽样）
$Symbols = Get-Symbols
if($Symbols.Count -eq 0){ Add-Finding "WARN" "DB" "未能获取任何符号（缺少symbols文件且库内无可识别表）" }
else { Add-Finding "INFO" "DB" ("检测符号数={0}" -f $Symbols.Count) }

$nowUtc = [DateTime]::UtcNow
foreach($sym in $Symbols){
  foreach($tf in $TFs){
    $tbl = "${sym}_${tf}"
    $exists = Invoke-SqliteJson $DB "SELECT name FROM sqlite_master WHERE type='table' AND name='$tbl';"
    if($exists -and -not $exists.__error__ -and $exists.Count -gt 0){
      $row = Invoke-SqliteJson $DB "SELECT COUNT(*) AS n, MIN(timestamp) AS tmin, MAX(timestamp) AS tmax FROM '$tbl';"
      if($row.__error__){
        Add-Finding "WARN" "DB" "$tbl 统计失败：$($row.__error__)"
        continue
      }
      $n=$row[0].n; $tmax=$row[0].tmax
      if($n -le 0){ Add-Finding "WARN" "DB" "$tbl 无数据" ; continue }
      if($tmax){
        $t = [DateTimeOffset]::FromUnixTimeSeconds([int64]$tmax).UtcDateTime
        $mins = [int]([TimeSpan]::FromTicks(($nowUtc - $t).Ticks).TotalMinutes)
        if($mins -gt $FreshThresholdMin){
          Add-Finding "WARN" "Fresh" ("{0} 最新K距当前 {1} 分钟 (> {2})" -f $tbl,$mins,$FreshThresholdMin)
        } else {
          Add-Finding "OK" "Fresh" ("{0} 最新K距当前 {1} 分钟" -f $tbl,$mins)
        }
      } else {
        Add-Finding "WARN" "DB" "$tbl 无 tmax"
      }
    } else {
      Add-Finding "WARN" "DB" "$tbl 表不存在"
    }
  }
}

# ---------------- 3) 采集层检查 ----------------
New-Section "采集层检查"
$collectors = @("tools\realtime_collector.py","tools\rt_updater_with_banner.py")
foreach($p in $collectors){
  if(Test-Path $p){ Add-Finding "OK" "Collector" "$p 存在" } else { Add-Finding "ERROR" "Collector" "$p 缺失" }
}
# 网络连通（只GET, 不下单）
try {
  $r = Invoke-WebRequest -UseBasicParsing -Uri "https://fapi.binance.com/fapi/v1/ping" -TimeoutSec 5
  Add-Finding "OK" "Net" "Binance /ping 通畅 (HTTP $($r.StatusCode))"
} catch { Add-Finding "WARN" "Net" "Binance /ping 异常：$($_.Exception.Message)" }

# ---------------- 4) 回测&优化层检查 ----------------
New-Section "回测&优化层检查"
$btMods = @(
  "backtest\multi_timeframe_backtester.py",
  "optimizer\parameter_optimizer.py",
  "report\backtest_report_generator.py",
  "tools\run_backtest_all.py"
)
foreach($m in $btMods){
  if(Test-Path $m){ Add-Finding "OK" "Backtest" "$m 存在" } else { Add-Finding "ERROR" "Backtest" "$m 缺失" }
}
# 关键依赖探测（仅探测能否 import）
$pyProbe = @"
import importlib, json
mods = ['yaml','matplotlib','numpy','pandas','sklearn','lightgbm','torch']
out = {}
for m in mods:
    try:
        importlib.import_module(m)
        out[m]='OK'
    except Exception as e:
        out[m]=str(e.__class__.__name__)
print(json.dumps(out))
"@
$probe = Invoke-Python $pyProbe
try{
  $probeObj = $probe | ConvertFrom-Json
  foreach($k in $probeObj.PSObject.Properties.Name){
    $v = $probeObj.$k
    if($v -eq 'OK'){ Add-Finding "INFO" "PyDep" "$k OK" }
    else { Add-Finding "WARN" "PyDep" "$k 导入失败 ($v)" }
  }
}catch{ Add-Finding "WARN" "PyDep" "依赖探测解析失败" }

# ---------------- 5) 策略包检查 ----------------
New-Section "策略包检查"
$strategies = @(
  "strategy\signal_ma_cross.py","strategy\signal_bollinger.py","strategy\signal_atr.py",
  "strategy\signal_rsi.py","strategy\signal_reversal.py",
  "models\model_lightgbm.py","models\model_rf.py","models\model_lstm.py",
  "strategy\model_blender.py"
)
foreach($s in $strategies){
  if(Test-Path $s){ Add-Finding "OK" "Strategy" "$s 存在" } else { Add-Finding "WARN" "Strategy" "$s 缺失" }
}

# ---------------- 6) 选优产出供实盘（关键闭环） ----------------
New-Section "选优产出供实盘"
$bestJson = "deploy\live_best_params.json"
$rawJson  = "deploy\_raw_best_params.json"
if(Test-Path $bestJson){
  $age = (Get-Date) - (Get-Item $bestJson).LastWriteTime
  $daysOld = [int]$age.TotalDays
  if($daysOld -le $BestParamsFreshDays){
    Add-Finding "OK" "BestParams" ("{0} 存在，{1} 天内生成" -f $bestJson,$daysOld)
  } else {
    Add-Finding "WARN" "BestParams" ("{0} 存在，但较旧（{1} 天）" -f $bestJson,$daysOld)
  }
  try {
    $bp = Get-Content $bestJson -Raw | ConvertFrom-Json
    $bpCount = ($bp | Measure-Object).Count
    Add-Finding "INFO" "BestParams" ("JSON条目数={0}" -f $bpCount)
  } catch { Add-Finding "WARN" "BestParams" "JSON 解析失败" }
} else {
  Add-Finding "ERROR" "BestParams" "$bestJson 缺失"
}

# DB 内 best_params 表覆盖率（如果存在）
$bpTblCheck = Invoke-SqliteJson $DB "SELECT name FROM sqlite_master WHERE type='table' AND name='best_params';"
if($bpTblCheck -and -not $bpTblCheck.__error__ -and $bpTblCheck.Count -gt 0){
  Add-Finding "OK" "BestParams" "best_params 表存在"
  # 覆盖与新鲜度
  $miss = @()
  $stale = @()
  foreach($sym in $Symbols){
    foreach($tf in $TFs){
      $q = "SELECT updated_at FROM best_params WHERE symbol='$sym' AND (timeframe='$tf' OR tf='$tf') LIMIT 1;"
      $r = Invoke-SqliteJson $DB $q
      if($r.__error__ -or $r.Count -eq 0 -or -not $r[0].updated_at){
        $miss += "$sym/$tf"
      } else {
        $ts = $r[0].updated_at
        try{
          $d = [DateTime]::Parse($ts).ToUniversalTime()
          $dDays = ([datetime]::UtcNow - $d).TotalDays
          if($dDays -gt $BestParamsFreshDays){ $stale += "$sym/$tf" }
        } catch { $stale += "$sym/$tf" }
      }
    }
  }
  if($miss.Count -gt 0){ Add-Finding "WARN" "BestParams" ("缺覆盖：{0}" -f (($miss | Select-Object -First 20) -join ", ") + ($(if($miss.Count>20){" ... +$($miss.Count-20)"}))) }
  if($stale.Count -gt 0){ Add-Finding "WARN" "BestParams" ("过期：{0}" -f (($stale | Select-Object -First 20) -join ", ") + ($(if($stale.Count>20){" ... +$($stale.Count-20)"}))) }
  if(($miss.Count + $stale.Count) -eq 0){ Add-Finding "OK" "BestParams" "覆盖完整且在新鲜度期限内" }
} else {
  Add-Finding "WARN" "BestParams" "best_params 表不存在（仅依赖 JSON 将降低鲁棒性）"
}

# ---------------- 7) 实盘层安全态 & API ----------------
New-Section "实盘层检查"
$liveMods = @("live_trading\bitget_live_trader.py","live_trading\execution_engine.py")
foreach($m in $liveMods){
  if(Test-Path $m){ Add-Finding "OK" "Live" "$m 存在" } else { Add-Finding "ERROR" "Live" "$m 缺失" }
}
# 配置文件提示（不读取敏感值）
$cfgPath = "configs\settings.yaml"
if(Test-Path $cfgPath){
  Add-Finding "INFO" "Live" "$cfgPath 存在（未读取敏感字段）"
} else {
  Add-Finding "WARN" "Live" "缺少 configs\settings.yaml（或未配置）"
}

# ---------------- 8) 日志体检 ----------------
New-Section "日志体检"
$logDir = "logs"
if(Test-Path $logDir){
  $deadline = (Get-Date).AddMinutes(-$LogScanMins)
  $logs = Get-ChildItem $logDir -Filter *.log -Recurse | Where-Object {$_.LastWriteTime -ge $deadline}
  if($logs){
    foreach($lg in $logs){
      try{
        $tail = Get-Content $lg.FullName -Tail 200
        $hasErr = ($tail | Select-String -SimpleMatch "ERROR","CRITICAL","Traceback")
        if($hasErr){ Add-Finding "WARN" "Log" ("{0} 近{1}分钟出现错误迹象" -f $lg.Name,$LogScanMins) }
        else { Add-Finding "OK" "Log" ("{0} 正常（近{1}分钟）" -f $lg.Name,$LogScanMins) }
      } catch { Add-Finding "WARN" "Log" "$($lg.Name) 读取失败：$($_.Exception.Message)" }
    }
  } else {
    Add-Finding "WARN" "Log" ("{0} 分钟内无新日志" -f $LogScanMins)
  }
} else {
  Add-Finding "WARN" "Log" "日志目录缺失"
}

# ---------------- 汇总评分、导出 ----------------
New-Section "汇总"
$err = ($AllFindings | ?{$_.Level -eq "ERROR"}).Count
$warn = ($AllFindings | ?{$_.Level -eq "WARN"}).Count
$score = if($err -eq 0 -and $warn -le 5){"A"} elseif($err -le 3){"B"} else {"C"}
$elapsed = "{0:N1}s" -f $Stopwatch.Elapsed.TotalSeconds
Write-Host ("总评分: {0} | 错误={1}, 警告={2}, 用时={3}" -f $score,$err,$warn,$elapsed) -ForegroundColor Magenta

# Markdown
"# 系统体检报告  ($(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))`n" | Out-File $ReportFile
"**总评分**: $score  | **错误**: $err  | **警告**: $warn  | **用时**: $elapsed`n" | Out-File $ReportFile -Append
"**链路**: 环境 → 库 → 采集 → 回测 → 策略 → 选优产出供实盘 → 实盘 → 日志`n" | Out-File $ReportFile -Append
"**参数**: DB=$DB  | TFs=$($TFs -join ', ')  | FreshMin=$FreshThresholdMin  | BestParamsFreshDays=$BestParamsFreshDays`n" | Out-File $ReportFile -Append
"---`n" | Out-File $ReportFile -Append
($AllFindings | ForEach-Object { "* [{0}] [{1}] {2}" -f $_.Level,$_.Section,$_.Message }) | Out-File $ReportFile -Append

# JSON
$AllFindings | ConvertTo-Json -Depth 5 | Out-File $SummaryJson

Write-Host "报告已生成: $ReportFile"
Write-Host "摘要JSON:  $SummaryJson"
