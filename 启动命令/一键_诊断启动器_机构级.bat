@echo off
chcp 65001 >nul
title ▶ 一键诊断启动器（机构级）· 采集/数据库/网络/锁表 全面体检
setlocal EnableExtensions EnableDelayedExpansion

REM ====== 基本参数 ======
set "ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "BINANCE_PING_URL=https://fapi.binance.com/fapi/v1/ping"
set "LOG_DIR=%ROOT%\logs"
for /f "tokens=1-3 delims=/: " %%a in ("%date% %time%") do set TS=%%a-%%b-%%c
set "TS=%date:~0,4%%date:~5,2%%date:~8,2%-%time:~0,2%%time:~3,2%%time:~6,2%"
set "TS=%TS: =0%"
set "REPORT=diag_report_%TS%.txt"

cd /d "%ROOT%"

echo ================== 一键诊断启动器（机构级） ================== >%REPORT%
echo 根目录: %ROOT%  >>%REPORT%
echo 数据库: %QS_DB% >>%REPORT%
echo 生成时间: %TS%    >>%REPORT%
echo ============================================================= >>%REPORT%
echo.>>%REPORT%

REM ====== 1) Python 检测 ======
echo [1/9] Python 路径 >>%REPORT%
where python >>%REPORT% 2>&1

REM ====== 2) 采集器进程 ======
echo.>>%REPORT%
echo [2/9] 采集器进程（rt_updater） >>%REPORT%
powershell -NoProfile -Command ^
  "$p=Get-CimInstance Win32_Process | ?{ $_.Name -eq 'python.exe' -and $_.CommandLine -match 'rt_updater' }; if($p){$p | Select ProcessId,CommandLine | Format-Table -Auto | Out-String | Write-Output} else {'[WARN] 未发现包含 rt_updater 的 python 进程'}" >>%REPORT% 2>&1

REM ====== 3) DB 文件信息 ======
echo.>>%REPORT%
echo [3/9] 数据库文件信息（含 WAL/SHM） >>%REPORT%
powershell -NoProfile -Command ^
  "$p='%QS_DB%'; if(Test-Path $p){ gi $p | Select Name,LastWriteTime,Length | ft -Auto | Out-String | Write-Output } else { '*** [ERROR] DB 不存在：%QS_DB%' } ; foreach($s in @('%QS_DB%.wal','%QS_DB%.shm')){ if(Test-Path $s){ gi $s | Select Name,LastWriteTime,Length | ft -Auto | Out-String | Write-Output } }" >>%REPORT% 2>&1

REM ====== 4) 近6小时写入检查 ======
>__diag3.py (
  echo import sqlite3, time
  echo db=r"%QS_DB%"
  echo cut=int(time.time())-6*3600
  echo con=sqlite3.connect(db, timeout=30)
  echo cur=con.cursor()
  echo tbl=[r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%[_]%'").fetchall()]
  echo def latest(t):
  echo.    try:
  echo.        r=cur.execute(f"SELECT MAX(timestamp) FROM '{t}'").fetchone()[0] or 0
  echo.    except Exception: r=0
  echo.    return r
  echo ok=[t for t in tbl if latest(t)>=cut*1000]
  echo print(f"[4/9] 近6小时有写入的表：{len(ok)}/{len(tbl)} (%.2f%%)" % (100.0*len(ok)/max(1,len(tbl))))
  echo if len(ok)^<5: print("[WARN] 近6小时几乎没有写入 —— 可能采集器停了或写错库")
  echo con.close()
)
python __diag3.py >>%REPORT% 2>&1 & del __diag3.py

REM ====== 5) 抽查 1h 表最近记录 ======
>__diag4.py (
  echo import sqlite3, datetime, random
  echo db=r"%QS_DB%"
  echo con=sqlite3.connect(db, timeout=30)
  echo cur=con.cursor()
  echo all1h=[r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%%_1h'").fetchall()]
  echo import random; random.shuffle(all1h)
  echo sample=all1h[:3]
  echo from datetime import datetime as D
  echo print("[5/9] 抽查 1h 表最近3条记录（UTC）")
  echo for t in sample:
  echo.    try:
  echo.        rows=cur.execute(f"SELECT timestamp FROM '{t}' ORDER BY timestamp DESC LIMIT 3").fetchall()
  echo.        human=[D.utcfromtimestamp(r[0]/1000).strftime('%%Y-%%m-%%d %%H:%%M:%%S') for r in rows]
  echo.        print(f"  {t:28s} -> {human}")
  echo.    except Exception as e:
  echo.        print(f"  {t:28s} -> ERROR {e}")
  echo con.close()
)
python __diag4.py >>%REPORT% 2>&1 & del __diag4.py

REM ====== 6) 不可补清单 ======
>__diag5.py (
  echo import sqlite3, time
  echo db=r"%QS_DB%"
  echo cut=int(time.time())-30*86400
  echo con=sqlite3.connect(db, timeout=30)
  echo cur=con.cursor()
  echo tbl=[r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%[_]%'").fetchall()]
  echo bad=[]
  echo for t in tbl:
  echo.    try:
  echo.        c=cur.execute(f"SELECT 1 FROM '{t}' WHERE timestamp>=? LIMIT 1",(cut*1000,)).fetchone()
  echo.        if c is None: bad.append(t)
  echo.    except Exception: bad.append(t)
  echo con.close()
  echo open("skiplist.txt","w",encoding="utf-8").write("\n".join(sorted(bad)))
  echo print(f"[6/9] 近30天零写入/空表：{len(bad)} 个 -> skiplist.txt")
)
python __diag5.py >>%REPORT% 2>&1 & del __diag5.py

REM ====== 7) PRAGMA / WAL 测试 ======
>__diag6.py (
  echo import sqlite3
  echo db=r"%QS_DB%"
  echo print("[7/9] PRAGMA 快检")
  echo try:
  echo.    con=sqlite3.connect(db, timeout=10)
  echo.    cur=con.cursor()
  echo.    print("  journal_mode=", cur.execute("PRAGMA journal_mode").fetchone())
  echo.    print("  wal_checkpoint(PASSIVE) ->", cur.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall())
  echo except Exception as e:
  echo.    print("  [WARN] PRAGMA 执行失败:", e)
  echo finally:
  echo.    try: con.close()
  echo.    except: pass
)
python __diag6.py >>%REPORT% 2>&1 & del __diag6.py

REM ====== 8) 磁盘空间 ======
echo.>>%REPORT%
echo [8/9] 磁盘空间 >>%REPORT%
powershell -NoProfile -Command ^
  "Get-PSDrive -PSProvider 'FileSystem' | Select Name,Used,Free | ft -Auto | Out-String | Write-Output" >>%REPORT% 2>&1

REM ====== 9) 网络连通（Binance） ======
echo.>>%REPORT%
echo [9/9] 网络连通性（Binance USDT-M ping）>>%REPORT%
powershell -NoProfile -Command ^
  "try { $r=Invoke-WebRequest -Uri '%BINANCE_PING_URL%' -TimeoutSec 10 -UseBasicParsing; '  HTTP ' + $r.StatusCode + ' OK' } catch { '  [WARN] 网络/接口不可达：' + $_.Exception.Message }" >>%REPORT% 2>&1

echo.
echo [OK] 诊断完成 → %CD%\%REPORT%
echo [OK] 不可补清单 → %CD%\skiplist.txt
start "" "%REPORT%"
pause
