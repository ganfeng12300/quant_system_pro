@echo off
REM ASCII-only menu, safe echo syntax
title QuanSys - Control Menu (S)
setlocal EnableExtensions EnableDelayedExpansion

REM ---- GLOBALS ----
set "ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "LOG_DIR=%ROOT%\logs"
set "EXCLUDE_FILE=%ROOT%\启动命令\skiplist.txt"
set "MAX_WORKERS=8"
set "INTERVAL=30"
set "BACKFILL_DAYS=365"
set "TOPK=40"
set "PAPER_UI_ROWS=30"
set "RISK_CAPITAL=100"
set "MAX_ORDER_PCT=5"
set "LEVERAGE=5"

:MENU
cls
echo(===========================================================
echo(  QuanSys - One-Key Control Menu (S-grade)
echo(===========================================================
echo(  1) Start Collector (with banner)
echo(  2) Progress Monitor (auto use skiplist.txt)
echo(  3) Diagnostics (make report + skiplist)
echo(  4) Backtest + Optimize (365d, all symbols)
echo(  5) PaperTrading Engine (sandbox)
echo(  6) REAL Trade - Bitget (small capital, confirm)
echo(  7) Web Dashboard
echo(  8) Tail Latest Log (auto-pick newest)
echo(  9) DB Maintenance: WAL checkpoint + VACUUM
echo( 10) STOP all related processes
echo( ----------------------------------------------------------
echo(  0) Exit
echo(===========================================================
if exist "%EXCLUDE_FILE%" (
  echo( [INFO] skiplist detected: %EXCLUDE_FILE%
) else (
  echo( [TIP] run Diagnostics (3) first to generate skiplist.txt if needed
)
echo( [DB] %QS_DB%
echo(
set /p choice=Select [0-10]: 

if "%choice%"=="1" goto COLLECTOR
if "%choice%"=="2" goto PROGRESS
if "%choice%"=="3" goto DIAG
if "%choice%"=="4" goto BACKTEST
if "%choice%"=="5" goto PAPER
if "%choice%"=="6" goto REAL
if "%choice%"=="7" goto DASH
if "%choice%"=="8" goto LOGS
if "%choice%"=="9" goto VAC
if "%choice%"=="10" goto STOPALL
if "%choice%"=="0" goto END
goto MENU

:COLLECTOR
start "Collector" cmd /k ^
 "cd /d ""%ROOT%"" ^& python tools\rt_updater_with_banner.py --db ""%QS_DB%"" --backfill-days %BACKFILL_DAYS% --max-workers %MAX_WORKERS% --interval %INTERVAL%"
goto MENU

:PROGRESS
if exist "%EXCLUDE_FILE%" (
  start "Progress" cmd /k ^
   "cd /d ""%ROOT%"" ^& python tools\show_total_progress.py --db ""%QS_DB%"" --days 365 --refresh 30 --topk 20 --exclude-file ""%EXCLUDE_FILE%"""
) else (
  start "Progress" cmd /k ^
   "cd /d ""%ROOT%"" ^& python tools\show_total_progress.py --db ""%QS_DB%"" --days 365 --refresh 30 --topk 20"
)
goto MENU

:DIAG
if exist "启动命令\一键_诊断启动器_机构级.bat" (
  start "Diagnostics" cmd /k ^
   "cd /d ""%ROOT%\启动命令"" ^& 一键_诊断启动器_机构级.bat"
) else (
  echo( [WARN] Diagnostics launcher not found in "启动命令".
  pause
)
goto MENU

:BACKTEST
start "Backtest" cmd /k ^
 "cd /d ""%ROOT%"" ^& python tools\progress_wrap_and_run_plus.py --workdir ""%ROOT%"" --cmd ""python -u tools\inject_and_run.py backtest\backtest_pro.py --db %QS_DB% --days 365 --topk %TOPK% --outdir results"" --results-dir results --top 10"
goto MENU

:PAPER
start "PaperTrading" cmd /k ^
 "cd /d ""%ROOT%"" ^& python live_trading\execution_engine_binance_ws.py --db ""%QS_DB%"" --mode paper --ui-rows %PAPER_UI_ROWS%"
goto MENU

:REAL
echo( --- Safety Check: REAL Bitget ---
echo(  Risk capital  = %RISK_CAPITAL% U
echo(  Max per order = %MAX_ORDER_PCT%%% 
echo(  Leverage      = %LEVERAGE%x
choice /M "Continue"
if errorlevel 2 goto MENU
start "Bitget REAL" cmd /k ^
 "cd /d ""%ROOT%"" ^& python live_trading\execution_engine.py --db ""%QS_DB%"" --exchange bitget --mode real --risk-capital %RISK_CAPITAL% --max-order-pct %MAX_ORDER_PCT% --leverage %LEVERAGE%"
goto MENU

:DASH
start "Dashboard" cmd /k ^
 "cd /d ""%ROOT%"" ^& python dashboard\live_monitor.py --db ""%QS_DB%"""
goto MENU

:LOGS
start "TailLog" powershell -NoProfile -Command ^
 "$d='%LOG_DIR%'; if(Test-Path $d){ $f=Get-ChildItem $d -File | Sort-Object LastWriteTime -Desc | Select-Object -First 1; if($f){ Write-Host ('LOG: ' + $f.FullName); Get-Content -Wait -Tail 200 $f.FullName } else { Write-Host 'no log file' } } else { Write-Host 'log dir not found' }"
goto MENU

:VAC
echo( [INFO] Running WAL checkpoint + VACUUM (do in off-peak)
>__vac.py (
  echo import sqlite3
  echo db=r"%QS_DB%"
  echo con=sqlite3.connect(db, timeout=30); cur=con.cursor()
  echo print("[PRAGMA] journal_mode=", cur.execute("PRAGMA journal_mode").fetchone())
  echo cur.execute("PRAGMA wal_checkpoint(TRUNCATE)"); con.commit(); print("[OK] checkpoint")
  echo cur.execute("VACUUM"); con.commit(); print("[OK] vacuum")
  echo con.close()
)
python __vac.py
del __vac.py
pause
goto MENU

:STOPALL
echo( [INFO] Killing python-related windows...
taskkill /F /IM python.exe /T >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Collector" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Progress" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Diagnostics" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Backtest" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq PaperTrading" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Bitget REAL" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Dashboard" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq TailLog" >nul 2>nul
echo( [OK] done.
pause
goto MENU

:END
exit /b 0
