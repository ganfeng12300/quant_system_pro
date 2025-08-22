@echo off
chcp 65001 >nul
title ▶ 量化交易系统 · 一键总控菜单（机构级）
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

:MENU
cls
echo.
echo ╔════════════════════════════════════════════════════╗
echo ║ 🚀 量化交易系统 · 一键总控菜单（机构级）                 ║
echo ╟────────────────────────────────────────────────────╢
echo ║  1. 启动采集器（带横幅进度）                          ║
echo ║  2. 启动总进度监视器（直读数据库）                    ║
echo ║  3. 启动日志跟随（自动选最新日志）                    ║
echo ║  4. 一键回测寻优（365天，全币种）                     ║
echo ║  5. 启动 Paper Trading 执行引擎                      ║
echo ║  6. 启动 Bitget 实盘（小资金试单）                     ║
echo ║  7. 启动网页仪表盘                                   ║
echo ║  8. 一键三连（采集 + PaperTrading + 仪表盘）           ║
echo ║  9. 数据库维护：WAL Checkpoint + VACUUM              ║
echo ║ 10. 一键停止全部进程（采集/实盘/仪表盘）               ║
echo ╟────────────────────────────────────────────────────╢
echo ║  0. 退出菜单                                         ║
echo ╚════════════════════════════════════════════════════╝
echo.

set /p choice=请选择操作 [0-10]: 

if "%choice%"=="1" goto COLLECTOR
if "%choice%"=="2" goto PROGRESS
if "%choice%"=="3" goto LOGS
if "%choice%"=="4" goto BACKTEST
if "%choice%"=="5" goto PAPER
if "%choice%"=="6" goto REAL
if "%choice%"=="7" goto DASHBOARD
if "%choice%"=="8" goto THREE
if "%choice%"=="9" goto VACUUM
if "%choice%"=="10" goto STOPALL
if "%choice%"=="0" goto END
goto MENU

:COLLECTOR
start "采集器" cmd /k python tools\rt_updater_with_banner.py --db "%QS_DB%" --backfill-days 365 --max-workers 8 --interval 30
goto MENU

:PROGRESS
start "总进度监视" cmd /k python tools\show_total_progress.py --db "%QS_DB%" --days 365 --refresh 30 --topk 20
goto MENU

:LOGS
start "日志跟随" powershell -NoProfile -Command ^
 "Get-ChildItem 'logs\*.log' | Sort-Object LastWriteTime -Desc | Select-Object -First 1 | ForEach-Object { Write-Host ('LOG: ' + $_.FullName); Get-Content -Wait -Tail 200 $_.FullName }"
goto MENU

:BACKTEST
start "回测寻优" cmd /k python tools\progress_wrap_and_run_plus.py --workdir "%CD%" --cmd "python -u tools\inject_and_run.py backtest\backtest_pro.py --db %QS_DB% --days 365 --topk 40 --outdir results" --results-dir results --top 10
goto MENU

:PAPER
start "PaperTrading" cmd /k python live_trading\execution_engine_binance_ws.py --db "%QS_DB%" --mode paper --ui-rows 30
goto MENU

:REAL
start "Bitget实盘" cmd /k python live_trading\execution_engine.py --db "%QS_DB%" --exchange bitget --mode real --risk-capital 100 --max-order-pct 5 --leverage 5
goto MENU

:DASHBOARD
start "仪表盘" cmd /k python dashboard\live_monitor.py --db "%QS_DB%"
goto MENU

:THREE
start "采集器" cmd /k python tools\rt_updater_with_banner.py --db "%QS_DB%" --backfill-days 365 --max-workers 8 --interval 30
start "PaperTrading" cmd /k python live_trading\execution_engine_binance_ws.py --db "%QS_DB%" --mode paper --ui-rows 30
start "仪表盘" cmd /k python dashboard\live_monitor.py --db "%QS_DB%"
goto MENU

:VACUUM
python - <<PY
import sqlite3
db = r"%QS_DB%"
con = sqlite3.connect(db, timeout=30)
cur = con.cursor()
print("[PRAGMA] journal_mode=", cur.execute("PRAGMA journal_mode").fetchone())
cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
con.commit()
cur.execute("VACUUM")
con.commit()
con.close()
print("[OK] VACUUM 完成")
PY
pause
goto MENU

:STOPALL
taskkill /F /IM python.exe /T >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq 采集器" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq PaperTrading" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq 仪表盘" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Bitget实盘" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq 总进度监视" >nul 2>nul
echo [OK] 已停止全部相关进程。
pause
goto MENU

:END
exit
