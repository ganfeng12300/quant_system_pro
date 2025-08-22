@echo off
setlocal EnableExtensions

rem ===== Quant Pro (S) SAFE MODE =====
rem Root: D:\quant_system_pro
rem DB  : D:\quant_system_v2\data\market_data.db
rem ===================================

cd /d D:\quant_system_pro 2>nul
if errorlevel 1 (
  echo [FATAL] Cannot cd to D:\quant_system_pro
  pause & exit /b 1
)

if not exist data    mkdir data
if not exist results mkdir results
if not exist logs    mkdir logs
if not exist config  mkdir config

if not exist requirements.txt (
  echo [FATAL] requirements.txt missing in root.
  pause & exit /b 1
)

where python >nul 2>nul
if errorlevel 1 (
  echo [FATAL] Python 3.10+ not found. Install and add to PATH.
  pause & exit /b 1
)

echo.
echo [STEP] Upgrade pip/setuptools/wheel...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 ( echo [ERROR] pip upgrade failed. pause & exit /b 1 )

echo.
echo [STEP] Install requirements...
python -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo [HINT] If network is slow, try mirror:
  echo        python -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
  pause & exit /b 1
)

rem ---- envs (no quotes)
setx QS_DB D:\quant_system_v2\data\market_data.db >nul
setx QS_RESULTS_DB D:\quant_system_pro\data\backtest_results.db >nul
setx QS_PAPER 1 >nul
setx QS_RISK_PER_TRADE 0.01 >nul
setx QS_MAX_DAILY_LOSS 0.05 >nul
setx QS_TAKER_FEE 0.0005 >nul
setx QS_SLIPPAGE 0.0003 >nul
setx QS_FUNDING_ON 1 >nul
setx QS_METRICS_PORT 9108 >nul
setx QS_LIVE_BINANCE 0 >nul
setx QS_LIVE_OKX 0 >nul
setx QS_LIVE_BITGET 0 >nul

:MENU
echo.
echo ==============================================
echo  Quant System Pro (S) - SAFE CONSOLE
echo  metrics: http://localhost:9108/metrics
echo ----------------------------------------------
echo  [1] 快速自检（文件/依赖/DB）
echo  [2] 覆盖率体检/历史补齐（可选）
echo  [3] S档回测寻优（Bayes+GA/冲击/SPA/PBO/DS）
echo  [4] PAPER 实盘（不真下单）
echo  [5] LIVE  真仓（需设置 QS_LIVE_xxx=1 和密钥）
echo  [0] 退出
echo ==============================================
set /p choice=Select: 
if "%choice%"=="1" goto DIAG
if "%choice%"=="2" goto COLLECT
if "%choice%"=="3" goto BACKTEST
if "%choice%"=="4" goto PAPER
if "%choice%"=="5" goto LIVE
if "%choice%"=="0" goto END
echo Invalid option.
goto MENU

:DIAG
echo.
echo [CHECK] Python version:
python --version
echo.
echo [CHECK] key files:
for %%F in (tools\config.py tools\db_util.py tools\fees_rules.py tools\collector_pro.py strategy\strategies_a1a8.py backtest\backtest_pro.py backtest\stats_validators.py live\live_router_multi.py live\executors.py) do (
  if exist "%%F" (echo   OK  %%F) else (echo   MISS %%F)
)
echo.
echo [CHECK] try importing libs...
python -c "import pandas,numpy,requests,statsmodels,scipy,sklearn,hyperopt,matplotlib; print('imports OK')"
echo.
echo [CHECK] DB connectivity:
python - <<PY
import os,sqlite3; db=os.environ.get('QS_DB',r'D:\quant_system_v2\data\market_data.db')
print('DB =',db)
if not os.path.exists(db): print('!! DB not found')
else:
  con=sqlite3.connect(f'file:{db}?mode=ro&cache=shared', uri=True, timeout=10)
  names=[r[0] for r in con.execute(\"select name from sqlite_master where type='table' limit 20\")]
  print('tables sample:', names)
  tgt=[n for n in names if '_' in n]
  if tgt:
    t=tgt[0]
    print('probe table:', t, con.execute(f'select count(*), max(ts) from \"{t}\"').fetchone())
  else:
    print('!! no SYMBOL_TF tables (e.g., BTCUSDT_5m)')
  con.close()
PY
echo.
pause
goto MENU

:COLLECT
if exist tools\collector_pro.py (
  echo.
  echo [COLLECT] coverage/backfill/daemon...
  python tools\collector_pro.py --db %QS_DB% --symbols-file config\symbols_whitelist_48.txt --backfill-days 365 --start-daemon 1
) else (
  echo [SKIP] tools\collector_pro.py not found
)
echo.
pause
goto MENU

:BACKTEST
if not exist backtest\backtest_pro.py ( echo [ERROR] backtest script missing & pause & goto MENU )
echo.
echo [BT] S backtest start...
python backtest\backtest_pro.py --db %QS_DB% --days 365 --topk 40 --outdir results
if errorlevel 1 ( echo [ERROR] backtest failed. See above. & pause & goto MENU )
echo.
echo [DONE] Look at results\* and data\best_combo.csv
echo.
pause
goto MENU

:PAPER
setx QS_PAPER 1 >nul
if not exist live\live_router_multi.py ( echo [ERROR] live router missing & pause & goto MENU )
echo.
echo [LIVE] PAPER mode...
python live\live_router_multi.py
echo.
pause
goto MENU

:LIVE
setx QS_PAPER 0 >nul
echo.
echo [LIVE] REAL trading. Be sure to set QS_LIVE_BINANCE / QS_LIVE_OKX / QS_LIVE_BITGET = 1 and keys.
python live\live_router_multi.py
echo.
pause
goto MENU

:END
endlocal
exit /b 0
