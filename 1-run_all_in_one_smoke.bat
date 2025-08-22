@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ===== One-click ALL-IN smoke backtest (GLOBAL progress, fixed) =====
cd /d "%~dp0" 2>nul || (echo [FATAL] cannot cd to script folder.& pause & exit /b 1)

if not exist requirements.txt (
  echo [FATAL] run this in your repo root: where requirements.txt lives.
  pause & exit /b 1
)

where python >nul 2>nul || (echo [FATAL] Python 3.9+ not found.& pause & exit /b 1)
python -c "import sys; sys.exit(0 if sys.version_info[:2]>=(3,9) else 1)" || (
  echo [FATAL] Python 3.9+ required. Detected: & python --version & pause & exit /b 1
)

echo [STEP] pip bootstrap...
python -m pip install --upgrade pip setuptools wheel >nul

echo [STEP] deps (skip if satisfied)...
python -m pip install -r requirements.txt >nul

rem --- env knobs (set for current session + persist for future) ---
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "QS_RESULTS_DB=%CD%\data\backtest_results.db"
set "QS_PAPER=1"
set "QS_RISK_PER_TRADE=0.01"
set "QS_MAX_DAILY_LOSS=0.05"
set "QS_TAKER_FEE=0.0005"
set "QS_SLIPPAGE=0.0003"
set "QS_FUNDING_ON=1"
set "QS_METRICS_PORT=9108"
set "QS_ENABLE_GA=1"
set "QS_ENABLE_VALIDATORS=1"
for %%V in (QS_DB QS_RESULTS_DB QS_PAPER QS_RISK_PER_TRADE QS_MAX_DAILY_LOSS QS_TAKER_FEE QS_SLIPPAGE QS_FUNDING_ON QS_METRICS_PORT QS_ENABLE_GA QS_ENABLE_VALIDATORS) do (
  setx %%V "!%%V!" >nul
)

rem --- small symbol universe file ---
if not exist config mkdir config
set "SMOKE_LIST=config\symbols_smoke.txt"
> "%SMOKE_LIST%" (
  echo BTCUSDT
  echo ETHUSDT
  echo BNBUSDT
  echo SOLUSDT
  echo XRPUSDT
  echo ADAUSDT
)

rem --- read symbols into one line for --symbols (backtest_pro.py does NOT support --symbols-file) ---
set "SYMS="
for /f usebackq tokens=* delims= %%S in ("%SMOKE_LIST%") do (
  if not "%%~S"=="" set "SYMS=!SYMS! %%~S"
)

if not exist results mkdir results

set "DAYS=120"
set "TOPK=5"

rem --- build backtest command (use --symbols <list...>) ---
set "BT_CMD=python -u backtest\backtest_pro.py --db %QS_DB% --days %DAYS% --topk %TOPK% --outdir results --symbols !SYMS!"

rem --- global progress wrapper ---
set "WRAP=tools\progress_wrap_and_run_plus.py"

echo.
echo [RUN] ALL-IN SMOKE BACKTEST (GLOBAL progress expected)
echo       symbols: !SYMS!    days=%DAYS%   topk=%TOPK%
echo.

if exist "%WRAP%" (
  echo [WRAP] %WRAP%
  rem 传 --tasks 给包装器作为初始总数（避免默认 40），后续若日志里出现 x/y、trials a/b 会自动纠正
  python "%WRAP%" --workdir "%CD%" --cmd "%BT_CMD%" --results-dir results --tasks %TOPK%
) else (
  echo [WARN] wrapper missing: %WRAP%
  echo       running raw command without global progress...
  %BT_CMD%
)
if errorlevel 1 (
  echo [ERROR] backtest failed. See logs above.
  pause & exit /b 1
)

rem --- validators (SPA/PBO/DS) if present ---
if exist "backtest\stats_validators.py" (
  echo.
  echo [VALIDATORS] SPA/PBO/DS (if enabled by script defaults)...
  python backtest\stats_validators.py --results-dir results
)

echo.
if exist "data\best_combo.csv" (
  echo [SUMMARY] data\best_combo.csv (first 10 lines)
  python - <<PY
import itertools, os
p=r"data\\best_combo.csv"
if os.path.exists(p):
    with open(p,'r',encoding='utf-8',errors='replace') as f:
        for row in itertools.islice(f,10):
            print(row.rstrip())
else:
    print("[WARN] best_combo.csv not found")
PY
) else (
  echo [WARN] data\best_combo.csv not found (maybe saved only under results\).
)

echo.
echo [DONE] Smoke backtest finished. Results under results\ and data\best_combo.csv
echo.
pause
exit /b 0
