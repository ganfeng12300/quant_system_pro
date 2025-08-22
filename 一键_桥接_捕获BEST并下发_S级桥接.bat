@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION

title [桥接A] 捕获BEST→JSON/DB（原子+回滚）

REM === 固定路径 ===
set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
set "SYM_PERP=results\symbols_binance_perp.txt"
set "OUT_JSON=deploy\live_best_params.json"
set "OPT=optimizer\a1a8_optimizer_and_deploy.py"
REM =================

cd /d "%QS_ROOT%"
if not exist tools mkdir tools
if not exist deploy mkdir deploy
if not exist logs mkdir logs

REM 模式（默认 WIDE；传 S 为严格口径）
set "MODE=%1"
if "%MODE%"=="" set "MODE=WIDE"

echo 🌈 模式: %MODE%
if /I "%MODE%"=="WIDE" (
  set "DAYS=90"
  set "TF=1h 4h"
  set "MINTR=5"
  set "MAXDD=0.9"
) else (
  set "DAYS=180"
  set "TF=1h 4h"
  set "MINTR=10"
  set "MAXDD=0.6"
)

set PYTHONUNBUFFERED=1

python tools\bridge_best_stdout.py ^
  --optimizer "%OPT%" ^
  --db "%DB_PATH%" ^
  --symbols-file "%SYM_PERP%" ^
  --timeframes %TF% ^
  --days %DAYS% ^
  --min-trades %MINTR% ^
  --max-dd %MAXDD% ^
  --json "%OUT_JSON%" ^
  --write-db 1 ^
  --approve-all 1 ^
  --fee-bps 5 --slip-bps 2 --exec-lag 1 --no-intrabar 1

set RC=%ERRORLEVEL%
if %RC% NEQ 0 (
  echo 🔴 失败，退出码=%RC%
) else (
  echo 🟢 完成。JSON: %OUT_JSON%  （备份: %OUT_JSON%.bak）
)

echo.
pause
