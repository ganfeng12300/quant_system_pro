@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION
title [S级·一次过] 寻优→校验→自动下发→中文导出

REM 固定路径
set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
set "SYM_FILE=results\symbols_binance_perp.txt"
set "JSON_OUT=deploy\live_best_params.json"

cd /d "%QS_ROOT%"
if not exist utils\__init__.py type NUL>utils\__init__.py
if not exist backtest\__init__.py type NUL>backtest\__init__.py
set "PYTHONPATH=%QS_ROOT%;%PYTHONPATH%"

echo 🌈 [1/2] S级寻优→校验→自动下发…
python tools\run_backtest_sgrade.py ^
  --db "%DB_PATH%" ^
  --symbols-file "%SYM_FILE%" ^
  --timeframes 1h 4h ^
  --days 180 ^
  --min-trades 10 ^
  --max-dd 0.6 ^
  --deploy ^
  --json "%JSON_OUT%" ^
  --sgrade 1 ^
  --fee-bps 5 --slip-bps 2 ^
  --exec-lag 1 --no-intrabar 1 ^
  --min-trades-1h 30 --min-trades-4h 20 --min-trades-default 15 ^
  --max-dd-cap 0.60 ^
  --approve-all 1

if errorlevel 1 (
  echo 🔴 S级流程失败，请查看上方报错。
  pause & exit /b 2
)

echo 🌈 [2/2] 导出中文优秀结果表…
python tools\export_best_params_cn.py --db "%DB_PATH%" ^
  --out "deploy\最佳参数表_A1A8.csv" --xlsx "deploy\最佳参数表_A1A8.xlsx" ^
  --eligible-only 1 --approved-only 1

if errorlevel 1 (
  echo 🟠 导出中文表失败（不影响上一步的实盘清单），请单独重试。
) else (
  echo 🟢 完成。实盘清单：%JSON_OUT%（已备份 .bak 可回滚）
)

echo.
pause
