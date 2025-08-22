@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION
title [寻优+下发] A1-A8 -> best_params + 中文表

set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
cd /d "%QS_ROOT%"

if not exist utils\__init__.py type NUL > utils\__init__.py
if not exist backtest\__init__.py type NUL > backtest\__init__.py
set "PYTHONPATH=%QS_ROOT%;%PYTHONPATH%"

set "SYM_FILE=results\symbols_bitget_perp.txt"
if not exist "%SYM_FILE%" set "SYM_FILE=results\symbols_from_db.txt"

echo [1/3] A1-A8 策略寻优并下发（近180天、1h/4h）…
python optimizer\a1a8_optimizer_and_deploy.py --db "%DB_PATH%" --symbols-file "%SYM_FILE%" ^
  --timeframes 1h 4h --days 180 --min-trades 10 --max-dd 0.5 --deploy --json deploy\live_best_params.json
if errorlevel 1 (
  echo [ERR] 寻优失败；请查看日志。
  pause & exit /b 2
)

echo [2/3] 导出中文《最佳参数表_A1A8.csv》…
if exist tools\export_best_params_cn.py (
  python tools\export_best_params_cn.py --db "%DB_PATH%" --out "deploy\最佳参数表_A1A8.csv"
) else (
  echo [WARN] 未找到 tools\export_best_params_cn.py，跳过导出中文表。
)

echo [3/3] 完成。按任意键退出…
pause >nul
