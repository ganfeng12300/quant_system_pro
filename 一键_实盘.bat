@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION
title [实盘] Bitget 永续（默认 dry-run）

set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
set "EXCHANGE=bitget"
cd /d "%QS_ROOT%"

if not exist utils\__init__.py type NUL > utils\__init__.py
if not exist backtest\__init__.py type NUL > backtest\__init__.py
set "PYTHONPATH=%QS_ROOT%;%PYTHONPATH%"

echo [INFO] 从 deploy\live_best_params.json / DB(best_params) 读取最优参数，按 ticks 实时评估。
echo [TIP] 真下单：先 set API_KEY/SECRET，然后在命令行最后加 --live
echo.
python live_trading\live_trader_pro.py --db "%DB_PATH%" --exchange %EXCHANGE% --symbol BTCUSDT --tf 1h --strategy auto
pause
