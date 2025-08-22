@echo off
setlocal
title ▶ 执行引擎 · Paper Trading
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM ui-rows 仅示例，按需修改
python live_trading\execution_engine_binance_ws.py --db "%QS_DB%" --mode paper --ui-rows 30

pause
