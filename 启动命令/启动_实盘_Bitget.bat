@echo off
setlocal
title ▶ Bitget 实盘（小资金试单）
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM 确保 configs\settings.yaml 已填入 API（仅下单权限）
REM 具体参数按你的 execution_engine 实现为准（exchange/mode等）
python live_trading\execution_engine.py ^
  --db "%QS_DB%" ^
  --exchange bitget ^
  --mode real ^
  --risk-capital 100 ^
  --max-order-pct 5 ^
  --leverage 5

pause
