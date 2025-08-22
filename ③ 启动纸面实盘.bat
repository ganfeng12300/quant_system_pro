@echo off
chcp 65001 >nul
title 【QS2】启动纸面实盘（含新鲜度闸门）

cd /d "D:\quant_system_pro (3)\quant_system_pro"

:: 配置数据库路径
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "TFS=5m,15m,30m,1h,2h,4h,1d"

echo [QS2] 正在执行新鲜度闸门检查...
python "tools_s2\qs2_pretrade_gate.py" --db "%QS_DB%" --timeframes "%TFS%"
if errorlevel 1 (
  echo [阻断] 数据延迟未达标，纸面实盘已阻止启动（请先追平采集）.
  pause
  exit /b 2
)

echo [QS2] 闸门通过，启动纸面实盘...
REM 可以在这里加参数，例如 --risk-capital 100 --max-order-pct 5 --leverage 5
python "live_trading\execution_engine_binance_ws.py" --db "%QS_DB%" --mode paper --ui-rows 30

echo [完成] 纸面实盘退出
