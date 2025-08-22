@echo off
setlocal
title ▶ 一键三连：采集 + PaperTrading + 仪表盘
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM 窗口1：采集器
start "采集器" cmd /k ^
 "cd /d ""D:\quant_system_pro (3)\quant_system_pro"" ^& python tools\rt_updater_with_banner.py --db ""%QS_DB%"" --backfill-days 365 --max-workers 8 --interval 30"

REM 窗口2：Paper Trading 执行引擎
start "PaperTrading" cmd /k ^
 "cd /d ""D:\quant_system_pro (3)\quant_system_pro"" ^& python live_trading\execution_engine_binance_ws.py --db ""%QS_DB%"" --mode paper --ui-rows 30"

REM 窗口3：网页仪表盘
start "仪表盘" cmd /k ^
 "cd /d ""D:\quant_system_pro (3)\quant_system_pro"" ^& python dashboard\live_monitor.py --db ""%QS_DB%"""

pause
