@echo off
setlocal
title ▶ Web 仪表盘
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM 如 live_monitor.py 支持 --port/--host 可加；默认启动本机
python dashboard\live_monitor.py --db "%QS_DB%"

pause
