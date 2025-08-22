@echo off
setlocal
title ▶ 一键：采集 + 总进度
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM 窗口1：采集器
start "采集器" cmd /k ^
 "cd /d ""D:\quant_system_pro (3)\quant_system_pro"" ^& python tools\rt_updater_with_banner.py --db ""%QS_DB%"" --backfill-days 365 --max-workers 8 --interval 30"

REM 窗口2：总进度监视器
start "总进度" cmd /k ^
 "cd /d ""D:\quant_system_pro (3)\quant_system_pro"" ^& python tools\show_total_progress.py --db ""%QS_DB%"" --days 365 --refresh 30 --topk 20"
