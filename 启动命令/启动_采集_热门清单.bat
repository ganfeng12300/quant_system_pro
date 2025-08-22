@echo off
setlocal
title ▶ 采集（热门清单）
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM 请在项目根创建 symbols_hot.txt（每行一个symbol）
set MAX_WORKERS=8
set INTERVAL=30
set BACKFILL_DAYS=365

python tools\rt_updater_with_banner.py ^
  --db "%QS_DB%" ^
  --symbols-file symbols_hot.txt ^
  --backfill-days %BACKFILL_DAYS% ^
  --max-workers %MAX_WORKERS% ^
  --interval %INTERVAL%

pause
