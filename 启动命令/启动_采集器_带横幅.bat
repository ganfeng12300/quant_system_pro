@echo off
setlocal
title ▶ 采集器（带横幅） · Binance USDT-M
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM 并发与间隔可调
set MAX_WORKERS=8
set INTERVAL=30
set BACKFILL_DAYS=365

echo.
echo [INFO] DB=%QS_DB%  workers=%MAX_WORKERS%  interval=%INTERVAL%s  backfill=%BACKFILL_DAYS%d
python tools\rt_updater_with_banner.py ^
  --db "%QS_DB%" ^
  --backfill-days %BACKFILL_DAYS% ^
  --max-workers %MAX_WORKERS% ^
  --interval %INTERVAL%

pause
