@echo off
chcp 65001 >nul
title 【QS2】启动采集守护（自调并发/自愈/夜间保洁 · CST）

cd /d "D:\quant_system_pro (3)\quant_system_pro"

:: 配置数据库路径与参数
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "INTERVAL=30"
set "BACKFILL_DAYS=365"
set "SCALE_STEPS=12,8,6,4,3"
set "MAX_WORKERS=12"
set "HOUSEKEEPING=02:00-02:30"

echo.
echo [QS2] 正在启动采集守护进程...
python "tools_s2\qs2_rt_guardian.py" ^
  --db "%QS_DB%" ^
  --interval %INTERVAL% ^
  --backfill-days %BACKFILL_DAYS% ^
  --scale-steps %SCALE_STEPS% ^
  --max-workers %MAX_WORKERS% ^
  --housekeeping-window "%HOUSEKEEPING%"

echo.
echo [完成] 采集守护退出
pause
