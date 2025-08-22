@echo off
REM ================================
REM  QS2 启动守护 - 低并发模式
REM ================================

cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM 设置编码，避免中文乱码
set PYTHONIOENCODING=utf-8

REM 创建日志目录（如果不存在）
if not exist logs mkdir logs

REM 启动采集守护，低并发 (max-workers=2)
python tools_s2\qs2_rt_guardian.py ^
  --db "D:\quant_system_v2\data\market_data.db" ^
  --interval 60 ^
  --backfill-days 365 ^
  --scale-steps 3,2,1 ^
  --max-workers 2 ^
  --housekeeping-window "02:00-02:30" ^
  1>logs\guardian_lowconcurrent.log 2>&1

pause
