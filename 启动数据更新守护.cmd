@echo off
chcp 65001 >nul
title 📡 实时数据更新守护

cd /d "D:\quant_system_pro (3)\quant_system_pro"

echo.
echo ================================
echo   📡 实时数据更新守护进程启动中...
echo ================================
echo.

python tools\rt_updater_pro.py --db D:\quant_system_v2\data\market_data.db --max-workers 8 --backfill 365

echo.
echo ================================
echo   ✅ 守护进程已退出
echo   （保持窗口打开即可持续更新）
echo ================================
pause >nul
