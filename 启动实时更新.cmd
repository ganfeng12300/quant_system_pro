@echo off
chcp 65001 >nul
title 📡 启动实时更新守护进程

cd /d "D:\quant_system_pro (3)\quant_system_pro"

echo.
echo ================================
echo   📡 启动实时更新守护进程
echo ================================
echo.

python tools\rt_updater_pro.py --db D:\quant_system_v2\data\market_data.db --max-workers 8 --backfill 365

echo.
echo ================================
echo   ✅ 守护进程已退出，按任意键关闭窗口
echo ================================
pause >nul
