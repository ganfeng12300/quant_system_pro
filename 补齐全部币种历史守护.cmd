@echo off
chcp 65001 >nul
title ⚙️ 动态并发守护（6↔4↔3，自适应418/429/锁表）

cd /d "D:\quant_system_pro (3)\quant_system_pro"
echo.
echo ================================
echo   ⚙️ 启动动态并发守护
echo   起步：6 并发，窗口：1分钟
echo   触发：错率≥5%% 或 锁表超阈值 → 降档
echo   恢复：连续10轮稳定 → 升档
echo ================================
echo.

python tools\dyn_guard_rt_updater.py

echo.
echo ================================
echo   ✅ 守护已退出，按任意键关闭
echo ================================
pause >nul
