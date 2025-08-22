@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION
title [采集·机构级] Binance USDT-M 永续（5m..1d｜补齐+实时｜彩色UI）

cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM 找 Python
set "PYEXE="
where python >nul 2>nul && set "PYEXE=python"
if not defined PYEXE ( where py >nul 2>nul && set "PYEXE=py" )
if not defined PYEXE (
  echo [FATAL] 未找到 Python（请加入 PATH）
  pause & exit /b 2
)

%PYEXE% scripts\collector_binance_usdm_pro.py
echo.
echo ===== 进程结束（窗口常驻，便于查看日志） =====
pause >nul
