@echo off
title ▶ 停止全部相关进程
echo [INFO] 正在结束 python/采集/实盘相关进程...

REM 停止所有 Python 进程（请确保没在跑其他无关Python程序）
taskkill /F /IM python.exe /T >nul 2>nul

REM 停止可能的后台窗口（采集器/执行引擎/仪表盘）
taskkill /F /FI "WINDOWTITLE eq 采集器" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq PaperTrading" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq 仪表盘" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Bitget 实盘*" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq Web 仪表盘" >nul 2>nul

echo [OK] 全部停止完成。
pause
