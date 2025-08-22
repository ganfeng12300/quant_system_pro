@echo off
chcp 65001 >nul
setlocal
title [全链路] 采集 → 寻优下发 → 实盘

set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
cd /d "%QS_ROOT%"

call "一键_检测+采集.bat"
call "一键_寻优+下发.bat"
call "一键_实盘.bat"
