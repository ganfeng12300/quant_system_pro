@echo off
setlocal
title ▶ 实时日志跟随
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM 这里示例监看 paper/real 日志目录，可按需修改路径或通配符
set LOG_GLOB=logs\*.log

powershell -NoProfile -Command ^
  "Get-ChildItem '%LOG_GLOB%' | Sort-Object LastWriteTime -Desc | Select-Object -First 1 | ForEach-Object { Write-Host ('LOG: ' + $_.FullName); Get-Content -Wait -Tail 200 $_.FullName }"
