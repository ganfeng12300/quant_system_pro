@echo off
chcp 65001 >nul
set "OUT_JSON=D:\quant_system_pro (3)\quant_system_pro\deploy\live_best_params.json"

if not exist "%OUT_JSON%.bak" (
  echo 没有可回滚的 .bak 文件
  pause & exit /b 2
)

copy /Y "%OUT_JSON%.bak" "%OUT_JSON%" >nul
if errorlevel 1 (
  echo 回滚失败
  pause & exit /b 3
)

echo 🟡 已回滚到上一版：%OUT_JSON%
pause
