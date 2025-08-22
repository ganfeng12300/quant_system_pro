@echo off
setlocal ENABLEDELAYEDEXECUTION
chcp 65001 >nul
title 【QS2】夜间寻优任务（保留窗口+日志）

REM 1) 切到项目根目录（注意引号，路径里有空格/括号）
cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM 2) 参数区 —— 按需修改
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "TFS=5m,15m,30m,1h,2h,4h,1d"
set "DAYS=180"
set "MIN_TRADES=1"
set "MAX_DD=10000"
set "SYMBOLS=results\symbols_binance_perp.txt"
set "OUT_JSON=deploy\qs2_live_best_params.json"
set "REPORT=results\qs2_optimizer_report.json"

REM 3) 日志（固定一个“最近一次”的文件，便于查看）
if not exist "logs" mkdir "logs"
set "LOG=logs\qs2_nightly_optimizer.last.log"

echo [QS2] ==== 夜间寻优启动 ====  > "%LOG%"
echo DB=%QS_DB%  >> "%LOG%"
echo TFS=%TFS%   >> "%LOG%"
echo OUT=%OUT_JSON% REPORT=%REPORT% >> "%LOG%"
echo ---------------------------------------------- >> "%LOG%"

REM 4) 基础自检：Python、脚本、文件存在性
where python >nul 2>&1
if errorlevel 1 (
  echo [ERROR] 未找到 python，请确认已安装并加入 PATH。>> "%LOG%"
  echo [ERROR] 未找到 python，请确认已安装并加入 PATH。
  goto :HOLD
)

if not exist "tools_s2\qs2_nightly_optimizer.py" (
  echo [ERROR] 缺少 tools_s2\qs2_nightly_optimizer.py >> "%LOG%"
  echo [ERROR] 缺少 tools_s2\qs2_nightly_optimizer.py
  goto :HOLD
)

if not exist "%QS_DB%" (
  echo [ERROR] 数据库不存在：%QS_DB% >> "%LOG%"
  echo [ERROR] 数据库不存在：%QS_DB%
  goto :HOLD
)

if not exist "optimizer\a1a8_optimizer_and_deploy.py" (
  echo [ERROR] 缺少 optimizer\a1a8_optimizer_and_deploy.py >> "%LOG%"
  echo [ERROR] 缺少 optimizer\a1a8_optimizer_and_deploy.py
  goto :HOLD
)

REM 5) 运行（把控制台输出同时写入日志）
echo [QS2] 校验新鲜度并启动寻优... | tee -a "%LOG%"
python "tools_s2\qs2_nightly_optimizer.py" ^
  --db "%QS_DB%" ^
  --timeframes "%TFS%" ^
  --days %DAYS% ^
  --min-trades %MIN_TRADES% ^
  --max-dd %MAX_DD% ^
  --symbols-file "%SYMBOLS%" ^
  --out-json "%OUT_JSON%" ^
  --report "%REPORT%" ^
  >> "%LOG%" 2>&1

set "RC=%ERRORLEVEL%"
echo [QS2] 退出码=%RC% >> "%LOG%"

if "%RC%"=="0" (
  echo [QS2] ? 夜间寻优完成，产物：%OUT_JSON%
  echo [QS2] ? 夜间寻优完成，产物：%OUT_JSON% >> "%LOG%"
) else if "%RC%"=="2" (
  echo [QS2] ? 新鲜度未达标，已终止（查看 %REPORT% 细节）
  echo [QS2] ? 新鲜度未达标，已终止 >> "%LOG%"
) else (
  echo [QS2] ? 运行失败（退出码=%RC%），请查看日志：%LOG%
)

:HOLD
echo.
echo [提示] 本窗口已保留，按任意键关闭……
pause >nul
endlocal
