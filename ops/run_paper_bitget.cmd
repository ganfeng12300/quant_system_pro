@echo off
chcp 65001 >nul
setlocal EnableExtensions EnableDelayedExpansion
title [纸面实盘] Bitget 永续（不下单）

rem === 基本参数（按需改） ===
set "QS_ROOT=Q:\"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
set "EXCHANGE=bitget"
set "SYMS_FILE=Q:\config\symbols_pilot.txt"
rem 先用 1h 稳定跑通；之后再加 4h：把 TIMEFRAMES 改成 1h 4h
set "TIMEFRAMES=1h"
set "PY=C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python.exe"
set "RUNNER=Q:\live_trading\live_trader_pro.py"
set "LOGDIR=Q:\logs\paper"

if not exist "%LOGDIR%" mkdir "%LOGDIR%"
cd /d "%QS_ROOT%" || (echo [FATAL] 仓库路径不可达 & pause & exit /b 1)

rem === 补 __init__，保证包导入 ===
if not exist utils\__init__.py type NUL > utils\__init__.py
if not exist backtest\__init__.py type NUL > backtest\__init__.py
set "PYTHONPATH=%QS_ROOT%;%PYTHONPATH%"

rem === 自检：库里是否有可读最佳参数 ===
"%PY%" "Q:\tools\verify_live_ready.py" --db "%DB_PATH%"
if errorlevel 1 ( echo [FATAL] 实盘可读性自检失败，请先回灌或回测。 & pause & exit /b 2 )

rem === 读符号清单 ===
if not exist "%SYMS_FILE%" (echo [FATAL] 缺少符号清单 %SYMS_FILE% & pause & exit /b 3)

for /f "usebackq tokens=1" %%S in ("%SYMS_FILE%") do (
  if not "%%~S"=="" (
    for %%T in (%TIMEFRAMES%) do (
      rem —— 针对当前 (symbol, timeframe) 做可读性检查
      "%PY%" "Q:\tools\verify_live_ready.py" --db "%DB_PATH%" --symbol "%%~S" --timeframe "%%~T" >nul 2>&1
      if errorlevel 1 (
        echo [WARN] 缺少 SYM=%%S TF=%%T 的最佳参数，已跳过。
      ) else (
        rem —— 组合时间戳与日志名
        for /f "tokens=1-4 delims=/- " %%a in ("%date%") do set "DT=%%a%%b%%c"
        for /f "tokens=1-3 delims=:." %%h in ("%time%") do set "TM=%%h%%i%%j"
        set "TS=!DT!_!TM!"
        set "LOG=%LOGDIR%\paper_%%S_%%T_!TS!.log"

        echo [RUN] 启动纸面实盘 SYM=%%S TF=%%T ，日志输出到：!LOG!
        "%PY%" -u "%RUNNER%" --db "%DB_PATH%" --exchange %EXCHANGE% --symbol %%S --tf %%T --strategy auto >> "!LOG!" 2>&1
        echo [OK] 完成 SYM=%%S TF=%%T ，日志：!LOG!
        echo.
      )
    )
  )
)

echo [DONE] 纸面实盘流程完成。
pause
endlocal
