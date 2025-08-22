@echo off
title 🚀 Bitget 实盘启动器 · LIVE
cd /d "%~dp0\.."

REM === 用户在这里填写自己的 API 信息 ===
set API_KEY=你的APIKEY
set API_SECRET=你的SECRET
set API_PASSPHRASE=你的PASSPHRASE

REM === 启动参数 ===
set SYMBOLS_FILE=Q:\config\symbols_pilot.txt
set LOGDIR=Q:\logs\live
set TIMEFRAMES=1h 4h

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

echo.
echo =====================================================
echo  ⚠️  即将启动 [真实实盘] · Bitget
echo  币种清单: %SYMBOLS_FILE%
echo  时间周期: %TIMEFRAMES%
echo  日志目录: %LOGDIR%
echo =====================================================
echo.

set /p CONFIRM="请输入 LIVE 确认启用实盘（输入LIVE才会下单）： "
if /i not "%CONFIRM%"=="LIVE" (
    echo ❌ 已取消启动。
    pause
    exit /b
)

echo ✅ 确认成功，实盘进程启动中...
echo.

REM === 实盘执行脚本（live_trader_pro.py） ===
for /f "usebackq tokens=*" %%S in ("%SYMBOLS_FILE%") do (
    for %%T in (%TIMEFRAMES%) do (
        set SYM=%%S
        set TF=%%T
        set LOGFILE=%LOGDIR%\live_%%S_%%T_%DATE:~0,10%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log
        echo ▶ 启动 %%S %%T → !LOGFILE!
        start "LIVE %%S %%T" cmd /c ^
        "python live_trading\live_trader_pro.py --exchange bitget --symbol %%S --timeframe %%T --live 1 > "!LOGFILE!" 2>&1"
    )
)

echo.
echo =====================================================
echo  🎯 所有实盘进程已启动，日志位于 %LOGDIR%
echo =====================================================
echo.
pause
