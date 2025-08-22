@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION
title 🌈 [S级桥接] A1-A8 -> DB/JSON/中文表（无侵入）

REM 固定路径
set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
cd /d "%QS_ROOT%"

REM 符号清单（优先用币安USDT永续；兜底用从DB扫出的）
set "SYM_FILE=results\symbols_binance_perp.txt"
if not exist "%SYM_FILE%" set "SYM_FILE=results\symbols_from_db.txt"

echo 🌈 [1/2] 启动桥接适配器（无侵入调用优化器并解析 [BEST]）…
python tools\bridge_best_results.py ^
  --db "%DB_PATH%" ^
  --symbols-file "%SYM_FILE%" ^
  --timeframes 1h 4h ^
  --days 180 ^
  --min-trades-default 10 ^
  --min-trades-1h 10 ^
  --min-trades-4h 10 ^
  --max-dd-cap 0.6 ^
  --fee-bps 5 --slip-bps 2 --exec-lag 1 --no-intrabar ^
  --json "deploy\live_best_params.json"

if errorlevel 1 (
  echo 🟥 失败：请查看 logs\opt_bridge_*.log 与 deploy\run_config.json
  pause & exit /b 2
)

echo 🌈 [2/2] 完成。将打开 deploy 目录与最新日志…
start "" explorer.exe "deploy"
for /f "tokens=*" %%F in ('dir /b /od "logs\opt_bridge_*.log"') do set "LAST=%%F"
if defined LAST start notepad "logs\!LAST!"
echo ✅ 全流程完成（可直接让实盘读取 deploy\live_best_params.json）
pause
