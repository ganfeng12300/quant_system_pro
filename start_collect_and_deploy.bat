@echo off
setlocal ENABLEDELAYEDEXPANSION

REM ==== 用户可改的 2 个变量 ====
set "QS_ROOT=D:\quant_system_pro (3)"
set "DB_PATH=D:\quant_data\market_data.db"

cd /d "%QS_ROOT%"

REM Python虚拟环境（可选）
REM call venv\Scripts\activate

REM 1) 采集器（历史+实时+ticks）
start "Collector" cmd /k python tools\live_collector_pro.py --db "%DB_PATH%" --exchange binance ^
  --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT ADAUSDT ^
  --timeframes 5m 15m 30m 1h 2h 4h 1d ^
  --backfill-days 365 --interval 30 --tick-interval 2 --max-workers 8

REM 2) 参数寻优 + 自动部署（写 JSON + best_params 表）
start "Optimizer+Deploy" cmd /k python optimizer\auto_opt_and_deploy.py --db "%DB_PATH%" ^
  --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT ADAUSDT --timeframes 1h 4h --days 180 --deploy --json deploy\live_best_params.json

REM 3) 演示：实盘端轮询读取最佳参数（你也可以在真实执行器里调用 utils.param_loader）
start "Params Watch" cmd /k python live_trading\params_watch_demo.py --db "%DB_PATH%" --json deploy\live_best_params.json --symbol BTCUSDT --tf 1h

echo 已启动三个窗口：采集 / 优化部署 / 参数观察
