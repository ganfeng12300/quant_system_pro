@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ====== 基本路径配置（按你现状默认，不用改）======
set PROJ=%~dp0
set PROJ=%PROJ:~0,-1%
set DB_MAIN=D:\quant_system_v2\data\market_data.db
set DB_SNAP=D:\quant_system_v2\data\market_data_snapshot.db

REM 可调参数
set BACKFILL_DAYS=365
set MAX_WORKERS=8
set RT_INTERVAL=30
set BTEST_DAYS=180
set BTEST_TOPK=40
set LIVE_TOP=20
set PAPER_OR_REAL=paper

REM ====== 准备日志目录 ======
if not exist "%PROJ%\logs" mkdir "%PROJ%\logs"
set START_LOG=%PROJ%\logs\starter_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log
echo ===================== 一键自检 + 启动日志 ===================== > "%START_LOG%"
echo 项目: %PROJ% >> "%START_LOG%"
echo 主库: %DB_MAIN% >> "%START_LOG%"
echo 快照: %DB_SNAP% >> "%START_LOG%"
echo. >> "%START_LOG%"

REM ====== 1) 自检 ======
echo [1/6] 运行系统自检...
python -u "%PROJ%\tools\system_diag_and_launch.py" ^
  --proj "%PROJ%" ^
  --db "%DB_MAIN%" ^
  --snapshot "%DB_SNAP%" ^
  --backfill-days %BACKFILL_DAYS% ^
  --max-workers %MAX_WORKERS% ^
  --rt-interval %RT_INTERVAL% ^
  --bt-days %BTEST_DAYS% ^
  --bt-topk %BTEST_TOPK% ^
  --live-top %LIVE_TOP% ^
  --mode %PAPER_OR_REAL% ^
  --log "%START_LOG%"
if errorlevel 2 (
  echo.
  echo [FATAL] 自检发现致命问题，已写入 %START_LOG%
  echo 请打开日志查看，修完再运行本启动器。
  goto :EOF
)

REM ====== 2) 生成快照（避免锁冲突）======
echo.
echo [2/6] 生成快照 DB ...
python -u "%PROJ%\tools\system_diag_and_launch.py" --make-snapshot --db "%DB_MAIN%" --snapshot "%DB_SNAP%" --log "%START_LOG%"
if errorlevel 1 (
  echo [WARN] 快照生成失败，继续，但建议排查（见 %START_LOG%）
)

REM ====== 3) 启动采集守护（独立窗口）======
echo.
echo [3/6] 启动采集（历史+实时）...
start "QS-Collector" cmd /k ^
  "cd /d "%PROJ%" ^&^& python -u tools\rt_updater_with_banner.py --db "%DB_MAIN%" --backfill-days %BACKFILL_DAYS% --max-workers %MAX_WORKERS% --interval %RT_INTERVAL% ^| tee logs\collector_live.log"

REM ====== 4) 回测寻优（基于快照，避免锁）======
echo.
echo [4/6] 启动回测（基于快照）...
start "QS-Backtest" cmd /k ^
  "cd /d "%PROJ%" ^&^& python -u backtest\backtest_pro.py --db "%DB_SNAP%" --days %BTEST_DAYS% --topk %BTEST_TOPK% --outdir results ^| tee logs\backtest_live.log"

REM ====== 5) 选币 + 最优参数导出 ======
echo.
echo [5/6] 导出选币与最优参数...
python -u "%PROJ%\tools\system_diag_and_launch.py" --emit-picks --emit-params --proj "%PROJ%" --log "%START_LOG%"

REM ====== 6) 启动实盘（paper/real）独立窗口 ======
echo.
echo [6/6] 启动实盘（%PAPER_OR_REAL%）...
set BEST_PARAMS=%PROJ%\deploy\live_best_params.json
if not exist "%BEST_PARAMS%" (
  echo [WARN] 未发现 %BEST_PARAMS% ，实盘将使用默认参数。>> "%START_LOG%"
)
start "QS-Live" cmd /k ^
  "cd /d "%PROJ%" ^&^& python -u live_trading\execution_engine_binance_ws.py --db "%DB_MAIN%" --mode %PAPER_OR_REAL% --best-params "%BEST_PARAMS%" --ui-rows 30 ^| tee logs\live_engine.log"

echo.
echo ✅ 已启动：采集 / 回测 / 选币&参数 / 实盘（详见多窗口与 logs\）
echo 💡 若需要停止全部：taskkill /F /IM python.exe
echo 🔎 自检报告：%START_LOG%
endlocal
