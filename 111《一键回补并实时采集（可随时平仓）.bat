@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION

REM ====================== 基本参数（按需修改） ======================
set BASEDIR=D:\quant_system_pro (3)\quant_system_pro
set DB=D:\quant_system_v2\data\market_data.db
set SYMBOLS=results\keep_symbols.txt
set TFS=5m 15m 30m 1h 2h

REM —— 回补参数（稳健） ——
set BF_DAYS=365
set BF_LIMIT=1000
set BF_WORKERS=2

REM —— 守护实时（自调并发）参数 ——
set GUARD_LIMIT=750
set GUARD_WORKERS_START=2
set GUARD_WORKERS_MIN=1
set GUARD_WORKERS_MAX=6
set GUARD_KINT_START=45
set GUARD_KINT_MIN=20
set GUARD_KINT_MAX=90
set GUARD_QINT_START=5
set GUARD_QINT_MIN=2
set GUARD_QINT_MAX=15
set GUARD_WINDOW=60
set GUARD_ERRTH=0.05
set GUARD_COOLDOWN=90

REM ====================== 切到工程目录 ======================
cd /d "%BASEDIR%" || (
  echo 无法进入目录：%BASEDIR%
  pause & exit /b 2
)

echo ╔══════════════════════════════════════════════════════════════╗
echo ║  🚀 步骤 1/2：开始历史回补（先补齐，后实时）                      ║
echo ╚══════════════════════════════════════════════════════════════╝

python tools\backfill_until_covered.py ^
  --db "%DB%" ^
  --symbols-file "%SYMBOLS%" ^
  --tfs %TFS% ^
  --days %BF_DAYS% ^
  --limit %BF_LIMIT% ^
  --max-workers %BF_WORKERS%

if errorlevel 1 (
  echo ⚠️ 回补脚本返回非零退出码，请检查上方日志（可能是网络/配额）。
  echo 继续尝试进入“守护实时”阶段…
)

echo.
echo ╔══════════════════════════════════════════════════════════════╗
echo ║  🛡 步骤 2/2：启动守护实时（新窗口、可随时平仓）                  ║
echo ╚══════════════════════════════════════════════════════════════╝

REM 以**独立窗口**启动实时守护，窗口最小化运行；随时可单独关闭该窗口停止采集。
start "◉ 守护实时（可随时平仓）" /MIN cmd /v /c ^
 "python tools\auto_guard_runner.py ^
   --db \"%DB%\" ^
   --symbols-file \"%SYMBOLS%\" ^
   --tfs %TFS% ^
   --limit %GUARD_LIMIT% ^
   --workers-start %GUARD_WORKERS_START% --workers-min %GUARD_WORKERS_MIN% --workers-max %GUARD_WORKERS_MAX% ^
   --k-interval-start %GUARD_KINT_START% --k-interval-min %GUARD_KINT_MIN% --k-interval-max %GUARD_KINT_MAX% ^
   --q-interval-start %GUARD_QINT_START%  --q-interval-min %GUARD_QINT_MIN%  --q-interval-max %GUARD_QINT_MAX% ^
   --window-sec %GUARD_WINDOW% --err-threshold %GUARD_ERRTH% --cooldown-sec %GUARD_COOLDOWN%"

echo.
echo ✅ 已在新窗口启动：『◉ 守护实时（可随时平仓）』。您可以继续在当前窗口做交易/平仓操作。
echo    如需停止采集：关闭该窗口，或运行《一键停止采集.bat》。
endlocal


:: ============== 另存为：一键停止采集.bat（单独文件） ==============
:: 以下内容请复制到新的文件：一键停止采集.bat
:: ---------------------------------------------------------------
:: @echo off
:: chcp 65001 >nul
:: echo 正在停止：◉ 守护实时（可随时平仓） ...
:: taskkill /FI "WINDOWTITLE eq ◉ 守护实时（可随时平仓）" /T /F >nul 2>nul
:: if errorlevel 1 (
::   echo 没找到窗口，可能已手动关闭；如仍在运行，请检查任务管理器中的 python 进程。
:: ) else (
::   echo ✅ 已发送结束信号。
:: )
:: pause
