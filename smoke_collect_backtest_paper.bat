@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title ▶ 最小冒烟测试：采集→回测→部署→纸面实盘

rem ======== 可调整变量 ========
set DB=D:\quant_system_v2\data\market_data.db
set SYMBOL=BTCUSDT
set TF=30m
set DAYS=30
set INTERVAL=30
set MAX_WORKERS=4
set OUTDIR=results
rem ============================

echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║ 🚀 最小冒烟测试启动：%SYMBOL% / %TF% / %DAYS%天                          ║
echo ║ DB: %DB%                                                       ║
echo ╚════════════════════════════════════════════════════════════════╝
echo.

rem 0) 目录与 Python 检查
where python >nul 2>nul || (echo [FATAL] 未发现 python，请先安装并加入 PATH & goto :end)

if not exist "%DB%" (
  echo [WARN] 目标数据库不存在：%DB%
  echo        将由采集器首次创建。
)

rem 1) 采集：开一个独立窗口【持续运行】，先补齐%DAYS%天，再转实时
echo.
echo [1/4] 启动采集（补齐 %DAYS% 天 → 实时，单独窗口常驻）...
start "COLLECTOR · %SYMBOL% %TF%" cmd /k ^
  python tools\rt_updater_with_banner.py ^
    --db "%DB%" ^
    --symbols %SYMBOL% ^
    --tfs %TF% ^
    --backfill-days %DAYS% ^
    --interval %INTERVAL% ^
    --max-workers %MAX_WORKERS%

echo    → 已在新窗口启动采集器，等待数据写入...
echo    → 稍等 10 秒再做 DB 校验。
timeout /t 10 >nul

rem 1a) 校验：表是否已写入
echo [校验] 检查表 %SYMBOL%_%TF% 是否存在并有数据...
python -c "import sqlite3,sys; db=r'%DB%'; t=f'%s_%s'.replace(':','_')%%(r'%SYMBOL%',r'%TF%'); con=sqlite3.connect(db); cur=con.cursor(); \
cur.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name=?\",(t,)); \
ok=cur.fetchone() is not None; \
n=0; \
print('[INFO] 表名:',t); \
if ok: \
  row=cur.execute(f\"SELECT COUNT(*) FROM '{t}'\").fetchone(); n=row[0]; \
  print('[OK] 表存在，行数=',n); \
else: \
  print('[ERR] 表不存在'); \
sys.exit(0 if ok and n>0 else 1)" || (echo [WARN] 首次写入可能未完成，将继续后续步骤；回测若读不到数据会报错。)

pause

rem 2) 回测：读取 %DAYS% 天，输出到 %OUTDIR%
echo.
echo [2/4] 回测（%SYMBOL% / %TF% / %DAYS% 天）...
python backtest\backtest_pro.py ^
  --db "%DB%" ^
  --symbols %SYMBOL% ^
  --tfs %TF% ^
  --days %DAYS% ^
  --outdir "%OUTDIR%" ^
  --topk 1
if errorlevel 1 (
  echo [FATAL] 回测失败，请查看上方错误信息。
  goto :end
)
echo [OK] 回测完成，产物位于 %OUTDIR%\
pause

rem 3) 部署：写入 deploy\live_best_params.json（供实盘读取）
echo.
echo [3/4] 部署最优参数到 deploy\live_best_params.json ...
python optimizer\a1a8_optimizer_and_deploy.py ^
  --db "%DB%" ^
  --symbols %SYMBOL% ^
  --tfs %TF% ^
  --days %DAYS% ^
  --deploy
if errorlevel 1 (
  echo [FATAL] 部署失败，请查看上方错误信息。
  goto :end
)

rem 3a) 校验部署 JSON 中包含该 symbol+tf
python -c "import json,sys,os; p=r'deploy\live_best_params.json'; \
print('[INFO] 检查',p); \
j=json.load(open(p,'r',encoding='utf-8')); \
key=(r'%SYMBOL%', r'%TF%'); \
ok=False; \
for it in j if isinstance(j,list) else j.get('items',[]): \
  sym=it.get('symbol') or it.get('sym'); tf=it.get('tf') or it.get('timeframe'); \
  if (sym,tf)==key: ok=True; \
print('[OK]' if ok else '[ERR]','已部署条目包含目标=',ok); \
sys.exit(0 if ok else 1)" || (echo [WARN] 未在 JSON 中发现 %SYMBOL%/%TF%，实盘可能回退到默认策略。)

pause

rem 4) 纸面实盘：独立窗口运行，读取部署参数并实时给出信号/开平仓
echo.
echo [4/4] 启动纸面实盘（独立窗口常驻，可随时平仓/停止）...
start "PAPER · %SYMBOL% %TF%" cmd /k ^
  python live_trading\execution_engine_binance_ws.py ^
    --db "%DB%" ^
    --mode paper ^
    --ui-rows 30

echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║ ✅ 冒烟链路已全部触发：                                     ║
echo ║   1) 采集窗口：COLLECTOR（持续运行）                         ║
echo ║   2) 回测：已完成                                            ║
echo ║   3) 部署：已写入 deploy\live_best_params.json               ║
echo ║   4) 纸面实盘窗口：PAPER（实时运行，可随时平仓 / Ctrl+C 停止）║
echo ╚════════════════════════════════════════════════════════════════╝

:end
echo.
echo [DONE] 如需终止采集或纸面实盘，请切到对应窗口按 Ctrl+C。
endlocal
