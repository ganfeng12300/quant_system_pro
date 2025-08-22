@echo off
chcp 65001 >nul
title ▶ 今日全流程 · 采集→回测→Paper→仪表盘→（可选）实盘
setlocal EnableDelayedExpansion

REM === 全局参数（按需修改） ===
set ROOT=D:\quant_system_pro (3)\quant_system_pro
set QS_DB=D:\quant_system_v2\data\market_data.db
set MAX_WORKERS=8
set INTERVAL=30
set BACKFILL_DAYS=365
set TOPK=40
set PAPER_UI_ROWS=30
set RISK_CAPITAL=100
set MAX_ORDER_PCT=5
set LEVERAGE=5

cd /d "%ROOT%"

echo.
echo ╔══════════════════════════════════════════════════════════════╗
echo ║ 🚀 今日全流程（机构级）：                                     ║
echo ║  1) 数据采集（带横幅） → 持续补齐+实时                         ║
echo ║  2) 全币种回测寻优（365d） → 产出 best_params                 ║
echo ║  3) 启动 PaperTrading 执行引擎（沙箱）                         ║
echo ║  4) 启动 Web 仪表盘（实时可视化）                              ║
echo ║  5) （可选）切换实盘 Bitget 小资金试单                         ║
echo ╚══════════════════════════════════════════════════════════════╝
echo.

REM === 0) 环境自检 ===
echo [CHECK] Python...
where python >nul 2>nul || (echo [ERROR] 未找到 python（未加 PATH）。请安装/加入 PATH 后重试 & pause & exit /b 1)
echo [OK] Python 就绪。

if not exist logs mkdir logs

echo.
echo [INFO] 使用数据库：%QS_DB%
echo [INFO] 项目根路径：%ROOT%
echo [INFO] 并发：%MAX_WORKERS%  间隔：%INTERVAL%s  回补：%BACKFILL_DAYS%d
echo.
pause

REM === 1) 启动采集器（带横幅） + 总进度监视器 ===
echo.
echo ▶ 阶段一：启动采集器（带横幅）与总进度监视器
start "采集器" cmd /k ^
 "cd /d ""%ROOT%"" ^& python tools\rt_updater_with_banner.py --db ""%QS_DB%"" --backfill-days %BACKFILL_DAYS% --max-workers %MAX_WORKERS% --interval %INTERVAL%"

start "总进度监视器" cmd /k ^
 "cd /d ""%ROOT%"" ^& python tools\show_total_progress.py --db ""%QS_DB%"" --days 365 --refresh 30 --topk 20"

echo [TIP] 让采集先跑起来（建议 ≥98%% 覆盖）。随时可看“总进度监视器”窗口。
echo.
pause

REM === 2) 启动回测寻优（365天，全币种，多周期） ===
echo.
echo ▶ 阶段二：启动回测寻优（365d，全币种，多周期）
start "回测寻优" cmd /k ^
 "cd /d ""%ROOT%"" ^& python tools\progress_wrap_and_run_plus.py --workdir ""%ROOT%"" --cmd ""python -u tools\inject_and_run.py backtest\backtest_pro.py --db %QS_DB% --days 365 --topk %TOPK% --outdir results"" --results-dir results --top 10"

echo [TIP] 回测完成后会在 results 目录产出策略评分表 / 报告；最优参数会写入 JSON/DB（供实盘调用）。
echo.
pause

REM === 3) PaperTrading 执行引擎（沙箱） ===
echo.
echo ▶ 阶段三：启动 PaperTrading 执行引擎（沙箱，实时信号→模拟下单）
start "PaperTrading" cmd /k ^
 "cd /d ""%ROOT%"" ^& python live_trading\execution_engine_binance_ws.py --db ""%QS_DB%"" --mode paper --ui-rows %PAPER_UI_ROWS%"

echo [TIP] 终端会显示下单/平仓与盈亏统计；对照回测结果，观察滑点/延迟影响。
echo.
pause

REM === 4) 启动 Web 仪表盘 ===
echo.
echo ▶ 阶段四：启动 Web 仪表盘（持仓、盈亏、账户汇总）
start "仪表盘" cmd /k ^
 "cd /d ""%ROOT%"" ^& python dashboard\live_monitor.py --db ""%QS_DB%"""

echo [TIP] 打开浏览器查看本地端口（脚本输出会提示），观察实时可视化。
echo.
pause

REM === 5) 维护操作（选做）：WAL checkpoint + VACUUM ===
choice /M "是否执行一次数据库维护（WAL Checkpoint + VACUUM）以释放体积"
if errorlevel 2 goto SKIP_MAINT
echo.
echo ▶ 维护：WAL Checkpoint + VACUUM
python - <<PY
import sqlite3
db = r"%QS_DB%"
con = sqlite3.connect(db, timeout=30)
cur = con.cursor()
print("[PRAGMA] journal_mode=", cur.execute("PRAGMA journal_mode").fetchone())
print("[ACTION] wal_checkpoint(TRUNCATE) ...")
cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
con.commit()
print("[ACTION] VACUUM ...")
cur.execute("VACUUM")
con.commit()
con.close()
print("[OK] 完成。")
PY
:SKIP_MAINT
echo.
pause

REM === 6) （可选）切换实盘 Bitget 小资金试单 ===
echo.
choice /M "是否切换到 Bitget 真实实盘（小资金试单 10U~100U）"
if errorlevel 2 goto END
echo.
echo ▶ 阶段五：Bitget 实盘（小资金试单）
echo [WARN] 请确认 configs\settings.yaml 已填入 Bitget API（仅下单权限），并已先跑过 PaperTrading 验证。
echo [TIP] 默认资金=%RISK_CAPITAL%U  单笔上限=%MAX_ORDER_PCT%%%  杠杆=%LEVERAGE%x
start "Bitget 实盘" cmd /k ^
 "cd /d ""%ROOT%"" ^& python live_trading\execution_engine.py --db ""%QS_DB%"" --exchange bitget --mode real --risk-capital %RISK_CAPITAL% --max-order-pct %MAX_ORDER_PCT% --leverage %LEVERAGE%"

echo.
echo [OK] 已启动实盘窗口。建议先用更小资金（例如 10U）试单，确认止盈止损与风控无误后再逐步加到 100U。
echo.
pause

:END
echo.
echo ╔════════════════════════════════════════════════════╗
echo ║ 🎯 今日流程已全部发车：                              ║
echo ║   采集器 + 总进度监视 + 回测寻优 + Paper + 仪表盘   ║
echo ║   （如已选择）Bitget 小资金实盘试单                 ║
echo ╚════════════════════════════════════════════════════╝
echo.
echo [TIP] 需要收尾可运行：一键_停止全部.bat
echo.
pause
exit /b 0
