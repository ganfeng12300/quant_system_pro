@echo off
setlocal enabledelayedexpansion

REM === 设置数值库多线程 ===
set OMP_NUM_THREADS=16
set MKL_NUM_THREADS=16
set NUMEXPR_MAX_THREADS=16
set CUDA_VISIBLE_DEVICES=0
set PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:64

REM === 工作目录 ===
cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM === 配置参数 ===
set DB=D:\quant_system_v2\data\market_data.db
set DAYS=90
set TOPK=40
set SYMBOL=BTCUSDT
set OUTDIR=results\full_run

echo.
echo ┌─────────────────────────────────────────────────────────────┐
echo │      机构级一键跑满 A1–A8 （指标并行，模型串行）           │
echo └─────────────────────────────────────────────────────────────┘
echo.

REM === 指标类 A1–A4 并行跑 ===
for %%S in (A1 A2 A3 A4) do (
    start "STRAT %%S" cmd /c python -u backtest\backtest_pro.py --db "%DB%" --days %DAYS% --topk %TOPK% --outdir "%OUTDIR%" --symbols %SYMBOL% --only-strategy %%S
)
REM 等用户按键继续（确保并行完成后再跑模型）
pause

REM === 模型类 A5–A8 串行跑 ===
for %%S in (A5 A6 A7 A8) do (
    echo.
    echo [RUN] %%S
    python -u backtest\backtest_pro.py --db "%DB%" --days %DAYS% --topk %TOPK% --outdir "%OUTDIR%" --symbols %SYMBOL% --only-strategy %%S
)

echo.
echo ┌──────────────────────────┐
echo │   ✅ 全部 A1–A8 完成    │
echo └──────────────────────────┘
echo.
pause
