@echo off
setlocal enabledelayedexpansion

REM === 工作目录（按需修改到你的项目根） ===
cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM === 依赖（若已装会跳过） ===
python -m pip install --upgrade pip >nul 2>&1
python -m pip install colorama pyfiglet >nul 2>&1

REM === 打补丁（可重复执行，自动跳过） ===
python -u tools\patch_only_strategy.py

REM === 数值库吃核 ===
set OMP_NUM_THREADS=16
set MKL_NUM_THREADS=16
set NUMEXPR_MAX_THREADS=16
set CUDA_VISIBLE_DEVICES=0
set PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:64

REM === 运行（可按需改 symbol/days/topk/workers/db 路径） ===
python -u tools\run_1symbol_parallel_strategies_safe.py ^
  --db "D:\quant_system_v2\data\market_data.db" ^
  --days 90 ^
  --symbol BTCUSDT ^
  --topk 40 ^
  --workers 3

pause
