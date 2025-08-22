@echo off
setlocal
title ▶ 一键回测+寻优 · 365d
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

REM TopK 可调；仅示例——若你有专用run_backtest_all.py可直接替换调用
python tools\progress_wrap_and_run_plus.py ^
  --workdir "%CD%" ^
  --cmd "python -u tools\inject_and_run.py backtest\backtest_pro.py --db %QS_DB% --days 365 --topk 40 --outdir results" ^
  --results-dir results ^
  --top 10

pause
