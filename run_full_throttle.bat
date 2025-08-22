@echo off
cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM 建议：先试安全档（模型并发=1），观察显存
python -u tools\run_all_in_one_full.py --db "D:\quant_system_v2\data\market_data.db" --days 90 --symbol BTCUSDT --topk 40 --workers 8 --model-workers 1 --omp-threads 4
pause
