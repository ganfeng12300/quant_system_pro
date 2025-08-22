@echo off
cd /d "D:\quant_system_pro (3)\quant_system_pro"
python -u tools\run_all_in_one.py --db "D:\quant_system_v2\data\market_data.db" --days 90 --symbol BTCUSDT --topk 40 --workers 3 --threads 16
pause
