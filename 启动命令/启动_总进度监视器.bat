@echo off
setlocal
title ▶ 回补总进度监视器（近365天）
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

python tools\show_total_progress.py --db "%QS_DB%" --days 365 --refresh 30 --topk 20
