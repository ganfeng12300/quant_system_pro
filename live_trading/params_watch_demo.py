# live_trading/params_watch_demo.py
"""
Demo: live engine periodically reads best params (JSON + DB) for routing.
This is a reader-only loop for integration testing.
"""
import time, argparse, json, os
from utils.param_loader import load_best_params_from_json, load_best_params_from_db, get_best_for

ap = argparse.ArgumentParser()
ap.add_argument("--db")
ap.add_argument("--json", default="deploy/live_best_params.json")
ap.add_argument("--symbol", default="BTCUSDT")
ap.add_argument("--tf", default="1h")
ap.add_argument("--strategy", default="ma_cross")
args = ap.parse_args()

while True:
    it = get_best_for(args.symbol, args.tf, args.strategy, db_path=args.db, json_path=args.json)
    if it:
        print("[BEST]", it)
    else:
        print("[BEST] 暂无记录")
    time.sleep(5)
