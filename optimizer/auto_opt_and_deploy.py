# optimizer/auto_opt_and_deploy.py
"""
Auto-optimizer (grid search) + deploy best params to JSON & DB.
- Loader: SQLite OHLCV tables 'SYMBOL_TF' with columns ts, open, high, low, close, volume
- Strategy: baseline MA cross (extendable)
Usage:
python optimizer/auto_opt_and_deploy.py --db D:\path\market_data.db --symbols BTCUSDT ETHUSDT --timeframes 1h 4h \
    --days 180 --deploy --json deploy\live_best_params.json
"""
from __future__ import annotations
import argparse, sqlite3, pandas as pd, numpy as np, os, json, itertools, math
from typing import Dict, Any, List
from backtest.backtest_engine_pro import simulate, BTConfig
from utils.param_loader import save_best_params_to_db, save_best_params_to_json

def load_from_db(db_path: str, table: str, days: int):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT ts, open, high, low, close, volume FROM '{table}' ORDER BY ts ASC", con)
    con.close()
    if df.empty: return df
    if days > 0:
        tail_ts = df['ts'].iloc[-1] - days*24*3600*1000
        df = df[df['ts'] >= tail_ts]
    df['time'] = pd.to_datetime(df['ts'], unit='ms')
    return df

def ma_cross_signal(df, fast: int, slow: int):
    if fast >= slow: return pd.Series([0]*len(df), index=df.index)
    f = df['close'].rolling(fast, min_periods=fast).mean()
    s = df['close'].rolling(slow, min_periods=slow).mean()
    sig = (f > s).astype(int)  # {0,1}
    sig = sig.fillna(0).astype(int)
    return sig

def run_grid(df, grid, cfg: BTConfig):
    best = None
    for fast, slow in grid:
        sig = ma_cross_signal(df, fast, slow)
        df2 = df.copy()
        df2['signal'] = sig
        res = simulate(df2, "signal", cfg)
        score = res['return']  # primary metric
        item = {"params": {"fast": int(fast), "slow": int(slow)}, "metrics": {"return": float(score), "trades": int(res['n_trades'])}, "res": res}
        if (best is None) or (score > best['metrics']['return']):
            best = item
    return best

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols", nargs="*", default=[])
    ap.add_argument("--symbols-file")
    ap.add_argument("--timeframes", nargs="*", default=["1h"])
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--fee", type=float, default=0.0005)
    ap.add_argument("--slip", type=float, default=0.0)
    ap.add_argument("--lev", type=float, default=1.0)
    ap.add_argument("--deploy", action="store_true")
    ap.add_argument("--json", default="deploy/live_best_params.json")
    args = ap.parse_args()

    # symbols
    syms = [s.strip().upper() for s in args.symbols if s.strip()]
    if args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t and not t.startswith("#"):
                    syms.append(t.upper())
    syms = sorted(set(syms))

    cfg = BTConfig(fee_rate=args.fee, slippage=args.slip, leverage=args.lev)

    # grid
    fasts = list(range(5, 41, 5))
    slows = list(range(30, 201, 10))
    grid = [(f, s) for f in fasts for s in slows if f < s]

    best_items = []
    for sym in syms:
        for tf in args.timeframes:
            table = f"{sym}_{tf}"
            df = load_from_db(args.db, table, args.days)
            if df.empty or len(df) < 260:  # not enough data
                print(f"[SKIP] {table} 数据不足")
                continue
            best = run_grid(df, grid, cfg)
            if best is None: continue
            item = {"symbol": sym, "tf": tf, "strategy": "ma_cross", "params": best["params"], "metrics": best["metrics"]}
            best_items.append(item)
            print(f"[BEST] {sym} {tf} {item['params']} ret={item['metrics']['return']*100:.2f}% trades={item['metrics']['trades']}")

    if args.deploy and best_items:
        os.makedirs(os.path.dirname(args.json) or ".", exist_ok=True)
        save_best_params_to_json(best_items, args.json)
        save_best_params_to_db(args.db, best_items)
        print(f"[DEPLOYED] 写入 JSON: {args.json} 以及 DB 表 best_params")

if __name__ == "__main__":
    main()
