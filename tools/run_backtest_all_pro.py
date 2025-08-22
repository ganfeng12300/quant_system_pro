# tools/run_backtest_all_pro.py
"""
Demo wrapper to run S-grade engine against a CSV or SQLite source.
This is a template to replace your existing runner; adapt loader to your DB schema.
"""
import argparse, sqlite3, pandas as pd, os, json
from backtest.backtest_engine_pro import simulate, BTConfig

def load_from_db(db_path: str, table: str, days: int = 365):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT ts, open, high, low, close, volume FROM '{table}' ORDER BY ts ASC", con)
    con.close()
    if days > 0:
        tail_ts = df['ts'].iloc[-1] - days*24*3600*1000
        df = df[df['ts'] >= tail_ts]
    df['time'] = pd.to_datetime(df['ts'], unit='ms')
    return df

def simple_signal(df):
    # sample: MA cross
    ma_fast = df['close'].rolling(20, min_periods=20).mean()
    ma_slow = df['close'].rolling(60, min_periods=60).mean()
    sig = (ma_fast > ma_slow).astype(int)  # 1 or 0
    return sig

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--table", required=True, help="symbol_timeframe, e.g., BTCUSDT_1h")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--fee", type=float, default=0.0005)
    ap.add_argument("--slip", type=float, default=0.0)
    ap.add_argument("--lev", type=float, default=1.0)
    ap.add_argument("--allow-short", action="store_true")
    ap.add_argument("--out", default="result.json")
    args = ap.parse_args()

    df = load_from_db(args.db, args.table, args.days)
    df['signal'] = simple_signal(df)

    cfg = BTConfig(fee_rate=args.fee, slippage=args.slip, leverage=args.lev, allow_short=args.allow_short)
    res = simulate(df, "signal", cfg)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    print(f"[OK] trades={res['n_trades']} return={res['return']*100:.2f}% out={args.out}")

if __name__ == "__main__":
    main()
