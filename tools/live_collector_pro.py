# tools/live_collector_pro.py
"""
S‑grade History + Realtime collector (Binance/Bitget via CCXT polling + optional fast tick polling).
- Backfills N days for multiple timeframes
- Periodic updates every --interval seconds
- Writes to a single SQLite DB with WAL
- Adds 'ticks' table for sub-minute "last price" to support instant close
Usage (binance example):
  python -m tools.live_collector_pro --db D:\quant_system_v2\data\market_data.db --exchange binance \
      --symbols BTCUSDT ETHUSDT BNBUSDT --timeframes 5m 15m 1h 4h 1d --backfill-days 365 --interval 30 --tick-interval 2

Dependencies: pip install ccxt pyyaml
"""
from __future__ import annotations
import argparse, time, math, threading, traceback, sys
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import ccxt
from utils.db import SQLite

TF_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

def now_ms():
    return int(time.time() * 1000)

def align_ms(ts_ms: int, tf: str) -> int:
    step = TF_MS[tf]
    return (ts_ms // step) * step

def make_table(symbol: str, tf: str) -> str:
    return f"{symbol}_{tf}"

def fetch_ohlcv(exchange, symbol: str, tf: str, since_ms: int, limit: int = 1500):
    # ccxt unified call
    return exchange.fetch_ohlcv(symbol, timeframe=tf, since=since_ms, limit=limit)

def backfill(db: SQLite, exchange, symbol: str, tf: str, days: int):
    step = TF_MS[tf]
    end = now_ms()
    since = end - days * 24 * 3600 * 1000
    table = make_table(symbol, tf)
    db.ensure_ohlcv_table(table)
    # Walk in chunks backwards
    cursor = since
    while cursor < end:
        batch = fetch_ohlcv(exchange, symbol, tf, since_ms=cursor, limit=1500)
        if not batch:
            break
        rows = []
        for ts, o,h,l,c,v in batch:
            rows.append((int(ts), float(o), float(h), float(l), float(c), float(v)))
        db.upsert_ohlcv(table, rows)
        cursor = batch[-1][0] + step

def do_updates(db: SQLite, exchange, symbol: str, tf: str):
    table = make_table(symbol, tf)
    step = TF_MS[tf]
    # pull the last 2*limit window to cover any missed bars
    since = now_ms() - 300 * step
    batch = fetch_ohlcv(exchange, symbol, tf, since_ms=since, limit=300)
    rows = []
    for ts, o,h,l,c,v in batch:
        rows.append((int(ts), float(o), float(h), float(l), float(c), float(v)))
    db.upsert_ohlcv(table, rows)

def tick_loop(db: SQLite, exchange, symbols: list[str], tick_interval: int):
    # simple ticker polling to keep an up-to-date last price
    while True:
        t0 = now_ms()
        try:
            for s in symbols:
                try:
                    ticker = exchange.fetch_ticker(s)
                    price = float(ticker["last"] or ticker["close"] or 0.0)
                    if price > 0:
                        db.upsert_tick(s, now_ms(), price)
                except Exception:
                    pass
        except Exception:
            pass
        dt = max(0, tick_interval - (now_ms() - t0)//1000)
        time.sleep(dt)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--exchange", choices=["binance", "bitget"], default="binance")
    ap.add_argument("--symbols", nargs="*", help="Symbols, space-separated")
    ap.add_argument("--symbols-file", help="Text file with one symbol per line")
    ap.add_argument("--timeframes", nargs="*", default=["5m","15m","30m","1h","2h","4h","1d"])
    ap.add_argument("--backfill-days", type=int, default=365)
    ap.add_argument("--interval", type=int, default=30, help="seconds between update rounds")
    ap.add_argument("--tick-interval", type=int, default=2, help="seconds for last-price polling")
    ap.add_argument("--max-workers", type=int, default=8)
    args = ap.parse_args()

    # symbols
    syms = list(args.symbols or [])
    if args.symbols_file:
        try:
            with open(args.symbols_file, "r", encoding="utf-8") as f:
                for line in f:
                    t = line.strip()
                    if t and not t.startswith("#"):
                        syms.append(t)
        except Exception:
            pass
    syms = sorted(set([s.strip().upper() for s in syms if s.strip()]))

    # exchange
    if args.exchange == "binance":
        ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    else:
        ex = ccxt.bitget({"enableRateLimit": True})

    db = SQLite(args.db)

    print(f"[INFO] DB: {args.db}")
    print(f"[INFO] EXCHANGE: {args.exchange} | SYMBOLS: {len(syms)} | TF: {args.timeframes}")
    print(f"[INFO] Backfill: {args.backfill_days} days | Update interval: {args.interval}s | Tick interval: {args.tick_interval}s")

    # 1) Backfill
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = []
        for s in syms:
            for tf in args.timeframes:
                futures.append(pool.submit(backfill, db, ex, s, tf, args.backfill_days))
        for i, fu in enumerate(as_completed(futures), 1):
            try:
                fu.result()
            except Exception as e:
                print("[WARN] backfill error:", e)

    # 2) Start tick thread
    th = threading.Thread(target=tick_loop, args=(db, ex, syms, args.tick_interval), daemon=True)
    th.start()

    # 3) Periodic updates
    while True:
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
            futs = []
            for s in syms:
                for tf in args.timeframes:
                    futs.append(pool.submit(do_updates, db, ex, s, tf))
            for fu in as_completed(futs):
                try: fu.result()
                except Exception: pass
        dt = time.time() - t0
        sleep = max(0, args.interval - dt)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 更新轮完成，耗时 {dt:.1f}s，休眠 {sleep:.1f}s")
        time.sleep(sleep)

if __name__ == "__main__":
    main()
