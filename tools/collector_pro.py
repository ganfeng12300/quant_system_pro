# -*- coding: utf-8 -*-
"""
采集层（Binance USDT 永续连续合约）：覆盖率体检、历史补齐、可选守护。
- 历史：REST continuousKlines 分页拉取（每批<=1500）
- 存储：SQLite WAL，表名 {SYMBOL}_{TF}，字段 [ts, open, high, low, close, volume]
"""
import argparse, time, os, math, requests, pandas as pd
from tools.db_util import connect_ro, connect_rw, ensure_index, table_exists, console, count_rows
from tools.config import get_db_path

BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/continuousKlines"  # 合约连续永续
TFS = ["5m","15m","30m","1h","2h","4h","1d"]
SYMBOLS_DEFAULT = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","LTCUSDT"]

def _fetch_klines(sym, tf, start_ms=None, end_ms=None, limit=1500):
    params = {
        "pair": sym,
        "contractType": "PERPETUAL",
        "interval": tf,
        "limit": min(1500, int(limit)),
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    r = requests.get(BINANCE_KLINES, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return pd.DataFrame()
    cols = ["open_time","open","high","low","close","volume","close_time","qav","trades","tb_base","tb_quote","ignore"]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    out = pd.DataFrame({
        "ts": df["open_time"].astype("int64"),
        "open": pd.to_numeric(df["open"], errors="coerce"),
        "high": pd.to_numeric(df["high"], errors="coerce"),
        "low": pd.to_numeric(df["low"], errors="coerce"),
        "close": pd.to_numeric(df["close"], errors="coerce"),
        "volume": pd.to_numeric(df["volume"], errors="coerce"),
    })
    return out

def _write_df(con, table, df):
    if df is None or df.empty: 
        return 0
    df = df.dropna()
    if df.empty:
        return 0
    con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (ts INTEGER PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL)')
    df.to_sql(table, con, if_exists="append", index=False)
    ensure_index(con, table)
    return len(df)

def backfill(db, symbols=None, days=365):
    symbols = list(symbols or SYMBOLS_DEFAULT)
    since = int(time.time()*1000) - int(days)*24*3600*1000
    with connect_rw(db) as con:
        for s in symbols:
            for tf in TFS:
                tb = f"{s}_{tf}"
                # 读最新 ts
                try:
                    if table_exists(con, tb):
                        last = con.execute(f'SELECT IFNULL(MAX(ts),0) FROM "{tb}"').fetchone()[0]
                    else:
                        last = 0
                except Exception:
                    last = 0
                start = max(since, int(last) + 1)
                console.print(f"[cyan][Backfill][/cyan] {s} {tf} 从 {start} 开始回补…")
                total = 0
                while True:
                    df = _fetch_klines(s, tf, start_ms=start, limit=1500)
                    if df is None or df.empty:
                        break
                    # 去重 + 仅新数据
                    if table_exists(con, tb):
                        max_ts = con.execute(f'SELECT IFNULL(MAX(ts),0) FROM "{tb}"').fetchone()[0]
                        df = df[df["ts"] > int(max_ts)]
                        df = df.drop_duplicates(subset=["ts"])
                    wrote = _write_df(con, tb, df)
                    total += int(wrote)
                    if wrote == 0:
                        break
                    start = int(df["ts"].iloc[-1]) + 1
                    # 如果不到满批次，说明已到末尾
                    if len(df) < 1500:
                        break
                console.print(f"[green]完成[/green] {s} {tf} 累计新增 {total} 行")

def coverage_report(db, days=365, symbols=None):
    symbols = symbols or SYMBOLS_DEFAULT
    console.rule("[bold]覆盖率体检[/bold]")
    with connect_rw(db) as con:
        for s in symbols:
            line = [s]
            for tf in TFS:
                tb = f"{s}_{tf}"
                n = count_rows(con, tb) if table_exists(con, tb) else 0
                line.append(f"{tf}:{n}")
            console.print("  ".join(line))

def daemon(db, sleep=30):
    # 简易守护：每次仅补齐最近一批
    console.print("[bold yellow]实时守护启动[/bold yellow]（每 %ss 补齐一次）" % sleep)
    while True:
        try:
            backfill(db, symbols=SYMBOLS_DEFAULT, days=3)
        except Exception as e:
            console.print(f"[red]守护异常：{e}[/red]")
        time.sleep(int(sleep))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=get_db_path())
    ap.add_argument("--symbols-file")
    ap.add_argument("--backfill-days", type=int, default=365)
    ap.add_argument("--start-daemon", type=int, default=0)
    args = ap.parse_args()
    syms = None
    if args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            syms = [x.strip().upper() for x in f if x.strip()]
    coverage_report(args.db, days=args.backfill_days, symbols=syms)
    backfill(args.db, symbols=syms, days=args.backfill_days)
    if args.start_daemon:
        daemon(args.db)
