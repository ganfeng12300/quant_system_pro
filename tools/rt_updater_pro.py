# -*- coding: utf-8 -*-
"""
rt_updater_pro.py — 实时守护（Binance 永续），批量并发拉取 5m，自动增量写表 & 聚合出高周期
- 自动从 DB 扫描出 SYMBOL 列表（*_5m 表），也可 --symbols-file 指定清单
- 仅请求 5m（减少耗流量/限频风险），本地聚合 15m/30m/1h/2h/4h/1d
- 断点续传：读取各表 MAX(ts) 作为起点
- 并发 + 节流：线程池 + 每次循环 sleep，适配几百币
"""
import argparse, os, time, sqlite3
import requests, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from rich.console import Console
from rich.progress import Progress

console=Console()
BINANCE_KLINES="https://fapi.binance.com/fapi/v1/continuousKlines"
TF_MS={"5m":300000,"15m":900000,"30m":1800000,"1h":3600000,"2h":7200000,"4h":14400000,"1d":86400000}
TARGET_TFS=["15m","30m","1h","2h","4h","1d"]

def ensure_dir(p): os.makedirs(p, exist_ok=True); return p
def floor_ts(ts, tf_ms): return (ts//tf_ms)*tf_ms

def list_symbols_from_db(db):
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    syms=set()
    for r in rows:
        n=r["name"]
        if n.endswith("_5m") and len(n)>3:
            syms.add(n[:-3])
    return sorted(syms)

def table_exists(con, tb):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (tb,)).fetchone() is not None

def get_last_ts(con, tb):
    try:
        r=con.execute(f'SELECT MAX(ts) FROM "{tb}"').fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception:
        return 0

def write_batch(con, tb, df):
    if df is None or df.empty: return 0
    con.execute(f'CREATE TABLE IF NOT EXISTS "{tb}" (ts INTEGER PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL)')
    con.executemany(f'INSERT OR IGNORE INTO "{tb}" (ts,open,high,low,close,volume) VALUES (?,?,?,?,?,?)',
                    list(map(tuple, df[["ts","open","high","low","close","volume"]].values)))
    return int(con.total_changes)

def fetch_5m(symbol, start_ms=None, limit=1500):
    params={"pair":symbol,"contractType":"PERPETUAL","interval":"5m","limit":min(1500,int(limit))}
    if start_ms is not None: params["startTime"]=int(start_ms)
    r=requests.get(BINANCE_KLINES, params=params, timeout=20)
    r.raise_for_status()
    data=r.json()
    if not isinstance(data,list): return pd.DataFrame()
    cols=["open_time","open","high","low","close","volume","close_time","qav","trades","tb_base","tb_quote","ignore"]
    df=pd.DataFrame(data, columns=cols)
    if df.empty: return df
    out=pd.DataFrame({
        "ts": df["open_time"].astype("int64"),
        "open": pd.to_numeric(df["open"], errors="coerce"),
        "high": pd.to_numeric(df["high"], errors="coerce"),
        "low":  pd.to_numeric(df["low"],  errors="coerce"),
        "close":pd.to_numeric(df["close"],errors="coerce"),
        "volume":pd.to_numeric(df["volume"],errors="coerce"),
    }).dropna()
    return out

def aggregate_from_5m(df5, tgt_tf):
    """
    稳健版聚合：严格 1D，避免 'Per-column arrays must each be 1-dimensional'
    - df5: 5m 数据（含 ts/open/high/low/close/volume），ts 毫秒
    - tgt_tf: '15m'/'30m'/'1h'/'2h'/'4h'/'1d'
    """
    if df5 is None or len(df5) == 0:
        return df5.iloc[0:0].copy()

    df = df5.copy()
    for c in ["ts", "open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = df["ts"].astype("int64")

    tf_ms = TF_MS[tgt_tf]
    df["bucket"] = (df["ts"] // tf_ms) * tf_ms

    agg = (
        df.groupby("bucket", as_index=False)
          .agg(
              open=("open", "first"),
              high=("high", "max"),
              low =("low",  "min"),
              close=("close","last"),
              volume=("volume","sum"),
          )
          .rename(columns={"bucket": "ts"})
          .sort_values("ts")
          .reset_index(drop=True)
    )
    return agg

def update_one_symbol(db, symbol, back_days=3, max_loops=12):
    """
    单次调用会尝试把 symbol 的 5m 表追到最新（近 back_days 天），并生成高周期
    """
    now_ms=int(time.time()*1000)
    win_start = now_ms - back_days*86400000
    with sqlite3.connect(db, timeout=60) as con:
        con.execute("PRAGMA journal_mode=WAL;"); con.execute("PRAGMA synchronous=NORMAL;")
        tb5=f"{symbol}_5m"
        last=get_last_ts(con,tb5)
        start=max(win_start,last+1)
        total=0; loops=0
        while loops<max_loops:
            loops+=1
            df=fetch_5m(symbol, start_ms=start, limit=1500)
            if df.empty: break
            if last>0: df=df[df["ts"]>last]
            if df.empty: break
            wrote=write_batch(con,tb5,df)
            total+=wrote
            last = get_last_ts(con,tb5)
            start = int(df["ts"].iloc[-1])+1
            if len(df)<1500: break

        # 聚合其他周期（只用近窗口的 5m）
        src5=pd.read_sql_query(f'SELECT ts,open,high,low,close,volume FROM "{tb5}" WHERE ts>=?', con, params=(win_start,))
        if not src5.empty:
            for tf in TARGET_TFS:
                tgt=f"{symbol}_{tf}"
                tgt_last=get_last_ts(con,tgt)
                agg=aggregate_from_5m(src5, tf)
                if tgt_last>0: agg=agg[agg["ts"]>tgt_last]
                if not agg.empty:
                    write_batch(con,tgt,agg)
    return total

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols-file")
    ap.add_argument("--interval", type=int, default=30, help="每轮循环间隔秒")
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--backfill-days", type=int, default=3, help="每轮补齐近N天")
    args=ap.parse_args()

    syms=None
    if args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file,"r",encoding="utf-8") as f:
            syms=[x.strip().upper() for x in f if x.strip()]
    if not syms:
        syms=list_symbols_from_db(args.db)
        if not syms:
            console.print("[red]未在 DB 中发现 *_5m 表，无法自动获取 SYMBOL 清单。请提供 --symbols-file[/red]")
            return

    console.rule(f"[bold green]实时守护启动 | {len(syms)} symbols | {args.max_workers} 并发 | 间隔 {args.interval}s")
    while True:
        t0=time.time()
        jobs=[]
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            for s in syms:
                jobs.append(ex.submit(update_one_symbol, args.db, s, args.backfill_days))
            with Progress() as prog:
                task=prog.add_task("[cyan]更新中...", total=len(jobs))
                for fut in as_completed(jobs):
                    try: fut.result()
                    except Exception as e: console.print(f"[red]任务错误：{e}[/red]")
                    prog.advance(task)
        dt_ms=int((time.time()-t0)*1000)
        console.print(f"[green]本轮完成[/green] 用时 {dt_ms/1000:.1f}s ；{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sleep=max(1, args.interval - int((time.time()-t0)))
        time.sleep(sleep)

if __name__=="__main__":
    main()
