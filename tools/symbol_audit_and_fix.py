# -*- coding: utf-8 -*-
"""
symbol_audit_and_fix.py — 币种体检 + 缺洞修复（Binance USDT 永续，表名 {SYMBOL}_{TF}）
功能：
1) 自动从 DB 识别已存在的币种（扫描 *_5m 表名得到 SYMBOL 清单）
2) 体检：各周期(5m/15m/30m/1h/2h/4h/1d) 行数、最新时间戳、新鲜度(分钟)、近N根是否有缺洞
3) --autofix：按需要拉取 REST 历史补洞；并用 5m → 聚合出其余周期（稳健版聚合）
4) 输出彩色终端报告 + 保存 CSV 到 results/health/yyyymmdd-hhmmss/
"""
import argparse, os, sqlite3, time, datetime as dt
import requests, pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

console = Console()

TFS = ["5m","15m","30m","1h","2h","4h","1d"]
TF_MS = {"5m":300000,"15m":900000,"30m":1800000,"1h":3600000,"2h":7200000,"4h":14400000,"1d":86400000}
BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/continuousKlines"

def ensure_dir(p): os.makedirs(p, exist_ok=True); return p

def list_symbols_from_db(db):
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    syms=set()
    for r in rows:
        n=r["name"]
        if n.endswith("_5m") and len(n)>3:
            syms.add(n[:-3])  # 去掉 _5m
    return sorted(syms)

def table_exists(con, tb):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (tb,)).fetchone() is not None

def get_last_ts(con, tb):
    try:
        r=con.execute(f'SELECT MAX(ts) FROM "{tb}"').fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception:
        return 0

def count_rows(con, tb):
    try:
        r=con.execute(f'SELECT COUNT(1) FROM "{tb}"').fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception:
        return 0

def fetch_binance_cont_klines(symbol, tf, start_ms=None, end_ms=None, limit=1500):
    params={"pair":symbol,"contractType":"PERPETUAL","interval":tf,"limit":min(1500,int(limit))}
    if start_ms is not None: params["startTime"]=int(start_ms)
    if end_ms is not None: params["endTime"]=int(end_ms)
    r = requests.get(BINANCE_KLINES, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return pd.DataFrame()
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
    })
    return out.dropna()

def write_batch(con, tb, df):
    if df is None or df.empty: return 0
    con.execute(f'CREATE TABLE IF NOT EXISTS "{tb}" (ts INTEGER PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL)')
    con.executemany(f'INSERT OR IGNORE INTO "{tb}" (ts,open,high,low,close,volume) VALUES (?,?,?,?,?,?)',
                    list(map(tuple, df[["ts","open","high","low","close","volume"]].values)))
    return int(con.total_changes)

def aggregate_from_5m(df5, target_tf):
    """
    稳健版聚合：严格 1D，避免 'Per-column arrays must each be 1-dimensional'
    - df5: 包含 ts/open/high/low/close/volume 的 5m 数据，ts 为毫秒
    - target_tf: 目标周期字符串（如 '15m','30m','1h','2h','4h','1d'）
    """
    if df5 is None or len(df5) == 0:
        return df5.iloc[0:0].copy()

    df = df5.copy()
    # 强制类型与清洗
    for c in ["ts", "open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = df["ts"].astype("int64")

    tf_ms = TF_MS[target_tf]
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

def recent_gaps(df, tf, check_n=200):
    if df.empty or len(df)<3: return 0
    tf_ms=TF_MS[tf]; x=df["ts"].values[-min(check_n,len(df)):]
    gaps=0
    for i in range(1,len(x)):
        if int(x[i])-int(x[i-1])>tf_ms:
            gaps+=1
    return gaps

def human_min_ago(ts):
    if not ts: return 1e9
    return (int(time.time()*1000)-int(ts))/60000.0

def audit_and_fix(db, symbols=None, backfill_days=7, autofix=False, out_dir=None):
    out_dir = ensure_dir(out_dir or os.path.join("results","health", dt.datetime.now().strftime("%Y%m%d-%H%M%S")))
    with sqlite3.connect(db) as con, Progress() as prog:
        con.execute("PRAGMA journal_mode=WAL;"); con.execute("PRAGMA synchronous=NORMAL;")
        sym_list = symbols or list_symbols_from_db(db)
        task = prog.add_task("[cyan]体检中...", total=len(sym_list)*len(TFS))
        rows=[]
        for s in sym_list:
            for tf in TFS:
                tb=f"{s}_{tf}"
                n = count_rows(con, tb) if table_exists(con, tb) else 0
                last = get_last_ts(con, tb) if n>0 else 0
                stale = human_min_ago(last) if n>0 else 1e9
                gaps=0
                if n>10:
                    df_ts=pd.read_sql_query(f'SELECT ts FROM "{tb}" ORDER BY ts', con)
                    gaps=recent_gaps(df_ts, tf)
                status="OK"
                if n==0: status="MISSING"
                elif stale>30: status="STALE"
                elif gaps>0: status="GAPS"

                if autofix:
                    try:
                        if tf=="5m":
                            since_ms = int(time.time()*1000) - backfill_days*86400000
                            start = max(since_ms, last+1)
                            while True:
                                df = fetch_binance_cont_klines(s, "5m", start_ms=start, limit=1500)
                                if df.empty: break
                                write_batch(con, tb, df)
                                start = int(df["ts"].iloc[-1])+1
                                if len(df)<1500: break
                        else:
                            # 从 5m 聚合
                            src=f"{s}_5m"
                            if table_exists(con, src):
                                tgt_last = get_last_ts(con, tb)
                                win_ms = max(TF_MS[tf]*400, 7*86400000)
                                src_df=pd.read_sql_query(
                                    f'SELECT ts,open,high,low,close,volume FROM "{src}" WHERE ts>=?',
                                    con, params=(int(time.time()*1000)-win_ms,))
                                if not src_df.empty:
                                    agg = aggregate_from_5m(src_df, tf)
                                    if tgt_last>0:
                                        agg = agg[agg["ts"]>tgt_last]
                                    if not agg.empty:
                                        write_batch(con, tb, agg)
                    except Exception as e:
                        console.print(f"[red]修复 {tb} 失败：{e}[/red]")

                rows.append(dict(symbol=s, tf=tf, rows=n, last_ts=last, stale_min=round(stale,1), gaps=gaps, status=status))
                prog.advance(task)

    # 输出报告
    df=pd.DataFrame(rows)
    csv_path=os.path.join(out_dir,"audit.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    tbl=Table(title=f"覆盖率体检（写入：{csv_path}）", show_lines=False)
    tbl.add_column("Symbol", style="bold")
    tbl.add_column("TF")
    tbl.add_column("Rows", justify="right")
    tbl.add_column("Last TS", justify="right")
    tbl.add_column("Stale(min)", justify="right")
    tbl.add_column("Gaps(近200)", justify="right")
    tbl.add_column("Status", style="bold")
    for _,r in df.sort_values(["symbol","tf"]).iterrows():
        color = "green"
        if r["status"]=="MISSING": color="red"
        elif r["status"]=="STALE": color="yellow"
        elif r["status"]=="GAPS": color="magenta"
        tbl.add_row(r["symbol"], r["tf"], str(r["rows"]), str(int(r["last_ts"])),
                    f"{r['stale_min']:.1f}", str(int(r["gaps"])), f"[{color}]{r['status']}[/{color}]")
    console.print(tbl)
    console.print(f"[green]报告已保存：{csv_path}[/green]")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols-file")
    ap.add_argument("--backfill-days", type=int, default=7)
    ap.add_argument("--autofix", type=int, default=0)
    args=ap.parse_args()
    syms=None
    if args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file,"r",encoding="utf-8") as f:
            syms=[x.strip().upper() for x in f if x.strip()]
    audit_and_fix(args.db, symbols=syms, backfill_days=args.backfill_days, autofix=bool(args.autofix))
