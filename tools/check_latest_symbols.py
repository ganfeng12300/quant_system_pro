# -*- coding: utf-8 -*-
"""
check_latest_symbols.py — 批量检测各币种在各周期的最新时间戳（UTC）
作者：你的量化助手

用法（PowerShell/CMD）：
  # 1) 自动从数据库里选取“出现次数最多”的前40个币
  python tools\check_latest_symbols.py --db "D:\quant_system_v2\data\market_data.db"

  # 2) 指定自定义清单（每行一个，如 BTCUSDT）
  python tools\check_latest_symbols.py --db "D:\quant_system_v2\data\market_data.db" --symbols-file ".\top40.txt"

可选参数：
  --limit 40                       # 自动挑选时的前N个（默认40）
  --timeframes 5m,15m,30m,1h,4h,1d # 要检查的周期（默认：5m,15m,30m,1h,2h,4h,1d）
  --out ".\latest_check.csv"       # 导出CSV路径（默认当前目录下 latest_check_<date>.csv）
"""

import argparse
import datetime as dt
import os
import re
import sqlite3
from pathlib import Path
from collections import Counter, defaultdict

DEF_TFS = ["5m","15m","30m","1h","2h","4h","1d"]

def parse_args():
    ap = argparse.ArgumentParser(description="Batch check latest candle timestamp for symbols & timeframes.")
    ap.add_argument("--db", required=True, help="Path to SQLite DB (market_data.db)")
    ap.add_argument("--symbols-file", default=None, help="Optional file with symbols (one per line)")
    ap.add_argument("--limit", type=int, default=40, help="Top-N symbols to auto-pick when symbols-file not provided")
    ap.add_argument("--timeframes", default=",".join(DEF_TFS), help="Comma-separated TFs, e.g. 5m,15m,30m,1h,4h,1d")
    ap.add_argument("--out", default=None, help="CSV output path")
    return ap.parse_args()

def list_tables(con):
    cur = con.cursor()
    # 表名形如 BTCUSDT_1h，用 ESCAPE 正确匹配带下划线
    rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%\\_%' ESCAPE '\\'").fetchall()
    return [r[0] for r in rows]

def auto_pick_symbols(tables, limit):
    """
    从表名中拆出 base 与 tf（形如 BASE_TF），统计 BASE 频次，取 Top-N。
    """
    pat = re.compile(r"^([A-Z0-9]+)_(\d+[mh]|1d|2h|4h)$", re.I)
    bases = []
    for t in tables:
        m = pat.match(t)
        if m:
            bases.append(m.group(1).upper())
    cnt = Counter(bases)
    return [s for s, _ in cnt.most_common(limit)]

def read_symbols_file(path):
    syms = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"): 
            continue
        syms.append(line.upper())
    return syms

def latest_ts(con, table):
    cur = con.cursor()
    try:
        val = cur.execute(f"SELECT MAX(timestamp) FROM '{table}'").fetchone()[0]
        return int(val) if val else None
    except Exception:
        return None

def ts_to_utc(ts_ms):
    return dt.datetime.utcfromtimestamp(ts_ms/1000).strftime("%Y-%m-%d %H:%M:%S") if ts_ms else ""

def main():
    args = parse_args()
    db = Path(args.db)
    if not db.exists():
        print(f"[ERROR] DB not found: {db}")
        return

    tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    out_path = Path(args.out) if args.out else Path.cwd() / f"latest_check_{dt.datetime.now():%Y%m%d-%H%M%S}.csv"

    con = sqlite3.connect(str(db), timeout=30)
    tables = list_tables(con)

    # 准备符号列表
    if args.symbols_file:
        symbols = read_symbols_file(args.symbols_file)
    else:
        symbols = auto_pick_symbols(tables, args.limit)
    symbols = list(dict.fromkeys(symbols))  # 去重保持顺序

    print("="*80)
    print(f"DB: {db}")
    print(f"SYMBOLS: {len(symbols)} (showing up to {min(10,len(symbols))} preview) -> {symbols[:10]}")
    print(f"TIMEFRAMES: {tfs}")
    print("="*80)

    # 查表
    rows = []
    header = ["symbol"] + [f"last_{tf}_utc" for tf in tfs]
    print(f"{'symbol':12s}  " + "  ".join([f"{tf:>10s}" for tf in tfs]))
    print("-"*80)

    for sym in symbols:
        row = {"symbol": sym}
        line_print = [f"{sym:12s}"]
        for tf in tfs:
            table = f"{sym}_{tf}"
            ts = latest_ts(con, table)
            row[f"last_{tf}_utc"] = ts_to_utc(ts)
            line_print.append(f"{row[f'last_{tf}_utc'][-8:] if row[f'last_{tf}_utc'] else '':>10s}")
        rows.append(row)
        print("  ".join(line_print))

    con.close()

    # 写 CSV
    try:
        import csv
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        print("-"*80)
        print(f"[OK] CSV written -> {out_path}")
    except Exception as e:
        print(f"[WARN] CSV write failed: {e}")

    # 简短总结：统计落后到当天0点之前的数量
    try:
        today_utc0 = int(dt.datetime.combine(dt.datetime.utcnow().date(), dt.time()).timestamp())*1000
        stale = 0
        total_cells = 0
        for r in rows:
            for tf in tfs:
                total_cells += 1
                val = r.get(f"last_{tf}_utc","")
                if val:
                    # 若非当天（简单判断日期字符串开头）
                    if not val.startswith(dt.datetime.utcnow().strftime("%Y-%m-%d")):
                        stale += 1
                else:
                    stale += 1
        rate = (stale/total_cells*100) if total_cells else 0.0
        print(f"[SUMMARY] stale cells: {stale}/{total_cells} ({rate:.1f}%)  (not today or empty)")
    except Exception:
        pass

if __name__ == "__main__":
    main()
