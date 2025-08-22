# -*- coding: utf-8 -*-
"""
qs2_pretrade_gate.py — 纸面实盘前的新鲜度闸门
返回码：0=通过；非0=不通过
"""
import argparse, sqlite3, time, sys
from pathlib import Path

LIMITS_MIN = {"5m":2, "15m":5, "30m":10, "1h":20, "2h":30, "4h":45, "1d":120}

def latest_ts(con, table):
    try:
        r=con.execute(f"SELECT MAX(timestamp) FROM '{table}'").fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except: return None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--timeframes", default="5m,15m,30m,1h,2h,4h,1d")
    ap.add_argument("--max-stale-count", type=int, default=10, help="允许落后的表数上限")
    args=ap.parse_args()

    tfs=[x.strip() for x in args.timeframes.split(",") if x.strip()]
    con=sqlite3.connect(args.db); con.execute("PRAGMA busy_timeout=3000;")
    tbls=[r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    now=int(time.time()*1000)
    stale_tables=[]
    for tf in tfs:
        lim=LIMITS_MIN.get(tf,10)*60*1000
        for t in (x for x in tbls if x.endswith("_"+tf)):
            mx=latest_ts(con, t)
            if mx is None or now-mx>lim:
                stale_tables.append(t)
                if len(stale_tables)>=args.max_stale_count: break
        if len(stale_tables)>=args.max_stale_count: break
    con.close()
    if stale_tables:
        print("❌ 新鲜度闸门未通过；示例落后表：", stale_tables[:5])
        sys.exit(3)
    print("✅ 新鲜度闸门通过")
    sys.exit(0)

if __name__=="__main__":
    main()
