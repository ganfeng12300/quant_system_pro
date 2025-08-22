# -*- coding: utf-8 -*-
"""
rt_quotes 监控（每 10 秒刷新）
- 自适应使用 updated_at 或 ts_ms 做“最近写入时间”
- 显示总行数、最近更新时间，并抽样几条 bid/ask
"""

import sqlite3, time
from datetime import datetime, timezone

DB_PATH = r"D:\quant_system_v2\data\market_data.db"
SAMPLE_N = 5       # 每次打印几个示例符号
INTERVAL_SEC = 10  # 刷新间隔

SQL_HAS_UPDATED_AT = "PRAGMA table_info(rt_quotes)"
SQL_COUNT = "SELECT COUNT(*) FROM rt_quotes"
SQL_MAX_UPDATED = "SELECT MAX(updated_at) FROM rt_quotes"
SQL_MAX_TS_MS = "SELECT MAX(ts_ms) FROM rt_quotes"
SQL_SAMPLES = "SELECT symbol, bid, ask, last, updated_at, ts_ms FROM rt_quotes ORDER BY symbol LIMIT ?"

def to_dt(ms_or_s):
    if ms_or_s is None:
        return "NULL"
    # 判断是否毫秒
    if ms_or_s > 10**12:  # 13位
        ts = ms_or_s / 1000.0
    else:
        ts = float(ms_or_s)
    # 显示 UTC 时间
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def main():
    # 发现用哪个时间字段
    with sqlite3.connect(DB_PATH) as con:
        cols = [r[1] for r in con.execute(SQL_HAS_UPDATED_AT)]
    use_updated = "updated_at" in cols

    while True:
        try:
            with sqlite3.connect(DB_PATH) as con:
                cur = con.cursor()
                rows = cur.execute(SQL_COUNT).fetchone()[0]
                if use_updated:
                    last_raw = cur.execute(SQL_MAX_UPDATED).fetchone()[0]
                else:
                    last_raw = cur.execute(SQL_MAX_TS_MS).fetchone()[0]

                samples = cur.execute(SQL_SAMPLES, (SAMPLE_N,)).fetchall()

            print("=" * 78)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] rows={rows}  "
                  f"last_update= {to_dt(last_raw)}  (field={'updated_at' if use_updated else 'ts_ms'})")

            for sym, bid, ask, last, up, ts in samples:
                ts_show = up if use_updated else ts
                print(f"  {sym:<14} bid={bid:.6f}  ask={ask:.6f}  last={last:.6f}  @ {to_dt(ts_show)}")

        except Exception as e:
            print("监控异常:", e)

        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
