import sqlite3, time
db = r"D:\quant_system_v2\data\market_data_snapshot.db"
con = sqlite3.connect(db)
con.execute("PRAGMA busy_timeout=3000;")
tbls = [r[0] for r in con.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
tfs = ["5m","15m","30m","1h","2h","4h","1d"]
now = int(time.time()*1000)

def probe(tf):
    ret=[]
    for t in tbls:
        if not t.endswith("_"+tf): 
            continue
        try:
            c = con.execute(f"SELECT COUNT(*), MAX(timestamp), MIN(timestamp) FROM '{t}'").fetchone()
            n, mx, mn = (c or (0, None, None))
            if n and mx:
                delay_min = (now - int(mx))/60000.0
                sym = t[:-(len(tf)+1)]
                ret.append((sym, n, int(mn or 0), int(mx or 0), round(delay_min,1)))
        except Exception:
            pass
    ret.sort(key=lambda x: x[4], reverse=True)  # 按延迟从大到小
    return ret

for tf in tfs:
    rows = probe(tf)
    print(f"\n=== TF {tf} | 有数据的表: {len(rows)} ===")
    for sym, n, mn, mx, d in rows[:20]:
        print(f"{sym:<18} rows={n:<8} max_ts={mx} delay_min={d}")
con.close()
