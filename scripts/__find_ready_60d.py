import sqlite3, os
from pathlib import Path

DB = r"D:\quant_system_v2\data\market_data_snapshot.db"
OUT = Path(r"results\qs2_ready_for_bt.txt")
OUT.parent.mkdir(parents=True, exist_ok=True)

# 要求：60天，15m>=5760根，30m>=2880根
REQ = {"15m": 96*60, "30m": 48*60}
tfs = list(REQ.keys())

con = sqlite3.connect(DB)
cur = con.cursor()
# 所有非sqlite内部表
tbls = [r[0] for r in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]

# 聚合每个symbol在各tf上的行数
from collections import defaultdict
cnt = defaultdict(dict)
for tf in tfs:
    suffix = "_" + tf
    for t in tbls:
        if t.endswith(suffix):
            sym = t[:-len(suffix)]
            try:
                n = cur.execute(f"SELECT COUNT(1) FROM '{t}'").fetchone()[0]
            except Exception:
                n = 0
            cnt[sym][tf] = n

ready = []
for sym, m in cnt.items():
    ok = all(m.get(tf, 0) >= REQ[tf] for tf in tfs)
    if ok:
        ready.append(sym)

ready.sort()
with open(OUT, "w", encoding="utf-8") as f:
    f.write("\n".join(ready))

print(f"[OK] 就绪符号数={len(ready)} -> {OUT}")
con.close()
