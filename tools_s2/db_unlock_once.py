import sqlite3
db = r"D:\quant_system_v2\data\market_data.db"
con = sqlite3.connect(db, timeout=30)
cur = con.cursor()
cur.execute("PRAGMA journal_mode=WAL;")
cur.execute("PRAGMA busy_timeout=60000;")
cur.execute("PRAGMA synchronous=NORMAL;")
cur.execute("PRAGMA wal_autocheckpoint=1000;")
con.commit()
# 先尝试普通 checkpoint，再 TRUNCATE
cur.execute("PRAGMA wal_checkpoint;")
con.commit()
cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
con.commit()
con.close()

# VACUUM 需新连接执行
con2 = sqlite3.connect(db, timeout=60)
con2.execute("VACUUM;")
con2.commit()
con2.close()
print("[OK] checkpoint+VACUUM 完成")
