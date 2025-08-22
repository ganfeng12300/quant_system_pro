import sqlite3, os
src = r"D:\quant_system_v2\data\market_data.db"
dst = r"D:\quant_system_v2\data\market_data_snapshot.db"
if os.path.exists(dst):
    os.remove(dst)
con_src = sqlite3.connect(src, timeout=10)
con_dst = sqlite3.connect(dst)
con_src.backup(con_dst)  # 在线快照
con_dst.close(); con_src.close()
print("[OK] Snapshot ->", dst)
