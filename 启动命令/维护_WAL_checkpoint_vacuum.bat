@echo off
setlocal
title ▶ 维护：WAL Checkpoint + VACUUM
chcp 65001 >nul
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set QS_DB=D:\quant_system_v2\data\market_data.db

echo [INFO] 对 %QS_DB% 执行 checkpoint + VACUUM（会短暂占用数据库，请在低峰执行）
python - <<PY
import sqlite3, os, sys
db = r"%QS_DB%"
con = sqlite3.connect(db, timeout=30)
cur = con.cursor()
print("[PRAGMA] journal_mode=", cur.execute("PRAGMA journal_mode").fetchone())
print("[ACTION] wal_checkpoint(TRUNCATE)")
cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
con.commit()
print("[ACTION] VACUUM")
cur.execute("VACUUM")
con.commit()
con.close()
print("[OK] 完成。")
PY

pause
