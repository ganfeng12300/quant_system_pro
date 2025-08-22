# -*- coding: utf-8 -*-
"""
检测数据库是否写入成功：输出每个表的最新一根K线时间
"""
import sqlite3, datetime
from pathlib import Path

DB = Path(r"D:\quant_system_v2\data\market_data.db")

con = sqlite3.connect(DB)
cur = con.cursor()

tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_%'").fetchall()
print(f"共发现 {len(tables)} 张K线表\n")

for (t,) in tables[:20]:  # 只预览前20张，防止刷屏
    ts, cnt = cur.execute(f"SELECT MAX(timestamp), COUNT(*) FROM '{t}'").fetchone()
    if ts:
        dt = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        print(f"{t:<20} 行数={cnt:<8} 最新={dt} UTC")
    else:
        print(f"{t:<20} 空表")
con.close()
