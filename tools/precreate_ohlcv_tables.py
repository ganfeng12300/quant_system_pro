# -*- coding: utf-8 -*-
"""
precreate_ohlcv_tables.py
为 symbols × tfs 预创建 K 线表，避免保存时报 'no such table'。
"""
import argparse, sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS "{table}" (
  timestamp INTEGER PRIMARY KEY,   -- 秒级 Unix 时间戳
  open      REAL,
  high      REAL,
  low       REAL,
  close     REAL,
  volume    REAL
);
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols-file", required=True)
    ap.add_argument("--tfs", nargs="*", required=True)
    args = ap.parse_args()

    # 读 symbols
    syms = []
    with open(args.symbols_file, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if s and not s.startswith("#"):
                syms.append(s)
    syms = sorted(set(syms))
    if not syms:
        print("[FATAL] 符号列表为空"); return

    con = sqlite3.connect(args.db, timeout=60, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    c = con.cursor()
    created = 0
    for s in syms:
        for tf in args.tfs:
            tbl = f"{s}_{tf}"
            c.execute(SCHEMA.replace("{table}", tbl))
            created += 1
    con.commit()
    con.close()
    print(f"✅ 预建完成：{created} 张表（{len(syms)} symbols × {len(args.tfs)} tfs）")

if __name__ == "__main__":
    main()
