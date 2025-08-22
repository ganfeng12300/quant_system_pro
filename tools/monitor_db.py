# -*- coding: utf-8 -*-
"""
monitor_db.py — 监控 SQLite 数据库大小与写入时间（Windows/Python 3.x）

用法（PowerShell/CMD）：
  python tools\monitor_db.py --db "D:\quant_system_v2\data\market_data.db" --interval 30

可选：
  --show-wal         同时显示 .wal / .shm 的大小和mtime
  --once             只打印一次（调试用）
  --interval N       刷新间隔秒（默认 30）
"""
import argparse
import datetime as dt
import os
import time
from pathlib import Path

def human_size(n):
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.2f} {unit}"
        n /= 1024

def stat_file(p: Path):
    if not p.exists():
        return None
    s = p.stat()
    return {
        "path": str(p),
        "size": s.st_size,
        "mtime": dt.datetime.fromtimestamp(s.st_mtime)
    }

def print_line(tag, cur, last):
    if cur is None:
        print(f"{tag:<10} MISSING")
        return
    size = human_size(cur["size"])
    mtime = cur["mtime"].strftime("%Y-%m-%d %H:%M:%S")
    if last is None:
        print(f"{tag:<10} size={size:>10}   mtime={mtime}   Δsize=--   Δtime=--")
    else:
        dsize = cur["size"] - last["size"]
        dtime = (cur["mtime"] - last["mtime"]).total_seconds()
        sign = "+" if dsize >= 0 else ""
        print(f"{tag:<10} size={size:>10}   mtime={mtime}   Δsize={sign}{human_size(abs(dsize))}   Δtime={int(dtime)}s")

def main():
    ap = argparse.ArgumentParser(description="Monitor SQLite DB file growth")
    ap.add_argument("--db", required=True, help="Path to market_data.db")
    ap.add_argument("--interval", type=int, default=30, help="Refresh interval seconds (default 30)")
    ap.add_argument("--show-wal", action="store_true", help="Also show .wal/.shm files")
    ap.add_argument("--once", action="store_true", help="Print once then exit")
    args = ap.parse_args()

    db = Path(args.db)
    wal = Path(str(db) + ".wal")
    shm = Path(str(db) + ".shm")

    last_db = last_wal = last_shm = None

    print("="*66)
    print("SQLite DB Monitor")
    print("="*66)
    print(f"DB   : {db}")
    if args.show_wal:
        print(f"WAL  : {wal}")
        print(f"SHM  : {shm}")
    print(f"INTV : {args.interval}s")
    print("-"*66)

    try:
        while True:
            cur_db  = stat_file(db)
            cur_wal = stat_file(wal) if args.show_wal else None
            cur_shm = stat_file(shm) if args.show_wal else None

            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}]")
            print_line("DB",  cur_db,  last_db)
            if args.show_wal:
                print_line("WAL", cur_wal, last_wal)
                print_line("SHM", cur_shm, last_shm)
            print("-"*66)

            last_db, last_wal, last_shm = cur_db, cur_wal, cur_shm

            if args.once:
                break
            time.sleep(max(1, args.interval))
    except KeyboardInterrupt:
        print("Bye.")

if __name__ == "__main__":
    main()
