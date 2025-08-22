# -*- coding: utf-8 -*-
"""
监控 SQLite DB 文件体积变化（含 WAL/SHM）
- 输出：.db / .db-wal / .db-shm 的大小、总大小、与上次相比的增量
- 主库不变但 WAL 增长，也表示正在写入
用法：
  python tools\monitor_db_size.py --db "D:\quant_system_v2\data\market_data.db" --interval 10
"""
import os, time, argparse
from datetime import datetime

def fsize(path):
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return 0

def human(n):
    neg = n < 0
    n = abs(n)
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024.0:
            return ("-" if neg else "") + f"{n:.1f}{unit}"
        n /= 1024.0
    return ("-" if neg else "") + f"{n:.1f}PB"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite 主库文件路径，例如 D:\\quant_system_v2\\data\\market_data.db")
    ap.add_argument("--interval", type=int, default=10, help="刷新间隔（秒），默认 10")
    args = ap.parse_args()

    DB = args.db
    INTERVAL = args.interval
    wal = DB + "-wal"
    shm = DB + "-shm"

    prev_db  = fsize(DB)
    prev_wal = fsize(wal)
    prev_shm = fsize(shm)
    prev_total = prev_db + prev_wal + prev_shm

    print(f"监控文件：\n  DB : {DB}\n  WAL: {wal}\n  SHM: {shm}\n刷新间隔: {INTERVAL}s\n")
    while True:
        db  = fsize(DB)
        wal_sz = fsize(wal)
        shm_sz = fsize(shm)
        total = db + wal_sz + shm_sz

        d_db   = db - prev_db
        d_wal  = wal_sz - prev_wal
        d_shm  = shm_sz - prev_shm
        d_total= total - prev_total

        now = datetime.now().strftime("%H:%M:%S")
        print("="*86)
        print(f"[{now}]  DB={human(db)} ({'+' if d_db>=0 else ''}{human(d_db)})  "
              f"WAL={human(wal_sz)} ({'+' if d_wal>=0 else ''}{human(d_wal)})  "
              f"SHM={human(shm_sz)} ({'+' if d_shm>=0 else ''}{human(d_shm)})")
        print(f"          合计={human(total)}  Δ合计=({'+' if d_total>=0 else ''}{human(d_total)})")

        if d_total>0 or d_wal>0 or d_db>0:
            print("➡ 检测到写入活动（总大小或 WAL 增长）。")
        else:
            print("… 暂无明显增长（可能在复用空闲页，或周期内无新增K线）。")

        prev_db, prev_wal, prev_shm, prev_total = db, wal_sz, shm_sz, total
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
