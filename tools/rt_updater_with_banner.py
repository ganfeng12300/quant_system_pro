# -*- coding: utf-8 -*-
# tools/rt_updater_with_banner.py
"""
包装器：先打印 S 级彩色横幅，再把参数原封不动转发给 rt_updater_pro.py
用法：
  python tools\rt_updater_with_banner.py --db ... --backfill-days 365 --max-workers 8 --sleep 30
"""
import argparse, subprocess, sys, os
from tools.db_banner import print_db_startup_banner

def main():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--db")
    p.add_argument("--days", type=int)
    p.add_argument("--backfill-days", type=int, dest="backfill_days")
    p.add_argument("--tfs", default="5m,15m,30m,1h,2h,4h,1d")
    known, _ = p.parse_known_args()

    db = known.db or r"D:\quant_system_v2\data\market_data.db"
    days = known.backfill_days or known.days or 365
    tfs = tuple(x.strip() for x in known.tfs.split(",") if x.strip())

    try:
        print_db_startup_banner(db_path=db, days=int(days), tfs=tfs, hard_time_budget_sec=8.0)
    except Exception as e:
        print("[WARN] 启动横幅打印失败：", e)

    script = os.path.join(os.path.dirname(__file__), "rt_updater_pro.py")
    cmd = [sys.executable, script] + sys.argv[1:]
    raise SystemExit(subprocess.call(cmd))

if __name__ == "__main__":
    main()
