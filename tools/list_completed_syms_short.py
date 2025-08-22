# -*- coding: utf-8 -*-
"""
列出 DB 中历史已补齐的币种 (只检查 5m/15m/30m/1h)
判定标准：
  - 币种必须包含所有要求的表
  - 每个表的最早 timestamp <= 当前时间 - target_days*86400
输出：
  results/completed_short.txt  → 已补齐币种
  results/coverage_gaps_short.csv → 未达标详情
"""
import argparse, os, sqlite3, time, re, csv, sys
from datetime import datetime, timezone

PAT = re.compile(r"^([A-Z0-9]+)_(\d+[mh]|1d)$", re.I)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="数据库路径")
    ap.add_argument("--days", type=int, default=365, help="目标覆盖天数 (默认365)")
    ap.add_argument("--outdir", default="results", help="输出目录 (默认 results)")
    return ap.parse_args()

def ts_to_str(ts):
    if not ts: return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def main():
    a = parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    cutoff = int(time.time()) - a.days*86400
    need_tfs = ["5m","15m","30m","1h"]

    con = sqlite3.connect(a.db, timeout=30)
    cur = con.cursor()
    rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%\_%' ESCAPE '\\'").fetchall()
    sym2tfs = {}
    for (name,) in rows:
        m = PAT.match(name)
        if m: sym2tfs.setdefault(m.group(1).upper(), set()).add(m.group(2).lower())

    completed, issues = [], []
    for sym, tfs in sorted(sym2tfs.items()):
        miss = [tf for tf in need_tfs if tf not in tfs]
        if miss:
            for tf in miss:
                issues.append([sym, tf, "MISSING_TABLE","","","",""])
            continue
        all_ok = True
        for tf in need_tfs:
            tbl = f"{sym}_{tf}"
            r = cur.execute(f"SELECT MIN(timestamp),MAX(timestamp),COUNT(1) FROM '{tbl}'").fetchone()
            if not r or r[0] is None:
                issues.append([sym,tf,"EMPTY","","","",""])
                all_ok = False
            elif r[0] > cutoff:
                issues.append([sym,tf,"TOO_SHORT",ts_to_str(r[0]),ts_to_str(r[1]),r[2],"need older than cutoff"])
                all_ok = False
        if all_ok: completed.append(sym)

    # 写文件
    txt = os.path.join(a.outdir,"completed_short.txt")
    with open(txt,"w",encoding="utf-8") as f: f.write("\n".join(completed))
    csvp = os.path.join(a.outdir,"coverage_gaps_short.csv")
    with open(csvp,"w",encoding="utf-8",newline="") as f:
        w=csv.writer(f); w.writerow(["symbol","timeframe","status","min_ts","max_ts","count","detail"])
        w.writerows(issues)

    print(f"[SUMMARY] 完成 {len(completed)}/{len(sym2tfs)} 个币种")
    print(f"[OUT] 已补齐列表: {txt}")
    print(f"[OUT] 缺口详情: {csvp}")

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt: sys.exit(130)
