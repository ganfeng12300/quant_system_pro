# -*- coding: utf-8 -*-
"""
watch_db_all_delta.py
在 '进度' 的基础上增加“是否正在写入”的直观信号：
- ΔROWS: 与上次刷新相比新增的行数
- RATE(rpm): 本轮写入速度（rows per minute）
- LASTΔ: 距离“最后一次有新增”过去了多少秒
- 顶部显示：本轮发生更新的表数 / 总监控表数
"""

import argparse, os, re, sqlite3, time, math
import datetime as dt
from pathlib import Path
from typing import List, Tuple, Optional, Dict

# ====== 颜色 ======
ANSI = os.getenv("NO_COLOR") is None
def c(s, code): return f"\033[{code}m{s}\033[0m" if ANSI else s
def green(s): return c(s, "32")
def yellow(s): return c(s, "33")
def red(s): return c(s, "31")
def cyan(s): return c(s, "36")
def dim(s): return c(s, "2")
PAT = re.compile(r"^([A-Z0-9]+)_(\d+[mh]|1d)$", re.I)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--interval", type=int, default=10)
    ap.add_argument("--tfs", default="5m,15m,1h")
    ap.add_argument("--symbols", default="")
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--age-warn", type=int, default=180)
    ap.add_argument("--age-bad", type=int, default=900)
    ap.add_argument("--expected-days", type=int, default=365)
    ap.add_argument("--goal-mode", choices=["fixed","by-range"], default="fixed")
    return ap.parse_args()

def clear(): os.system("cls" if os.name == "nt" else "clear")

def load_symbol_filter(arg: str) -> List[str]:
    if not arg: return []
    p = Path(arg)
    if p.exists() and p.is_file():
        return [x.strip().upper() for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
    return [x.strip().upper() for x in arg.split(",") if x.strip()]

def query_tables(con) -> List[str]:
    cur = con.cursor()
    rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_%'").fetchall()
    return [r[0] for r in rows]

def tf_to_minutes(tf: str) -> Optional[int]:
    tf=tf.lower()
    if tf.endswith("m"): return int(tf[:-1])
    if tf.endswith("h"): return int(tf[:-1])*60
    if tf=="1d": return 1440
    return None

def expected_rows_fixed(tf: str, days: int) -> int:
    mins=tf_to_minutes(tf)
    return int(days*(1440/mins)) if mins else 0

def normalize_ts(v) -> Optional[int]:
    if v is None: return None
    try: ts=int(v)
    except Exception:
        try: ts=int(float(v))
        except Exception: return None
    if ts>10**12: ts//=1000
    if ts<315532800 or ts>4102444800: return None
    return ts

def latest_stats(con, table: str) -> Tuple[Optional[int], int]:
    cur=con.cursor()
    try:
        raw_ts,cnt=cur.execute(f"SELECT MAX(timestamp), COUNT(*) FROM '{table}'").fetchone()
        return normalize_ts(raw_ts), int(cnt or 0)
    except Exception:
        return None,0

def status_from_age(age, warn, bad):
    if age is None: return red("EMPTY")
    if age<warn: return green("LIVE")
    if age<bad: return yellow("STALE")
    return red("OLD")

def main():
    args=parse_args()
    db=Path(args.db)
    if not db.exists():
        print(red(f"[FATAL] DB 不存在：{db}")); return

    tf_filter=[t.strip().lower() for t in args.tfs.split(",")] if args.tfs else []
    sym_filter=load_symbol_filter(args.symbols)

    prev: Dict[str, Dict[str,int]] = {}  # key=table -> {rows, ts, last_change_ts}

    while True:
        loop_start=time.time()
        try:
            con=sqlite3.connect(str(db), timeout=5)
        except Exception as e:
            clear(); print(red(f"[FATAL] 无法打开 DB：{e}")); time.sleep(args.interval); continue

        now=int(time.time())
        tables=query_tables(con)
        items=[]
        updated_count=0

        for t in tables:
            m=PAT.match(t)
            if not m: continue
            sym,tf=m.group(1).upper(),m.group(2).lower()
            if tf_filter and tf not in tf_filter: continue
            if sym_filter and sym not in sym_filter: continue

            ts, cnt = latest_stats(con, t)
            age = (now - ts) if ts is not None else None

            # 进度
            goal = expected_rows_fixed(tf, args.expected_days) if args.goal_mode=="fixed" else 0
            remain = max(0, goal - cnt) if goal>0 else 0
            progress = min(100.0, (cnt/goal)*100.0) if goal>0 else 0.0

            # 增量
            p = prev.get(t, {"rows": cnt, "ts": ts or 0, "last_change_ts": now})
            d_rows = cnt - p["rows"]
            dt_s = max(1, now - loop_start + args.interval)  # 用近似间隔避免除0
            rpm = (d_rows / max(1, time.time()-loop_start)) * 60.0

            # 最后一次变化时间
            last_change_ts = p["last_change_ts"]
            if d_rows>0:
                last_change_ts = now
                updated_count += 1

            prev[t] = {"rows": cnt, "ts": ts or 0, "last_change_ts": last_change_ts}

            items.append({
                "sym": sym, "tf": tf, "t": t,
                "cnt": cnt, "goal": goal, "remain": remain, "prog": progress,
                "ts": ts or 0, "age": age,
                "d_rows": d_rows, "rpm": rpm,
                "since_change": now - last_change_ts if last_change_ts else None
            })

        con.close()

        # 排序：优先“本轮有新增”，其次按最近时间
        items.sort(key=lambda r: ((r["d_rows"]>0), r["ts"]), reverse=True)
        if args.top>0: items=items[:args.top]

        clear()
        header = f"🛰 DB: {db}   周期: {','.join(tf_filter) if tf_filter else '全部'}   刷新: {args.interval}s"
        print(cyan(header))
        print(dim(f"UTC: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}   有更新: {updated_count}/{len(items)}   目标: {args.goal_mode}({args.expected_days}d)"))
        print("-"*150)
        print(f"{'SYMBOL':<12}{'TF':<5}{'ROWS':>9}/{ 'GOAL':<8}  {'ΔROWS':>7}  {'RATE(rpm)':>10}  {'LASTΔ':>7}  {'PROG%':>7}  {'LATEST_UTC':<19}  {'AGE':>8}  STATUS")
        print("-"*150)

        for r in items:
            sym, tf = r["sym"], r["tf"]
            cnt, goal, remain, prog = r["cnt"], r["goal"], r["remain"], r["prog"]
            d_rows, rpm = r["d_rows"], r["rpm"]
            since = r["since_change"]
            latest = dt.datetime.utcfromtimestamp(r["ts"]).strftime("%Y-%m-%d %H:%M:%S") if r["ts"] else "-"
            age = r["age"]
            st = status_from_age(age, args.age_warn, args.age_bad)

            # 彩色信号
            d_txt = (green(f"+{d_rows}") if d_rows>0 else (dim("0") if d_rows==0 else red(str(d_rows))))
            rpm_txt = green(f"{rpm:6.2f}") if d_rows>0 else dim(f"{rpm:6.2f}")
            prog_txt = green(f"{prog:6.2f}%") if prog>=99.99 else (yellow(f"{prog:6.2f}%") if prog>=50 else dim(f"{prog:6.2f}%"))
            lastd_txt = f"{since:>5}s" if since is not None else "  -  "
            age_txt = f"{age:>6}s" if isinstance(age,int) else "   -  "
            print(f"{sym:<12}{tf:<5}{cnt:>9}/{goal:<8}  {d_txt:>7}  {rpm_txt:>10}  {lastd_txt:>7}  {prog_txt:>7}  {latest:<19}  {age_txt:>8}  {st}")

        dur=time.time()-loop_start
        print("-"*150)
        print(dim(f"刷新耗时 {dur:.2f}s  下一次 {max(0, args.interval-int(dur))}s（Ctrl+C 退出）"))
        try:
            time.sleep(max(0, args.interval - dur))
        except KeyboardInterrupt:
            print("\n退出监控。"); break

if __name__ == "__main__":
    main()
