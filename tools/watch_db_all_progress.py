# -*- coding: utf-8 -*-
"""
watch_db_all_progress.py  (SAFE版)
实时监控 DB 中全部 K线表的写入情况，显示“采集进度/剩余条数/完成率”
修复点：
- 安全时间戳规范化（毫秒→秒，越界/异常不抛错）
- 排序/显示容错
"""

import argparse, os, re, sqlite3, time, math
import datetime as dt
from pathlib import Path
from typing import List, Tuple, Optional

# ====== 终端颜色 ======
ANSI = os.getenv("NO_COLOR") is None  # 设 NO_COLOR=1 可关闭彩色输出
def c(s, code): return f"\033[{code}m{s}\033[0m" if ANSI else s
def green(s): return c(s, "32")
def yellow(s): return c(s, "33")
def red(s): return c(s, "31")
def cyan(s): return c(s, "36")
def dim(s): return c(s, "2")

# 匹配表名：SYMBOL_TF（TF: 5m/15m/30m/1h）
PAT = re.compile(r"^([A-Z0-9]+)_(\d+[mh]|1d)$", re.I)

# ====== 参数 ======
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite DB 路径")
    ap.add_argument("--interval", type=int, default=10, help="刷新间隔秒")
    ap.add_argument("--tfs", default="5m,15m,30m,1h", help="监控这些周期，逗号分隔；留空=全部")
    ap.add_argument("--symbols", default="", help="仅监控这些符号（逗号分隔或txt路径）")
    ap.add_argument("--top", type=int, default=200, help="最多展示多少行（按最近更新时间排序）")
    ap.add_argument("--age-warn", type=int, default=180, help="AGE≥此秒为“黄”")
    ap.add_argument("--age-bad", type=int, default=900, help="AGE≥此秒为“红”")
    ap.add_argument("--expected-days", type=int, default=365, help="fixed 模式下的目标天数")
    ap.add_argument("--goal-mode", choices=["fixed","by-range"], default="fixed", help="目标行数算法：fixed/by-range")
    ap.add_argument("--snapshot-csv", default="", help="每次刷新将当前视图写入该CSV路径（可选）")
    return ap.parse_args()

# ====== 工具函数 ======
def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

def load_symbol_filter(arg: str) -> List[str]:
    if not arg: return []
    p = Path(arg)
    if p.exists() and p.is_file():
        syms = [line.strip().upper() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        return syms
    return [s.strip().upper() for s in arg.split(",") if s.strip()]

def query_tables(con: sqlite3.Connection) -> List[str]:
    cur = con.cursor()
    rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_%'").fetchall()
    return [r[0] for r in rows]

def tf_to_minutes(tf: str) -> Optional[int]:
    tf = tf.lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf == "1d":
        return 1440
    return None

def expected_rows_fixed(tf: str, days: int) -> int:
    mins = tf_to_minutes(tf)
    if not mins: return 0
    return int(days * (1440 / mins))

def expected_rows_by_range(con: sqlite3.Connection, table: str, tf: str) -> int:
    """按该表最早一条到现在的范围计算期望行数"""
    mins = tf_to_minutes(tf)
    if not mins: return 0
    cur = con.cursor()
    try:
        mn_row = cur.execute(f"SELECT MIN(timestamp) FROM '{table}'").fetchone()
        if not mn_row or not mn_row[0]: return 0
        start_ts = normalize_ts(mn_row[0])
        if start_ts is None: return 0
        now_ts = int(time.time())
        span_min = max(0, (now_ts - start_ts) / 60.0)
        return int(math.floor(span_min / mins) + 1)  # +1 覆盖起点
    except Exception:
        return 0

def normalize_ts(ts_val) -> Optional[int]:
    """
    将DB中的timestamp规范为“秒级Unix时间戳（int）”：
    - None/空 → None
    - 13位毫秒 → /1000
    - 过小(< 315532800, 1980-01-01) 或过大(> 4102444800, 2100-01-01) → None
    """
    if ts_val is None: return None
    try:
        ts = int(ts_val)
    except Exception:
        try:
            ts = int(float(ts_val))
        except Exception:
            return None
    # 毫秒 → 秒（粗判）
    if ts > 10**12:
        ts = ts // 1000
    if ts < 315532800 or ts > 4102444800:
        return None
    return ts

def safe_utc_str(ts: Optional[int]) -> str:
    if ts is None: return "-"
    try:
        return dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"

def latest_stats(con: sqlite3.Connection, table: str) -> Tuple[Optional[int], int]:
    cur = con.cursor()
    try:
        raw_ts, cnt = cur.execute(f"SELECT MAX(timestamp), COUNT(*) FROM '{table}'").fetchone()
        ts = normalize_ts(raw_ts)
        return (ts, int(cnt or 0))
    except Exception:
        return (None, 0)

def status_from_age(age: Optional[int], warn: int, bad: int) -> str:
    if age is None:
        return red("EMPTY")
    if age < warn:
        return green("LIVE")
    if age < bad:
        return yellow("STALE")
    return red("OLD")

def write_snapshot_csv(path: Path, rows: List[dict]):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        import csv
        with path.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [
                "symbol","tf","rows","goal","remain","progress_pct","latest_utc","age_sec","status"
            ])
            wr.writeheader()
            for r in rows:
                wr.writerow(r)
    except Exception as e:
        print(red(f"[WARN] 写CSV失败：{e}"))

# ====== 主程序 ======
def main():
    args = parse_args()
    db = Path(args.db)
    if not db.exists():
        print(red(f"[FATAL] DB 不存在：{db}")); return

    tf_filter = [t.strip().lower() for t in args.tfs.split(",")] if args.tfs else []
    sym_filter = load_symbol_filter(args.symbols)
    csv_path = Path(args.snapshot_csv) if args.snapshot_csv else None

    while True:
        loop_start = time.time()
        try:
            con = sqlite3.connect(str(db), timeout=5)
        except Exception as e:
            clear_screen()
            print(red(f"[FATAL] 无法打开 DB：{db}  err={e}"))
            time.sleep(args.interval)
            continue

        now_utc = int(time.time())
        tables = query_tables(con)
        rows = []
        for t in tables:
            m = PAT.match(t)
            if not m:
                continue
            sym, tf = m.group(1).upper(), m.group(2).lower()
            if tf_filter and tf not in tf_filter:
                continue
            if sym_filter and sym not in sym_filter:
                continue

            ts, cnt = latest_stats(con, t)
            age = (now_utc - ts) if ts is not None else None

            # 目标行数
            if args.goal_mode == "by-range":
                goal = expected_rows_by_range(con, t, tf)
            else:
                goal = expected_rows_fixed(tf, args.expected_days)

            # 进度与剩余
            if goal > 0:
                remain = max(0, goal - cnt)
                progress = min(100.0, (cnt / goal) * 100.0)
            else:
                remain = 0
                progress = 0.0

            latest_txt = safe_utc_str(ts)
            rows.append({
                "symbol": sym,
                "tf": tf,
                "rows": cnt,
                "goal": goal,
                "remain": remain,
                "progress_pct": f"{progress:6.2f}",
                "latest_utc": latest_txt,
                "age_sec": age if age is not None else "",
                "status": status_from_age(age, args.age_warn, args.age_bad),
                "sort_key": ts or 0
            })

        con.close()

        # 排序：最近时间降序
        rows.sort(key=lambda r: r["sort_key"], reverse=True)
        if args.top > 0:
            rows = rows[:args.top]

        # 输出
        clear_screen()
        header = f"🛰  DB: {db}   周期: {','.join(tf_filter) if tf_filter else '全部'}   符号: {len(sym_filter) if sym_filter else '全部'}   刷新: {args.interval}s"
        print(cyan(header))
        print(dim(f"UTC: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}   显示 {len(rows)} 行（按最近更新时间排序）   目标模式: {args.goal_mode}（expected_days={args.expected_days}）"))
        print("-"*132)
        print(f"{'SYMBOL':<14}{'TF':<6}{'ROWS':>9}/{ 'GOAL':<9}  {'REMAIN':>8}  {'PROG%':>7}  {'LATEST_UTC':<19}  {'AGE':>8}  STATUS")
        print("-"*132)

        csv_rows = []
        for r in rows:
            sym, tf = r["symbol"], r["tf"]
            cnt, goal = r["rows"], r["goal"]
            remain, prog = r["remain"], r["progress_pct"]
            latest, age = r["latest_utc"], r["age_sec"]
            status = r["status"]
            # 彩色渲染
            try:
                prog_val = float(prog)
            except Exception:
                prog_val = 0.0
            prog_txt = f"{prog_val:6.2f}%"
            if prog_val >= 99.99:
                prog_txt = green(prog_txt)
            elif prog_val >= 50:
                prog_txt = yellow(prog_txt)
            else:
                prog_txt = dim(prog_txt)
            age_txt = f"{age:>6}s" if isinstance(age, int) else "   -  "
            print(f"{sym:<14}{tf:<6}{cnt:>9}/{goal:<9}  {remain:>8}  {prog_txt:>7}  {latest:<19}  {age_txt:>8}  {status}")

            # CSV 原始行（不带颜色）
            csv_rows.append({
                "symbol": sym, "tf": tf, "rows": cnt, "goal": goal,
                "remain": remain, "progress_pct": f"{prog_val:.2f}",
                "latest_utc": latest, "age_sec": age if isinstance(age,int) else "", "status": status.replace("\x1b","")
            })

        print("-"*132)
        dur = time.time() - loop_start
        print(dim(f"刷新耗时 {dur:.2f}s   下次刷新 {max(0, args.interval - int(dur))}s 后（Ctrl+C 退出）"))

        # 可选导出CSV快照
        if csv_path := (Path(args.snapshot_csv) if args.snapshot_csv else None):
            try:
                write_snapshot_csv(csv_path, csv_rows)
                print(dim(f"已写CSV快照 → {csv_path}"))
            except Exception as e:
                print(red(f"[WARN] 写CSV失败：{e}"))

        try:
            time.sleep(max(0, args.interval - dur))
        except KeyboardInterrupt:
            print("\n退出监控。")
            break

if __name__ == "__main__":
    main()
