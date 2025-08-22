# -*- coding: utf-8 -*-
"""
watch_db_all.py
实时监控 DB 中全部 K线表的写入情况（自动刷新、按最近更新时间排序）
用法示例：
  python tools/watch_db_all.py --db D:\quant_system_v2\data\market_data.db --interval 10 --tfs 5m,15m,30m,1h,2h,4h,1d --top 200
"""
import argparse, os, re, sqlite3, time, datetime as dt
from pathlib import Path
from typing import List, Tuple

ANSI = os.getenv("NO_COLOR") is None  # 允许关闭颜色：设置环境变量 NO_COLOR=1

def c(s, code): return f"\033[{code}m{s}\033[0m" if ANSI else s
def green(s): return c(s, "32")
def yellow(s): return c(s, "33")
def red(s): return c(s, "31")
def cyan(s): return c(s, "36")
def dim(s): return c(s, "2")

PAT = re.compile(r"^([A-Z0-9]+)_(\d+[mh]|1d)$", re.I)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite DB 路径")
    ap.add_argument("--interval", type=int, default=10, help="刷新间隔秒")
    ap.add_argument("--tfs", default="5m,15m,30m,1h,2h,4h,1d", help="仅监控这些周期，逗号分隔；留空=全部")
    ap.add_argument("--symbols", default="", help="仅监控这些符号，逗号分隔或提供txt文件路径")
    ap.add_argument("--top", type=int, default=200, help="最多展示多少行（按最近更新时间排序）")
    ap.add_argument("--age-warn", type=int, default=180, help="超此秒视为“未更新”（黄）")
    ap.add_argument("--age-bad", type=int, default=900, help="超此秒视为“陈旧”（红）")
    return ap.parse_args()

def load_symbol_filter(arg: str) -> List[str]:
    if not arg: return []
    p = Path(arg)
    if p.exists() and p.is_file():
        syms = [line.strip().upper() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        return syms
    # 逗号分隔
    return [s.strip().upper() for s in arg.split(",") if s.strip()]

def query_tables(con: sqlite3.Connection) -> List[str]:
    cur = con.cursor()
    rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_%'").fetchall()
    return [r[0] for r in rows]

def latest_for_table(con: sqlite3.Connection, table: str) -> Tuple[int,int]:
    cur = con.cursor()
    try:
        ts, cnt = cur.execute(f"SELECT MAX(timestamp), COUNT(*) FROM '{table}'").fetchone()
        return (int(ts) if ts else 0, int(cnt or 0))
    except Exception:
        return (0, 0)

def clear():
    # Windows CMD/PowerShell 兼容清屏
    os.system("cls" if os.name == "nt" else "clear")

def main():
    args = parse_args()
    db = Path(args.db)
    if not db.exists():
        print(red(f"[FATAL] DB 不存在：{db}")); return

    tf_filter = [t.strip().lower() for t in args.tfs.split(",")] if args.tfs else []
    sym_filter = load_symbol_filter(args.symbols)

    while True:
        start = time.time()
        try:
            con = sqlite3.connect(str(db), timeout=5)
        except Exception as e:
            clear()
            print(red(f"[FATAL] 无法打开 DB：{db}  err={e}"))
            time.sleep(args.interval)
            continue

        tables = query_tables(con)
        now_utc = int(time.time())
        rows = []
        for t in tables:
            m = PAT.match(t)
            if not m: continue
            sym, tf = m.group(1).upper(), m.group(2).lower()
            if tf_filter and tf not in tf_filter: 
                continue
            if sym_filter and sym not in sym_filter:
                continue
            ts, cnt = latest_for_table(con, t)
            if ts <= 0:
                age = None
            else:
                age = now_utc - ts
            rows.append((sym, tf, t, ts, age, cnt))

        con.close()

        # 按“最近更新时间”排序（ts 降序）
        rows.sort(key=lambda x: x[3] if x[3] else 0, reverse=True)
        if args.top > 0:
            rows = rows[:args.top]

        # 统计
        with_data = sum(1 for r in rows if r[3] > 0)
        stale_y = sum(1 for r in rows if r[4] is not None and args.age_warn <= r[4] < args.age_bad)
        stale_r = sum(1 for r in rows if r[4] is not None and r[4] >= args.age_bad)

        # 绘制
        clear()
        header = f"🛰  DB: {db}   监控周期: {','.join(tf_filter) if tf_filter else '全部'}   " \
                 f"筛选符号: {len(sym_filter) if sym_filter else '全部'}   刷新: {args.interval}s"
        print(cyan(header))
        print(dim(f"UTC 现在: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}   显示前 {len(rows)} 行（按最近更新时间排序）"))
        print(dim(f"统计：有数据 {with_data}  黄线 {stale_y}  红线 {stale_r}   阈值：黄 {args.age_warn}s / 红 {args.age_bad}s"))
        print("-"*108)
        print(f"{'SYMBOL':<14}{'TF':<6}{'ROWS':>8}  {'LATEST_UTC':<19}  {'AGE':>8}  STATUS")
        print("-"*108)

        for sym, tf, tname, ts, age, cnt in rows:
            if ts:
                latest = dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                if age < args.age_warn:
                    status = green("LIVE")
                elif age < args.age_bad:
                    status = yellow("STALE")
                else:
                    status = red("OLD")
                age_txt = f"{age:>6}s"
            else:
                latest = "-"
                status = red("EMPTY")
                age_txt = "   -  "
            print(f"{sym:<14}{tf:<6}{cnt:>8}  {latest:<19}  {age_txt:>8}  {status}")

        dur = time.time() - start
        print("-"*108)
        print(dim(f"刷新耗时 {dur:.2f}s   将在 {max(0, args.interval - int(dur))}s 后更新（Ctrl+C 退出）"))
        try:
            time.sleep(max(0, args.interval - dur))
        except KeyboardInterrupt:
            print("\n退出监控。")
            break

if __name__ == "__main__":
    main()
