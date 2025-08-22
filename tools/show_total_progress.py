# -*- coding: utf-8 -*-
# 文件：tools/show_total_progress.py
import argparse, sqlite3, time, math, sys, os, datetime as dt
from collections import defaultdict

TF_MIN = {"5m":5, "15m":15, "30m":30, "1h":60, "2h":120, "4h":240, "1d":1440}

BAR = 40  # 进度条宽度

def eprint(*a): print(*a, file=sys.stderr)

def list_tables(con):
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in cur.fetchall()]

def parse_table(tname):
    # 期望形如 BTCUSDT_5m / ETHUSDT_1h / XXX_1d
    if "_" not in tname: return None, None
    sym, tf = tname.rsplit("_", 1)
    if tf not in TF_MIN: return None, None
    return sym, tf

def expected_rows(days, tf):
    mins = TF_MIN[tf]
    if tf == "1d":
        return days  # 一天一根
    return int((days*1440)//mins)

def count_rows_since(con, table, cutoff_ts):
    try:
        cur = con.execute(f"SELECT COUNT(1) FROM {table} WHERE timestamp >= ?", (cutoff_ts,))
        return int(cur.fetchone()[0])
    except sqlite3.OperationalError as e:
        eprint(f"[WARN] 不能统计 {table}: {e}")
        return 0

def fmt_bar(p):
    full = int(p*BAR)
    return "[" + "#"*full + "."*(BAR-full) + f"] {p*100:5.1f}%"

def human(n):
    if n >= 1_000_000: return f"{n/1_000_000:.2f}M"
    if n >= 1_000: return f"{n/1_000:.2f}K"
    return str(n)

def main():
    ap = argparse.ArgumentParser(description="显示 DB 近N天回补总进度（不影响采集进程）")
    ap.add_argument("--db", required=True, help="SQLite 路径，例如 D:\\quant_system_v2\\data\\market_data.db")
    ap.add_argument("--days", type=int, default=365, help="目标覆盖天数（默认365）")
    ap.add_argument("--tfs", default="5m,15m,30m,1h,2h,4h,1d", help="统计的周期，逗号分隔")
    ap.add_argument("--symbols-file", help="只统计文件中的 symbol（每行一个；不填则自动从表名推断）")
    ap.add_argument("--refresh", type=int, default=30, help="刷新秒数（默认30s）")
    ap.add_argument("--topk", type=int, default=10, help="显示落后 topK 表（默认10）")
    args = ap.parse_args()

    tfs = [x.strip() for x in args.tfs.split(",") if x.strip()]
    for tf in tfs:
        if tf not in TF_MIN:
            eprint(f"[FATAL] 不支持的周期: {tf}")
            return 1

    if not os.path.exists(args.db):
        eprint(f"[FATAL] DB 不存在: {args.db}")
        return 1

    con = sqlite3.connect(args.db, timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")

    # 目标时间戳（含边界）
    now = int(time.time())
    cutoff_ts = now - args.days*86400

    # 1) 发现待统计的 (symbol, tf, table)
    tables = list_tables(con)
    pairs = []
    symbol_set = None
    if args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            symbol_set = {line.strip() for line in f if line.strip()}

    for t in tables:
        sym, tf = parse_table(t)
        if not sym: continue
        if tf not in tfs: continue
        if symbol_set and sym not in symbol_set: continue
        pairs.append((sym, tf, t))

    if not pairs:
        eprint("[WARN] 没有匹配到任何表（symbol/tf 过滤过严？）")
        return 0

    # 2) 预计算期望总量
    exp_by_tf = {tf: expected_rows(args.days, tf) for tf in tfs}
    exp_total = 0
    for sym, tf, _ in pairs:
        exp_total += exp_by_tf[tf]

    # 3) 循环刷新显示
    hist_points = []
    try:
        while True:
            have_total = 0
            have_tables = 0
            lag_list = []  # (ratio, sym, tf, have, exp, table)
            tf_agg_have = defaultdict(int)
            tf_agg_exp = defaultdict(int)

            for sym, tf, table in pairs:
                exp_k = exp_by_tf[tf]
                have_k = count_rows_since(con, table, cutoff_ts)
                have_total += min(have_k, exp_k)  # 上限裁切，避免超采集导致>100%
                have_tables += 1 if have_k >= exp_k else 0
                tf_agg_have[tf] += min(have_k, exp_k)
                tf_agg_exp[tf] += exp_k
                ratio = 0 if exp_k==0 else min(have_k/exp_k, 1.0)
                lag_list.append((ratio, sym, tf, have_k, exp_k, table))

            prog = 0 if exp_total==0 else have_total/exp_total
            hist_points.append((time.time(), have_total))
            # 简单 ETA（基于最近 10 个点的斜率）
            eta_txt = "--"
            if len(hist_points) >= 3:
                last = hist_points[-10:]
                dt_sec = last[-1][0] - last[0][0]
                drows = last[-1][1] - last[0][1]
                if dt_sec > 0 and drows > 0 and have_total < exp_total:
                    rate = drows / dt_sec  # rows/sec
                    remain = exp_total - have_total
                    eta_sec = remain / rate
                    if eta_sec > 0:
                        m, s = divmod(int(eta_sec), 60)
                        h, m = divmod(m, 60)
                        eta_txt = f"{h:02d}:{m:02d}:{s:02d}"

            # 清屏
            os.system("cls" if os.name=="nt" else "clear")
            print(f"数据库: {args.db}")
            print(f"目标: 近 {args.days} 天 | 周期: {','.join(tfs)} | 表计数: {len(pairs)} | 刷新: {args.refresh}s")
            print(fmt_bar(prog), f"  总计 {human(have_total)}/{human(exp_total)} 行  | ETA: {eta_txt}")
            print(f"✔ 达标表数: {have_tables}/{len(pairs)}")

            # 每周期覆盖率
            tf_lines = []
            for tf in tfs:
                have_tf = tf_agg_have.get(tf,0)
                exp_tf  = tf_agg_exp.get(tf,0)
                r = 0 if exp_tf==0 else have_tf/exp_tf
                tf_lines.append(f"{tf}: {r*100:5.1f}%")
            print("周期覆盖:", " | ".join(tf_lines))

            # 落后TopK
            lag_list.sort(key=lambda x: x[0])  # 按覆盖率升序
            print("\n落后 Top{}: (覆盖率  已有/应有  表名)".format(args.topk))
            for ratio, sym, tf, have_k, exp_k, table in lag_list[:args.topk]:
                print(f"{ratio*100:5.1f}%  {have_k}/{exp_k}  {table}")

            # 时间戳说明
            print("\n说明：按“近 N 天”的应有K线数做总量估算；对每张表统计 WHERE timestamp>=cutoff 的已入库行数，裁切到应有上限，避免>100%。")
            print(dt.datetime.now().strftime("更新时间：%Y-%m-%d %H:%M:%S"))
            time.sleep(args.refresh)
    except KeyboardInterrupt:
        print("\n已退出。")

if __name__ == "__main__":
    sys.exit(main())
