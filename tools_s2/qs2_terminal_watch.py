# -*- coding: utf-8 -*-
"""
qs2_terminal_watch.py — 终端看板（S级）
功能：
1) 数据新鲜度热力：按 TF 统计总表数、达标数、落后率，给出示例落后表
2) 最优参数榜单：best_params 表 TopN（按 score 或收益）
3) Guardian 并发档位：解析 logs/qs2_guardian.log 最近一次并发档位
4) DB 指标：db/wal/shm 文件大小

默认新鲜度门槛（分钟）：5m=2, 15m=5, 30m=10, 1h=20, 2h=30, 4h=45, 1d=120
刷新：--refresh 秒，默认 2s
"""

import argparse, os, sys, time, sqlite3, csv, json, math
from pathlib import Path
from datetime import datetime, timezone, timedelta

TF_DEFAULT = ["5m","15m","30m","1h","2h","4h","1d"]
FRESH_LIMITS_MIN = {"5m":2, "15m":5, "30m":10, "1h":20, "2h":30, "4h":45, "1d":120}

CSI = "\033["
def color(s, fg=None, bold=False):
    if os.name == "nt":
        # 尽量启用 ANSI（Win10+）
        os.system("")  # no-op 启用VT
    codes=[]
    if bold: codes.append("1")
    if fg=="red": codes.append("31")
    elif fg=="green": codes.append("32")
    elif fg=="yellow": codes.append("33")
    elif fg=="blue": codes.append("34")
    elif fg=="magenta": codes.append("35")
    elif fg=="cyan": codes.append("36")
    if not codes: return s
    return CSI + ";".join(codes) + "m" + s + CSI + "0m"

def clear():
    sys.stdout.write(CSI+"2J"+CSI+"H"); sys.stdout.flush()

def human_size(path: Path):
    try:
        b = path.stat().st_size
    except Exception:
        return "N/A"
    units = ["B","KB","MB","GB","TB"]
    i=0
    while b>=1024 and i<len(units)-1:
        b/=1024.0; i+=1
    return f"{b:.2f} {units[i]}"

def latest_ts(con, table):
    try:
        r=con.execute(f"SELECT MAX(timestamp) FROM '{table}'").fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except Exception:
        return None

def check_freshness(db:str, tfs, limits):
    now_ms = int(time.time()*1000)
    con = sqlite3.connect(db); con.execute("PRAGMA busy_timeout=3000;")
    tbls = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    summary = []
    stale_examples=[]
    for tf in tfs:
        tf_tbls = [t for t in tbls if t.endswith("_"+tf)]
        lim_ms = limits.get(tf, 10)*60*1000
        ok_cnt=0; total=len(tf_tbls); stale_cnt=0
        for t in tf_tbls:
            mx = latest_ts(con, t)
            if mx is not None and now_ms - mx <= lim_ms:
                ok_cnt += 1
            else:
                stale_cnt += 1
                # 收集少量样本
                if len(stale_examples) < 10:
                    delay_min = (now_ms-(mx or 0))/60000.0
                    stale_examples.append((t, round(delay_min,1)))
        rate = (stale_cnt/total*100.0) if total else 0.0
        summary.append({"tf":tf, "total":total, "fresh":ok_cnt, "stale":stale_cnt, "stale_rate":rate})
    con.close()
    return summary, stale_examples

def top_best_params(db:str, top:int, order_by:str):
    con=sqlite3.connect(db); con.execute("PRAGMA busy_timeout=2000;")
    try:
        order = "score" if order_by=="score" else "metric_return"
        rows = con.execute(f"""
            SELECT symbol, timeframe, strategy, score, metric_return, metric_trades, updated_at
            FROM best_params
            ORDER BY {order} DESC NULLS LAST, metric_trades DESC
            LIMIT ?;
        """, (top,)).fetchall()
    except Exception:
        rows=[]
    con.close()
    return rows

def read_guardian_workers(logfile:Path):
    # 解析最后一次“并发升/降档 → N”或“--max-workers N”
    if not logfile.exists():
        return None
    try:
        txt = logfile.open("r", encoding="utf-8", errors="ignore").read()[-20000:]
        import re
        m = re.findall(r"并发[降升]档\s*→\s*(\d+)", txt)
        if m: return int(m[-1])
        m2 = re.findall(r"--max-workers\s+(\d+)", txt)
        if m2: return int(m2[-1])
    except Exception:
        pass
    return None

def render_table(headers, rows, widths=None):
    if not widths:
        widths=[len(h) for h in headers]
        for r in rows:
            for i,cell in enumerate(r):
                widths[i]=max(widths[i], len(str(cell)))
    line="│ " + " │ ".join(str(h).ljust(widths[i]) for i,h in enumerate(headers)) + " │"
    sep ="├-" + "-+-".join("-"*widths[i] for i,_ in enumerate(headers)) + "-┤"
    top ="┌-" + "-+-".join("-"*widths[i] for i,_ in enumerate(headers)) + "-┐"
    bot ="└-" + "-+-".join("-"*widths[i] for i,_ in enumerate(headers)) + "-┘"
    print(top)
    print(line)
    print(sep)
    for r in rows:
        print("│ " + " │ ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))) + " │")
    print(bot)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", default=r"D:\quant_system_v2\data\market_data.db")
    ap.add_argument("--timeframes", default=",".join(TF_DEFAULT))
    ap.add_argument("--refresh", type=int, default=2)
    ap.add_argument("--top", type=int, default=20, help="best_params 榜单条数")
    ap.add_argument("--order-by", default="score", choices=["score","return"], help="榜单排序字段（score/return）")
    ap.add_argument("--log-guardian", default="logs/qs2_guardian.log")
    args=ap.parse_args()

    tfs=[x.strip() for x in args.timeframes.split(",") if x.strip()]
    dbp=Path(args.db); wal=dbp.with_suffix(dbp.suffix+".wal"); shm=dbp.with_suffix(dbp.suffix+".shm")
    guardian_log=Path(args.log_guardian)

    while True:
        clear()
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(color(f"《QS2 终端看板》 {now}", "cyan", True))
        print()

        # 1) DB 基本信息
        print(color("[数据库]", "yellow", True))
        print(f"DB: {dbp}   大小: {human_size(dbp)}   WAL: {human_size(wal)}   SHM: {human_size(shm)}")
        workers = read_guardian_workers(guardian_log)
        if workers:
            print(f"采集守护并发档位：{color(str(workers),'green',True)}")
        else:
            print("采集守护并发档位：N/A（还未解析到）")
        print()

        # 2) 新鲜度汇总
        print(color("[新鲜度热力]", "yellow", True))
        summary, samples = check_freshness(str(dbp), tfs, FRESH_LIMITS_MIN)
        hdr=["TF","总表","达标","落后","落后率%"]
        rows=[]
        for s in summary:
            rate_str = f"{s['stale_rate']:.2f}"
            color_rate = "green" if s['stale_rate']<=1.0 else ("yellow" if s['stale_rate']<=5.0 else "red")
            rows.append([s["tf"], s["total"], s["fresh"], s["stale"], color(rate_str, color_rate, True)])
        render_table(hdr, rows)
        if samples:
            print("落后示例（最多 10）：", ", ".join(f"{t}({d}m)" for t,d in samples[:10]))
        else:
            print(color("所有 TF 达标 ✅", "green", True))
        print()

        # 3) best_params 榜单
        print(color("[最优参数榜单 best_params]", "yellow", True))
        order = "score" if args.order_by=="score" else "metric_return"
        bps = top_best_params(str(dbp), args.top, "score" if order=="score" else "return")
        if not bps:
            print("未查询到 best_params（可能尚未夜间寻优）")
        else:
            hdr=["#","Symbol","TF","Strategy","Score","Return%","Trades","Updated(UTC)"]
            rows=[]
            for i,(sym,tf,st,sc,ret,tr,upd) in enumerate(bps, start=1):
                scv = "-" if sc is None else f"{sc:.4f}"
                rtv = "-" if ret is None else f"{ret:.2f}"
                rows.append([i, sym, tf, st, scv, rtv, tr, upd or ""])
            render_table(hdr, rows, widths=[3,10,5,22,8,8,6,19])

        # 4) 刷新提示
        print()
        print(color(f"每 {args.refresh}s 自动刷新（Ctrl+C 退出）", "magenta"))
        try:
            time.sleep(args.refresh)
        except KeyboardInterrupt:
            print("\nBye.")
            return

if __name__=="__main__":
    main()
