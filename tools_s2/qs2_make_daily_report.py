# -*- coding: utf-8 -*-
"""
qs2_make_daily_report.py — 每日报告生成器（S级）
输出目录：reports/YYYY-MM-DD/
文件：
  - report.md（Markdown）
  - freshness_summary.csv（各TF汇总）
  - stale_samples.csv（落后样本）
  - best_params_today.csv（当日/近期最优参数摘要）
  - 引用 results/qs2_optimizer_report.json（若存在）
"""

import argparse, os, sys, csv, json, sqlite3, time
from pathlib import Path
from datetime import datetime, timedelta, timezone

TF_DEFAULT = ["5m","15m","30m","1h","2h","4h","1d"]
FRESH_LIMITS_MIN = {"5m":2, "15m":5, "30m":10, "1h":20, "2h":30, "4h":45, "1d":120}

def now_local(): return datetime.now()

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
    now_ms=int(time.time()*1000)
    con=sqlite3.connect(db); con.execute("PRAGMA busy_timeout=3000;")
    tbls=[r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    summary=[]; samples=[]
    for tf in tfs:
        tf_tbls=[t for t in tbls if t.endswith("_"+tf)]
        lim_ms = limits.get(tf,10)*60*1000
        ok_cnt=0; total=len(tf_tbls); stale_cnt=0
        for t in tf_tbls:
            mx=latest_ts(con, t)
            if mx is not None and now_ms-mx<=lim_ms:
                ok_cnt+=1
            else:
                stale_cnt+=1
                if len(samples)<500:
                    delay_min = (now_ms-(mx or 0))/60000.0
                    samples.append({"table":t,"tf":tf,"delay_min":round(delay_min,1),"max_ts":mx})
        rate=(stale_cnt/total*100.0) if total else 0.0
        summary.append({"tf":tf,"total":total,"fresh":ok_cnt,"stale":stale_cnt,"stale_rate":round(rate,2)})
    con.close()
    return summary, samples

def fetch_best_params(db:str, since_hours:int=48, limit:int=500):
    con=sqlite3.connect(db); con.execute("PRAGMA busy_timeout=2000;")
    try:
        # 兼容没有 updated_at 的情况；优先取最近48小时
        rows = con.execute("""
        SELECT symbol, timeframe, strategy, score, metric_return, metric_trades, dd, turnover, updated_at
        FROM best_params
        ORDER BY updated_at DESC
        LIMIT ?;
        """, (limit,)).fetchall()
    except Exception:
        rows=[]
    con.close()
    return rows

def write_csv(path:Path, headers, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(r)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", default=r"D:\quant_system_v2\data\market_data.db")
    ap.add_argument("--timeframes", default=",".join(TF_DEFAULT))
    ap.add_argument("--outdir", default="reports")
    ap.add_argument("--optimizer-report", default="results/qs2_optimizer_report.json")
    args=ap.parse_args()

    tfs=[x.strip() for x in args.timeframes.split(",") if x.strip()]
    dbp=Path(args.db); wal=dbp.with_suffix(dbp.suffix+".wal"); shm=dbp.with_suffix(dbp.suffix+".shm")

    day=now_local().strftime("%Y-%m-%d")
    base=Path(args.outdir)/day
    base.mkdir(parents=True, exist_ok=True)

    # 1) 新鲜度
    summary, samples = check_freshness(str(dbp), tfs, FRESH_LIMITS_MIN)
    write_csv(base/"freshness_summary.csv", ["tf","total","fresh","stale","stale_rate%"],
              [[s["tf"], s["total"], s["fresh"], s["stale"], s["stale_rate"]] for s in summary])
    write_csv(base/"stale_samples.csv", ["table","tf","delay_min","max_ts"], 
              [[x["table"], x["tf"], x["delay_min"], x["max_ts"]] for x in samples])

    # 2) best_params 摘要
    bps = fetch_best_params(str(dbp), since_hours=48, limit=2000)
    write_csv(base/"best_params_today.csv",
              ["symbol","timeframe","strategy","score","return%","trades","dd","turnover","updated_at"],
              bps)

    # 3) 组装 Markdown
    db_size = human_size(dbp); wal_size = human_size(wal); shm_size = human_size(shm)
    ok_tfs = [s for s in summary if s["stale_rate"]<=1.0]
    warn_tfs = [s for s in summary if 1.0 < s["stale_rate"] <= 5.0]
    bad_tfs = [s for s in summary if s["stale_rate"]>5.0]

    md = []
    md.append(f"# 日报 · {day}")
    md.append("")
    md.append("## 数据库")
    md.append(f"- 路径：`{dbp}`")
    md.append(f"- 大小：**{db_size}**，WAL：**{wal_size}**，SHM：**{shm_size}**")
    md.append("")
    md.append("## 新鲜度汇总（按 TF）")
    md.append("")
    md.append("| TF | 总表 | 达标 | 落后 | 落后率% |")
    md.append("|---:|----:|----:|----:|------:|")
    for s in summary:
        md.append(f"| {s['tf']} | {s['total']} | {s['fresh']} | {s['stale']} | {s['stale_rate']:.2f} |")
    md.append("")
    if samples:
        md.append(f"> 落后示例（最多 10）：{', '.join(f\"{x['table']}({x['delay_min']}m)\" for x in samples[:10])}")
    else:
        md.append("> ✅ 所有 TF 达标")
    md.append("")

    md.append("## best_params 摘要（最近条目）")
    md.append("")
    if bps:
        md.append("| Symbol | TF | Strategy | Score | Return% | Trades | updated_at |")
        md.append("|:------:|:--:|:---------|------:|--------:|-------:|:----------:|")
        for (sym,tf,st,sc,ret,tr,dd,turn,upd) in bps[:50]:
            sv = "-" if sc is None else f"{sc:.4f}"
            rv = "-" if ret is None else f"{ret:.2f}"
            md.append(f"| {sym} | {tf} | {st} | {sv} | {rv} | {tr} | {upd or ''} |")
    else:
        md.append("_暂无数据（可能尚未运行夜间寻优）_")
    md.append("")

    # 4) 引用夜间寻优 JSON 报告（如果有）
    opt_path = Path(args.optimizer_report)
    if opt_path.exists():
        try:
            j = json.loads(opt_path.read_text(encoding="utf-8"))
            md.append("## 夜间寻优快照")
            md.append("")
            md.append(f"- 报告文件：`{opt_path}`")
            md.append(f"- fresh_ok：**{j.get('fresh_ok')}**")
            md.append(f"- 生成时间（UTC）：{j.get('time')}")
            md.append("")
        except Exception:
            pass

    (base/"report.md").write_text("\n".join(md), encoding="utf-8")

    print(f"[OK] 报告已生成：{base}")
    print(" - report.md")
    print(" - freshness_summary.csv")
    print(" - stale_samples.csv")
    print(" - best_params_today.csv")

if __name__=="__main__":
    main()
