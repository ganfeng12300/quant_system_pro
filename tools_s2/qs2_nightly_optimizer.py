# -*- coding: utf-8 -*-
"""
qs2_nightly_optimizer.py — 夜间寻优总控（CST 17:30 触发为宜）
流程：校验 DB 新鲜度 → 调用 a1a8_optimizer_and_deploy.py → 生成新 JSON 产物 → 同步 best_params 表 → 产出日报快照
"""
import argparse, os, sys, json, sqlite3, subprocess, time
from datetime import datetime, timedelta
from pathlib import Path

DEFAULT_TFS = ["5m","15m","30m","1h","2h","4h","1d"]  # 排除 1m
FRESH_LIMITS_MIN = {"5m":2, "15m":5, "30m":10, "1h":20, "2h":30, "4h":45, "1d":120}

def now_ts(): return int(time.time()*1000)

def latest_ts_for_table(con, table):
    try:
        cur=con.cursor()
        r=cur.execute(f"SELECT MAX(timestamp) FROM '{table}'").fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except Exception:
        return None

def check_freshness(db:str, tfs, margin_map):
    con=sqlite3.connect(db); con.execute("PRAGMA busy_timeout=5000;")
    ok=True; detail=[]
    # 粗略根据表名约定：SYMBOL_TF
    tbls=[r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    now_ms=now_ts()
    for tf in tfs:
        tf_tbls=[t for t in tbls if t.endswith("_"+tf)]
        if not tf_tbls: detail.append({"tf":tf,"status":"no_tables"}); ok=False; continue
        lim_min = margin_map.get(tf, 10)
        lim_ms = lim_min*60*1000
        tf_ok=True; stale=[]
        for t in tf_tbls:
            mx=latest_ts_for_table(con, t)
            if mx is None or (now_ms - mx) > lim_ms:
                tf_ok=False
                stale.append({"table":t, "delay_min": round((now_ms-(mx or 0))/60000,1)})
        detail.append({"tf":tf, "ok":tf_ok, "limit_min":lim_min, "stale":stale[:5]})
        if not tf_ok: ok=False
    con.close()
    return ok, detail

def ensure_best_params_table(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS best_params(
      symbol TEXT, timeframe TEXT, strategy TEXT, params_json TEXT,
      metric_return REAL, metric_trades INTEGER, score REAL, dd REAL, turnover REAL,
      updated_at TEXT,
      PRIMARY KEY(symbol,timeframe)
    );
    """)

def upsert_best_params(con, records):
    ensure_best_params_table(con)
    cur=con.cursor()
    for it in records:
        cur.execute("""INSERT OR REPLACE INTO best_params
        (symbol,timeframe,strategy,params_json,metric_return,metric_trades,score,dd,turnover,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,datetime('now','utc'))""", (
            it["symbol"], it.get("tf") or it.get("timeframe"), it["strategy"],
            json.dumps(it.get("params",{}), ensure_ascii=False),
            (it.get("metrics") or {}).get("return"),
            (it.get("metrics") or {}).get("trades"),
            it.get("score") or (it.get("metrics") or {}).get("score"),
            (it.get("metrics") or {}).get("dd"),
            (it.get("metrics") or {}).get("turnover"),
        ))
    con.commit()

def run_optimizer(project_root:Path, db:str, tfs, out_json:str, days:int, min_trades:int, max_dd:float, symbols_file:str=None):
    py=sys.executable
    opt = project_root/"optimizer"/"a1a8_optimizer_and_deploy.py"
    if not opt.exists(): raise SystemExit(f"optimizer not found: {opt}")
    args=[py, str(opt),
          "--db", db,
          "--json", out_json,
          "--days", str(days),
          "--min-trades", str(min_trades),
          "--max-dd", str(max_dd),
          "--deploy"]
    if symbols_file:
        args += ["--symbols-file", symbols_file]
    if tfs:
        args += ["--timeframes"] + tfs
    print("▶ 启动寻优："," ".join(args))
    p=subprocess.run(args, cwd=str(project_root), text=True)
    if p.returncode!=0: raise SystemExit(f"optimizer failed: {p.returncode}")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--timeframes", type=str, default=",".join(DEFAULT_TFS))
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--min-trades", type=int, default=1)
    ap.add_argument("--max-dd", type=float, default=10000.0)
    ap.add_argument("--symbols-file", type=str, default="results/symbols_binance_perp.txt")
    ap.add_argument("--out-json", type=str, default="deploy/qs2_live_best_params.json")
    ap.add_argument("--report", type=str, default="results/qs2_optimizer_report.json")
    ap.add_argument("--force", action="store_true", help="忽略新鲜度直接运行（不建议）")
    args=ap.parse_args()

    project_root=Path(__file__).resolve().parents[1]
    Path(project_root/"deploy").mkdir(exist_ok=True)
    Path(project_root/"results").mkdir(exist_ok=True)

    tfs=[x.strip() for x in args.timeframes.split(",") if x.strip()]
    ok, detail = check_freshness(args.db, tfs, FRESH_LIMITS_MIN)
    snapshot={"time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
              "db": args.db, "fresh_ok": ok, "detail": detail}
    Path(args.report).write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    if not ok and not args.force:
        print("❌ 新鲜度未达标，已生成报告：", args.report)
        sys.exit(2)

    run_optimizer(project_root, args.db, tfs, args.out_json, args.days, args.min_trades, args.max_dd, args.symbols_file)

    # 验证产物并落库
    if not Path(args.out_json).exists():
        raise SystemExit(f"missing output json: {args.out_json}")
    data=json.loads(Path(args.out_json).read_text(encoding="utf-8"))
    # data 可能是 list 或 dict 包裹
    if isinstance(data, dict) and "records" in data:
        records=data["records"]
    elif isinstance(data, list):
        records=data
    else:
        # 尝试兼容 {symbol:{tf:{...}}} 之类（按需展开）
        tmp=[]
        for v in (data.values() if isinstance(data, dict) else []):
            if isinstance(v, dict):
                for vv in v.values():
                    if isinstance(vv, dict) and "strategy" in vv:
                        tmp.append(vv)
        records=tmp

    con=sqlite3.connect(args.db); con.execute("PRAGMA busy_timeout=5000;")
    upsert_best_params(con, records)
    con.close()

    print("✅ 夜间寻优完成：", args.out_json)
    print("📝 报告：", args.report)

if __name__=="__main__":
    main()
