# -*- coding: utf-8 -*-
"""
桥接适配器（无侵入）：
- 启动你现有优化器 optimizer\a1a8_optimizer_and_deploy.py（原样参数）
- 实时捕获控制台输出，解析 [BEST] 行
- 将结果 upsert 到 SQLite: best_params / best_params_meta（仅当次键）
- S 级闸门（DD 自适应、交易数门槛、收益为正、DD非负）
- 生成 deploy\run_config.json（含口径/窗口/哈希/DB指纹）
- 生成 deploy\live_best_params.json（备份 .bak，强一致校验，失败自动回滚）
- 导出 deploy\最佳参数表_A1A8.csv（中文表头；只含达标&已批准）
"""
import argparse, os, re, json, sqlite3, time, datetime as dt, subprocess, hashlib, ast, shutil, sys

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
DEPLOY_DIR = os.path.join(ROOT, "deploy")
LOG_DIR = os.path.join(ROOT, "logs")
os.makedirs(DEPLOY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

BEST_RE = re.compile(
    r'^\[BEST\]\s+'
    r'(?P<symbol>\S+)\s+'
    r'(?P<tf>(?:1m|5m|15m|30m|1h|2h|4h|1d))\s+'
    r'(?P<strategy>[A-Za-z0-9_]+)\s+'
    r'(?P<params>\{.*\})\s+'
    r'ret=(?P<ret>[-+]?\d+(?:\.\d+)?)%\s+'
    r'trades=(?P<trades>\d+)\s+'
    r'score=(?P<score>[-+]?\d+(?:\.\d+)?)\s+'
    r'dd=(?P<dd>[-+]?\d+(?:\.\d+)?)\s*$'
)

def sha256_file(path):
    h = hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

def db_fingerprint(con: sqlite3.Connection):
    cur=con.cursor()
    try:
        pc = cur.execute("PRAGMA page_count").fetchone()[0]
        ps = cur.execute("PRAGMA page_size").fetchone()[0]
    except Exception:
        pc=ps=0
    try:
        tables = [r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1")]
    except Exception:
        tables=[]
    sig = hashlib.sha256(",".join(tables).encode('utf-8')).hexdigest()[:8]
    return {"page_bytes": pc*ps, "tables": len(tables), "schema_sig": sig}

def ensure_tables(con: sqlite3.Connection):
    con.execute("""CREATE TABLE IF NOT EXISTS best_params(
        symbol TEXT, timeframe TEXT, strategy TEXT, params_json TEXT,
        metric_return REAL, metric_trades INTEGER, score REAL, dd REAL,
        turnover REAL, updated_at TEXT,
        PRIMARY KEY(symbol,timeframe)
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS best_params_meta(
        symbol TEXT, timeframe TEXT,
        eligible_live INTEGER, approved_live INTEGER,
        dd_small REAL, dd_raw REAL,
        fee_bps REAL, slip_bps REAL,
        exec_lag_bars INTEGER, no_intrabar INTEGER,
        window_days INTEGER, timeframes TEXT,
        version_hash TEXT, db_fingerprint TEXT,
        run_id TEXT, updated_at TEXT,
        PRIMARY KEY(symbol,timeframe)
    )""")
    con.commit()

def parse_params_dict(s: str):
    # 兼容单引号/None/True False
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # 退化尝试 JSON
    try:
        return json.loads(s)
    except Exception:
        return {}

def dd_to_small_nonneg(dd_raw: float):
    # 自适应单位：>1 视为百分数
    dd_abs = abs(dd_raw)
    dd_small = dd_abs/100.0 if dd_abs>1 else dd_abs
    # 负零处理
    if dd_small < 1e-12: dd_small = 0.0
    return dd_small

def timeframe_min_trades(tf, dflt, t1h, t4h):
    if tf == "1h" and t1h is not None: return t1h
    if tf == "4h" and t4h is not None: return t4h
    return dflt

def upsert(con, items, args, version_hash, db_fp, run_id):
    ensure_tables(con)
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for it in items:
        s, tf, strat = it["symbol"], it["tf"], it["strategy"]
        params_json = json.dumps(it["params"], ensure_ascii=False, separators=(',',':'))
        ret = float(it["ret_pct"])/100.0  # 存小数收益
        trades = int(it["trades"])
        score = float(it["score"])
        dd_raw = float(it["dd"])
        dd_small = dd_to_small_nonneg(dd_raw)

        con.execute("INSERT OR REPLACE INTO best_params(symbol,timeframe,strategy,params_json,metric_return,metric_trades,score,dd,turnover,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (s, tf, strat, params_json, ret, trades, score, dd_raw, None, now))

        # S级闸门（达标）
        min_tr = timeframe_min_trades(tf, args.min_trades_default, args.min_trades_1h, args.min_trades_4h)
        eligible = (trades >= min_tr) and (ret > 0.0) and (dd_small <= args.max_dd_cap + 1e-12) and (dd_raw >= -1e-9)
        approved = 1 if args.approve_all else 0

        con.execute("""INSERT OR REPLACE INTO best_params_meta
            (symbol,timeframe,eligible_live,approved_live,dd_small,dd_raw,
             fee_bps,slip_bps,exec_lag_bars,no_intrabar,window_days,timeframes,
             version_hash,db_fingerprint,run_id,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (s, tf, int(eligible), int(approved), dd_small, dd_raw,
             args.fee_bps, args.slip_bps, args.exec_lag, int(args.no_intrabar),
             args.days, ",".join(args.timeframes), version_hash,
             json.dumps(db_fp, separators=(',',':')), run_id, now))
    con.commit()

def write_run_config(args, version_hash, db_fp, run_id):
    cfg = {
        "run_id": run_id,
        "fee_bps": args.fee_bps,
        "slip_bps": args.slip_bps,
        "exec_lag_bars": args.exec_lag,
        "no_intrabar": bool(args.no_intrabar),
        "window_days": args.days,
        "timeframes": args.timeframes,
        "symbols_file": args.symbols_file,
        "optimizer_script": args.optimizer_script,
        "optimizer_python": args.optimizer_python,
        "version_hash": version_hash,
        "db_fingerprint": db_fp,
        "created_at_utc": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    p = os.path.join(DEPLOY_DIR, "run_config.json")
    with open(p,"w",encoding="utf-8") as f: json.dump(cfg,f,ensure_ascii=False,indent=2)
    return p

def gen_live_json_and_consistency(con, out_json):
    # 只取达标且已批准
    rows = con.execute("""
        SELECT p.symbol, p.timeframe, p.strategy, p.params_json,
               p.metric_return, p.metric_trades, p.score, p.dd
        FROM best_params p
        JOIN best_params_meta m ON p.symbol=m.symbol AND p.timeframe=m.timeframe
        WHERE m.eligible_live=1 AND m.approved_live=1
        ORDER BY p.symbol, p.timeframe
    """).fetchall()
    items=[]
    for s,tf,strat,params_json,ret,trs,score,dd_raw in rows:
        try:
            params = json.loads(params_json)
        except Exception:
            params = {}
        items.append({
            "symbol": s,
            "tf": tf,
            "strategy": strat,
            "params": params,
            "metrics": {
                "return": float(ret),      # 小数 0.25=25%
                "trades": int(trs),
                "score": float(score),
                "dd": float(dd_raw)        # 原始口径，保留
            }
        })
    # 备份旧文件
    if os.path.exists(out_json):
        bak = out_json + ".bak"
        try:
            shutil.copy2(out_json, bak)
        except Exception:
            pass
    with open(out_json,"w",encoding="utf-8") as f:
        json.dump(items,f,ensure_ascii=False,indent=2)

    # 强一致校验（以 DB 为准）
    j = json.load(open(out_json,encoding="utf-8"))
    key_db = [(x[0],x[1]) for x in rows]
    key_js = [(d["symbol"], d.get("tf") or d.get("timeframe")) for d in j]
    if key_db != key_js or len(j)!=len(rows):
        # 回滚
        if os.path.exists(out_json+".bak"):
            shutil.copy2(out_json+".bak", out_json)
        raise RuntimeError("JSON↔DB 不一致，已回滚 .bak")

    return len(items)

def export_cn(con, out_csv):
    headers = ["交易对","周期","策略","参数","收益(小数)","交易次数","评分","最大回撤(原口径)","达标","已批准","更新时间"]
    rows = con.execute("""
        SELECT p.symbol, p.timeframe, p.strategy, p.params_json,
               p.metric_return, p.metric_trades, p.score, p.dd,
               m.eligible_live, m.approved_live, p.updated_at
        FROM best_params p
        LEFT JOIN best_params_meta m ON p.symbol=m.symbol AND p.timeframe=m.timeframe
        WHERE m.eligible_live=1 AND m.approved_live=1
        ORDER BY p.symbol, p.timeframe
    """).fetchall()
    with open(out_csv,"w",encoding="utf-8") as f:
        f.write(",".join(headers)+"\n")
        for s,tf,strat,params_json,ret,trs,score,dd_raw,elig,appr,upd in rows:
            f.write(",".join([
                s, tf, strat,
                json.dumps(json.loads(params_json),ensure_ascii=False, separators=(',',':')),
                f"{ret:.6f}", str(trs), f"{score:.6f}", f"{dd_raw:.4f}",
                str(elig or 0), str(appr or 0), (upd or "")
            ])+"\n")
    return len(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols-file", required=True)
    ap.add_argument("--timeframes", nargs="+", required=True)
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--min-trades-default", type=int, default=10)
    ap.add_argument("--min-trades-1h", type=int, default=10)
    ap.add_argument("--min-trades-4h", type=int, default=10)
    ap.add_argument("--max-dd-cap", type=float, default=0.6)  # 小数口径（0.6=60%）
    ap.add_argument("--approve-all", action="store_true", default=True)  # 默认先放行，可按需关
    ap.add_argument("--fee-bps", type=float, default=5.0)
    ap.add_argument("--slip-bps", type=float, default=2.0)
    ap.add_argument("--exec-lag", type=int, default=1)
    ap.add_argument("--no-intrabar", action="store_true", default=True)
    ap.add_argument("--json", default=os.path.join(DEPLOY_DIR,"live_best_params.json"))

    # 优化器调用
    ap.add_argument("--optimizer-python", default=sys.executable)
    ap.add_argument("--optimizer-script", default=os.path.join(ROOT, "optimizer", "a1a8_optimizer_and_deploy.py"))
    ap.add_argument("--optimizer-min-trades", type=int, default=1)   # 让上游尽量多产出
    ap.add_argument("--optimizer-max-dd", type=float, default=10000) # 上游尽量不挡
    args = ap.parse_args()

    # 运行优化器 & 解析 [BEST]
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOG_DIR, f"opt_bridge_{ts}.log")
    cmd = [
        args.optimizer_python,
        args.optimizer_script,
        "--db", args.db,
        "--symbols-file", args.symbols_file,
        "--json", os.path.join(DEPLOY_DIR, "_raw_best_params.json"),
        "--timeframes", *args.timeframes,
        "--days", str(args.days),
        "--min-trades", str(args.optimizer_min_trades),
        "--max-dd", str(args.optimizer_max_dd),
        "--deploy"
    ]
    print(f"▶ 启动优化器：{' '.join([c if ' ' not in c else repr(c) for c in cmd])}")
    items=[]
    with open(log_path,"w",encoding="utf-8") as lf:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="ignore")
        while True:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None: break
                time.sleep(0.02); continue
            sys.stdout.write(line); lf.write(line)
            m = BEST_RE.match(line.strip())
            if m:
                d = m.groupdict()
                items.append({
                    "symbol": d["symbol"],
                    "tf": d["tf"],
                    "strategy": d["strategy"],
                    "params": parse_params_dict(d["params"]),
                    "ret_pct": float(d["ret"]),
                    "trades": int(d["trades"]),
                    "score": float(d["score"]),
                    "dd": float(d["dd"]),
                })
    rc = proc.returncode or 0
    if rc != 0:
        raise SystemExit(f"优化器退出码 {rc}，请查看日志：{log_path}")

    # 没抓到 BEST，也给一次兜底：尝试读取 _raw_best_params.json
    if not items:
        raw_json = os.path.join(DEPLOY_DIR, "_raw_best_params.json")
        if os.path.exists(raw_json):
            try:
                arr = json.load(open(raw_json,encoding="utf-8"))
                for it in arr:
                    items.append({
                        "symbol": it.get("symbol"),
                        "tf": it.get("tf") or it.get("timeframe"),
                        "strategy": it.get("strategy"),
                        "params": it.get("params") or {},
                        "ret_pct": float((it.get("metrics") or {}).get("return", 0))*100.0,
                        "trades": int((it.get("metrics") or {}).get("trades", 0)),
                        "score": float((it.get("metrics") or {}).get("score", 0)),
                        "dd": float((it.get("metrics") or {}).get("dd", 0)),
                    })
            except Exception:
                pass

    print(f"🟢 捕获 BEST 条目：{len(items)}  （日志：{log_path}）")

    # 将 BEST 入库 + S级闸门 + meta
    con = sqlite3.connect(args.db)
    ensure_tables(con)
    v1 = sha256_file(args.optimizer_script) if os.path.exists(args.optimizer_script) else "0"*64
    v2 = sha256_file(__file__)
    version_hash = f"{v1[:8]}+{v2[:8]}"
    db_fp = db_fingerprint(con)
    run_id = ts

    if items:
        upsert(con, items, args, version_hash, db_fp, run_id)
    else:
        # 没有候选也要写 run_config.json，保持可审计
        pass

    # 写 run_config.json
    cfg_path = write_run_config(args, version_hash, db_fp, run_id)
    print("🟢 已写入 run_config.json ->", cfg_path)

    # 生成 live JSON（强一致 + 可回滚）
    count = gen_live_json_and_consistency(con, args.json)
    print(f"🟢 实盘清单：{args.json}  条目数={count}")

    # 导出中文优秀结果表（只含达标&已批准）
    out_csv = os.path.join(DEPLOY_DIR, "最佳参数表_A1A8.csv")
    cnt_cn = export_cn(con, out_csv)
    print(f"🟢 中文优秀表 -> {out_csv}  条目数={cnt_cn}")

    # 结束
    con.close()
    if count==0:
        print("🟠 提示：当前闸门下为 0，可调整 --max-dd-cap / --min-trades-* 或先手动审核。")
    print("✅ 桥接流程完成：DB↔JSON一致｜meta更新｜中文导出｜可回滚")

if __name__ == "__main__":
    main()
