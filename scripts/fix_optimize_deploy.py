# -*- coding: utf-8 -*-
"""
机构级：修复→寻优→下发→校验（放宽参数版）
- 自检: Python/依赖/DB
- 扫描 DB 表 -> results/symbols_from_db.txt
- 与交易所上市清单求交集 -> results/symbols_whitelist.txt
- 若有 optimizer 脚本则寻优并下发到 JSON（放宽参数）
- JSON -> best_params（建表/更新）
- 抽样打印 best_params
- 全过程结构化日志 + 窗口不闪退
"""
import os, sys, re, json, sqlite3, datetime, traceback, subprocess
from typing import List, Set

# ---------- 可按需修改 ----------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = r"D:\quant_system_v2\data\market_data.db"
BEST_JSON = os.path.join(PROJECT_ROOT, "deploy", "live_best_params.json")
SYM_DB_LIST = os.path.join(PROJECT_ROOT, "results", "symbols_from_db.txt")
SYM_WHITELIST = os.path.join(PROJECT_ROOT, "results", "symbols_whitelist.txt")
EXCHANGE = "binance"       # binance / bitget
INSTRUMENT = "swap"        # swap(USDT永续) / spot
# —— 放宽参数（确保产出 JSON）——
TIMEFRAMES = ["1h", "4h"]
DAYS       = 90            # ← 从 180d 改为 90d
MIN_TRADES = 5             # ← 从 10 降到 5
MAX_DD     = 0.9           # ← 从 0.6 放宽到 0.9
# --------------------------------

# 日志
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOGDIR = os.path.join(PROJECT_ROOT, f"logs/opt_{ts}")
os.makedirs(LOGDIR, exist_ok=True)
LOG = os.path.join(LOGDIR, "run.log")

def log(*args):
    msg = " ".join(str(a) for a in args)
    print(msg, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def ensure_dirs():
    for d in ["results", "deploy"]:
        os.makedirs(os.path.join(PROJECT_ROOT, d), exist_ok=True)
    for d in ["utils", "backtest"]:
        p = os.path.join(PROJECT_ROOT, d, "__init__.py")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        if not os.path.exists(p):
            open(p, "w", encoding="utf-8").write("")

def check_env():
    log("[INFO] ROOT=", PROJECT_ROOT)
    log("[INFO] DB=", DB_PATH)
    log("[PARAMS]", f"days={DAYS} min_trades={MIN_TRADES} max_dd={MAX_DD} tfs={TIMEFRAMES}")
    # DB
    sqlite3.connect(DB_PATH).close()
    log("[DB] OK")
    # 依赖
    for m in ("pandas", "ccxt"):
        __import__(m)
        log("[OK] import", m)

def scan_db_symbols():
    con = sqlite3.connect(DB_PATH)
    names = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    con.close()
    pat = re.compile(r"^([A-Za-z0-9]+)_(1m|5m|15m|30m|1h|2h|4h|1d)$", re.I)
    by = {}
    for t in names:
        m = pat.match(t)
        if m:
            by.setdefault(m.group(1).upper(), set()).add(m.group(2))
    os.makedirs(os.path.dirname(SYM_DB_LIST), exist_ok=True)
    with open(SYM_DB_LIST, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(by)) + "\n")
    with open(os.path.join(PROJECT_ROOT, "results", "timeframes_by_symbol.json"), "w", encoding="utf-8") as f:
        json.dump({k: sorted(list(v)) for k, v in by.items()}, f, ensure_ascii=False, indent=2)
    log("[OK] symbols from DB:", len(by))

def gen_whitelist():
    import ccxt  # type: ignore
    if not os.path.exists(SYM_DB_LIST):
        raise FileNotFoundError(SYM_DB_LIST)
    ex = getattr(ccxt, EXCHANGE.lower())({"enableRateLimit": True})
    mk = ex.load_markets()
    ok: Set[str] = set()
    for s, m in mk.items():
        base, rest = s.split("/")
        quote = rest.split(":")[0]
        sym = (base + quote).upper()
        if INSTRUMENT.lower() == "swap":
            if m.get("swap") and m.get("settle") == "USDT":
                ok.add(sym)
        else:  # spot
            if not m.get("swap") and quote == "USDT":
                ok.add(sym)
    res: List[str] = []
    with open(SYM_DB_LIST, encoding="utf-8") as f:
        for line in f:
            t = line.strip().upper()
            if t and not t[0].isdigit() and t in ok:
                res.append(t)
    os.makedirs(os.path.dirname(SYM_WHITELIST), exist_ok=True)
    with open(SYM_WHITELIST, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(set(res))) + "\n")
    log(f"[OK] {EXCHANGE}/{INSTRUMENT} match:", len(set(res)))

def run_optimizer_if_exists():
    script = os.path.join(PROJECT_ROOT, "optimizer", "a1a8_optimizer_and_deploy.py")
    if not os.path.exists(script):
        log("[WARN] 缺少", script, "—— 跳过寻优，仅做 JSON→DB 同步")
        return
    cmd = [
        sys.executable, script,
        "--db", DB_PATH,
        "--symbols-file", SYM_WHITELIST,       # 白名单文件
        "--timeframes", *TIMEFRAMES,
        "--days", str(DAYS),
        "--min-trades", str(MIN_TRADES),
        "--max-dd", str(MAX_DD),
        "--deploy",
        "--json", BEST_JSON,
    ]
    log("[STEP] 运行寻优：", " ".join(cmd))
    try:
        r = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
        if r.stdout.strip():   log(r.stdout.strip())
        if r.stderr.strip():   log("[OPT-STDERR]", r.stderr.strip())
    except Exception as e:
        log("[ERR] 优化器执行失败：", e)

def sync_json_to_db_and_sample():
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """CREATE TABLE IF NOT EXISTS best_params(
           symbol TEXT,timeframe TEXT,strategy TEXT,params_json TEXT,
           metric_return REAL,metric_trades INTEGER,score REAL,dd REAL,turnover REAL,
           updated_at TEXT, PRIMARY KEY(symbol,timeframe)
        )"""
    )
    if os.path.exists(BEST_JSON) and os.path.getsize(BEST_JSON) > 0:
        try:
            items = json.load(open(BEST_JSON, encoding="utf-8"))
            for it in items:
                m = it.get("metrics") or {}
                con.execute(
                    """INSERT OR REPLACE INTO best_params
                       (symbol,timeframe,strategy,params_json,metric_return,metric_trades,score,dd,turnover,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))""",
                    (
                        it.get("symbol"),
                        it.get("tf") or it.get("timeframe"),
                        it.get("strategy"),
                        json.dumps(it.get("params", {}), ensure_ascii=False),
                        m.get("return"), m.get("trades"), m.get("score"), m.get("dd"), m.get("turnover"),
                    ),
                )
            con.commit()
            log("[OK] JSON synced → best_params:", len(items))
        except Exception as e:
            log("[ERR] 解析/写入 JSON 失败：", e)
    else:
        log("[WARN] 未发现或为空：", BEST_JSON)

    try:
        import pandas as pd  # type: ignore
        df = pd.read_sql(
            """SELECT symbol,timeframe,strategy,
                      substr(params_json,1,60) AS params,
                      round(metric_return*100,2)||'%%' AS ret,
                      metric_trades, round(score,4) AS score, updated_at
               FROM best_params ORDER BY updated_at DESC LIMIT 12""",
            con,
        )
        log(df.to_string(index=False))
    except Exception as e:
        log("[WARN] 抽样读取 best_params 失败：", e)
    finally:
        con.close()

def main():
    try:
        ensure_dirs()
        check_env()
        scan_db_symbols()
        gen_whitelist()
        run_optimizer_if_exists()
        sync_json_to_db_and_sample()
        log("[DONE] 全流程完成。日志：", LOG)
    except Exception as e:
        log("[FATAL]", e)
        log(traceback.format_exc())
    try:
        os.startfile(LOG)  # Windows 打开日志
    except Exception:
        pass
    input("按回车退出（窗口常驻，便于查看日志）> ")

if __name__ == "__main__":
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    main()
