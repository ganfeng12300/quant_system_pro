# -*- coding: utf-8 -*-
"""
验证实盘是否正确读取 best_params  —— V2
- 保底：自动建表 best_params；若表空且有 JSON，则自动 JSON→DB 同步
- 适配你的 live_trader_pro.py CLI（无 --mode/--once，SYMBOL 为位置参数）
- 运行 dry-run（paper：默认不加 --live），20s 超时收集输出
"""
import os, sys, sqlite3, datetime, subprocess, json, traceback

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = r"D:\quant_system_v2\data\market_data.db"
BEST_JSON = os.path.join(PROJECT_ROOT, "deploy", "live_best_params.json")

EXCHANGE = "binance"   # binance/bitget
SYMBOL   = "BTCUSDT"
TF       = "1h"
LIVE     = False       # True => 加 --live，False => paper（不加）

ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOGDIR = os.path.join(PROJECT_ROOT, f"logs/livecheck_{ts}")
os.makedirs(LOGDIR, exist_ok=True)
LOG = os.path.join(LOGDIR, "run.log")

def log(*args):
    msg = " ".join(str(a) for a in args)
    print(msg, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def ensure_table_and_sync():
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS best_params(
            symbol TEXT,timeframe TEXT,strategy TEXT,params_json TEXT,
            metric_return REAL,metric_trades INTEGER,score REAL,dd REAL,turnover REAL,
            updated_at TEXT, PRIMARY KEY(symbol,timeframe)
        )""")
        # 表是否为空
        cnt = con.execute("SELECT COUNT(1) FROM best_params").fetchone()[0]
        if cnt == 0 and os.path.exists(BEST_JSON):
            try:
                items = json.load(open(BEST_JSON, encoding="utf-8"))
                for it in items:
                    m = it.get("metrics") or {}
                    con.execute("""INSERT OR REPLACE INTO best_params
                        (symbol,timeframe,strategy,params_json,metric_return,metric_trades,score,dd,turnover,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))""",
                        (it.get("symbol"),
                         it.get("tf") or it.get("timeframe"),
                         it.get("strategy"),
                         json.dumps(it.get("params", {}), ensure_ascii=False),
                         m.get("return"), m.get("trades"), m.get("score"), m.get("dd"), m.get("turnover")))
                con.commit()
                log("[OK] JSON synced → best_params:", len(items))
            except Exception as e:
                log("[ERR] 解析/写入 JSON 失败：", e)
    finally:
        con.close()

def peek_best():
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute("""SELECT symbol,timeframe,strategy,
                                   substr(params_json,1,60),
                                   round(metric_return*100,2)||'%%',
                                   metric_trades, round(score,4), updated_at
                              FROM best_params ORDER BY updated_at DESC LIMIT 10""")
        any_row = False
        for r in cur:
            any_row = True
            log(r)
        if not any_row:
            log("[WARN] best_params 为空：请先跑寻优/或检查 JSON 是否生成。")
    except Exception as e:
        log("[ERR] 读取 best_params 失败：", e)
    finally:
        con.close()

def run_live_dryrun():
    script = os.path.join(PROJECT_ROOT, "live_trading", "live_trader_pro.py")
    if not os.path.exists(script):
        log("[WARN] 缺少", script, "—— 无法验证读取逻辑")
        return
    cmd = [
        sys.executable, script,
        "--db", DB_PATH,
        "--exchange", EXCHANGE,
        SYMBOL,
        "--tf", TF,
        "--strategy", "auto",
    ]
    if LIVE:
        cmd.append("--live")
    log("[STEP] 启动 dry-run：", " ".join(cmd))
    try:
        r = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=20)
        if r.stdout.strip():
            log(r.stdout.strip())
        if r.stderr.strip():
            log("[LIVE-STDERR]", r.stderr.strip())
    except subprocess.TimeoutExpired as e:
        # 20s 到时，收集到的输出也写入
        if e.stdout:
            log(e.stdout.strip())
        if e.stderr:
            log("[LIVE-STDERR]", e.stderr.strip())
        log("[INFO] dry-run 超时自动结束（这是预期，用于抓取“加载 best_params …”日志）。")
    except Exception as e:
        log("[ERR] 启动失败：", e)

def main():
    try:
        log("[INFO] DB=", DB_PATH)
        ensure_table_and_sync()
        peek_best()
        run_live_dryrun()
        log("[DONE] 验证完成。日志：", LOG)
    except Exception as e:
        log("[FATAL]", e)
        log(traceback.format_exc())
    try:
        os.startfile(LOG)  # Windows 打开记事本
    except Exception:
        pass
    input("按回车退出（窗口常驻，便于查看日志）> ")

if __name__ == "__main__":
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    main()
