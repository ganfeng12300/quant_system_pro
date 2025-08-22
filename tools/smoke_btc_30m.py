# -*- coding: utf-8 -*-
"""
冒烟测试：BTCUSDT · 30m · 近30天
流程：
- 快照（SQLite在线备份）
- 回测（仅 BTCUSDT_30m / 30天）
- 从分数表提取最佳参数 -> deploy/live_best_params.json
- 写入 deploy/qs2_live_symbols.txt（只含 BTCUSDT）
- 启动实盘（paper）并采用最佳参数
"""
import sqlite3, subprocess, sys, time, csv, json, os
from pathlib import Path
from datetime import datetime

# ===== 路径与参数 =====
PROJ   = Path(r"D:\quant_system_pro (3)\quant_system_pro")
DB_MAIN= Path(r"D:\quant_system_v2\data\market_data.db")
DB_SNAP= Path(r"D:\quant_system_v2\data\market_data_snapshot.db")

SYMBOL      = "BTCUSDT"
TF          = "30m"
BACKTEST_DAYS = 30
TOPK          = 8       # 回测内寻优的候选数，越大越慢
SCORES_WAIT_S = 1800    # 最长等回测分数表秒数（30分钟）
POLL_S        = 5

LOG_DIR = PROJ / "logs"
RESULTS = PROJ / "results"
DEPLOY  = PROJ / "deploy"
LOG_DIR.mkdir(exist_ok=True); DEPLOY.mkdir(exist_ok=True)

def log(msg):
    print(f"{datetime.now().strftime('%H:%M:%S')} | {msg}", flush=True)

def make_snapshot_sqlite(src: Path, dst: Path):
    try:
        uri = f"file:{src.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30) as con_src:
            try:
                con_src.execute("PRAGMA wal_checkpoint(FULL);")
            except Exception:
                pass
            with sqlite3.connect(str(dst), timeout=60) as con_dst:
                con_src.backup(con_dst)
        log(f"[OK] 快照已生成：{dst}")
        return True
    except Exception as e:
        log(f"[WARN] 快照失败（在线备份）：{e}")
        return False

def run_block(title, cmd, cwd):
    log(f"[RUN] {title}: {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if r.returncode == 0:
        log(f"[OK] {title} 完成")
    else:
        log(f"[ERR] {title} 失败 rc={r.returncode}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")
    return r.returncode, r.stdout, r.stderr

def run_async(title, cmd, cwd, logfile: Path):
    log(f"[START] {title}: {' '.join(cmd)}")
    with logfile.open("w", encoding="utf-8") as f:
        subprocess.Popen(cmd, cwd=cwd, stdout=f, stderr=subprocess.STDOUT)

def find_latest_scores(results_dir: Path):
    if not results_dir.exists():
        return None
    cands = sorted(results_dir.glob("a6_strategy_scores*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def wait_scores(timeout=SCORES_WAIT_S):
    log(f"[WAIT] 等待回测分数表 a6_strategy_scores*.csv (≤{timeout}s)")
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        p = find_latest_scores(RESULTS)
        if p and p != last:
            log(f"[OK] 发现分数表：{p}")
            return p
        time.sleep(POLL_S)
    return None

def pick_best_params(scores_csv: Path, symbol: str, tf: str):
    """
    从分数表里找 (symbol, tf) 的最高分行，解析 params 字段
    返回：{"symbol":..,"tf":..,"strategy":..,"params":{...}} 或 None
    """
    best_row = None
    best_score = -1e18
    with scores_csv.open("r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            sym = r.get("symbol") or r.get("Symbol") or ""
            tfx = (r.get("timeframe") or r.get("tf") or "").lower()
            if sym != symbol or tfx != tf.lower():
                continue
            score = r.get("score") or r.get("Score") or r.get("metric_score") or "0"
            try:
                sc = float(str(score).replace("%",""))
            except:
                sc = 0.0
            if sc > best_score:
                best_score = sc
                best_row = r
    if not best_row:
        return None
    strat = best_row.get("strategy") or best_row.get("Strategy") or ""
    ptxt  = (best_row.get("params") or best_row.get("Params") or "{}").strip()
    try:
        params = json.loads(ptxt) if ptxt.startswith("{") else {}
    except:
        params = {}
    return {"symbol": symbol, "tf": tf, "strategy": strat, "params": params}

def main():
    # 0) 基本检查
    for rel in [
        "backtest/backtest_pro.py",
        "live_trading/execution_engine_binance_ws.py",
        "tools/rt_updater_with_banner.py",
    ]:
        if not (PROJ/rel).exists():
            log(f"[FATAL] 缺文件：{rel}"); return

    if not DB_MAIN.exists():
        log(f"[FATAL] 主库不存在：{DB_MAIN}"); return

    # 1) 快照
    make_snapshot_sqlite(DB_MAIN, DB_SNAP)

    # 2) 启动采集（异步；保证最新K线持续写库）
    run_async("采集", [
        sys.executable, "-u", "tools/rt_updater_with_banner.py",
        "--db", str(DB_MAIN),
        "--backfill-days", "365",
        "--max-workers", "8",
        "--interval", "30",
    ], PROJ, LOG_DIR/"smoke_collector.log")

    # 3) 回测（仅 BTCUSDT_30m / 近30天；基于快照）
    rc, _, _ = run_block("回测(BTCUSDT 30m 30d)", [
        sys.executable, "-u", "backtest/backtest_pro.py",
        "--db", str(DB_SNAP),
        "--symbols", SYMBOL,
        "--tfs", TF,
        "--days", str(BACKTEST_DAYS),
        "--topk", str(TOPK),
        "--outdir", "results"
    ], PROJ)
    if rc != 0:
        log("[FATAL] 回测失败，终止冒烟测试"); return

    # 4) 等待分数表 → 生成最优参数 & 选币文件
    scores = wait_scores()
    if not scores:
        log("[FATAL] 等不到 a6_strategy_scores*.csv；请先查 logs/backtest_live.log 或 results/ 目录"); return

    best = pick_best_params(scores, SYMBOL, TF)
    if not best:
        log(f"[FATAL] 分数表中未找到 {SYMBOL}_{TF} 的记录"); return

    # 写入 best_params.json（仅此一条）
    best_json = [best]
    (DEPLOY/"live_best_params.json").write_text(json.dumps(best_json, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[OK] 写入最佳参数：{DEPLOY/'live_best_params.json'}")
    # 写入 live_symbols.txt（仅 BTCUSDT）
    (DEPLOY/"qs2_live_symbols.txt").write_text(SYMBOL+"\n", encoding="utf-8")
    log(f"[OK] 写入选币列表：{DEPLOY/'qs2_live_symbols.txt'} (仅 {SYMBOL})")

    # 5) 启动实盘（paper）读取最佳参数（异步）
    run_async("实盘（paper）", [
        sys.executable, "-u", "live_trading/execution_engine_binance_ws.py",
        "--db", str(DB_MAIN),
        "--mode", "paper",
        "--best-params", "deploy/live_best_params.json",
        "--ui-rows", "30"
    ], PROJ, LOG_DIR/"smoke_live.log")

    log("✅ 冒烟测试链路已拉起：采集 → 回测(BTC30m30d) → 最优参数 → 实盘（paper）")
    log("👉 日志：logs/smoke_collector.log、logs/smoke_live.log；回测产物在 results/")
    log("⚠️ 切换真仓：把上面启动命令里的 --mode 改成 real（先小仓验证）")

if __name__ == "__main__":
    main()
