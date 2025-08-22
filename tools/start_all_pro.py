# -*- coding: utf-8 -*-
"""
一键检测 + 启动全流程 (专门适配路径: D:\quant_system_pro (3)\quant_system_pro)
- 等待回测产出 a6_strategy_scores*.csv 后再执行选币与导出最优参数
"""

import subprocess, sys, os, datetime as dt, shutil, time
from pathlib import Path

# ========= 固定路径 =========
PROJ = Path(r"D:\quant_system_pro (3)\quant_system_pro")
DB_MAIN = Path(r"D:\quant_system_v2\data\market_data.db")
DB_SNAP = Path(r"D:\quant_system_v2\data\market_data_snapshot.db")

BACKFILL_DAYS = 365
MAX_WORKERS   = 8
RT_INTERVAL   = 30
BTEST_DAYS    = 180
BTEST_TOPK    = 40
LIVE_TOP      = 20
MODE          = "paper"       # 或 "real"
SCORES_WAIT_SEC = 600         # ⏳ 等待回测分数文件的最大秒数（默认 10 分钟）
POLL_SEC        = 5           # 轮询间隔

# ========= 日志 =========
LOG_DIR = PROJ / "logs"
LOG_DIR.mkdir(exist_ok=True)
stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
START_LOG = LOG_DIR / f"starter_{stamp}.log"

def log(msg):
    ts = dt.datetime.now().strftime("%H:%M:%S")
    line = f"{ts} | {msg}"
    print(line, flush=True)
    with open(START_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========= 自检 =========
def check_env():
    ok = True
    if not DB_MAIN.exists():
        log(f"[FATAL] 主库不存在: {DB_MAIN}"); ok = False
    for rel in [
        "tools/rt_updater_with_banner.py",
        "backtest/backtest_pro.py",
        "tools/qs2_pick_live_syms.py",
        "live_trading/execution_engine_binance_ws.py",
    ]:
        if not (PROJ/rel).exists():
            log(f"[FATAL] 缺文件: {rel}"); ok = False
    return ok

def make_snapshot():
    try:
        shutil.copy2(DB_MAIN, DB_SNAP)
        for ext in (".wal",".shm"):
            p = str(DB_MAIN)+ext
            if os.path.exists(p):
                shutil.copy2(p, str(DB_SNAP)+ext)
        log(f"[OK] 已生成快照: {DB_SNAP}")
    except Exception as e:
        log(f"[WARN] 快照生成失败: {e}")

def run_async(title, cmd, logfile):
    log(f"[START] {title}: {' '.join(cmd)}")
    with open(logfile,"w",encoding="utf-8") as f:
        subprocess.Popen(cmd, cwd=PROJ, stdout=f, stderr=subprocess.STDOUT)

def run_block(title, cmd):
    log(f"[RUN] {title}: {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=PROJ, text=True, capture_output=True)
    if res.returncode==0:
        log(f"[OK] {title} 完成")
    else:
        log(f"[ERR] {title} 失败 rc={res.returncode}\n{res.stdout}\n{res.stderr}")

def find_latest_scores():
    results = PROJ / "results"
    if not results.exists():
        return None
    cands = sorted(results.glob("a6_strategy_scores*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def wait_for_scores(timeout_sec=SCORES_WAIT_SEC, poll=POLL_SEC):
    log(f"[WAIT] 等待回测分数表出现 a6_strategy_scores*.csv（最长 {timeout_sec}s）...")
    deadline = time.time() + timeout_sec
    last_seen = None
    while time.time() < deadline:
        p = find_latest_scores()
        if p and p != last_seen:
            log(f"[OK] 发现分数表：{p}")
            return p
        time.sleep(poll)
    return None

# ========= 主流程 =========
def main():
    log("=== 一键启动流程开始 ===")
    if not check_env():
        log("[FATAL] 自检失败，请先修复")
        return

    # 1) 快照
    make_snapshot()

    # 2) 启动采集 (异步)
    run_async("采集", [
        sys.executable, "-u", "tools/rt_updater_with_banner.py",
        "--db", str(DB_MAIN),
        "--backfill-days", str(BACKFILL_DAYS),
        "--max-workers", str(MAX_WORKERS),
        "--interval", str(RT_INTERVAL)
    ], LOG_DIR/"collector_live.log")

    # 3) 启动回测 (异步，基于快照)
    run_async("回测", [
        sys.executable, "-u", "backtest/backtest_pro.py",
        "--db", str(DB_SNAP),
        "--days", str(BTEST_DAYS),
        "--topk", str(BTEST_TOPK),
        "--outdir", "results"
    ], LOG_DIR/"backtest_live.log")

    # 4) 等待回测结果 → 选币 + 导出最优参数
    scores = wait_for_scores()
    if not scores:
        log("[WARN] 在等待窗口内未发现分数表。将跳过本轮的“选币/导出参数”，可稍后手动执行两条命令：")
        log("       ① python -u tools\\qs2_pick_live_syms.py --top 20 --out deploy\\qs2_live_symbols.txt")
        log("       ② python -u tools\\emit_best_params.py --out deploy\\live_best_params.json")
    else:
        run_block("选币", [
            sys.executable, "-u", "tools/qs2_pick_live_syms.py",
            "--scores", str(scores),
            "--top", str(LIVE_TOP),
            "--out", "deploy/qs2_live_symbols.txt"
        ])
        if (PROJ/"tools/emit_best_params.py").exists():
            run_block("导出最优参数", [
                sys.executable, "-u", "tools/emit_best_params.py",
                "--scores", str(scores),
                "--out", "deploy/live_best_params.json"
            ])
        else:
            log("[WARN] 缺少 tools/emit_best_params.py，暂跳过参数导出（仅影响实盘参数自动接管）")

    # 5) 启动实盘 (异步)
    run_async("实盘", [
        sys.executable, "-u", "live_trading/execution_engine_binance_ws.py",
        "--db", str(DB_MAIN),
        "--mode", MODE,
        "--best-params", "deploy/live_best_params.json",
        "--ui-rows", "30"
    ], LOG_DIR/"live_engine.log")

    log("=== 全部流程已拉起，请在 logs/ 下查看详细日志 ===")

if __name__=="__main__":
    main()
