# -*- coding: utf-8 -*-
"""
机构级·S级冒烟测试（实时进度）
目标：BTCUSDT · 30m · 近30天
链路：快照(在线备份) → 回测(实时打印进度) → 抽取最佳参数 → 写入deploy → 启动实盘(paper) → 可选到时自动收尾

特性：
- 回测阶段逐行透传 stdout/stderr（真正“实时打印进度”），同时tee到日志文件
- 失败点有清晰报错与下一步排查建议
- 所有关键产物：logs/smoke_S_*.log、results/a6_strategy_scores*.csv、deploy/{qs2_live_symbols.txt,live_best_params.json}
- 可配置：仅BTC/30m/30d；支持自动在 N 分钟后停止实盘进程

适配你的工程路径：
  PROJ = D:\quant_system_pro (3)\quant_system_pro
  DB_MAIN = D:\quant_system_v2\data\market_data.db
"""

import os, sys, time, csv, json, sqlite3, subprocess, signal
from pathlib import Path
from datetime import datetime

# ===== 固定路径 & 参数 =====
PROJ    = Path(r"D:\quant_system_pro (3)\quant_system_pro")
DB_MAIN = Path(r"D:\quant_system_v2\data\market_data.db")
DB_SNAP = Path(r"D:\quant_system_v2\data\market_data_snapshot.db")

SYMBOL = "BTCUSDT"
TF     = "30m"
BT_DAYS = 30
TOPK    = 8

# 实盘自动收尾（分钟）。0 = 不自动收尾
AUTO_STOP_LIVE_MIN = 30

LOG_DIR = PROJ / "logs"
RES_DIR = PROJ / "results"
DEPLOY  = PROJ / "deploy"
LOG_DIR.mkdir(exist_ok=True); RES_DIR.mkdir(exist_ok=True); DEPLOY.mkdir(exist_ok=True)

LOG_MAIN  = LOG_DIR / f"smoke_S_main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_BT    = LOG_DIR / "smoke_S_backtest.log"
LOG_LIVE  = LOG_DIR / "smoke_S_live.log"
LOG_COL   = LOG_DIR / "smoke_S_collector.log"

PY = sys.executable  # 当前python
A6 = "[A6-S]"

def now(): return datetime.now().strftime("%H:%M:%S")

def out(msg, also_main=True):
    line = f"{now()} | {msg}"
    print(line, flush=True)
    if also_main:
        LOG_MAIN.write_text((LOG_MAIN.read_text(encoding="utf-8") if LOG_MAIN.exists() else "") + line + "\n", encoding="utf-8")

def tee_stream(cmd, cwd, logfile: Path, title: str) -> int:
    """实时逐行打印子进程输出，并tee到日志。返回rc"""
    out(f"{A6} [RUN] {title}: {' '.join(cmd)}")
    logfile.parent.mkdir(parents=True, exist_ok=True)
    with subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, text=True
    ) as p, logfile.open("w", encoding="utf-8") as f:
        for line in p.stdout:
            # 逐行透传回测进度
            print(line.rstrip())
            f.write(line)
        rc = p.wait()
    if rc == 0:
        out(f"{A6} [OK ] {title} 完成")
    else:
        out(f"{A6} [ERR] {title} 失败 rc={rc} （详见 {logfile}）")
    return rc

def run_async(cmd, cwd, logfile: Path, title: str):
    out(f"{A6} [START] {title}: {' '.join(cmd)}")
    with logfile.open("w", encoding="utf-8") as f:
        subprocess.Popen(cmd, cwd=cwd, stdout=f, stderr=subprocess.STDOUT)

def make_snapshot_sqlite(src: Path, dst: Path):
    try:
        uri = f"file:{src.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30) as con_src:
            try: con_src.execute("PRAGMA wal_checkpoint(FULL);")
            except Exception: pass
            with sqlite3.connect(str(dst), timeout=60) as con_dst:
                con_src.backup(con_dst)
        out(f"{A6} [OK ] 快照已生成：{dst}")
        return True
    except Exception as e:
        out(f"{A6} [WARN] 快照失败（在线备份）：{e}")
        return False

def latest_scores():
    cands = sorted(RES_DIR.glob("a6_strategy_scores*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def pick_best(scores_csv: Path, symbol: str, tf: str):
    best_row, best_score = None, -1e18
    with scores_csv.open("r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            sym = r.get("symbol") or r.get("Symbol") or ""
            tfx = (r.get("timeframe") or r.get("tf") or "").lower()
            if sym != symbol or tfx != tf.lower(): continue
            score = r.get("score") or r.get("Score") or r.get("metric_score") or "0"
            try: sc = float(str(score).replace("%",""))
            except: sc = 0.0
            if sc > best_score:
                best_score, best_row = sc, r
    if not best_row: return None
    strat = best_row.get("strategy") or best_row.get("Strategy") or ""
    ptxt  = (best_row.get("params") or best_row.get("Params") or "{}").strip()
    try: params = json.loads(ptxt) if ptxt.startswith("{") else {}
    except: params = {}
    return {"symbol": symbol, "tf": tf, "strategy": strat, "params": params, "score": best_score}

def ensure_exists():
    for rel in [
        "backtest/backtest_pro.py",
        "live_trading/execution_engine_binance_ws.py",
        "tools/rt_updater_with_banner.py",
    ]:
        if not (PROJ/rel).exists():
            out(f"{A6} [FATAL] 缺文件：{rel}"); sys.exit(2)
    if not DB_MAIN.exists():
        out(f"{A6} [FATAL] 主库不存在：{DB_MAIN}"); sys.exit(2)

def main():
    ensure_exists()

    # 1) 快照
    make_snapshot_sqlite(DB_MAIN, DB_SNAP)

    # 2) 拉起采集（异步，保证最新K线持续写库）
    run_async(
        [PY, "-u", "tools/rt_updater_with_banner.py", "--db", str(DB_MAIN),
         "--backfill-days", "365", "--max-workers", "8", "--interval", "30"],
        PROJ, LOG_COL, "采集"
    )

    # 3) 回测（实时打印进度；仅 BTCUSDT_30m / 30d）
    rc = tee_stream(
        [PY, "-u", "backtest/backtest_pro.py",
         "--db", str(DB_SNAP),
         "--symbols", SYMBOL, "--tfs", TF,
         "--days", str(BT_DAYS), "--topk", str(TOPK),
         "--outdir", "results"],
        PROJ, LOG_BT, "回测（BTCUSDT 30m 30d）"
    )
    if rc != 0:
        out(f"{A6} [HINT] 回测失败排查路径：{LOG_BT}，以及 results/ 下是否有 a6_strategy_scores*.csv")
        sys.exit(2)

    # 4) 读取分数表 → 最优参数 → 写 deploy
    sc = latest_scores()
    if not sc:
        out(f"{A6} [FATAL] 未发现分数表 a6_strategy_scores*.csv，请检查回测输出目录：{RES_DIR}")
        sys.exit(2)

    best = pick_best(sc, SYMBOL, TF)
    if not best:
        out(f"{A6} [FATAL] 分数表中没有 {SYMBOL}_{TF} 记录。请确认回测是否覆盖到该组合。")
        sys.exit(2)

    (DEPLOY / "live_best_params.json").write_text(json.dumps([{
        "symbol": best["symbol"], "tf": best["tf"], "strategy": best["strategy"], "params": best["params"]
    }], ensure_ascii=False, indent=2), encoding="utf-8")
    (DEPLOY / "qs2_live_symbols.txt").write_text(SYMBOL + "\n", encoding="utf-8")

    out(f"{A6} [OK ] 已写最佳参数 → {DEPLOY/'live_best_params.json'}（score={best['score']:.4f}）")
    out(f"{A6} [OK ] 已写选币列表 → {DEPLOY/'qs2_live_symbols.txt'}（{SYMBOL}）")

    # 5) 启动实盘（paper）
    live_proc = subprocess.Popen(
        [PY, "-u", "live_trading/execution_engine_binance_ws.py",
         "--db", str(DB_MAIN), "--mode", "paper",
         "--best-params", "deploy/live_best_params.json", "--ui-rows", "30"],
        cwd=PROJ, stdout=LOG_LIVE.open("w", encoding="utf-8"), stderr=subprocess.STDOUT
    )
    out(f"{A6} [START] 实盘(paper) 已启动，日志：{LOG_LIVE}")

    if AUTO_STOP_LIVE_MIN > 0:
        out(f"{A6} [SAFE] 将在 {AUTO_STOP_LIVE_MIN} 分钟后自动尝试关闭实盘（冒烟保护）。")
        deadline = time.time() + AUTO_STOP_LIVE_MIN * 60
        try:
            while time.time() < deadline:
                time.sleep(5)
                if live_proc.poll() is not None:
                    out(f"{A6} [INFO] 实盘进程已自行退出（rc={live_proc.returncode}）")
                    break
        except KeyboardInterrupt:
            out(f"{A6} [CTRL] 收到中断，准备关闭实盘…")
        finally:
            if live_proc.poll() is None:
                try:
                    if os.name == "nt":
                        live_proc.send_signal(signal.CTRL_BREAK_EVENT)  # 尝试优雅
                    time.sleep(2)
                    live_proc.terminate()
                    time.sleep(2)
                except Exception:
                    pass
                if live_proc.poll() is None:
                    live_proc.kill()
            out(f"{A6} [DONE] 冒烟流程收尾完成。可查看 {LOG_LIVE} 了解实盘细节。")
    else:
        out(f"{A6} [DONE] 冒烟流程已完成，实盘继续运行中（手动结束请结束 python 进程或关闭窗口）。")

if __name__ == "__main__":
    # 让主日志先创建，避免多次打开追加造成编码问题
    LOG_MAIN.touch(exist_ok=True)
    main()
