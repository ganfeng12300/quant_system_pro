# -*- coding: utf-8 -*-
"""
S级·机构级 冒烟测试（彩色 + 实时进度）
目标：BTCUSDT · 30m · 近30天
链路：快照(在线备份) → 采集(异步) → 回测(实时彩色进度) → 抽取最佳参数 → 写入 deploy → 启动实盘(paper)
附加：回测与等待分数表超时保护、旋转指示器、阶段耗时统计、优雅收尾
适配路径：
  PROJ = D:\quant_system_pro (3)\quant_system_pro
  DB   = D:\quant_system_v2\data\market_data.db
"""
import os, sys, time, csv, json, re, sqlite3, subprocess, threading, signal
from pathlib import Path
from datetime import datetime

# ========= 可调配置 =========
PROJ    = Path(r"D:\quant_system_pro (3)\quant_system_pro")
DB_MAIN = Path(r"D:\quant_system_v2\data\market_data.db")
DB_SNAP = Path(r"D:\quant_system_v2\data\market_data_snapshot.db")

SYMBOL   = "BTCUSDT"
TF       = "30m"
BT_DAYS  = 30
BT_TOPK  = 8

# 时限/等待
BACKTEST_TIMEOUT_SEC = 3600        # 回测最长允许时长（秒）
WAIT_SCORES_SEC      = 1800        # 等待分数表最长时长（秒）
POLL_SEC             = 2           # 轮询间隔

# 实盘自动收尾（分钟），0 = 不自动停机
AUTO_STOP_LIVE_MIN   = 30

# ========= 彩色输出（自动 colorama，缺失也可运行） =========
try:
    from colorama import init as _cinit, Fore, Style
    _cinit()
    C_OK, C_ERR, C_WARN, C_INFO, C_DIM = Fore.GREEN, Fore.RED, Fore.YELLOW, Fore.CYAN, Style.DIM
    C_RST = Style.RESET_ALL
except Exception:
    class _Dummy:  # 退化为无色
        def __getattr__(self, k): return ""
    Fore = Style = _Dummy()
    C_OK=C_ERR=C_WARN=C_INFO=C_DIM=C_RST=""

A6 = f"{C_INFO}[A6-S]{C_RST}"
PY = sys.executable

# ========= 日志与目录 =========
LOG_DIR = PROJ / "logs"; RES_DIR = PROJ / "results"; DEPLOY = PROJ / "deploy"
for d in (LOG_DIR, RES_DIR, DEPLOY): d.mkdir(exist_ok=True, parents=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_MAIN = LOG_DIR / f"smoke_S_main_{STAMP}.log"
LOG_BT   = LOG_DIR / "smoke_S_backtest.log"
LOG_COL  = LOG_DIR / "smoke_S_collector.log"
LOG_LIVE = LOG_DIR / "smoke_S_live.log"

def now(): return datetime.now().strftime("%H:%M:%S")

def write_line(fp: Path, s: str):
    with fp.open("a", encoding="utf-8") as f:
        f.write(s + "\n")

def out(msg, color=""):
    line = f"{now()} | {color}{msg}{C_RST}"
    print(line, flush=True)
    write_line(LOG_MAIN, re.sub(r"\x1b\\[[0-9;]*m","",line))  # 写日志时去色（稳妥）

# ========= 工具 =========
def ensure_exists():
    miss = []
    for rel in [
        "backtest/backtest_pro.py",
        "tools/rt_updater_with_banner.py",
        "live_trading/execution_engine_binance_ws.py",
    ]:
        if not (PROJ/rel).exists(): miss.append(rel)
    if miss:
        out(f"{A6} 缺文件：{', '.join(miss)}", C_ERR); sys.exit(2)
    if not DB_MAIN.exists():
        out(f"{A6} 主库不存在：{DB_MAIN}", C_ERR); sys.exit(2)

def make_snapshot_sqlite(src: Path, dst: Path):
    t0=time.time()
    try:
        uri=f"file:{src.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30) as con_src:
            try: con_src.execute("PRAGMA wal_checkpoint(FULL);")
            except Exception: pass
            with sqlite3.connect(str(dst), timeout=60) as con_dst:
                con_src.backup(con_dst)
        out(f"{A6} 快照已生成 → {dst}  ({time.time()-t0:.2f}s)", C_OK)
        return True
    except Exception as e:
        out(f"{A6} 快照失败（在线备份）：{e}", C_WARN); return False

def start_collector():
    cmd=[PY,"-u","tools/rt_updater_with_banner.py",
         "--db",str(DB_MAIN),"--backfill-days","365","--max-workers","8","--interval","30"]
    out(f"{A6} 启动采集: {' '.join(cmd)}", C_INFO)
    with LOG_COL.open("w",encoding="utf-8") as f:
        subprocess.Popen(cmd, cwd=PROJ, stdout=f, stderr=subprocess.STDOUT)

def tee_backtest_with_timeout(cmd, cwd, logfile: Path, timeout_sec: int) -> int:
    """
    实时逐行输出 + 超时保护（到时发送终止信号）
    """
    out(f"{A6} 回测启动（实时进度）: {' '.join(cmd)}", C_INFO)
    logfile.write_text("", encoding="utf-8")
    start=time.time()

    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    last_line_time = time.time()
    spinner = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
    sp_idx = 0

    def monitor():
        nonlocal last_line_time, sp_idx
        with logfile.open("a", encoding="utf-8") as f:
            for line in proc.stdout:
                last_line_time = time.time()
                print(f"{C_DIM}{spinner[sp_idx%len(spinner)]}{C_RST} {line.rstrip()}")
                f.write(line)
                sp_idx += 1

    th = threading.Thread(target=monitor, daemon=True); th.start()

    # 等待 + 超时
    while proc.poll() is None:
        elapsed = time.time() - start
        if timeout_sec and elapsed > timeout_sec:
            out(f"{A6} 回测超时 {timeout_sec}s，尝试终止…", C_WARN)
            try:
                if os.name=="nt":
                    proc.send_signal(signal.CTRL_BREAK_EVENT)  # 温柔
                time.sleep(1)
                proc.terminate()
                time.sleep(1)
            except Exception:
                pass
            try:
                proc.kill()
            except Exception:
                pass
            break
        time.sleep(0.2)

    rc = proc.wait()
    out(f"{A6} 回测结束 rc={rc}，耗时 {time.time()-start:.1f}s  日志→{logfile}", C_OK if rc==0 else C_ERR)
    return rc

def find_latest_scores():
    c = sorted(RES_DIR.glob("a6_strategy_scores*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return c[0] if c else None

def wait_scores(timeout=WAIT_SCORES_SEC):
    out(f"{A6} 等待分数表 a6_strategy_scores*.csv（≤{timeout}s）", C_INFO)
    start=time.time()
    spinner="|/-\\"
    i=0; last_print=0
    while time.time()-start <= timeout:
        p = find_latest_scores()
        if p:
            out(f"{A6} 发现分数表：{p}", C_OK); return p
        # 旋转指示器（每秒刷新）
        if time.time()-last_print>=1:
            print(f"{C_DIM}{spinner[i%4]} 等待中… {int(time.time()-start)}s{C_RST}", end="\r", flush=True)
            i+=1; last_print=time.time()
        time.sleep(POLL_SEC)
    print("")  # 换行
    return None

def pick_best_params(scores_csv: Path, symbol: str, tf: str):
    best, best_score = None, -1e18
    with scores_csv.open("r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            sym = r.get("symbol") or r.get("Symbol") or ""
            tfx = (r.get("timeframe") or r.get("tf") or "").lower()
            if sym != symbol or tfx != tf.lower(): continue
            sc  = r.get("score") or r.get("Score") or r.get("metric_score") or "0"
            try: val = float(str(sc).replace("%",""))
            except: val = 0.0
            if val > best_score:
                best_score, best = val, r
    if not best: return None
    strat = best.get("strategy") or best.get("Strategy") or ""
    ptxt  = (best.get("params") or best.get("Params") or "{}").strip()
    try: params = json.loads(ptxt) if ptxt.startswith("{") else {}
    except: params = {}
    return {"symbol": symbol, "tf": tf, "strategy": strat, "params": params, "score": best_score}

def start_live():
    cmd=[PY,"-u","live_trading/execution_engine_binance_ws.py",
         "--db",str(DB_MAIN),"--mode","paper",
         "--best-params","deploy/live_best_params.json","--ui-rows","30"]
    out(f"{A6} 启动实盘(paper): {' '.join(cmd)}", C_INFO)
    with LOG_LIVE.open("w",encoding="utf-8") as f:
        p = subprocess.Popen(cmd, cwd=PROJ, stdout=f, stderr=subprocess.STDOUT)
    return p

# ========= 主流程 =========
def main():
    out(f"{A6} 启动 S级冒烟 · {SYMBOL} {TF} {BT_DAYS}d", C_INFO)
    ensure_exists()

    t0=time.time()
    make_snapshot_sqlite(DB_MAIN, DB_SNAP)

    start_collector()

    # 回测（实时彩色）
    bt_cmd=[PY,"-u","backtest/backtest_pro.py",
            "--db",str(DB_SNAP),
            "--symbols",SYMBOL,"--tfs",TF,
            "--days",str(BT_DAYS),"--topk",str(BT_TOPK),
            "--outdir","results"]
    rc = tee_backtest_with_timeout(bt_cmd, PROJ, LOG_BT, BACKTEST_TIMEOUT_SEC)
    if rc != 0:
        out(f"{A6} 回测失败。请查看 {LOG_BT} 与 results/ 目录。", C_ERR); sys.exit(2)

    # 分数表
    scores = wait_scores(WAIT_SCORES_SEC)
    if not scores:
        out(f"{A6} 等待分数表超时。请确认 results/ 是否产出 a6_strategy_scores*.csv", C_ERR); sys.exit(2)

    # 抽取最佳参数 → 写入 deploy
    best = pick_best_params(scores, SYMBOL, TF)
    if not best:
        out(f"{A6} 分数表中未找到 {SYMBOL}_{TF} 记录。", C_ERR); sys.exit(2)

    DEPLOY.joinpath("live_best_params.json").write_text(
        json.dumps([{"symbol":best["symbol"],"tf":best["tf"],
                     "strategy":best["strategy"],"params":best["params"]}],
                   ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    DEPLOY.joinpath("qs2_live_symbols.txt").write_text(SYMBOL+"\n", encoding="utf-8")
    out(f"{A6} 最佳参数已写 → {DEPLOY/'live_best_params.json'}（score={best['score']:.4f}）", C_OK)
    out(f"{A6} 选币列表已写 → {DEPLOY/'qs2_live_symbols.txt'}", C_OK)

    # 实盘
    live_proc = start_live()
    out(f"{A6} 全流程耗时 {time.time()-t0:.1f}s  日志：{LOG_MAIN}", C_INFO)

    # 自动收尾
    if AUTO_STOP_LIVE_MIN > 0:
        out(f"{A6} 冒烟保护：{AUTO_STOP_LIVE_MIN} 分钟后自动尝试关闭实盘。", C_WARN)
        deadline = time.time() + AUTO_STOP_LIVE_MIN*60
        try:
            while time.time() < deadline:
                if live_proc.poll() is not None:
                    out(f"{A6} 实盘进程已退出 rc={live_proc.returncode}", C_INFO); break
                time.sleep(2)
        except KeyboardInterrupt:
            out(f"{A6} 收到中断，准备收尾…", C_WARN)
        # 收尾
        if live_proc.poll() is None:
            try:
                if os.name=="nt": live_proc.send_signal(signal.CTRL_BREAK_EVENT)
                time.sleep(1); live_proc.terminate(); time.sleep(1)
            except Exception: pass
            if live_proc.poll() is None:
                live_proc.kill()
        out(f"{A6} 冒烟流程收尾完成。实盘日志 → {LOG_LIVE}", C_OK)
    else:
        out(f"{A6} 冒烟结束，实盘保持运行（手动结束其 python 进程即可）。", C_OK)

if __name__ == "__main__":
    LOG_MAIN.touch(exist_ok=True)
    main()
