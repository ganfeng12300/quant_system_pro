# -*- coding: utf-8 -*-
"""
满血机构版 · 一窗到底并行总控（单窗口）
- 任务调度：总并发 W、模型并发 M（默认 M=1 防显存抢占）
- 指标 A1~A4 / 模型 A5~A8 混合流水，自动保持“槽位满载”
- 同一窗口彩色输出（带策略前缀），实时捕捉 best loss / trial 速率 / ETA
- 结束：彩色汇总表 + best_combo 预览 + 大字 banner

依赖：
    pip install colorama pyfiglet
（best_combo 预览可选 pandas：pip install pandas）
"""

import os, sys, time, subprocess, threading, queue, math, re
from pathlib import Path
from datetime import datetime
import argparse
from collections import deque
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)
PRINT_LOCK = threading.Lock()

ROOT        = Path(__file__).resolve().parents[1]
BACKTEST_PY = ROOT / "backtest" / "backtest_pro.py"
PYEXE       = sys.executable
RESULTS_DIR = ROOT / "results" / "full_throttle"

INDICATOR = ["A1","A2","A3","A4"]
MODEL     = ["A5","A6","A7","A8"]

SUMMARY = []  # {strat,status,rc,best_loss,log,dur_sec}
RUNSTAT = {}  # tag -> {"done":int, "total":int, "rate":float, "eta_sec":float, "start":float}
TOTAL_TRIALS_DEFAULT = 25  # 若无法从输出解析总试验数，按此估算

def set_threads_env(total_hw_threads:int, total_workers:int, manual_omp:int=None):
    """
    合理分配数值库线程，避免过度超订阅。
    - 若用户指定 --omp-threads 则使用之；
    - 否则按 total_threads / total_workers 估算单进程线程数（取区间 [2,16]）。
    """
    if manual_omp is not None and manual_omp > 0:
        per = manual_omp
    else:
        per = max(2, min(16, (total_hw_threads or 8) // max(1,total_workers)))
    os.environ.setdefault("OMP_NUM_THREADS", str(per))
    os.environ.setdefault("MKL_NUM_THREADS", str(per))
    os.environ.setdefault("NUMEXPR_MAX_THREADS", str(per))
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "0")
    os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "max_split_size_mb:64")
    with PRINT_LOCK:
        print(Fore.YELLOW + f"[ENV] OMP/MKL/NUMEXPR threads per-proc = {per}  (HW={total_hw_threads}, workers={total_workers})" + Style.RESET_ALL)

def ts(): return datetime.now().strftime("%H:%M:%S")

def banner_big(msg, color=Fore.GREEN):
    try:
        import pyfiglet
        print(color + Style.BRIGHT + pyfiglet.figlet_format(msg, font="slant"))
    except Exception:
        print(color + Style.BRIGHT + f"\n==== {msg} ====\n")

def fmt_eta(sec: float) -> str:
    try:
        sec = max(0, float(sec))
        h = int(sec // 3600)
        m = int((sec % 3600) // 60)
        s = int(sec % 60)
        return f"{h:d}:{m:02d}:{s:02d}"
    except Exception:
        return "--:--:--"

def hdr(db, symbol, days, topk, outdir, workers, model_workers):
    box = 104
    print(f"""
┌{'─'*(box-2)}┐
│  {Style.BRIGHT}满血机构版 · 一窗并行总控{Style.RESET_ALL}  |  总并发: {workers}  |  模型并发上限: {model_workers}
│  Symbol: {Fore.CYAN}{symbol}{Style.RESET_ALL}   Days: {days}   TopK: {topk}
│  DB    : {db}
│  OutDir: {outdir}
└{'─'*(box-2)}┘
""".rstrip("\n"))

# 彩色前缀
COLORS = {
    "A1":Fore.CYAN, "A2":Fore.CYAN, "A3":Fore.CYAN, "A4":Fore.CYAN,
    "A5":Fore.MAGENTA, "A6":Fore.MAGENTA, "A7":Fore.MAGENTA, "A8":Fore.MAGENTA
}

def log_line(tag, msg):
    with PRINT_LOCK:
        col = COLORS.get(tag, Fore.WHITE)
        print(col + f"[{ts()}][{tag}] " + Style.RESET_ALL + msg)

def stream_proc(p:subprocess.Popen, logf:Path, tag:str):
    """
    持续读取子进程输出，写日志并在主窗打印关键行，返回最后一条关键行文本。
    解析形如： 19/25 [00:09<00:03, 1.92trial/s, best loss: 0.28]
    更新 RUNSTAT[tag] 的 done/total/rate/eta。
    """
    last_key = None
    RUNSTAT[tag] = RUNSTAT.get(tag, {"done":0,"total":TOTAL_TRIALS_DEFAULT,"rate":0.0,"eta_sec":0.0,"start":time.time()})
    with open(logf, "w", encoding="utf-8", errors="ignore") as lf:
        for line in iter(p.stdout.readline, ""):
            lf.write(line)
            low = line.lower().strip()

            # 捕捉典型关键行： 19/25 [..., 1.92trial/s, best loss: ...]
            if ("/" in low) and ("trial/s" in low):
                try:
                    # 提取 done/total
                    m1 = re.search(r"(\d+)\s*/\s*(\d+)", low)
                    if m1:
                        done, total = int(m1.group(1)), int(m1.group(2))
                    else:
                        done, total = RUNSTAT[tag]["done"], RUNSTAT[tag]["total"]
                    # 提取 rate
                    m2 = re.search(r"([0-9.]+)\s*trial/s", low)
                    rate = float(m2.group(1)) if m2 else RUNSTAT[tag]["rate"] or 0.0

                    # 估算 ETA（按剩余 trial / 当前速率）
                    rem = max(0, (total - done))
                    eta_sec = rem / max(1e-6, rate)

                    RUNSTAT[tag].update({"done":done, "total":total, "rate":rate, "eta_sec":eta_sec})
                    last_key = line.strip()

                    # 回显并附带 ETA
                    log_line(tag, (last_key[:110] + f"  | ETA {fmt_eta(eta_sec)}"))
                    continue
                except Exception:
                    pass

            # 其它信息：best loss / 百分比 / 速率等，轻量回显
            if ("best loss" in low) or ("best score" in low) or ("trial/s" in low) or low.endswith("%"):
                last_key = line.strip()
                log_line(tag, last_key[:110])
    p.wait()
    return last_key

def launch_one(db, days, symbol, topk, outdir, tag):
    """启动一个策略子进程并阻塞等待结束；返回 (rc,last_key,log_path,dur_sec)"""
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    logf = outdir / f"run_{symbol}_{tag}_{int(time.time())}.log"
    cmd = [
        PYEXE, "-u", str(BACKTEST_PY),
        "--db", str(db), "--days", str(days), "--topk", str(topk),
        "--outdir", str(outdir), "--symbols", symbol,
        "--only-strategy", tag
    ]
    t0 = time.time()
    log_line(tag, f"RUN  → {cmd!r}")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    last = stream_proc(p, logf, tag)
    rc = p.returncode
    dur = time.time() - t0
    status = "OK" if rc == 0 else "ERR"
    if rc == 0:
        log_line(tag, f"{Fore.GREEN}OK{Style.RESET_ALL}  ({dur:.1f}s) {last or ''}")
    else:
        log_line(tag, f"{Fore.RED}ERR rc={rc}{Style.RESET_ALL}  ({dur:.1f}s)  log={logf}")
    # 解析 best loss（若有）
    best_loss = None
    if last:
        m = re.search(r"best\s+loss[:=]\s*([-+]?\d+(\.\d+)?)", last.lower())
        if m:
            try: best_loss = float(m.group(1))
            except: pass
    SUMMARY.append({"strat":tag, "status":status, "rc":rc, "best_loss":best_loss, "log":str(logf), "dur_sec":dur})
    return rc, last, str(logf), dur

class Task:
    __slots__=("tag","kind")
    def __init__(self, tag, kind): self.tag, self.kind = tag, kind  # kind="ind" | "mdl"

def schedule(db, days, symbol, topk, outdir, workers, model_workers):
    """
    资源约束调度器：
      - 任意时刻最多 workers 个任务；
      - 模型类同时不超过 model_workers 个；
      - 任务就绪即发，尽量保持满槽。
    """
    q = queue.Queue()
    for t in INDICATOR: q.put(Task(t,"ind"))
    for t in MODEL:     q.put(Task(t,"mdl"))

    running = {}  # tag -> (thread)
    counters = {"mdl":0}
    stop = False

    def worker(task:Task):
        try:
            launch_one(db, days, symbol, topk, outdir, task.tag)
        finally:
            running.pop(task.tag, None)
            if task.kind == "mdl":
                counters["mdl"] -= 1

    last_eta_print = 0.0
    while not stop:
        # 启动尽可能多的任务
        while (len(running) < workers) and (not q.empty()):
            # 检查模型并发限制
            nxt: Task = q.queue[0]  # 先偷看队首
            if nxt.kind == "mdl" and counters["mdl"] >= model_workers:
                # 不能上模型；尝试在队列中找到指标任务先上
                moved = False
                for i in range(q.qsize()):
                    t: Task = q.get()
                    if t.kind == "ind":  # 找到指标任务，先上它
                        th = threading.Thread(target=worker, args=(t,), daemon=True)
                        running[t.tag] = th
                        th.start()
                        moved = True
                        break
                    else:
                        q.put(t)  # 模型任务暂且回队尾
                if not moved:
                    break  # 没有可上的指标任务，只能等
            else:
                # 可上（指标；或模型且未超限）
                t: Task = q.get()
                if t.kind == "mdl": counters["mdl"] += 1
                th = threading.Thread(target=worker, args=(t,), daemon=True)
                running[t.tag] = th
                th.start()

        # 终止判断
        if not running and q.empty():
            stop = True
            continue

        # === 每 ~10s 打印一次 Overall ETA ===
        now = time.time()
        if now - last_eta_print >= 10.0:
            try:
                # 当前总速率（所有在跑策略的 trial/s 之和）
                sum_rate = 0.0
                rem_trials_running = 0
                for tag, st in list(RUNSTAT.items()):
                    sum_rate += float(st.get("rate", 0.0) or 0.0)
                    total = int(st.get("total", TOTAL_TRIALS_DEFAULT) or TOTAL_TRIALS_DEFAULT)
                    done  = int(st.get("done", 0) or 0)
                    rem_trials_running += max(0, total - done)

                # 队列中尚未开跑的任务：用默认 trial 数估计
                rem_trials_queued = 0
                try:
                    rem_trials_queued = q.qsize() * TOTAL_TRIALS_DEFAULT
                except Exception:
                    pass

                rem_total = rem_trials_running + rem_trials_queued

                # 如果当前 sum_rate 过小，用 workers 的近似兜底（每个 1.0 trial/s 的保守估计）
                denom = sum_rate if sum_rate > 1e-3 else max(1.0, float(workers))
                eta_all = rem_total / denom
                with PRINT_LOCK:
                    print(Fore.CYAN + f"[ETA] Overall remaining ≈ {fmt_eta(eta_all)}   "
                                      f"(running rate={sum_rate:.2f} trial/s, rem trials={rem_total})"
                          + Style.RESET_ALL)
            except Exception:
                pass
            last_eta_print = now

        time.sleep(0.2)  # 降低轮询开销

def print_summary():
    def fmt_loss(x):  return ("{:.6f}".format(x)) if isinstance(x,(int,float)) else "-"
    def color_status(s): return (Fore.GREEN+s+Style.RESET_ALL) if s=="OK" else (Fore.RED+s+Style.RESET_ALL)
    order = INDICATOR + MODEL
    by = {r["strat"]: r for r in SUMMARY}
    print("\n" + Fore.CYAN + "策略执行汇总：" + Style.RESET_ALL)
    print("┌────────┬────────┬───────────────┬──────────┬──────────────────────────────────────────────┐")
    print("│ 策略    │ 状态   │ best loss     │ 用时(s)  │ 日志                                         │")
    print("├────────┼────────┼───────────────┼──────────┼──────────────────────────────────────────────┤")
    for tag in order:
        r = by.get(tag, {"status":"-","best_loss":None,"dur_sec":0,"log":"-"})
        print("│ {:<6} │ {:<6} │ {:>13} │ {:>8.1f} │ {:<44} │".format(
            tag, color_status(r["status"]), fmt_loss(r["best_loss"]), float(r.get("dur_sec",0)), Path(r["log"]).name[:44]
        ))
    print("└────────┴────────┴───────────────┴──────────┴──────────────────────────────────────────────┘")

    # best_combo 预览
    try:
        import pandas as pd
        combo = ROOT / "data" / "best_combo.csv"
        if combo.exists():
            df = pd.read_csv(combo, nrows=10)
            cols = [c for c in df.columns if c not in ("参数JSON",)]
            print("\n" + Fore.CYAN + "best_combo.csv 预览（前 10 行）：" + Style.RESET_ALL)
            print(df[cols].to_string(index=False))
    except Exception:
        pass

def main():
    cpu_threads = os.cpu_count() or 8
    rec_workers = max(4, min(12, math.ceil(cpu_threads * 0.6)))  # 自适应建议：60% 线程数，封顶 12
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",   default=r"D:\quant_system_v2\data\market_data.db")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--topk", type=int, default=40)
    ap.add_argument("--outdir", default=str(RESULTS_DIR))
    ap.add_argument("--workers", type=int, default=rec_workers, help="总并发（默认≈CPU*0.6，封顶12）")
    ap.add_argument("--model-workers", type=int, default=1, help="模型策略并发上限（建议 1；显存足可试 2）")
    ap.add_argument("--omp-threads", type=int, default=0, help="每个子进程 OMP/MKL 线程数（0=自动）")
    args = ap.parse_args()

    set_threads_env(cpu_threads, args.workers, manual_omp=args.omp_threads)
    hdr(args.db, args.symbol, args.days, args.topk, args.outdir, args.workers, args.model_workers)

    t0 = time.time()
    schedule(args.db, args.days, args.symbol, args.topk, args.outdir, workers=args.workers, model_workers=args.model_workers)
    t1 = time.time()

    print_summary()
    print("\n" + Fore.CYAN + "═"*90 + Style.RESET_ALL)
    print(Fore.GREEN + f"总用时：{t1 - t0:.1f}s" + Style.RESET_ALL)
    banner_big("回測完成!", color=Fore.GREEN)

if __name__ == "__main__":
    main()
