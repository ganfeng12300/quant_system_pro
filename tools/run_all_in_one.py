# -*- coding: utf-8 -*-
"""
一窗到底 · 机构级并行总控
- 指标类 A1–A4 并行（同一窗口内）
- 模型类 A5–A8 串行（避免 GPU 抢占）
- 彩色状态行 + 关键输出回显
- 结束大字 + 策略汇总表 + best_combo 预览

依赖：
    pip install colorama pyfiglet
可选（仅用于 best_combo 预览，不装也不影响运行）：
    pip install pandas
"""

import os, sys, time, subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)

# ==== 通用配置 ====
ROOT        = Path(__file__).resolve().parents[1]
BACKTEST_PY = ROOT / "backtest" / "backtest_pro.py"
PYEXE       = sys.executable
RESULTS_DIR = ROOT / "results" / "parallel_run"

# A1-A8 分组（按你当前映射：A1→MA, A2→BOLL, A3→ATR, A4→REVERSAL, A5→LGBM, A6→XGB, A7→LSTM, A8→ENSEMBLE）
INDICATOR = ["A1", "A2", "A3", "A4"]   # 指标类并行
MODEL     = ["A5", "A6", "A7", "A8"]   # 模型类串行（防 GPU 抢占）

SUMMARY = []  # 收集每个策略的执行摘要

def set_threads_env(num=16):
    os.environ.setdefault("OMP_NUM_THREADS", str(num))
    os.environ.setdefault("MKL_NUM_THREADS", str(num))
    os.environ.setdefault("NUMEXPR_MAX_THREADS", str(num))
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "0")
    os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "max_split_size_mb:64")

def ts(): return datetime.now().strftime("%H:%M:%S")

def banner_big(msg, color=Fore.GREEN):
    try:
        import pyfiglet
        print(color + Style.BRIGHT + pyfiglet.figlet_format(msg, font="slant"))
    except Exception:
        print(color + Style.BRIGHT + f"\n==== {msg} ====\n")

def print_hdr(db, symbol, days, topk, outdir, workers):
    box = 96
    print(f"""
┌{'─'*(box-2)}┐
│  {Style.BRIGHT}机构级一窗并行总控{Style.RESET_ALL}  |  指标: 并行 {workers}  |  模型: 串行
│  Symbol: {Fore.CYAN}{symbol}{Style.RESET_ALL}   Days: {days}   TopK: {topk}
│  DB    : {db}
│  OutDir: {outdir}
└{'─'*(box-2)}┘
""".rstrip("\n"))

def row(status, strat, extra=""):
    col = {"RUN":Fore.CYAN, "OK":Fore.GREEN, "ERR":Fore.RED, "SKIP":Fore.YELLOW}.get(status, Fore.WHITE)
    print(f"[{ts()}] {col}{status:<4}{Style.RESET_ALL} | 策略 {Fore.MAGENTA}{strat:<8}{Style.RESET_ALL} {extra}")

def stream_proc(cmd, log_file):
    # 同一窗口内读取子进程输出；不再新开窗口
    lf = open(log_file, "w", encoding="utf-8", errors="ignore")
    p  = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    last_line = None
    for line in iter(p.stdout.readline, ""):
        lf.write(line)
        low = line.lower()
        # 轻量回显关键行（提神但不刷屏）
        if ("best loss" in low) or ("best score" in low) or ("trial/s" in low) or (line.strip().endswith("%")):
            last_line = line.strip()
            print(Fore.WHITE + "  ↳ " + last_line[:110] + Style.RESET_ALL)
    p.wait()
    lf.close()
    return p.returncode, last_line

def run_one(db, days, symbol, topk, outdir, strat):
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    logf = outdir / f"run_{symbol}_{strat}_{int(time.time())}.log"
    cmd = [
        PYEXE, "-u", str(BACKTEST_PY),
        "--db", str(db), "--days", str(days), "--topk", str(topk),
        "--outdir", str(outdir), "--symbols", symbol,
        "--only-strategy", strat
    ]
    row("RUN", strat)
    rc, last = stream_proc(cmd, str(logf))
    status = "OK" if rc == 0 else "ERR"
    extra  = (last or "")
    if rc == 0: row("OK", strat, extra=extra)
    else:       row("ERR", strat, extra=f"rc={rc}  log={logf}")

    # —— 收集摘要（尽量从 last 中提取 best loss 浮点数）——
    best_loss = None
    if last:
        import re
        m = re.search(r"best\s+loss[:=]\s*([-+]?\d+(\.\d+)?)", last.lower())
        if m:
            try: best_loss = float(m.group(1))
            except: pass
    SUMMARY.append({
        "strat": strat, "status": status, "rc": rc,
        "best_loss": best_loss, "log": str(logf)
    })
    return strat, rc, str(logf), last

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",   default=r"D:\quant_system_v2\data\market_data.db")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--topk", type=int, default=40)
    ap.add_argument("--outdir", default=str(RESULTS_DIR))
    ap.add_argument("--workers", type=int, default=3, help="指标类并行进程数")
    ap.add_argument("--threads", type=int, default=16, help="数值库线程数")
    args = ap.parse_args()

    set_threads_env(args.threads)
    print_hdr(args.db, args.symbol, args.days, args.topk, args.outdir, args.workers)

    errors = []

    # === 1) 指标类并行（同一窗口，不新开窗）
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = { ex.submit(run_one, args.db, args.days, args.symbol, args.topk, args.outdir, s): s for s in INDICATOR }
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                strat, rc, logf, last = fut.result()
                if rc != 0: errors.append((strat, rc, logf))
            except Exception as e:
                row("ERR", s, extra=str(e))
                errors.append((s, -1, f"exception: {e}"))

    # === 2) 模型类串行（防止 GPU 抢占）
    for s in MODEL:
        strat, rc, logf, last = run_one(args.db, args.days, args.symbol, args.topk, args.outdir, s)
        if rc != 0: errors.append((strat, rc, logf))

    # === 3) 结果汇总表 ===
    def fmt_loss(x):  return ("{:.6f}".format(x)) if isinstance(x, (int,float)) else "-"
    def color_status(s): return (Fore.GREEN + s + Style.RESET_ALL) if s=="OK" else (Fore.RED + s + Style.RESET_ALL)

    order = INDICATOR + MODEL
    by_tag = {r["strat"]: r for r in SUMMARY}
    print("\n" + Fore.CYAN + "策略执行汇总：" + Style.RESET_ALL)
    print("┌────────┬────────┬───────────────┬──────────────────────────────────────────────┐")
    print("│ 策略    │ 状态   │ best loss     │ 日志                                         │")
    print("├────────┼────────┼───────────────┼──────────────────────────────────────────────┤")
    for tag in order:
        r = by_tag.get(tag, {"status":"-", "best_loss":None, "log":"-"})
        print("│ {:<6} │ {:<6} │ {:>13} │ {:<44} │".format(
            tag, color_status(r["status"]), fmt_loss(r.get("best_loss")), Path(r.get("log","-")).name[:44]
        ))
    print("└────────┴────────┴───────────────┴──────────────────────────────────────────────┘")

    # === 4) best_combo 预览（可选） ===
    try:
        import pandas as pd
        combo = Path("data") / "best_combo.csv"
        if combo.exists():
            df = pd.read_csv(combo, nrows=8)
            print("\n" + Fore.CYAN + "best_combo.csv 预览（前 8 行）：" + Style.RESET_ALL)
            cols = [c for c in df.columns if c not in ("参数JSON",)]
            print(df[cols].to_string(index=False))
    except Exception:
        pass

    # === 5) 完成大字与错误回顾 ===
    print("\n" + Fore.CYAN + "═"*78 + Style.RESET_ALL)
    if errors:
        print(Fore.YELLOW + "完成（含报错策略已记录）：" + Style.RESET_ALL)
        for s, rc, lf in errors:
            print(f"  {Fore.RED}{s}{Style.RESET_ALL} rc={rc}  日志：{lf}")
    else:
        print(Fore.GREEN + "全部策略完成，无错误。" + Style.RESET_ALL)

    banner_big("回測完成!", color=Fore.GREEN)

if __name__ == "__main__":
    main()
