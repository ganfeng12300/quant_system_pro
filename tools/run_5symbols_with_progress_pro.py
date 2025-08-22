# -*- coding: utf-8 -*-
"""
机构级 · 彩色总进度+ETA 外层控制器（终极修正版）
- 汇总 5 个币 × 多策略 × Trial(25) 的全局进度
- 平滑 ETA（EMA），高亮关键里程碑（A5/A6/A7/export）
- 回测结束后自动打开纸面实盘战情面板（独立窗口，稳健处理含空格/括号路径）
- 仅依赖 colorama（若缺失自动降级到无色）
"""
import argparse, os, sys, re, time, subprocess
from pathlib import Path

# ────────────────────────── 颜色层 ──────────────────────────
try:
    from colorama import init as cinit, Fore, Back, Style
    cinit(autoreset=True)
    C = dict(
        ok=Fore.GREEN+Style.BRIGHT,
        warn=Fore.YELLOW+Style.BRIGHT,
        err=Fore.RED+Style.BRIGHT,
        info=Fore.CYAN+Style.BRIGHT,
        emph=Fore.MAGENTA+Style.BRIGHT,
        bar=Fore.GREEN+Style.BRIGHT,
        dim=Style.DIM,
        rst=Style.RESET_ALL
    )
except Exception:
    class _Dummy:
        def __getattr__(self, k): return ""
    Fore=Back=Style=_Dummy()
    C = dict(ok="", warn="", err="", info="", emph="", bar="", dim="", rst="")

# ────────────────────────── 常量与正则 ──────────────────────────
TRIALS_PER_STRAT = 25
# 支持无百分比，仅出现 “N/25” 的日志行
RE_TRIAL = re.compile(r"(?P<n>\d{1,2})/25\b")
RE_DONE  = re.compile(r"\b25/25\b")  # 单策略unit完成的最低判断
# run_id 捕捉（若内层打印了 results\yyyyMMdd-...）
RE_RUNID = re.compile(r"results[\\/](\d{8}-\d{6}-[0-9a-f]{8})")

MILESTONES = (
    "a5_optimized_params.csv",
    "a6_strategy_scores", "a7_blended_portfolio.csv",
    "final_portfolio.json", "live_best_params.json"
)

# ────────────────────────── 工具函数 ──────────────────────────
def draw_bar(pct: float, width=42, ch_full="█", ch_empty="░", color=C["bar"]):
    pct = max(0.0, min(1.0, pct))
    n = int(round(pct*width))
    return f"{color}[{'':<{width}}]{C['rst']}".replace(' ' * width, ch_full*n + ch_empty*(width-n)) + f" {pct*100:5.1f}%"

def fmt_eta(sec: float):
    if not (sec and sec > 0 and sec < 10*24*3600):
        return f"{C['dim']}ETA --:--{C['rst']}"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"ETA {h:02d}:{m:02d}:{s:02d}" if h else f"ETA {m:02d}:{s:02d}"

def ema(prev, val, alpha=0.18):
    return val if prev is None else prev*(1-alpha) + val*alpha

def _latest_run_id(outdir: Path):
    """兜底：从结果目录找最新子目录名当 run_id。"""
    try:
        subs = [p for p in outdir.iterdir() if p.is_dir()]
        subs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return subs[0].name if subs else None
    except Exception:
        return None

def start_paper_console(project_root: Path, db_path: str):
    """打开纸面实盘战情面板（新窗口，稳健处理含空格/括号路径）"""
    engine = project_root / "live_trading" / "execution_engine_binance_ws.py"
    if not engine.exists():
        print(f"{C['warn']}[WARN]{C['rst']} 未找到 {engine}，请改成你的纸面执行器路径。")
        return
    # 用 cmd 的 start 打开独立窗口；PowerShell 中使用 -NoExit + -Command 执行复合命令
    subprocess.call([
        "cmd", "/c", "start", "", "powershell", "-NoExit", "-Command",
        f"& {{ Set-Location -LiteralPath '{project_root}'; "
        f"$env:PYTHONPATH='{project_root}'; "
        f"python '{engine}' --db '{db_path}' --mode paper --ui-rows 30 }}"
    ])

# ────────────────────────── 主流程 ──────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--topk", type=int, default=40)
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--strategies", type=int, default=8,
                    help="每个币策略数量，用于全局进度估算（默认8）")
    # 稳健层外挂参数（透传给 backtest_pro.py）
    ap.add_argument("--spa", choices=["on","off"], default="on")
    ap.add_argument("--spa-alpha", dest="spa_alpha", type=float, default=0.05)
    ap.add_argument("--pbo", choices=["on","off"], default="on")
    ap.add_argument("--pbo-bins", dest="pbo_bins", type=int, default=10)
    ap.add_argument("--impact-recheck", dest="impact_recheck", choices=["on","off"], default="on")
    ap.add_argument("--wfo", choices=["on","off"], default="off")
    ap.add_argument("--wfo-train", dest="wfo_train", type=int, default=180)
    ap.add_argument("--wfo-test",  dest="wfo_test",  type=int, default=30)
    ap.add_argument("--wfo-step",  dest="wfo_step",  type=int, default=30)
    ap.add_argument("--tf-consistency", dest="tf_consistency", choices=["on","off"], default="on")
    ap.add_argument("--tf-consistency-w", dest="tf_consistency_w", type=float, default=0.2)
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    outdir = (project_root / args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    total_symbols = len(args.symbols)
    strategies = max(1, args.strategies)
    total_units = total_symbols * strategies  # unit=某币某策略的25个trial
    done_units = 0
    cur_trial = 0
    start_ts = time.time()
    eta_ema = None
    seen_run_id = None

    # ── Header ──
    print()
    print(f"{C['emph']}┌────────────────────────  S-A档 回测总控  ────────────────────────┐{C['rst']}")
    print(f"{C['emph']}│{C['rst']}  {C['info']}Symbols{C['rst']}: {', '.join(args.symbols)}")
    print(f"{C['emph']}│{C['rst']}  {C['info']}Trials/Strategy{C['rst']}: {TRIALS_PER_STRAT}   "
          f"{C['info']}Strategies/Symbol{C['rst']}: {strategies}   {C['info']}TopK{C['rst']}: {args.topk}")
    print(f"{C['emph']}│{C['rst']}  {C['info']}DB{C['rst']}: {args.db}")
    print(f"{C['emph']}└──────────────────────────────────────────────────────────────────┘{C['rst']}\n")

    # 内层命令拼装（注意：连字符参数已在上面映射为下划线属性）
    inner = [
        sys.executable, "-u", str(project_root / "backtest" / "backtest_pro.py"),
        "--db", args.db,
        "--days", str(args.days),
        "--topk", str(args.topk),
        "--outdir", str(outdir),
        "--symbols", *args.symbols
    ]
    if args.spa == "on":
        inner += ["--spa", "on", "--spa-alpha", str(args.spa_alpha)]
    if args.pbo == "on":
        inner += ["--pbo", "on", "--pbo-bins", str(args.pbo_bins)]
    if args.impact_recheck == "on":
        inner += ["--impact-recheck", "on"]
    if args.wfo == "on":
        inner += ["--wfo", "on",
                  "--wfo-train", str(args.wfo_train),
                  "--wfo-test",  str(args.wfo_test),
                  "--wfo-step",  str(args.wfo_step)]
    if args.tf_consistency == "on":
        inner += ["--tf-consistency", "on",
                  "--tf-consistency-w", str(args.tf_consistency_w)]

    # 启动内层回测
    proc = subprocess.Popen(inner, cwd=project_root,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1)

    # 渲染循环
    try:
        for raw in proc.stdout:
            line = raw.rstrip("\n")

            # 捕 run_id
            mrun = RE_RUNID.search(line)
            if mrun: seen_run_id = mrun.group(1)

            # 识别单unit完成 or trial进度
            if RE_DONE.search(line):
                done_units += 1
                cur_trial = TRIALS_PER_STRAT
            else:
                mt = RE_TRIAL.search(line)
                if mt:
                    try:
                        cur_trial = int(mt.group("n"))
                    except Exception:
                        pass

            # 全局进度与 ETA
            unit_pct = cur_trial / TRIALS_PER_STRAT
            gprog = (done_units + unit_pct) / max(1, total_units)
            elapsed = time.time() - start_ts
            eta = elapsed * (1/gprog - 1) if gprog > 0 else None
            eta_ema = ema(eta_ema, eta) if eta is not None else eta_ema

            # 推断当前币与策略序号（估算）
            cur_symbol_idx = min(done_units // strategies + 1, total_symbols)
            cur_symbol_idx = max(1, cur_symbol_idx)
            cur_symbol = args.symbols[cur_symbol_idx-1]
            cur_unit_in_symbol = (done_units % strategies) + (1 if cur_trial < TRIALS_PER_STRAT else 0)
            cur_unit_in_symbol = min(cur_unit_in_symbol, strategies)

            # ── 绘制两条进度条 ──
            top_line = (
                f"{C['info']}全局{C['rst']} {cur_symbol_idx}/{total_symbols}  "
                f"{draw_bar(gprog)}  {fmt_eta(eta_ema)}"
            )
            sub_line = (
                f"{C['dim']}当前{C['rst']} {C['emph']}{cur_symbol}{C['rst']}  "
                f"策略 {cur_unit_in_symbol}/{strategies}  "
                f"Trials {cur_trial:02d}/{TRIALS_PER_STRAT}"
            )

            # 用 \r 覆盖刷新区域
            sys.stdout.write("\r" + " " * 160 + "\r")
            sys.stdout.write(top_line + "\n")
            sys.stdout.write(sub_line + " " * 10 + "\r")
            sys.stdout.flush()

            # 关键里程碑（不覆盖）
            if any(key in line for key in MILESTONES):
                print("\n" + f"{C['ok']}✓ 里程碑{C['rst']}  " + line)

            # 常见告警
            if "UserWarning: X does not have valid feature names" in line:
                print("\n" + f"{C['warn']}⚠ Sklearn/LightGBM 特征名警告（不影响回测，但建议对齐特征列名）{C['rst']}")

        proc.wait()
    finally:
        try:
            if proc.poll() is None:
                proc.terminate()
        except Exception:
            pass

    # ── 收尾 & 喂纸面 ──
    print("\n" + f"{C['ok']}✔ 回测阶段完成{C['rst']}  输出目录：{outdir}")
    if not RE_RUNID and not _latest_run_id(outdir):
        print(f"{C['warn']}未捕获 run_id（不影响实盘喂入）{C['rst']}")
    # 若没抓到日志中的 run_id，则用目录兜底
    seen = RE_RUNID.pattern if isinstance(RE_RUNID, str) else None
    if not seen:
        seen = _latest_run_id(outdir)
    if seen:
        print(f"{C['info']}run_id{C['rst']}: {seen}")

    # 自动开启纸面实盘战情面板
    start_paper_console(project_root, args.db)
    print(f"{C['emph']}→ 已启动纸面实盘窗口（独立 PowerShell），参数从 live_best_params.json / top_symbols.txt 读取{C['rst']}")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n" + f"{C['warn']}中断退出{C['rst']}")
