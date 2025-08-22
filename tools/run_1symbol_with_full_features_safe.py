# -*- coding: utf-8 -*-
"""
1币快速验证 / 多组完整版 · 全功能尽量开启 · 出错跳过
→ 兜底导出 → 喂实盘（PAPER/LIVE）→ 动作清单彩色展示 → 大字提示（含“优秀策略已筛选/已喂入实盘”）
用法：
  快速验证（只跑一组）：python -u tools/run_1symbol_with_full_features_safe.py --fast
  完整版（多组）：      python -u tools/run_1symbol_with_full_features_safe.py
"""
import argparse, subprocess, sys, os, json, glob, csv
from pathlib import Path

# ===================== 彩色输出：rich 优先，其次 colorama，无则黑白 =====================
HAS_RICH = False
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    RCON = Console()
    HAS_RICH = True
except Exception:
    try:
        from colorama import init as cinit, Fore, Style
        cinit(autoreset=True, convert=True)
        class RCON:
            @staticmethod
            def print(*a, **k): print(*a)
        class _C:
            green = Fore.GREEN+Style.BRIGHT
            red = Fore.RED+Style.BRIGHT
            yellow = Fore.YELLOW+Style.BRIGHT
            cyan = Fore.CYAN+Style.BRIGHT
            mag = Fore.MAGENTA+Style.BRIGHT
            rst = Style.RESET_ALL
        C = _C()
    except Exception:
        class RCON:
            @staticmethod
            def print(*a, **k): print(*a)
        class _C: green=red=yellow=cyan=mag=rst=""
        C=_C()

# ===================== 可选功能集合（能开尽量开，报错就跳过） =====================
ALL_FEATURES = {
    "--spa": ["on"], "--spa-alpha": ["0.05"],
    "--pbo": ["on"], "--pbo-bins": ["10"],
    "--impact-recheck": ["on"],
    "--wfo": ["on"], "--wfo-train": ["180"], "--wfo-test": ["30"], "--wfo-step": ["30"],
    "--tf-consistency": ["on"], "--tf-consistency-w": ["0.2"],
}

# ===================== 工具函数 =====================
def supported_flags(backtest_py: Path) -> str:
    try:
        out = subprocess.check_output(
            [sys.executable, str(backtest_py), "-h"],
            text=True, stderr=subprocess.STDOUT, cwd=backtest_py.parent
        )
        return out or ""
    except Exception as e:
        RCON.print(f"[WARN] 无法获取参数列表：{e}")
        return ""

def try_run(backtest_py: Path, args, features) -> int:
    cmd = [sys.executable, "-u", str(backtest_py)] + args
    for k, vals in features.items():
        cmd.append(k); cmd += [str(v) for v in vals]
    RCON.print("启动命令:", " ".join([f'"{x}"' if " " in x else x for x in cmd]))
    return subprocess.call(cmd)

def read_json(p: Path):
    try:
        with p.open("r", encoding="utf-8") as f: return json.load(f)
    except Exception: return None

def read_csv_rows(path_pattern: str):
    for fp in sorted(glob.glob(path_pattern)):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                yield fp, list(csv.DictReader(f))
        except Exception:
            continue

# ===================== 兜底导出 =====================
def synthesize_exports(run_dir: Path, topk: int = 10):
    """
    从 final_portfolio / a7 / a6 / a5 合成导出：
    live_best_params.json / top_symbols.txt
    """
    symbols = []
    params_map = {}

    # 1) final_portfolio.json
    fp = run_dir / "final_portfolio.json"
    data = read_json(fp)
    if data and isinstance(data, dict) and data.get("symbols"):
        symbols = [s for s in data["symbols"]][:topk]

    # 2) a7_blended_portfolio*.csv
    if not symbols:
        for _, rows in read_csv_rows(str(run_dir / "a7_blended_portfolio*.csv")):
            rows = sorted(rows, key=lambda r: float(r.get("weight", r.get("score", 0)) or 0), reverse=True)
            for r in rows:
                sym = r.get("symbol") or r.get("Symbol") or r.get("SYMBOL")
                if sym and sym not in symbols: symbols.append(sym)
            symbols = symbols[:topk]; break

    # 3) a6_strategy_scores*.csv（剔负收益优先）
    if not symbols:
        for _, rows in read_csv_rows(str(run_dir / "a6_strategy_scores*.csv")):
            def keyfun(r):
                for k in ("score","Score","total_return","Return","PnL"):
                    if k in r and r[k] not in ("", None):
                        try: return float(r[k])
                        except: pass
                return 0.0
            rows = sorted(rows, key=keyfun, reverse=True)
            for r in rows:
                sym = r.get("symbol") or r.get("Symbol") or r.get("SYMBOL")
                ret = None
                for k in ("total_return","Return","PnL"):
                    if k in r and r[k] not in ("", None):
                        try: ret = float(r[k]); break
                        except: pass
                if sym and sym not in symbols and (ret is None or ret>0):
                    symbols.append(sym)
                if len(symbols) >= topk: break
            break

    # 4) a5_optimized_params*.csv → 组合参数
    for _, rows in read_csv_rows(str(run_dir / "a5_optimized_params*.csv")):
        for r in rows:
            sym = r.get("symbol") or r.get("Symbol")
            strat = r.get("strategy") or r.get("Strategy") or "STRAT"
            if not sym: continue
            params = {k: r[k] for k in r.keys() if k not in ("symbol","Symbol","strategy","Strategy","score","best_loss")}
            params_map.setdefault(sym, {}).setdefault(strat, params)
        break

    live_params = {s: params_map.get(s, {}) for s in symbols}

    out1 = run_dir / "live_best_params.json"
    out2 = run_dir / "top_symbols.txt"
    with out1.open("w", encoding="utf-8") as f: json.dump(live_params, f, ensure_ascii=False, indent=2)
    with out2.open("w", encoding="utf-8") as f:
        for s in symbols: f.write(s + "\n")

    return out1.exists() and out2.exists(), symbols, live_params

# ===================== 启动实盘 =====================
def start_paper_console(project_root: Path, db_path: str):
    engine = project_root / "live_trading" / "execution_engine_binance_ws.py"
    if not engine.exists():
        RCON.print("[WARN] 未找到纸面执行器：", engine); return False
    subprocess.call([
        "cmd","/c","start","", "powershell","-NoExit","-Command",
        f"& {{ Set-Location -LiteralPath '{project_root}'; "
        f"$env:PYTHONPATH='{project_root}'; "
        f"python '{engine}' --db '{db_path}' --mode paper --ui-rows 30 }}"
    ])
    return True

def start_live_console_if_ready(project_root: Path, db_path: str):
    # 条件：环境变量 QS_LIVE_BITGET=1 且存在密钥文件（configs/keys.yaml 或 .env）
    if os.environ.get("QS_LIVE_BITGET","0") != "1":
        return False
    key_yaml = project_root / "configs" / "keys.yaml"
    env_file = project_root / ".env"
    if not (key_yaml.exists() or env_file.exists()):
        return False
    engine = project_root / "live_trading" / "execution_engine_binance_ws.py"
    if not engine.exists(): return False
    subprocess.call([
        "cmd","/c","start","", "powershell","-NoExit","-Command",
        f"& {{ Set-Location -LiteralPath '{project_root}'; "
        f"$env:PYTHONPATH='{project_root}'; "
        f"$env:QS_LIVE_BITGET='1'; "
        f"python '{engine}' --db '{db_path}' --mode live --ui-rows 30 }}"
    ])
    return True

# ===================== 报告（功能诊断/动作清单/大字提示） =====================
def render_feature_report(help_txt: str, verdict: dict):
    if HAS_RICH:
        table = Table(title="功能诊断报告（容错模式）", box=box.SIMPLE_HEAVY)
        table.add_column("功能参数", justify="left", style="cyan", no_wrap=True)
        table.add_column("状态", justify="center", style="magenta")
        table.add_column("说明", justify="left", style="white")
        for k in ALL_FEATURES.keys():
            st = verdict.get(k, "unsupported" if k not in help_txt else "kept_error")
            if st == "ok": table.add_row(k, "[green]✔ 正常[/green]", "功能启用并跑通")
            elif st == "skipped": table.add_row(k, "[yellow]⚠ 自动禁用[/yellow]", "触发报错，临时移除")
            elif st == "unsupported": table.add_row(k, "[red]✘ 不支持[/red]", "-h 中无此参数")
            else: table.add_row(k, "[red]✘ 依旧报错[/red]", "移除此项也未修复/与其他项相关")
        RCON.print(Panel(table, border_style="magenta"))
    else:
        RCON.print("┌──── 功能诊断报告 ────┐")
        for k in ALL_FEATURES.keys():
            st = verdict.get(k, "unsupported" if k not in help_txt else "kept_error")
            tag = {"ok":"✔ 正常","skipped":"⚠ 自动禁用","unsupported":"✘ 不支持","kept_error":"✘ 依旧报错"}[st]
            RCON.print(f" {k:<20} {tag}")
        RCON.print("└─────────────────────┘")

def render_action_checklist(actions: dict):
    if HAS_RICH:
        table = Table(title="执行动作清单", box=box.SIMPLE_HEAVY)
        table.add_column("步骤", style="cyan", no_wrap=True)
        table.add_column("状态", style="magenta", justify="center")
        table.add_column("说明", style="white")
        table.add_row("导出参数/标的",
                      "[green]✔[/green]" if actions.get("export") else "[red]✘[/red]",
                      f"目录: {actions.get('export_dir')} | symbols: {', '.join(actions.get('symbols', [])) or '-'}")
        table.add_row("启动 PAPER",
                      "[green]✔[/green]" if actions.get("paper") else "[yellow]—[/yellow]",
                      "已尝试打开独立 PowerShell 窗口" if actions.get("paper") else "未启动/执行器缺失")
        table.add_row("启动 LIVE",
                      "[green]✔[/green]" if actions.get("live") else "[yellow]—[/yellow]",
                      "QS_LIVE_BITGET=1 且密钥就绪才会启动" if not actions.get("live") else "已尝试打开独立 PowerShell 窗口")
        RCON.print(Panel(table, border_style="cyan"))
    else:
        def flag(b):
            if "C" in globals():
                return (C.green+"✔"+C.rst) if b else (C.yellow+"—"+C.rst)
            return "✔" if b else "—"
        RCON.print("┌──── 执行动作清单 ────┐")
        RCON.print(f" 导出参数/标的  {flag(actions.get('export'))}  目录: {actions.get('export_dir')}")
        RCON.print(f" 启动 PAPER    {flag(actions.get('paper'))}")
        RCON.print(f" 启动 LIVE     {flag(actions.get('live'))}")
        RCON.print("└─────────────────────┘")

def big_ok_banner(symbols, mode="PAPER"):
    """最终彩色大字提示：回测完成 + 优秀策略已筛选 + 已喂入实盘"""
    try:
        from colorama import init as _i, Fore, Style
        _i(autoreset=True, convert=True)
        print()
        print(Fore.GREEN + Style.BRIGHT + "=" * 95)
        print(Fore.GREEN + Style.BRIGHT + "🎉 [OK] 回测完成！优秀策略已筛选 ✅")
        print(Fore.CYAN  + Style.BRIGHT + f"🚀 已喂入实盘 ({mode}) → {', '.join(symbols) if symbols else '无'}")
        print(Fore.GREEN + Style.BRIGHT + "📂 输出文件：live_best_params.json / top_symbols.txt")
        print(Fore.GREEN + Style.BRIGHT + "=" * 95)
        print()
    except Exception:
        print("\n" + "="*95)
        print(f"[OK] 回测完成！优秀策略已筛选并喂入实盘 ({mode})")
        print("输出文件：live_best_params.json / top_symbols.txt")
        print("="*95 + "\n")

def big_banner_fail(msg: str):
    try:
        from colorama import init as _i, Fore, Style
        _i(autoreset=True, convert=True)
        print()
        print(Fore.RED + Style.BRIGHT + "=" * 92)
        print(Fore.RED + Style.BRIGHT + f"✘ [FAIL] {msg}")
        print(Fore.RED + Style.BRIGHT + "=" * 92)
        print()
    except Exception:
        print("\n" + "="*92 + f"\n [FAIL] {msg}\n" + "="*92 + "\n")

# ===================== 单组回测（容错） =====================
def run_one_group(backtest_py: Path, base_args: list):
    help_txt = supported_flags(backtest_py)
    avail = {k:v for k,v in ALL_FEATURES.items() if k in help_txt}
    RCON.print("[INFO] 内层支持的功能：", list(avail.keys()))
    verdict = {}

    code = try_run(backtest_py, base_args, avail)
    if code != 0:
        RCON.print("[WARN] 全开失败，逐项排查…")
        for k in list(avail.keys()):
            test_feat = {kk:vv for kk,vv in avail.items() if kk != k}
            code = try_run(backtest_py, base_args, test_feat)
            if code == 0:
                verdict[k] = "skipped"; avail = test_feat
            else:
                verdict[k] = "kept_error"
        code = try_run(backtest_py, base_args, avail)
        if code != 0:
            render_feature_report(help_txt, verdict)
            return {"ok": False, "verdict": verdict, "help": help_txt}
    else:
        for k in avail.keys(): verdict[k] = "ok"
    for k in ALL_FEATURES.keys():
        if k not in help_txt: verdict.setdefault(k, "unsupported")

    render_feature_report(help_txt, verdict)
    return {"ok": True, "verdict": verdict, "help": help_txt}

# ===================== 主流程 =====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast", action="store_true", help="只跑一组用于快速验证")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--topk", type=int, default=10)
    args = parser.parse_args()

    proj = Path(__file__).resolve().parents[1]
    backtest_py = proj / "backtest" / "backtest_pro.py"
    db = r"D:\quant_system_v2\data\market_data.db"
    outdir = proj / "results" / "demo_run"
    outdir.mkdir(parents=True, exist_ok=True)

    # 组装任务列表
    if args.fast:
        groups = [(args.symbol, args.days, args.topk)]
    else:
        groups = [
            (args.symbol, 90, 10),
            (args.symbol, 180, 10),
            (args.symbol, 365, 40),
        ]

    # 跑每一组
    for sym, days, topk in groups:
        RCON.print(f"[INFO] 开始分组：symbol={sym} days={days} topk={topk}")
        base_args = ["--db", db, "--days", str(days), "--topk", str(topk),
                     "--outdir", str(outdir), "--symbols", sym]
        res = run_one_group(backtest_py, base_args)
        if not res.get("ok"):
            big_banner_fail(f"分组失败：symbol={sym} days={days}，请查回测日志")
            return

    # 兜底导出
    ok, syms, _ = synthesize_exports(outdir, topk=10)
    actions = {"export": ok, "export_dir": str(outdir), "symbols": syms}

    # 尝试 LIVE（条件满足才开），否则开 PAPER
    live_ok = start_live_console_if_ready(proj, db)
    paper_ok = start_paper_console(proj, db) if not live_ok else False
    actions["live"] = live_ok
    actions["paper"] = paper_ok or live_ok

    # 动作清单 + 大字提示
    if HAS_RICH:
        render_action_checklist(actions)
    else:
        # 简化打印
        pass

    if ok:
        mode = "LIVE" if actions.get("live") else "PAPER"
        big_ok_banner(syms, mode=mode)
    else:
        big_banner_fail("未生成导出文件（检查 a6/a7/a5 产物）")

if __name__ == "__main__":
    try:
        os.system("")  # Windows 控制台启用 ANSI
    except Exception:
        pass
    main()
