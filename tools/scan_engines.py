# -*- coding: utf-8 -*-
"""
执行引擎自动检测器（Python 版，零依赖）
用法示例（在 PowerShell 或 CMD 均可）：
    python tools\scan_engines.py
    python tools\scan_engines.py --root "D:\quant_system_pro (3)\quant_system_pro" --db "D:\quant_system_v2\data\market_data.db"
可选参数：
    --root ROOT_DIR      项目根目录（默认=脚本上级的上级目录）
    --db   DB_PATH       主数据库路径（默认=D:\quant_system_v2\data\market_data.db）
    --exchange EX        交易所（默认=bitget）
    --risk  N            风险本金U（默认=100）
    --maxpct P           单笔上限百分比（默认=5）
    --lev   L            杠杆（默认=5）
    --timeout SEC        每个候选脚本的帮助探测超时秒数（默认=6）
输出：
    在脚本同目录生成 engine_scan_YYYYMMDD-HHMMSS.txt，并在控制台打印推荐命令
"""
import argparse
import datetime as dt
import os
import sys
import subprocess
import textwrap
from pathlib import Path

DEFAULT_ROOT = r"D:\quant_system_pro (3)\quant_system_pro"
DEFAULT_DB   = r"D:\quant_system_v2\data\market_data.db"

KEYWORDS_CORE = [
    " --db", " --mode", "paper", "real", "exchange", "--exchange",
    "risk", "risk-capital", "max-order", "leverage", "Bitget", "OKX", "Binance",
    "websocket", "ws", "execution", "engine"
]

def run_help(pyexe, script_path, timeout):
    """尝试 -h / --help，返回 (exitcode, stdout+stderr, which_flag_used)"""
    for flag in ("-h", "--help"):
        try:
            cp = subprocess.run(
                [pyexe, str(script_path), flag],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                text=True,
                encoding="utf-8",
            )
            return cp.returncode, cp.stdout or "", flag
        except subprocess.TimeoutExpired:
            return 124, f"[TIMEOUT] {script_path.name} {flag} > {timeout}s", flag
        except Exception as e:
            # 尝试下一个 flag
            last_err = f"[ERROR] {script_path.name} {flag}: {e}"
    # 两个 flag 都失败
    return 1, last_err, None

def score_output(s: str) -> int:
    s_low = s.lower()
    score = 0
    for kw in KEYWORDS_CORE:
        if kw.strip().lower() in s_low:
            score += 1
    # 简单加权：同时出现 paper 与 real 视为更像引擎
    if ("paper" in s_low) and ("real" in s_low):
        score += 2
    if "usage" in s_low or "help" in s_low:
        score += 1
    return score

def find_candidates(live_dir: Path):
    """按优先级搜集候选脚本，保持去重与顺序"""
    pats = [
        "execution_engine*.py",
        "*engine*.py",
        "*exec*.py",
        "*trade*.py",
        "*ws*.py",
    ]
    seen = set()
    out = []
    for pat in pats:
        for p in sorted(live_dir.glob(pat)):
            if p.name not in seen:
                seen.add(p.name)
                out.append(p)
    # 如果还是空，兜底列出所有 .py
    if not out:
        out = sorted(live_dir.glob("*.py"))
    return out

def main():
    parser = argparse.ArgumentParser(description="执行引擎自动检测器（Python 版）")
    # 尝试自动推断默认 ROOT：脚本/上级/上上级
    script_dir = Path(__file__).resolve().parent
    inferred_root = script_dir.parent if script_dir.name.lower() == "tools" else script_dir.parent
    # 若推断失败，回退到 DEFAULT_ROOT
    if not (inferred_root / "live_trading").exists():
        inferred_root = Path(DEFAULT_ROOT)

    parser.add_argument("--root", default=str(inferred_root), help="项目根目录")
    parser.add_argument("--db", default=DEFAULT_DB, help="数据库路径")
    parser.add_argument("--exchange", default="bitget", help="交易所（默认 bitget）")
    parser.add_argument("--risk", type=int, default=100, help="风险本金U")
    parser.add_argument("--maxpct", type=int, default=5, help="单笔上限%%")
    parser.add_argument("--lev", type=int, default=5, help="杠杆")
    parser.add_argument("--timeout", type=int, default=6, help="帮助探测超时秒数")

    args = parser.parse_args()
    pyexe = sys.executable
    root = Path(args.root)
    live = root / "live_trading"

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    report = script_dir / f"engine_scan_{ts}.txt"

    lines = []
    def w(line=""):
        lines.append(line)

    w("="*56)
    w("Engine Scanner · Python")
    w("="*56)
    w(f"ROOT   : {root}")
    w(f"LIVE   : {live}")
    w(f"DB     : {args.db}")
    w(f"TIME   : {ts}")
    w("-"*56)

    if not live.exists():
        w(f"[ERROR] live_trading 目录不存在：{live}")
        report.write_text("\n".join(lines), encoding="utf-8")
        print(report)
        print("\n".join(lines))
        sys.exit(1)

    candidates = find_candidates(live)
    if not candidates:
        w("[ERROR] 未找到任何 .py 候选文件")
        report.write_text("\n".join(lines), encoding="utf-8")
        print(report)
        print("\n".join(lines))
        sys.exit(1)

    w("[1/3] 候选脚本：")
    for p in candidates:
        w(f"  - {p.name}")
    w("")

    best = None
    best_score = -1
    detail_blocks = []

    w("[2/3] 探测 -h / --help 输出（前 40 行）：")
    for p in candidates:
        code, out, used = run_help(pyexe, p, args.timeout)
        head = "\n".join(out.splitlines()[:40])
        detail_blocks.append((p.name, used or "(none)", code, head))
        sc = score_output(out)
        if sc > best_score:
            best_score = sc
            best = p
    for name, used, code, head in detail_blocks:
        w(f"---- {name} ----   flag={used}  exit={code}")
        w(head if head.strip() else "  (no output)")
        w("")

    w("[3/3] 推荐命令：")
    if best is None:
        w("  未能确定最佳脚本，请检查 live_trading 目录。")
    else:
        w(f"  Best match : {best.name}  (score={best_score})")
        paper = f'python "{best}" --db "{args.db}" --mode paper'
        real  = f'python "{best}" --db "{args.db}" --exchange {args.exchange} --mode real --risk-capital {args.risk} --max-order-pct {args.maxpct} --leverage {args.lev}'
        w("  Paper cmd :")
        w(f"    {paper}")
        w("  Real  cmd :")
        w(f"    {real}")

    report.write_text("\n".join(lines), encoding="utf-8")

    # 控制台输出关键信息
    print("="*56)
    print(f"[OK] 扫描完成，报告已生成：{report}")
    if best is not None:
        print(f"[BEST] {best.name}  （score={best_score}）")
        print("\n可直接复制运行：")
        print("Paper:")
        print("  " + f'python "{best}" --db "{args.db}" --mode paper')
        print("Real :")
        print("  " + f'python "{best}" --db "{args.db}" --exchange {args.exchange} --mode real --risk-capital {args.risk} --max-order-pct {args.maxpct} --leverage {args.lev}')
    else:
        print("[WARN] 暂未找到明显的执行引擎脚本；请把 live_trading 目录截图/列表发我。")

if __name__ == "__main__":
    main()
