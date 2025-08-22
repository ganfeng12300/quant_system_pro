# -*- coding: utf-8 -*-
"""
从回测打分表 a6_strategy_scores*.csv 中挑选 TopN 符号，写入 deploy/qs2_live_symbols.txt
- 兼容多种表头: symbol/Symbol, timeframe/tf, score/Score, trades/metric_trades 等
- 支持按 timeframe 过滤、最小成交数/最大回撤约束
- 若指定的 --scores 文件不存在，会自动在 results/ 目录中寻找最新的 a6_strategy_scores*.csv
用法示例：
  python -u tools/qs2_pick_live_syms.py --scores results/a6_strategy_scores_latest.csv --top 20 --out deploy/qs2_live_symbols.txt
  python -u tools/qs2_pick_live_syms.py --top 30 --tf 1h
"""
import argparse, csv, sys, os, glob, time
from pathlib import Path

PROJ = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS = PROJ / "results"
DEFAULT_OUT = PROJ / "deploy" / "qs2_live_symbols.txt"

def log(msg): print(msg, flush=True)

def _as_float(x, default=0.0):
    try:
        if x is None: return default
        return float(str(x).replace("%","").strip())
    except: return default

def _as_int(x, default=0):
    try:
        return int(float(str(x).strip()))
    except: return default

def find_scores_file(scores_arg: str) -> Path:
    """
    解析 --scores：
    - 若是存在的文件：直接用
    - 若是目录：在其中找最新 a6_strategy_scores*.csv
    - 若文件不存在：在其父目录(或 results/)回退寻找最新 a6_strategy_scores*.csv
    - 若未提供：默认在 results/ 下找最新
    """
    if scores_arg:
        p = Path(scores_arg)
        if p.is_file():
            return p
        # 若传的是目录
        if p.is_dir():
            cand = sorted(p.glob("a6_strategy_scores*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
            if cand: return cand[0]
        # 若传的是一个不存在的文件名，则在同目录或默认 results 查找
        parent = p.parent if p.parent.name else DEFAULT_RESULTS
        cand = sorted(Path(parent).glob("a6_strategy_scores*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
        if cand: return cand[0]
    # 完全没给或没找到 → 默认 results
    cand = sorted(DEFAULT_RESULTS.glob("a6_strategy_scores*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    return cand[0] if cand else None

def pick_symbols(scores_file: Path, top: int, tf_filter: str, min_trades: int, max_dd: float):
    chosen = {}
    rows_seen = 0
    with open(scores_file, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            rows_seen += 1
            sym = row.get("symbol") or row.get("Symbol") or row.get("SYMBOL") or ""
            tf  = row.get("timeframe") or row.get("tf") or row.get("Timeframe") or ""
            if not sym: continue
            if tf_filter and str(tf).lower() != str(tf_filter).lower():
                continue

            score = _as_float(row.get("score") or row.get("Score") or row.get("metric_score") or 0)
            trades = _as_int(row.get("metric_trades") or row.get("trades") or row.get("Trades") or 0)
            dd = row.get("dd") or row.get("max_dd") or row.get("drawdown") or row.get("DD")
            ddv = _as_float(dd, 0.0)

            if trades < min_trades: 
                continue
            if max_dd > 0 and abs(ddv) > abs(max_dd):
                continue

            # 以 symbol 维度保留最高分
            cur = chosen.get(sym)
            if (cur is None) or (score > cur["score"]):
                chosen[sym] = {"symbol": sym, "tf": tf, "score": score, "trades": trades, "dd": ddv}

    # 按 score 降序
    sorted_syms = sorted(chosen.values(), key=lambda x: x["score"], reverse=True)
    keep = sorted_syms[:max(1, top)]
    return keep, rows_seen

def write_list(out_path: Path, items):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(f"{it['symbol']}\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", help="scores CSV 路径/目录；若缺省或不存在，将自动在 results/ 中寻找最新", default="")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--tf", help="限定 timeframe，例如 1h/4h（可选）", default="")
    ap.add_argument("--min-trades", type=int, default=1, help="最小成交笔数过滤")
    ap.add_argument("--max-dd", type=float, default=0.0, help="最大允许回撤（绝对值过滤），0 表示不限制")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    scores_file = find_scores_file(args.scores)
    if not scores_file or not scores_file.exists():
        log(f"[FATAL] 未找到打分文件 a6_strategy_scores*.csv（尝试 --scores 指定或先完成一次回测输出）。")
        log(f"      已尝试：{args.scores or '(默认)'} 以及 {DEFAULT_RESULTS}")
        sys.exit(2)

    log(f"[OK] 使用打分文件：{scores_file}")
    keep, rows_seen = pick_symbols(scores_file, args.top, args.tf, args.min_trades, args.max_dd)
    if not keep:
        log(f"[WARN] 无满足条件的符号（rows_seen={rows_seen}，请检查过滤条件，例如 --tf / --min-trades / --max-dd）")
        # 仍然写出空文件，避免后续脚本找不到
        write_list(Path(args.out), [])
        sys.exit(1)

    write_list(Path(args.out), keep)
    log(f"[OK] 已写出 Top{len(keep)} 符号 → {args.out}")
    log("----- 预览 -----")
    for i, it in enumerate(keep, 1):
        log(f"{i:>2}. {it['symbol']}  tf={it['tf']}  score={it['score']:.4f}  trades={it['trades']}  dd={it['dd']}")
    sys.exit(0)

if __name__ == "__main__":
    main()
