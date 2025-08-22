# -*- coding: utf-8 -*-
"""
从 a6_strategy_scores*.csv 导出 deploy/live_best_params.json
- 自动寻找最新分数表（也可用 --scores 指定）
- 按 (symbol,timeframe) 取 score 最高的一行
- 解析 params 字段（若为 JSON 字符串）
用法：
  python -u tools/emit_best_params.py --scores results/a6_strategy_scores_*.csv --out deploy/live_best_params.json
  python -u tools/emit_best_params.py --out deploy/live_best_params.json   # 自动找最新
"""
import argparse, csv, json
from pathlib import Path

PROJ = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS = PROJ / "results"
DEFAULT_OUT = PROJ / "deploy" / "live_best_params.json"

def find_scores(scores_arg: str) -> Path:
    if scores_arg:
        p = Path(scores_arg)
        if p.is_file(): return p
        if p.is_dir():
            c = sorted(p.glob("a6_strategy_scores*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
            return c[0] if c else None
        parent = p.parent if p.parent.name else DEFAULT_RESULTS
        c = sorted(parent.glob("a6_strategy_scores*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
        return c[0] if c else None
    c = sorted(DEFAULT_RESULTS.glob("a6_strategy_scores*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    return c[0] if c else None

def as_float(x, default=0.0):
    try: return float(str(x).replace("%","").strip())
    except: return default

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", default="", help="分数CSV路径/目录/模式（可省略，自动找最新）")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    scores = find_scores(args.scores)
    if not scores or not scores.exists():
        print(f"[FATAL] 未找到 a6_strategy_scores*.csv，先运行回测。已尝试：{args.scores or '(默认)'} 与 {DEFAULT_RESULTS}")
        exit(2)

    best = {}  # (symbol, tf) -> row
    with scores.open("r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            sym = r.get("symbol") or r.get("Symbol") or ""
            tf  = r.get("timeframe") or r.get("tf") or ""
            if not sym or not tf: continue
            score = as_float(r.get("score") or r.get("Score") or r.get("metric_score") or 0)
            key = (sym, tf)
            if key not in best or score > as_float(best[key].get("score") or 0):
                best[key] = r

    payload = []
    for (sym, tf), r in best.items():
        strat = r.get("strategy") or r.get("Strategy") or ""
        ptxt  = (r.get("params") or r.get("Params") or "{}").strip()
        try:
            params = json.loads(ptxt) if ptxt.startswith("{") else {}
        except:
            params = {}
        payload.append({"symbol": sym, "tf": tf, "strategy": strat, "params": params})

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] 写出最优参数 {out}  共 {len(payload)} 条")

if __name__ == "__main__":
    main()
