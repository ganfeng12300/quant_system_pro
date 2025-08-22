import json, os, sys, re
from pathlib import Path

bt = r"results\qs2_bt_smoke_14d.json"  # 换成你要联调的回测结果文件
out = Path(r"deploy\qs2_live_symbols.txt")
out.parent.mkdir(parents=True, exist_ok=True)

syms = []
if os.path.exists(bt):
    with open(bt,"r",encoding="utf-8") as f:
        data = json.load(f)
    # 尝试几种常见结构：按 夏普/收益率/PnL 排序取前5
    buckets = []
    if isinstance(data, dict):
        # 可能是 { "results": [ { "symbol": "...", "sharpe": ..., "pnl": ...}, ...] }
        if "results" in data and isinstance(data["results"], list):
            buckets = data["results"]
        elif "by_symbol" in data and isinstance(data["by_symbol"], dict):
            buckets = [dict(symbol=k, **v) for k,v in data["by_symbol"].items() if isinstance(v, dict)]
    elif isinstance(data, list):
        buckets = data

    def key_fn(x):
        # 兼容不同字段名
        return (
            float(x.get("sharpe", x.get("Sharpe", 0)) or 0),
            float(x.get("pnl", x.get("PnL", x.get("return", 0)) ) or 0)
        )
    if buckets:
        buckets = [b for b in buckets if isinstance(b, dict) and "symbol" in b]
        buckets.sort(key=key_fn, reverse=True)
        syms = [b["symbol"] for b in buckets[:5]]

# 回退：没有回测文件或结构不识别，就取 ready 清单前5
if not syms:
    try:
        with open(r"results\qs2_ready_for_bt.txt","r",encoding="utf-8") as f:
            syms = [line.strip() for line in f if line.strip()][:5]
    except:
        pass

if not syms:
    print("[ERR] 没有可用符号，请先准备 results\\qs2_ready_for_bt.txt 或回测 JSON"); sys.exit(2)

with open(out,"w",encoding="utf-8") as f:
    f.write("\n".join(syms))
print("[OK] 写入", out, "symbols=", syms)
