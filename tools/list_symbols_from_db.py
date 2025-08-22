# tools/list_symbols_from_db.py
"""
从 SQLite 数据库枚举可用的 (symbol, timeframe)。
- 识别表名模式：SYMBOL_TIMEFRAME（如 BTCUSDT_1h）
- 输出：
  1) results/symbols_from_db.txt         （BTCUSDT 格式，每行一个）
  2) results/symbols_from_db_ccxt.txt    （BTC/USDT 格式，每行一个）
  3) results/timeframes_by_symbol.json   （每个币对应已有周期）
Usage:
  python tools/list_symbols_from_db.py --db D:\quant\market_data.db
"""
import argparse, sqlite3, re, os, json

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True)
ap.add_argument("--outdir", default="results")
args = ap.parse_args()

os.makedirs(args.outdir, exist_ok=True)

con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
names = [r[0] for r in cur.fetchall()]
con.close()

pat = re.compile(r"^([A-Z0-9]+)_(1m|5m|15m|30m|1h|2h|4h|1d)$", re.I)
by_symbol = {}
for t in names:
    m = pat.match(t)
    if not m: 
        continue
    sym, tf = m.group(1).upper(), m.group(2)
    by_symbol.setdefault(sym, set()).add(tf)

symbols = sorted(by_symbol.keys())
with open(os.path.join(args.outdir, "symbols_from_db.txt"), "w", encoding="utf-8") as f:
    for s in symbols:
        f.write(s + "\n")

def to_ccxt(s):
    if "/" in s: return s
    if s.endswith("USDT"): return s[:-4] + "/USDT"
    return s

with open(os.path.join(args.outdir, "symbols_from_db_ccxt.txt"), "w", encoding="utf-8") as f:
    for s in symbols:
        f.write(to_ccxt(s) + "\n")

json.dump({k: sorted(list(v)) for k,v in by_symbol.items()}, open(os.path.join(args.outdir,"timeframes_by_symbol.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)

print(f"[OK] 发现 {len(symbols)} 个币；已写入 results/ 下三份文件。")
