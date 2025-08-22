# list_db_symbols.py
# 用法：python list_db_symbols.py --db "D:\quant_system_v2\data\market_data.db" [--outdir results]
import argparse, os, sqlite3, re, json, sys

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True, help="SQLite 数据库文件路径（market_data.db）")
ap.add_argument("--outdir", default="results", help="输出目录（默认 results）")
args = ap.parse_args()

if not os.path.exists(args.db):
    print(f"[ERR] 数据库不存在：{args.db}")
    sys.exit(2)

os.makedirs(args.outdir, exist_ok=True)

con = sqlite3.connect(args.db)
cur = con.cursor()
# 读取所有表名（只取普通表）
cur.execute("SELECT name FROM sqlite_master WHERE type=?", ("table",))
names = [r[0] for r in cur.fetchall()]
con.close()

pat = re.compile(r"^([A-Za-z0-9]+)_(1m|5m|15m|30m|1h|2h|4h|1d)$", re.I)
by_symbol = {}
for t in names:
    m = pat.match(t)
    if not m:
        continue
    sym, tf = m.group(1).upper(), m.group(2)
    by_symbol.setdefault(sym, set()).add(tf)

symbols = sorted(by_symbol.keys())

# 写出结果
txt_path = os.path.join(args.outdir, "symbols_from_db.txt")
with open(txt_path, "w", encoding="utf-8") as f:
    for s in symbols:
        f.write(s + "\n")

ccxt_path = os.path.join(args.outdir, "symbols_from_db_ccxt.txt")
def to_ccxt(s):
    if "/" in s: return s
    if s.endswith("USDT"): return s[:-4] + "/USDT"
    return s
with open(ccxt_path, "w", encoding="utf-8") as f:
    for s in symbols:
        f.write(to_ccxt(s) + "\n")

json_path = os.path.join(args.outdir, "timeframes_by_symbol.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump({k: sorted(list(v)) for k, v in by_symbol.items()}, f, ensure_ascii=False, indent=2)

print(f"[OK] 发现 {len(symbols)} 个币；已生成：")
print(" -", txt_path)
print(" -", ccxt_path)
print(" -", json_path)
