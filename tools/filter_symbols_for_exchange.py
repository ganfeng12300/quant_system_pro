import argparse, ccxt, os, re, sys
ap=argparse.ArgumentParser()
ap.add_argument("--in",dest="inp",required=True)
ap.add_argument("--out",dest="outp",required=True)
ap.add_argument("--perp",action="store_true",help="筛选USDT本位永续；默认筛现货")
args=ap.parse_args()

if not os.path.exists(args.inp): print("[ERR] no input",args.inp); sys.exit(2)
ex=ccxt.binance({"enableRateLimit":True}); ex.load_markets()
spot=set(s.replace("/","") for s,m in ex.markets.items() if m.get("spot"))
perp=set(s.split("/")[0]+s.split("/")[1].split(":")[0] for s,m in ex.markets.items() if m.get("swap") and m.get("settle")=="USDT")
ok=perp if args.perp else spot

def norm(s):
    s=s.strip().upper()
    if not s or re.match(r"^\d",s): return None
    return s

res=[]
for line in open(args.inp,encoding="utf-8"): 
    t=norm(line); 
    if t and t in ok: res.append(t)
os.makedirs(os.path.dirname(args.outp) or ".",exist_ok=True)
open(args.outp,"w",encoding="utf-8").write("\n".join(sorted(set(res)))+"\n")
print(f"[OK] Binance {'USDT永续' if args.perp else '现货'}可用 {len(res)} 个 → {args.outp}")
