import ccxt,os,re,sys
inp=r"results\symbols_from_db.txt"; outp=r"results\symbols_bitget_perp.txt"
ms=open(inp,"r",encoding="utf-8").read().splitlines() if os.path.exists(inp) else []
ex=ccxt.bitget(); ex.load_markets()
ok=set([s for s in ex.symbols if s.endswith(":USDT")])  # USDT本位永续
res=[]
for m in ms:
    t=m.strip().upper()
    if not t or re.match(r"^\d",t): continue
    tccxt=(t[:-4]+"/USDT:USDT") if ("/" not in t and t.endswith("USDT")) else t
    if tccxt in ok: res.append(t)  # 写回原始BTCUSDT命名，表名保持一致
os.makedirs("results",exist_ok=True); open(outp,"w",encoding="utf-8").write("\n".join(sorted(set(res)))+"\n")
print(f"[OK] Bitget永续可用: {len(res)} 个，已写入 {outp}")
