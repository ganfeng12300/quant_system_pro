# -*- coding: utf-8 -*-
"""
patch_resolver_aliases_v4.py —— 末尾追加覆盖 _resolve_fn（A1/A2/A3/A7 直接映射 + 兜底）
运行：cd /d D:\quant_system_pro && python patch_resolver_aliases_v4.py
"""
import io, os, datetime

BASE   = r"D:\quant_system_pro"
TARGET = os.path.join(BASE, "backtest", "backtest_pro.py")

NEW_RESOLVER = r'''
# === auto override (append v4): direct alias + robust fallback ===
from importlib import import_module as _imp

def _resolve_fn(strat_key):
    S   = _imp("strategy.strategies_a1a8")
    key = str(strat_key)
    K   = key.upper()

    # 0) 直接别名映射（根据你模块里已有函数名）
    direct = {
        "A1":  "strat_bbands",
        "A2":  "strat_atr_break",
        "A3":  "strat_rsi_rev",
        "A7":  "strat_ma_cross",
        "XGB": "strat_xgb_gpu",
        "LGBM":"strat_lgbm_gpu",
        "LSTM":"strat_lstm_gpu",
    }
    name = direct.get(K)
    if name and hasattr(S, name) and callable(getattr(S, name)):
        print(f"[Resolver] {K} -> {name}")
        return getattr(S, name)

    # 1) 直接属性
    if hasattr(S, key) and callable(getattr(S, key)):
        return getattr(S, key)

    # 2) 常见注册表/字典
    for k in ("STRATEGIES","STRATEGY_FUNCS","STRAT_TABLE","REGISTRY","ALIASES","ALIAS"):
        if hasattr(S, k):
            M = getattr(S, k)
            try:
                fn = M.get(key) if hasattr(M, "get") else (M[key] if key in M else None)
            except Exception:
                fn = None
            if callable(fn):
                return fn

    # 3) 关键词兜底
    names = [n for n in dir(S) if n.startswith("strat_") and callable(getattr(S, n, None))]
    low   = {n.lower(): n for n in names}
    kw_map = {
        "A1":  ["bbands","band"],
        "A2":  ["break","don","channel","bo","brk","donch"],
        "A3":  ["rsi"],
        "A4":  ["macd"],
        "A7":  ["ma_cross","cross","trend","sma","ema","adx"],
    }
    for kw in kw_map.get(K, []):
        for ln, orig in low.items():
            if kw in ln:
                print(f"[Resolver:fuzzy] {K} -> {orig}")
                return getattr(S, orig)

    raise KeyError(f"Unknown strategy: {strat_key}")
'''

def main():
    if not os.path.exists(TARGET):
        print(f"[ERR] 未找到 {TARGET}")
        return
    s = io.open(TARGET, "r", encoding="utf-8").read()
    ts  = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = TARGET + f".bak.{ts}"
    try:
        io.open(bak, "w", encoding="utf-8").write(s)
        print(f"[BACKUP] {TARGET} -> {bak}")
    except Exception as e:
        print(f"[WARN] 备份失败: {e}")
    s2 = s.rstrip()+"\n\n"+NEW_RESOLVER.strip()+"\n"
    io.open(TARGET, "w", encoding="utf-8").write(s2)
    print("[PATCH] 末尾追加覆盖版 _resolve_fn 已写入（A1/A2/A3/A7 直接映射 + 兜底）")

if __name__ == "__main__":
    main()
