# -*- coding: utf-8 -*-
"""
一次性为 strategies_a1a8 中常见整数超参加入口整型化（lookback/period/atr_n/...）
运行：cd /d D:\quant_system_pro && python fix_strategies_casts.py
"""
import io, os, datetime, textwrap

BASE = r"D:\quant_system_pro"
TARGET = os.path.join(BASE, "strategy", "strategies_a1a8.py")

APPEND = r'''
# === auto-guard: int-cast for integer-like args (do not edit) ===
def __qs__toi(x):
    try:
        import numpy as _np
        import numbers as _nb
        if isinstance(x, _nb.Real) and _np.isfinite(x):
            xf=float(x)
            return int(round(xf)) if abs(xf-round(xf))<1e-9 else x
        return x
    except Exception:
        try: return int(x)
        except Exception: return x

def __qs__wrap_intish(fn, names=('lookback','period','atr_n','rsi_n','n_estimators','num_leaves','max_depth','epochs','hidden')):
    import inspect
    def _w(*args, **kwargs):
        sig=inspect.signature(fn)
        ba=sig.bind_partial(*args, **kwargs); ba.apply_defaults()
        for k in list(ba.arguments.keys()):
            if k in names:
                ba.arguments[k]=__qs__toi(ba.arguments[k])
        return fn(*ba.args, **ba.kwargs)
    _w.__name__=fn.__name__
    _w.__doc__ = fn.__doc__
    return _w

try:
    # 针对已存在的策略名做包裹（存在才包）
    for _n in ('strat_lgbm','strat_lgbm_gpu','strat_xgb_gpu','strat_lstm','strat_lstm_gpu'):
        if _n in globals() and callable(globals()[_n]):
            globals()[_n]=__qs__wrap_intish(globals()[_n])
except Exception:
    pass
'''

def main():
    if not os.path.exists(TARGET):
        print(f"[ERR] 未找到 {TARGET}")
        return
    with io.open(TARGET, "r", encoding="utf-8") as f:
        s=f.read()
    ts=datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    bak=TARGET+f".bak.{ts}"
    try:
        io.open(bak,"w",encoding="utf-8").write(s)
        print(f"[BACKUP] {TARGET} -> {bak}")
    except Exception as e:
        print(f"[WARN] 备份失败: {e}")
    if "auto-guard: int-cast" not in s:
        s2=s.rstrip()+"\n\n"+APPEND.strip()+"\n"
        io.open(TARGET,"w",encoding="utf-8").write(s2)
        print("[PATCH] 已追加入口整型化守护")
    else:
        print("[SKIP] 已存在，无需重复")

if __name__ == "__main__":
    main()
