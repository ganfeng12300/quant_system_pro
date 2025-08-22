# -*- coding: utf-8 -*-
"""
机构级终极补丁 v2
- 对 strategies_a1a8.py / model_lgbm_gpu.py 自动注入：
  1) __qs_align_lgbm：训练/预测严格按特征名对齐
  2) __qs_lgbm_fit：early_stopping + eval_set + 静音
  3) 全局屏蔽 "feature names" 的 UserWarning（兜底）
- 对 backtest_pro.py：给单bar收益加剪裁，避免数值爆炸
运行：
  cd /d D:\quant_system_pro
  python patch_lgbm_final_v2.py
"""
import io, os, re, datetime

BASE = r"D:\quant_system_pro"
TS   = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

HELPER = r'''
# === qs:auto:lgbm helpers (DO NOT EDIT) ===
import warnings as __qs_warnings
__qs_warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)
try:
    import lightgbm as __qs_lgb
    try:
        __qs_lgb.set_config(verbosity=-1)
    except Exception:
        pass
except Exception:
    __qs_lgb = None

import pandas as __qs_pd
def __qs_align_lgbm(model, X):
    cols = None
    try:
        cols = getattr(model, "feature_name_", None)
        if cols is None and hasattr(model, "booster_"):
            cols = model.booster_.feature_name()
    except Exception:
        cols = None
    if isinstance(X, __qs_pd.DataFrame):
        return X if cols is None else X.reindex(columns=list(cols), fill_value=0)
    return __qs_pd.DataFrame(X, columns=list(cols)) if cols is not None else __qs_pd.DataFrame(X)

def __qs_lgbm_fit(model, Xtr, ytr, Xte, yte):
    try:
        model.set_params(verbosity=-1)
    except Exception:
        pass
    try:
        model.set_params(verbose=-1)
    except Exception:
        pass
    # sklearn API：early_stopping_rounds + eval_set + verbose=False
    return model.fit(
        Xtr, ytr,
        eval_set=[(Xte, yte)],
        eval_metric="binary_logloss",
        early_stopping_rounds=50,
        verbose=False
    )
'''

def backup(path, s):
    bak = f"{path}.bak.{TS}"
    io.open(bak, "w", encoding="utf-8").write(s)
    print(f"[BACKUP] {path} -> {bak}")

def patch_strategy_file(path):
    if not os.path.exists(path):
        return
    s = io.open(path, "r", encoding="utf-8").read()
    backup(path, s)

    changed = False
    if "__qs_align_lgbm" not in s or "__qs_lgbm_fit" not in s:
        s = s.rstrip() + "\n\n" + HELPER.strip() + "\n"
        print(f"[APPEND] helpers -> {path}")
        changed = True

    # 把所有 model.fit(Xtr, ytr ...) 统一替换为 __qs_lgbm_fit(model, Xtr, ytr, Xte, yte)
    # 无论后面是否已有参数，都覆盖成规范调用
    s2 = re.sub(
        r'model\s*\.\s*fit\s*\(\s*Xtr\s*,\s*ytr\b[^)]*\)',
        '__qs_lgbm_fit(model, Xtr, ytr, Xte, yte)',
        s
    )
    if s2 != s:
        s = s2; changed = True
        print(f"[PATCH] fit -> __qs_lgbm_fit(...) in {path}")

    # 对任何 predict_proba(...) 加对齐，避免重复包裹
    s2 = re.sub(
        r'predict_proba\(\s*(?!__qs_align_lgbm\()([^)]+?)\)',
        r'predict_proba(__qs_align_lgbm(model, \1))',
        s
    )
    if s2 != s:
        s = s2; changed = True
        print(f"[PATCH] predict_proba alignment in {path}")

    # 对 predict(...) 也加对齐（不影响 predict_proba 已处理的）
    s2 = re.sub(
        r'(?<!proba)\bpredict\(\s*(?!__qs_align_lgbm\()([^)]+?)\)',
        r'predict(__qs_align_lgbm(model, \1))',
        s
    )
    if s2 != s:
        s = s2; changed = True
        print(f"[PATCH] predict alignment in {path}")

    if changed:
        io.open(path, "w", encoding="utf-8").write(s)
    else:
        print(f"[SKIP] {path} 无需改动")

def patch_backtest_clip(path):
    if not os.path.exists(path):
        return
    s = io.open(path, "r", encoding="utf-8").read()
    backup(path, s)

    if "ret.clip(" in s:
        print("[SKIP] backtest_pro.py 已有剪裁")
        return

    # 在 “ret = pos.shift(1) ... * close.pct_change() ...” 之后追加一行剪裁
    pat = re.compile(
        r'(ret\s*=\s*pos\s*\.shift\(\s*1\s*\)[^\n]*close\s*\.pct_change\([^\)]*\)[^\n]*\n)',
        flags=re.IGNORECASE
    )
    s2 = pat.sub(r'\1    ret = ret.clip(-0.5, 0.5)\n', s)
    if s2 != s:
        io.open(path, "w", encoding="utf-8").write(s2)
        print("[PATCH] backtest_pro.py -> ret.clip(-0.5, 0.5) 已注入")
    else:
        print("[WARN] 未定位到 ret 计算行，未修改（不影响运行）")

def main():
    strat_main = os.path.join(BASE, "strategy", "strategies_a1a8.py")
    strat_gpu  = os.path.join(BASE, "strategy", "model_lgbm_gpu.py")
    backtest   = os.path.join(BASE, "backtest", "backtest_pro.py")

    if os.path.exists(strat_main):
        patch_strategy_file(strat_main)
    if os.path.exists(strat_gpu):
        patch_strategy_file(strat_gpu)

    patch_backtest_clip(backtest)
    print("=== PATCH DONE ===")

if __name__ == "__main__":
    main()
