# -*- coding: utf-8 -*-
"""
一次性机构级终极补丁：
- 对 LGBM 策略：对齐训练/预测特征列，加入 early stopping，降低噪声
- 对回测：剪裁极端单bar收益，防止数值爆炸导致 score 异常巨大
运行：cd /d D:\quant_system_pro && python patch_institutional_final.py
"""
import io, os, re, datetime

BASE = r"D:\quant_system_pro"
TS = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

def backup(path, content):
    bak = f"{path}.bak.{TS}"
    try:
        io.open(bak, "w", encoding="utf-8").write(content)
        print(f"[BACKUP] {path} -> {bak}")
    except Exception as e:
        print(f"[WARN] 备份失败 {path}: {e}")

APPEND_ALIGN = r'''
# === qs:auto: LightGBM 预测对齐 & 降噪（请勿手改） ===
import pandas as _pd
def __qs_align_lgbm(model, X):
    cols = None
    try:
        cols = getattr(model, "feature_name_", None)
        if cols is None and hasattr(model, "booster_"):
            cols = model.booster_.feature_name()
    except Exception:
        cols = None
    if isinstance(X, _pd.DataFrame):
        return X if cols is None else X.reindex(columns=list(cols), fill_value=0)
    return _pd.DataFrame(X, columns=list(cols)) if cols is not None else _pd.DataFrame(X)
'''

def patch_strategies_file(path):
    if not os.path.exists(path):
        return
    s = io.open(path, "r", encoding="utf-8").read()
    backup(path, s)
    changed = False

    # 追加对齐辅助函数（若不存在）
    if "__qs_align_lgbm" not in s:
        s = s.rstrip() + "\n\n" + APPEND_ALIGN.strip() + "\n"
        changed = True
        print(f"[APPEND] __qs_align_lgbm -> {path}")

    # predict_proba(Xte) -> predict_proba(__qs_align_lgbm(model, Xte))
    s2 = re.sub(r'predict_proba\s*\(\s*Xte\s*\)',
                'predict_proba(__qs_align_lgbm(model, Xte))', s)
    if s2 != s:
        s = s2; changed = True
        print(f"[PATCH] predict_proba 对齐 -> {path}")

    # model.fit(Xtr, ytr) -> 增加 early_stopping 与 eval_set、屏蔽verbose
    fit_pat = re.compile(r'model\s*\.\s*fit\s*\(\s*Xtr\s*,\s*ytr\s*\)')
    s2 = fit_pat.sub('model.fit(Xtr, ytr, eval_set=[(Xte, yte)], '
                     'eval_metric="binary_logloss", early_stopping_rounds=50, verbose=False)', s)
    if s2 != s:
        s = s2; changed = True
        print(f"[PATCH] early_stopping -> {path}")

    if changed:
        io.open(path, "w", encoding="utf-8").write(s)
    else:
        print(f"[SKIP] {path} 无需改动")

def patch_backtest_ret_clip(path):
    if not os.path.exists(path): return
    s = io.open(path, "r", encoding="utf-8").read()
    backup(path, s)
    if "ret = ret.clip(" in s:
        print("[SKIP] 已存在剪裁，无需重复")
        return
    # 在 ret = pos.shift(1)*close.pct_change() 之后插入剪裁
    pat = re.compile(r'(ret\s*=\s*pos\.shift\(1\)\.fillna\(0\.0\)\s*\*\s*close\.pct_change\(\)\.fillna\(0\.0\)\s*)')
    s2 = pat.sub(r'\1\n    # 防极端数据导致数值爆炸\n    ret = ret.clip(-0.5, 0.5)', s)
    if s2 != s:
        io.open(path, "w", encoding="utf-8").write(s2)
        print("[PATCH] backtest_pro.py: 加入 ret.clip(-0.5, 0.5)")
    else:
        print("[WARN] 未匹配到 ret 计算位置，未修改（不影响运行）")

def main():
    # 1) 策略文件补丁（两处：内置 LGBM 与 GPU 版 LGBM，如存在）
    strat_path = os.path.join(BASE, "strategy", "strategies_a1a8.py")
    patch_strategies_file(strat_path)

    lgbm_gpu_path = os.path.join(BASE, "strategy", "model_lgbm_gpu.py")
    if os.path.exists(lgbm_gpu_path):
        patch_strategies_file(lgbm_gpu_path)

    # 2) 回测收益剪裁（防 score 异常巨大）
    backtest_path = os.path.join(BASE, "backtest", "backtest_pro.py")
    patch_backtest_ret_clip(backtest_path)

    print("=== PATCH DONE ===")

if __name__ == "__main__":
    main()
