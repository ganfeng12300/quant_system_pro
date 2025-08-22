# -*- coding: utf-8 -*-
"""LightGBM GPU 策略（分类）。若检测到 GPU 版 LightGBM 自动启用 device_type=gpu。"""
import numpy as np, pandas as pd
from utils.gpu_accel import lgbm_params

def _toi(x):
    try:
        return int(round(float(x)))
    except Exception:
        try:
            return int(x)
        except Exception:
            return 0

def _ml_feats(c, lookback):
    c = pd.to_numeric(c, errors="coerce").astype(float)
    L = _toi(lookback)
    X=[]; y=[]
    for i in range(L, len(c)-1):
        base = c.iloc[i-1]
        feat = (c.iloc[i-L:i]/base - 1.0).values
        X.append(feat); y.append(1 if c.iloc[i+1]>c.iloc[i] else 0)
    return np.array(X), np.array(y), L

def strat_lgbm(df, lookback=20, n_estimators=200, num_leaves=31, lr=0.05, threshold=0.5):
    try:
        import lightgbm as lgb
    except Exception:
        return pd.Series(0.0, index=df.index)
    X, y, L = _ml_feats(df['close'], lookback)
    if len(y) < 50:
        return pd.Series(0.0, index=df.index)
    tr = int(len(X)*0.8)
    params = lgbm_params({
        'n_estimators'   : _toi(n_estimators),
        'num_leaves'     : _toi(num_leaves),
        'learning_rate'  : float(lr),
        'subsample'      : 0.8,
        'colsample_bytree': 0.8,
        'random_state'   : 42,
        'n_jobs'         : -1
    })
    model = lgb.LGBMClassifier(**params)
    try:
        model.set_params(verbosity=-1)
    except Exception:
        pass
    model.fit(
        X[:tr], y[:tr],
        eval_set=[(X[tr:], y[tr:])],
        eval_metric="binary_logloss",
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    proba = model.predict_proba(X)[:,1]
    sig = (proba > float(threshold)).astype(float)
    out = pd.Series(0.0, index=df.index)
    out.iloc[L:L+len(sig)] = sig
    return out

__all__ = ["strat_lgbm"]
