# -*- coding: utf-8 -*-
import numpy as np, pandas as pd

# ---- helpers ----
def _toi(x):
    try:
        return int(round(float(x)))
    except Exception:
        try:
            return int(x)
        except Exception:
            return 0

def _rsi(c, n=14):
    c = pd.to_numeric(c, errors="coerce")
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1/_toi(n), adjust=False).mean()
    dn = (-d).clip(lower=0).ewm(alpha=1/_toi(n), adjust=False).mean()
    rs = up/(dn+1e-12)
    return 100 - 100/(1+rs)

def _atr(df, n=14):
    c = pd.to_numeric(df["close"], errors="coerce")
    h = pd.to_numeric(df["high"],  errors="coerce")
    l = pd.to_numeric(df["low"],   errors="coerce")
    tr = (h-l).combine((h-c.shift()).abs(), max).combine((l-c.shift()).abs(), max)
    return tr.rolling(_toi(n), min_periods=_toi(n)).mean()

# ---- A1: 布林带 ----
def strat_bbands(df, period=20, n=2.0):
    c = pd.to_numeric(df["close"], errors="coerce")
    period = _toi(period); n = float(n)
    ma  = c.rolling(period, min_periods=period).mean()
    std = c.rolling(period, min_periods=period).std(ddof=0)
    lower = ma - n*std
    pos = (c < lower).astype(float)
    pos = pos.where(c <= ma, 0.0)
    return pos

# ---- A2: 均线交叉 ----
def strat_ma_cross(df, fast=10, slow=50):
    c = pd.to_numeric(df["close"], errors="coerce")
    f = _toi(fast); s = _toi(slow)
    if f >= s: s = f + 1
    ma_f = c.rolling(f, min_periods=f).mean()
    ma_s = c.rolling(s, min_periods=s).mean()
    return (ma_f > ma_s).astype(float)

# ---- A3: RSI 反转 ----
def strat_rsi_rev(df, period=14, low=30, high=70):
    c = pd.to_numeric(df["close"], errors="coerce")
    r = _rsi(c, period)
    sig_long  = (r < float(low)).astype(float)
    sig_close = (r > float(high)).astype(float)
    pos = sig_long.copy()
    pos[sig_close > 0] = 0.0
    return pos

# ---- A4: ATR 突破 ----
def strat_atr_break(df, atr_n=14, k=1.5):
    c = pd.to_numeric(df["close"], errors="coerce")
    atr = _atr(df, atr_n)
    entry = c.shift(1) + float(k)*atr.shift(1)
    return (c > entry).astype(float)

# ---- 机器学习通用特征 ----
def _ml_feats(c, lookback):
    c = pd.to_numeric(c, errors="coerce").astype(float)
    L = _toi(lookback)
    X = []; y = []
    for i in range(L, len(c)-1):
        base = c.iloc[i-1]
        feat = (c.iloc[i-L:i]/base - 1.0).values
        X.append(feat)
        y.append(1 if c.iloc[i+1] > c.iloc[i] else 0)
    return np.array(X), np.array(y), len(c)-1, L

# ---- A5: LightGBM ----
def strat_lgbm(df, lookback=20, n_estimators=200, num_leaves=31, lr=0.05, threshold=0.5):
    try:
        import lightgbm as lgb
    except Exception:
        return pd.Series(0.0, index=df.index)
    c = df["close"]
    X, y, last_idx, L = _ml_feats(c, lookback)
    if len(y) < 50:
        return pd.Series(0.0, index=df.index)
    tr = int(len(X)*0.8)
    params = dict(
        n_estimators=_toi(n_estimators),
        num_leaves=_toi(num_leaves),
        learning_rate=float(lr),
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
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

# ---- A6: 随机森林 ----
def strat_rf(df, lookback=24, n_estimators=300, max_depth=6):
    try:
        from sklearn.ensemble import RandomForestClassifier
    except Exception:
        return pd.Series(0.0, index=df.index)
    c = df["close"]
    X, y, last_idx, L = _ml_feats(c, lookback)
    if len(y) < 50:
        return pd.Series(0.0, index=df.index)
    tr = int(len(X)*0.8)
    rf = RandomForestClassifier(
        n_estimators=_toi(n_estimators),
        max_depth=_toi(max_depth),
        n_jobs=-1,
        random_state=42
    )
    rf.fit(X[:tr], y[:tr])
    proba = rf.predict_proba(X)[:,1]
    sig = (proba > 0.5).astype(float)
    out = pd.Series(0.0, index=df.index)
    out.iloc[L:L+len(sig)] = sig
    return out

# ---- A7: LSTM（可选CUDA） ----
def strat_lstm(df, lookback=30, hidden=32, epochs=2):
    try:
        import torch, torch.nn as nn
    except Exception:
        return pd.Series(0.0, index=df.index)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    c = pd.to_numeric(df["close"], errors="coerce").astype(float)
    L = _toi(lookback)
    X = []; y = []
    for i in range(L, len(c)-1):
        seq = (c.iloc[i-L:i]/c.iloc[i-1]-1.0).values.astype(np.float32)
        X.append(seq[:,None])
        y.append([1.0 if c.iloc[i+1]>c.iloc[i] else 0.0])
    if len(y) < 50:
        return pd.Series(0.0, index=df.index)
    X = torch.tensor(np.array(X), dtype=torch.float32).to(device)
    y = torch.tensor(np.array(y), dtype=torch.float32).to(device)

    class M(nn.Module):
        def __init__(self,h=32):
            super().__init__()
            self.l = nn.LSTM(1,h,batch_first=True)
            self.o = nn.Linear(h,1)
        def forward(self,x):
            _,(h,_) = self.l(x)
            return torch.sigmoid(self.o(h[-1]))

    m = M(_toi(hidden)).to(device)
    opt = torch.optim.Adam(m.parameters(), lr=1e-3)
    loss = nn.BCELoss()
    for ep in range(_toi(epochs)):
        m.train(); opt.zero_grad()
        p = m(X)
        l = loss(p, y)
        l.backward(); opt.step()

    m.eval()
    with torch.no_grad():
        p = m(X).detach().cpu().numpy().ravel()
    sig = (p > 0.5).astype(float)
    out = pd.Series(0.0, index=df.index)
    out.iloc[L:L+len(sig)] = sig
    return out

# ---- A8: RSI+ATR 组合 ----
def strat_rsi_atr(df, rsi_n=14, rsi_low=25, atr_n=14, k=1.2):
    c = pd.to_numeric(df["close"], errors="coerce")
    rsi = _rsi(c, rsi_n)
    atr = _atr(df, atr_n)
    entry = c.shift(1) - float(k)*atr.shift(1)
    return ((rsi < float(rsi_low)) & (c < entry)).astype(float)

# ---- 统一导出 ----
STRATS = {
    "A1": ("布林带", strat_bbands),
    "A2": ("均线交叉", strat_ma_cross),
    "A3": ("RSI反转", strat_rsi_rev),
    "A4": ("ATR突破", strat_atr_break),
    "A5": ("LightGBM", strat_lgbm),
    "A6": ("随机森林", strat_rf),
    "A7": ("LSTM", strat_lstm),
    "A8": ("RSI+ATR", strat_rsi_atr),
}

__all__ = ["STRATS", "strat_bbands", "strat_ma_cross", "strat_rsi_rev", "strat_atr_break",
           "strat_lgbm", "strat_rf", "strat_lstm", "strat_rsi_atr"]
