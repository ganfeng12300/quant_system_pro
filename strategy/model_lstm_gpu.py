# -*- coding: utf-8 -*-
"""LSTM GPU 策略（次日方向分类）——自动用 CUDA；无 Torch/CUDA 则回退 CPU/空仓。"""
import numpy as np, pandas as pd
from utils.gpu_accel import torch, torch_device

def _prep_series(df):
    c = pd.to_numeric(df.get('close'), errors='coerce').astype(float)
    r = c.pct_change().fillna(0.0)
    return r

def strat_lstm(df: 'pd.DataFrame', lookback:int=5000, seq_len:int=32, train_ratio:float=0.7,
               hidden:int=32, num_layers:int=1, epochs:int=3, lr:float=1e-3, threshold:float=0.0):
    if torch is None:
        return pd.Series(0, index=df.index, dtype=int)
    r = _prep_series(df)
    if len(r) < max(seq_len+200, int(lookback*0.6)):
        return pd.Series(0, index=df.index, dtype=int)
    r = r.iloc[-lookback:]
    X = r.values.astype('float32')
    y = (np.roll(X,-1) > threshold).astype('float32')
    y[-1] = y[-2]  # 尾巴对齐

    # 构造序列样本
    xs, ys, idx = [], [], []
    for i in range(seq_len, len(X)):
        xs.append(X[i-seq_len:i])
        ys.append(y[i])
        idx.append(r.index[i])
    xs = np.asarray(xs, dtype='float32'); ys = np.asarray(ys, dtype='float32')

    ntr = max(100, int(len(xs)*train_ratio))
    Xtr, Xte = xs[:ntr], xs[ntr:]
    ytr, yte = ys[:ntr], ys[ntr:]
    id_te    = idx[ntr:]
    if len(Xte)==0:
        return pd.Series(0, index=df.index, dtype=int)

    dev = torch_device()
    import torch.nn as nn
    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.lstm = nn.LSTM(input_size=1, hidden_size=int(hidden), num_layers=int(num_layers), batch_first=True)
            self.head = nn.Sequential(nn.Linear(int(hidden), 1), nn.Sigmoid())
        def forward(self, x):
            h,_ = self.lstm(x)
            out = self.head(h[:,-1,:])
            return out

    model = Net().to(dev)
    optim = torch.optim.Adam(model.parameters(), lr=float(lr))
    lossf = nn.BCELoss()

    def _to(x): 
        import torch as T
        return T.from_numpy(x).unsqueeze(-1).to(dev)

    Xtr_t, ytr_t = _to(Xtr), torch.from_numpy(ytr).view(-1,1).to(dev)
    model.train()
    for _ in range(int(epochs)):
        optim.zero_grad()
        pred = model(Xtr_t)
        loss = lossf(pred, ytr_t)
        loss.backward()
        optim.step()

    model.eval()
    with torch.no_grad():
        proba = model(_to(Xte)).squeeze(1).detach().cpu().numpy()
    sig = (proba>0.5).astype(int)*2-1

    pos = pd.Series(0, index=df.index, dtype=int)
    pos.loc[id_te] = sig
    return pos.shift(1).fillna(0).astype(int)
