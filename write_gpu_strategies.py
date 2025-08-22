# -*- coding: utf-8 -*-
"""
write_gpu_strategies.py — 一次性写入/注册 GPU 策略 + 注入回测解析/日志（机构级）
运行：cd /d D:\quant_system_pro && python write_gpu_strategies.py
"""

import os, io, re, sys, datetime
BASE = r"D:\quant_system_pro"
STRATEGY_DIR = os.path.join(BASE, "strategy")
UTILS_DIR = os.path.join(BASE, "utils")
BACKTEST_DIR = os.path.join(BASE, "backtest")

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def write_file(path, content):
    ensure_dir(os.path.dirname(path))
    # 备份
    if os.path.exists(path):
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        bak = path + f".bak.{ts}"
        try:
            with io.open(path, "r", encoding="utf-8") as f:
                old = f.read()
            with io.open(bak, "w", encoding="utf-8") as f:
                f.write(old)
            print(f"[BACKUP] {path} -> {bak}")
        except Exception as e:
            print(f"[WARN] backup failed for {path}: {e}")
    with io.open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[WRITE] {path}")

def patch_file(path, patcher, desc):
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            s = f.read()
    except FileNotFoundError:
        print(f"[ERR] Missing file to patch: {path}")
        return False
    s2, changed = patcher(s)
    if changed:
        write_file(path, s2)
        print(f"[PATCH] {desc}: applied")
    else:
        print(f"[SKIP] {desc}: already present / no change")
    return changed

# ==========================
# 1) utils/gpu_accel.py
# ==========================
GPU_ACCEL = r'''# -*- coding: utf-8 -*-
"""GPU 加速工具：Torch/XGBoost/LightGBM 自动检测与参数适配（机构级）"""
import os

def _try_import(name):
    try:
        return __import__(name)
    except Exception:
        return None

torch = _try_import('torch')
xgb   = _try_import('xgboost')
lgb   = _try_import('lightgbm')

def has_cuda():
    try:
        return bool(torch and getattr(torch.cuda, 'is_available', lambda: False)())
    except Exception:
        return False

def torch_device():
    return 'cuda' if has_cuda() else 'cpu'

def log_env():
    lines=[]
    if torch:
        try:
            if has_cuda():
                dev = torch.cuda.get_device_name(0)
                cc  = getattr(torch.version,'cuda',None)
                try:
                    torch.set_float32_matmul_precision('high')
                except Exception:
                    pass
                lines.append(f'[GPU] Torch {torch.__version__} | CUDA {cc} | Device {dev} | available=True')
            else:
                lines.append(f'[GPU] Torch {torch.__version__} | CUDA=None | Device=CPU | available=False')
        except Exception as e:
            lines.append(f'[GPU] Torch present but check failed: {e}')
    else:
        lines.append('[GPU] Torch not installed')

    if xgb:
        try:
            ver = getattr(xgb, "__version__", "?")
        except Exception:
            ver = "?"
        lines.append(f'[GPU] XGBoost {ver} (gpu_hist supported)')
    else:
        lines.append('[GPU] XGBoost not installed')

    if lgb:
        lines.append('[GPU] LightGBM available (GPU depends on build)')
    else:
        lines.append('[GPU] LightGBM not installed')

    print("\n".join(lines))

def xgb_params(params=None):
    """给 XGBoost 参数打上 GPU 适配（自动回退 CPU）。"""
    p=dict(params or {})
    if xgb and has_cuda():
        p.setdefault('tree_method','gpu_hist')
        p.setdefault('predictor','gpu_predictor')
    else:
        p.setdefault('tree_method','hist')
    return p

def lgbm_params(params=None):
    """给 LightGBM 参数打上 GPU 适配（若为 GPU 版 LightGBM）。"""
    p=dict(params or {})
    if lgb and has_cuda():
        p.setdefault('device_type','gpu')   # 新版 LightGBM
        p.setdefault('gpu_platform_id',0)
        p.setdefault('gpu_device_id',0)
        p.setdefault('max_bin',255)
    return p
'''

# ==========================
# 2) strategy/model_xgb_gpu.py
# ==========================
MODEL_XGB = r'''# -*- coding: utf-8 -*-
"""XGBoost GPU 策略（分类）——自动用 GPU，无 GPU 时回退 CPU。"""
import numpy as np, pandas as pd
from utils.gpu_accel import xgb_params
try:
    import xgboost as xgb
except Exception:
    xgb=None

def _rsi(s, period=14):
    d = s.diff()
    up = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    dn = (-d).clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    rs = up/(dn+1e-12)
    return 100 - 100/(1+rs)

def _features(df: pd.DataFrame):
    c = pd.to_numeric(df.get('close'), errors='coerce').astype(float)
    v = pd.to_numeric(df.get('volume', 0), errors='coerce').astype(float)
    ret1  = c.pct_change()
    ret5  = c.pct_change(5)
    ema12 = c.ewm(span=12, min_periods=12).mean()
    ema26 = c.ewm(span=26, min_periods=26).mean()
    macd  = ema12 - ema26
    hist  = macd - macd.ewm(span=9, min_periods=9).mean()
    rsi14 = _rsi(c,14)
    volz  = (v.rolling(20,min_periods=20).mean()-v.rolling(60,min_periods=60).mean())/(v.rolling(60,min_periods=60).std()+1e-9)
    X = pd.DataFrame({
        'ret1':ret1,'ret5':ret5,'ema12':ema12,'ema26':ema26,
        'macd':macd,'hist':hist,'rsi14':rsi14,'volz':volz
    }, index=df.index).replace([np.inf,-np.inf], np.nan).dropna()
    # 目标：下一根收益是否为正（0/1）
    y = (c.pct_change().shift(-1).reindex(X.index)>0).astype(int)
    return X, y

def strat_xgb(df: 'pd.DataFrame', lookback:int=3000, train_ratio:float=0.7,
              n_estimators:int=400, max_depth:int=6, learning_rate:float=0.05,
              subsample:float=0.8, colsample_bytree:float=0.8, threshold:float=0.5):
    if xgb is None:
        # 没安装 xgboost → 返回全 0（空仓）
        return pd.Series(0, index=df.index, dtype=int)
    X, y = _features(df)
    if len(X) < max(300, int(lookback*0.6)):
        return pd.Series(0, index=df.index, dtype=int)
    X = X.iloc[-lookback:]; y = y.loc[X.index]
    ntr = max(50, int(len(X)*train_ratio))
    Xtr, Xte = X.iloc[:ntr], X.iloc[ntr:]
    ytr, yte = y.iloc[:ntr], y.iloc[ntr:]
    if len(Xte)==0:
        return pd.Series(0, index=df.index, dtype=int)
    params = xgb_params({
        'n_estimators': int(n_estimators),
        'max_depth'   : int(max_depth),
        'learning_rate': float(learning_rate),
        'subsample'   : float(subsample),
        'colsample_bytree': float(colsample_bytree),
        'random_state': 42,
        'n_jobs'      : 0,
        'verbosity'   : 0,
        'objective'   : 'binary:logistic'
    })
    model = xgb.XGBClassifier(**params)
    model.fit(Xtr, ytr)
    proba = pd.Series(model.predict_proba(Xte)[:,1], index=Xte.index)
    sig   = (proba > float(threshold)).astype(int)*2-1  # 1/-1
    pos   = pd.Series(0, index=df.index, dtype=int)
    pos.loc[sig.index] = sig.values
    return pos.shift(1).fillna(0).astype(int)
'''

# ==========================
# 3) strategy/model_lgbm_gpu.py
# ==========================
MODEL_LGBM = r'''# -*- coding: utf-8 -*-
"""LightGBM GPU 策略（分类）——device_type=gpu（若为 GPU 版 LightGBM）。"""
import numpy as np, pandas as pd
from utils.gpu_accel import lgbm_params
try:
    import lightgbm as lgb
except Exception:
    lgb=None

def _rsi(s, period=14):
    d = s.diff()
    up = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    dn = (-d).clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    rs = up/(dn+1e-12)
    return 100 - 100/(1+rs)

def _features(df: pd.DataFrame):
    c = pd.to_numeric(df.get('close'), errors='coerce').astype(float)
    v = pd.to_numeric(df.get('volume', 0), errors='coerce').astype(float)
    ret1  = c.pct_change()
    ret10 = c.pct_change(10)
    ema20 = c.ewm(span=20, min_periods=20).mean()
    ema60 = c.ewm(span=60, min_periods=60).mean()
    gap   = (ema20-ema60)/ema60
    rsi14 = _rsi(c,14)
    volr  = v.pct_change().rolling(10,min_periods=10).mean()
    X = pd.DataFrame({'ret1':ret1,'ret10':ret10,'gap':gap,'rsi14':rsi14,'volr':volr}, index=df.index).replace([np.inf,-np.inf],np.nan).dropna()
    y = (c.pct_change().shift(-1).reindex(X.index)>0).astype(int)
    return X,y

def strat_lgbm(df: 'pd.DataFrame', lookback:int=3000, train_ratio:float=0.7,
               num_leaves:int=64, n_estimators:int=800, learning_rate:float=0.05,
               subsample:float=0.8, colsample_bytree:float=0.8, threshold:float=0.5):
    if lgb is None:
        return pd.Series(0, index=df.index, dtype=int)
    X,y = _features(df)
    if len(X) < max(300, int(lookback*0.6)):
        return pd.Series(0, index=df.index, dtype=int)
    X = X.iloc[-lookback:]; y = y.loc[X.index]
    ntr = max(50, int(len(X)*train_ratio))
    Xtr, Xte = X.iloc[:ntr], X.iloc[ntr:]
    ytr, yte = y.iloc[:ntr], y.iloc[ntr:]
    if len(Xte)==0:
        return pd.Series(0, index=df.index, dtype=int)
    params = lgbm_params({
        'num_leaves'     : int(num_leaves),
        'n_estimators'   : int(n_estimators),
        'learning_rate'  : float(learning_rate),
        'subsample'      : float(subsample),
        'colsample_bytree': float(colsample_bytree),
        'random_state'   : 42,
        'n_jobs'         : -1
    })
    model = lgb.LGBMClassifier(**params)
    model.fit(Xtr, ytr)
    proba = pd.Series(model.predict_proba(Xte)[:,1], index=Xte.index)
    sig   = (proba > float(threshold)).astype(int)*2-1
    pos   = pd.Series(0, index=df.index, dtype=int)
    pos.loc[sig.index] = sig.values
    return pos.shift(1).fillna(0).astype(int)
'''

# ==========================
# 4) strategy/model_lstm_gpu.py
# ==========================
MODEL_LSTM = r'''# -*- coding: utf-8 -*-
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
'''

# ==========================
# 5) 注册到 strategies_a1a8.py
# ==========================
def patch_register_gpu_strats(s: str):
    MARK = "auto-registered GPU strategies (do not edit)"
    if MARK in s:
        return s, False
    patch = r'''
# === auto-registered GPU strategies (do not edit) ===
try:
    from .model_xgb_gpu import strat_xgb as strat_xgb_gpu
    from .model_lgbm_gpu import strat_lgbm as strat_lgbm_gpu
    from .model_lstm_gpu import strat_lstm as strat_lstm_gpu
    globals().update({
        'strat_xgb_gpu': strat_xgb_gpu,
        'strat_lgbm_gpu': strat_lgbm_gpu,
        'strat_lstm_gpu': strat_lstm_gpu,
    })
except Exception as _e:
    pass
'''
    return s + patch, True

# ==========================
# 6) 给 backtest/backtest_pro.py 注入：GPU日志 + 策略解析器
# ==========================
def patch_backtest_gpu_log_and_resolver(s: str):
    changed = False
    # 注入 log_env() 调用（进入 main 即打印）
    if "log_env()" not in s and re.search(r"def\s+main\s*\(", s):
        s = re.sub(r"(?m)^def\s+main\s*\([^)]*\)\s*:\s*\n",
                   "def main():\n    from utils.gpu_accel import log_env\n    log_env()\n",
                   s, count=1)
        changed = True
    # 注入解析器
    if "_resolve_fn(" not in s:
        s = s.replace(
            "import numpy as np",
            "import numpy as np\n\n# === auto add: strategy resolver\nfrom importlib import import_module\n\ndef _resolve_fn(strat_key):\n"
            "    try:\n"
            "        S = import_module('strategy.strategies_a1a8')\n"
            "    except Exception as _e:\n"
            "        raise\n"
            "    if hasattr(S, strat_key):\n"
            "        return getattr(S, strat_key)\n"
            "    for k in ('STRATEGIES','STRATEGY_FUNCS','STRAT_TABLE','REGISTRY'):\n"
            "        if hasattr(S,k) and strat_key in getattr(S,k):\n"
            "            return getattr(S,k)[strat_key]\n"
            "    raise KeyError(f'Unknown strategy: {strat_key}')\n"
        )
        s = re.sub(r"pos\s*=\s*fn\(", "fn = _resolve_fn(strat_key); pos = fn(", s, count=1)
        changed = True
    return s, changed

def main():
    print("=== write_gpu_strategies.py: START ===")
    ensure_dir(STRATEGY_DIR); ensure_dir(UTILS_DIR); ensure_dir(BACKTEST_DIR)

    # 1) 写入/覆盖工具与策略
    write_file(os.path.join(UTILS_DIR, "gpu_accel.py"), GPU_ACCEL)
    write_file(os.path.join(STRATEGY_DIR, "model_xgb_gpu.py"), MODEL_XGB)
    write_file(os.path.join(STRATEGY_DIR, "model_lgbm_gpu.py"), MODEL_LGBM)
    write_file(os.path.join(STRATEGY_DIR, "model_lstm_gpu.py"), MODEL_LSTM)

    # 2) 注册 GPU 策略到 strategies_a1a8.py
    strat_a1a8 = os.path.join(STRATEGY_DIR, "strategies_a1a8.py")
    if not os.path.exists(strat_a1a8):
        print(f"[ERR] 未找到 {strat_a1a8}，请确认路径。")
    else:
        patch_file(strat_a1a8, patch_register_gpu_strats, "register GPU strategies")

    # 3) 给 backtest_pro 注入 GPU 日志+解析器
    bt_pro = os.path.join(BACKTEST_DIR, "backtest_pro.py")
    if not os.path.exists(bt_pro):
        print(f"[ERR] 未找到 {bt_pro}，请确认路径。")
    else:
        patch_file(bt_pro, patch_backtest_gpu_log_and_resolver, "backtest_pro gpu_log + resolver")

    print("=== write_gpu_strategies.py: DONE ===")
    print("\n后续运行：")
    print("  set PYTHONPATH=D:\\quant_system_pro")
    print("  python -m backtest.backtest_pro --db D:\\quant_system_v2\\data\\market_data.db --days 365 --topk 40 --outdir results")
    print("\n回测启动时会输出 [GPU] 开头的环境信息；包含这些新策略：strat_xgb_gpu / strat_lgbm_gpu / strat_lstm_gpu")

if __name__ == "__main__":
    main()
