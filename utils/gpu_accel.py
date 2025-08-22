# -*- coding: utf-8 -*-
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
