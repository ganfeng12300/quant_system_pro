# -*- coding: utf-8 -*-
# 注入式引导器（不改 backtest_pro.py）：
# 1) 打印 FEATURES/COSTS/RISK 回显（便于核验开关与口径）
# 2) best_combo.csv 原子落盘（防半截）
# 3) 进程退出时自动跑 validators（SPA/PBO/DS），受 QS_ENABLE_VALIDATORS 控制

from __future__ import annotations
import os, sys, re, atexit, tempfile, importlib, runpy

def _env(k, d=None):
    v = os.getenv(k)
    return v if v is not None else d

def _print_features():
    ENABLE_GA  = _env('QS_ENABLE_GA','0') == '1'
    ENABLE_VAL = _env('QS_ENABLE_VALIDATORS','0') == '1'
    print(f'[FEATURES] GA={ENABLE_GA} VALIDATORS={ENABLE_VAL}', flush=True)

    print('[COSTS] taker=', _env('QS_TAKER_FEE','0.0005'),
          ' slippage=', _env('QS_SLIPPAGE','0.0003'),
          ' funding_on=', _env('QS_FUNDING_ON','1'), flush=True)

    print('[RISK ] per_trade=', _env('QS_RISK_PER_TRADE','0.01'),
          ' daily_stop=', _env('QS_MAX_DAILY_LOSS','0.05'), flush=True)

def _patch_pandas_atomic():
    try:
        import pandas as pd
    except Exception:
        return
    _orig = pd.DataFrame.to_csv
    def _atomic_to_csv(self, path_or_buf=None, *args, **kwargs):
        p = path_or_buf
        is_path = isinstance(p, (str, bytes, os.PathLike))
        if is_path:
            pstr = os.fspath(p)
            low = pstr.lower()
            if low.endswith('best_combo.csv') or re.search(r'(^|[\\/])best_combo\.csv$', low):
                d = os.path.dirname(pstr) or '.'
                fd, tmp = tempfile.mkstemp(dir=d, prefix='best_combo.', suffix='.tmp')
                os.close(fd)
                _orig(self, tmp, *args, **kwargs)
                os.replace(tmp, pstr)
                print('[ATOMIC] best_combo.csv saved atomically.', flush=True)
                return
        return _orig(self, path_or_buf, *args, **kwargs)
    pd.DataFrame.to_csv = _atomic_to_csv

def _register_validators():
    if _env('QS_ENABLE_VALIDATORS','0') != '1':
        return
    def _run_validators():
        try:
            print('[VALIDATORS] SPA/PBO/DS running...', flush=True)
            try:
                mod = importlib.import_module('backtest.stats_validators')
                if hasattr(mod, 'main'):
                    mod.main(results_dir=_env('QS_RESULTS_DIR','results'))
                else:
                    sav = sys.argv[:]
                    try:
                        sys.argv = ['stats_validators.py','--results-dir', _env('QS_RESULTS_DIR','results')]
                        runpy.run_path(os.path.join('backtest','stats_validators.py'), run_name='__main__')
                    finally:
                        sys.argv = sav
            finally:
                print('[VALIDATORS] done.', flush=True)
        except Exception as e:
            print(f'[WARN] validators skipped: {e}', flush=True)
    atexit.register(_run_validators)

def main():
    if len(sys.argv) < 2:
        print('Usage: inject_and_run.py <target_script.py> [args...]', flush=True)
        sys.exit(2)
    target = sys.argv[1]
    fwd    = sys.argv[2:]
    _print_features()
    _patch_pandas_atomic()
    _register_validators()
    sys.argv = [os.path.basename(target)] + fwd
    runpy.run_path(target, run_name='__main__')

if __name__ == '__main__':
    main()
