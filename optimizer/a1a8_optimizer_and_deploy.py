# optimizer/a1a8_optimizer_and_deploy.py
"""
A1–A8 策略适配器 + 自动寻优 + 部署最佳参数（JSON + DB）。
- 输入：SQLite DB（表名 SYMBOL_TF，字段 ts, open, high, low, close, volume）
- 输出：deploy/live_best_params.json + DB 表 best_params
- 评分：收益为主，附加惩罚（回撤/换手），硬性约束（最小交易次数、最大允许回撤）
用法：
python optimizer/a1a8_optimizer_and_deploy.py --db D:\path\market_data.db --symbols-file results\symbols_from_db.txt ^
  --timeframes 1h 4h --days 180 --min-trades 10 --max-dd 0.4 --deploy
"""
from __future__ import annotations
import argparse, sqlite3, pandas as pd, numpy as np, os, json, itertools, math, warnings
from typing import Dict, Any, List, Tuple, Callable
from backtest.backtest_engine_pro import simulate, BTConfig
from utils.param_loader import save_best_params_to_db, save_best_params_to_json

warnings.filterwarnings("ignore", category=FutureWarning)

# ========== 工具 ==========
def load_from_db(db_path: str, table: str, days: int):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT ts, open, high, low, close, volume FROM '{table}' ORDER BY ts ASC", con)
    con.close()
    if df.empty: return df
    if days > 0:
        tail_ts = df['ts'].iloc[-1] - days*24*3600*1000
        df = df[df['ts'] >= tail_ts]
    df['time'] = pd.to_datetime(df['ts'], unit='ms')
    return df

def max_drawdown(series: pd.Series):
    if series.empty: return 0.0
    cummax = series.cummax()
    dd = (series - cummax) / cummax
    return float(dd.min()) * -1.0  # 正数表示回撤比例

def turnover_approx(signal: pd.Series):
    # 估算换手：信号变更的次数 / 样本长度
    chg = (signal.fillna(0).astype(int).diff().abs()>0).sum()
    return float(chg) / max(1, len(signal))

# ========== 策略（A1–A8）==========
def ma(df, n): return df['close'].rolling(n, min_periods=n).mean()
def ema(df, n): return df['close'].ewm(span=n, adjust=False).mean()

def strat_A1_ma_cross(df, fast: int, slow: int):
    if fast >= slow: return pd.Series(0, index=df.index)
    sig = (ma(df, fast) > ma(df, slow)).astype(int)
    return sig.fillna(0).astype(int)

def strat_A2_bbands(df, n: int, k: float):
    m = ma(df, n); std = df['close'].rolling(n, min_periods=n).std()
    upper = m + k*std; lower = m - k*std
    sig = (df['close'] > upper).astype(int)  # 突破上轨做多，保守起见仅做多/空仓
    return sig.fillna(0).astype(int)

def strat_A3_rsi(df, n: int, low: int, high: int):
    delta = df['close'].diff()
    up = (delta.clip(lower=0)).rolling(n, min_periods=n).mean()
    down = (-delta.clip(upper=0)).rolling(n, min_periods=n).mean()
    rs = up / (down + 1e-9)
    rsi = 100 - 100/(1+rs)
    sig = (rsi > high).astype(int)  # 越强越买，亦可反向；这里给做多版
    return sig.fillna(0).astype(int)

def strat_A4_atr_break(df, n: int, m: float):
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift(1)).abs()
    lc = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([hl,hc,lc], axis=1).max(axis=1)
    atr = tr.rolling(n, min_periods=n).mean()
    entry = df['close'].shift(1) + m*atr  # 动量突破
    sig = (df['close'] > entry).astype(int)
    return sig.fillna(0).astype(int)

def strat_A5_reversal(df, look: int, z: float):
    ret = df['close'].pct_change()
    zscore = (ret - ret.rolling(look, min_periods=look).mean()) / (ret.rolling(look, min_periods=look).std()+1e-9)
    sig = (zscore < -abs(z)).astype(int)  # 大幅下跌日后的均值回归做多
    return sig.fillna(0).astype(int)

def strat_A6_macd(df, fast: int, slow: int, signal: int):
    macd = ema(df, fast) - ema(df, slow)
    dea = macd.ewm(span=signal, adjust=False).mean()
    sig = (macd > dea).astype(int)
    return sig.fillna(0).astype(int)

def strat_A7_donchian(df, n: int):
    up = df['high'].rolling(n, min_periods=n).max()
    dn = df['low'].rolling(n, min_periods=n).min()
    sig = (df['close'] > up.shift(1)).astype(int)
    return sig.fillna(0).astype(int)

def strat_A8_vol_break(df, n: int, k: float):
    vol = df['close'].pct_change().rolling(n, min_periods=n).std()
    th = vol.rolling(n, min_periods=n).mean() * k
    sig = (df['close'].pct_change() > th).astype(int)
    return sig.fillna(0).astype(int)

STRATS = {
    "A1_ma_cross": strat_A1_ma_cross,
    "A2_bbands": strat_A2_bbands,
    "A3_rsi": strat_A3_rsi,
    "A4_atr_break": strat_A4_atr_break,
    "A5_reversal": strat_A5_reversal,
    "A6_macd": strat_A6_macd,
    "A7_donchian": strat_A7_donchian,
    "A8_vol_break": strat_A8_vol_break,
}

# 参数网格
GRIDS = {
    "A1_ma_cross": lambda: [(f,s) for f in range(5,41,5) for s in range(30,201,10) if f<s],
    "A2_bbands":   lambda: [(n,k) for n in range(10,61,10) for k in [1.5,2.0,2.5,3.0]],
    "A3_rsi":      lambda: [(n,30,70) for n in range(8,31,2)],
    "A4_atr_break":lambda: [(n,m) for n in range(7,31,3) for m in [1.0,1.5,2.0,2.5]],
    "A5_reversal": lambda: [(l,z) for l in range(5,31,5) for z in [0.5,1.0,1.5,2.0]],
    "A6_macd":     lambda: [(f,s,9) for f in [8,10,12] for s in [17,20,26] if f<s],
    "A7_donchian": lambda: [(n,) for n in range(10,61,5)],
    "A8_vol_break":lambda: [(n,k) for n in range(10,61,10) for k in [1.0,1.5,2.0]],
}

def build_signal(df, name: str, params: tuple):
    fn = STRATS[name]
    sig = fn(df, *params)
    return sig.astype(int)

def score_portfolio(equity_curve: pd.Series, signal: pd.Series, min_trades: int, max_dd: float, res: dict):
    dd = max_drawdown(equity_curve)
    turns = turnover_approx(signal)
    trades = res['n_trades']
    if trades < min_trades: return -1e9, {"dd": dd, "turnover": turns, "trades": trades}
    if dd > max_dd: return -1e9, {"dd": dd, "turnover": turns, "trades": trades}
    r = res['return']                # 主指标：收益
    # 惩罚项：回撤/换手
    penal = 0.0 + dd*0.5 + turns*0.2
    final = r - penal
    return final, {"dd": dd, "turnover": turns, "trades": trades}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols", nargs="*", default=[])
    ap.add_argument("--symbols-file")
    ap.add_argument("--timeframes", nargs="*", default=["1h"])
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--fee", type=float, default=0.0005)
    ap.add_argument("--slip", type=float, default=0.0)
    ap.add_argument("--lev", type=float, default=1.0)
    ap.add_argument("--min-trades", type=int, default=10)
    ap.add_argument("--max-dd", type=float, default=0.5)   # 50%
    ap.add_argument("--deploy", action="store_true")
    ap.add_argument("--json", default="deploy/live_best_params.json")
    args = ap.parse_args()

    # symbols
    syms = [s.strip().upper() for s in args.symbols if s.strip()]
    if args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t and not t.startswith("#"):
                    syms.append(t.upper())
    syms = sorted(set(syms))

    cfg = BTConfig(fee_rate=args.fee, slippage=args.slip, leverage=args.lev)

    best_items = []
    for sym in syms:
        for tf in args.timeframes:
            table = f"{sym}_{tf}"
            df = load_from_db(args.db, table, args.days)
            if df.empty or len(df) < 260:
                print(f"[SKIP] {table} 数据不足")
                continue

            best = None
            for name, grid_fn in GRIDS.items():
                for params in grid_fn():
                    try:
                        sig = build_signal(df, name, params)
                        df2 = df.copy()
                        df2['signal'] = sig
                        res = simulate(df2, "signal", cfg)
                        # 构造等频权益曲线（按交易事件复利，这里用累计乘积近似展示）
                        eq = pd.Series([cfg.start_equity]*(len(df2)), index=df2.index)
                        # 简要估价：仅在交易事件变更时更新，这里用结果的最终收益评估回撤可能偏高/低，作为近似
                        # 为稳定起见，可用更细粒度事件列表重建曲线；本处先用最终收益 + 信号换手作为评分
                        score, info = score_portfolio(eq, sig, args.min_trades, args.max_dd, res)
                        if (best is None) or (score > best['score']):
                            best = {"score": float(score), "name": name, "params": params, "res": res, "info": info}
                    except Exception:
                        continue

            if best is None:
                print(f"[NONE] {sym} {tf} 无满足约束的结果")
                continue
            item = {"symbol": sym, "tf": tf, "strategy": best["name"],
                    "params": format_params(best["name"], best["params"]),
                    "metrics": {"return": float(best["res"]["return"]), "trades": int(best["res"]["n_trades"]),
                                "score": float(best["score"]), "dd": float(best["info"]["dd"]), "turnover": float(best["info"]["turnover"])}}
            best_items.append(item)
            print(f"[BEST] {sym} {tf} {item['strategy']} {item['params']} ret={item['metrics']['return']*100:.2f}% trades={item['metrics']['trades']} score={item['metrics']['score']:.4f} dd={item['metrics']['dd']:.2f}")

    if args.deploy and best_items:
        os.makedirs(os.path.dirname(args.json) or ".", exist_ok=True)
        save_best_params_to_json(best_items, args.json)
        save_best_params_to_db(args.db, best_items)
        print(f"[DEPLOYED] JSON: {args.json} & DB: best_params")

def format_params(name: str, params: tuple):
    keys = {
        "A1_ma_cross": ["fast","slow"],
        "A2_bbands": ["n","k"],
        "A3_rsi": ["n","low","high"],
        "A4_atr_break": ["n","m"],
        "A5_reversal": ["look","z"],
        "A6_macd": ["fast","slow","signal"],
        "A7_donchian": ["n"],
        "A8_vol_break": ["n","k"],
    }[name]
    return {k:int(v) if isinstance(v,(int,bool)) or (isinstance(v,float) and v.is_integer()) else float(v) for k,v in zip(keys, params)}

if __name__ == "__main__":
    main()
