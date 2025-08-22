# live_trading/live_trader_pro.py
"""
实盘执行器（安全默认：dry-run）。
- 读取 best_params（DB 表 best_params 或 JSON deploy/live_best_params.json）
- 依据最佳参数在 DB 的 OHLCV 上生成信号，结合 ticks 最新价来做“随时平仓/开仓”评估
- 默认 dry-run：仅打印/记录交易意图；传 --live 才会用 ccxt 下单（需设置 API_KEY/SECRET）
- 支持 Bitget / Binance（--exchange）

示例：
python live_trading/live_trader_pro.py --db D:\quant_system_v2\data\market_data.db --exchange binance --symbol BTCUSDT --tf 1h --strategy auto --live
"""
from __future__ import annotations
import argparse, time, json, os, sqlite3, pandas as pd
from utils.param_loader import get_best_for
from optimizer.a1a8_optimizer_and_deploy import STRATS  # 复用策略构建
from backtest.backtest_engine_pro import BTConfig, simulate
import ccxt

def load_ohlcv(db_path: str, table: str, tail_bars: int = 1000):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT ts, open, high, low, close, volume FROM '{table}' ORDER BY ts ASC", con)
    con.close()
    if df.empty: return df
    df['time'] = pd.to_datetime(df['ts'], unit='ms')
    if tail_bars > 0 and len(df) > tail_bars:
        df = df.iloc[-tail_bars:].copy()
    return df

def latest_tick(db_path: str, symbol_ccxt: str):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT price, ts FROM ticks WHERE symbol=? ORDER BY ts DESC LIMIT 1", (symbol_ccxt,))
    row = cur.fetchone()
    con.close()
    if not row: return None, None
    return float(row[0]), int(row[1])

def to_ccxt_symbol(sym):
    t = sym.upper()
    return t if "/" in t else (t[:-4] + "/USDT" if t.endswith("USDT") else t)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--exchange", choices=["binance","bitget"], default="binance")
    ap.add_argument("--symbol", required=True, help="BTCUSDT 等")
    ap.add_argument("--tf", required=True, help="如 1h/4h")
    ap.add_argument("--strategy", default="auto", help="auto=用 best_params 里的策略；或指定 A1_ma_cross 等")
    ap.add_argument("--fee", type=float, default=0.0005)
    ap.add_argument("--slip", type=float, default=0.0)
    ap.add_argument("--lev", type=float, default=1.0)
    ap.add_argument("--poll", type=int, default=5, help="秒；轮询参数/快价/新K线")
    ap.add_argument("--live", action="store_true", help="默认 dry-run；加此参数才会通过 ccxt 真下单（需 API）")
    args = ap.parse_args()

    symbol_ccxt = to_ccxt_symbol(args.symbol)

    # 交易所
    ex = ccxt.binance({"enableRateLimit": True}) if args.exchange=="binance" else ccxt.bitget({"enableRateLimit": True})
    if args.live:
        # 需要用户自行配置 API KEY/SECRET
        ex.apiKey = os.getenv("API_KEY","")
        ex.secret = os.getenv("API_SECRET","")
        if not ex.apiKey or not ex.secret:
            print("[ERR] 实盘下单需要设置环境变量 API_KEY/API_SECRET；当前改为 dry-run。")
            args.live = False

    # 参数与策略
    best = get_best_for(args.symbol, args.tf, args.strategy if args.strategy!="auto" else "A1_ma_cross", db_path=args.db)
    if args.strategy == "auto" and best:
        strategy = best["strategy"]
        params = best["params"]
    else:
        strategy = args.strategy if args.strategy!="auto" else "A1_ma_cross"
        params = best["params"] if best else {}

    print(f"[INFO] 使用策略: {strategy} 参数: {params} 交易所: {args.exchange} 符号: {symbol_ccxt}")

    table = f"{args.symbol}_{args.tf}"
    last_pos = 0  # -1/0/1
    last_bar_ts = None

    while True:
        df = load_ohlcv(args.db, table, tail_bars=1500)
        if df.empty or len(df) < 100:
            print("[WAIT] 数据不足，稍后再试")
            time.sleep(args.poll); continue

        # 生成信号
        sig = STRATS[strategy](df, *params.values()) if params else STRATS[strategy](df, *[10,60] if strategy=="A1_ma_cross" else [])
        df2 = df.copy(); df2['signal'] = sig.astype(int)

        # 最新 bar 的 next-open 执行（实盘中可结合轮询对齐）
        cur_bar_ts = int(df2['ts'].iloc[-1])
        cur_sig = int(df2['signal'].iloc[-1])
        tick_price, tick_ts = latest_tick(args.db, symbol_ccxt)

        if last_bar_ts != cur_bar_ts:
            # 新K线到来，评估上根的收盘信号，下一根开盘执行；这里用 tick 价格近似执行价
            if cur_sig != last_pos:
                action = "BUY" if cur_sig>0 else ("SELL" if cur_sig<0 else "FLAT")
                print(f"[TRADE] {action} @tick {tick_price} ts={tick_ts} signal={cur_sig}")
                if args.live and tick_price:
                    try:
                        amt = 0.001  # 示例下单数量；实际请调整或接资金管理
                        if action=="BUY":
                            ex.create_market_buy_order(symbol_ccxt, amt)
                        elif action=="SELL":
                            ex.create_market_sell_order(symbol_ccxt, amt)
                    except Exception as e:
                        print("[ERR] 下单失败：", e)
                last_pos = cur_sig
            last_bar_ts = cur_bar_ts

        # 轮询检查参数是否更新（热更新）
        nb = get_best_for(args.symbol, args.tf, strategy, db_path=args.db)
        if nb and nb["params"] != params:
            print("[PARAM] 发现更优参数，热更新：", nb["params"])
            params = nb["params"]

        time.sleep(args.poll)

if __name__ == "__main__":
    main()
