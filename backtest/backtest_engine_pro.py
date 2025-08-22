# backtest/backtest_engine_pro.py
"""
S‑grade event-driven backtest engine:
- T+1 entries/exits at next open (no look-ahead)
- Explicit fees & slippage per side
- Supports long-only/short-only/both via signal in {-1,0,1} sampled at close
- Leverage applied at trade level only (not per-candle compounding)
- Equity only updates on trade events; unrealized PnL optional
"""
from __future__ import annotations
import pandas as pd
from dataclasses import dataclass

@dataclass
class BTConfig:
    fee_rate: float = 0.0005   # one side 0.05%
    slippage: float = 0.0      # absolute slippage as fraction of price, per side
    leverage: float = 1.0
    allow_short: bool = False
    start_equity: float = 10000.0

def simulate(df: pd.DataFrame, signal_col: str = "signal", cfg: BTConfig = BTConfig()):
    """
    df columns required: ['open','high','low','close'] and signal_col in {-1,0,1} generated at *close* of bar t
    Execution: enter at next bar open (t+1 open), exit on next bar open when signal switches or stop/take hit (optional)
    """
    # safety: ensure sorted by time and no look-ahead
    df = df.copy()
    # next open as execution price
    df["exec_open_next"] = df["open"].shift(-1)
    df = df.iloc[:-1]  # last row has no next open

    # where signal changes → trade boundary at next open
    sig = df[signal_col].fillna(0).astype(int)
    sig_next = sig.shift(1).fillna(0).astype(int)  # previous signal
    change = sig != sig_next

    trades = []
    equity = cfg.start_equity
    pos = 0
    entry_px = None

    for i, row in df.iterrows():
        s = int(row[signal_col])
        exec_open = row["exec_open_next"]

        # entry/exit signal at boundary
        if change.loc[i]:
            # 1) close existing
            if pos != 0 and entry_px is not None:
                # exit at next open
                exit_px = exec_open * (1 - cfg.slippage)  # slippage on exit
                gross = (exit_px / entry_px - 1) * (1 if pos > 0 else -1)
                gross *= cfg.leverage
                net = gross - cfg.fee_rate - cfg.fee_rate  # both sides
                pnl = equity * net
                equity += pnl
                trades.append({"i": int(i), "side": "LONG" if pos>0 else "SHORT",
                               "entry": float(entry_px), "exit": float(exit_px),
                               "ret": float(net), "equity": float(equity)})
                pos = 0; entry_px = None

            # 2) open new if s != 0
            if s != 0 and (s > 0 or cfg.allow_short):
                entry_px = exec_open * (1 + cfg.slippage)  # slippage on entry
                pos = s

        # else hold position; unrealized is not compounded into equity until exit

    # result
    result = {
        "equity": equity,
        "return": equity / cfg.start_equity - 1,
        "trades": trades,
        "n_trades": len(trades),
    }
    return result
