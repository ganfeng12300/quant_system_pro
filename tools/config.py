# -*- coding: utf-8 -*-
import os, json

def get_db_path(default=r"D:/quant_system_v2/data/market_data.db"):
    return os.environ.get("QS_DB", default)

def get_results_db(default=r"D:/quant_system_pro/data/backtest_results.db"):
    return os.environ.get("QS_RESULTS_DB", default)

def load_keys(path=r"D:/quant_system_v2/data/keys.json"):
    if os.path.exists(path):
        try:
            with open(path,"r",encoding="utf-8") as f: j=json.load(f)
            return {
                "api_key": j.get("BINANCE_API_KEY",""),
                "api_secret": j.get("BINANCE_API_SECRET",""),
                "testnet": bool(j.get("BINANCE_TESTNET", True)),
            }
        except: pass
    return {
        "api_key": os.environ.get("BINANCE_API_KEY",""),
        "api_secret": os.environ.get("BINANCE_API_SECRET",""),
        "testnet": os.environ.get("BINANCE_TESTNET","1")=="1",
    }

def runtime_params():
    return {
        # 风控
        "risk_per_trade": float(os.environ.get("QS_RISK_PER_TRADE","0.01")),
        "max_daily_loss": float(os.environ.get("QS_MAX_DAILY_LOSS","0.05")),
        "leverage": int(os.environ.get("QS_LEVERAGE","5")),
        "min_trades": int(os.environ.get("QS_MIN_TRADES","5")),
        "max_dd_cap": float(os.environ.get("QS_MAX_DD","30")),

        # 成本
        "taker_fee": float(os.environ.get("QS_TAKER_FEE","0.0005")),
        "slippage": float(os.environ.get("QS_SLIPPAGE","0.0003")),
        "funding_on": os.environ.get("QS_FUNDING_ON","1")=="1",

        # 实盘
        "paper": os.environ.get("QS_PAPER","1")=="1",

        # 监控/告警
        "metrics_port": int(os.environ.get("QS_METRICS_PORT","9108")),
        "alert_url": os.environ.get("QS_ALERT_URL",""),
    }
