# -*- coding: utf-8 -*-
import time, threading, requests, pandas as pd, numpy as np
from math import sqrt
from datetime import datetime

# -------- 资金费率 --------
_FUND_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
def fetch_funding_series(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    out=[]; cur=start_ms
    while True:
        r=requests.get(_FUND_URL, params={"symbol":symbol.upper(),"startTime":cur,"endTime":end_ms,"limit":1000}, timeout=15)
        r.raise_for_status()
        arr=r.json()
        if not arr: break
        for it in arr:
            out.append((int(it["fundingTime"]), float(it["fundingRate"])))
        last=int(arr[-1]["fundingTime"])
        if last>=end_ms or len(arr)<1000: break
        cur=last+1
    if not out: return pd.DataFrame(columns=["ts","rate"])
    df=pd.DataFrame(out, columns=["ts","rate"]).drop_duplicates("ts").sort_values("ts")
    return df

def apply_costs(ret_series, pos_series, taker_fee=0.0005, slippage=0.0003,
                funding_df=None, bar_ts=None, impact_bps_series=None):
    ret = ret_series.copy().fillna(0.0)
    pos = pos_series.copy().fillna(0.0)
    # 进出成本（双边）
    turns = pos.diff().abs().fillna(0.0)
    trading_cost = (taker_fee + slippage) * turns
    ret = ret - trading_cost
    # 冲击成本（bps → 小数）
    if impact_bps_series is not None:
        ret = ret - (impact_bps_series.fillna(0.0)/10000.0) * turns
    # 资金费率在结算点计入
    if funding_df is not None and bar_ts is not None and len(funding_df)>0:
        fund_idx = {int(ts):rate for ts,rate in funding_df[["ts","rate"]].itertuples(index=False)}
        add = ret.copy(); add[:] = 0.0
        for i,ts in enumerate(bar_ts):
            r = fund_idx.get(int(ts))
            if r is not None:
                p = pos.iloc[i-1] if i>0 else 0.0
                add.iloc[i] = - r * p
        ret = ret + add
    return ret

# -------- 交易所规则（Binance/OKX/Bitget） --------
def _round_step(x, step):
    if step is None or step==0: return float(x)
    n = round(float(x)/float(step))
    return max(step, n*step)

# Binance
class BinanceRules:
    URL="https://fapi.binance.com/fapi/v1/exchangeInfo"
    def __init__(self, ttl=600):
        self.ttl=ttl; self._ts=0; self._m={}; self._lk=threading.Lock()
    def _refresh(self):
        r=requests.get(self.URL, timeout=20); r.raise_for_status()
        m={}
        for s in r.json().get("symbols",[]):
            sym=s["symbol"].upper()
            info={"tickSize":None,"stepSize":None,"minNotional":0.0,"multiplier":1.0}
            for f in s.get("filters",[]):
                t=f.get("filterType")
                if t=="PRICE_FILTER": info["tickSize"]=float(f["tickSize"])
                elif t=="LOT_SIZE": info["stepSize"]=float(f["stepSize"])
                elif t=="MIN_NOTIONAL": info["minNotional"]=float(f["notional"])
            m[sym]=info
        with self._lk:
            self._m=m; self._ts=time.time()
    def get(self, symbol):
        with self._lk:
            need = (not self._m) or (time.time()-self._ts>self.ttl)
        if need: self._refresh()
        return self._m.get(symbol.upper())
    def norm_px(self, symbol, px): return _round_step(px, (self.get(symbol) or {}).get("tickSize",0.0))
    def norm_qty(self, symbol, qty): return _round_step(qty, (self.get(symbol) or {}).get("stepSize",0.0))
    def pass_min_notional(self, symbol, px, qty):
        inf=self.get(symbol) or {}; return float(px)*float(qty)*float(inf.get("multiplier",1.0)) >= float(inf.get("minNotional",0.0))

# OKX
_OKX_INS_URL="https://www.okx.com/api/v5/public/instruments?instType=SWAP"
class OkxRules:
    def __init__(self, ttl=600): self.ttl=ttl; self._ts=0; self._m={}; self._lk=threading.Lock()
    def _refresh(self):
        r=requests.get(_OKX_INS_URL, timeout=15); r.raise_for_status()
        m={}
        for it in r.json().get("data",[]):
            m[it["instId"]]={
                "tickSz": float(it.get("tickSz","0") or 0),
                "lotSz": float(it.get("lotSz","0") or 0),
                "minSz": float(it.get("minSz","0") or 0),
                "ctVal": float(it.get("ctVal","1") or 1.0),
                "ctValCcy": it.get("ctValCcy","USDT"),
            }
        with self._lk: self._m=m; self._ts=time.time()
    def get(self, instId):
        if (not self._m) or (time.time()-self._ts>self.ttl): self._refresh()
        return self._m.get(instId)
    def norm_px(self, instId, px): return _round_step(px, (self.get(instId) or {}).get("tickSz",0.0))
    def norm_sz(self, instId, sz):
        inf=self.get(instId) or {}; step=max(inf.get("lotSz",0.0), inf.get("minSz",0.0)); return _round_step(sz, step)
    def contracts_from_notional(self, instId, notional_usdt):
        inf=self.get(instId) or {}; ct=float(inf.get("ctVal",1.0))
        raw=max(1.0, notional_usdt/ct); return self.norm_sz(instId, raw)

# Bitget
_BG_INS_URL="https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
class BitgetRules:
    def __init__(self, ttl=600): self.ttl=ttl; self._ts=0; self._m={}; self._lk=threading.Lock()
    def _refresh(self):
        r=requests.get(_BG_INS_URL, timeout=15); r.raise_for_status()
        m={}
        for it in r.json().get("data",[]):
            sym=it["symbol"].upper()
            m[sym]={
                "pricePlace": int(it.get("pricePlace","4") or 4),
                "sizePlace": int(it.get("sizePlace","0") or 0),
                "minTradeNum": float(it.get("minTradeNum","1") or 1.0),
                "contractSize": float(it.get("contractSize","1") or 1.0),
            }
        with self._lk: self._m=m; self._ts=time.time()
    def get(self, symbol):
        if (not self._m) or (time.time()-self._ts>self.ttl): self._refresh()
        return self._m.get(symbol)
    def norm_px(self, symbol, px):
        dp=(self.get(symbol) or {}).get("pricePlace",4)
        return float(f"{float(px):.{dp}f}")
    def norm_sz(self, symbol, sz):
        inf=self.get(symbol) or {}; sp=int(inf.get("sizePlace",0)); mn=float(inf.get("minTradeNum",1.0))
        val=max(mn, float(sz)); return float(f"{val:.{sp}f}")
    def contracts_from_notional(self, symbol, notional_usdt):
        inf=self.get(symbol) or {}; face=max(1.0, float(inf.get("contractSize",1.0)))
        raw=max(inf.get("minTradeNum",1.0), notional_usdt/face); return self.norm_sz(symbol, raw)

# -------- 符号映射 --------
def to_okx(inst:str)->str:
    inst=inst.upper().replace("_","").replace("-","")
    return f"{inst[:-4]}-USDT-SWAP" if inst.endswith("USDT") else f"{inst}-USDT-SWAP"
def to_binance(inst:str)->str: return inst.upper().replace("-","").replace("_","")
def to_bitget(inst:str)->str: return inst.upper().replace("-","").replace("_","")

# -------- 冲击成本（平方根冲击 + 体量/ADV） --------
def estimate_impact_bps(notional_series_usdt: pd.Series, adv_usdt: float, kappa: float=15.0):
    adv = max(1.0, float(adv_usdt))
    x = (notional_series_usdt.fillna(0.0).abs()/adv).clip(lower=0.0)
    return kappa*np.sqrt(x)*100  # 返回 bps
