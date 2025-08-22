# -*- coding: utf-8 -*-
import os, time, hmac, hashlib, base64, json, requests
from urllib.parse import urlencode
from tools.fees_rules import BinanceRules, OkxRules, BitgetRules, to_okx, to_binance, to_bitget

def _flag(name, default="0"): return os.environ.get(name, default)=="1"
LIVE_BINANCE=lambda:_flag("QS_LIVE_BINANCE","0")
LIVE_OKX=lambda:_flag("QS_LIVE_OKX","0")
LIVE_BITGET=lambda:_flag("QS_LIVE_BITGET","0")

class BinanceExec:
    BASE="https://fapi.binance.com"
    def __init__(self, key, sec): self.key=key; self.sec=sec.encode(); self.rules=BinanceRules()
    def _signed(self, path, params):
        qs=urlencode(params); sig=hmac.new(self.sec, qs.encode(), hashlib.sha256).hexdigest()
        headers={"X-MBX-APIKEY": self.key}
        return requests.post(self.BASE+path+"?"+qs+"&signature="+sig, headers=headers, timeout=15)
    def market(self, symbol, side, qty, paper=True):
        if paper or not LIVE_BINANCE():
            return {"paper":True,"exchange":"BINANCE","symbol":symbol,"side":side,"qty":qty}
        ts=int(time.time()*1000)
        payload={"symbol":to_binance(symbol),"side":side,"type":"MARKET","quantity":qty,"timestamp":ts,"recvWindow":5000}
        r=self._signed("/fapi/v1/order", payload)
        return r.json() if r.headers.get("content-type","").startswith("application/json") else {"status":r.status_code,"text":r.text}

class OkxExec:
    BASE="https://www.okx.com"
    def __init__(self,key,sec,pf): self.key=key; self.sec=sec; self.pf=pf; self.rules=OkxRules()
    @staticmethod
    def _ts(): import time; return time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
    def _sign(self, ts, method, path, body=""):
        msg=f"{ts}{method}{path}{body}"; return base64.b64encode(hmac.new(self.sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
    def _headers(self, ts, sign):
        return {"OK-ACCESS-KEY":self.key,"OK-ACCESS-SIGN":sign,"OK-ACCESS-TIMESTAMP":ts,"OK-ACCESS-PASSPHRASE":self.pf,"Content-Type":"application/json"}
    def market(self, symbol, side, notional, last_px, paper=True):
        instId=to_okx(symbol)
        if paper or not LIVE_OKX(): return {"paper":True,"exchange":"OKX","instId":instId,"side":side,"notional":notional}
        sz=self.rules.contracts_from_notional(instId, notional)
        body={"instId":instId,"tdMode":"cross","side":"buy" if side=="BUY" else "sell","ordType":"market","sz":str(sz)}
        path="/api/v5/trade/order"; js=json.dumps(body)
        ts=self._ts(); sign=self._sign(ts,"POST",path,js)
        r=requests.post(self.BASE+path, headers=self._headers(ts,sign), data=js, timeout=20)
        return r.json() if r.headers.get("content-type","").startswith("application/json") else {"status":r.status_code,"text":r.text}

class BitgetExec:
    BASE="https://api.bitget.com"
    def __init__(self,key,sec,pf,flag="0"): self.key=key; self.sec=sec; self.pf=pf; self.rules=BitgetRules(); self.flag=flag
    @staticmethod
    def _ts(): import time; return str(int(time.time()*1000))
    def _sign(self, ts, method, path, body=""):
        msg=f"{ts}{method}{path}{body}"; return base64.b64encode(hmac.new(self.sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
    def _headers(self, ts, sign):
        return {"ACCESS-KEY":self.key,"ACCESS-SIGN":sign,"ACCESS-TIMESTAMP":ts,"ACCESS-PASSPHRASE":self.pf,"Content-Type":"application/json","locale":"en-US"}
    def market(self, symbol, side, notional, last_px, paper=True):
        inst=to_bitget(symbol)
        if paper or not LIVE_BITGET(): return {"paper":True,"exchange":"BITGET","symbol":inst,"side":side,"notional":notional}
        size=self.rules.contracts_from_notional(inst, notional)
        sd="open_long" if side=="BUY" else "close_long"
        body={"symbol":inst,"marginCoin":"USDT","side":sd,"orderType":"market","size":str(size),"productType":"USDT-FUTURES"}
        path="/api/v2/mix/order/placeOrder"; js=json.dumps(body,separators=(",",":"))
        ts=self._ts(); sign=self._sign(ts,"POST",path,js)
        r=requests.post(self.BASE+path, headers=self._headers(ts,sign), data=js, timeout=20)
        return r.json() if r.headers.get("content-type","").startswith("application/json") else {"status":r.status_code,"text":r.text}
