# utils/param_loader.py
from __future__ import annotations
import json, os, time, sqlite3
from typing import Dict, Any, List

DEFAULT_JSON = os.path.join("deploy", "live_best_params.json")
BEST_TABLE = "best_params"

def load_best_params_from_json(json_path: str = DEFAULT_JSON) -> Dict[str, Any]:
    if not os.path.exists(json_path):
        return {"updated_at": None, "items": []}
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_best_params_to_json(items: List[Dict[str, Any]], json_path: str = DEFAULT_JSON):
    os.makedirs(os.path.dirname(json_path) or ".", exist_ok=True)
    payload = {"updated_at": time.strftime("%Y-%m-%d %H:%M:%S"), "items": items}
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload

def save_best_params_to_db(db_path: str, items: List[Dict[str, Any]]):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {BEST_TABLE}(
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        strategy TEXT NOT NULL,
        params_json TEXT NOT NULL,
        metric_return REAL,
        metric_trades INTEGER,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(symbol, timeframe, strategy)
    );""")
    for it in items:
        cur.execute(f"""INSERT INTO {BEST_TABLE}(symbol,timeframe,strategy,params_json,metric_return,metric_trades,updated_at)
                        VALUES(?,?,?,?,?,?,datetime('now'))
                        ON CONFLICT(symbol,timeframe,strategy) DO UPDATE SET
                            params_json=excluded.params_json,
                            metric_return=excluded.metric_return,
                            metric_trades=excluded.metric_trades,
                            updated_at=excluded.updated_at;""",
                    (it["symbol"], it["tf"], it["strategy"], json.dumps(it["params"], ensure_ascii=False),
                     float(it["metrics"].get("return", 0.0)), int(it["metrics"].get("trades", 0))))
    con.commit(); con.close()

def load_best_params_from_db(db_path: str) -> List[Dict[str, Any]]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(f"SELECT symbol,timeframe,strategy,params_json,metric_return,metric_trades,updated_at FROM {BEST_TABLE}")
    rows = cur.fetchall()
    con.close()
    items = []
    for s,tf,st,pj,ret,tr,upd in rows:
        items.append({"symbol": s, "tf": tf, "strategy": st, "params": json.loads(pj), "metrics": {"return": ret, "trades": tr, "updated_at": upd}})
    return items

def get_best_for(symbol: str, timeframe: str, strategy: str, db_path: str = None, json_path: str = DEFAULT_JSON):
    if db_path and os.path.exists(db_path):
        for it in load_best_params_from_db(db_path):
            if it["symbol"].upper()==symbol.upper() and it["tf"]==timeframe and it["strategy"]==strategy:
                return it
    payload = load_best_params_from_json(json_path)
    for it in payload.get("items", []):
        if it["symbol"].upper()==symbol.upper() and it["tf"]==timeframe and it["strategy"]==strategy:
            return it
    return None
