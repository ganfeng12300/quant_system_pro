# -*- coding: utf-8 -*-
"""
把 deploy/live_best_params.json 同步进 DB.best_params（建表+覆盖）
用法：
  python scripts/sync_json_to_db.py <DB_PATH> <BEST_JSON>
"""
import sys, os, json, sqlite3, traceback

def main(db_path: str, json_path: str):
    print("[SYNC] DB =", db_path)
    print("[SYNC] JSON =", json_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS best_params(
            symbol TEXT,timeframe TEXT,strategy TEXT,params_json TEXT,
            metric_return REAL,metric_trades INTEGER,score REAL,dd REAL,turnover REAL,
            updated_at TEXT, PRIMARY KEY(symbol,timeframe)
        )""")
        if not os.path.exists(json_path) or os.path.getsize(json_path) == 0:
            print("[WARN] JSON 不存在或为空：", json_path)
        else:
            items = json.load(open(json_path, encoding="utf-8"))
            n = 0
            for it in items:
                m = it.get("metrics") or {}
                con.execute("""INSERT OR REPLACE INTO best_params
                    (symbol,timeframe,strategy,params_json,metric_return,metric_trades,score,dd,turnover,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))""",
                    (it.get("symbol"),
                     it.get("tf") or it.get("timeframe"),
                     it.get("strategy"),
                     json.dumps(it.get("params", {}), ensure_ascii=False),
                     m.get("return"), m.get("trades"), m.get("score"), m.get("dd"), m.get("turnover")))
                n += 1
            con.commit()
            print("[OK] JSON synced → best_params:", n)
        # 抽样打印
        try:
            import pandas as pd  # type: ignore
            df = pd.read_sql("""SELECT symbol,timeframe,strategy,
                                       substr(params_json,1,60) AS params,
                                       round(metric_return*100,2)||'%%' AS ret,
                                       metric_trades, round(score,4) AS score, updated_at
                                FROM best_params ORDER BY updated_at DESC LIMIT 12""", con)
            print(df.to_string(index=False))
        except Exception as e:
            print("[WARN] 抽样失败：", e)
    finally:
        con.close()

if __name__ == "__main__":
    try:
        db = sys.argv[1]
        js = sys.argv[2]
    except Exception:
        print("用法: python scripts/sync_json_to_db.py <DB_PATH> <BEST_JSON>")
        sys.exit(2)
    try:
        main(db, js)
    except Exception as e:
        print("[FATAL]", e)
        print(traceback.format_exc())
        sys.exit(1)
