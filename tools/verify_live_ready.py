# tools/verify_live_ready.py
# 目的：验证实盘是否能读取最佳参数
# - 检查表是否存在、是否有数据
# - 若给定 --symbol/--timeframe，验证该键是否可读
# - 否则抽取最新一行
# - 尝试 json 解析 params_json，并输出关键字段
# 退出码：0=通过；非0=不通过

import argparse, sqlite3, json, os, sys

def fail(msg, code=1):
    print(f"[FAIL] {msg}")
    sys.exit(code)

def ok(msg):
    print(f"[OK] {msg}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to market_data.db")
    ap.add_argument("--symbol", help="Optional symbol to check, e.g. BTCUSDT")
    ap.add_argument("--timeframe", help="Optional timeframe to check, e.g. 1h")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        fail(f"DB not found: {args.db}", 2)

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    # 1) 表是否存在
    tab = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='best_params'"
    ).fetchone()
    if not tab:
        fail("Table best_params does not exist.", 3)
    ok("Table best_params exists.")

    # 2) 是否有数据
    cnt = cur.execute("SELECT COUNT(*) FROM best_params").fetchone()[0]
    if cnt <= 0:
        fail("best_params is empty.", 4)
    ok(f"best_params rows = {cnt}")

    # 3) 选取检查目标
    row = None
    if args.symbol and args.timeframe:
        row = cur.execute(
            """SELECT symbol,timeframe,strategy,params_json,score,dd,turnover,updated_at
               FROM best_params WHERE symbol=? AND timeframe=?""",
            (args.symbol, args.timeframe),
        ).fetchone()
        if not row:
            fail(f"No row for ({args.symbol}, {args.timeframe}).", 5)
        ok(f"Found row for ({args.symbol}, {args.timeframe}).")
    else:
        row = cur.execute(
            """SELECT symbol,timeframe,strategy,params_json,score,dd,turnover,updated_at
               FROM best_params
               ORDER BY updated_at DESC, rowid DESC
               LIMIT 1"""
        ).fetchone()
        ok(f"Picked latest row: ({row[0]}, {row[1]})")

    sym, tf, strat, params_json, score, dd, turnover, ts = row

    # 4) 解析 params_json
    try:
        params = json.loads(params_json or "{}")
    except Exception as e:
        fail(f"params_json is not valid JSON: {e}", 6)

    # 5) 打印关键信息（供实盘对接核验）
    print("---- BEST PARAM SNAPSHOT ----")
    print("symbol     :", sym)
    print("timeframe  :", tf)
    print("strategy   :", strat)
    print("score      :", score)
    print("dd         :", dd)
    print("turnover   :", turnover)
    print("updated_at :", ts)
    print("params_keys:", sorted(list(params.keys()))[:20])
    print("-----------------------------")

    con.close()
    ok("Live-readiness check PASSED.")
    sys.exit(0)

if __name__ == "__main__":
    main()
