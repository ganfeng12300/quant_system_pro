# tools/best_params_importer.py
# 功能：将 live_best_params.json（或结构相近的 JSON）回灌到 SQLite 的 best_params 表
# 要点：
# - 兼容多种 JSON 结构：顶层 list / {"items":[...]} / dict-of-dicts / 深层嵌套
# - 仅导入含 symbol / (tf|timeframe) / strategy 的对象
# - -0.00 规整为 0.0
# - 表主键：(symbol,timeframe) —— A 方案“单最佳”
# - UPSERT：只有当新 score 更高(或旧分数为空)时，才会“整行完整覆盖”

import json
import sqlite3
import datetime
import argparse
import collections
import os

def _num(x):
    if x is None:
        return None
    try:
        v = float(x)
        if abs(v) < 1e-12:  # 归零，避免 -0.00
            v = 0.0
        return v
    except Exception:
        return None

def _collect(obj):
    """
    广度优先扫描容器，只收集含 symbol / (tf|timeframe) / strategy 的 dict
    兼容：list / {"items":[...]} / dict-of-dicts / 更深层嵌套
    """
    q = collections.deque([obj])
    out = []
    while q:
        o = q.popleft()
        if isinstance(o, dict):
            if ('symbol' in o) and (('tf' in o) or ('timeframe' in o)) and ('strategy' in o):
                out.append(o)
                continue
            # 常见容器键
            for k in ('items', 'data', 'results', 'list', 'payload', 'best', 'entries', 'records'):
                v = o.get(k)
                if isinstance(v, (list, tuple, dict)):
                    q.append(v)
            # 其他 value 也扫
            for v in o.values():
                if isinstance(v, (list, tuple, dict)):
                    q.append(v)
        elif isinstance(o, (list, tuple, set)):
            for v in o:
                if isinstance(v, (list, tuple, dict, set)):
                    q.append(v)
    return out

def main():
    ap = argparse.ArgumentParser(description='Import BEST params JSON into SQLite best_params')
    ap.add_argument('--db', required=True, help='Path to market_data.db')
    ap.add_argument('--json', required=True, help='Path to live_best_params.json')
    args = ap.parse_args()

    if not os.path.exists(args.json):
        raise FileNotFoundError(f'JSON not found: {args.json}')
    os.makedirs(os.path.dirname(args.db), exist_ok=True)

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    # 基本优化
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA synchronous=NORMAL')

    # 表结构：A 方案（单最佳）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS best_params(
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      strategy TEXT NOT NULL,
      params_json TEXT,
      metric_return REAL,
      metric_trades INTEGER,
      score REAL,
      dd REAL,
      turnover REAL,
      updated_at TEXT,
      PRIMARY KEY(symbol,timeframe)
    )
    """)

    with open(args.json, encoding='utf-8') as f:
        raw = json.load(f)

    items = _collect(raw)
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    import json as J

    n = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        sym = it.get('symbol')
        tf = it.get('tf') or it.get('timeframe')
        strat = it.get('strategy')
        if not (sym and tf and strat):
            continue

        p = it.get('params') or {}
        m = it.get('metrics') or {}
        ret = _num(m.get('return'))
        try:
            trd = int(m.get('trades')) if m.get('trades') is not None else None
        except Exception:
            trd = None
        scr = _num(m.get('score'))
        dd  = _num(m.get('dd'))
        tnr = _num(m.get('turnover'))

        # —— 关键：UPSERT（仅当新 score 更高时，整行完整覆盖）——
        cur.execute(
            'INSERT INTO best_params('
            ' symbol,timeframe,strategy,params_json,'
            ' metric_return,metric_trades,score,dd,turnover,updated_at'
            ') VALUES(?,?,?,?,?,?,?,?,?,?) '
            'ON CONFLICT(symbol,timeframe) DO UPDATE SET '
            ' strategy=excluded.strategy, '
            ' params_json=excluded.params_json, '
            ' metric_return=excluded.metric_return, '
            ' metric_trades=excluded.metric_trades, '
            ' score=excluded.score, '
            ' dd=excluded.dd, '
            ' turnover=excluded.turnover, '
            ' updated_at=excluded.updated_at '
            'WHERE best_params.score IS NULL '
            '  OR (excluded.score IS NOT NULL AND excluded.score > best_params.score)',
            (sym, tf, strat, J.dumps(p, ensure_ascii=False), ret, trd, scr, dd, tnr, now)
        )
        n += 1

    con.commit()

    # 索引（幂等）
    cur.execute('CREATE INDEX IF NOT EXISTS idx_best_params_sym_tf ON best_params(symbol,timeframe)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_best_params_updated ON best_params(updated_at)')
    con.commit()

    print('upserted=', n)
    cnt = cur.execute('SELECT COUNT(*) FROM best_params').fetchone()[0]
    rows = cur.execute(
        'SELECT symbol,timeframe,ROUND(COALESCE(score,0),6),ROUND(COALESCE(dd,0),6) '
        'FROM best_params ORDER BY updated_at DESC, rowid DESC LIMIT 5'
    ).fetchall()
    print('best_params rows=', cnt)
    print(rows)
    con.close()

if __name__ == '__main__':
    main()
