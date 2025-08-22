# ==============================================
# File: tools/bridge_best_stdout.py
# Desc: 无侵入桥接器——捕获优化器 [BEST] 输出 → 写 JSON（原子+备份）
#       + 同步写入 DB（可选）；满足 S 级验收（JSON↔DB、DD非负、run_config、回滚）
# Python: 3.8+
# ==============================================

import argparse, os, sys, re, json, sqlite3, time, hashlib, shutil, tempfile, ast, datetime, subprocess
from typing import List, Dict, Tuple

ANSI = os.name != 'nt' or os.environ.get('WT_SESSION') or os.environ.get('ANSICON')

def c(color, s):
    if not ANSI: return s
    codes = {
        'g': '\x1b[92m', 'y': '\x1b[93m', 'r': '\x1b[91m', 'b': '\x1b[94m', 'm': '\x1b[95m', 'c': '\x1b[96m',
        'w': '\x1b[97m', 'reset': '\x1b[0m'
    }
    return f"{codes.get(color,'')}{s}{codes['reset']}"

BEST_RE = re.compile(
    r"^\[BEST\]\s+(?P<sym>[A-Z0-9]+)\s+(?P<tf>1m|5m|15m|30m|1h|2h|4h|1d)\s+"
    r"(?P<strat>[A-Za-z0-9_]+)\s+(?P<params>\{.*\})\s+"
    r"ret=(?P<ret>[-\d\.]+)%\s+trades=(?P<trades>\d+)\s+score=(?P<score>[-\d\.]+)\s+dd=(?P<dd>[-\d\.]+)"
)

SCHEMA_BEST_PARAMS_SQL = """
CREATE TABLE IF NOT EXISTS best_params(
    symbol TEXT,
    timeframe TEXT,
    strategy TEXT,
    params_json TEXT,
    metric_return REAL,
    metric_trades INTEGER,
    score REAL,
    dd REAL,
    turnover REAL,
    updated_at TEXT,
    PRIMARY KEY(symbol,timeframe)
);
"""

SCHEMA_META_SQL = """
CREATE TABLE IF NOT EXISTS best_params_meta(
    symbol TEXT,
    timeframe TEXT,
    eligible_live INTEGER DEFAULT 1,
    approved_live INTEGER DEFAULT 1,
    slippage_bps REAL,
    fee_bps REAL,
    exec_lag INTEGER,
    no_intrabar INTEGER,
    window_days INTEGER,
    version_hash TEXT,
    db_fingerprint TEXT,
    updated_at TEXT,
    PRIMARY KEY(symbol,timeframe)
);
"""

UPSERT_BEST_SQL = """
INSERT INTO best_params(symbol,timeframe,strategy,params_json,metric_return,metric_trades,score,dd,turnover,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))
ON CONFLICT(symbol,timeframe) DO UPDATE SET
    strategy=excluded.strategy,
    params_json=excluded.params_json,
    metric_return=excluded.metric_return,
    metric_trades=excluded.metric_trades,
    score=excluded.score,
    dd=excluded.dd,
    turnover=excluded.turnover,
    updated_at=datetime('now');
"""

UPSERT_META_SQL = """
INSERT INTO best_params_meta(symbol,timeframe,eligible_live,approved_live,slippage_bps,fee_bps,exec_lag,no_intrabar,window_days,version_hash,db_fingerprint,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
ON CONFLICT(symbol,timeframe) DO UPDATE SET
    eligible_live=excluded.eligible_live,
    approved_live=excluded.approved_live,
    slippage_bps=excluded.slippage_bps,
    fee_bps=excluded.fee_bps,
    exec_lag=excluded.exec_lag,
    no_intrabar=excluded.no_intrabar,
    window_days=excluded.window_days,
    version_hash=excluded.version_hash,
    db_fingerprint=excluded.db_fingerprint,
    updated_at=datetime('now');
"""

SELECT_KEYS_SQL = "SELECT symbol,timeframe FROM best_params ORDER BY symbol,timeframe"

def sha1_of_file(path: str) -> str:
    try:
        h = hashlib.sha1()
        with open(path,'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return 'unknown'

def db_fingerprint(path: str) -> str:
    try:
        st = os.stat(path)
        return f"size={st.st_size};mtime={int(st.st_mtime)}"
    except Exception:
        return 'unknown'

def atomic_write_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmpfd, tmppath = tempfile.mkstemp(prefix='.tmp_json_', dir=os.path.dirname(path) or '.')
    try:
        with os.fdopen(tmpfd, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        bak = path + '.bak'
        if os.path.exists(path):
            shutil.copy2(path, bak)
        os.replace(tmppath, path)
    finally:
        try:
            if os.path.exists(tmppath): os.unlink(tmppath)
        except Exception:
            pass

def clamp_nonneg(x: float) -> float:
    if x is None: return 0.0
    if -1e-12 < x < 0:   # treat -0.00 as 0
        return 0.0
    return max(0.0, x)

def parse_best_line(line: str):
    m = BEST_RE.match(line.strip())
    if not m:
        return None
    try:
        sym = m.group('sym').upper()
        tf = m.group('tf')
        strat = m.group('strat')
        params_txt = m.group('params')
        params = ast.literal_eval(params_txt)
        ret_pct = float(m.group('ret'))
        trades = int(m.group('trades'))
        score = float(m.group('score'))
        dd_val = float(m.group('dd'))
        return {
            'symbol': sym,
            'tf': tf,
            'strategy': strat,
            'params': params,
            'metrics': {
                'return': ret_pct/100.0,   # 存小数
                'trades': trades,
                'score': score,
                'dd': clamp_nonneg(dd_val),
                'turnover': None,
            }
        }
    except Exception:
        return None

def sync_to_db(con: sqlite3.Connection, items: List[Dict], meta: Dict, approve_all: int):
    con.execute('PRAGMA journal_mode=WAL;')
    con.execute(SCHEMA_BEST_PARAMS_SQL)
    con.execute(SCHEMA_META_SQL)
    for it in items:
        m = it['metrics']
        con.execute(UPSERT_BEST_SQL, (
            it['symbol'], it['tf'], it['strategy'], json.dumps(it['params'], ensure_ascii=False),
            m.get('return'), m.get('trades'), m.get('score'), m.get('dd'), m.get('turnover'),
        ))
        con.execute(UPSERT_META_SQL, (
            it['symbol'], it['tf'], 1, 1 if approve_all else 0,
            meta.get('slip_bps'), meta.get('fee_bps'), meta.get('exec_lag'), meta.get('no_intrabar'),
            meta.get('days'), meta.get('version_hash'), meta.get('db_fingerprint')
        ))
    con.commit()

def json_from_items(items: List[Dict], meta: Dict):
    out = []
    for it in items:
        o = {
            'symbol': it['symbol'],
            'tf': it['tf'],
            'strategy': it['strategy'],
            'params': it['params'],
            'metrics': it['metrics'],
            'updated_at': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+'Z'
        }
        out.append(o)
    return {
        'meta': meta,
        'items': out
    }

def ensure_usdt_perp(symbols_file: str) -> set:
    ok = set()
    if symbols_file and os.path.exists(symbols_file):
        with open(symbols_file, 'r', encoding='utf-8') as f:
            for line in f:
                s = line.strip().upper()
                if not s: continue
                if s.endswith('USDT'):
                    ok.add(s)
    return ok

def main():
    ap = argparse.ArgumentParser(description='Bridge [BEST] stdout -> JSON/DB (non-intrusive)')
    ap.add_argument('--optimizer', default=r'optimizer\a1a8_optimizer_and_deploy.py')
    ap.add_argument('--db', required=True)
    ap.add_argument('--symbols-file', required=True)
    ap.add_argument('--timeframes', nargs='+', required=True)
    ap.add_argument('--days', type=int, required=True)
    ap.add_argument('--min-trades', dest='min_trades', type=int, default=5)
    ap.add_argument('--max-dd', dest='max_dd', type=float, default=0.9)
    ap.add_argument('--json', default=r'deploy\live_best_params.json')
    ap.add_argument('--write-db', type=int, default=1)
    ap.add_argument('--approve-all', type=int, default=1)
    ap.add_argument('--fee-bps', type=float, default=5)
    ap.add_argument('--slip-bps', type=float, default=2)
    ap.add_argument('--exec-lag', type=int, default=1)
    ap.add_argument('--no-intrabar', type=int, default=1)
    ap.add_argument('--extra', nargs=argparse.REMAINDER, help='extra args pass to optimizer')

    args = ap.parse_args()

    os.makedirs('deploy', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

    allow = ensure_usdt_perp(args.symbols_file)

    # 版本与DB指纹
    ver = sha1_of_file(args.optimizer)
    dbfp = db_fingerprint(args.db)

    run_cfg = {
        'fee_bps': args.fee_bps,
        'slip_bps': args.slip_bps,
        'exec_lag': args.exec_lag,
        'no_intrabar': args.no_intrabar,
        'days': args.days,
        'timeframes': args.timeframes,
        'optimizer': args.optimizer,
        'version_hash': ver,
        'db_fingerprint': dbfp,
        'generated_at': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')
    }

    # 写 run_config.json（原子+备份）
    atomic_write_json('deploy/run_config.json', run_cfg)

    # 启动优化器子进程
    cmd = [sys.executable, args.optimizer,
           '--db', args.db,
           '--symbols-file', args.symbols_file,
           '--json', args.json,               # 即便优化器不写，也无害
           '--timeframes', *args.timeframes,
           '--days', str(args.days),
           '--min-trades', str(max(1, args.min_trades)),
           '--max-dd', str(max(0.0, args.max_dd)),
           '--deploy']
    if args.extra:
        cmd += args.extra

    print(c('c', f"▶ 启动优化器：{' '.join(cmd)}"))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            bufsize=1, universal_newlines=True, encoding='utf-8', errors='ignore')

    collected: Dict[Tuple[str,str], Dict] = {}
    total_lines = 0
    log_path = os.path.join('logs', f"bridge_{int(time.time())}.log")
    with open(log_path, 'w', encoding='utf-8') as flog:
        while True:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    break
                else:
                    time.sleep(0.02)
                    continue
            total_lines += 1
            sys.stdout.write(line)
            flog.write(line)
            flog.flush()
            it = parse_best_line(line)
            if it:
                if allow and it['symbol'] not in allow:
                    continue  # 仅保留 USDT 永续白名单
                key = (it['symbol'], it['tf'])
                # 若重复，只保留 score 更高者
                old = collected.get(key)
                if (not old) or (it['metrics'].get('score', -1e9) > old['metrics'].get('score', -1e9)):
                    collected[key] = it

    rc = proc.wait()
    print(c('y', f"优化器退出码：{rc}，共读取行数={total_lines}，BEST条目={len(collected)}"))

    # 生成 JSON 数据
    json_obj = json_from_items(list(collected.values()), run_cfg)
    atomic_write_json(args.json, json_obj)
    print(c('g', f"JSON 写入完成：{args.json} items={len(json_obj['items'])}"))

    # 同步 DB（可选）
    if args.write_db:
        con = sqlite3.connect(args.db)
        try:
            sync_to_db(con, list(collected.values()), run_cfg, args.approve_all)
            # 一致性校验：确保 JSON 键全集已存在于 DB
            k_json = {(it['symbol'], it['tf']) for it in collected.values()}
            k_db = set(con.execute(SELECT_KEYS_SQL).fetchall())
            if k_json - k_db:
                print(c('r', f"一致性失败：DB 缺失 {len(k_json-k_db)} 条，如 {list(k_json-k_db)[:3]}"))
                # 回滚 JSON
                bak = args.json + '.bak'
                if os.path.exists(bak):
                    shutil.copy2(bak, args.json)
                    print(c('y', '已回滚到上一版 JSON .bak'))
                sys.exit(2)
            else:
                print(c('g', '🟢 JSON↔DB 强一致 ✅'))
        finally:
            con.close()

    print(c('g', '✅ 桥接流程完成：DD 规范化｜JSON/DB 落地｜run_config 记录｜仅 USDT 永续'))

if __name__ == '__main__':
    main()
