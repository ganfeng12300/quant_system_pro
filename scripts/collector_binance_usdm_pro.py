# -*- coding: utf-8 -*-
"""
采集·机构级（S级）：Binance USDT-M 永续（不含 1m）
- 只写已收盘K（严禁未来数据）
- 只补齐不足；绝不删除历史
- DB：D:\quant_system_v2\data\market_data.db
- 表结构遵循现有 <SYMBOL>_<TF> 形如 BTCUSDT_1h（检测毫秒/秒时间戳）
- 彩色终端 UI：总览统计条 + 总进度条 + 分TF进度条
- 限流/退避/自愈；断点续传；保守并发（批次）
- 周期性 WAL -> 主库 checkpoint（让 market_data.db “肉眼变大”）
"""
import os, re, sys, time, math, json, sqlite3, traceback, datetime, threading
from typing import Dict, List, Tuple, Optional

# ===== 固定配置 =====
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH      = r"D:\quant_system_v2\data\market_data.db"
TIMEFRAMES   = ["5m","15m","30m","1h","2h","4h","1d"]  # 无 1m
BATCH_SIZE   = 60
MAX_RETRY    = 5
BACKOFF_MAX  = 60       # 指数退避上限（秒）
UPDATE_CAP   = 30       # 实时阶段单TF最大轮询间隔（秒）
CHECKPOINT_EVERY_SEC = 20   # ← 让主库大小“肉眼可见变化”：每 20s 合并一次 WAL
AUTOCHECKPOINT_PAGES = 1000 # 双保险：每约1000页自动合并
LOG_DIR      = os.path.join(PROJECT_ROOT, f"logs/collect_binance_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE     = os.path.join(LOG_DIR, "run.log")
HEALTH_FILE  = os.path.join(LOG_DIR, "health.json")

# === ANSI 彩色 ===
WIN = os.name == "nt"
try:
    if WIN:
        import ctypes
        ctypes.windll.kernel32.SetConsoleMode(ctypes.windll.kernel32.GetStdHandle(-11), 7)
except Exception:
    pass
C = {
    "reset":"\033[0m","dim":"\033[2m","bold":"\033[1m",
    "green":"\033[38;5;40m","amber":"\033[38;5;214m","orange":"\033[38;5;208m",
    "red":"\033[38;5;196m","blue":"\033[38;5;39m","gold":"\033[38;5;220m","gray":"\033[38;5;245m"
}
def cfmt(s, color): return f"{C.get(color,'')}{s}{C['reset']}"

def log_line(*args, color=None, end="\n"):
    msg = " ".join(str(a) for a in args)
    t   = datetime.datetime.utcnow().strftime("%H:%M:%S")
    line = f"[{t}] {msg}"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")
    except Exception: pass
    print(cfmt(line, color) if color else line, end=end, flush=True)

# === 进度/总览 ===
STATS = {"inserted":0, "rate_last_t": time.time(), "rate_last_v":0, "rate_rps":0.0, "max_delay_sec":0}
def _bar(cur, total, width=30):
    total = max(1, int(total)); cur = max(0, min(int(cur), total))
    pct = cur/total; fill = int(width*pct + 0.5)
    return f"[{'#'*fill}{'.'*(width-fill)}] {cur}/{total} {pct*100:5.1f}%"
def _update_rate(add_rows:int):
    STATS["inserted"] += int(add_rows)
    now = time.time()
    if now - STATS["rate_last_t"] >= 2.0:
        delta = STATS["inserted"] - STATS["rate_last_v"]
        dt    = max(1e-6, now - STATS["rate_last_t"])
        STATS["rate_rps"] = delta / dt
        STATS["rate_last_v"] = STATS["inserted"]
        STATS["rate_last_t"] = now
def _render_status(done_pairs, total_pairs, tf_done, tf_total, tf, tail=""):
    ins, rate, lag = f"{STATS['inserted']:,}", f"{STATS['rate_rps']:.1f}", int(STATS["max_delay_sec"])
    summary = f"[总览] ins={ins}  rate={rate} r/s  max_lag={lag}s"
    status  = f"[总进度] {_bar(done_pairs,total_pairs,30)}  |  [TF {tf:>3}] {_bar(tf_done,tf_total,18)}"
    sys.stdout.write("\x1b[2K" + "\r" + cfmt(summary,"gold") + "  " + status + ("  "+tail if tail else ""))
    sys.stdout.flush()

# === 时间/TF ===
def tf_to_ms(tf:str)->int:
    if tf.endswith("m"): return int(tf[:-1])*60_000
    if tf.endswith("h"): return int(tf[:-1])*3_600_000
    if tf.endswith("d"): return int(tf[:-1])*86_400_000
    raise ValueError("bad tf "+tf)
def now_ms()->int: return int(time.time()*1000)
def last_closed_ms(tf_ms:int)->int:
    n = now_ms(); return n - (n % tf_ms) - tf_ms
def sleep_s(sec:float):
    try: time.sleep(max(0.0,sec))
    except KeyboardInterrupt: raise

# === SQLite 层 ===
def db_connect()->sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(f"PRAGMA wal_autocheckpoint={AUTOCHECKPOINT_PAGES}")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    return con

def detect_ts_unit(con)->int:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    pat = re.compile(r"^([A-Za-z0-9]+)_(5m|15m|30m|1h|2h|4h|1d)$", re.I)
    for (name,) in cur.fetchall():
        if not pat.match(name): continue
        try:
            m = con.execute(f"SELECT MAX(ts) FROM '{name}'").fetchone()[0]
            if m: return 1000 if m < 10_000_000_000 else 1
        except Exception: pass
    return 1

def clone_or_create_table(con, table:str, ref_table:Optional[str])->None:
    cur = con.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    ddls = {r[0]: r[1] for r in cur.fetchall()}
    if table in ddls: return
    ddl = None
    if ref_table and ref_table in ddls and ddls[ref_table]:
        ddl = ddls[ref_table].replace(ref_table, table)
    if not ddl:
        ddl = f"""CREATE TABLE IF NOT EXISTS "{table}"(
            ts INTEGER PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL, volume REAL
        )"""
    con.execute(ddl)
    try: con.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table}_ts" ON "{table}"(ts)')
    except Exception: pass

def get_db_symbols(con)->List[str]:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    pat = re.compile(r"^([A-Za-z0-9]+)_(5m|15m|30m|1h|2h|4h|1d)$", re.I)
    syms=set()
    for (name,) in cur.fetchall():
        m=pat.match(name)
        if m: syms.add(m.group(1).upper())
    return sorted(syms)

def max_ts(con, table:str)->Optional[int]:
    try: return con.execute(f"SELECT MAX(ts) FROM '{table}'").fetchone()[0]
    except Exception: return None

def upsert_ohlcv(con, table:str, rows:List[Tuple[int,float,float,float,float,float]])->Tuple[int,int,int]:
    if not rows: return (0,0,0)
    inserted = replaced = skipped = 0
    con.execute("BEGIN IMMEDIATE")
    try:
        for (ts,o,h,l,c,v) in rows:
            try:
                con.execute(f'INSERT OR REPLACE INTO "{table}"(ts,open,high,low,close,volume) VALUES(?,?,?,?,?,?)',
                            (ts,o,h,l,c,v))
                inserted += 1
            except Exception:
                skipped += 1
        con.commit()
    except Exception:
        con.rollback(); raise
    return inserted, replaced, skipped

# === 交易所（Binance USDT-M 永续：ccxt） ===
def build_exchange():
    import ccxt
    ex = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "future", "recvWindow": 10_000}
    })
    markets = ex.load_markets()
    id2ccxtsymbol = {}   # "BTCUSDT" -> "BTC/USDT:USDT"
    ccxtsymbol2id = {}
    for s,m in markets.items():
        if m.get("swap") and m.get("settle")=="USDT":
            _id = m.get("id")
            if _id:
                id2ccxtsymbol[_id.upper()] = s
                ccxtsymbol2id[s] = _id.upper()
    return ex, id2ccxtsymbol, ccxtsymbol2id

# === Checkpoint 后台线程 ===
def _checkpoint_worker():
    while True:
        try:
            con = sqlite3.connect(DB_PATH, timeout=60)
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            con.close()
            log_line("[CHKPT] wal->db 合并完成", color="blue")
        except Exception as e:
            log_line(f"[CHKPT][WARN] {e}", color="amber")
        sleep_s(CHECKPOINT_EVERY_SEC)

# === 业务：补齐一只 ===
def backfill_one(ex, con, symbol_id:str, ccxt_symbol:str, tf:str, ts_unit:int, ref_table:Optional[str])->Tuple[int,int,int,int,int,int]:
    """
    返回：fetched, kept, inserted, skipped, gaps, delay_sec
    """
    tfms = tf_to_ms(tf)
    table = f"{symbol_id}_{tf}"
    clone_or_create_table(con, table, ref_table)
    last_db_ts = max_ts(con, table)
    target = last_closed_ms(tfms)

    # 如果数据库里有“未来K”（旧系统写的未收盘K），提示但不回退、不删除
    if last_db_ts and last_db_ts*ts_unit > target:
        log_line(f"[WARN] {table} 含未来K（MAX(ts)>{int(target/ts_unit)}），本次忽略未来段，仅追平至 last_closed", color="amber")

    # 起点：空表回溯一年；有表从下一根开始
    start_ms = (last_db_ts*ts_unit + tfms) if last_db_ts else (target - 365*86_400_000)

    fetched = kept = inserted = skipped = gaps = 0
    since = max(0, start_ms)
    backoff = 1
    while since <= target:
        try:
            batch = ex.fetch_ohlcv(ccxt_symbol, timeframe=tf, since=since, limit=1500)
            if not batch: break

            cleaned=[]
            for ohlc in batch:
                ts_ms,o,h,l,c,v = ohlc
                if ts_ms > target: continue
                if None in (o,h,l,c,v): continue
                if any(x!=x for x in (o,h,l,c,v)): continue
                if o<=0 or h<=0 or l<=0 or c<=0 or v<0: continue
                cleaned.append((ts_ms,float(o),float(h),float(l),float(c),float(v)))
            kept += len(cleaned)
            fetched += len(cleaned)   # ← 统计修正：以清洗后有效条数为准

            for i in range(1,len(cleaned)):
                if cleaned[i][0]-cleaned[i-1][0] != tfms: gaps += 1

            rows=[(int(ts_ms//ts_unit),o,h,l,c,v) for (ts_ms,o,h,l,c,v) in cleaned]
            ins, rep, skp = upsert_ohlcv(con, table, rows)
            inserted += ins; skipped += skp
            _update_rate(ins)

            last = cleaned[-1][0] if cleaned else since + tfms*1500
            since = last + tfms
            backoff = 1
        except Exception as e:
            log_line(f"[BACKFILL] {symbol_id} {tf} error -> backoff {backoff}s :: {type(e).__name__}: {e}", color="amber")
            sleep_s(backoff); backoff = min(BACKOFF_MAX, backoff*2)

    cur_last = max_ts(con, table) or 0
    delay_sec = max(0, int((target - cur_last*ts_unit)/1000))
    STATS["max_delay_sec"] = max(STATS["max_delay_sec"], delay_sec)
    return fetched, kept, inserted, skipped, gaps, delay_sec

# === 实时阶段（只写已收盘K） ===
def realtime_loop(ex, con, id2ccxt:Dict[str,str], symbols:List[str], ts_unit:int):
    next_tick={tf:0.0 for tf in TIMEFRAMES}
    ref_table = pick_ref_table(con)
    while True:
        now=time.time()
        for tf in TIMEFRAMES:
            if now < next_tick[tf]: continue
            tfms=tf_to_ms(tf); target=last_closed_ms(tfms); max_delay_this_tf=0
            for i in range(0,len(symbols),BATCH_SIZE):
                batch=symbols[i:i+BATCH_SIZE]
                for sym in batch:
                    ccxt_symbol=id2ccxt.get(sym)
                    if not ccxt_symbol: continue
                    table=f"{sym}_{tf}"; clone_or_create_table(con, table, ref_table)
                    last_db=max_ts(con, table)
                    since=int(max(0,(last_db or (target-tfms*3))*ts_unit - tfms*2))
                    backoff=1; tries=0
                    while True:
                        try:
                            ohlcvs=ex.fetch_ohlcv(ccxt_symbol, timeframe=tf, since=since, limit=200)
                            cleaned=[]
                            for ohlc in ohlcvs:
                                ts_ms,o,h,l,c,v = ohlc
                                if ts_ms>target: continue
                                if None in (o,h,l,c,v): continue
                                if any(x!=x for x in (o,h,l,c,v)): continue
                                if o<=0 or h<=0 or l<=0 or c<=0 or v<0: continue
                                cleaned.append((int(ts_ms//ts_unit),float(o),float(h),float(l),float(c),float(v)))
                            ins,rep,skp = upsert_ohlcv(con, table, cleaned)
                            if ins:
                                _update_rate(ins)
                                log_line(f"[UPD] {sym} {tf} +{ins} (target {datetime.datetime.utcfromtimestamp(target/1000).isoformat()}Z)", color="green")
                            now_last = max_ts(con, table) or 0
                            delay_s = max(0,int((target - now_last*ts_unit)/1000))
                            if delay_s > max_delay_this_tf: max_delay_this_tf = delay_s
                            break
                        except Exception as e:
                            tries+=1
                            log_line(f"[REAL] {sym} {tf} error -> backoff {backoff}s :: {type(e).__name__}: {e}", color="amber")
                            sleep_s(backoff); backoff=min(BACKOFF_MAX, backoff*2)
                            if tries>=MAX_RETRY:
                                log_line(f"[REAL][GIVEUP] {sym} {tf} after {tries} retries", color="red"); break
            STATS["max_delay_sec"]=max(STATS["max_delay_sec"], max_delay_this_tf)
            next_tick[tf]=time.time()+min(UPDATE_CAP, tfms/2000.0)

        # 健康探针 + 顶部状态行
        try:
            with open(HEALTH_FILE,"w",encoding="utf-8") as f:
                json.dump({"utc": datetime.datetime.utcnow().isoformat()+"Z",
                           "inserted":STATS["inserted"],"rate_rps":STATS["rate_rps"],
                           "max_delay_sec":STATS["max_delay_sec"],"next_tick":next_tick},
                          f, ensure_ascii=False, indent=2)
        except Exception: pass
        _render_status(0,1,1,1,"RT", tail=cfmt("实时增量运行中","green"))
        sleep_s(1.0)

def pick_ref_table(con)->Optional[str]:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    pat = re.compile(r"^([A-Za-z0-9]+)_(5m|15m|30m|1h|2h|4h|1d)$", re.I)
    for (name,) in cur.fetchall():
        if pat.match(name): return name
    return None

def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_line(cfmt("=== Binance USDT-M 永续 采集·机构级（补齐+实时｜5m..1d） ===","gold"))
    log_line(f"[INFO] ROOT={PROJECT_ROOT}")
    log_line(f"[INFO] DB  ={DB_PATH}")
    log_line(f"[INFO] TFs ={TIMEFRAMES}")

    # 只读备份
    try:
        bkp=os.path.join(os.path.dirname(DB_PATH), f"market_data.backup.{datetime.datetime.utcnow().strftime('%Y%m%d')}.sqlite")
        if not os.path.exists(bkp):
            con_tmp=sqlite3.connect(DB_PATH, timeout=60)
            con_tmp.execute(f"VACUUM INTO '{bkp}'"); con_tmp.close()
            log_line(f"[SAFE] 备份已生成：{bkp}", color="blue")
    except Exception as e:
        log_line(f"[SAFE][WARN] 备份失败：{e}", color="amber")

    con = db_connect()
    ts_unit = detect_ts_unit(con)
    log_line(f"[INFO] ts 单位：{'毫秒' if ts_unit==1 else '秒'}")

    # 启动后台 checkpoint 线程（让主库体积持续增长）
    threading.Thread(target=_checkpoint_worker, daemon=True).start()

    db_syms = get_db_symbols(con)
    if not db_syms:
        log_line("[FATAL] 数据库未发现任何 K 线表（形如 BTCUSDT_1h）", color="red"); return

    try:
        ex,id2ccxt,ccxt2id = build_exchange()
    except Exception as e:
        log_line(f"[FATAL] 初始化交易所失败：{e}", color="red"); return

    white=[s for s in db_syms if s in id2ccxt]
    miss =[s for s in db_syms if s not in id2ccxt]
    log_line(f"[OK] DB符号={len(db_syms)}  永续白名单交集={len(white)}  不存在/不支持={len(miss)}", color="green")
    if miss:
        log_line(f"[WARN] 跳过（非USDT永续或交易所无此合约）：{', '.join(miss[:12])}{' ...' if len(miss)>12 else ''}", color="amber")

    ref_table = pick_ref_table(con)

    # === 补齐阶段（带进度条） ===
    total_pairs = len(white)*len(TIMEFRAMES)
    done_pairs  = 0
    log_line(cfmt(f"=== 补齐阶段启动：目标组合 {total_pairs}（symbols {len(white)} × TF {len(TIMEFRAMES)}） ===","gold"))
    for tf in TIMEFRAMES:
        tf_total=len(white); tf_done=0
        log_line(cfmt(f"[TF] {tf} → 预期 {tf_total} 个标的","blue"))
        _render_status(done_pairs,total_pairs,tf_done,tf_total,tf)
        for i in range(0,len(white),BATCH_SIZE):
            batch=white[i:i+BATCH_SIZE]
            for sym in batch:
                ccxt_symbol=id2ccxt.get(sym)
                if not ccxt_symbol:
                    tf_done+=1; done_pairs+=1; _render_status(done_pairs,total_pairs,tf_done,tf_total,tf); continue
                try:
                    fetched,kept,ins,skp,gaps,delay = backfill_one(ex, con, sym, ccxt_symbol, tf, ts_unit, ref_table)
                    tf_done+=1; done_pairs+=1; _render_status(done_pairs,total_pairs,tf_done,tf_total,tf)
                    log_line(f"[BF] {sym} {tf} fetched={fetched} kept={kept} inserted={ins} skipped={skp} gaps={gaps} delay={delay}s", color="gray")
                except Exception as e:
                    tf_done+=1; done_pairs+=1; _render_status(done_pairs,total_pairs,tf_done,tf_total,tf)
                    log_line(f"[BF][ERR] {sym} {tf} :: {type(e).__name__}: {e}", color="red")
            sleep_s(10)
        print()

    log_line(cfmt("=== 补齐完成，进入实时增量（只写已收盘K） ===","gold"))
    try:
        realtime_loop(ex, con, id2ccxt, white, ts_unit)
    except KeyboardInterrupt:
        log_line("[EXIT] 用户中断，安全退出。", color="amber")
    except Exception as e:
        log_line(f"[FATAL] 实时环失败：{e}\n{traceback.format_exc()}", color="red")
    finally:
        try: con.close()
        except Exception: pass

if __name__=="__main__":
    try: main()
    except Exception as e:
        log_line(f"[FATAL] {e}\n{traceback.format_exc()}", color="red")
        input("按回车退出 > ")
