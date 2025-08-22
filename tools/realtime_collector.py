# -*- coding: utf-8 -*-
"""
实时采集 · 稳定版（彩色）
- 按周期抓取 Binance USDT‑M K 线（/fapi/v1/klines），写入 SQLite（INSERT OR IGNORE）。
- 同步抓取盘口快照（/fapi/v1/ticker/bookTicker），写入 rt_quotes（UPSERT）。
- 多 HOST 轮询 + 429 指数退避 + 451 冷却 + 418 冷却（含 Retry-After 识别）。
- K / Q 各自的 OK/ERR 计数与异常分类（429/418/451/HTTP/CONN/TO/JSON/SQL/OTHER）。
- 终端彩色横幅 + 实时汇总条，逐行输出：
    ✅ K: SYMBOL_TF 新增N    或   ⚠️ K-ERR: 分类
    ✅ Q: SYMBOL bid/ask ok  或   ⚠️ Q-ERR: 分类

用法示例：
python tools/realtime_collector.py \
  --db "D:\\quant_system_v2\\data\\market_data.db" \
  --symbols-file "results\\keep_symbols.txt" \
  --tfs 5m 15m 30m 1h 2h \
  --k-interval 30 \
  --q-interval 3 \
  --limit 750 \
  --max-workers 4
"""
from __future__ import annotations
import argparse
import os
import sys
import time
import math
import threading
import queue
import random
import sqlite3
from typing import List, Tuple, Optional

import requests

from utils.utils_fetch import (
    fetch_futures_klines_smart,
    save_klines_to_db,
    last_ts,
    upsert_rt_quote,
)

# ───────────────────────── 常量 & 颜色 ─────────────────────────
BINANCE_HOSTS = [
    "https://fapi.binance.com",
    "https://fapi.binance.net",
    "https://fapi.binance.me",
]
UA = "Mozilla/5.0 (compatible; QuantCollector/1.0; +https://local.example)"

RESET = "\033[0m"; DIM="\033[2m"; BOLD="\033[1m"
FG = {
    'grey':"\033[90m", 'red':"\033[91m", 'green':"\033[92m",
    'yellow':"\033[93m", 'blue':"\033[94m", 'magenta':"\033[95m",
    'cyan':"\033[96m", 'white':"\033[97m",
}
print_lock = threading.Lock()

def cfmt(txt, color):
    return f"{FG.get(color,'')}{txt}{RESET}"

# ───────────────────────── 请求封装（含 418/429/451） ─────────────────────────

def http_get_json(url: str, timeout: float = 8.0, session: Optional[requests.Session] = None,
                  max_tries: int = 5) -> dict | list:
    sess = session or requests.Session()
    base_sleep = 0.6; jitter = 0.3
    host_idx = 0
    last_http: Optional[int] = None
    last_err: Optional[str] = None
    for attempt in range(1, max_tries+1):
        # 轮换 host
        if url.startswith("/" ):
            host = BINANCE_HOSTS[host_idx % len(BINANCE_HOSTS)]
            host_idx += 1
            full = host + url
        else:
            full = url
        try:
            r = sess.get(full, headers={"User-Agent": UA}, timeout=timeout)
            code = r.status_code
            if code in (429, 418, 451):
                last_http = code
                # 若有 Retry-After，严格等待
                ra = r.headers.get('Retry-After')
                if ra and ra.isdigit():
                    time.sleep(min(180, int(ra)))
                else:
                    # 418 / 451 冷却时间更长一点
                    if code == 418:
                        time.sleep(60 + random.random()*90)
                    elif code == 451:
                        time.sleep(1.2 + random.random()*0.8)
                    else:  # 429 指数退避
                        sl = base_sleep * (2 ** (attempt-1)) + random.random()*jitter
                        time.sleep(min(8.0, sl))
                continue
            r.raise_for_status()
            try:
                return r.json()
            except Exception:
                last_err = "JSON parse error"; time.sleep(0.6+random.random()*0.4); continue
        except requests.HTTPError as he:
            # 抛给上层，由上层分类
            raise he
        except requests.Timeout:
            last_err = "timeout"; time.sleep(0.8+random.random()*0.6); continue
        except requests.ConnectionError:
            last_err = "conn"; time.sleep(0.8+random.random()*0.6); continue
        except Exception as e:
            last_err = str(e); time.sleep(0.6+random.random()*0.5); continue
    if last_http is not None:
        dummy = requests.Response(); dummy.status_code = last_http
        raise requests.HTTPError(f"HTTP {last_http}", response=dummy)
    raise RuntimeError(last_err or "http_get_json exhausted")

# ───────────────────────── 业务：Quotes ─────────────────────────

def fetch_book_ticker(symbol: str) -> dict:
    # 返回：{"symbol":..., "bidPrice":..., "askPrice":...}
    j = http_get_json(f"/fapi/v1/ticker/bookTicker?symbol={symbol}")
    if not isinstance(j, dict) or 'bidPrice' not in j:
        raise ValueError("unexpected bookTicker json")
    return j

# ───────────────────────── CLI ─────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--symbols-file', required=True)
    p.add_argument('--tfs', nargs='+', required=True)
    p.add_argument('--k-interval', type=int, default=30, help='K线轮询间隔（秒）')
    p.add_argument('--q-interval', type=int, default=3, help='报价轮询间隔（秒）')
    p.add_argument('--limit', type=int, default=750)
    p.add_argument('--max-workers', type=int, default=4)
    return p.parse_args()

# ───────────────────────── 计数器 ─────────────────────────

class Ctr:
    def __init__(self):
        self.k_ok=0; self.k_err=0; self.k_new=0
        self.q_ok=0; self.q_err=0
        self.emap={ '429':0,'418':0,'451':0,'HTTP':0,'CONN':0,'TIMEOUT':0,'JSON':0,'SQL':0,'OTHER':0 }
        self.start=time.time(); self.lock=threading.Lock()
    def add_k_ok(self, added:int):
        with self.lock:
            self.k_ok+=1; self.k_new+=max(0,added)
    def add_q_ok(self):
        with self.lock:
            self.q_ok+=1
    def add_err(self, kind:str):
        with self.lock:
            if kind not in self.emap: kind='OTHER'
            # 归类到 K 或 Q 任何一方都计入总错误
            if kind:
                self.emap[kind]+=1
            # 无法区分来源时，统一 +1 到 k_err（更保守）；调用点会修正
            self.k_err+=1
    def add_k_err(self, kind:str):
        with self.lock:
            self.k_err+=1; self.emap[kind]=self.emap.get(kind,0)+1
    def add_q_err(self, kind:str):
        with self.lock:
            self.q_err+=1; self.emap[kind]=self.emap.get(kind,0)+1
    def snap(self):
        with self.lock:
            el = time.time()-self.start
            return (
                f"{cfmt('K-OK '+str(self.k_ok),'green')} · "
                f"{cfmt('K-ERR '+str(self.k_err),'red')} · "
                f"{cfmt('Q-OK '+str(self.q_ok),'green')} · "
                f"{cfmt('Q-ERR '+str(self.q_err),'red')} · "
                f"{cfmt('新增 '+str(self.k_new),'cyan')} · "
                f"{DIM}429:{self.emap['429']} 418:{self.emap['418']} 451:{self.emap['451']} "
                f"HTTP:{self.emap['HTTP']} CONN:{self.emap['CONN']} TO:{self.emap['TIMEOUT']} JSON:{self.emap['JSON']} SQL:{self.emap['SQL']} OTHER:{self.emap['OTHER']}{RESET} · "
                f"{DIM}{el:,.1f}s{RESET}"
            )

# ───────────────────────── 读取符号 ─────────────────────────

def load_symbols(path:str)->List[str]:
    with open(path,'r',encoding='utf-8') as f:
        return [x.strip().upper() for x in f if x.strip() and not x.startswith('#')]

# ───────────────────────── K 线任务 ─────────────────────────

def k_task(db_path:str, symbol:str, tf:str, limit:int, ctr:Ctr):
    con = sqlite3.connect(db_path, timeout=60)
    con.execute('PRAGMA journal_mode=WAL;')
    con.execute('PRAGMA synchronous=NORMAL;')
    con.execute('PRAGMA temp_store=MEMORY;')
    try:
        table=f"{symbol}_{tf}"; last=last_ts(con, table)
        if last is None:
            # 若该表还没历史，由实时来回补最近若干根（limit 控制每轮最大）
            start_ms = int((time.time()-7*86400)*1000)  # 默认回看 7 天，避免权重暴涨
        else:
            start_ms = (last+1)*1000
        rows = fetch_futures_klines_smart(con, symbol, tf, start_ms, limit=limit)
        added = 0 if not rows else save_klines_to_db(con, symbol, tf, rows)
        ctr.add_k_ok(added)
        with print_lock:
            print(cfmt('✅ K','green'), f"{symbol}_{tf}", cfmt(f"新增{added}",'cyan'), ' · ', ctr.snap())
    except requests.HTTPError as he:
        code = getattr(getattr(he,'response',None), 'status_code', None)
        kind = str(code) if code in (418,429,451) else 'HTTP'
        ctr.add_k_err(kind)
        with print_lock:
            print(cfmt('⚠️ K-ERR','yellow'), f"{symbol}_{tf}", f"status={code}", ' · ', ctr.snap())
    except requests.Timeout:
        ctr.add_k_err('TIMEOUT');
        with print_lock:
            print(cfmt('⌛ K-TO','yellow'), f"{symbol}_{tf}", ' · ', ctr.snap())
    except requests.ConnectionError:
        ctr.add_k_err('CONN');
        with print_lock:
            print(cfmt('🔌 K-CONN','red'), f"{symbol}_{tf}", ' · ', ctr.snap())
    except sqlite3.Error as se:
        ctr.add_k_err('SQL');
        with print_lock:
            print(cfmt('🗄 K-SQL','red'), f"{symbol}_{tf}", se, ' · ', ctr.snap())
    except ValueError as ve:
        ctr.add_k_err('JSON');
        with print_lock:
            print(cfmt('🧩 K-JSON','magenta'), f"{symbol}_{tf}", ve, ' · ', ctr.snap())
    except Exception as e:
        ctr.add_k_err('OTHER');
        with print_lock:
            print(cfmt('❗ K-OTH','red'), f"{symbol}_{tf}", e, ' · ', ctr.snap())
    finally:
        try: con.close()
        except: pass

# ───────────────────────── 报价任务 ─────────────────────────

def q_task(db_path:str, symbol:str, ctr:Ctr):
    con = sqlite3.connect(db_path, timeout=60)
    try:
        j = fetch_book_ticker(symbol)
        bid = float(j['bidPrice']); ask = float(j['askPrice'])
        upsert_rt_quote(con, symbol, {"bid": bid, "ask": ask, "updated_at": int(time.time())})
        ctr.add_q_ok()
        with print_lock:
            print(cfmt('✅ Q','green'), f"{symbol} bid={bid:.4f} ask={ask:.4f}", ' · ', ctr.snap())
    except requests.HTTPError as he:
        code = getattr(getattr(he,'response',None), 'status_code', None)
        kind = str(code) if code in (418,429,451) else 'HTTP'
        ctr.add_q_err(kind)
        with print_lock:
            print(cfmt('⚠️ Q-ERR','yellow'), f"{symbol}", f"status={code}", ' · ', ctr.snap())
    except requests.Timeout:
        ctr.add_q_err('TIMEOUT');
        with print_lock:
            print(cfmt('⌛ Q-TO','yellow'), f"{symbol}", ' · ', ctr.snap())
    except requests.ConnectionError:
        ctr.add_q_err('CONN');
        with print_lock:
            print(cfmt('🔌 Q-CONN','red'), f"{symbol}", ' · ', ctr.snap())
    except sqlite3.Error as se:
        ctr.add_q_err('SQL');
        with print_lock:
            print(cfmt('🗄 Q-SQL','red'), f"{symbol}", se, ' · ', ctr.snap())
    except ValueError as ve:
        ctr.add_q_err('JSON');
        with print_lock:
            print(cfmt('🧩 Q-JSON','magenta'), f"{symbol}", ve, ' · ', ctr.snap())
    except Exception as e:
        ctr.add_q_err('OTHER');
        with print_lock:
            print(cfmt('❗ Q-OTH','red'), f"{symbol}", e, ' · ', ctr.snap())
    finally:
        try: con.close()
        except: pass

# ───────────────────────── 调度 ─────────────────────────

def banner(sym_count:int, tfs:List[str], kint:int, qint:int, mw:int):
    text = (
        f"{BOLD}{FG['cyan']}🛰  实时采集 · 稳定版（彩色）{RESET}\n"
        f"符号数：{sym_count:<4}     周期：{','.join(tfs)}    K每{kint}s  Q每{qint}s    并发：{mw}"
    )
    print("╔" + "═"*72 + "╗")
    for row in text.splitlines():
        print("║ " + row.ljust(70) + " ║")
    print("╚" + "═"*72 + "╝")


def main():
    args = parse_args()
    if os.name == 'nt':
        os.system("")  # 开启 Win ANSI

    syms = load_symbols(args.symbols_file)
    banner(len(syms), args.tfs, args.k_interval, args.q_interval, args.max_workers)

    ctr = Ctr()
    stop_event = threading.Event()

    # 任务队列（K、Q 共用线程池，控制并发）
    qtasks: queue.Queue[tuple] = queue.Queue(max(0, args.max_workers*4))

    def worker():
        while not stop_event.is_set():
            try:
                fn, payload = qtasks.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                fn(*payload)
            finally:
                qtasks.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, args.max_workers))]
    for t in threads: t.start()

    # 调度循环：按间隔把任务塞进队列；轻微抖动防止齐刷
    next_k = time.time(); next_q = time.time()
    try:
        while True:
            now = time.time()
            if now >= next_k:
                # 一轮 K：对每个 (symbol, tf) 生成任务
                for s in syms:
                    for tf in args.tfs:
                        try:
                            qtasks.put_nowait((k_task, (args.db, s, tf, args.limit, ctr)))
                        except queue.Full:
                            break
                next_k = now + args.k_interval + random.random()*0.5
            if now >= next_q:
                # 一轮 Q：对每个 symbol 生成任务（轻量）
                for s in syms:
                    try:
                        qtasks.put_nowait((q_task, (args.db, s, ctr)))
                    except queue.Full:
                        break
                next_q = now + args.q_interval + random.random()*0.2
            time.sleep(0.2)
    except KeyboardInterrupt:
        with print_lock: print(cfmt('用户中断，正在收尾…','yellow'))
    finally:
        stop_event.set()
        # 等待队列清空一小会
        try:
            qtasks.join()
        except Exception:
            pass

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(cfmt(f"[FATAL] {e}",'red'))
        sys.exit(2)
