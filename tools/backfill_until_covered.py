# -*- coding: utf-8 -*-
"""
历史回补 · 稳定版（彩色）
- 读取 symbols 文件与周期，按天数回补 Binance USDT‑M K线，写入 SQLite。
- 使用 utils.utils_fetch 内的稳定抓取：多HOST轮询、429退避、451冷却、418冷却。
- 线程安全的彩色实时汇总：OK / ERR 计数 + 各类异常分布（429/418/451/HTTP/连接/超时/JSON/SQL 等）。
- 每个任务行内轻量日志：✅ 正常/ 📈 新增N / ⚠️ 分类错误原因。

用法（示例）：
python tools/backfill_until_covered.py \
  --db "D:\\quant_system_v2\\data\\market_data.db" \
  --symbols-file results\\keep_symbols.txt \
  --tfs 5m 15m 30m 1h 2h \
  --days 365 \
  --limit 1000 \
  --max-workers 2
"""
from __future__ import annotations
import argparse
import os
import sys
import time
import math
import threading
import queue
import sqlite3
from typing import List, Tuple

import requests

from utils.utils_fetch import (
    fetch_futures_klines_smart,
    save_klines_to_db,
    last_ts,
)

# ───────────────────────── 颜色 ─────────────────────────
RESET = "\033[0m"; DIM="\033[2m"; BOLD="\033[1m"
FG = {
    'grey':"\033[90m", 'red':"\033[91m", 'green':"\033[92m",
    'yellow':"\033[93m", 'blue':"\033[94m", 'magenta':"\033[95m",
    'cyan':"\033[96m", 'white':"\033[97m",
}

# ───────────────────────── 打印工具 ─────────────────────────
print_lock = threading.Lock()

def cfmt(txt, color):
    return f"{FG.get(color,'')}{txt}{RESET}"

def banner(args, sym_count):
    line = (
        f"{BOLD}{FG['magenta']}🚀 历史回补 · 稳定版（彩色）{RESET}\n"
        f"符号数：{sym_count:<4}    周期：{','.join(args.tfs)}    天数：{args.days:<4}    limit：{args.limit:<4}    并发：{args.max_workers}"
    )
    print("╔" + "═"*70 + "╗")
    for row in line.splitlines():
        print("║ " + row.ljust(68) + " ║")
    print("╚" + "═"*70 + "╝")

# ───────────────────────── 参数 ─────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--symbols-file', required=True)
    p.add_argument('--tfs', nargs='+', required=True)
    p.add_argument('--days', type=int, default=365)
    p.add_argument('--limit', type=int, default=1000)
    p.add_argument('--max-workers', type=int, default=2)
    return p.parse_args()

# ───────────────────────── 任务执行 ─────────────────────────

class Counters:
    def __init__(self):
        self.ok = 0; self.err = 0
        self.err_map = {
            '429':0,'418':0,'451':0,
            'HTTP':0,'CONN':0,'TIMEOUT':0,'JSON':0,'SQL':0,'OTHER':0
        }
        self.new_rows = 0
        self.start = time.time()
        self.lock = threading.Lock()

    def add_ok(self, added:int):
        with self.lock:
            self.ok += 1
            self.new_rows += max(0, added)

    def add_err(self, kind:str):
        with self.lock:
            self.err += 1
            self.err_map[kind] = self.err_map.get(kind,0) + 1

    def snapshot(self):
        with self.lock:
            elapsed = time.time()-self.start
            parts = [
                cfmt(f"OK {self.ok}", 'green'),
                cfmt(f"ERR {self.err}", 'red'),
                cfmt(f"新增 {self.new_rows}", 'cyan'),
                DIM + (
                    f"429:{self.err_map['429']} 418:{self.err_map['418']} 451:{self.err_map['451']} "
                    f"HTTP:{self.err_map['HTTP']} CONN:{self.err_map['CONN']} TO:{self.err_map['TIMEOUT']} "
                    f"JSON:{self.err_map['JSON']} SQL:{self.err_map['SQL']} OTHER:{self.err_map['OTHER']}"
                ) + RESET,
                DIM+f"{elapsed:,.1f}s"+RESET
            ]
            return " · ".join(parts)


def load_symbols(path:str)->List[str]:
    with open(path,'r',encoding='utf-8') as f:
        syms = [x.strip().upper() for x in f.readlines() if x.strip() and not x.startswith('#')]
    return syms


def backfill_symbol_tf(db_path:str, symbol:str, tf:str, days:int, limit:int, ctrs:Counters):
    # 每个线程独立连接（SQLite 线程隔离）
    con = sqlite3.connect(db_path, timeout=60)
    con.execute('PRAGMA journal_mode=WAL;')
    con.execute('PRAGMA synchronous=NORMAL;')
    con.execute('PRAGMA temp_store=MEMORY;')

    try:
        table = f"{symbol}_{tf}"
        last = last_ts(con, table)
        now_ms = int(time.time()*1000)
        start_ms = now_ms - days*86400*1000 if last is None else (last+1)*1000

        total_added = 0
        rounds = 0
        while True:
            rounds += 1
            rows = fetch_futures_klines_smart(con, symbol, tf, start_ms, limit=limit)
            if not rows:
                break
            added = save_klines_to_db(con, symbol, tf, rows)
            total_added += added
            start_ms = (rows[-1][0]+1)*1000
            if added == 0:
                # 已覆盖到头
                break
        ctrs.add_ok(total_added)
        with print_lock:
            msg = cfmt("✅", 'green')+f" {symbol}_{tf} " + cfmt(f"新增{total_added}", 'cyan') + "  ·  " + ctrs.snapshot()
            print(msg)
    except requests.HTTPError as he:
        code = getattr(getattr(he,'response',None), 'status_code', None)
        kind = str(code) if code in (418,429,451) else 'HTTP'
        ctrs.add_err(kind)
        with print_lock:
            print(cfmt("⚠️ HTTP", 'yellow'), f"{symbol}_{tf} · status={code} · {ctrs.snapshot()}")
    except requests.Timeout:
        ctrs.add_err('TIMEOUT')
        with print_lock:
            print(cfmt("⌛ 超时", 'yellow'), f"{symbol}_{tf} · {ctrs.snapshot()}")
    except requests.ConnectionError:
        ctrs.add_err('CONN')
        with print_lock:
            print(cfmt("🔌 连接", 'red'), f"{symbol}_{tf} · {ctrs.snapshot()}")
    except ValueError as ve:
        # JSON/参数等
        ctrs.add_err('JSON')
        with print_lock:
            print(cfmt("🧩 解析", 'magenta'), f"{symbol}_{tf} · {ve} · {ctrs.snapshot()}")
    except sqlite3.Error as se:
        ctrs.add_err('SQL')
        with print_lock:
            print(cfmt("🗄 SQL", 'red'), f"{symbol}_{tf} · {se} · {ctrs.snapshot()}")
    except Exception as e:
        ctrs.add_err('OTHER')
        with print_lock:
            print(cfmt("❗ 其他", 'red'), f"{symbol}_{tf} · {e} · {ctrs.snapshot()}")
    finally:
        try:
            con.close()
        except Exception:
            pass


# ───────────────────────── 主流程 ─────────────────────────

def main():
    args = parse_args()
    syms = load_symbols(args.symbols_file)
    banner(args, len(syms))

    tasks: List[Tuple[str,str]] = []
    for s in syms:
        for tf in args.tfs:
            tasks.append((s, tf))

    ctrs = Counters()

    # 简单线程池
    q = queue.Queue()
    for t in tasks:
        q.put(t)

    def worker(idx:int):
        while True:
            try:
                s, tf = q.get_nowait()
            except queue.Empty:
                break
            backfill_symbol_tf(args.db, s, tf, args.days, args.limit, ctrs)
            q.task_done()

    threads = []
    for i in range(max(1, args.max_workers)):
        th = threading.Thread(target=worker, args=(i,), daemon=True)
        th.start(); threads.append(th)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        with print_lock:
            print(cfmt("用户中断，正在收尾…", 'yellow'))

    # 收尾输出
    print("\n" + "═"*72)
    print("收尾汇总：", ctrs.snapshot())


if __name__ == '__main__':
    if os.name == 'nt':
        # 启用 Windows ANSI（Win10+ 支持）
        os.system("")
    try:
        main()
    except Exception as e:
        print(cfmt(f"[FATAL] {e}", 'red'))
        sys.exit(2)
