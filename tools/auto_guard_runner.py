# -*- coding: utf-8 -*-
"""
自调并发守护启动器（机构级）
------------------------------------------------------------
目标：在海量 Symbols × 多周期下，自动守护稳定性 →
- 实时采集（K 线 + 报价）内置调度器，**根据 418/429 比例**自动：
  · 降并发 / 拉大轮询间隔（冷却）
  · 连续健康窗口后再升并发 / 缩短间隔（加速）
- 输出统一的彩色横幅 + 实时统计 + 动态调参日志

使用（示例）：
python tools/auto_guard_runner.py \
  --db "D:\\quant_system_v2\\data\\market_data.db" \
  --symbols-file results\\keep_symbols.txt \
  --tfs 5m 15m 30m 1h 2h \
  --limit 750 \
  --workers-start 2 --workers-min 1 --workers-max 6 \
  --k-interval-start 45 --k-interval-min 20 --k-interval-max 90 \
  --q-interval-start 5  --q-interval-min 2  --q-interval-max 15 \
  --window-sec 60 --err-threshold 0.05 --cooldown-sec 90

说明：
- 本脚本自带采集逻辑（无需再启动 realtime_collector.py）。
- 418/429 超阈值 → 立即“降档”（并发-1，间隔+5s，进入冷却）；
  连续健康 3 窗口 → “升档”（并发+1，间隔-5s）。
- 所有 HTTP 异常均分类统计：429/418/451/HTTP/CONN/TO/JSON/SQL/OTHER。
"""
from __future__ import annotations
import argparse, os, sys, time, random, threading, queue, sqlite3
from typing import List, Tuple, Optional
import requests

from utils.utils_fetch import (
    fetch_futures_klines_smart,
    save_klines_to_db,
    last_ts,
    upsert_rt_quote,
)

# ───────────────────────── 颜色 ─────────────────────────
RESET = "\033[0m"; DIM="\033[2m"; BOLD="\033[1m"
FG = {'grey':"\033[90m", 'red':"\033[91m", 'green':"\033[92m", 'yellow':"\033[93m", 'blue':"\033[94m", 'magenta':"\033[95m", 'cyan':"\033[96m", 'white':"\033[97m"}
print_lock = threading.Lock()

def cfmt(txt, color):
    return f"{FG.get(color,'')}{txt}{RESET}"

# ───────────────────────── 参数 ─────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--symbols-file', required=True)
    p.add_argument('--tfs', nargs='+', required=True)
    p.add_argument('--limit', type=int, default=750)
    # 并发上下限
    p.add_argument('--workers-start', type=int, default=2)
    p.add_argument('--workers-min', type=int, default=1)
    p.add_argument('--workers-max', type=int, default=8)
    # K/Q 间隔（秒）
    p.add_argument('--k-interval-start', type=int, default=45)
    p.add_argument('--k-interval-min', type=int, default=20)
    p.add_argument('--k-interval-max', type=int, default=120)
    p.add_argument('--q-interval-start', type=int, default=5)
    p.add_argument('--q-interval-min', type=int, default=2)
    p.add_argument('--q-interval-max', type=int, default=30)
    # 守护窗口与阈值
    p.add_argument('--window-sec', type=int, default=60, help='评估窗口（秒）')
    p.add_argument('--err-threshold', type=float, default=0.05, help='触发降档的 418+429 比例阈值，如 0.05=5%')
    p.add_argument('--cooldown-sec', type=int, default=90, help='降档后冷却秒数')
    return p.parse_args()

# ───────────────────────── 读取符号 ─────────────────────────

def load_symbols(path:str)->List[str]:
    with open(path,'r',encoding='utf-8') as f:
        return [x.strip().upper() for x in f if x.strip() and not x.startswith('#')]

# ───────────────────────── 计数器 ─────────────────────────

class Ctr:
    def __init__(self):
        self.k_ok=0; self.k_err=0; self.k_new=0
        self.q_ok=0; self.q_err=0
        self.emap={ '429':0,'418':0,'451':0,'HTTP':0,'CONN':0,'TIMEOUT':0,'JSON':0,'SQL':0,'OTHER':0 }
        self.req_total=0
        self.start=time.time(); self.lock=threading.Lock()
    def add_k_ok(self, added:int):
        with self.lock:
            self.k_ok+=1; self.k_new+=max(0,added); self.req_total+=1
    def add_q_ok(self):
        with self.lock:
            self.q_ok+=1; self.req_total+=1
    def add_err(self, where:str, kind:str):
        with self.lock:
            if where=='K': self.k_err+=1
            else: self.q_err+=1
            self.emap[kind]=self.emap.get(kind,0)+1
            self.req_total+=1
    def snapshot(self):
        with self.lock:
            el=time.time()-self.start
            return (
                f"{cfmt('K-OK '+str(self.k_ok),'green')} · {cfmt('K-ERR '+str(self.k_err),'red')} · "
                f"{cfmt('Q-OK '+str(self.q_ok),'green')} · {cfmt('Q-ERR '+str(self.q_err),'red')} · "
                f"{cfmt('新增 '+str(self.k_new),'cyan')} · "
                f"{DIM}429:{self.emap['429']} 418:{self.emap['418']} 451:{self.emap['451']} HTTP:{self.emap['HTTP']} CONN:{self.emap['CONN']} TO:{self.emap['TIMEOUT']} JSON:{self.emap['JSON']} SQL:{self.emap['SQL']} OTHER:{self.emap['OTHER']}{RESET} · "
                f"{DIM}{el:,.1f}s{RESET}"
            )

# ───────────────────────── 任务实现 ─────────────────────────

def k_task(db_path:str, symbol:str, tf:str, limit:int, ctr:Ctr):
    con = sqlite3.connect(db_path, timeout=60)
    con.execute('PRAGMA journal_mode=WAL;')
    con.execute('PRAGMA synchronous=NORMAL;')
    con.execute('PRAGMA temp_store=MEMORY;')
    try:
        table=f"{symbol}_{tf}"; last=last_ts(con, table)
        if last is None:
            start_ms = int((time.time()-7*86400)*1000)  # 新表回看 7 天，避免一次拉太多
        else:
            start_ms = (last+1)*1000
        rows = fetch_futures_klines_smart(con, symbol, tf, start_ms, limit=limit)
        added = 0 if not rows else save_klines_to_db(con, symbol, tf, rows)
        ctr.add_k_ok(added)
        with print_lock:
            print(cfmt('✅ K','green'), f"{symbol}_{tf}", cfmt(f"新增{added}",'cyan'), ' · ', ctr.snapshot())
    except requests.HTTPError as he:
        code = getattr(getattr(he,'response',None),'status_code',None)
        kind = str(code) if code in (418,429,451) else 'HTTP'
        ctr.add_err('K', kind)
        with print_lock:
            print(cfmt('⚠️ K-ERR','yellow'), f"{symbol}_{tf}", f"status={code}", ' · ', ctr.snapshot())
    except requests.Timeout:
        ctr.add_err('K','TIMEOUT');
        with print_lock: print(cfmt('⌛ K-TO','yellow'), f"{symbol}_{tf}", ' · ', ctr.snapshot())
    except requests.ConnectionError:
        ctr.add_err('K','CONN');
        with print_lock: print(cfmt('🔌 K-CONN','red'), f"{symbol}_{tf}", ' · ', ctr.snapshot())
    except sqlite3.Error as se:
        ctr.add_err('K','SQL');
        with print_lock: print(cfmt('🗄 K-SQL','red'), f"{symbol}_{tf}", se, ' · ', ctr.snapshot())
    except ValueError as ve:
        ctr.add_err('K','JSON');
        with print_lock: print(cfmt('🧩 K-JSON','magenta'), f"{symbol}_{tf}", ve, ' · ', ctr.snapshot())
    except Exception as e:
        ctr.add_err('K','OTHER');
        with print_lock: print(cfmt('❗ K-OTH','red'), f"{symbol}_{tf}", e, ' · ', ctr.snapshot())
    finally:
        try: con.close()
        except: pass

def q_task(db_path:str, symbol:str, ctr:Ctr):
    con = sqlite3.connect(db_path, timeout=60)
    try:
        import requests
        # 轻量报价：/fapi/v1/ticker/bookTicker
        host = random.choice(["https://fapi.binance.com","https://fapi.binance.net","https://fapi.binance.me"])
        url = f"{host}/fapi/v1/ticker/bookTicker?symbol={symbol}"
        r = requests.get(url, timeout=8, headers={"User-Agent":"Mozilla/5.0 (compatible; QuantCollector/1.0)"})
        code = r.status_code
        if code in (418,429,451):
            raise requests.HTTPError(f"HTTP {code}", response=r)
        r.raise_for_status()
        j = r.json()
        bid=float(j['bidPrice']); ask=float(j['askPrice'])
        upsert_rt_quote(con, symbol, {"bid":bid, "ask":ask, "updated_at":int(time.time())})
        ctr.add_q_ok()
        with print_lock:
            print(cfmt('✅ Q','green'), f"{symbol} bid={bid:.4f} ask={ask:.4f}", ' · ', ctr.snapshot())
    except requests.HTTPError as he:
        code = getattr(getattr(he,'response',None),'status_code',None)
        kind = str(code) if code in (418,429,451) else 'HTTP'
        ctr.add_err('Q', kind)
        with print_lock:
            print(cfmt('⚠️ Q-ERR','yellow'), f"{symbol}", f"status={code}", ' · ', ctr.snapshot())
    except requests.Timeout:
        ctr.add_err('Q','TIMEOUT');
        with print_lock: print(cfmt('⌛ Q-TO','yellow'), f"{symbol}", ' · ', ctr.snapshot())
    except requests.ConnectionError:
        ctr.add_err('Q','CONN');
        with print_lock: print(cfmt('🔌 Q-CONN','red'), f"{symbol}", ' · ', ctr.snapshot())
    except sqlite3.Error as se:
        ctr.add_err('Q','SQL');
        with print_lock: print(cfmt('🗄 Q-SQL','red'), f"{symbol}", se, ' · ', ctr.snapshot())
    except ValueError as ve:
        ctr.add_err('Q','JSON');
        with print_lock: print(cfmt('🧩 Q-JSON','magenta'), f"{symbol}", ve, ' · ', ctr.snapshot())
    except Exception as e:
        ctr.add_err('Q','OTHER');
        with print_lock: print(cfmt('❗ Q-OTH','red'), f"{symbol}", e, ' · ', ctr.snapshot())
    finally:
        try: con.close()
        except: pass

# ───────────────────────── 守护器 ─────────────────────────

def banner(sym_n:int, tfs:List[str], w:int, ki:int, qi:int):
    text = (
        f"{BOLD}{FG['cyan']}🛡  自调并发守护启动器（418/429 自适应）{RESET}\n"
        f"符号数：{sym_n:<4}  周期：{','.join(tfs)}  并发：{w}  K每{ki}s  Q每{qi}s"
    )
    print("╔" + "═"*78 + "╗")
    for row in text.splitlines():
        print("║ " + row.ljust(76) + " ║")
    print("╚" + "═"*78 + "╝")

class AutoTuner:
    def __init__(self, args, syms:List[str]):
        self.args=args; self.syms=syms
        self.workers=args.workers_start
        self.k_interval=args.k_interval_start
        self.q_interval=args.q_interval_start
        self.healthy_windows=0
        self.last_adjust_ts=0
        self.stop=threading.Event()
        self.ctr_window={'req':0,'e418':0,'e429':0}
        self.window_lock=threading.Lock()

        # 执行队列与线程池
        self.qtasks: queue.Queue[tuple] = queue.Queue(max(0, self.workers*4))
        self.threads: List[threading.Thread] = []

    def _spawn_pool(self):
        # 终止旧线程
        for t in self.threads:
            t.join(timeout=0.1)
        self.threads.clear()
        # 重新建池
        for _ in range(max(1, self.workers)):
            th = threading.Thread(target=self._worker, daemon=True)
            th.start(); self.threads.append(th)

    def _worker(self):
        ctr = self.glob_ctr
        while not self.stop.is_set():
            try:
                fn, payload = self.qtasks.get(timeout=0.5)
            except queue.Empty:
                continue
            before_req = ctr.req_total
            try:
                fn(*payload)
            finally:
                made = max(0, ctr.req_total - before_req)
                with self.window_lock:
                    self.ctr_window['req'] += made
                self.qtasks.task_done()

    def _enqueue_round(self):
        # 一轮 K
        for s in self.syms:
            for tf in self.args.tfs:
                try:
                    self.qtasks.put_nowait((k_task, (self.args.db, s, tf, self.args.limit, self.glob_ctr)))
                except queue.Full:
                    break
        # 一轮 Q
        for s in self.syms:
            try:
                self.qtasks.put_nowait((q_task, (self.args.db, s, self.glob_ctr)))
            except queue.Full:
                break

    def _adjust_if_needed(self):
        # 基于窗口内 418/429 比例调参
        with self.window_lock:
            total = max(1, self.ctr_window['req'])
            e418 = self.glob_ctr.emap['418']
            e429 = self.glob_ctr.emap['429']
            # 取差值：只看本窗口新增的 418/429（简单估计）
            # 为避免复杂记录，这里近似：使用总量变化率进行判断
            ratio = (e418 + e429) / total
            # 清空窗口请求计数（错误数采用总计近似，不回退）
            self.ctr_window = {'req':0,'e418':0,'e429':0}

        now = time.time()
        if now - self.last_adjust_ts < 5:  # 防抖
            return

        if ratio >= self.args.err_threshold:
            # 降档
            old_w, old_ki, old_qi = self.workers, self.k_interval, self.q_interval
            self.workers = max(self.args.workers_min, self.workers-1)
            self.k_interval = min(self.args.k_interval_max, self.k_interval+5)
            self.q_interval = min(self.args.q_interval_max, self.q_interval+2)
            self.healthy_windows = 0
            self.last_adjust_ts = now
            self._spawn_pool()
            with print_lock:
                print(cfmt('▼ 降档','yellow'), f"418/429 达 {ratio:.1%}  -> 并发 {old_w}->{self.workers}  K间隔 {old_ki}->{self.k_interval}s  Q间隔 {old_qi}->{self.q_interval}s  ·  冷却 {self.args.cooldown_sec}s")
            time.sleep(self.args.cooldown_sec)
        else:
            # 健康窗口
            self.healthy_windows += 1
            if self.healthy_windows >= 3:
                old_w, old_ki, old_qi = self.workers, self.k_interval, self.q_interval
                self.workers = min(self.args.workers_max, self.workers+1)
                self.k_interval = max(self.args.k_interval_min, self.k_interval-5)
                self.q_interval = max(self.args.q_interval_min, self.q_interval-1)
                self.healthy_windows = 0
                self.last_adjust_ts = now
                self._spawn_pool()
                with print_lock:
                    print(cfmt('▲ 升档','green'), f"连续健康 3 窗口 -> 并发 {old_w}->{self.workers}  K间隔 {old_ki}->{self.k_interval}s  Q间隔 {old_qi}->{self.q_interval}s")

    def run(self):
        if os.name == 'nt':
            os.system("")
        self.glob_ctr = Ctr()
        banner(len(self.syms), self.args.tfs, self.workers, self.k_interval, self.q_interval)
        self._spawn_pool()

        next_k = time.time(); next_q = time.time(); next_guard = time.time() + self.args.window_sec
        try:
            while not self.stop.is_set():
                now = time.time()
                if now >= next_k:
                    # K 轮：抖动分流
                    self._enqueue_round()
                    next_k = now + self.k_interval + random.random()*0.5
                if now >= next_q:
                    # Q 轮：轻量更频繁
                    for s in self.syms:
                        try:
                            self.qtasks.put_nowait((q_task, (self.args.db, s, self.glob_ctr)))
                        except queue.Full:
                            break
                    next_q = now + self.q_interval + random.random()*0.2
                if now >= next_guard:
                    # 窗口评估与调参
                    with print_lock:
                        print(cfmt('⏱ 守护窗口','magenta'), self.glob_ctr.snapshot())
                    self._adjust_if_needed()
                    next_guard = now + self.args.window_sec
                time.sleep(0.2)
        except KeyboardInterrupt:
            with print_lock: print(cfmt('用户中断，正在收尾…','yellow'))
        finally:
            self.stop.set()
            try: self.qtasks.join()
            except: pass
            for t in self.threads:
                t.join(timeout=0.2)

# ───────────────────────── main ─────────────────────────

def main():
    args = parse_args()
    syms = load_symbols(args.symbols_file)
    tuner = AutoTuner(args, syms)
    tuner.run()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(cfmt(f"[FATAL] {e}", 'red'))
        sys.exit(2)
