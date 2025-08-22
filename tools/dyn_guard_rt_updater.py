# -*- coding: utf-8 -*-
"""
动态并发守护 · 20核适配 · 二步升降（6↔4↔3）
监控 1 分钟窗口内的 418/429 错误比例与数据库锁表次数，自动降/升并发。
"""

import os, sys, time, signal, threading, queue, subprocess, re
from datetime import datetime, timedelta
from collections import deque

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_EXE   = sys.executable
UPDATER      = os.path.join(PROJECT_ROOT, "tools", "rt_updater_pro.py")

# ======== 你可以改的参数 ========
DB_PATH          = r"D:\quant_system_v2\data\market_data.db"
START_WORKERS    = 6                    # 起步并发
FALLBACK_LADDER  = [6, 4, 3]           # 二步升降梯子
SLEEP_SECS       = 30                   # rt_updater 的轮询间隔（若脚本支持 --sleep，会用上）
BACKFILL_DAYS    = 365
WINDOW_SECS      = 60                   # 统计窗口长度（秒）
ERR_RATE_LIMIT   = 0.05                 # 错误率阈值（≥5%）
LOCK_LIMIT_PER_WIN = 2                  # 一窗口内锁表阈值
STABLE_ROUNDS_TO_UP = 10                # 连续多少轮稳定才升档
PRINT_EVERY_SECS = 5                    # 控制台刷新频率（秒）
# =================================

# 统计用：窗口内逐条记录 (ts, ok/is_err/is_lock)
Line = re.compile(r".*")
ERR418 = re.compile(r"\b(418|I'm a teapot)\b", re.IGNORECASE)
ERR429 = re.compile(r"\b429\b|\brate limit\b", re.IGNORECASE)
LOCKED = re.compile(r"database is locked", re.IGNORECASE)

class RollingStats:
    def __init__(self, window_secs):
        self.window_secs = window_secs
        self.buf = deque()  # (ts, ok, is_err, is_lock)

    def add(self, ok:bool, is_err:bool, is_lock:bool):
        now = time.time()
        self.buf.append((now, ok, is_err, is_lock))
        self._trim(now)

    def _trim(self, now=None):
        if now is None: now = time.time()
        boundary = now - self.window_secs
        while self.buf and self.buf[0][0] < boundary:
            self.buf.popleft()

    def snapshot(self):
        self._trim()
        total = len(self.buf)
        errs  = sum(1 for _,_,e,_ in self.buf if e)
        locks = sum(1 for *_,l in self.buf if l)
        return total, errs, locks

def spawn_updater(max_workers:int):
    """启动 rt_updater_pro.py 子进程"""
    args = [
        PYTHON_EXE, UPDATER,
        "--db", DB_PATH,
        "--max-workers", str(max_workers),
        "--backfill", str(BACKFILL_DAYS),
    ]
    # 若你的 rt_updater_pro.py 支持 --sleep，则解注释下一行
    # args += ["--sleep", str(SLEEP_SECS)]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    return proc

def reader_thread(proc, q:queue.Queue):
    """读取子进程输出，逐行推入队列"""
    try:
        for line in proc.stdout:
            q.put(line.rstrip("\n"))
    except Exception:
        pass
    finally:
        q.put(None)  # EOF 标记

def should_downgrade(total, err, locks):
    if total == 0: 
        return False
    err_rate = err / total
    return err_rate >= ERR_RATE_LIMIT or locks >= LOCK_LIMIT_PER_WIN

def main():
    assert os.path.exists(UPDATER), f"找不到 rt_updater_pro.py: {UPDATER}"
    current_idx = 0  # 指向 FALLBACK_LADDER
    stats = RollingStats(WINDOW_SECS)

    stable_rounds = 0
    started_at = time.time()

    def banner():
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total, err, locks = stats.snapshot()
        rate = (err/total*100) if total else 0.0
        runmins = int((time.time() - started_at)//60)
        ladder = " ↔ ".join(str(x) for x in FALLBACK_LADDER)
        return (f"[{now}] 并发={FALLBACK_LADDER[current_idx]} | 窗口{WINDOW_SECS}s: "
                f"总:{total}  错:{err}({rate:.1f}%)  锁:{locks} | 连续稳定:{stable_rounds}/{STABLE_ROUNDS_TO_UP} | 梯子:{ladder} | 运行{runmins}m")

    while True:
        max_workers = FALLBACK_LADDER[current_idx]
        print(f"\n=== 启动 rt_updater（并发 {max_workers}）===\n")
        proc = spawn_updater(max_workers)
        q = queue.Queue()
        t = threading.Thread(target=reader_thread, args=(proc,q), daemon=True)
        t.start()

        last_print = 0
        need_restart = False

        while True:
            try:
                try:
                    line = q.get(timeout=1.0)
                except queue.Empty:
                    line = ""

                if line is None:
                    # 子进程自然退出
                    need_restart = True
                    print("子进程退出，准备重启…")
                    break

                if line:
                    # 判定一行是成功还是错误/锁表
                    is_err = bool(ERR418.search(line) or ERR429.search(line))
                    is_lock = bool(LOCKED.search(line))
                    ok = not (is_err or is_lock)
                    stats.add(ok, is_err, is_lock)

                # 控制台节流刷新
                now = time.time()
                if now - last_print > PRINT_EVERY_SECS:
                    print(banner())
                    last_print = now

                # 在每个统计窗口末尾做一次升降判定
                # 这里简化：每次刷新都判定一次，但是只有跨过阈值才动作
                total, err, locks = stats.snapshot()
                if should_downgrade(total, err, locks):
                    # 触发降档
                    if current_idx < len(FALLBACK_LADDER)-1:
                        current_idx += 1
                        stable_rounds = 0
                        print(f"⚠️ 错误率或锁表超阈值：降档至并发 {FALLBACK_LADDER[current_idx]}")
                        need_restart = True
                        break
                    else:
                        # 已在最低档，继续监控
                        stable_rounds = 0
                else:
                    # 稳定一轮
                    if total > 0:
                        stable_rounds = min(stable_rounds + 1, STABLE_ROUNDS_TO_UP)
                        # 足够稳定，尝试升档
                        if stable_rounds >= STABLE_ROUNDS_TO_UP and current_idx > 0:
                            current_idx -= 1
                            print(f"✅ 连续 {STABLE_ROUNDS_TO_UP} 轮稳定：升档至并发 {FALLBACK_LADDER[current_idx]}")
                            stable_rounds = 0
                            need_restart = True
                            break

            except KeyboardInterrupt:
                print("\n收到中断信号，准备退出…")
                try:
                    proc.terminate()
                    proc.wait(timeout=10)
                except Exception:
                    pass
                return

        # 平滑重启：优先结束子进程
        if need_restart:
            try:
                proc.terminate()
                proc.wait(timeout=10)
            except Exception:
                try:
                    if os.name == "nt":
                        subprocess.run(["taskkill", "/PID", str(proc.pid), "/F", "/T"])
                    else:
                        os.kill(proc.pid, signal.SIGKILL)
                except Exception:
                    pass
            time.sleep(2)  # 小憩，避免频繁重启
            continue

if __name__ == "__main__":
    main()
