import os
import sys
import time
import signal
import subprocess
import threading
from pathlib import Path
import logging
from collections import deque


class Window:
    """环形窗口，用于统计最近 N 条 stdout，检测锁冲突/限流"""

    def __init__(self, maxlen=200):
        self.buf = deque(maxlen=maxlen)
        self.lock = threading.Lock()

    def add(self, line: str):
        with self.lock:
            self.buf.append((time.time(), line))

    def snapshot(self):
        with self.lock:
            return list(self.buf)


class Guardian:
    def __init__(self, db, steps, interval, backfill_days,
                 max_workers, housekeeping_window):
        self.db = db
        self.steps = steps
        self.idx = 0
        self.interval = interval
        self.backfill_days = backfill_days
        self.max_workers = max_workers
        self.housekeeping_window = housekeeping_window

        self.child = None
        self.win = Window()
        self.log = logging.getLogger("qs2_guardian")
        self.log.setLevel(logging.INFO)
        fh = logging.FileHandler("logs/qs2_guardian.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.log.addHandler(fh)

    def _child_cmd(self, workers):
        return [
            sys.executable,
            str(Path(__file__).resolve().parents[1] / "tools" / "rt_updater_with_banner.py"),
            "--db", self.db,
            "--backfill-days", str(self.backfill_days),
            "--max-workers", str(workers),
            "--interval", str(self.interval),
        ]

    def _spawn(self):
        cmd = self._child_cmd(self.steps[self.idx])
        self.log.info("▶ 启动采集子进程：%s", " ".join(map(str, cmd)))
        root = Path(__file__).resolve().parents[1]

        # 强制 UTF-8 输出解码，避免 Windows 下 GBK 报错
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        self.child = subprocess.Popen(
            cmd,
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
            env=env
        )

        def _pump():
            for line in self.child.stdout:
                s = line.rstrip("\n")
                if s:
                    print(s)
                    self.win.add(s)

        t = threading.Thread(target=_pump, daemon=True)
        t.start()

    def _kill_child(self):
        if self.child and self.child.poll() is None:
            self.log.info("⛔ 杀掉子进程 pid=%s", self.child.pid)
            try:
                self.child.terminate()
                time.sleep(2)
                if self.child.poll() is None:
                    self.child.kill()
            except Exception as e:
                self.log.error("kill 失败: %s", e)

    def run(self):
        self.log.info("🚀 QS2 采集守护启动 | DB=%s", self.db)
        self._spawn()
        stable = 0

        while True:
            time.sleep(30)
            snap = self.win.snapshot()
            now = time.time()
            recent = [s for t, s in snap if now - t < 60]

            # 检测数据库锁/限流
            bad = [s for s in recent if ("locked" in s.lower() or "429" in s)]
            if bad:
                self.log.warning("⚠ 检测到锁冲突/限流: %s", bad[-1])
                self._kill_child()
                self.idx = min(self.idx + 1, len(self.steps) - 1)
                self._spawn()
                stable = 0
                continue

            # 子进程退出自动重启
            if self.child.poll() is not None:
                self.log.warning("⚠ 子进程异常退出，重启 idx=%s", self.idx)
                self.idx = min(self.idx + 1, len(self.steps) - 1)
                self._spawn()
                stable = 0
                continue

            # 稳定一段时间 → 尝试升档
            stable += 1
            if stable > 20 and self.idx > 0:
                self.idx -= 1
                self.log.info("⬆ 并发升档 idx=%s", self.idx)
                self._kill_child()
                self._spawn()
                stable = 0


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--interval", type=int, default=30)
    p.add_argument("--backfill-days", type=int, default=365)
    p.add_argument("--scale-steps", type=str, default="12,8,6,4,3")
    p.add_argument("--max-workers", type=int, default=12)
    p.add_argument("--housekeeping-window", type=str, default="02:00-02:30")
    args = p.parse_args()

    steps = [int(x) for x in args.scale_steps.split(",")]

    g = Guardian(
        db=args.db,
        steps=steps,
        interval=args.interval,
        backfill_days=args.backfill_days,
        max_workers=args.max_workers,
        housekeeping_window=args.housekeeping_window
    )
    g.run()
