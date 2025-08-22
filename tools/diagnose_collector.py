# -*- coding: utf-8 -*-
"""
diagnose_collector.py — 采集一键诊断（Windows/PowerShell/CMD 友好，纯标准库）

用法（PowerShell 或 CMD 都可）：
  cd "D:\quant_system_pro (3)\quant_system_pro"
  python tools\diagnose_collector.py ^
    --db "D:\quant_system_v2\data\market_data.db" ^
    --lookback-hours 6 ^
    --skiplist-out "D:\quant_system_pro (3)\quant_system_pro\启动命令\skiplist.txt"

参数：
  --root            项目根目录（默认：脚本的上两级目录或当前工作目录）
  --db              必填，SQLite 主库路径
  --lookback-hours  近多少小时统计写入（默认 6）
  --skiplist-out    skiplist 输出路径（默认：根目录下 skiplist.txt）
  --sample-n        抽查表数量（默认 3）
  --skiplist-days   判定零写入的天数（默认 30）
"""
import argparse
import datetime as dt
import locale
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

SYS_ENC = locale.getpreferredencoding(False) or "utf-8"

def run(cmd, timeout=8):
    """运行外部命令，返回 (exitcode, stdout_text)。容忍编码问题。"""
    try:
        cp = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=timeout, shell=True
        )
        out = cp.stdout.decode(SYS_ENC, errors="ignore") if cp.stdout else ""
        return cp.returncode, out.strip()
    except Exception as e:
        return 1, f"[ERROR] {e}"

def check_rt_updater():
    """尝试用 PowerShell 查询 rt_updater 进程；退化到 tasklist 过滤。"""
    # PowerShell 版
    ps = (
        r"powershell -NoProfile -Command "
        r"\"$p=Get-CimInstance Win32_Process | "
        r"?{ $_.Name -eq 'python.exe' -and $_.CommandLine -match 'rt_updater' } | "
        r"Select-Object ProcessId,CommandLine | Format-Table -Auto | Out-String; "
        r"Write-Output $p\""
    )
    code, out = run(ps, timeout=6)
    if out.strip():
        return out

    # 任务列表退化
    code, out = run("tasklist /v", timeout=6)
    lines = []
    if out:
        for line in out.splitlines():
            low = line.lower()
            if "python.exe" in low and "rt_updater" in low:
                lines.append(line)
    return "\n".join(lines) if lines else "[WARN] 未发现包含 rt_updater 的进程"

def file_info(p: Path):
    if not p.exists():
        return f"[MISS] {p}"
    sz = f"{p.stat().st_size/1024/1024:.2f} MB"
    mt = dt.datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    return f"{p.name:30s}  mtime={mt}  size={sz}"

def list_tables(con):
    cur = con.cursor()
    return [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%[_]%'"
    )]

def recent_write_stats(db_path: Path, lookback_hours=6):
    now = int(time.time())
    cut_ms = (now - lookback_hours*3600) * 1000
    con = sqlite3.connect(str(db_path), timeout=30)
    cur = con.cursor()
    tbls = list_tables(con)
    def latest(t):
        try:
            v = cur.execute(f"SELECT MAX(timestamp) FROM '{t}'").fetchone()[0]
            return v or 0
        except Exception:
            return 0
    ok = 0
    for t in tbls:
        if latest(t) >= cut_ms:
            ok += 1
    con.close()
    return ok, len(tbls)

def sample_last_rows(db_path: Path, sample_n=3):
    con = sqlite3.connect(str(db_path), timeout=30)
    cur = con.cursor()
    # 优先抽 _1h；没有就抽任意表
    all_1h = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_1h'"
    )]
    source = all_1h if all_1h else [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%[_]%'"
    )]
    random.shuffle(source)
    sample = source[:max(1, sample_n)]
    out = []
    for t in sample:
        try:
            rows = cur.execute(
                f"SELECT timestamp FROM '{t}' ORDER BY timestamp DESC LIMIT 3"
            ).fetchall()
            human = [
                dt.datetime.utcfromtimestamp(r[0]/1000).strftime("%Y-%m-%d %H:%M:%S")
                for r in rows
            ]
            out.append((t, human))
        except Exception as e:
            out.append((t, [f"ERROR {e}"]))
    con.close()
    return out

def build_skiplist(db_path: Path, days=30, out_path: Path=None):
    con = sqlite3.connect(str(db_path), timeout=30)
    cur = con.cursor()
    tbls = list_tables(con)
    cut_ms = (int(time.time()) - days*86400) * 1000
    bad = []
    for t in tbls:
        try:
            hit = cur.execute(
                f"SELECT 1 FROM '{t}' WHERE timestamp>=? LIMIT 1",
                (cut_ms,)
            ).fetchone()
            if hit is None:
                bad.append(t)
        except Exception:
            bad.append(t)
    con.close()
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(sorted(bad)), encoding="utf-8")
    return bad

def pragma_quickcheck(db_path: Path):
    try:
        con = sqlite3.connect(str(db_path), timeout=10)
        cur = con.cursor()
        jm = cur.execute("PRAGMA journal_mode").fetchone()
        lc = cur.execute("PRAGMA locking_mode").fetchone()
        ps = cur.execute("PRAGMA page_size").fetchone()
        cur.execute("PRAGMA wal_checkpoint(PASSIVE)")
        con.commit()
        con.close()
        return f"journal_mode={jm}  locking_mode={lc}  page_size={ps}  checkpoint=OK"
    except Exception as e:
        return f"[WARN] PRAGMA/连接测试异常: {e}"

def disk_free_of(path: Path):
    try:
        total, used, free = shutil.disk_usage(path.drive if path.drive else path.anchor)
        def fmt(x): return f"{x/1024/1024/1024:.2f} GB"
        return f"disk free={fmt(free)} / total={fmt(total)}"
    except Exception as e:
        return f"[WARN] 磁盘查询异常: {e}"

def main():
    parser = argparse.ArgumentParser(description="采集一键诊断")
    default_root = Path(__file__).resolve().parents[2] if len(Path(__file__).parts) >= 2 else Path.cwd()
    parser.add_argument("--root", default=str(default_root), help="项目根目录")
    parser.add_argument("--db", required=True, help="SQLite 主库路径")
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--skiplist-out", default=None, help="skiplist 输出路径")
    parser.add_argument("--sample-n", type=int, default=3)
    parser.add_argument("--skiplist-days", type=int, default=30)
    args = parser.parse_args()

    root = Path(args.root)
    db_path = Path(args.db)
    report = root / f"diagnose_report_{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    skip_out = Path(args.skiplist_out) if args.skiplist_out else (root / "skiplist.txt")

    lines = []
    def w(x=""): lines.append(x)

    w("="*64)
    w("采集一键诊断 · diagnose_collector.py")
    w("="*64)
    w(f"ROOT     : {root}")
    w(f"DB       : {db_path}")
    w(f"TIME     : {dt.datetime.now():%Y-%m-%d %H:%M:%S}")
    w("-"*64)

    # 1) 进程
    w("[1/8] 采集器进程（rt_updater）")
    w(check_rt_updater())
    w("")

    # 2) DB/WAL/SHM
    w("[2/8] 数据库文件信息")
    w(file_info(db_path))
    w(file_info(Path(str(db_path) + ".wal")))
    w(file_info(Path(str(db_path) + ".shm")))
    w("")

    # 3) 近 N 小时写入
    w(f"[3/8] 近{args.lookback_hours}小时是否有写入")
    if not db_path.exists():
        w("[ERROR] DB 文件不存在，无法统计")
    else:
        ok, total = recent_write_stats(db_path, args.lookback_hours)
        w(f"近{args.lookback_hours}小时有写入的表：{ok}/{total}  ({(ok/max(1,total))*100:.2f}%)")
    w("")

    # 4) 抽查最近 3 条
    w(f"[4/8] 抽查 {args.sample_n} 张 _1h 表最近3条（UTC）")
    if db_path.exists():
        for t, human in sample_last_rows(db_path, args.sample_n):
            w(f"  {t:30s} -> {human}")
    w("")

    # 5) 生成 skiplist
    w(f"[5/8] 生成 skiplist（近{args.skiplist_days}天零写入/空表） -> {skip_out}")
    if db_path.exists():
        bad = build_skiplist(db_path, args.skiplist_days, skip_out)
        w(f"  条目数：{len(bad)}")
    w("")

    # 6) PRAGMA/WAL 检
    w("[6/8] PRAGMA 快检")
    if db_path.exists():
        w("  " + pragma_quickcheck(db_path))
    w("")

    # 7) 磁盘空间
    w("[7/8] 数据盘空间")
    w("  " + disk_free_of(db_path))
    w("")

    # 8) 建议动作
    w("[8/8] 建议动作")
    w("  • [3/8] 接近 0 → 采集没写库：重启采集器，或降低 --max-workers（8→6→4）")
    w("  • [5/8] 数量很大 → 属于不可补表：进度展示时加入 --exclude-file skiplist.txt")
    w("  • 频繁 429/418 → 并发降档；稳定后再升")
    w("  • database is locked → 并发降到 4；低峰做一次 wal_checkpoint(TRUNCATE)+VACUUM")
    w("-"*64)

    report.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] 诊断完成：{report}")
    print("\n".join(lines[-12:]))

if __name__ == "__main__":
    main()
