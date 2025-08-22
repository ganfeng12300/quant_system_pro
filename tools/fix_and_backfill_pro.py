# -*- coding: utf-8 -*-
"""
并行版一键补齐缺口脚本 · 最终修正版（机构级）
路径：tools/fix_and_backfill_pro.py

功能：
1. 自动识别目录，调用 binance_data_collector.py。
2. 检测数据库覆盖度，找出不达标表。
3. 12 并发补齐缺口，带总进度条。
4. 自动重试 3 次，最大限度保证成功。
"""

import argparse
import os
import sqlite3
import datetime as dt
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

console = Console()

# === 配置 ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = r"D:\quant_system_v2\data\market_data.db"
COLLECTOR = os.path.join(BASE_DIR, "tools", "binance_data_collector.py")
TARGET_DAYS = 365
FULL_THRESHOLD = 0.98
MAX_WORKERS = 12
RETRY_TIMES = 3


def check_tables(db_path, target_days=365, threshold=0.98):
    """检查数据库中所有表的覆盖度"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall() if "_" in r[0]]

    results = []
    for tbl in tables:
        try:
            cur.execute(f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM {tbl}")
            row = cur.fetchone()
            if not row or row[0] is None:
                continue
            min_ts, max_ts, cnt = row
            min_dt = dt.datetime.utcfromtimestamp(min_ts/1000)
            max_dt = dt.datetime.utcfromtimestamp(max_ts/1000)
            days = (max_dt - min_dt).days + 1
            coverage = days / target_days
            ok = coverage >= threshold
            parts = tbl.split("_")
            results.append({
                "表名": tbl,
                "symbol": parts[0],
                "tf": parts[1],
                "覆盖天数": days,
                "是否达标": ok,
                "行数": cnt
            })
        except Exception as e:
            console.print(f"[yellow]⚠️ 表 {tbl} 出错: {e}[/]")
    conn.close()
    return results


def run_backfill(symbol, timeframe, days=365):
    """执行单个回补任务，带重试"""
    for attempt in range(1, RETRY_TIMES + 1):
        cmd = [
            "python", COLLECTOR,
            "--db", DB_PATH,
            "--symbol", symbol,
            "--timeframes", timeframe,
            "--days", str(days)
        ]
        try:
            res = subprocess.run(" ".join(cmd), shell=True, capture_output=True, text=True)
            if res.returncode == 0:
                return 0, symbol, timeframe, f"成功 (尝试{attempt})"
            else:
                console.print(f"[red]✘ {symbol}_{timeframe} 第{attempt}次失败: {res.stderr.strip() or res.stdout.strip()}[/]")
        except Exception as e:
            console.print(f"[red]任务异常 {symbol}_{timeframe} 第{attempt}次: {e}[/]")
    return 1, symbol, timeframe, f"最终失败（已重试 {RETRY_TIMES} 次）"


def main():
    parser = argparse.ArgumentParser(description="并行一键补齐缺口脚本（最终修正版）")
    parser.add_argument("--db", default=DB_PATH, help="数据库路径")
    parser.add_argument("--days", type=int, default=TARGET_DAYS, help="目标天数")
    args = parser.parse_args()

    console.print("[bold blue]📊 第一步：检测数据库覆盖度...[/]")
    results = check_tables(args.db, args.days, FULL_THRESHOLD)

    # 输出初步报告
    table = Table(title="覆盖度报告（检测前）", expand=True)
    table.add_column("表名")
    table.add_column("覆盖天数")
    table.add_column("是否达标")
    table.add_column("行数")
    for r in results:
        color = "green" if r["是否达标"] else "red"
        table.add_row(r["表名"], str(r["覆盖天数"]), f"[{color}]{r['是否达标']}[/{color}]", str(r["行数"]))
    console.print(table)

    # 找出不达标表
    bad = [r for r in results if not r["是否达标"]]
    if not bad:
        console.print("[bold green]🎉 所有表都达标，无需回补！[/]")
        return

    console.print(f"[bold yellow]⚠️ 发现 {len(bad)} 个不达标表，开始并行回补（{MAX_WORKERS} 并发，每表最多重试 {RETRY_TIMES} 次）...[/]")

    # 并行回补
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("回补进度", total=len(bad))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(run_backfill, r["symbol"], r["tf"], args.days) for r in bad]
            for future in as_completed(futures):
                code, sym, tf, msg = future.result()
                if code == 0:
                    console.print(f"[green]✔ {sym}_{tf} {msg}[/]")
                else:
                    console.print(f"[red]✘ {sym}_{tf} {msg}[/]")
                progress.advance(task)

    console.print("[bold blue]📊 第二步：回补完成后重新检测...[/]")
    results2 = check_tables(args.db, args.days, FULL_THRESHOLD)

    # 输出最终报告
    table2 = Table(title="覆盖度报告（检测后）", expand=True)
    table2.add_column("表名")
    table2.add_column("覆盖天数")
    table2.add_column("是否达标")
    table2.add_column("行数")
    for r in results2:
        color = "green" if r["是否达标"] else "red"
        table2.add_row(r["表名"], str(r["覆盖天数"]), f"[{color}]{r['是否达标']}[/{color}]", str(r["行数"]))
    console.print(table2)


if __name__ == "__main__":
    main()
