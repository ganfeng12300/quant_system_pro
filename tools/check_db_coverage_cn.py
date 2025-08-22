# -*- coding: utf-8 -*-
"""
数据库覆盖度检测器 · 机构级容错版
路径：tools/check_db_coverage_cn.py

功能：
1. 检查数据库中每个 (symbol, timeframe) 的覆盖天数与完整率。
2. 自动判定是否达到设定阈值（如 98%）。
3. CSV 写出时动态生成表头（不会因新增字段报错）。
4. 终端彩色输出，便于快速查看。

作者：甘总专属机构级版本
"""

import argparse
import sqlite3
import pandas as pd
import datetime as dt
import csv
import os
from rich.console import Console
from rich.table import Table

console = Console()

def check_coverage(db_path, target_days=365, full_threshold=0.98, out_csv="db_coverage_report.csv"):
    if not os.path.exists(db_path):
        console.print(f"[red]数据库不存在: {db_path}[/]")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 获取所有表名
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
            ok = coverage >= full_threshold
            results.append({
                "表名": tbl,
                "起始日期": min_dt.strftime("%Y-%m-%d"),
                "结束日期": max_dt.strftime("%Y-%m-%d"),
                "覆盖天数": days,
                "目标天数": target_days,
                "达标率(%)": f"{coverage*100:.1f}",
                "是否达标": "✅" if ok else "❌",
                "总行数": cnt
            })
        except Exception as e:
            console.print(f"[yellow]警告: 读取表 {tbl} 出错: {e}[/]")

    conn.close()

    if not results:
        console.print("[red]未找到任何交易对表，请检查数据库。[/]")
        return

    # === 1) CSV 动态写表头 ===
    out_path = os.path.join(os.getcwd(), out_csv)
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    # === 2) 终端彩色表格输出 ===
    table = Table(title="📊 数据库覆盖度检测结果", expand=True)
    for col in results[0].keys():
        style = "green" if col == "是否达标" else "cyan"
        table.add_column(col, style=style, justify="center")

    for r in results:
        row = [str(v) for v in r.values()]
        table.add_row(*row)

    console.print(table)
    console.print(f"[bold blue]已保存 CSV 报告: {out_path}[/]")


def main():
    parser = argparse.ArgumentParser(description="数据库覆盖度检测器（机构级容错版）")
    parser.add_argument("--db", required=True, help="数据库路径")
    parser.add_argument("--target-days", type=int, default=365, help="目标天数")
    parser.add_argument("--full-threshold", type=float, default=0.98, help="完整阈值 (0-1)")
    parser.add_argument("--out", default="db_coverage_report.csv", help="输出 CSV 文件名")
    args = parser.parse_args()

    check_coverage(args.db, args.target_days, args.full_threshold, args.out)


if __name__ == "__main__":
    main()
