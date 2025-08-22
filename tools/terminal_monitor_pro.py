# -*- coding: utf-8 -*-
"""
终端实时监控器 · S级机构版
彩色输出策略状态 / 持仓 / 收益
路径：tools/terminal_monitor_pro.py
"""

import sqlite3
import time
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.align import Align

# === 固定数据库路径 ===
DB_PATH = r"D:\quant_system_v2\data\market_data.db"

# 监控刷新间隔（秒）
REFRESH_INTERVAL = 5

# 初始化控制台
console = Console()

def fetch_positions(db_path):
    """从数据库读取最新持仓与收益信息"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 确保表存在（paper/live trader 会实时写入）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_positions (
            symbol TEXT,
            timeframe TEXT,
            strategy TEXT,
            position REAL,
            entry_price REAL,
            current_price REAL,
            pnl REAL,
            cum_pnl REAL,
            win_rate REAL,
            drawdown REAL,
            updated_at TEXT
        )
    """)
    conn.commit()

    cur.execute("SELECT * FROM paper_positions ORDER BY cum_pnl DESC")
    rows = cur.fetchall()
    conn.close()
    return rows

def render_table(rows):
    """渲染彩色表格"""
    table = Table(title="📊 实时交易监控 · 纸面实盘", expand=True)

    table.add_column("币种", justify="center", style="bold cyan")
    table.add_column("周期", justify="center", style="cyan")
    table.add_column("策略", justify="center", style="magenta")
    table.add_column("仓位", justify="center")
    table.add_column("入场价", justify="right")
    table.add_column("现价", justify="right")
    table.add_column("单笔PnL", justify="right")
    table.add_column("累计PnL", justify="right")
    table.add_column("胜率", justify="center")
    table.add_column("回撤", justify="center")
    table.add_column("更新时间", justify="center")

    total_pnl = 0
    for r in rows:
        pnl = r["pnl"] or 0
        cum_pnl = r["cum_pnl"] or 0
        total_pnl += cum_pnl

        pnl_color = "green" if pnl >= 0 else "red"
        cum_color = "bold green" if cum_pnl >= 0 else "bold red"
        pos_str = f"{r['position']:.2f}" if r["position"] else "-"

        table.add_row(
            r["symbol"],
            r["timeframe"],
            r["strategy"],
            pos_str,
            f"{r['entry_price']:.2f}" if r["entry_price"] else "-",
            f"{r['current_price']:.2f}" if r["current_price"] else "-",
            f"[{pnl_color}]{pnl:.2f}[/{pnl_color}]",
            f"[{cum_color}]{cum_pnl:.2f}[/{cum_color}]",
            f"{(r['win_rate'] or 0)*100:.1f}%",
            f"{(r['drawdown'] or 0)*100:.1f}%",
            r["updated_at"] or "-"
        )

    summary = Panel(
        Align.center(f"💰 总账户累计收益: [bold {'green' if total_pnl>=0 else 'red'}]{total_pnl:.2f} USDT[/]", vertical="middle"),
        style="bold blue"
    )
    return table, summary

def run_monitor():
    with Live(console=console, refresh_per_second=2) as live:
        while True:
            try:
                rows = fetch_positions(DB_PATH)
                table, summary = render_table(rows)

                layout = Table.grid(expand=True)
                layout.add_row(table)
                layout.add_row(summary)

                live.update(Panel(layout, title="🚀 战情中心 · 实时监控", border_style="bold yellow"))
            except Exception as e:
                console.print(f"[red]监控异常: {e}[/]")
            time.sleep(REFRESH_INTERVAL)

if __name__ == "__main__":
    console.print("[bold green]启动实时终端监控器...[/]")
    run_monitor()
