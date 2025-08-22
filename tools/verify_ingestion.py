# -*- coding: utf-8 -*-
"""
verify_ingestion.py · 采集写入自检工具（支持单库/双库对比 & 实时监控 --watch）

功能：
1) 快速体检：rt_quotes 新鲜度、K线表覆盖度/最新K时间（按 symbol+tf）。
2) 可选双库对比：逐表对比行数与最后K时间（判断是否写入了“老库”）。
3) 额外信息：journal_mode、WAL/SHM 文件是否存在及大小。
4) 新增 --watch N：每 N 秒自动重复体检，适合“实时盯盘式”监控。

用法示例：
# 单库体检（老库）
python tools/verify_ingestion.py \
  --db "D:\quant_system_v2\data\market_data.db" \
  --symbols-file results\keep_symbols.txt \
  --tfs 5m 15m 30m 1h 2h \
  --fresh-sec 300

# 双库对比（老库 vs 测试库，只看 BAT）
python tools/verify_ingestion.py \
  --db "D:\quant_system_v2\data\market_data.db" \
  --db-b "D:\quant_system_v2\data\market_data_test.db" \
  --symbols "BATUSDT" \
  --tfs 5m 15m 30m 1h 2h

# 实时监控模式（每 30 秒刷新一次）
python tools/verify_ingestion.py \
  --db "D:\quant_system_v2\data\market_data.db" \
  --symbols-file results\keep_symbols.txt \
  --tfs 5m 15m 30m 1h 2h \
  --fresh-sec 300 --watch 30
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import time
import datetime as dt
from typing import List, Tuple, Optional, Dict


def fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "N/A"
    try:
        return dt.datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "N/A"


def tf_to_seconds(tf: str) -> Optional[int]:
    tf = tf.strip().lower()
    if tf.endswith("m") and tf[:-1].isdigit():
        return int(tf[:-1]) * 60
    if tf.endswith("h") and tf[:-1].isdigit():
        return int(tf[:-1]) * 3600
    if tf.endswith("d") and tf[:-1].isdigit():
        return int(tf[:-1]) * 86400
    return None


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


def get_symbols(con: sqlite3.Connection, symbols_file: Optional[str], symbols_csv: Optional[str], tfs: List[str]) -> List[str]:
    # 1) --symbols 显式列表
    if symbols_csv:
        return [s.strip() for s in symbols_csv.split(",") if s.strip()]
    # 2) --symbols-file 文件（每行一个）
    if symbols_file:
        with open(symbols_file, "r", encoding="utf-8") as f:
            syms = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
        return syms
    # 3) rt_quotes 表（若存在）
    if table_exists(con, "rt_quotes"):
        try:
            rows = con.execute("SELECT symbol FROM rt_quotes").fetchall()
            if rows:
                return sorted({r[0] for r in rows if r and r[0]})
        except Exception:
            pass
    # 4) 从 sqlite_master 猜（_tf 后缀）
    like_preds = [f"%_{tf}" for tf in tfs]
    cond = " OR ".join(["name LIKE ?" for _ in like_preds])
    rows = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND ({cond})", like_preds).fetchall()
    syms = set()
    for (name,) in rows:
        for tf in tfs:
            suf = f"_{tf}"
            if name.endswith(suf):
                syms.add(name[: -len(suf)])
                break
    return sorted(syms)


def stat_rt_quotes(con: sqlite3.Connection, fresh_sec: int) -> Dict[str, int]:
    out = {"rows": 0, "fresh": 0, "min_ts": None, "max_ts": None}
    if not table_exists(con, "rt_quotes"):
        return out
    try:
        out["rows"] = con.execute("SELECT COUNT(*) FROM rt_quotes").fetchone()[0] or 0
        now = int(time.time())
        out["fresh"] = con.execute("SELECT COUNT(*) FROM rt_quotes WHERE updated_at >= ?", (now - fresh_sec,)).fetchone()[0] or 0
        out["min_ts"] = con.execute("SELECT MIN(updated_at) FROM rt_quotes").fetchone()[0]
        out["max_ts"] = con.execute("SELECT MAX(updated_at) FROM rt_quotes").fetchone()[0]
    except Exception:
        pass
    return out


def stat_k_table(con: sqlite3.Connection, table: str) -> Tuple[int, Optional[int]]:
    if not table_exists(con, table):
        return 0, None
    cnt = con.execute(f"SELECT COUNT(*) FROM '{table}'").fetchone()[0] or 0
    last_ts = con.execute(f"SELECT MAX(timestamp) FROM '{table}'").fetchone()[0]
    return cnt, last_ts


def human_size(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    f = float(n)
    for u in units:
        if f < 1024:
            return f"{f:.1f}{u}"
        f /= 1024
    return f"{f:.1f}PB"


def file_info(db_path: str) -> str:
    wal = db_path + "-wal"
    shm = db_path + "-shm"
    parts = []
    if os.path.exists(db_path):
        parts.append(f"DB={human_size(os.path.getsize(db_path))}")
    else:
        parts.append("DB=missing")
    parts.append(f"WAL={'present' if os.path.exists(wal) else 'absent'}")
    parts.append(f"SHM={'present' if os.path.exists(shm) else 'absent'}")
    return ", ".join(parts)


def compare_two(con_a: sqlite3.Connection, con_b: sqlite3.Connection, symbols: List[str], tfs: List[str]) -> None:
    print("
==== 双库对比（A vs B）====")
    diff_cnt = 0
    for sym in symbols:
        for tf in tfs:
            tbl = f"{sym}_{tf}"
            cnt_a, last_a = stat_k_table(con_a, tbl)
            cnt_b, last_b = stat_k_table(con_b, tbl)
            if cnt_a != cnt_b or (last_a or 0) != (last_b or 0):
                diff_cnt += 1
                print(f"[DIFF] {tbl:<18} A: rows={cnt_a:<7} last={fmt_ts(last_a):<22} | B: rows={cnt_b:<7} last={fmt_ts(last_b)}")
    if diff_cnt == 0:
        print("两库在所选 symbols/tfs 上行数与最后K时间完全一致 ✅")


def run_once(db_a: str, db_b: Optional[str], symbols_file: Optional[str], symbols_csv: Optional[str], tfs: List[str], fresh_sec: int):
    print("="*90)
    print("DB-A:", db_a)
    print("   ", file_info(db_a))
    if db_b:
        print("DB-B:", db_b)
        print("   ", file_info(db_b))
    print("tfs:", ",".join(tfs))
    print("window fresh-sec:", fresh_sec)
    print("now:", dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    print("="*90)

    con_a = sqlite3.connect(db_a)
    con_a.row_factory = sqlite3.Row
    try:
        jm = con_a.execute("PRAGMA journal_mode").fetchone()[0]
    except Exception:
        jm = "?"
    print(f"journal_mode(A) = {jm}")

    symbols = get_symbols(con_a, symbols_file, symbols_csv, tfs)
    if not symbols:
        print("[FATAL] 没有获取到 symbols（请指定 --symbols 或 --symbols-file）")
        con_a.close()
        return
    print(f"symbols: {len(symbols)} 个（示例前10）:", ", ".join(symbols[:10]))

    # 1) rt_quotes 体检
    rq = stat_rt_quotes(con_a, fresh_sec)
    print("
---- rt_quotes 体检(A) ----")
    if rq["rows"] == 0:
        print("rt_quotes 不存在或无数据。")
    else:
        now = int(time.time())
        max_age = (now - rq["max_ts"]) if rq["max_ts"] else None
        print(f"rows={rq['rows']}  fresh(<= {fresh_sec}s)={rq['fresh']}  min={fmt_ts(rq['min_ts'])}  max={fmt_ts(rq['max_ts'])}")
        if max_age is not None:
            print(f"最新一条延迟：{max_age}s")
        print("示例TOP10(最近更新)：")
        try:
            rows = con_a.execute("SELECT symbol, updated_at FROM rt_quotes ORDER BY updated_at DESC LIMIT 10").fetchall()
            for s, t in rows:
                print(f"  {s:<12} {fmt_ts(t)}")
        except Exception as e:
            print("  [WARN] 无法读取 rt_quotes TOP10:", e)

    # 2) K线覆盖/新鲜度
    print("
---- K线覆盖(A) ----")
    total_tables = 0
    missing_tables = 0
    stale_items = []  # (staleness_sec, tbl, last_ts, cnt)
    for sym in symbols:
        for tf in tfs:
            tbl = f"{sym}_{tf}"
            cnt, last_ts = stat_k_table(con_a, tbl)
            if cnt == 0 and last_ts is None:
                missing_tables += 1
                print(f"[MISS] {tbl}")
            else:
                total_tables += 1
                if last_ts is not None:
                    age = int(time.time()) - int(last_ts)
                    stale_items.append((age, tbl, last_ts, cnt))

    print(f"存在的K线表：{total_tables}  缺失表：{missing_tables}")
    if stale_items:
        stale_items.sort(reverse=True)  # 延迟大的在前
        print("最滞后的前10张表（按延迟秒）：")
        for age, tbl, last_ts, cnt in stale_items[:10]:
            print(f"  {tbl:<18} age={age:>6}s  last={fmt_ts(last_ts):<22} rows={cnt}")

    # 3) 双库对比（可选）
    if db_b:
        con_b = sqlite3.connect(db_b)
        con_b.row_factory = sqlite3.Row
        try:
            jm_b = con_b.execute("PRAGMA journal_mode").fetchone()[0]
        except Exception:
            jm_b = "?"
        print(f"
journal_mode(B) = {jm_b}")
        compare_two(con_a, con_b, symbols, tfs)
        con_b.close()

    con_a.close()


def main():
    ap = argparse.ArgumentParser(description="采集写入自检（单库/双库对比 + 实时监控）")
    ap.add_argument("--db", required=True, help="主库（通常是老库）")
    ap.add_argument("--db-b", default=None, help="对比库（可选，例如测试库）")
    ap.add_argument("--symbols-file", default=None, help="符号文件（每行一个）")
    ap.add_argument("--symbols", default=None, help="逗号分隔的符号列表，如 BTCUSDT,ETHUSDT")
    ap.add_argument("--tfs", nargs="+", default=["5m","15m","30m","1h","2h"], help="周期列表")
    ap.add_argument("--fresh-sec", type=int, default=300, help="rt_quotes 判断新鲜的时间窗口（秒）")
    ap.add_argument("--watch", type=int, default=0, help="每隔 N 秒重复体检（0=只运行一次）")
    args = ap.parse_args()

    if args.watch <= 0:
        run_once(os.path.abspath(args.db), os.path.abspath(args.db_b) if args.db_b else None,
                 args.symbols_file, args.symbols, args.tfs, args.fresh_sec)
        return

    # 实时监控模式
    print(f"进入实时监控模式：每 {args.watch}s 刷新一次。Ctrl+C 结束。
")
    try:
        while True:
            # 清屏（Windows: cls / *nix: clear）
            try:
                os.system("cls" if os.name == "nt" else "clear")
            except Exception:
                pass
            run_once(os.path.abspath(args.db), os.path.abspath(args.db_b) if args.db_b else None,
                     args.symbols_file, args.symbols, args.tfs, args.fresh_sec)
            time.sleep(args.watch)
    except KeyboardInterrupt:
        print("
已退出实时监控模式。")


if __name__ == "__main__":
    main()
