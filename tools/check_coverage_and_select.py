# -*- coding: utf-8 -*-
"""
check_coverage_and_select.py
扫描 SQLite 库中符号_周期表的采集覆盖度，输出报告与“保留/剔除”清单。
- 不删除任何数据，只生成 CSV 报告 + 两份符号清单文本
- 默认评估近 N 天应有的 K 数量，并计算 coverage=rows/expected
- 还会检测“是否新鲜”（最后一根K是否接近当前时间，避免长时间断更）
用法示例：
  python tools\check_coverage_and_select.py --db D:\quant_system_v2\data\market_data.db --days 365 --min-coverage 0.90 --min-rows 1000 --freshness-k 3 --tfs 5m 15m 30m 1h 2h 4h 1d
"""

import argparse, os, re, sqlite3, sys, time, math, csv
from datetime import datetime, timezone

TF_SEC = {
    "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
    "2h": 7200, "4h": 14400, "1d": 86400
}

TABLE_PAT = re.compile(r"^([A-Z0-9]+)_(5m|15m|30m|1h|2h|4h|1d)$", re.I)

def ts_to_iso(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ""

def list_symbol_tf_tables(con, wanted_tfs):
    """返回[(symbol, tf, table_name)]"""
    rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_%'").fetchall()
    out = []
    for (name,) in rows:
        m = TABLE_PAT.match(name)
        if not m:
            continue
        sym = m.group(1).upper()
        tf = m.group(2)
        if wanted_tfs and tf not in wanted_tfs:
            continue
        out.append((sym, tf, name))
    return out

def table_stats(con, table):
    """返回 rows, min_ts, max_ts（秒）"""
    q = f'SELECT COUNT(1), MIN(timestamp), MAX(timestamp) FROM "{table}"'
    c, mn, mx = con.execute(q).fetchone()
    return int(c or 0), (int(mn) if mn is not None else None), (int(mx) if mx is not None else None)

def expected_bars(days, tf):
    return int(math.floor(days*86400 / TF_SEC[tf]))

def is_fresh(last_ts, tf, freshness_k=3):
    """最后一根K是否新鲜：距离现在不超过 freshness_k 根K"""
    if last_ts is None:
        return False
    now_s = int(time.time())
    return (now_s - last_ts) <= freshness_k * TF_SEC[tf]

def main():
    ap = argparse.ArgumentParser(description="DB 覆盖度检测与符号筛选")
    ap.add_argument("--db", required=True, help="SQLite DB 路径")
    ap.add_argument("--days", type=int, default=365, help="覆盖评估天数（默认 365）")
    ap.add_argument("--tfs", nargs="*", default=["5m","15m","30m","1h","2h","4h","1d"], help="参与评估的周期")
    ap.add_argument("--min-coverage", type=float, default=0.90, help="最小覆盖率阈值，例如 0.90（默认）")
    ap.add_argument("--min-rows", type=int, default=1000, help="最少K数下限（低于则视为不合格，默认 1000）")
    ap.add_argument("--freshness-k", type=int, default=3, help="新鲜度：最后 ≤N 根K 内（默认 3）")
    ap.add_argument("--require-all-tfs", action="store_true",
                    help="为 True 时，币种只有当所选全部周期都达标才保留；默认 False=任一周期达标即保留")
    ap.add_argument("--outdir", default="results", help="输出目录（默认 results）")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    report_csv = os.path.join(args.outdir, "coverage_report.csv")
    keep_txt   = os.path.join(args.outdir, "keep_symbols.txt")
    drop_txt   = os.path.join(args.outdir, "drop_symbols.txt")

    con = sqlite3.connect(args.db, timeout=60)
    tables = list_symbol_tf_tables(con, set(args.tfs))
    if not tables:
        print(f"[FATAL] 未找到符合命名规则的表（SYMBOL_TF），或所选周期为空：{args.tfs}")
        sys.exit(2)

    # 统计
    rows_out = []
    sym_ok_map = {}   # symbol -> 每个 tf 的达标布尔
    sym_seen_tfs = {} # symbol -> 见到过的 tf 集合

    for sym, tf, table in sorted(tables):
        c, mn, mx = table_stats(con, table)
        exp = expected_bars(args.days, tf)
        cov = (c/exp) if exp > 0 else 0.0
        fresh = is_fresh(mx, tf, args.freshness_k)

        status = []
        if c < args.min_rows:
            status.append("TOO_FEW_ROWS")
        if cov < args.min_coverage:
            status.append("LOW_COVERAGE")
        if not fresh:
            status.append("STALE")

        ok = (c >= args.min_rows) and (cov >= args.min_coverage) and fresh
        rows_out.append({
            "symbol": sym,
            "timeframe": tf,
            "rows": c,
            "first_ts": mn or "",
            "last_ts": mx or "",
            "first_iso": ts_to_iso(mn),
            "last_iso": ts_to_iso(mx),
            "expected": exp,
            "coverage": round(cov, 4),
            "fresh": int(bool(fresh)),
            "ok": int(bool(ok)),
            "status": "|".join(status) if status else "OK",
        })

        sym_seen_tfs.setdefault(sym, set()).add(tf)
        sym_ok_map.setdefault(sym, {})[tf] = ok

    con.close()

    # 写 CSV 报告
    headers = ["symbol","timeframe","rows","first_ts","last_ts","first_iso","last_iso",
               "expected","coverage","fresh","ok","status"]
    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    # 生成保留/剔除清单
    keep_syms, drop_syms = set(), set()
    for sym, tf_map in sym_ok_map.items():
        seen_tfs = sym_seen_tfs.get(sym, set())
        ok_tfs = [tf for tf, ok in tf_map.items() if ok]
        if args.require_all_tfs:
            # 所有出现的周期都要 OK
            all_ok = all(tf_map.get(tf, False) for tf in seen_tfs)
            (keep_syms if all_ok else drop_syms).add(sym)
        else:
            # 任一周期 OK 即保留
            (keep_syms if len(ok_tfs) > 0 else drop_syms).add(sym)

    with open(keep_txt, "w", encoding="utf-8") as f:
        for s in sorted(keep_syms): f.write(s + "\n")
    with open(drop_txt, "w", encoding="utf-8") as f:
        for s in sorted(drop_syms): f.write(s + "\n")

    # 终端摘要
    print(f"▶ 覆盖报告：{report_csv}")
    print(f"▶ 保留清单：{keep_txt} （{len(keep_syms)} 个）")
    print(f"▶ 剔除清单：{drop_txt} （{len(drop_syms)} 个）")
    print("说明：不改动数据库。如需在采集/回测中排除，启动时传 --symbols-file 使用 keep/drop 清单即可。")

if __name__ == "__main__":
    main()
