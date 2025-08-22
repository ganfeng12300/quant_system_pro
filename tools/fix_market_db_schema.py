import argparse, sqlite3, os, datetime, sys

REQUIRED_COLS = ["ts","open","high","low","close","volume"]
SCHEMA_TMPL = '''CREATE TABLE IF NOT EXISTS "{tbl}" (
  ts     INTEGER NOT NULL,
  open   REAL    NOT NULL,
  high   REAL    NOT NULL,
  low    REAL    NOT NULL,
  close  REAL    NOT NULL,
  volume REAL    NOT NULL,
  PRIMARY KEY (ts)
);'''

def table_exists(c, tbl):
    return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (tbl,)).fetchone() is not None

def list_cols(c, tbl):
    return {row[1] for row in c.execute(f"PRAGMA table_info('{tbl}')")}

def ensure_pragmas(conn):
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--symbols-file", required=True)
    ap.add_argument("--tfs", nargs="*", default=["5m","15m","30m","1h","2h","4h","1d"])
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"[FATAL] DB 不存在: {args.db}", file=sys.stderr); sys.exit(2)
    if not os.path.exists(args.symbols_file):
        print(f"[FATAL] 符号清单不存在: {args.symbols_file}", file=sys.stderr); sys.exit(2)

    with open(args.symbols_file, "r", encoding="utf-8", errors="ignore") as f:
        symbols = [s.strip() for s in f if s.strip()]

    conn = sqlite3.connect(args.db)
    c = conn.cursor()
    ensure_pragmas(conn)

    ts_tag = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    created = rebuilt = ok = 0

    for sym in symbols:
        for tf in args.tfs:
            tbl = f"{sym}_{tf}"
            if not table_exists(c, tbl):
                c.executescript(SCHEMA_TMPL.format(tbl=tbl))
                print(f"[CREATE] {tbl}")
                created += 1
                continue
            cols = list_cols(c, tbl)
            missing = [x for x in REQUIRED_COLS if x not in cols]
            if missing:
                bak = f"{tbl}_bak_{ts_tag}"
                c.execute(f'ALTER TABLE "{tbl}" RENAME TO "{bak}";')
                c.executescript(SCHEMA_TMPL.format(tbl=tbl))
                print(f"[REBUILD] {tbl}  (missing: {','.join(missing)})  -> backup: {bak}")
                rebuilt += 1
            else:
                ok += 1

    conn.commit()
    conn.close()
    print(f"[DONE] OK={ok}  CREATE={created}  REBUILD={rebuilt}")

if __name__ == "__main__":
    main()
