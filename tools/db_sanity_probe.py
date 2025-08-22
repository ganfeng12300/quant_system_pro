# tools/db_sanity_probe.py
import argparse, sqlite3, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True)
ap.add_argument("--table", required=True)
ap.add_argument("--n", type=int, default=5)
args = ap.parse_args()

con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute(f"PRAGMA table_info('{args.table}')")
cols = [r[1] for r in cur.fetchall()]
print("[INFO] cols:", cols)
cur.execute(f"SELECT COUNT(*) FROM '{args.table}'")
print("[INFO] rows:", cur.fetchone()[0])
df = pd.read_sql_query(f"SELECT * FROM '{args.table}' ORDER BY ts DESC LIMIT {args.n}", con)
print(df)
con.close()
