# -*- coding: utf-8 -*-
import sqlite3, os, contextlib
from rich.console import Console
console = Console()

def connect_ro(db_path):
    uri = f"file:{db_path}?mode=ro&cache=shared"
    return contextlib.closing(sqlite3.connect(uri, uri=True, timeout=60))

def connect_rw(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path, timeout=60)
    with con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
    return con

def ensure_index(con, table):
    try:
        con.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_ts ON "{table}"(ts);')
    except Exception: pass

def table_exists(con, table):
    cur = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def count_rows(con, table):
    try:
        return con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except Exception:
        return 0
