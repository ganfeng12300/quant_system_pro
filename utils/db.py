# utils/db.py
# S-grade SQLite helper with WAL + per-thread connections
from __future__ import annotations
import sqlite3, threading, time
from contextlib import contextmanager

class SQLite:
    def __init__(self, path: str, timeout: float = 30.0, pragmas: dict | None = None):
        self.path = path
        self.timeout = timeout
        self._local = threading.local()
        self.pragmas = pragmas or {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "temp_store": "MEMORY",
            "cache_size": -20000,  # ~20MB page cache
        }

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=self.timeout, check_same_thread=False)
        con.row_factory = sqlite3.Row
        for k, v in self.pragmas.items():
            con.execute(f"PRAGMA {k}={v};")
        return con

    @property
    def con(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = self._connect()
            self._local.con = con
        return con

    @contextmanager
    def cursor(self):
        cur = self.con.cursor()
        try:
            yield cur
            self.con.commit()
        finally:
            cur.close()

    def exec(self, sql: str, params: tuple = ()):
        with self.cursor() as cur:
            cur.execute(sql, params)

    def query(self, sql: str, params: tuple = ()):
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    def ensure_ohlcv_table(self, table: str):
        self.exec(f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            ts INTEGER NOT NULL PRIMARY KEY,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low  REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL
        );
        """)
        self.exec(f'CREATE INDEX IF NOT EXISTS "idx_{table}_ts" ON "{table}"(ts);')

    def upsert_ohlcv(self, table: str, rows: list[tuple]):
        if not rows: return
        self.ensure_ohlcv_table(table)
        with self.cursor() as cur:
            cur.executemany(
                f'INSERT INTO "{table}" (ts,open,high,low,close,volume) VALUES (?,?,?,?,?,?) '
                f'ON CONFLICT(ts) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume;',
                rows,
            )

    def ensure_ticks_table(self):
        self.exec("""
        CREATE TABLE IF NOT EXISTS ticks (
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            price REAL NOT NULL,
            PRIMARY KEY(symbol, ts)
        );
        """)
        self.exec('CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);')

    def upsert_tick(self, symbol: str, ts: int, price: float):
        self.ensure_ticks_table()
        self.exec("INSERT OR REPLACE INTO ticks(symbol, ts, price) VALUES (?, ?, ?)", (symbol, ts, price))
