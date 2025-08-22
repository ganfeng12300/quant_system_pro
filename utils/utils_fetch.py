# -*- coding: utf-8 -*-
"""
utils_fetch.py · 机构级稳定实现（Binance Futures）
提供：
- fetch_futures_klines_smart(con, symbol, tf, start_ms, limit, session) -> list[tuple]
- save_klines_to_db(con, symbol, tf, rows)
- last_ts(con, table_name) -> Optional[int]
- upsert_rt_quote(con, symbol, qdict)

特性：
- 多HOST轮询：避免单域名阻断
- 429 限流指数退避 + 抖动
- 451 区域阻断/合规限制：自动切HOST并冷却
- JSON 解析兜底（空响应/HTML误返）
- SQLite WAL 友好：插入使用 INSERT OR IGNORE，主键 timestamp（秒）

说明：
- K线源使用 /fapi/v1/klines?symbol=...，适配主流合约（BTCUSDT, ETHUSDT...）
- 如果您的环境确实需要 continuousKlines，可在 _build_kline_url 中切换
"""

from __future__ import annotations
import time
import math
import random
import sqlite3
from typing import List, Optional, Tuple

import requests

# ===== 可按需增删的 HOST 列表（自动轮询） =====
BINANCE_HOSTS = [
    "https://fapi.binance.com",
    "https://fapi.binance.net",
    "https://fapi.binance.me",
]

# 统一的 User-Agent（有些节点会校验UA）
UA = "Mozilla/5.0 (compatible; QuantCollector/1.0; +https://example.local)"

# ---- 映射/校验 ----
_VALID_TFS = {"1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"}

def _norm_tf(tf: str) -> str:
    tf = tf.strip()
    # 允许传入 60m/120m 这类写法
    if tf.endswith("m") and tf[:-1].isdigit():
        m = int(tf[:-1])
        if m == 60:
            return "1h"
        elif m == 120:
            return "2h"
    return tf

def _build_kline_url(host: str, symbol: str, tf: str, start_ms: int, limit: int) -> str:
    """
    如需使用连续合约（continuousKlines）可切到：
    /fapi/v1/continuousKlines?pair={symbol}&contractType=PERPETUAL&interval={tf}&startTime={start_ms}&limit={limit}
    但大多数主流永续USDT合约，/klines?symbol=... 更稳定。
    """
    return (
        f"{host}/fapi/v1/klines"
        f"?symbol={symbol}"
        f"&interval={tf}"
        f"&startTime={start_ms}"
        f"&limit={limit}"
    )

# ---- DB schema ----
CREATE_OHLCV_SQL = """
CREATE TABLE IF NOT EXISTS "{table}" (
  timestamp INTEGER PRIMARY KEY,   -- 秒级Unix时间
  open      REAL,
  high      REAL,
  low       REAL,
  close     REAL,
  volume    REAL
);
"""

INSERT_OHLCV_SQL = """
INSERT OR IGNORE INTO "{table}"(timestamp, open, high, low, close, volume)
VALUES(?,?,?,?,?,?)
"""

CREATE_QUOTES_SQL = """
CREATE TABLE IF NOT EXISTS rt_quotes (
  symbol TEXT PRIMARY KEY,
  bid REAL,
  ask REAL,
  updated_at INTEGER
);
"""

UPSERT_QUOTES_SQL = """
INSERT INTO rt_quotes(symbol, bid, ask, updated_at)
VALUES(?,?,?,?)
ON CONFLICT(symbol) DO UPDATE SET
  bid=excluded.bid,
  ask=excluded.ask,
  updated_at=excluded.updated_at
"""

# =========================================================
#                     公共工具函数
# =========================================================
def last_ts(con: sqlite3.Connection, table_name: str) -> Optional[int]:
    """
    返回表中最后一根K（最大 timestamp，单位秒）。表不存在或无数据 -> None
    """
    try:
        r = con.execute(f'SELECT MAX(timestamp) FROM "{table_name}"').fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except sqlite3.OperationalError:
        return None


def save_klines_to_db(con: sqlite3.Connection, symbol: str, tf: str, rows: List[Tuple[int,float,float,float,float,float]]) -> int:
    """
    rows: 列表，每项为 (ts_sec, open, high, low, close, volume)
    自动建表 + INSERT OR IGNORE，返回成功插入的行数（可能小于 rows 长度）
    """
    table = f"{symbol}_{tf}"
    con.execute(CREATE_OHLCV_SQL.replace("{table}", table))

    # 批量插入（executemany），再提交
    cur = con.cursor()
    cur.executemany(INSERT_OHLCV_SQL.replace("{table}", table), rows)
    con.commit()
    return cur.rowcount or 0


def upsert_rt_quote(con: sqlite3.Connection, symbol: str, q: dict) -> None:
    """
    把实时行情写入 rt_quotes 表
    q 示例: {"symbol": "BTCUSDT", "bid": 68000.1, "ask": 68000.3, "updated_at": 1726822712}
    """
    con.execute(CREATE_QUOTES_SQL)
    con.execute(UPSERT_QUOTES_SQL, (symbol, float(q.get("bid", 0.0)), float(q.get("ask", 0.0)), int(q.get("updated_at", int(time.time())))))
    con.commit()


# =========================================================
#                     K线抓取核心
# =========================================================
def _http_get(session: requests.Session, url: str, timeout: float = 10.0) -> requests.Response:
    """
    统一的 GET 请求，附带UA；让上层来判断状态码。
    """
    headers = {"User-Agent": UA}
    r = session.get(url, headers=headers, timeout=timeout)
    return r


def _parse_klines_json(j) -> List[Tuple[int,float,float,float,float,float]]:
    """
    解析 /fapi/v1/klines 返回的数组
    每项格式：[ openTime, open, high, low, close, volume, closeTime, ... ]
    转成 (ts_sec, open, high, low, close, volume)
    """
    if not isinstance(j, list):
        # 非法/空返回
        return []
    out: List[Tuple[int,float,float,float,float,float]] = []
    for it in j:
        if not isinstance(it, list) or len(it) < 6:
            continue
        open_time_ms = int(it[0])
        ts = open_time_ms // 1000
        try:
            o = float(it[1]); h = float(it[2]); l = float(it[3]); c = float(it[4]); v = float(it[5])
        except Exception:
            # 有可能出现 "0" / None 之类，尝试跳过
            continue
        out.append((ts, o, h, l, c, v))
    return out


def fetch_futures_klines_smart(
    con: sqlite3.Connection,
    symbol: str,
    tf: str,
    start_ms: int,
    limit: int = 1500,
    session: Optional[requests.Session] = None,
    max_tries: int = 6,
) -> List[Tuple[int,float,float,float,float,float]]:
    """
    稳定抓取 Binance USDT 永续K线（主流合约）
    - 多HOST轮询
    - 429 指数退避
    - 451 冷却切换
    - 空返回/HTML误返 -> 视为临时失败，换HOST或退避
    失败到最后仍拿不到，抛出异常（由上层决定是否记录为 ERR）。

    返回 rows（可能为空列表，表示在时间窗口内确实无新K）
    """
    tf = _norm_tf(tf)
    if tf not in _VALID_TFS:
        raise ValueError(f"Unsupported interval: {tf}")

    sess = session or requests.Session()

    # 退避参数
    base_sleep = 0.6   # 初始退避
    jitter = 0.3       # 抖动上限
    last_http_status: Optional[int] = None
    last_error: Optional[str] = None

    # host 轮询
    host_idx = 0

    for attempt in range(1, max_tries + 1):
        host = BINANCE_HOSTS[host_idx % len(BINANCE_HOSTS)]
        host_idx += 1

        url = _build_kline_url(host, symbol, tf, start_ms, limit)

        try:
            resp = _http_get(sess, url, timeout=12.0)
            code = resp.status_code

            # 429 限流：指数退避后重试（不立即抛）
            if code == 429:
                last_http_status = code
                # 指数退避 + 抖动
                sleep_s = base_sleep * (2 ** (attempt - 1)) + random.random() * jitter
                time.sleep(min(8.0, sleep_s))
                continue

            # 451 区域限制/合规：换HOST + 冷却
            if code == 451:
                last_http_status = code
                time.sleep(1.2 + random.random()*0.8)  # 冷却
                continue

            # 其它非200：抛 HTTPError 让上层记录
            resp.raise_for_status()

            # 安全解析（有些时候会返回空字符串/HTML）
            try:
                j = resp.json()
            except Exception:
                # 解析失败：视为临时异常 -> 退避后再试
                last_error = "JSON parse error"
                time.sleep(0.8 + random.random()*0.4)
                continue

            rows = _parse_klines_json(j)
            # 返回空列表：可能是窗口内确实无新K；让上层自己判断是否继续推进窗口
            return rows

        except requests.HTTPError as he:
            # 明确的 HTTP 错误，直接抛给上层（用于在日志里标记 K-ERR）
            raise he
        except requests.RequestException as re:
            # 网络类异常：退避后重试
            last_error = f"RequestException: {str(re)}"
            time.sleep(0.8 + random.random()*0.6)
            continue
        except Exception as e:
            # 其它异常：小退避后重试
            last_error = f"Exception: {str(e)}"
            time.sleep(0.6 + random.random()*0.5)
            continue

    # 多次尝试仍失败：如果有 HTTP 状态，构造相应异常；否则抛通用异常
    if last_http_status:
        # 伪造一个 HTTPError 让上层能识别
        resp = requests.Response()
        resp.status_code = last_http_status
        http_err = requests.HTTPError(f"HTTP {last_http_status}", response=resp)
        raise http_err
    else:
        raise RuntimeError(last_error or "fetch_futures_klines_smart: exhausted attempts")
