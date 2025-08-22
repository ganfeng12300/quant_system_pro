@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION
title [采集] Bitget 永续 - 检测币种 + 过滤 + 1年回补 + 实时采集

set "QS_ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "DB_PATH=D:\quant_system_v2\data\market_data.db"
set "EXCHANGE=bitget"
cd /d "%QS_ROOT%"

echo [INFO] 工程: %CD%
echo [INFO] DB: %DB_PATH%
echo [INFO] EX: %EXCHANGE%
echo.

REM 0) 保底：确保包&临时PYTHONPATH
if not exist utils\__init__.py type NUL > utils\__init__.py
if not exist backtest\__init__.py type NUL > backtest\__init__.py
set "PYTHONPATH=%QS_ROOT%;%PYTHONPATH%"

REM 1) 枚举数据库币种
if exist list_db_symbols.py (
  echo [1/4] 枚举数据库里已有币种...
  python list_db_symbols.py --db "%DB_PATH%"
) else (
  echo [WARN] 缺少 list_db_symbols.py，继续尝试使用历史白名单。
)

REM 2) 生成 Bitget 永续可用白名单（写一个临时 Python 脚本再执行；更稳）
mkdir tmp 2>nul
>tmp\gen_bitget_perp.py echo import ccxt,os,re,sys
>>tmp\gen_bitget_perp.py echo inp=r"results\symbols_from_db.txt"; outp=r"results\symbols_bitget_perp.txt"
>>tmp\gen_bitget_perp.py echo ms=open(inp,"r",encoding="utf-8").read().splitlines() if os.path.exists(inp) else []
>>tmp\gen_bitget_perp.py echo ex=ccxt.bitget(); ex.load_markets()
>>tmp\gen_bitget_perp.py echo ok=set([s for s in ex.symbols if s.endswith(":USDT")])  # USDT本位永续
>>tmp\gen_bitget_perp.py echo res=[]
>>tmp\gen_bitget_perp.py echo for m in ms:
>>tmp\gen_bitget_perp.py echo ^    t=m.strip().upper()
>>tmp\gen_bitget_perp.py echo ^    if not t or re.match(r"^\d",t): continue
>>tmp\gen_bitget_perp.py echo ^    tccxt=(t[:-4]+"/USDT:USDT") if ("/" not in t and t.endswith("USDT")) else t
>>tmp\gen_bitget_perp.py echo ^    if tccxt in ok: res.append(t)  # 写回原始BTCUSDT命名，表名保持一致
>>tmp\gen_bitget_perp.py echo os.makedirs("results",exist_ok=True); open(outp,"w",encoding="utf-8").write("\n".join(sorted(set(res)))+"\n")
>>tmp\gen_bitget_perp.py echo print(f"[OK] Bitget永续可用: {len(res)} 个，已写入 {outp}")
echo [2/4] 过滤出 Bitget 永续可用标的…
python tmp\gen_bitget_perp.py

REM 3) 启动历史+实时采集（1年回补 + ticks）
set "SYMBOLS_ARG=--symbols-file results\symbols_bitget_perp.txt"
if not exist results\symbols_bitget_perp.txt (
  echo [WARN] 未生成 Bitget 永续白名单，退化为 results\symbols_from_db.txt
  set "SYMBOLS_ARG=--symbols-file results\symbols_from_db.txt"
)
echo [3/4] 启动采集（可最小化运行；关闭窗口即停止采集）…
python tools\live_collector_pro.py --db "%DB_PATH%" --exchange %EXCHANGE% %SYMBOLS_ARG% ^
  --timeframes 5m 15m 30m 1h 2h 4h 1d --backfill-days 365 --interval 30 --tick-interval 2 --max-workers 8

echo [4/4] 采集进程退出。按任意键关闭窗口…
pause >nul
