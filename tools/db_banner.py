# -*- coding: utf-8 -*-
# tools/db_banner.py
import os, sys, time, sqlite3, ctypes
from datetime import datetime

TF_MIN = {"5m":5, "15m":15, "30m":30, "1h":60, "2h":120, "4h":240, "1d":1440}

def _enable_win_vt():
    if os.name != "nt": return
    try:
        k32 = ctypes.windll.kernel32
        h = k32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if k32.GetConsoleMode(h, ctypes.byref(mode)):
            k32.SetConsoleMode(h, mode.value | 0x0004)
    except Exception:
        pass
_enable_win_vt()

C = {"dim":"\033[2m","reset":"\033[0m","ok":"\033[38;5;47m","warn":"\033[38;5;214m",
     "err":"\033[38;5;203m","acc":"\033[38;5;39m","title":"\033[38;5;51m",
     "muted":"\033[38;5;245m","bar":"\033[38;5;45m"}

def _fmt_bytes(n):
    f=float(n)
    for u in ("B","KB","MB","GB","TB"):
        if f<1024 or u=="TB": return f"{f:.2f} {u}"
        f/=1024

def _list_tables(con): 
    return [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]

def _parse_table(t):
    if "_" not in t: return None,None
    s,tf=t.rsplit("_",1);  return (s,tf) if tf in TF_MIN else (None,None)

def _expected_rows(days,tf): 
    return days if tf=="1d" else int(days*1440//TF_MIN[tf])

def _safe_count(con,t,cut): 
    try: return int(con.execute(f"SELECT COUNT(1) FROM {t} WHERE timestamp>=?",(cut,)).fetchone()[0])
    except Exception: return 0

def _bar(r,w=42):
    r=max(0,min(1,r)); full=int(r*w)
    return f"{C['bar']}"+"#"*full+"."*(w-full)+f"{C['reset']} {r*100:5.1f}%"

def print_db_startup_banner(db_path:str, days:int=365, tfs=("5m","15m","30m","1h","2h","4h","1d"), hard_time_budget_sec:float=8.0):
    tfs=tuple(tf for tf in tfs if tf in TF_MIN)
    cutoff=int(time.time())-days*86400

    db,wal,shm=db_path,db_path+"-wal",db_path+"-shm"
    db_b=os.path.getsize(db) if os.path.exists(db) else 0
    wal_b=os.path.getsize(wal) if os.path.exists(wal) else 0
    shm_b=os.path.getsize(shm) if os.path.exists(shm) else 0
    total=db_b+wal_b+shm_b

    uri=f"file:{db_path}?mode=ro"
    try: con=sqlite3.connect(uri,uri=True,timeout=2)
    except sqlite3.OperationalError: con=sqlite3.connect(db_path,timeout=2)
    con.execute("PRAGMA journal_mode=WAL"); con.execute("PRAGMA synchronous=OFF"); con.execute("PRAGMA temp_store=MEMORY")

    pairs=[(s,tf,t) for t in _list_tables(con) for (s,tf) in [_parse_table(t)] if s and tf in tfs]
    exp_by_tf={tf:_expected_rows(days,tf) for tf in tfs}
    exp_total=sum(exp_by_tf[tf] for _,tf,_ in pairs) or 1

    t0=time.time(); have_total=0; sampled=0
    for s,tf,t in pairs:
        if time.time()-t0>hard_time_budget_sec: break
        have_total+=min(_safe_count(con,t,cutoff),exp_by_tf[tf]); sampled+=1
    if sampled and sampled<len(pairs): have_total=int(have_total*(len(pairs)/sampled))

    ratio=have_total/exp_total
    span=max(1,int(time.time()-t0)); rps=have_total/span if span else 0
    eta="--"
    if rps>0 and have_total<exp_total:
        sec=int((exp_total-have_total)/rps); h,m=divmod(sec//60,60); s=sec%60; eta=f"{h:02d}:{m:02d}:{s:02d}"

    tf_have={tf:0 for tf in tfs}; tf_exp={tf:0 for tf in tfs}; t1=time.time(); counted=0
    for s,tf,t in pairs:
        tf_exp[tf]+=exp_by_tf[tf]
        if time.time()-t1>max(1.5,hard_time_budget_sec/2): continue
        tf_have[tf]+=min(_safe_count(con,t,cutoff),exp_by_tf[tf]); counted+=1
    if counted and counted<len(pairs):
        scale=len(pairs)/counted
        for tf in tfs: tf_have[tf]=int(tf_have[tf]*scale)

    w=78
    def line(txt="",color="muted"):
        t=txt[:w-4]; pad=" "*(w-4-len(t)); print(f"║ {C[color]}{t}{C['reset']}{pad} ║")

    print(f"{C['title']}╔{'═'*(w-2)}╗{C['reset']}")
    line("🚀 采集器已启动 · 数据库连通确认（机构级彩色横幅）","title")
    print(f"╟{'─'*(w-2)}╢")
    line(f"DB 路径：{db_path}","acc")
    line(f"主库：{_fmt_bytes(db_b)}   WAL：{_fmt_bytes(wal_b)}   SHM：{_fmt_bytes(shm_b)}","muted")
    line(f"当前占用合计：{_fmt_bytes(total)}","muted")
    print(f"╟{'─'*(w-2)}╢")
    line(f"统计窗口：近 {days} 天    周期：{', '.join(tfs)}","muted")
    line(f"匹配表数：{len(pairs)}","muted")
    level="ok" if ratio>=0.98 else ("warn" if ratio>=0.8 else "err")
    line(f"总进度：{_bar(ratio)}   已有/应有：{have_total:,}/{exp_total:,} 行    估计ETA：{eta}", level)
    tf_line=" | ".join(f"{tf}: {min(100.0,(tf_have[tf]/(tf_exp[tf] or 1))*100):5.1f}%" for tf in tfs)
    line(f"分周期覆盖：{tf_line}","muted")
    print(f"{C['title']}╚{'═'*(w-2)}╝{C['reset']}")
    print(f"{C['dim']}{datetime.now().strftime('时间：%Y-%m-%d %H:%M:%S')} · 说明：限时统计，超时采用样本等比估算；仅用于“近N天”覆盖确认。{C['reset']}")
