@echo off
chcp 65001 >nul
title ▶ 诊断：采集卡住原因（是否在写库 / 不可补清单）
setlocal EnableDelayedExpansion
cd /d "D:\quant_system_pro (3)\quant_system_pro"
set "QS_DB=D:\quant_system_v2\data\market_data.db"

echo [0/5] 环境检查
where python >nul 2>nul || (echo [ERROR] 未找到 python（未加入 PATH）。& pause & exit /b 1)

echo.
echo [1/5] 检测采集器进程（rt_updater_*）
powershell -NoProfile -Command ^
  "$p=Get-CimInstance Win32_Process | ?{$_.Name -eq 'python.exe' -and $_.CommandLine -match 'rt_updater'} | select CommandLine,ProcessId; if($p){$p | ft -AutoSize; } else {Write-Host '[WARN] 未发现包含 rt_updater 的 python 进程'; }"

echo.
echo [2/5] 数据库最近写入时间与大小（含 WAL/SHM）
powershell -NoProfile -Command ^
  "$p='%QS_DB%'; if(Test-Path $p){ $f=Get-Item $p; Write-Host ('DB   : ' + $f.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss') + '   ' + [math]::Round($f.Length/1MB,2) + ' MB'); } else { Write-Host '[ERROR] DB 不存在：%QS_DB%'; }" ^
  "; foreach($s in @('%QS_DB%.wal','%QS_DB%.shm')){ if(Test-Path $s){ $g=Get-Item $s; Write-Host ('  ' + $g.Name + ': ' + $g.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss') + '   ' + [math]::Round($g.Length/1MB,2) + ' MB'); } }"

echo.
echo [3/5] 最近6小时是否有写入（统计有新K线的表数量）
>__diag3.py (
  echo import sqlite3, time
  echo db=r"%QS_DB%"
  echo cut=int(time.time())-6*3600
  echo con=sqlite3.connect(db, timeout=30)
  echo cur=con.cursor()
  echo tbl=[r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%[_]%'").fetchall()]
  echo def latest(t):
  echo.    try:
  echo.        r=cur.execute(f"SELECT MAX(timestamp) FROM '%s'"%%t).fetchone()[0]
  echo.        return r or 0
  echo.    except: return 0
  echo ok=[t for t in tbl if latest(t)>=cut*1000]
  echo print(f"[INFO] 近6小时有写入的表：{len(ok)}/{len(tbl)}")
  echo if len(ok)^<5: print("[WARN] 近6小时几乎没有写入 —— 检查采集器是否在运行且 --db 指向同一库")
  echo con.close()
)
python __diag3.py & del __diag3.py

echo.
echo [4/5] 抽查3张 1h 表最近3条记录（应为今天/最近）
>__diag4.py (
  echo import sqlite3, datetime
  echo db=r"%QS_DB%"
  echo con=sqlite3.connect(db, timeout=30)
  echo cur=con.cursor()
  echo tbl=[r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%%_1h' LIMIT 50").fetchall()]
  echo tbl=tbl[:3] if len(tbl)>=3 else tbl
  echo from datetime import datetime as D
  echo for t in tbl:
  echo.    try:
  echo.        rows=cur.execute(f"SELECT timestamp FROM '%s' ORDER BY timestamp DESC LIMIT 3"%%t).fetchall()
  echo.        human=[D.utcfromtimestamp(r[0]/1000).strftime('%%Y-%%m-%%d %%H:%%M:%%S') for r in rows]
  echo.        print(f"{t:28s} -> {human}")
  echo.    except Exception as e:
  echo.        print(f"{t:28s} -> ERROR {e}")
  echo con.close()
)
python __diag4.py & del __diag4.py

echo.
echo [5/5] 生成不可补清单（近30天零写入/空表） -> skiplist.txt
>__diag5.py (
  echo import sqlite3, time
  echo db=r"%QS_DB%"
  echo cut=int(time.time())-30*86400
  echo con=sqlite3.connect(db, timeout=30)
  echo cur=con.cursor()
  echo tbl=[r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%[_]%'").fetchall()]
  echo bad=[]
  echo for t in tbl:
  echo.    try:
  echo.        c=cur.execute(f"SELECT 1 FROM '%s' WHERE timestamp>=? LIMIT 1"%%t,(cut*1000,)).fetchone()
  echo.        if c is None: bad.append(t)
  echo.    except: bad.append(t)
  echo con.close()
  echo open("skiplist.txt","w",encoding="utf-8").write("\n".join(sorted(bad)))
  echo print(f"[INFO] 近30天零写入/空表：{len(bad)} 个  ->  已写入 skiplist.txt")
)
python __diag5.py & del __diag5.py

echo.
echo === 结论/操作建议 ===
echo • [3/5] 若近6小时写入表=0 或很少：采集器可能停了/或 --db 指到别处。请用「一键_采集_加监视.bat」重启，并确认 DB 路径就是 %QS_DB%。
echo • [5/5] 若不可补数量很大：说明大量冷门/失效合约导致总进度卡住。我可给您升级进度监视器，支持 --exclude-file skiplist.txt，立刻反映“真实可达覆盖率”。
echo.
echo 已在当前目录生成 skiplist.txt（可打开查看）。按任意键打开目录...
pause >nul
start "" "%CD%"
echo.
pause
