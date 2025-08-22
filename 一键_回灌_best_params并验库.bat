@echo off
setlocal

set "PY=C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python.exe"
set "WORKDIR=D:\quant_system_pro (3)\quant_system_pro"
set "DB=D:\quant_system_v2\data\market_data.db"
set "JS=D:\quant_system_pro (3)\quant_system_pro\deploy\live_best_params.json"

cd /d "%WORKDIR%" || (echo [FATAL] cannot cd to %WORKDIR% & pause & exit /b 1)

rem 1) 回灌 JSON -> SQLite(best_params)
"%PY%" "tools\best_params_importer.py" ^
  --db "%DB%" ^
  --json "%JS%"

if errorlevel 1 (
  echo [FATAL] import failed.
  pause
  exit /b 1
)

rem 2) 立刻验库
echo.
echo [VERIFY]
"%PY%" -c "import sqlite3;con=sqlite3.connect(r'%DB%'); \
print('rows=',con.execute('select count(*) from best_params').fetchone()[0]); \
[print(r) for r in con.execute(\"select symbol,timeframe,strategy,round(coalesce(score,0),6),updated_at from best_params order by updated_at desc, rowid desc limit 20\")]; \
con.close()"
echo.
echo [OK] 完成。
pause
endlocal
