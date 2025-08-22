@echo off
setlocal
set "PY=C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python.exe"
set "DB=D:\quant_system_v2\data\market_data.db"
set "JS=Q:\deploy\live_best_params.json"

"%PY%" "Q:\tools\best_params_importer.py" --db "%DB%" --json "%JS%"
echo.
echo VERIFY:
"%PY%" -c "import sqlite3; con=sqlite3.connect(r'%DB%'); print('rows=', con.execute('select count(*) from best_params').fetchone()[0]); print(list(con.execute('select symbol,timeframe,strategy,round(coalesce(score,0),6),updated_at from best_params order by updated_at desc, rowid desc limit 20'))); con.close()"
echo.
echo DONE
pause
endlocal
