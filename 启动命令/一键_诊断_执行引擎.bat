@echo off
title QuanSys - Engine Scanner
setlocal EnableExtensions EnableDelayedExpansion

REM ---------- PATHS ----------
set "SCRIPT_DIR=%~dp0"
set "ROOT=D:\quant_system_pro (3)\quant_system_pro"
set "LIVE=%ROOT%\live_trading"
set "DB=D:\quant_system_v2\data\market_data.db"

REM ---------- TIMESTAMP ----------
for /f "tokens=1-3 delims=/: " %%a in ("%date% %time%") do set TS=%%a-%%b-%%c
set "TS=%date:~0,4%%date:~5,2%%date:~8,2%-%time:~0,2%%time:~3,2%%time:~6,2%"
set "TS=%TS: =0%"
set "REPORT=%SCRIPT_DIR%engine_scan_%TS%.txt"

REM ---------- HEADER ----------
echo(================= Engine Scanner ================= > "%REPORT%"
echo(ROOT   : %ROOT%>> "%REPORT%"
echo(LIVE   : %LIVE%>> "%REPORT%"
echo(DB     : %DB%>> "%REPORT%"
echo(TIME   : %TS%>> "%REPORT%"
echo(==================================================>> "%REPORT%"
echo(>> "%REPORT%"

REM ---------- CHECK PYTHON ----------
where python >nul 2>&1
if errorlevel 1 (
  echo([ERROR] Python not in PATH >> "%REPORT%"
  start "" "%REPORT%"
  echo Python 未在 PATH；请先安装/加入 PATH。
  pause
  exit /b 1
)

REM ---------- CHECK LIVE DIR ----------
if not exist "%LIVE%" (
  echo([ERROR] live_trading not found: %LIVE% >> "%REPORT%"
  start "" "%REPORT%"
  echo 未找到 live_trading 目录：%LIVE%
  pause
  exit /b 1
)

REM ---------- LIST CANDIDATES ----------
echo([1/4] Listing candidate engine scripts... >> "%REPORT%"
REM 规则：execution_engine*.py 优先；其次包含 engine/exec/ws/trade 的 .py
dir /b "%LIVE%\execution_engine*.py" > "%SCRIPT_DIR%__eng.list"
dir /b "%LIVE%\*engine*.py" >> "%SCRIPT_DIR%__eng.list"
dir /b "%LIVE%\*exec*.py"   >> "%SCRIPT_DIR%__eng.list"
dir /b "%LIVE%\*trade*.py"  >> "%SCRIPT_DIR%__eng.list"

REM 去重
for /f "usebackq delims=" %%F in ("%SCRIPT_DIR%__eng.list") do (
  set "f=%%~F"
  if not defined SEEN_%%~nxF (
    set "SEEN_%%~nxF=1"
    echo(  - %%~F>> "%REPORT%"
    echo(%%~F>> "%SCRIPT_DIR%__eng.unique"
  )
)
del "%SCRIPT_DIR%__eng.list" >nul 2>&1

REM 若仍为空，扩大范围：列出 live_trading 下全部 py
for %%A in ("%SCRIPT_DIR%__eng.unique") do if %%~zA==0 (
  del "%SCRIPT_DIR%__eng.unique" >nul 2>&1
  dir /b "%LIVE%\*.py" > "%SCRIPT_DIR%__eng.unique"
  echo(  (fallback) showing all .py under live_trading >> "%REPORT%"
)

echo(>> "%REPORT%"

REM ---------- PROBE HELP OUTPUT ----------
echo([2/4] Probing each candidate with -h/--help ... >> "%REPORT%"
set "BEST_FILE="
set "BEST_SCORE=0"

for /f "usebackq delims=" %%F in ("%SCRIPT_DIR%__eng.unique") do (
  set "CAND=%%F"
  set "FULL=%LIVE%\%%F"
  echo(---- %%F ---- >> "%REPORT%"

  REM 尝试 -h
  python "%FULL%" -h > "%SCRIPT_DIR%__h.out" 2>&1
  if errorlevel 1 (
    REM 尝试 --help
    python "%FULL%" --help > "%SCRIPT_DIR%__h.out" 2>&1
  )

  REM 只摘前40行
  powershell -NoProfile -Command "$p='%SCRIPT_DIR%__h.out'; if(Test-Path $p){Get-Content $p -TotalCount 40 | ForEach-Object { $_ }} else { '  (no output)' }" >> "%REPORT%"

  REM 粗略打分：包含这些关键词加分
  set /a SCORE=0
  findstr /I "db --db exchange --exchange mode --mode paper real leverage risk max-order pct ws websocket" "%SCRIPT_DIR%__h.out" >nul && set /a SCORE+=3
  findstr /I "Bitget OKX Binance" "%SCRIPT_DIR%__h.out" >nul && set /a SCORE+=2
  findstr /I "usage help" "%SCRIPT_DIR%__h.out" >nul && set /a SCORE+=1

  if !SCORE! GTR !BEST_SCORE! (
    set "BEST_SCORE=!SCORE!"
    set "BEST_FILE=!CAND!"
  )

  echo(>> "%REPORT%"
)

del "%SCRIPT_DIR%__h.out" >nul 2>&1
del "%SCRIPT_DIR%__eng.unique" >nul 2>&1

REM ---------- RECOMMEND COMMANDS ----------
echo([3/4] Recommendation >> "%REPORT%"
if defined BEST_FILE (
  echo(  Best match : %BEST_FILE% (score=%BEST_SCORE%) >> "%REPORT%"
  echo(  Paper cmd  : >> "%REPORT%"
  echo(    python "%%LIVE%%\%BEST_FILE%" --db "%DB%" --mode paper >> "%REPORT%"
  echo(  Real  cmd  : >> "%REPORT%"
  echo(    python "%%LIVE%%\%BEST_FILE%" --db "%DB%" --exchange bitget --mode real --risk-capital 100 --max-order-pct 5 --leverage 5 >> "%REPORT%"
) else (
  echo(  No obvious engine script found. Please check live_trading folder names. >> "%REPORT%"
)

echo(>> "%REPORT%"
echo([4/4] Hints >> "%REPORT%"
echo(  * Run in CMD (not PowerShell) if using .bat menus. >> "%REPORT%"
echo(  * If command fails, try adding -h/--help to see valid flags. >> "%REPORT%"
echo(  * Ensure DB path is correct: %DB% >> "%REPORT%"

REM ---------- SHOW REPORT ----------
start "" "%REPORT%"
echo(
echo Report saved to: %REPORT%
pause
exit /b 0
