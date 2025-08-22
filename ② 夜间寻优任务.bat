@echo off
setlocal ENABLEDELAYEDEXECUTION
chcp 65001 >nul
title ��QS2��ҹ��Ѱ�����񣨱�������+��־��

REM 1) �е���Ŀ��Ŀ¼��ע�����ţ�·�����пո�/���ţ�
cd /d "D:\quant_system_pro (3)\quant_system_pro"

REM 2) ������ ���� �����޸�
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "TFS=5m,15m,30m,1h,2h,4h,1d"
set "DAYS=180"
set "MIN_TRADES=1"
set "MAX_DD=10000"
set "SYMBOLS=results\symbols_binance_perp.txt"
set "OUT_JSON=deploy\qs2_live_best_params.json"
set "REPORT=results\qs2_optimizer_report.json"

REM 3) ��־���̶�һ�������һ�Ρ����ļ������ڲ鿴��
if not exist "logs" mkdir "logs"
set "LOG=logs\qs2_nightly_optimizer.last.log"

echo [QS2] ==== ҹ��Ѱ������ ====  > "%LOG%"
echo DB=%QS_DB%  >> "%LOG%"
echo TFS=%TFS%   >> "%LOG%"
echo OUT=%OUT_JSON% REPORT=%REPORT% >> "%LOG%"
echo ---------------------------------------------- >> "%LOG%"

REM 4) �����Լ죺Python���ű����ļ�������
where python >nul 2>&1
if errorlevel 1 (
  echo [ERROR] δ�ҵ� python����ȷ���Ѱ�װ������ PATH��>> "%LOG%"
  echo [ERROR] δ�ҵ� python����ȷ���Ѱ�װ������ PATH��
  goto :HOLD
)

if not exist "tools_s2\qs2_nightly_optimizer.py" (
  echo [ERROR] ȱ�� tools_s2\qs2_nightly_optimizer.py >> "%LOG%"
  echo [ERROR] ȱ�� tools_s2\qs2_nightly_optimizer.py
  goto :HOLD
)

if not exist "%QS_DB%" (
  echo [ERROR] ���ݿⲻ���ڣ�%QS_DB% >> "%LOG%"
  echo [ERROR] ���ݿⲻ���ڣ�%QS_DB%
  goto :HOLD
)

if not exist "optimizer\a1a8_optimizer_and_deploy.py" (
  echo [ERROR] ȱ�� optimizer\a1a8_optimizer_and_deploy.py >> "%LOG%"
  echo [ERROR] ȱ�� optimizer\a1a8_optimizer_and_deploy.py
  goto :HOLD
)

REM 5) ���У��ѿ���̨���ͬʱд����־��
echo [QS2] У�����ʶȲ�����Ѱ��... | tee -a "%LOG%"
python "tools_s2\qs2_nightly_optimizer.py" ^
  --db "%QS_DB%" ^
  --timeframes "%TFS%" ^
  --days %DAYS% ^
  --min-trades %MIN_TRADES% ^
  --max-dd %MAX_DD% ^
  --symbols-file "%SYMBOLS%" ^
  --out-json "%OUT_JSON%" ^
  --report "%REPORT%" ^
  >> "%LOG%" 2>&1

set "RC=%ERRORLEVEL%"
echo [QS2] �˳���=%RC% >> "%LOG%"

if "%RC%"=="0" (
  echo [QS2] ? ҹ��Ѱ����ɣ����%OUT_JSON%
  echo [QS2] ? ҹ��Ѱ����ɣ����%OUT_JSON% >> "%LOG%"
) else if "%RC%"=="2" (
  echo [QS2] ? ���ʶ�δ��꣬����ֹ���鿴 %REPORT% ϸ�ڣ�
  echo [QS2] ? ���ʶ�δ��꣬����ֹ >> "%LOG%"
) else (
  echo [QS2] ? ����ʧ�ܣ��˳���=%RC%������鿴��־��%LOG%
)

:HOLD
echo.
echo [��ʾ] �������ѱ�������������رա���
pause >nul
endlocal
