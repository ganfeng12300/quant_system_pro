@echo off
chcp 65001 >nul
title ��QS2������ֽ��ʵ�̣������ʶ�բ�ţ�

cd /d "D:\quant_system_pro (3)\quant_system_pro"

:: �������ݿ�·��
set "QS_DB=D:\quant_system_v2\data\market_data.db"
set "TFS=5m,15m,30m,1h,2h,4h,1d"

echo [QS2] ����ִ�����ʶ�բ�ż��...
python "tools_s2\qs2_pretrade_gate.py" --db "%QS_DB%" --timeframes "%TFS%"
if errorlevel 1 (
  echo [���] �����ӳ�δ��ֽ꣬��ʵ������ֹ����������׷ƽ�ɼ���.
  pause
  exit /b 2
)

echo [QS2] բ��ͨ��������ֽ��ʵ��...
REM ����������Ӳ��������� --risk-capital 100 --max-order-pct 5 --leverage 5
python "live_trading\execution_engine_binance_ws.py" --db "%QS_DB%" --mode paper --ui-rows 30

echo [���] ֽ��ʵ���˳�
