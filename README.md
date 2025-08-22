# 量化系统 Pro（S 档，机构级）
安装路径：`D:\quant_system_pro\`  
数据源：复用 `D:\quant_system_v2\data\market_data.db`（无需搬库）

## 功能概览
- 采集层（可选）：历史补齐、实时守护、覆盖率体检/修复（若你已有数据，可直接回测/实盘）
- 策略盒 A1–A8（含 LGBM/RF/LSTM，GPU 可用），多周期并行回测 + **贝叶斯/遗传混合寻优**
- **全成本模型**：手续费、滑点、资金费率、**冲击成本（平方根冲击）**
- **稳健统计**：Walk-Forward、**Deflated Sharpe**、**SPA/Reality Check**、**PBO 可视化报告**
- 多交易所实盘路由（Binance/OKX/Bitget），**冰山分单**执行，合规 tick/step/minNotional/面值
- Prometheus 指标 + Webhook 告警，一键脚本（bat）

## 快速上手（最少步骤）
1. 安装依赖（cmd 以管理员运行）  
   ```bat
   cd /d D:\quant_system_pro
   pip install -r requirements.txt
