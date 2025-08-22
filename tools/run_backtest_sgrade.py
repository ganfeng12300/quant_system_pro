# -*- coding: utf-8 -*-
"""
S级机构版 run_backtest_sgrade.py
- 自动分辨 --symbol 与 --symbols-file
- 若下游仅接受 --symbols-file，则自动生成临时清单文件传递
- 强化 deploy/live_best_params.json 的鲁棒解析（支持对象列表/字符串列表/单对象）
- 全程 shell=False 与 list 传参，100% 兼容含空格/括号路径（如 D:\... (3)\...）
"""

import argparse
import json
import os
import sys
import tempfile
import subprocess
from datetime import datetime
from typing import List, Dict, Any, Optional

# 默认路径（按您的项目结构）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_JSON = os.path.join(PROJECT_ROOT, "deploy", "live_best_params.json")
OPTIMIZER = os.path.join(PROJECT_ROOT, "optimizer", "a1a8_optimizer_and_deploy.py")

def norm_symbols_from_args(symbols: Optional[List[str]], symbols_file: Optional[str]) -> List[str]:
    """优先使用 --symbol；否则读取 --symbols-file；都没给则报错。"""
    if symbols and len(symbols) > 0:
        return [s.strip() for s in symbols if s and s.strip()]
    if symbols_file:
        if not os.path.isabs(symbols_file):
            symbols_file_path = os.path.join(PROJECT_ROOT, symbols_file)
        else:
            symbols_file_path = symbols_file
        if not os.path.exists(symbols_file_path):
            raise FileNotFoundError(f"symbols 文件不存在: {symbols_file_path}")
        with open(symbols_file_path, "r", encoding="utf-8") as f:
            rows = [ln.strip() for ln in f if ln.strip()]
        if not rows:
            raise ValueError(f"symbols 文件为空: {symbols_file_path}")
        return rows
    raise ValueError("必须通过 --symbol 或 --symbols-file 指定至少一个交易对")

def ensure_timeframes(tfs: Optional[List[str]]) -> List[str]:
    if not tfs:
        raise ValueError("必须通过 --timeframes 指定至少一个周期，如 30m/1h/4h")
    return [tf.strip() for tf in tfs if tf.strip()]

def read_best_from_json_safe(json_path: str) -> List[Dict[str, Any]]:
    """
    鲁棒解析：
    - 若是对象列表：[{"symbol": "...", "timeframe": "...", ...}, ...] -> 直接返回
    - 若是字符串列表：["BTCUSDT","ETHUSDT"] -> 映射为 [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]
    - 若是单对象/单字符串 -> 转为列表
    - 若文件不存在或为空 -> 返回 []
    """
    if not json_path:
        return []
    jp = json_path if os.path.isabs(json_path) else os.path.join(PROJECT_ROOT, json_path)
    if not os.path.exists(jp):
        return []
    try:
        with open(jp, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            return []
        data = json.loads(raw)
        # 统一成列表
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, str):
            data = [data]
        # 规范化
        norm = []
        for it in data:
            if isinstance(it, dict):
                norm.append({
                    "symbol": it.get("symbol"),
                    "timeframe": it.get("timeframe"),
                    "params": it.get("params", {}),
                    "meta": it.get("meta", {}),
                })
            elif isinstance(it, str):
                norm.append({"symbol": it, "timeframe": None, "params": {}, "meta": {}})
            else:
                # 未知类型，跳过但不报错
                continue
        return norm
    except Exception:
        # 解析失败时不让主流程崩，回退空列表
        return []

def build_optimizer_cmd(db: str,
                        symbols: List[str],
                        timeframes: List[str],
                        days: int,
                        out_json_path: str) -> (List[str], str):
    """
    构建调用优化器的命令：
    - 若优化器需要 --symbols-file，我们统一写入一个临时文件并传递路径，避免“把 BTCUSDT 当文件名”的坑。
    - 全部参数以 list 形式传递，shell=False，避免路径空格/括号问题。
    返回：(cmd_list, temp_symbols_file_path or "")
    """
    # 写入临时 symbols 文件
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", prefix="symbols_", mode="w", encoding="utf-8")
    try:
        for s in symbols:
            temp_file.write(s + "\n")
    finally:
        temp_file.close()
    temp_path = temp_file.name

    # 统一相对路径为绝对路径
    db_path = db if os.path.isabs(db) else os.path.join(PROJECT_ROOT, db)
    json_out = out_json_path if os.path.isabs(out_json_path) else os.path.join(PROJECT_ROOT, out_json_path)
    optimizer_path = OPTIMIZER

    # timeframes 展开为用空格分隔，传参时逐项加入
    cmd = [
        sys.executable, optimizer_path,
        "--db", db_path,
        "--symbols-file", temp_path,
        "--json", json_out,
        "--days", str(days),
    ]
    # 追加 --timeframes
    cmd += ["--timeframes"] + timeframes

    return cmd, temp_path

def main():
    parser = argparse.ArgumentParser(description="S级机构版：回测与最优参数部署入口")
    parser.add_argument("--db", required=True, help="数据库路径，如 D:\\quant_system_v2\\data\\market_data.db")
    parser.add_argument("--symbol", nargs="*", help="一个或多个交易对，如 BTCUSDT ETHUSDT（优先于 --symbols-file）")
    parser.add_argument("--symbols-file", help="symbols 清单文件（每行一个交易对）")
    parser.add_argument("--timeframes", nargs="+", required=True, help="一个或多个周期，如 5m 15m 30m 1h 4h")
    parser.add_argument("--days", type=int, default=90, help="回测天数，默认 90")
    parser.add_argument("--json", default=DEFAULT_JSON, help="最优参数输出/读取 JSON，默认 deploy\\live_best_params.json")
    args = parser.parse_args()

    # 1) 解析 symbols 与 timeframes
    symbols = norm_symbols_from_args(args.symbol, args.symbols_file)
    tfs = ensure_timeframes(args.timeframes)

    # 2) 安全读取历史 best json（不作为硬依赖，仅日志/参考）
    js_rows = read_best_from_json_safe(args.json)

    # 3) 打印运行配置
    print("🟢 run_config.json 已生成")
    run_cfg = {
        "db": args.db,
        "symbols": symbols,
        "timeframes": tfs,
        "days": args.days,
        "json": args.json,
        "seen_best_items": len(js_rows),
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    # 可选：写 run_config.json 以便排障
    try:
        with open(os.path.join(PROJECT_ROOT, "run_config.json"), "w", encoding="utf-8") as f:
            json.dump(run_cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # 4) 组装并调用优化器（强制传 --symbols-file=临时文件，避免历史坑）
    cmd, temp_symbols_path = build_optimizer_cmd(
        db=args.db,
        symbols=symbols,
        timeframes=tfs,
        days=args.days,
        out_json_path=args.json
    )

    print("▶ 运行优化器：", " ".join([f'"{c}"' if " " in c else c for c in cmd]))
    try:
        # 关键：shell=False + list 传参，完美兼容含空格/括号路径
        proc = subprocess.run(cmd, shell=False)
        if proc.returncode != 0:
            print(f"❌ 优化器退出码：{proc.returncode}（请检查上方输出）")
            sys.exit(proc.returncode)
        else:
            print("✅ 优化与参数部署完成")
    finally:
        # 删除临时 symbols 文件
        try:
            if temp_symbols_path and os.path.exists(temp_symbols_path):
                os.remove(temp_symbols_path)
        except Exception:
            pass

if __name__ == "__main__":
    main()
