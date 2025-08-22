# -*- coding: utf-8 -*-
# tools/patch_inject_banner.py
"""
把“彩色启动横幅”自动注入到既有 rt_updater_pro.py：
- 寻找 args 赋值行（常见写法：args = parser.parse_args() / args = parse_args()）
- 在其后插入 try/except 调用块（若已存在则跳过）
- 自动备份 .bak
用法：
  python tools/patch_inject_banner.py --file D:\quant_system_pro\tools\rt_updater_pro.py
"""

import io, os, re, argparse, sys, time

INJECT_BLOCK = r'''
# === 启动横幅（S级机构彩色） ===
try:
    from tools.db_banner import print_db_startup_banner
    _tfs = ("5m","15m","30m","1h","2h","4h","1d")
    _days = getattr(args, "backfill_days", None) or getattr(args, "days", 365) or 365
    print_db_startup_banner(db_path=args.db, days=int(_days), tfs=_tfs, hard_time_budget_sec=8.0)
except Exception as _e:
    print("[WARN] 启动横幅打印失败：", _e)
'''.lstrip("\n")

PATTERN_AFTER_ARGS = re.compile(
    r'(?m)^(?P<indent>[ \t]*)args\s*=\s*(?:parser\.)?parse_args\([^)]*\)\s*$'
)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="rt_updater_pro.py 路径")
    args = ap.parse_args()

    p = args.file
    if not os.path.exists(p):
        print(f"[FATAL] 文件不存在：{p}")
        return 1

    s = io.open(p, "r", encoding="utf-8", errors="ignore").read()

    if "print_db_startup_banner(" in s:
        print("[SKIP] 似乎已注入过（找到 print_db_startup_banner 调用），不重复操作。")
        return 0

    m = PATTERN_AFTER_ARGS.search(s)
    if not m:
        # 兜底：搜一处 “def main(” 结尾后首次出现 “args = ”
        m2 = re.search(r'(?s)def\s+main\s*\([^)]*\).*?^\s*args\s*=.*$', s)
        if not m2:
            print("[FATAL] 未找到 args = parse_args() 注入点，请手动粘贴注入段。")
            return 2
        insert_pos = s.find("\n", m2.end()) + 1
        new_s = s[:insert_pos] + INJECT_BLOCK + s[insert_pos:]
    else:
        insert_pos = s.find("\n", m.end()) + 1
        new_s = s[:insert_pos] + INJECT_BLOCK + s[insert_pos:]

    bak = p + f".bak.{time.strftime('%Y%m%d-%H%M%S')}"
    io.open(bak, "w", encoding="utf-8").write(s)
    io.open(p, "w", encoding="utf-8").write(new_s)
    print(f"[OK] 已注入横幅调用，并备份到：{bak}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
