# -*- coding: utf-8 -*-
"""
tools/fix_strategies_a1a8.py — 终极修复（正则修 + STRATS 兜底 + 缩进自愈）
"""
import io, os, re, time, py_compile, importlib

TARGET = r"D:\quant_system_pro\strategy\strategies_a1a8.py"

def backup(path:str)->str:
    ts=time.strftime("%Y%m%d-%H%M%S")
    bak=f"{path}.bak.{ts}"
    with io.open(path,"r",encoding="utf-8") as f: src=f.read()
    with io.open(bak,"w",encoding="utf-8") as f: f.write(src)
    return bak

def fix_content(s:str)->str:
    s=s.replace("\t","    ")
    # 1) return ... )],  → return ... ),
    s=re.sub(r'(?m)(\breturn[^\n]*?\))\]\s*,', r'\1,', s)
    # 2) 行尾：return ... ] → return ...
    s=re.sub(r'(?m)(\breturn[^\n]*?)\]\s*$', r'\1', s)
    # 3) 兜底：predict(_proba)(...) 行尾 ] → 去掉
    s=re.sub(r'(?m)(\b(?:predict|predict_proba)\([^\n]*?\))\]\s*$', r'\1', s)
    # 4) 若缺 STRATS 导出则追加
    if "STRATS =" not in s:
        s += (
            "\n\n# 导出策略映射（供实盘路由使用）\n"
            "from strategy.registry import list_strategies\n"
            "STRATS = {s.key: s for s in list_strategies()}\n"
        )
    if not s.endswith("\n"): s+="\n"
    return s

def main():
    if not os.path.exists(TARGET):
        raise SystemExit(f"[ERROR] 找不到：{TARGET}")
    bak=backup(TARGET)
    with io.open(TARGET,"r",encoding="utf-8") as f: s=f.read()
    s=fix_content(s)
    with io.open(TARGET,"w",encoding="utf-8") as f: f.write(s)
    # 先尝试编译
    try:
        py_compile.compile(TARGET, doraise=True)
        print(f"✅ 正则修复完成且可编译：{TARGET}\n🗄️ 备份：{bak}")
    except Exception:
        # 调用通用缩进自愈
        from tools.auto_indent_fix import auto_fix
        auto_fix(TARGET)
        print(f"✅ 正则修复 + 缩进自愈完成：{TARGET}\n🗄️ 备份：{bak}")

if __name__=="__main__":
    main()
