# -*- coding: utf-8 -*-
"""
tools/auto_indent_fix.py — 通用缩进自愈器
用法：
  python -m tools.auto_indent_fix --path "D:\\quant_system_pro\\strategy\\strategies_a1a8.py"
  python -m tools.auto_indent_fix --path "D:\\quant_system_pro\\tools\\collector_pro.py"
"""
import argparse, io, os, re, time, py_compile, traceback

def _read(p):  return io.open(p, "r", encoding="utf-8").read()
def _write(p,s): io.open(p, "w", encoding="utf-8").write(s if s.endswith("\n") else s+"\n")

def _backup(p):
    ts=time.strftime("%Y%m%d-%H%M%S")
    bak=f"{p}.bak.{ts}"
    io.open(bak,"w",encoding="utf-8").write(_read(p))
    return bak

def try_compile(path):
    try:
        py_compile.compile(path, doraise=True)
        return True, ""
    except py_compile.PyCompileError as e:
        return False, str(e)
    except Exception as e:
        return False, traceback.format_exc()

def auto_fix(path, max_rounds=30):
    if not os.path.exists(path):
        raise SystemExit(f"[ERROR] 文件不存在：{path}")
    bak=_backup(path)

    s=_read(path).replace("\t","    ")
    s=re.sub(r"[ \t]+(\r?\n)", r"\1", s)  # 行尾空白
    _write(path,s)

    ok,msg=try_compile(path)
    rounds=0
    while (not ok) and rounds<max_rounds:
        rounds+=1
        m=re.search(r"IndentationError: unexpected indent \([^)]+, line (\d+)\)", msg)
        if not m: break
        ln=int(m.group(1))
        lines=_read(path).splitlines()
        if 1<=ln<=len(lines):
            # 只清这一行的前导空白
            fixed=lines[ln-1].lstrip()
            lines[ln-1]=fixed
            _write(path, "\n".join(lines)+"\n")
        ok,msg=try_compile(path)

    if ok:
        print(f"✅ 缩进自愈完成：{path}  修复轮次={rounds}  🗄️ 备份={bak}")
    else:
        print(f"⚠️ 仍未通过编译：{path}\n最后错误：\n{msg}\n已保留备份：{bak}")
        raise SystemExit(1)

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    args=ap.parse_args()
    auto_fix(args.path)
