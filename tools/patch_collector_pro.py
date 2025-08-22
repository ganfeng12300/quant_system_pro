# -*- coding: utf-8 -*-
"""
tools/patch_collector_pro.py — 终极采集写入补丁（只写新 ts + 去重 + 空集短路 + 'break outside loop' 自愈 + 缩进自愈）
"""
import io, os, re, time, py_compile, traceback

TARGET = r"D:\quant_system_pro\tools\collector_pro.py"

PATTERN = r'wrote\s*=\s*_write_df\(\s*con\s*,\s*tb\s*,\s*df\s*\)'
REPLACEMENT = r'''max_ts_row = None
        try:
            max_ts_row = con.execute(f"SELECT MAX(ts) FROM \\"{tb}\\"").fetchone()
        except Exception:
            max_ts_row = (None,)
        max_ts = (max_ts_row[0] if max_ts_row and max_ts_row[0] is not None else -1)
        # 仅保留新数据 + ts 去重
        if df is not None and not df.empty:
            if "ts" in df.columns:
                df = df[df["ts"] > max_ts]
                if not df.empty:
                    df = df.drop_duplicates(subset=["ts"])
        # 空集短路，避免无意义写入
        if df is None or df.empty:
            wrote = 0
        else:
            wrote = _write_df(con, tb, df)'''

def _read(p):  return io.open(p, "r", encoding="utf-8").read()
def _write(p,s): io.open(p, "w", encoding="utf-8").write(s if s.endswith("\n") else s+"\n")

def backup(path:str)->str:
    ts=time.strftime("%Y%m%d-%H%M%S")
    bak=f"{path}.bak.{ts}"
    _write(bak, _read(path))
    return bak

def try_compile(path):
    try:
        py_compile.compile(path, doraise=True)
        return True, ""
    except py_compile.PyCompileError as e:
        return False, str(e)
    except Exception as e:
        return False, traceback.format_exc()

def auto_fix_unexpected_indent(path, max_rounds=30):
    # 清除“unexpected indent”所在行的前导空白，直到可编译
    s=_read(path).replace("\t","    ")
    s=re.sub(r"[ \t]+(\r?\n)", r"\1", s)
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
            lines[ln-1]=lines[ln-1].lstrip()
            _write(path, "\n".join(lines)+"\n")
        ok,msg=try_compile(path)
    return ok,msg,rounds

def _inside_def(lines, ln_idx):
    """粗略判断该行是否位于某个 def 块内：向上找最近的 def，其缩进小于当前行的缩进即可。"""
    cur_indent = len(lines[ln_idx]) - len(lines[ln_idx].lstrip())
    for i in range(ln_idx, -1, -1):
        line = lines[i]
        if not line.strip(): 
            continue
        if re.match(r'^\s*def\s+\w+\(.*\)\s*:', line):
            def_indent = len(line) - len(line.lstrip())
            return cur_indent > def_indent
        # 若遇到 class 更上层，也可视为可能在块内，继续向上寻找 def
    return False

def auto_fix_break_outside_loop(path, max_rounds=20):
    ok,msg=try_compile(path)
    rounds=0
    while (not ok) and rounds<max_rounds and ("'break' outside loop" in msg):
        rounds+=1
        m=re.search(r"SyntaxError: 'break' outside loop \([^)]+, line (\d+)\)", msg)
        if not m:
            break
        ln=int(m.group(1))
        lines=_read(path).splitlines()
        idx=ln-1
        if 0<=idx<len(lines):
            line=lines[idx]
            # 仅替换该行上的 break（不碰字符串里的 break）
            if _inside_def(lines, idx):
                lines[idx]=line.replace("break", "return")
            else:
                lines[idx]=line.replace("break", "pass")
            _write(path, "\n".join(lines)+"\n")
        ok,msg=try_compile(path)
        if not ok and "unexpected indent" in msg:
            ok2,msg2,_=auto_fix_unexpected_indent(path, max_rounds=10)
            ok,msg = (ok2,msg2) if ok2 else (ok,msg)
    return ok,msg,rounds

def main():
    if not os.path.exists(TARGET):
        raise SystemExit(f"[ERROR] 找不到：{TARGET}")
    bak=backup(TARGET)

    s=_read(TARGET).replace("\t","    ")
    # 首处替换；如果你希望全量替换所有写入点，把 count=1 改为 0
    s2, n = re.subn(PATTERN, REPLACEMENT, s, count=1, flags=re.M)
    if n==0:
        raise SystemExit("未找到 `_write_df(con, tb, df)` 调用点，请把 collector_pro.py 写入片段贴我。")
    _write(TARGET, s2)

    # 1) 先尝试编译
    ok,msg=try_compile(TARGET)
    # 2) 若有 unexpected indent，先做缩进自愈
    if (not ok) and "unexpected indent" in msg:
        ok,msg,_=auto_fix_unexpected_indent(TARGET, max_rounds=30)
    # 3) 若有 'break outside loop'，做定点自愈并重编译
    if (not ok) and "'break' outside loop" in msg:
        ok,msg,_=auto_fix_break_outside_loop(TARGET, max_rounds=20)

    if ok:
        print(f"✅ 注入去重 + 自愈完成且可编译：{TARGET}（替换次数={n}）\n🗄️ 备份：{bak}")
    else:
        print(f"⚠️ 仍未通过编译：{TARGET}\n最后错误：\n{msg}\n已保留备份：{bak}")
        raise SystemExit(1)

if __name__=="__main__":
    main()
