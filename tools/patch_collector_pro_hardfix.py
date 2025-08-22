# -*- coding: utf-8 -*-
"""
tools/patch_collector_pro_hardfix.py
- 注入去重写入：仅写 ts>MAX(ts) + ts 去重 + 空集短路（替换所有 _write_df(con,tb,df) 调用点）
- 规范缩进：tab→4空格、去行尾空白
- 结构扫描：把“不在 for/while 里的 break” → 函数内改 return；非函数内改 pass（多处一次性修复）
- 自愈编译：若仍有 unexpected indent 自动消除该行前导空白并重试，直到通过（最多 50 轮）
"""
import io, os, re, time, py_compile, traceback

TARGET = r"D:\quant_system_pro\tools\collector_pro.py"

WRITE_PAT = r'wrote\s*=\s*_write_df\(\s*con\s*,\s*tb\s*,\s*df\s*\)'
WRITE_REP = r'''max_ts_row = None
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

BLOCK_START_RE = re.compile(
    r'^\s*(?:(for|while|def|class|if|elif|else|try|except|finally|with)\b.*:\s*(#.*)?)$'
)
ONLY_BREAK_RE   = re.compile(r'^\s*break\s*(#.*)?$')
COMMENT_ONLY_RE = re.compile(r'^\s*(#.*)?$')

def _read(p):  return io.open(p, "r", encoding="utf-8").read()
def _write(p,s): io.open(p, "w", encoding="utf-8").write(s if s.endswith("\n") else s+"\n")

def backup(path:str)->str:
    ts=time.strftime("%Y%m%d-%H%M%S")
    bak=f"{path}.bak.{ts}"
    _write(bak, _read(path))
    return bak

def normalize_whitespace(s:str)->str:
    s = s.replace("\t", "    ")
    s = re.sub(r"[ \t]+(\r?\n)", r"\1", s)
    return s

def inject_write_filter_all(s:str)->str:
    s2, _ = re.subn(WRITE_PAT, WRITE_REP, s, count=0, flags=re.M)
    return s2

def compute_indent(line:str)->int:
    return len(line) - len(line.lstrip(" "))

def fix_break_outside_loop_structural(s:str)->str:
    """一次性结构扫描，把不在任何 for/while 块内的 独立 break 改成 return/pass"""
    lines = s.splitlines()
    stack = []  # list of (indent, kind)
    for i, line in enumerate(lines):
        if COMMENT_ONLY_RE.match(line):
            continue
        cur_indent = compute_indent(line)
        # 出栈：当前缩进 <= 栈顶缩进
        while stack and cur_indent <= stack[-1][0]:
            stack.pop()
        # 是否块起始
        m = BLOCK_START_RE.match(line)
        if m:
            kind = m.group(1)
            stack.append((cur_indent, kind))
            continue
        # 处理独立 break
        if ONLY_BREAK_RE.match(line):
            # 查看栈内是否存在 for/while
            in_loop = any(k in ("for","while") for _,k in stack)
            in_def  = any(k == "def"           for _,k in stack)
            if not in_loop:
                # 替换为 return/pass，保留原来缩进与注释
                indent = " " * cur_indent
                comment = ""
                if "#" in line:
                    comment = line[line.index("#"):]
                lines[i] = indent + ("return" if in_def else "pass") + ("" if not comment else " "+comment)
    return "\n".join(lines) + "\n"

def try_compile(path):
    try:
        py_compile.compile(path, doraise=True)
        return True, ""
    except py_compile.PyCompileError as e:
        return False, str(e)
    except Exception as e:
        return False, traceback.format_exc()

def auto_fix_unexpected_indent(path, max_rounds=50):
    rounds = 0
    ok, msg = try_compile(path)
    while (not ok) and rounds < max_rounds and "unexpected indent" in msg:
        rounds += 1
        m = re.search(r"IndentationError: unexpected indent \([^)]+, line (\d+)\)", msg)
        if not m:
            break
        ln = int(m.group(1))
        lines = _read(path).splitlines()
        if 1 <= ln <= len(lines):
            lines[ln-1] = lines[ln-1].lstrip()
            _write(path, "\n".join(lines) + "\n")
        ok, msg = try_compile(path)
    return ok, msg, rounds

def main():
    if not os.path.exists(TARGET):
        raise SystemExit(f"[ERROR] 找不到：{TARGET}")
    bak = backup(TARGET)

    s = normalize_whitespace(_read(TARGET))
    s = inject_write_filter_all(s)
    s = fix_break_outside_loop_structural(s)
    _write(TARGET, s)

    ok, msg = try_compile(TARGET)
    if (not ok) and "unexpected indent" in msg:
        ok, msg, _ = auto_fix_unexpected_indent(TARGET, max_rounds=50)
    # 再跑一遍结构修正（若缩进自愈导致行号变化）
    if not ok and "'break' outside loop" in msg:
        _write(TARGET, fix_break_outside_loop_structural(_read(TARGET)))
        ok, msg = try_compile(TARGET)

    if ok:
        print(f"✅ collector_pro.py 硬核修复完成并可编译。\n🗄️ 备份：{bak}")
    else:
        print(f"⚠️ 仍未通过编译：{TARGET}\n最后错误：\n{msg}\n🗄️ 备份：{bak}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
