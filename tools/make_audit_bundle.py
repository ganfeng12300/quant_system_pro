# -*- coding: utf-8 -*-
"""
make_audit_bundle.py
生成最小审计包（含脱敏配置 + DB 元信息 + 日志尾部 + 项目结构）
Usage:
  python tools/make_audit_bundle.py --db D:\quant_system_v2\data\market_data.db --out audit_bundle.zip
"""
import os, re, io, sys, json, time, zipfile, argparse, subprocess, sqlite3, shutil, traceback
from pathlib import Path
from datetime import datetime, timedelta

SENSITIVE_KEYS = [
    r'api[_-]?key', r'secret', r'passphrase', r'password', r'token',
    r'access[_-]?key', r'secret[_-]?key', r'private[_-]?key'
]
SENSITIVE_RE = re.compile(r'("?(?:' + "|".join(SENSITIVE_KEYS) + r')"?\s*[:=]\s*)(["\']?)([^"\',#\s]+)(["\']?)', re.IGNORECASE)

EXCLUDE_DIRS = {'.git', '.hg', '.svn', '.idea', '.vscode', '__pycache__', '.venv', 'venv', 'env', 'node_modules'}
CONFIG_EXTS = {'.yaml', '.yml', '.json'}
LOG_EXTS = {'.log', '.txt'}
RECENT_HOURS = 72
TAIL_LINES = 300
CHECK_TOOLS = [
    # (cmd list, save_as)
    (['python', 'tools/check_latest_symbols.py', '--timeframes', '5m,15m,30m,1h,2h,4h,1d', '--limit', '40'], 'checks/check_latest_symbols.txt'),
]

def redact(text: str) -> str:
    # 将密钥类字段的值替换为 ****
    return SENSITIVE_RE.sub(r'\1\2****\4', text)

def safe_read_text(p: Path, limit_mb=5) -> str:
    try:
        if p.stat().st_size > limit_mb * 1024 * 1024:
            return f"[SKIP large file: {p.name} >{limit_mb}MB]\n"
        with p.open('r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception as e:
        return f"[ERROR reading {p}: {e}]\n"

def tail_file(p: Path, lines=TAIL_LINES) -> str:
    try:
        with p.open('rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 4096
            data = b''
            while size > 0 and data.count(b'\n') <= lines:
                step = min(block, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
            text = data.decode('utf-8', errors='ignore')
            return "\n".join(text.splitlines()[-lines:])
    except Exception as e:
        return f"[ERROR tail {p}: {e}]"

def list_tree(root: Path, max_depth=3):
    out = []
    root = root.resolve()
    for path, dirs, files in os.walk(root):
        rel = Path(path).relative_to(root)
        depth = len(rel.parts)
        # 剪枝
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        if depth > max_depth:
            dirs[:] = []
            continue
        prefix = "  " * depth
        out.append(f"{prefix}{rel if str(rel)!='.' else '.'}/")
        for fn in sorted(files):
            out.append(f"{prefix}  {fn}")
    return "\n".join(out)

def collect_meta(project_dir: Path):
    info = {
        "now": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "python": sys.version.replace("\n", " "),
        "platform": sys.platform,
        "cwd": str(project_dir),
    }
    # pip freeze（尽力而为）
    try:
        frz = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True, timeout=30)
        info["pip_freeze"] = frz.stdout.splitlines()[:500]
    except Exception as e:
        info["pip_freeze_error"] = str(e)
    return info

def collect_recent_files(project_dir: Path, hours=RECENT_HOURS):
    cutoff = time.time() - hours * 3600
    items = []
    for path, dirs, files in os.walk(project_dir):
        rel = Path(path).relative_to(project_dir)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        for fn in files:
            fp = Path(path) / fn
            try:
                st = fp.stat()
                if st.st_mtime >= cutoff and st.st_size <= 5 * 1024 * 1024:
                    items.append({
                        "path": str(fp.relative_to(project_dir)),
                        "mtime": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        "size_kb": round(st.st_size / 1024, 1)
                    })
            except Exception:
                pass
    items.sort(key=lambda x: x["mtime"], reverse=True)
    return items[:500]

def dump_db_meta(db_path: Path):
    meta = {"ok": False, "error": None, "pragma": {}, "tables": []}
    if not db_path.exists():
        meta["error"] = f"DB not found: {db_path}"
        return meta
    try:
        con = sqlite3.connect(str(db_path))
        con.execute("PRAGMA busy_timeout=3000;")
        cur = con.cursor()
        pragmas = ["journal_mode", "page_size", "synchronous", "locking_mode", "wal_autocheckpoint"]
        for k in pragmas:
            try:
                val = cur.execute(f"PRAGMA {k};").fetchone()
                meta["pragma"][k] = val[0] if val else None
            except Exception:
                meta["pragma"][k] = None
        # tables
        tbls = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;").fetchall()
        tbls = [t[0] for t in tbls]
        for name in tbls:
            row = {"name": name, "max_ts": None, "min_ts": None, "cols": []}
            try:
                cols = cur.execute(f"PRAGMA table_info('{name}')").fetchall()
                row["cols"] = [c[1] for c in cols]
                if "timestamp" in row["cols"]:
                    row["max_ts"] = cur.execute(f"SELECT MAX(timestamp) FROM '{name}';").fetchone()[0]
                    row["min_ts"] = cur.execute(f"SELECT MIN(timestamp) FROM '{name}';").fetchone()[0]
            except Exception as e:
                row["error"] = str(e)
            meta["tables"].append(row)
        meta["ok"] = True
        con.close()
    except Exception as e:
        meta["error"] = str(e)
    return meta

def try_run_checks(project_dir: Path, db_path: Path):
    results = {}
    env = os.environ.copy()
    # 若用户的工具存在，则追加 --db
    for cmd, save_as in CHECK_TOOLS:
        out = ""
        try:
            full = list(cmd)
            # 如果目标脚本存在且支持 --db，则加上
            if Path(project_dir, cmd[1]).exists() and db_path:
                full += ["--db", str(db_path)]
            p = subprocess.run(full, cwd=str(project_dir), capture_output=True, text=True, timeout=120)
            out = p.stdout + ("\n[stderr]\n" + p.stderr if p.stderr.strip() else "")
        except Exception as e:
            out = f"[ERROR running {' '.join(cmd)}: {e}]"
        results[save_as] = out
    return results

def add_to_zip(zf: zipfile.ZipFile, arcname: str, content: str):
    zf.writestr(arcname, content if isinstance(content, str) else json.dumps(content, ensure_ascii=False, indent=2))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', type=str, default='', help='Path to SQLite DB (optional but recommended)')
    ap.add_argument('--out', type=str, default='', help='Output zip path')
    ap.add_argument('--project-dir', type=str, default='.', help='Project root (default=.)')
    args = ap.parse_args()

    project_dir = Path(args.project_dir).resolve()
    db_path = Path(args.db).resolve() if args.db else None
    out_zip = Path(args.out) if args.out else Path(project_dir, f"audit_bundle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")

    with zipfile.ZipFile(out_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        # meta
        add_to_zip(zf, "meta/info.json", collect_meta(project_dir))
        add_to_zip(zf, "meta/tree.txt", list_tree(project_dir, max_depth=3))
        add_to_zip(zf, "meta/recent_files.json", collect_recent_files(project_dir))

        # configs (sanitized)
        for p in project_dir.rglob("*"):
            if p.is_file() and p.suffix.lower() in CONFIG_EXTS:
                if any(part in EXCLUDE_DIRS for part in p.relative_to(project_dir).parts):
                    continue
                raw = safe_read_text(p, limit_mb=2)
                add_to_zip(zf, f"configs_sanitized/{p.relative_to(project_dir)}", redact(raw))

        # db meta
        if db_path:
            add_to_zip(zf, "db/target.txt", str(db_path))
            add_to_zip(zf, "db/meta.json", dump_db_meta(db_path))

        # logs tail
        logs_dir = project_dir / "logs"
        if logs_dir.exists():
            for p in logs_dir.rglob("*"):
                if p.is_file() and p.suffix.lower() in LOG_EXTS:
                    rel = p.relative_to(project_dir)
                    add_to_zip(zf, f"logs_tail/{rel}", tail_file(p, TAIL_LINES))

        # optional checks
        checks = try_run_checks(project_dir, db_path if db_path else Path())
        for rel, txt in checks.items():
            add_to_zip(zf, rel, txt)

    print(f"[OK] Audit bundle written to: {out_zip}")

if __name__ == "__main__":
    main()
