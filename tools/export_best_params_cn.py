# -*- coding: utf-8 -*-
import os, sys, json, argparse, sqlite3, pandas as pd

CN_HEADERS=["序号","合约","中文名","周期","策略","参数(JSON)","年化收益(%)","最大回撤(%)",
            "交易次数","分数","换手率","费用(单边%)","滑点(单边%)","执行口径","更新时间",
            "达标(可部署)","已审核(已发布)"]
TF_ORDER={"5m":1,"15m":2,"30m":3,"1h":4,"2h":5,"4h":6,"1d":7}

def table_exists(con,name):
    try:
        return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None
    except: return False

def pct100(x):
    try: return round(abs(float(x))*100,2)
    except: return ""

def sround(x,nd=4):
    try: return round(float(x),nd)
    except: return x

def load_cn_map(path):
    mp={}
    if path and os.path.exists(path):
        try:
            df=pd.read_csv(path,encoding="utf-8")
            for _,r in df.iterrows():
                s=str(r.get("symbol") or "").upper().strip()
                c=str(r.get("中文名称") or r.get("cn") or "").strip()
                if s and c: mp[s]=c
        except: pass
    return mp

def read_db(db):
    con=sqlite3.connect(db, timeout=30)
    try:
        if not table_exists(con,"best_params"): return None,None
        df=pd.read_sql("""SELECT symbol,timeframe,strategy,params_json,
                                 metric_return AS ret,metric_trades AS trades,
                                 score, dd, turnover, updated_at
                          FROM best_params""", con)
        meta=None
        if table_exists(con,"best_params_meta"):
            meta=pd.read_sql("""SELECT symbol,timeframe,strategy,params_json,
                                       eligible_live,approved_live,fee_bps,slip_bps,exec_lag,no_intrabar,
                                       score_def,created_at
                                FROM best_params_meta""", con)
        return df, meta
    finally:
        con.close()

def read_json(path):
    if not os.path.exists(path): return None
    arr=json.load(open(path,"r",encoding="utf-8"))
    rows=[]
    for it in arr:
        rows.append({
            "symbol":it.get("symbol"),
            "timeframe":it.get("tf") or it.get("timeframe"),
            "strategy":it.get("strategy"),
            "params_json":json.dumps(it.get("params",{}),ensure_ascii=False),
            "ret":(it.get("metrics") or {}).get("return"),
            "trades":(it.get("metrics") or {}).get("trades"),
            "score":(it.get("metrics") or {}).get("score"),
            "dd":(it.get("metrics") or {}).get("dd"),
            "turnover":(it.get("metrics") or {}).get("turnover"),
            "updated_at": it.get("updated_at") or ""
        })
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--json", default=r"deploy\live_best_params.json")
    ap.add_argument("--out", default=r"deploy\最佳参数表_A1A8.csv")
    ap.add_argument("--xlsx", default=r"deploy\最佳参数表_A1A8.xlsx")
    ap.add_argument("--symbol-cn-map", default=r"tools\symbol_cn_map.csv")
    ap.add_argument("--eligible-only", type=int, default=0)
    ap.add_argument("--approved-only", type=int, default=0)
    args=ap.parse_args()

    df_base, df_meta = read_db(args.db)
    if df_base is None:
        df_base = read_json(args.json)
        df_meta = None
        if df_base is None:
            print("[ERR] 无可导出数据"); sys.exit(2)

    if df_meta is not None and not df_meta.empty:
        on=["symbol","timeframe","strategy","params_json"]
        df=df_base.merge(df_meta,on=on,how="left")
    else:
        df=df_base.copy()
        for col in ["eligible_live","approved_live","fee_bps","slip_bps","exec_lag","no_intrabar","score_def"]:
            df[col]=None

    # 过滤（只在用户要求时）
    if args.eligible_only:
        df=df[(df["eligible_live"]==1) | (df["eligible_live"]=="1")]
    if args.approved_only:
        df=df[(df["approved_live"]==1) | (df["approved_live"]=="1")]

    if df is None or len(df)==0:
        # 空集也要导出空表头 CSV/XLSX
        empty=pd.DataFrame(columns=CN_HEADERS)
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        empty.to_csv(args.out, index=False, encoding="utf-8-sig")
        try:
            if args.xlsx: empty.to_excel(args.xlsx, index=False)
        except Exception:
            pass
        print(f"🟡 没有符合条件的记录，已导出空表：{args.out}")
        sys.exit(0)

    df["timeframe"]=df["timeframe"].astype(str)
    tf_order=df["timeframe"].map(lambda x: TF_ORDER.get(x,99)).tolist()

    # 一维化/标量化所有输出列
    sym      = df["symbol"].astype(str).tolist()
    tf       = df["timeframe"].astype(str).tolist()
    strat    = df["strategy"].astype(str).tolist()
    params_s = df["params_json"].astype(str).tolist()
    ret_pct  = [pct100(x) for x in df["ret"].tolist()]
    dd_pct   = [pct100(x) for x in df["dd"].tolist()]
    trades   = df["trades"].tolist()
    score    = [sround(x,4) for x in df["score"].tolist()]
    turnover = [sround(x,4) for x in df["turnover"].tolist()]
    fee_col  = [sround(float(x)/100.0,4) if x is not None and x!="" else "" for x in df["fee_bps"].tolist()]
    slip_col = [sround(float(x)/100.0,4) if x is not None and x!="" else "" for x in df["slip_bps"].tolist()]
    elag     = [int(x) if x not in (None,"") else 1 for x in df["exec_lag"].tolist()]
    noi      = [int(x) if x not in (None,"") else 1 for x in df["no_intrabar"].tolist()]
    execcol  = [f"t-1收盘→t开盘 (lag={a}, no_intrabar={b})" for a,b in zip(elag,noi)]
    updated  = [("" if x is None else str(x)) for x in df["updated_at"].tolist()]
    eligible = [("是" if str(x)=="1" else ("否" if str(x)=="0" else "")) for x in df["eligible_live"].tolist()]
    approved = [("是" if str(x)=="1" else ("否" if str(x)=="0" else "")) for x in df["approved_live"].tolist()]

    out=pd.DataFrame({
        "合约": sym,
        "中文名": ["" for _ in sym],
        "周期": tf,
        "策略": strat,
        "参数(JSON)": params_s,
        "年化收益(%)": ret_pct,
        "最大回撤(%)": dd_pct,
        "交易次数": trades,
        "分数": score,
        "换手率": turnover,
        "费用(单边%)": fee_col,
        "滑点(单边%)": slip_col,
        "执行口径": execcol,
        "更新时间": updated,
        "达标(可部署)": eligible,
        "已审核(已发布)": approved,
        "__tf_order": tf_order,
        "__score": [float(v) if v not in ("",None) else -1e9 for v in score]
    })

    # 中文名映射（可选）
    mp=load_cn_map(args.symbol_cn_map)
    if mp:
        out["中文名"]=out["合约"].map(lambda s: mp.get(str(s).upper(),""))

    out.sort_values(["__tf_order","__score","合约"], ascending=[True,False,True], inplace=True)
    out.drop(columns=["__tf_order","__score"], inplace=True)
    out.insert(0,"序号", range(1,len(out)+1))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig", header=CN_HEADERS)
    try:
        if args.xlsx:
            out.to_excel(args.xlsx, index=False)
    except Exception:
        pass

    print(f"🟢 已导出：{args.out}  行数={len(out)}")
    if args.xlsx and os.path.exists(args.xlsx):
        print(f"🟢 已导出：{args.xlsx}")

if __name__=="__main__":
    main()
