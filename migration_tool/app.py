import os
import json
from datetime import datetime


def _log_path():
    base = os.path.dirname(__file__)
    p = os.path.join(base, "logs", "migration.log")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p


def write_log(event: dict):
    p = _log_path()
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def main():
    import streamlit as st
    try:
        from migration_tool.converter.oracle_to_snowflake import convert
        from migration_tool.db.oracle_client import OracleClient
        from migration_tool.db.snowflake_client import SnowflakeClient
        from migration_tool.ai_agent.log_analyzer import analyze_logs
    except ImportError:
        from converter.oracle_to_snowflake import convert
        from db.oracle_client import OracleClient
        from db.snowflake_client import SnowflakeClient
        from ai_agent.log_analyzer import analyze_logs

    st.set_page_config(page_title="Oracle → Snowflake Migration Tool", page_icon="🧭", layout="wide")
    st.title("Oracle → Snowflake SQL 转换与测试工具(BETA)")
    st.markdown(
        """
        <div style="padding:16px;border-radius:14px;background:linear-gradient(90deg,#1f6feb 0%,#2ea043 100%);color:#fff;box-shadow:0 8px 24px rgba(0,0,0,.15);">
          <div style="font-size:18px;font-weight:700;">高效完成 Oracle → Snowflake 迁移与验证</div>
          <div style="opacity:0.95;margin-top:6px;">集成 SQL 转换、连接测试、执行校验、日志与 AI 分析</div>
        </div>
        <style>
          .stButton>button{border-radius:10px;padding:0.55rem 1.05rem;background:#1f6feb;color:#fff;border:0}
          .stButton>button:hover{background:#1559c0}
          .stTextInput>div>div>input{border-radius:10px}
          .stTextArea textarea{border-radius:10px}
          pre, code{border-radius:10px}
          .card{background:#fff;border-radius:12px;padding:16px;box-shadow:0 4px 16px rgba(0,0,0,.08);border:1px solid rgba(0,0,0,.06)}
          .muted{opacity:.8}
          .section-title{font-weight:700;margin-bottom:8px}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.subheader("🧩 SQL 转换")
    cA, cB = st.columns([1,1])
    with cA:
        st.markdown("<div class='card'><div class='section-title'>输入与规则</div>", unsafe_allow_html=True)
        oracle_sql = st.text_area("Oracle SQL 输入", height=160, key="oracle_sql")
        default_rules_path = os.path.join(os.path.dirname(__file__), "converter", "rules.json")
        rules_path = st.text_input("规则文件路径", default_rules_path, key="rules_path_input", help="从文件加载/保存转换规则")
        with st.expander("高级规则配置", expanded=False):
            rules_json = st.text_area("规则配置(JSON，可选)", "", height=120, key="rules_json", help="可覆盖/扩展函数映射与正则规则")
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("加载规则文件", key="btn_load_rules"):
                    try:
                        with open(rules_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        st.session_state["rules_loaded"] = data
                        st.success("已加载规则文件")
                        st.caption(f"来源: {rules_path}")
                        st.write(data)
                    except Exception as e:
                        st.error(f"加载失败: {e}")
            with c2:
                if st.button("保存当前规则为文件", key="btn_save_rules"):
                    try:
                        data = None
                        if rules_json.strip():
                            data = json.loads(rules_json)
                        elif "rules_loaded" in st.session_state:
                            data = st.session_state["rules_loaded"]
                        else:
                            data = {
                                "replacements": [
                                    [r"\bNVL\s*\(", "COALESCE("],
                                    [r"\bSYSDATE\b", "CURRENT_TIMESTAMP()"],
                                    [r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP()"],
                                    [r"\bFROM\s+DUAL\b", ""],
                                ],
                                "regex": [
                                    {"pattern": r"\bTRUNC\s*\(\s*([^,\)]+)\s*\)", "repl": r"DATE_TRUNC('day', \1)"},
                                    {"pattern": r"\bTRUNC\s*\(\s*([^,\)]+)\s*,\s*'?(MONTH|MON)'?\s*\)", "repl": r"DATE_TRUNC('month', \1)"},
                                    {"pattern": r"\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^\)]+)\)", "repl": r"DATEADD(month, \2, \1)"},
                                    {"pattern": r"\bDECODE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,\)]+)\s*(?:,\s*([^\)]+))?\)", "repl": r"CASE WHEN \1=\2 THEN \3 ELSE \4 END"},
                                    {"pattern": r"\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\)", "repl": r"IFF(\1 IS NOT NULL, \2, \3)"},
                                ],
                                "warnings": [
                                    {"pattern": r"\bCONNECT\s+BY\b", "message": "CONNECT BY detected; manual rewrite to WITH RECURSIVE required"},
                                ],
                            }
                        os.makedirs(os.path.dirname(rules_path), exist_ok=True)
                        with open(rules_path, "w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        st.success(f"规则已保存到: {rules_path}")
                    except Exception as e:
                        st.error(f"保存失败: {e}")
            with c3:
                if st.button("一键生成示例规则文件", key="btn_gen_rules"):
                    try:
                        sample = {
                            "replacements": [
                                [r"\bNVL\s*\(", "COALESCE("],
                                [r"\bSYSDATE\b", "CURRENT_TIMESTAMP()"],
                                [r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP()"],
                                [r"\bFROM\s+DUAL\b", ""],
                            ],
                            "regex": [
                                {"pattern": r"\bTRUNC\s*\(\s*([^,\)]+)\s*\)", "repl": r"DATE_TRUNC('day', \1)"},
                                {"pattern": r"\bTRUNC\s*\(\s*([^,\)]+)\s*,\s*'?(MONTH|MON)'?\s*\)", "repl": r"DATE_TRUNC('month', \1)"},
                                {"pattern": r"\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^\)]+)\)", "repl": r"DATEADD(month, \2, \1)"},
                                {"pattern": r"\bDECODE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,\)]+)\s*(?:,\s*([^\)]+))?\)", "repl": r"CASE WHEN \1=\2 THEN \3 ELSE \4 END"},
                                {"pattern": r"\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\)", "repl": r"IFF(\1 IS NOT NULL, \2, \3)"},
                            ],
                            "warnings": [
                                {"pattern": r"\bCONNECT\s+BY\b", "message": "CONNECT BY detected; manual rewrite to WITH RECURSIVE required"},
                            ],
                        }
                        os.makedirs(os.path.dirname(rules_path), exist_ok=True)
                        with open(rules_path, "w", encoding="utf-8") as f:
                            json.dump(sample, f, ensure_ascii=False, indent=2)
                        st.success(f"示例规则文件已生成: {rules_path}")
                    except Exception as e:
                        st.error(f"生成失败: {e}")
        st.markdown("</div>", unsafe_allow_html=True)
    with cB:
        st.markdown("<div class='card'><div class='section-title'>转换结果</div>", unsafe_allow_html=True)
        st.caption("点击“转换 SQL”后展示结果")
        result_placeholder = st.empty()
        warn_placeholder = st.empty()
        st.markdown("</div>", unsafe_allow_html=True)
    def _merge_rules(a: dict | None, b: dict | None):
        a = a or {}
        b = b or {}
        out = {"replacements": [], "regex": [], "warnings": []}
        out["replacements"] = (a.get("replacements") or []) + (b.get("replacements") or [])
        out["regex"] = (a.get("regex") or []) + (b.get("regex") or [])
        out["warnings"] = (a.get("warnings") or []) + (b.get("warnings") or [])
        return out

    if st.button("转换 SQL", key="btn_convert"):
        rules_in_text = None
        rules_loaded = st.session_state.get("rules_loaded")
        rules_text = st.session_state.get("rules_json", "")
        if rules_text.strip():
            try:
                rules_in_text = json.loads(rules_text)
            except Exception as e:
                st.error(f"规则解析失败: {e}")
        rules = _merge_rules(rules_loaded, rules_in_text)
        converted_sql, warnings = convert(oracle_sql or "", rules=rules)
        with cB:
            result_placeholder.code(converted_sql or "", language="sql")
            if warnings:
                warn_placeholder.warning("; ".join(warnings))
        write_log({
            "timestamp": datetime.utcnow().isoformat(),
            "event": "convert",
            "input_sql": oracle_sql,
            "converted_sql": converted_sql,
            "warnings": warnings,
            "error": None,
        })
    st.divider()

    st.subheader("🔌 数据库连接配置")
    tab1, tab2 = st.tabs(["Oracle", "Snowflake"])
    with tab1:
        oc1, oc2 = st.columns(2)
        with oc1:
            o_host = st.text_input("host", "localhost", key="oracle_host", help="数据库主机，如 127.0.0.1")
            o_port = st.text_input("port", "1521", key="oracle_port", help="监听端口，常见为 1521")
            o_service = st.text_input("service_name", "xepdb1", key="oracle_service", help="PDB 服务名，如 xepdb1")
            o_sid = st.text_input("sid", "", key="oracle_sid", help="CDB 的 SID，如 xe")
            o_ez = st.text_input("EZ Connect (host:port/service)", "", key="oracle_ez", help="直接填写连接串，例如 127.0.0.1:1521/xepdb1")
        with oc2:
            o_user = st.text_input("user", "", key="oracle_user", help="数据库用户名")
            o_password = st.text_input("password", "", type="password", key="oracle_password")
            if st.button("测试连接 (Oracle)", key="btn_test_oracle"):
                client = OracleClient({
                    "host": o_host,
                    "port": o_port,
                    "service_name": o_service,
                    "sid": o_sid,
                    "connect_string": o_ez,
                    "user": o_user,
                    "password": o_password,
                })
                ok, ms, err = client.test_connection()
                if ok:
                    st.success(f"连接成功，用时 {ms} ms")
                else:
                    st.error(f"连接失败：{err}")
                write_log({
                    "timestamp": datetime.utcnow().isoformat(),
                    "event": "test_connection",
                    "db": "oracle",
                    "elapsed_ms": ms,
                    "error": None if ok else err,
                })
    with tab2:
        sc1, sc2 = st.columns(2)
        with sc1:
            s_account = st.text_input("account", "", key="snowflake_account", help="账户标识，例如 xy12345")
            s_user = st.text_input("user", "", key="snowflake_user", help="用户名")
            s_password = st.text_input("password", "", type="password", key="snowflake_password")
        with sc2:
            s_warehouse = st.text_input("warehouse", "", key="snowflake_warehouse", help="虚拟仓库")
            s_database = st.text_input("database", "", key="snowflake_database", help="数据库")
            s_schema = st.text_input("schema", "", key="snowflake_schema", help="模式")
            s_role = st.text_input("role", "", key="snowflake_role", help="角色")
        st.divider()
        if st.button("测试连接 (Snowflake)", key="btn_test_snowflake"):
            client = SnowflakeClient({
                "account": s_account,
                "user": s_user,
                "password": s_password,
                "warehouse": s_warehouse,
                "database": s_database,
                "schema": s_schema,
                "role": s_role,
            })
            ok, ms, err = client.test_connection()
            if ok:
                st.success(f"连接成功，用时 {ms} ms")
            else:
                st.error(f"连接失败：{err}")
            write_log({
                "timestamp": datetime.utcnow().isoformat(),
                "event": "test_connection",
                "db": "snowflake",
                "elapsed_ms": ms,
                "error": None if ok else err,
            })

    st.subheader("🧪 执行与一致性")
    t_exec, t_cons = st.tabs(["执行 SQL", "一致性对比"])
    with t_exec:
        exec_db = st.selectbox("选择执行数据库", ["oracle", "snowflake"], key="exec_db")
        exec_sql_src = st.radio("选择执行的 SQL", ["Oracle 原 SQL", "转换后 Snowflake SQL"], key="exec_sql_src") 
        exec_epoch_convert = st.checkbox("自动识别时间戳并转日期", value=True, key="exec_epoch")
        exec_sql = oracle_sql or ""
        if exec_sql_src == "转换后 Snowflake SQL":
            exec_sql, _ = convert(exec_sql)
        if st.button("执行 SQL", key="btn_exec"):
            if exec_db == "oracle":
                client = OracleClient({
                    "host": o_host,
                    "port": o_port,
                    "service_name": o_service,
                    "sid": o_sid,
                    "connect_string": o_ez,
                    "user": o_user,
                    "password": o_password,
                })
            else:
                client = SnowflakeClient({
                    "account": s_account,
                    "user": s_user,
                    "password": s_password,
                    "warehouse": s_warehouse,
                    "database": s_database,
                    "schema": s_schema,
                    "role": s_role,
                })
            data, ms, err = client.execute(exec_sql)
            write_log({
                "timestamp": datetime.utcnow().isoformat(),
                "event": "execute",
                "db": exec_db,
                "executed_sql": exec_sql,
                "elapsed_ms": ms,
                "rows": len(data),
                "error": err,
            })
            if err:
                st.error(err)
            else:
                st.success(f"执行成功，用时 {ms} ms，返回 {len(data)} 行")
                if data:
                    from datetime import datetime as _dt
                    def _n(v):
                        if v is None:
                            return None
                        if isinstance(v, (int, float)):
                            x = float(v)
                            t = None
                            if x >= 1e11:
                                t = _dt.utcfromtimestamp(x / 1000.0)
                            elif x >= 1e9:
                                t = _dt.utcfromtimestamp(x)
                            if t is not None:
                                return t.date().isoformat()
                            return v
                        if isinstance(v, _dt):
                            return v.replace(microsecond=0).date().isoformat()
                        if isinstance(v, str):
                            s0 = v.strip()
                            try:
                                t = _dt.fromisoformat(s0)
                                return t.date().isoformat()
                            except Exception:
                                try:
                                    t = _dt.strptime(s0, "%Y-%m-%d")
                                    return t.date().isoformat()
                                except Exception:
                                    return v
                        return v
                    norm_data = [{k: (_n(v) if exec_epoch_convert else v) for k, v in r.items()} for r in data]
                    st.dataframe(norm_data, use_container_width=True)
                    try:
                        import pandas as pd
                        df = pd.DataFrame(norm_data if exec_epoch_convert else data)
                        from datetime import datetime as _dt, date as _date
                        st.download_button("下载 JSON", json.dumps(norm_data if exec_epoch_convert else data, ensure_ascii=False, indent=2, default=lambda o: o.isoformat() if isinstance(o, (_dt, _date)) else str(o)), file_name="exec_result.json", mime="application/json", key="dl_exec_json")
                        st.download_button("下载 CSV", df.to_csv(index=False), file_name="exec_result.csv", mime="text/csv", key="dl_exec_csv")
                    except Exception:
                        from datetime import datetime as _dt, date as _date
                        st.download_button("下载 JSON", json.dumps(norm_data if exec_epoch_convert else data, ensure_ascii=False, default=lambda o: o.isoformat() if isinstance(o, (_dt, _date)) else str(o)), file_name="exec_result.json", mime="application/json", key="dl_exec_json")
                        headers = list((norm_data if exec_epoch_convert else data)[0].keys())
                        rows = [headers] + [[str(r.get(h, "")) for h in headers] for r in (norm_data if exec_epoch_convert else data)]
                        csv = "\n".join([",".join(row) for row in rows])
                        st.download_button("下载 CSV", csv, file_name="exec_result.csv", mime="text/csv", key="dl_exec_csv")
    with t_cons:
        compare_mode = st.radio("对比模式", ["按表对比", "按SQL对比"], index=0, key="cons_mode")
        sort_cols = st.text_input("排序/对齐列(逗号分隔)", "", key="cons_sort_cols")
        pk_cols = st.text_input("主键列(逗号分隔)", "", key="cons_pk_cols")
        ignore_case = st.checkbox("字符串忽略大小写", value=True, key="cons_ignore_case")
        num_tol = st.number_input("数值容差", min_value=0.0, max_value=1.0, value=0.0, step=0.0001, key="cons_num_tol")
        trunc_ts = st.checkbox("时间比较截断到秒", value=True, key="cons_trunc_ts")
        nfkc_norm = st.checkbox("字符归一化(NFKC)", value=True, key="cons_nfkc")
        tz_offset_min = st.number_input("时区偏移(分钟，JST=540)", min_value=-720.0, max_value=840.0, value=540.0, step=1.0, key="cons_tz_offset")
        src_table = ""
        tgt_table = ""
        sel_cols = ""
        where_clause = ""
        if compare_mode == "按表对比":
            src_table = st.text_input("源表(可含schema)", "", key="cons_src_table")
            tgt_table = st.text_input("目标表(可含db.schema)", "", key="cons_tgt_table")
            sel_cols = st.text_input("列选择(逗号，默认*)", "", key="cons_sel_cols")
            where_clause = st.text_input("条件(不含WHERE，选填)", "", key="cons_where")
        if oracle_sql:
            if compare_mode == "按SQL对比":
                preview_sql, _ = convert(oracle_sql or "")
                st.caption("转换后 SQL 预览")
                st.code(preview_sql or "", language="sql")
            else:
                scols = sel_cols.strip() or "*"
                w = where_clause.strip()
                src_preview = f"SELECT {scols} FROM {src_table}" + (f" WHERE {w}" if w else "")
                tgt_full = tgt_table.strip()
                if tgt_full and "." not in tgt_full and (s_database and s_schema):
                    tgt_full = f"{s_database}.{s_schema}.{tgt_full}"
                tgt_preview = f"SELECT {scols} FROM {tgt_full}" + (f" WHERE {w}" if w else "")
                st.caption("预览：源/目标对比 SQL")
                st.code(src_preview or "", language="sql")
                st.code(tgt_preview or "", language="sql")
        if st.button("开始一致性测试", key="btn_consistency"):
            o_client = OracleClient({
                "host": o_host,
                "port": o_port,
                "service_name": o_service,
                "sid": o_sid,
                "connect_string": o_ez,
                "user": o_user,
                "password": o_password,
            })
            s_client = SnowflakeClient({
                "account": s_account,
                "user": s_user,
                "password": s_password,
                "warehouse": s_warehouse,
                "database": s_database,
                "schema": s_schema,
                "role": s_role,
            })
            if compare_mode == "按SQL对比":
                src_sql = oracle_sql or ""
                tgt_sql, _ = convert(src_sql)
                src_tbl_meta = None
                tgt_tbl_meta = None
            else:
                scols = sel_cols.strip() or "*"
                w = where_clause.strip()
                src_sql = f"SELECT {scols} FROM {src_table}" + (f" WHERE {w}" if w else "")
                tgt_full = tgt_table.strip()
                if tgt_full and "." not in tgt_full and (s_database and s_schema):
                    tgt_full = f"{s_database}.{s_schema}.{tgt_full}"
                tgt_sql = f"SELECT {scols} FROM {tgt_full}" + (f" WHERE {w}" if w else "")
                src_tbl_meta = src_table
                tgt_tbl_meta = tgt_full
            o_data, o_ms, o_err = o_client.execute(src_sql)
            s_data, s_ms, s_err = s_client.execute(tgt_sql)
            def _normalize(v):
                import unicodedata as _ud
                from datetime import datetime as _dt, date as _date, timedelta as _td
                if v is None:
                    return None
                if isinstance(v, (int, float)):
                    x = float(v)
                    t = None
                    if x >= 1e11:
                        t = _dt.utcfromtimestamp(x / 1000.0)
                    elif x >= 1e9:
                        t = _dt.utcfromtimestamp(x)
                    if t is not None:
                        d = t.replace(microsecond=0) if trunc_ts else t
                        try:
                            off = int(tz_offset_min)
                            d = d + _td(minutes=off) if off != 0 else d
                        except Exception:
                            pass
                        return d
                    return x
                if isinstance(v, _dt):
                    d = v.replace(microsecond=0) if trunc_ts else v
                    try:
                        off = int(tz_offset_min)
                        d = d + _td(minutes=off) if off != 0 else d
                    except Exception:
                        pass
                    return d
                if isinstance(v, _date):
                    t = _dt(v.year, v.month, v.day)
                    d = t.replace(microsecond=0) if trunc_ts else t
                    try:
                        off = int(tz_offset_min)
                        d = d + _td(minutes=off) if off != 0 else d
                    except Exception:
                        pass
                    return d
                if isinstance(v, str):
                    s0 = v.strip()
                    t = None
                    try:
                        t = _dt.fromisoformat(s0)
                    except Exception:
                        t = None
                    if t is None:
                        try:
                            t = _dt.strptime(s0, "%Y-%m-%d")
                        except Exception:
                            t = None
                    if t is not None:
                        d = t.replace(microsecond=0) if trunc_ts else t
                        try:
                            off = int(tz_offset_min)
                            d = d + _td(minutes=off) if off != 0 else d
                        except Exception:
                            pass
                        return d
                    s = _ud.normalize("NFKC", v) if nfkc_norm else v
                    return s.lower() if ignore_case else s
                try:
                    return float(v)
                except Exception:
                    s = str(v)
                    s = _ud.normalize("NFKC", s) if nfkc_norm else s
                    return s.lower() if ignore_case else s
            def _sort_rows(rows, cols):
                cs = [c.strip() for c in (cols or "").split(",") if c.strip()]
                if not cs or not rows:
                    return rows
                def kf(r):
                    return tuple(_normalize(r.get(c)) for c in cs)
                try:
                    return sorted(rows, key=kf)
                except Exception:
                    return rows
            def _keyed_map(rows, cols):
                cs = [c.strip() for c in (cols or "").split(",") if c.strip()]
                if not cs or not rows:
                    return {}
                m = {}
                for r in rows:
                    k = tuple(_normalize(r.get(c)) for c in cs)
                    m[k] = {kk.lower(): _normalize(vv) for kk, vv in r.items()}
                return m
            report = {
                "source_rows": len(o_data),
                "target_rows": len(s_data),
                "source_error": o_err,
                "target_error": s_err,
                "row_match": None,
                "columns_match": None,
                "source_columns": [],
                "target_columns": [],
                "column_diff": {"missing_in_target": [], "missing_in_source": []},
                "missing_keys_in_target": [],
                "missing_keys_in_source": [],
                "samples_mismatch": [],
                "elapsed_ms": {"oracle": o_ms, "snowflake": s_ms},
            }
            if not o_err and not s_err:
                o_cols = list(o_data[0].keys()) if o_data else []
                s_cols = list(s_data[0].keys()) if s_data else []
                report["source_columns"] = o_cols
                report["target_columns"] = s_cols
                o_set = {c.lower() for c in o_cols}
                s_set = {c.lower() for c in s_cols}
                report["columns_match"] = o_set == s_set
                report["column_diff"]["missing_in_target"] = sorted(list(o_set - s_set))
                report["column_diff"]["missing_in_source"] = sorted(list(s_set - o_set))
                if pk_cols.strip():
                    om = _keyed_map(o_data, pk_cols)
                    sm = _keyed_map(s_data, pk_cols)
                    ko = set(om.keys())
                    ks = set(sm.keys())
                    report["missing_keys_in_target"] = sorted(list(ko - ks))
                    report["missing_keys_in_source"] = sorted(list(ks - ko))
                    report["row_match"] = len(report["missing_keys_in_target"]) == 0 and len(report["missing_keys_in_source"]) == 0 and len(ko) == len(ks)
                    inter = sorted(list(ko & ks))[:50]
                    for i, k in enumerate(inter):
                        so = om.get(k) or {}
                        stg = sm.get(k) or {}
                        all_keys = sorted(set(so.keys()) | set(stg.keys()))
                        mismatch = False
                        for kk in all_keys:
                            a = so.get(kk)
                            b = stg.get(kk)
                            if isinstance(a, float) and isinstance(b, float) and num_tol > 0:
                                if abs(a - b) > num_tol:
                                    mismatch = True
                                    break
                            else:
                                if a != b:
                                    mismatch = True
                                    break
                        if mismatch:
                            report["samples_mismatch"].append({"index": i, "key": k, "source": so, "target": stg})
                else:
                    od = _sort_rows(o_data, sort_cols)
                    sd = _sort_rows(s_data, sort_cols)
                    report["row_match"] = len(od) == len(sd)
                    n = min(20, len(od), len(sd))
                    for i in range(n):
                        so = {k.lower(): _normalize(v) for k, v in od[i].items()}
                        stg = {k.lower(): _normalize(v) for k, v in sd[i].items()}
                        all_keys = sorted(set(so.keys()) | set(stg.keys()))
                        mismatch = False
                        for k in all_keys:
                            a = so.get(k)
                            b = stg.get(k)
                            if isinstance(a, float) and isinstance(b, float) and num_tol > 0:
                                if abs(a - b) > num_tol:
                                    mismatch = True
                                    break
                            else:
                                if a != b:
                                    mismatch = True
                                    break
                        if mismatch:
                            report["samples_mismatch"].append({"index": i, "source": so, "target": stg})
            write_log({
                "timestamp": datetime.utcnow().isoformat(),
                "event": "consistency",
                "source_sql": src_sql,
                "target_sql": tgt_sql,
                "source_table": src_tbl_meta,
                "target_table": tgt_tbl_meta,
                "summary": {
                    "row_match": report["row_match"],
                    "columns_match": report["columns_match"],
                    "source_rows": report["source_rows"],
                    "target_rows": report["target_rows"],
                },
                "error": {"oracle": o_err, "snowflake": s_err},
            })
            if o_err or s_err:
                if o_err:
                    st.error(f"源库执行失败：{o_err}")
                if s_err:
                    st.error(f"目标库执行失败：{s_err}")
            st.json({
                "汇总": {
                    "行数一致": report["row_match"],
                    "列一致": report["columns_match"],
                    "源行数": report["source_rows"],
                    "目标行数": report["target_rows"],
                },
                "列差异": report["column_diff"],
                "耗时ms": report["elapsed_ms"],
            })
            if report["samples_mismatch"]:
                st.write("样例不一致行")
                st.dataframe(report["samples_mismatch"])
                try:
                    import pandas as pd
                    dfm = pd.DataFrame(report["samples_mismatch"])
                    from datetime import datetime as _dt, date as _date
                    st.download_button("下载不一致样例 JSON", json.dumps(report["samples_mismatch"], ensure_ascii=False, indent=2, default=lambda o: o.isoformat() if isinstance(o, (_dt, _date)) else str(o)), file_name="cons_mismatch.json", mime="application/json", key="dl_cons_json")
                    st.download_button("下载不一致样例 CSV", dfm.to_csv(index=False), file_name="cons_mismatch.csv", mime="text/csv", key="dl_cons_csv")
                except Exception:
                    from datetime import datetime as _dt, date as _date
                    st.download_button("下载不一致样例 JSON", json.dumps(report["samples_mismatch"], ensure_ascii=False, default=lambda o: o.isoformat() if isinstance(o, (_dt, _date)) else str(o)), file_name="cons_mismatch.json", mime="application/json", key="dl_cons_json")
                    headers = []
                    if report["samples_mismatch"]:
                        headers = sorted(set().union(*[m.keys() for m in report["samples_mismatch"]]))
                    rows = [headers] + [[str(r.get(h, "")) for h in headers] for r in report["samples_mismatch"]]
                    csv = "\n".join([",".join(row) for row in rows])
                    st.download_button("下载不一致样例 CSV", csv, file_name="cons_mismatch.csv", mime="text/csv", key="dl_cons_csv")

    st.subheader("📜 日志与分析")
    t_logs_view, t_logs_ai = st.tabs(["查看与筛选", "AI 分析与图表"])
    with t_logs_view:
        c_lv1, c_lv2 = st.columns([1,1])
        with c_lv1:
            if st.button("查看日志", key="btn_view_logs"):
                try:
                    p = _log_path()
                    items = []
                    if os.path.exists(p):
                        with open(p, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    items.append(json.loads(line))
                                except Exception:
                                    pass
                    if items:
                        st.dataframe(items[-100:])
                    else:
                        st.info("暂无日志")
                except Exception as e:
                    st.error(str(e))
            if st.button("写入示例日志", key="btn_seed_logs"):
                write_log({"timestamp": datetime.utcnow().isoformat(), "event": "convert", "input_sql": "SELECT SYSDATE FROM DUAL", "converted_sql": "SELECT CURRENT_TIMESTAMP()", "warnings": [], "error": None})
                write_log({"timestamp": datetime.utcnow().isoformat(), "event": "convert", "input_sql": "SELECT * FROM t CONNECT BY PRIOR id=pid", "converted_sql": "", "warnings": ["CONNECT BY detected; manual rewrite to WITH RECURSIVE required"], "error": None})
                write_log({"timestamp": datetime.utcnow().isoformat(), "event": "execute", "db": "oracle", "executed_sql": "SELECT bad_col FROM dual", "elapsed_ms": 12, "rows": 0, "error": "ORA-00904: invalid identifier"})
                st.success("已写入示例日志")
        with c_lv2:
            selected_events = st.multiselect("事件类型", ["convert", "execute", "test_connection", "consistency"], default=["convert", "execute"], key="log_events")
            keyword = st.text_input("关键字过滤", "", key="log_keyword", help="按 SQL/错误信息包含关键字过滤")
            start_iso = st.text_input("起始时间(ISO，可选)", "", key="log_start_iso", help="例如 2025-12-15T00:00:00")
            end_iso = st.text_input("结束时间(ISO，可选)", "", key="log_end_iso", help="例如 2025-12-15T23:59:59")
            if st.button("筛选日志", key="btn_filter_logs"):
                try:
                    p = _log_path()
                    items = []
                    if os.path.exists(p):
                        with open(p, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    items.append(json.loads(line))
                                except Exception:
                                    pass
                    if items:
                        fs = []
                        kw = keyword.strip().lower()
                        from datetime import datetime as _dt
                        sdt = None
                        edt = None
                        if start_iso.strip():
                            try:
                                sdt = _dt.fromisoformat(start_iso.strip())
                            except Exception:
                                st.warning("起始时间解析失败，忽略时间筛选")
                                sdt = None
                        if end_iso.strip():
                            try:
                                edt = _dt.fromisoformat(end_iso.strip())
                            except Exception:
                                st.warning("结束时间解析失败，忽略时间筛选")
                                edt = None
                        for it in items:
                            if selected_events and it.get("event") not in selected_events:
                                continue
                            if kw:
                                blob = json.dumps(it, ensure_ascii=False).lower()
                                if kw not in blob:
                                    continue
                            if sdt or edt:
                                ts = it.get("timestamp")
                                ok = True
                                try:
                                    tdt = _dt.fromisoformat(ts) if isinstance(ts, str) else None
                                except Exception:
                                    tdt = None
                                if sdt and tdt and tdt < sdt:
                                    ok = False
                                if edt and tdt and tdt > edt:
                                    ok = False
                                if sdt and not tdt:
                                    ok = False
                                if not ok:
                                    continue
                            fs.append(it)
                        if fs:
                            st.dataframe(fs[-200:])
                            st.download_button("下载筛选日志 JSON", json.dumps(fs, ensure_ascii=False, indent=2), file_name="filtered_logs.json", mime="application/json", key="dl_logs_json")
                        else:
                            st.info("无匹配日志")
                    else:
                        st.info("暂无日志")
                except Exception as e:
                    st.error(str(e))
            if st.button("生成错误分类图表", key="btn_chart_logs"):
                try:
                    from collections import Counter
                    p = _log_path()
                    items = []
                    if os.path.exists(p):
                        with open(p, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    items.append(json.loads(line))
                                except Exception:
                                    pass
                    errs = []
                    for it in items:
                        if it.get("event") == "execute" and it.get("error"):
                            errs.append("执行失败")
                        if it.get("event") == "convert" and (it.get("error") or it.get("warnings")):
                            errs.append("转换失败")
                    cnt = Counter(errs)
                    chart_data = {"类型": list(cnt.keys()), "数量": list(cnt.values())}
                    try:
                        import pandas as pd  # type: ignore
                        df = pd.DataFrame(chart_data)
                        st.bar_chart(df.set_index("类型"))
                    except Exception:
                        st.bar_chart(chart_data)
                except Exception as e:
                    st.error(str(e))
    with t_logs_ai:
        provider = st.selectbox("LLM 提供方", ["DashScope", "OpenAI"], index=0)
        provider_key = provider.lower()
        use_env = st.checkbox("使用环境变量", value=True)
        api_key_input = st.text_input("LLM API Key", "", type="password", key="llm_api_key")
        model_name = st.text_input("LLM 模型", "qwen-plus" if provider_key == "dashscope" else "gpt-4o-mini", key="llm_model")
        if use_env:
            st.caption("将使用 DASHSCOPE_API_KEY/QWEN_API_KEY 或 OPENAI_API_KEY")
        if st.button("测试 LLM 连接", key="btn_llm_ping"):
            try:
                from openai import OpenAI
                base_url = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1" if provider_key == "dashscope" else None
                ak = None if use_env else (api_key_input or None)
                if ak:
                    client = OpenAI(api_key=ak, base_url=base_url) if provider_key == "dashscope" else OpenAI(api_key=ak)
                else:
                    import os as _os
                    if provider_key == "dashscope":
                        env_key = _os.environ.get("DASHSCOPE_API_KEY") or _os.environ.get("QWEN_API_KEY")
                        client = OpenAI(api_key=env_key, base_url=base_url) if env_key else None
                    else:
                        env_key = _os.environ.get("OPENAI_API_KEY")
                        client = OpenAI(api_key=env_key) if env_key else None
                if client is None:
                    st.error("缺少密钥：请在 UI 输入或设置环境变量")
                else:
                    m = model_name or ("qwen-plus" if provider_key == "dashscope" else "gpt-4o-mini")
                    resp = client.chat.completions.create(model=m, messages=[{"role": "user", "content": "ping"}], temperature=0)
                    st.success(f"连接成功：{provider_key} / {m}")
                    st.write(resp.choices[0].message.content)
            except Exception:
                try:
                    import requests, os as _os
                    base = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1" if provider_key == "dashscope" else "https://api.openai.com/v1"
                    ak = None if use_env else (api_key_input or None)
                    if not ak:
                        ak = (_os.environ.get("DASHSCOPE_API_KEY") or _os.environ.get("QWEN_API_KEY")) if provider_key == "dashscope" else _os.environ.get("OPENAI_API_KEY")
                    if not ak:
                        st.error("缺少密钥：请在 UI 输入或设置环境变量")
                    else:
                        m = model_name or ("qwen-plus" if provider_key == "dashscope" else "gpt-4o-mini")
                        url = base.rstrip("/") + "/chat/completions"
                        headers = {"Authorization": f"Bearer {ak}", "Content-Type": "application/json"}
                        payload = {"model": m, "messages": [{"role": "user", "content": "ping"}], "temperature": 0}
                        r = requests.post(url, headers=headers, json=payload, timeout=20)
                        if r.status_code == 200:
                            st.success(f"连接成功：{provider_key} / {m}")
                            st.write(r.json().get("choices", [{}])[0].get("message", {}).get("content"))
                        else:
                            st.error(f"{r.status_code}: {r.text}")
                except Exception as e2:
                    st.error(str(e2))
        if st.button("AI Agent 分析日志", key="btn_llm_analyze"):
            base_url = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1" if provider_key == "dashscope" else None
            ak = None if use_env else (api_key_input or None)
            report = analyze_logs(api_key=ak, provider=provider_key, model_name=model_name or None, base_url=base_url)
            st.json(report.get("summary"))
            st.write("建议")
            st.write("\n".join(report.get("suggestions", [])))
            if report.get("top_failed_sql"):
                st.write("失败样例")
                st.dataframe(report["top_failed_sql"])
            if report.get("llm_report"):
                st.write("LLM 报告")
                st.write(report["llm_report"])
                st.success(f"LLM 调用成功：{report.get('llm_provider')} / {report.get('llm_model')}")
            else:
                st.warning("未生成 LLM 报告")
                if report.get("llm_error"):
                    st.error(report.get("llm_error"))

    st.caption("日志文件位置: " + _log_path())
    owner = 'Yuzh'
    year = os.environ.get("COPYRIGHT_YEAR") or str(datetime.utcnow().year)
    st.markdown(
        f"<div style='text-align:center;opacity:0.7;font-size:12px;margin-top:8px;'>© {year} {owner} 版权所有</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
