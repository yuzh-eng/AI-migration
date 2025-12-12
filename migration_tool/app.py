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
        <div style="padding:16px;border-radius:12px;background:linear-gradient(90deg,#1f6feb 0%,#2ea043 100%);color:#fff;">
          <div style="font-size:18px;font-weight:600;">高效完成 Oracle → Snowflake 迁移与验证</div>
          <div style="opacity:0.9;margin-top:6px;">集成 SQL 转换、连接测试、执行校验、日志与 AI 分析</div>
      
        </div>
        <style>
          .stButton>button{border-radius:8px;padding:0.5rem 1rem;background:#1f6feb;color:#fff;border:0}
          .stButton>button:hover{background:#1559c0}
          .stTextInput>div>div>input{border-radius:8px}
          .stTextArea textarea{border-radius:8px}
          pre, code{border-radius:8px}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.subheader("🧩 SQL 转换")
    oracle_sql = st.text_area("Oracle SQL 输入", height=160, key="oracle_sql")
    if st.button("转换 SQL"):
        converted_sql, warnings = convert(oracle_sql or "")
        st.code(converted_sql or "", language="sql")
        write_log({
            "timestamp": datetime.utcnow().isoformat(),
            "event": "convert",
            "input_sql": oracle_sql,
            "converted_sql": converted_sql,
            "warnings": warnings,
            "error": None,
        })
        if warnings:
            st.warning("; ".join(warnings))
    st.divider()

    st.subheader("🔌 数据库连接配置")
    tab1, tab2 = st.tabs(["Oracle", "Snowflake"])
    with tab1:
        o_host = st.text_input("host", "localhost", key="oracle_host", help="数据库主机，如 127.0.0.1")
        o_port = st.text_input("port", "1521", key="oracle_port", help="监听端口，常见为 1521")
        o_service = st.text_input("service_name", "xepdb1", key="oracle_service", help="PDB 服务名，如 xepdb1")
        o_sid = st.text_input("sid", "", key="oracle_sid", help="CDB 的 SID，如 xe")
        o_ez = st.text_input("EZ Connect (host:port/service)", "", key="oracle_ez", help="直接填写连接串，例如 127.0.0.1:1521/xepdb1")
        o_user = st.text_input("user", "", key="oracle_user", help="数据库用户名")
        o_password = st.text_input("password", "", type="password", key="oracle_password")
        if st.button("测试连接 (Oracle)"):
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
        s_account = st.text_input("account", "", key="snowflake_account", help="账户标识，例如 xy12345")
        s_user = st.text_input("user", "", key="snowflake_user", help="用户名")
        s_password = st.text_input("password", "", type="password", key="snowflake_password")
        s_warehouse = st.text_input("warehouse", "", key="snowflake_warehouse", help="虚拟仓库")
        s_database = st.text_input("database", "", key="snowflake_database", help="数据库")
        s_schema = st.text_input("schema", "", key="snowflake_schema", help="模式")
        s_role = st.text_input("role", "", key="snowflake_role", help="角色")
        st.divider()
        if st.button("测试连接 (Snowflake)"):
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

    st.subheader("🧪 SQL 执行测试")
    exec_db = st.selectbox("选择执行数据库", ["oracle", "snowflake"])
    exec_sql_src = st.radio("选择执行的 SQL", ["Oracle 原 SQL", "转换后 Snowflake SQL"]) 
    exec_sql = oracle_sql or ""
    if exec_sql_src == "转换后 Snowflake SQL":
        exec_sql, _ = convert(exec_sql)
    if st.button("执行 SQL"):
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
                st.dataframe(data, use_container_width=True)

    st.subheader("📜 日志与分析")
    provider = st.selectbox("LLM 提供方", ["DashScope", "OpenAI"], index=0)
    use_env = st.checkbox("使用环境变量", value=True)
    api_key_input = st.text_input("LLM API Key", "", type="password", key="llm_api_key")
    model_name = st.text_input("LLM 模型", "qwen-plus" if provider == "dashscope" else "gpt-4o-mini", key="llm_model")
    if use_env:
        st.caption("将使用 DASHSCOPE_API_KEY/QWEN_API_KEY 或 OPENAI_API_KEY")
    if st.button("测试 LLM 连接"):
        try:
            from openai import OpenAI
            base_url = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1" if provider == "dashscope" else None
            ak = None if use_env else (api_key_input or None)
            if ak:
                client = OpenAI(api_key=ak, base_url=base_url) if provider == "dashscope" else OpenAI(api_key=ak)
            else:
                import os as _os
                if provider == "dashscope":
                    env_key = _os.environ.get("DASHSCOPE_API_KEY") or _os.environ.get("QWEN_API_KEY")
                    client = OpenAI(api_key=env_key, base_url=base_url) if env_key else None
                else:
                    env_key = _os.environ.get("OPENAI_API_KEY")
                    client = OpenAI(api_key=env_key) if env_key else None
            if client is None:
                st.error("缺少密钥：请在 UI 输入或设置环境变量")
            else:
                m = model_name or ("qwen-plus" if provider == "dashscope" else "gpt-4o-mini")
                resp = client.chat.completions.create(model=m, messages=[{"role": "user", "content": "ping"}], temperature=0)
                st.success(f"连接成功：{provider} / {m}")
                st.write(resp.choices[0].message.content)
        except Exception:
            try:
                import requests, os as _os
                base = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1" if provider == "dashscope" else "https://api.openai.com/v1"
                ak = None if use_env else (api_key_input or None)
                if not ak:
                    ak = (_os.environ.get("DASHSCOPE_API_KEY") or _os.environ.get("QWEN_API_KEY")) if provider == "dashscope" else _os.environ.get("OPENAI_API_KEY")
                if not ak:
                    st.error("缺少密钥：请在 UI 输入或设置环境变量")
                else:
                    m = model_name or ("qwen-plus" if provider == "dashscope" else "gpt-4o-mini")
                    url = base.rstrip("/") + "/chat/completions"
                    headers = {"Authorization": f"Bearer {ak}", "Content-Type": "application/json"}
                    payload = {"model": m, "messages": [{"role": "user", "content": "ping"}], "temperature": 0}
                    r = requests.post(url, headers=headers, json=payload, timeout=20)
                    if r.status_code == 200:
                        st.success(f"连接成功：{provider} / {m}")
                        st.write(r.json().get("choices", [{}])[0].get("message", {}).get("content"))
                    else:
                        st.error(f"{r.status_code}: {r.text}")
            except Exception as e2:
                st.error(str(e2))
    if st.button("查看日志"):
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
    if st.button("写入示例日志"):
        write_log({"timestamp": datetime.utcnow().isoformat(), "event": "convert", "input_sql": "SELECT SYSDATE FROM DUAL", "converted_sql": "SELECT CURRENT_TIMESTAMP()", "warnings": [], "error": None})
        write_log({"timestamp": datetime.utcnow().isoformat(), "event": "convert", "input_sql": "SELECT * FROM t CONNECT BY PRIOR id=pid", "converted_sql": "", "warnings": ["CONNECT BY detected; manual rewrite to WITH RECURSIVE required"], "error": None})
        write_log({"timestamp": datetime.utcnow().isoformat(), "event": "execute", "db": "oracle", "executed_sql": "SELECT bad_col FROM dual", "elapsed_ms": 12, "rows": 0, "error": "ORA-00904: invalid identifier"})
        st.success("已写入示例日志")
    if st.button("AI Agent 分析日志"):
        base_url = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1" if provider == "dashscope" else None
        ak = None if use_env else (api_key_input or None)
        report = analyze_logs(api_key=ak, provider=provider, model_name=model_name or None, base_url=base_url)
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
