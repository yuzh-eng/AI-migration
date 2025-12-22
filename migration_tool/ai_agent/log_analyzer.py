import os
import json
from collections import Counter, defaultdict
from datetime import datetime


def _classify_error(msg: str):
    m = msg.lower()
    if "ora-00904" in m or "invalid identifier" in m:
        return "字段不存在"
    if "table or view does not exist" in m or "ora-00942" in m:
        return "表不存在"
    if "syntax" in m or "not supported" in m or "unsupported" in m:
        return "语法差异"
    if "permission" in m or "not authorized" in m:
        return "权限问题"
    return "其他"


def _read_logs(path: str):
    items = []
    if not os.path.exists(path):
        return items
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                pass
    return items


def analyze_logs(
    log_path: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "migration.log"),
    api_key: str | None = None,
    provider: str | None = None,
    model_name: str | None = None,
    base_url: str | None = None,
):
    provider = (provider or "").lower() or None
    events = _read_logs(log_path)
    convert_fail = []
    exec_fail = []
    error_counter = Counter()
    per_type_counter = defaultdict(int)
    samples = defaultdict(list)

    for e in events:
        if e.get("event") == "convert":
            ws = e.get("warnings") or []
            if e.get("error") or ws:
                convert_fail.append(e)
                if e.get("error"):
                    k = _classify_error(e["error"])
                    error_counter[k] += 1
                    per_type_counter["转换失败"] += 1
                    samples[k].append(e.get("input_sql") or e.get("executed_sql"))
        elif e.get("event") == "execute":
            if e.get("error"):
                exec_fail.append(e)
                k = _classify_error(e["error"])
                error_counter[k] += 1
                per_type_counter["执行失败"] += 1
                samples[k].append(e.get("executed_sql"))

    suggestions = []
    if error_counter["语法差异"]:
        suggestions.append("检查函数映射与保留字，必要时手工改写复杂语法")
    if error_counter["字段不存在"]:
        suggestions.append("比对源与目标库列名/大小写，补充缺失字段或修正别名")
    if error_counter["表不存在"]:
        suggestions.append("确认目标库中表是否已创建并选择正确的数据库/模式")
    if error_counter["权限问题"]:
        suggestions.append("为执行用户授予所需的 SELECT/USAGE 权限")
    if not suggestions:
        suggestions.append("当前未发现显著问题或日志不足以诊断")

    summary = {
        "总事件数": len(events),
        "转换失败数": len(convert_fail),
        "执行失败数": len(exec_fail),
        "错误分类统计": error_counter,
        "失败类型统计": per_type_counter,
    }

    top_failed_sql = []
    for k, lst in samples.items():
        for s in lst[:5]:
            if s:
                top_failed_sql.append({"类别": k, "SQL": s})

    llm_report = None
    llm_error = None
    llm_provider = None
    llm_model = None
    if not api_key:
        k_openai = os.environ.get("OPENAI_API_KEY")
        k_qwen = os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("QWEN_API_KEY")
        if k_qwen:
            api_key = k_qwen
            provider = provider or "dashscope"
            llm_provider = "dashscope"
        elif k_openai:
            api_key = k_openai
            provider = provider or "openai"
            llm_provider = "openai"
    if not api_key and provider in ("dashscope", "openai"):
        llm_error = "缺少密钥：请在 UI 输入或设置环境变量"
    if api_key and (provider == "dashscope" or provider == "openai"):
        try:
            from openai import OpenAI
            prompt = (
                "请根据如下统计生成一份简要迁移报告，包含关键失败原因与下一步建议：\n"
                f"汇总: {json.dumps(summary, ensure_ascii=False)}\n"
                f"建议: {json.dumps(suggestions, ensure_ascii=False)}\n"
            )
            if provider == "dashscope":
                bu = base_url or "https://dashscope.aliyuncs.com/compatible-mode/v1"
                client = OpenAI(api_key=api_key, base_url=bu)
                model = model_name or "qwen-plus"
                llm_provider = "dashscope"
                llm_model = model
            else:
                client = OpenAI(api_key=api_key)
                model = model_name or "gpt-4o-mini"
                llm_provider = "openai"
                llm_model = model
            resp = client.chat.completions.create(
                model=model, messages=[{"role": "user", "content": prompt}], temperature=0.2
            )
            llm_report = resp.choices[0].message.content
        except Exception as e:
            llm_report = None
            llm_error = str(e)
            try:
                import requests
                prompt = (
                    "请根据如下统计生成一份简要迁移报告，包含关键失败原因与下一步建议：\n"
                    f"汇总: {json.dumps(summary, ensure_ascii=False)}\n"
                    f"建议: {json.dumps(suggestions, ensure_ascii=False)}\n"
                )
                if provider == "dashscope":
                    bu = base_url or "https://dashscope.aliyuncs.com/compatible-mode/v1"
                    url = bu.rstrip("/") + "/chat/completions"
                    model = model_name or "qwen-plus"
                    llm_provider = "dashscope"
                    llm_model = model
                else:
                    url = "https://api.openai.com/v1/chat/completions"
                    model = model_name or "gpt-4o-mini"
                    llm_provider = "openai"
                    llm_model = model
                headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
                payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
                r = requests.post(url, headers=headers, json=payload, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    llm_report = data.get("choices", [{}])[0].get("message", {}).get("content")
                    llm_error = None
                else:
                    llm_error = f"{r.status_code}: {r.text}"
            except Exception as e2:
                llm_error = llm_error or str(e2)

    return {
        "summary": summary,
        "suggestions": suggestions,
        "top_failed_sql": top_failed_sql,
        "llm_report": llm_report,
        "llm_error": llm_error,
        "llm_provider": llm_provider,
        "llm_model": llm_model,
        "generated_at": datetime.utcnow().isoformat(),
    }
