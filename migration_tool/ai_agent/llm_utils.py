import os
import requests
import json

def get_llm_client(api_key=None, provider="openai", base_url=None):
    """
    Returns an initialized OpenAI client or a dict configuration for HTTP fallback.
    Handles environment variables if api_key is not provided.
    """
    if not api_key:
        if provider == "dashscope":
            api_key = os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("QWEN_API_KEY")
        else:
            api_key = os.environ.get("OPENAI_API_KEY")
    
    if not api_key:
        return None

    if provider == "dashscope" and not base_url:
        base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    
    try:
        from openai import OpenAI
        return OpenAI(api_key=api_key, base_url=base_url)
    except ImportError:
        # Fallback configuration for requests
         return {
             "api_key": api_key,
             "base_url": base_url or "https://api.openai.com/v1",
             "is_http_fallback": True
         }

def simple_chat(client, model, messages, temperature=0):
    """
    Simple wrapper for chat completions with error handling.
    Supports both OpenAI client and HTTP fallback.
    Returns content string or raises exception.
    """
    try:
        if isinstance(client, dict) and client.get("is_http_fallback"):
            # HTTP Fallback implementation
            url = client["base_url"].rstrip("/") + "/chat/completions"
            headers = {
                "Authorization": f"Bearer {client['api_key']}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": model,
                "messages": messages,
                "temperature": temperature
            }
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            if r.status_code == 200:
                return r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            else:
                raise Exception(f"HTTP Error {r.status_code}: {r.text}")
        else:
            # OpenAI Client implementation
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature
            )
            return resp.choices[0].message.content
    except Exception as e:
        raise e

def simple_chat_raw(client, model, messages, temperature=0):
    try:
        if isinstance(client, dict) and client.get("is_http_fallback"):
            url = client["base_url"].rstrip("/") + "/chat/completions"
            headers = {
                "Authorization": f"Bearer {client['api_key']}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": model,
                "messages": messages,
                "temperature": temperature
            }
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            return r.json()
        else:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature
            )
            data = {
                "choices": [{"message": {"content": resp.choices[0].message.content}}],
                "usage": getattr(resp, "usage", None)
            }
            return data
    except Exception as e:
        raise e
def probe_quota(provider="openai", api_key=None, base_url=None, model=None, timeout=15):
    if not api_key:
        if provider == "dashscope":
            api_key = os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("QWEN_API_KEY")
        else:
            api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return {"ok": False, "error": "missing_api_key"}
    if provider == "dashscope" and not base_url:
        base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    url = (base_url or "https://api.openai.com/v1").rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model or "qwen-plus", "messages": [{"role": "user", "content": "ping"}], "temperature": 0}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        hdr = {k.lower(): v for k, v in r.headers.items()}
        fields = {}
        for k, v in hdr.items():
            if ("ratelimit" in k) or ("quota" in k) or (k.startswith("x-request-id")):
                fields[k] = v
        body = {}
        try:
            body = r.json()
        except Exception:
            body = {}
        usage = body.get("usage") or {}
        return {"ok": True, "status_code": r.status_code, "headers": hdr, "quota": fields, "usage": usage}
    except Exception as e:
        return {"ok": False, "error": str(e)}
