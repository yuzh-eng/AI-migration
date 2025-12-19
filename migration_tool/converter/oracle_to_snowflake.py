import re
import json


def _apply_regex(s: str, items):
    for it in items or []:
        p = it.get("pattern")
        r = it.get("repl")
        if p is None or r is None:
            continue
        s = re.sub(p, r, s, flags=re.IGNORECASE)
    return s


def _apply_replacements(s: str, items):
    for it in items or []:
        if isinstance(it, (list, tuple)) and len(it) == 2:
            p, r = it
            s = re.sub(p, r, s, flags=re.IGNORECASE)
        elif isinstance(it, dict):
            p = it.get("pattern")
            r = it.get("repl")
            if p is None or r is None:
                continue
            s = re.sub(p, r, s, flags=re.IGNORECASE)
    return s


def _default_rules():
    return {
        "replacements": [
            [r"\bNVL\s*\(", "COALESCE("],
            [r"\bSYSDATE\b", "CURRENT_TIMESTAMP()"],
            [r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP()"],
            [r"\bFROM\s+DUAL\b", ""],
            [r"\bSUBSTR\s*\(", "SUBSTRING("],
        ],
        "regex": [
            {"pattern": r"\bTRUNC\s*\(\s*([^,\)]+)\s*\)", "repl": r"DATE_TRUNC('day', \1)"},
            {"pattern": r"\bTRUNC\s*\(\s*([^,\)]+)\s*,\s*'?(MONTH|MON)'?\s*\)", "repl": r"DATE_TRUNC('month', \1)"},
            {"pattern": r"\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^\)]+)\)", "repl": r"DATEADD(month, \2, \1)"},
            {"pattern": r"\bDECODE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,\)]+)\s*(?:,\s*([^\)]+))?\)", "repl": r"CASE WHEN \1=\2 THEN \3 ELSE \4 END"},
            {"pattern": r"\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\)", "repl": r"IFF(\1 IS NOT NULL, \2, \3)"},
            {"pattern": r"\bTO_CHAR\s*\(\s*([^,]+)\s*,\s*'([^']+)'\s*\)", "repl": r"TO_VARCHAR(\1, '\2')"},
            {"pattern": r"\bTO_DATE\s*\(\s*([^,]+)\s*,\s*'([^']+)'\s*\)", "repl": r"TO_DATE(\1, '\2')"},
            {"pattern": r"\bTO_DATE\s*\(\s*([^,\)]+)\s*\)", "repl": r"TO_DATE(\1)"},
            {"pattern": r"\bTO_TIMESTAMP\s*\(\s*([^,]+)\s*,\s*'([^']+)'\s*\)", "repl": r"TO_TIMESTAMP(\1, '\2')"},
            {"pattern": r"\bTO_TIMESTAMP\s*\(\s*([^,\)]+)\s*\)", "repl": r"TO_TIMESTAMP(\1)"},
        ],
        "warnings": [
            {"pattern": r"\bCONNECT\s+BY\b", "message": "CONNECT BY detected; manual rewrite to WITH RECURSIVE required"},
            {"pattern": r"\bNLS_DATE_LANGUAGE\b", "message": "NLS_DATE_LANGUAGE detected; ensure format strings are locale-agnostic"},
        ],
    }


def convert(sql: str, rules: dict | None = None):
    warnings = []
    s = sql or ""

    base = _default_rules()
    user = rules or {}

    s = _apply_replacements(s, base.get("replacements"))
    s = _apply_regex(s, base.get("regex"))
    s = _apply_replacements(s, user.get("replacements"))
    s = _apply_regex(s, user.get("regex"))

    for it in (base.get("warnings") or []) + (user.get("warnings") or []):
        p = it.get("pattern")
        m = it.get("message")
        if not p or not m:
            continue
        if re.search(p, s, flags=re.IGNORECASE):
            warnings.append(m)

    dm = re.search(r"\bDECODE\s*\(([^)]*)\)", s, flags=re.IGNORECASE)
    if dm:
        args = [a.strip() for a in re.split(r"\s*,\s*", dm.group(1))]
        if len(args) > 4:
            warnings.append("DECODE with multiple pairs; manual CASE expansion recommended")

    m = re.search(r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s*;?\s*$", s, flags=re.IGNORECASE | re.MULTILINE)
    if m:
        n = m.group(1)
        s = re.sub(r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s*;?\s*$", "", s, flags=re.IGNORECASE | re.MULTILINE).rstrip()
        s = s + f" LIMIT {n}"
    elif re.search(r"\bROWNUM\b", s, flags=re.IGNORECASE):
        warnings.append("ROWNUM detected; consider LIMIT or ROW_NUMBER() for pagination")

    return s.strip(), warnings
