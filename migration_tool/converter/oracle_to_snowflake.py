import re


def convert(sql: str):
    warnings = []
    s = sql

    def r(pattern, repl):
        return re.sub(pattern, repl, s, flags=re.IGNORECASE)

    s = re.sub(r"\bNVL\s*\(", "COALESCE(", s, flags=re.IGNORECASE)
    s = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP()", s, flags=re.IGNORECASE)
    s = re.sub(r"\bFROM\s+DUAL\b", "", s, flags=re.IGNORECASE)

    s = re.sub(r"\bTRUNC\s*\(\s*([^,\)]+)\s*\)", r"DATE_TRUNC('day', \1)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bTRUNC\s*\(\s*([^,\)]+)\s*,\s*'?(MONTH|MON)'?\s*\)", r"DATE_TRUNC('month', \1)", s, flags=re.IGNORECASE)
    s = re.sub(r"\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^\)]+)\)", r"DATEADD(month, \2, \1)", s, flags=re.IGNORECASE)

    if re.search(r"\bCONNECT\s+BY\b", s, flags=re.IGNORECASE):
        warnings.append("CONNECT BY detected; manual rewrite to WITH RECURSIVE required")

    return s.strip(), warnings

