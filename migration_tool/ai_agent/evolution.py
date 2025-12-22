import json
import os
from datetime import datetime
from migration_tool.converter.oracle_to_snowflake import convert
from migration_tool.ai_agent.llm_utils import get_llm_client, simple_chat

class EvolutionManager:
    def __init__(self, api_key=None, provider="openai", model="gpt-4o-mini", base_url=None):
        self.client = get_llm_client(api_key, provider, base_url)
        self.model = model
        self.history_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "converter", "rules_history.json")
        self.rules_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "converter", "rules.json")

    def generate_sql(self, topic):
        """1. Generator Agent"""
        if not self.client:
            return "Error: LLM client not initialized"
        
        prompt = f"""You are an Oracle SQL expert. Generate a complex Oracle SQL statement that demonstrates: {topic}.
        Include typical Oracle-specific functions, syntax, or patterns relevant to this topic.
        Output ONLY the SQL statement, no markdown, no explanation."""
        
        return simple_chat(self.client, self.model, [{"role": "user", "content": prompt}])

    def convert_sql(self, oracle_sql, current_rules=None):
        """2. Converter (Deterministic)"""
        return convert(oracle_sql, rules=current_rules)

    def review_conversion(self, oracle_sql, snowflake_sql):
        """3. Reviewer Agent"""
        if not self.client:
            return {"score": 0, "issues": ["LLM client missing"]}

        prompt = f"""You are a Snowflake SQL Expert and Code Reviewer.
        Compare the source Oracle SQL and the converted Snowflake SQL.
        
        Source (Oracle):
        {oracle_sql}
        
        Target (Snowflake):
        {snowflake_sql}
        
        Identify:
        1. Syntax errors in Snowflake SQL.
        2. Functional differences or semantic changes.
        3. Oracle functions that were not converted or converted incorrectly (e.g. keeping 'NVL' instead of 'COALESCE', or 'SYSDATE' issues).
        4. Any 'CLOB', 'BLOB', 'ROWNUM' usages that are not supported or need change.
        
        Output a JSON object with this structure:
        {{
            "score": <0-10 integer, 10 is perfect>,
            "issues": ["list of specific issues found"],
            "suggestion": "general suggestion"
        }}
        Output ONLY valid JSON."""

        res = simple_chat(self.client, self.model, [{"role": "user", "content": prompt}])
        try:
            # Clean up potential markdown code blocks
            res = res.replace("```json", "").replace("```", "").strip()
            return json.loads(res)
        except Exception as e:
            return {"score": 0, "issues": [f"Failed to parse Reviewer output: {res}"], "error": str(e)}

    def optimize_rule(self, oracle_sql, snowflake_sql, issues):
        """4. Optimizer Agent"""
        if not self.client:
            return None

        prompt = f"""You are a Regex and Python Expert for SQL Migration Tools.
        The current conversion rule failed to handle a specific pattern.
        
        Source: {oracle_sql}
        Current Result: {snowflake_sql}
        Issues: {json.dumps(issues)}
        
        Suggest a NEW rule to fix this. The rule system supports 'replacements' (simple string replace) and 'regex' (re.sub pattern/repl).
        
        Output a JSON object with ONE rule to add. Format:
        {{
            "type": "regex" or "replacement",
            "pattern": "regex_pattern_here",
            "repl": "replacement_string_here"
        }}
        For regex, ensure you use Python regex syntax. Escape backslashes correctly for JSON.
        Output ONLY valid JSON."""

        res = simple_chat(self.client, self.model, [{"role": "user", "content": prompt}])
        try:
            res = res.replace("```json", "").replace("```", "").strip()
            return json.loads(res)
        except Exception:
            return None

    def save_rule_snapshot(self, rule_proposal, oracle_sql, snowflake_sql, review_result):
        """5. Version Control / Logging"""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "trigger_sql": oracle_sql,
            "before_conversion": snowflake_sql,
            "issues": review_result.get("issues"),
            "proposed_rule": rule_proposal,
            "applied": False
        }
        
        history = []
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except:
                pass
        
        history.append(entry)
        with open(self.history_path, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        
        return len(history) - 1  # Return index

    def apply_rule(self, rule_proposal):
        """Apply the rule to the local rules.json file"""
        current_rules = {"replacements": [], "regex": [], "warnings": []}
        if os.path.exists(self.rules_path):
            try:
                with open(self.rules_path, "r", encoding="utf-8") as f:
                    current_rules = json.load(f)
            except:
                pass
        
        # Ensure keys exist
        for k in ["replacements", "regex", "warnings"]:
            if k not in current_rules:
                current_rules[k] = []

        if rule_proposal.get("type") == "regex":
            # Check for duplicates? simple check
            new_rule = {"pattern": rule_proposal["pattern"], "repl": rule_proposal["repl"]}
            if new_rule not in current_rules["regex"]:
                current_rules["regex"].append(new_rule)
        elif rule_proposal.get("type") == "replacement":
            new_rule = [rule_proposal["pattern"], rule_proposal["repl"]]
            if new_rule not in current_rules["replacements"]:
                current_rules["replacements"].append(new_rule)
        
        with open(self.rules_path, "w", encoding="utf-8") as f:
            json.dump(current_rules, f, indent=2, ensure_ascii=False)
        
        return True
