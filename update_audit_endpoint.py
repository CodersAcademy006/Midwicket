import re

with open("midwicket/serve/api.py", "r") as f:
    content = f.read()

old_get_audit = """                result = self.session.engine.execute_sql(
                    "SELECT ts, user_id, query_text, row_count, duration_ms "
                    "FROM audit_log ORDER BY ts DESC LIMIT ?",
                    [limit],
                )
                rows = result.to_pydict()
                entries = [
                    {
                        "ts": str(rows["ts"][i]),
                        "user_id": rows["user_id"][i],
                        "query": rows["query_text"][i],
                        "row_count": rows["row_count"][i],
                        "duration_ms": rows["duration_ms"][i],
                    }
                    for i in range(len(rows.get("ts", [])))
                ]"""

new_get_audit = """                result = self.session.engine.execute_sql(
                    "SELECT ts, user_id, query_text, row_count, duration_ms, endpoint, action, ip_address "
                    "FROM audit_log ORDER BY ts DESC LIMIT ?",
                    [limit],
                )
                rows = result.to_pydict()
                entries = [
                    {
                        "ts": str(rows["ts"][i]),
                        "user_id": rows["user_id"][i],
                        "query": rows["query_text"][i],
                        "row_count": rows["row_count"][i],
                        "duration_ms": rows["duration_ms"][i],
                        "endpoint": rows["endpoint"][i] if "endpoint" in rows else "unknown",
                        "action": rows["action"][i] if "action" in rows else "unknown",
                        "ip_address": rows["ip_address"][i] if "ip_address" in rows else "unknown",
                    }
                    for i in range(len(rows.get("ts", [])))
                ]"""

content = content.replace(old_get_audit, new_get_audit)
with open("midwicket/serve/api.py", "w") as f:
    f.write(content)

