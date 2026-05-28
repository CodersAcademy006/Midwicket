import re

with open("midwicket/serve/api.py", "r") as f:
    content = f.read()

# Update audit table schema
schema_old = """            self.session.engine.execute_sql(\"\"\"
                CREATE TABLE IF NOT EXISTS audit_log (
                    id BIGINT DEFAULT nextval('audit_log_id_seq'),
                    ts TIMESTAMP DEFAULT current_timestamp,
                    user_id VARCHAR,
                    query_text VARCHAR,
                    row_count INTEGER,
                    duration_ms DOUBLE
                )
            \"\"\", read_only=False)"""

schema_new = """            self.session.engine.execute_sql(\"\"\"
                CREATE TABLE IF NOT EXISTS audit_log (
                    id BIGINT DEFAULT nextval('audit_log_id_seq'),
                    ts TIMESTAMP DEFAULT current_timestamp,
                    user_id VARCHAR,
                    query_text VARCHAR,
                    row_count INTEGER,
                    duration_ms DOUBLE,
                    endpoint VARCHAR DEFAULT 'unknown',
                    action VARCHAR DEFAULT 'unknown',
                    ip_address VARCHAR DEFAULT 'unknown'
                )
            \"\"\", read_only=False)
            try:
                self.session.engine.execute_sql("ALTER TABLE audit_log ADD COLUMN endpoint VARCHAR DEFAULT 'unknown'", read_only=False)
                self.session.engine.execute_sql("ALTER TABLE audit_log ADD COLUMN action VARCHAR DEFAULT 'unknown'", read_only=False)
                self.session.engine.execute_sql("ALTER TABLE audit_log ADD COLUMN ip_address VARCHAR DEFAULT 'unknown'", read_only=False)
            except Exception:
                pass"""

content = content.replace(schema_old, schema_new)

# Update /analyze to use clear text and new columns
analyze_old = """                    auth_identity = (
                        request.headers.get("Authorization")
                        or request.headers.get("X-API-Key")
                        or "anonymous"
                    )
                    user_id = hashlib.sha256(auth_identity.encode("utf-8")).hexdigest()[:16]
                    query_fingerprint = hashlib.sha256(sql.encode("utf-8")).hexdigest()
                    self.session.engine.execute_sql(
                        "INSERT INTO audit_log (ts, user_id, query_text, row_count, duration_ms) "
                        "VALUES (current_timestamp, ?, ?, ?, ?)",
                        [user_id, f"sha256:{query_fingerprint}", n, round(duration_ms, 2)],
                        read_only=False,
                    )"""

analyze_new = """                    auth_identity = (
                        request.headers.get("Authorization")
                        or request.headers.get("X-API-Key")
                        or "anonymous"
                    )
                    user_id = hashlib.sha256(auth_identity.encode("utf-8")).hexdigest()[:16]
                    client_ip = request.client.host if request.client else "unknown"
                    self.session.engine.execute_sql(
                        "INSERT INTO audit_log (ts, user_id, query_text, row_count, duration_ms, endpoint, action, ip_address) "
                        "VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
                        [user_id, sql, n, round(duration_ms, 2), "/analyze", "custom_query", client_ip],
                        read_only=False,
                    )"""

content = content.replace(analyze_old, analyze_new)

with open("midwicket/serve/api.py", "w") as f:
    f.write(content)

