import re

with open("midwicket/serve/api.py", "r") as f:
    content = f.read()

# 1. Add Middleware
middleware_anchor = "        # Add request logging"
middleware_code = """        # Add API Key Audit Middleware
        @self.app.middleware("http")
        async def audit_api_key_usage(request: Request, call_next):
            start_time = time.time()
            response = await call_next(request)
            duration_ms = (time.time() - start_time) * 1000
            
            path = request.url.path
            
            sensitive_prefixes = ("/v1/players", "/v1/teams", "/matches", "/v1/venues")
            
            if response.status_code < 400 and path.startswith(sensitive_prefixes):
                auth_identity = request.headers.get("Authorization") or request.headers.get("X-API-Key")
                if auth_identity:
                    user_id = hashlib.sha256(auth_identity.encode("utf-8")).hexdigest()[:16]
                    client_ip = request.client.host if request.client else "unknown"
                    try:
                        self.session.engine.execute_sql(
                            "INSERT INTO audit_log (ts, user_id, query_text, row_count, duration_ms, endpoint, action, ip_address) "
                            "VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
                            [user_id, f"API key usage on {path}", 0, round(duration_ms, 2), path, "api_key_usage", client_ip],
                            read_only=False,
                        )
                    except Exception as e:
                        logger.warning("Failed to log API key usage: %s", e)
                        
            return response

        # Add request logging"""
content = content.replace(middleware_anchor, middleware_code)

# 2. Add Export endpoint
export_code = """
        @self.app.get("/v1/export")
        async def bulk_export(
            request: Request,
            table: str = Query("ball_events", description="Table to export"),
            limit: int = Query(10000, le=100000, description="Max rows to export"),
            authenticated: bool = Depends(verify_api_key)
        ):
            \"\"\"Bulk export data.\"\"\"
            if table not in ["ball_events", "matches", "players", "venues"]:
                raise HTTPException(status_code=400, detail="Invalid table for export")
                
            try:
                t0 = time.time()
                result = self.session.engine.execute_sql(f"SELECT * FROM {table} LIMIT ?", [limit], read_only=True)
                duration_ms = (time.time() - t0) * 1000
                rows = result.to_pydict()
                
                keys = list(rows.keys())
                n = min(len(rows[keys[0]]) if keys else 0, limit)
                records = [{k: rows[k][i] for k in keys} for i in range(n)]
                
                # Audit log
                auth_identity = request.headers.get("Authorization") or request.headers.get("X-API-Key") or "anonymous"
                user_id = hashlib.sha256(auth_identity.encode("utf-8")).hexdigest()[:16]
                client_ip = request.client.host if request.client else "unknown"
                try:
                    self.session.engine.execute_sql(
                        "INSERT INTO audit_log (ts, user_id, query_text, row_count, duration_ms, endpoint, action, ip_address) "
                        "VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
                        [user_id, f"Export {table}", n, round(duration_ms, 2), "/v1/export", "bulk_export", client_ip],
                        read_only=False,
                    )
                except Exception as e:
                    logger.warning("audit_log write failed for bulk_export: %s", e)
                
                return {"table": table, "exported_rows": n, "data": records}
            except Exception as e:
                logger.warning("Bulk export failed: %s", e)
                raise HTTPException(status_code=500, detail="Export failed")

        @self.app.post("/analyze")"""
content = content.replace("        @self.app.post(\"/analyze\")", export_code)

with open("midwicket/serve/api.py", "w") as f:
    f.write(content)

