# PyPitch Configuration Reference

All runtime configuration is supplied through environment variables prefixed `PYPITCH_`.
No configuration file is required; sane defaults are provided for local development.

---

## Core

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_ENV` | `development` | Runtime environment. Set to `production` to enforce secret-key requirement, disable Swagger UI, and enable production-grade guards. Accepted values: `development`, `production`, `testing`. |
| `PYPITCH_DATA_DIR` | `~/.pypitch_data` | Absolute path to the data directory. Houses DuckDB files, raw JSON data, and the dev secret key. |
| `PYPITCH_SECRET_KEY` | _(generated)_ | JWT signing key. **Required in production** (`PYPITCH_ENV=production`). In development a persistent key is auto-generated and stored in `PYPITCH_DATA_DIR/.pypitch_dev_secret`. |

---

## Database (DuckDB)

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_DB_THREADS` | `4` | DuckDB worker thread count. Integer between 1 and 16. |
| `PYPITCH_DB_MEMORY` | `2GB` | DuckDB memory limit string, e.g. `4GB`, `512MB`. |

---

## API Server

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_API_HOST` | `0.0.0.0` | Bind address for the uvicorn server. |
| `PYPITCH_API_PORT` | `8000` | Listen port. |
| `PYPITCH_WEBHOOK_HOST` | `localhost` | Bind host for the live-ingestor webhook HTTP server. Keep `localhost` when webhook traffic is proxied locally; set an explicit interface/IP only when externally reachable webhooks are required. |
| `PYPITCH_ALLOWED_HOSTS` | `localhost,127.0.0.1` | Comma-separated list of permitted `Host` header values. Prevents host-header injection in reverse-proxy deployments. Add your public domain here in production. |
| `PYPITCH_CORS_ORIGINS` | _(empty — no CORS)_ | Comma-separated list of allowed cross-origin origins. Example: `https://app.example.com,https://admin.example.com`. Wildcards (`*`) are never accepted. |
| `PYPITCH_API_KEY_REQUIRED` | `true` | Set to `false` to disable API key authentication. Only for local development. |
| `PYPITCH_API_KEYS` | _(none)_ | Comma-separated list of valid API keys. Required when `PYPITCH_API_KEY_REQUIRED=true`. |

---

## Custom SQL Analysis (`/analyze`)

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_ANALYZE_ENABLED` | `false` | Set to `true` to enable the `POST /analyze` endpoint. Disabled by default; only enable after reviewing the sql_guard allowlist. |
| `PYPITCH_ANALYZE_TIMEOUT_SECONDS` | `8` | Max execution time for one `/analyze` query. Values are clamped to `1..120` seconds. Longer-running queries are interrupted and return HTTP 408. |

---

## Rate Limiting

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_RATE_LIMIT_REQUESTS_PER_MINUTE` | `60` | Max requests per API key (or IP) per minute. |
| `PYPITCH_RATE_LIMIT_BACKEND` | `memory` (dev) / `duckdb` (prod) | Backend for rate-limit state. `memory` — in-process, resets on restart. `duckdb` — persisted, survives restarts and works across workers. |
| `PYPITCH_RATE_LIMIT_DB_PATH` | _(auto, inside PYPITCH_DATA_DIR)_ | Absolute path to the DuckDB file used by the `duckdb` rate-limit backend. |
| `PYPITCH_TRUSTED_PROXIES` | _(empty)_ | Comma-separated list of trusted reverse-proxy IPs or CIDRs (for example `127.0.0.1,10.0.0.0/8`). `X-Forwarded-For` is honored only when the direct peer matches this list. |

---

## Cache

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_CACHE_TTL` | `3600` | Query-result cache TTL in seconds. |

---

## Data Download

| Variable | Default | Description |
|---|---|---|
| `CRICSHEET_URL` | `https://cricsheet.org/downloads/ipl_json.zip` | Source URL for the Cricsheet IPL dataset. Override to point at a mirror or a locally-hosted copy. |
| `PYPITCH_DOWNLOAD_TIMEOUT` | `60` | HTTP request timeout in seconds for the initial ZIP download. |
| `PYPITCH_EXTRACT_TIMEOUT` | `120` | Timeout in seconds for ZIP extraction. |

---

## Win Probability Model

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_WIN_MODEL_MODE` | `default` | Model loading mode. `default` — use the bundled heuristic/trained model. `path` — load from `PYPITCH_WIN_MODEL_PATH` (dev/staging only; blocked in `production`). |
| `PYPITCH_WIN_MODEL_PATH` | _(none)_ | Absolute path to a `.joblib` or `.pkl` model file. Only used when `PYPITCH_WIN_MODEL_MODE=path`. |
| `PYPITCH_WIN_MODEL_SHA256` | _(none)_ | SHA-256 hex digest of the model file at `PYPITCH_WIN_MODEL_PATH`. **Required** when `PYPITCH_WIN_MODEL_MODE=path`. The file is rejected if the digest does not match. |

---

## Plugins

| Variable | Default | Description |
|---|---|---|
| `PYPITCH_PLUGINS` | _(none)_ | Comma-separated plugin specs (`name:entry_point`) to auto-load on import. Example: `myplugin:myplugin.pypitch_plugin`. Has no effect in `production`. |
| `PYPITCH_PLUGIN_ALLOWLIST` | _(none — plugins disabled)_ | Comma-separated top-level package prefixes that plugins are permitted to use. Example: `myplugin,trusted_analytics`. An empty value disables all plugin loading. |

---

## Quick-start `.env` for local development

```dotenv
PYPITCH_ENV=development
PYPITCH_API_KEY_REQUIRED=false
PYPITCH_ANALYZE_ENABLED=true
PYPITCH_DB_THREADS=4
PYPITCH_DB_MEMORY=2GB
```

## Minimal production `.env`

```dotenv
PYPITCH_ENV=production
PYPITCH_SECRET_KEY=<64-char random hex>
PYPITCH_API_KEYS=<key1>,<key2>
PYPITCH_ALLOWED_HOSTS=api.example.com
PYPITCH_CORS_ORIGINS=https://app.example.com
PYPITCH_DB_THREADS=8
PYPITCH_DB_MEMORY=4GB
```
