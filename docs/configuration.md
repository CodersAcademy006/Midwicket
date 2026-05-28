# Midwicket Configuration Reference

All runtime configuration is supplied through environment variables prefixed `MIDWICKET_`.
No configuration file is required; sane defaults are provided for local development.

---

## Core

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_ENV` | `development` | Runtime environment. Set to `production` to enforce secret-key requirement, disable Swagger UI, and enable production-grade guards. Accepted values: `development`, `production`, `testing`. |
| `MIDWICKET_DATA_DIR` | `~/.midwicket_data` | Absolute path to the data directory. Houses DuckDB files, raw JSON data, and the dev secret key. |
| `MIDWICKET_SECRET_KEY` | _(generated)_ | JWT signing key. **Required in production** (`MIDWICKET_ENV=production`). In development a persistent key is auto-generated and stored in `MIDWICKET_DATA_DIR/.midwicket_dev_secret`. |

---

## Database (DuckDB)

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_DB_THREADS` | `4` | DuckDB worker thread count. Integer between 1 and 16. |
| `MIDWICKET_DB_MEMORY` | `2GB` | DuckDB memory limit string, e.g. `4GB`, `512MB`. |

---

## API Server

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_API_HOST` | `0.0.0.0` | Bind address for the uvicorn server. |
| `MIDWICKET_API_PORT` | `8000` | Listen port. |
| `MIDWICKET_WEBHOOK_HOST` | `localhost` | Bind host for the live-ingestor webhook HTTP server. Keep `localhost` when webhook traffic is proxied locally; set an explicit interface/IP only when externally reachable webhooks are required. |
| `MIDWICKET_ALLOWED_HOSTS` | `localhost,127.0.0.1` | Comma-separated list of permitted `Host` header values. Prevents host-header injection in reverse-proxy deployments. Add your public domain here in production. |
| `MIDWICKET_CORS_ORIGINS` | _(empty — no CORS)_ | Comma-separated list of allowed cross-origin origins. Example: `https://app.example.com,https://admin.example.com`. Wildcards (`*`) are never accepted. |
| `MIDWICKET_API_KEY_REQUIRED` | `true` | Set to `false` to disable API key authentication. Only for local development. |
| `MIDWICKET_API_KEYS` | _(none)_ | Comma-separated list of valid API keys. Required when `MIDWICKET_API_KEY_REQUIRED=true`. |

---

## Custom SQL Analysis (`/analyze`)

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_ANALYZE_ENABLED` | `false` | Set to `true` to enable the `POST /analyze` endpoint. Disabled by default; only enable after reviewing the sql_guard allowlist. |
| `MIDWICKET_ANALYZE_TIMEOUT_SECONDS` | `8` | Max execution time for one `/analyze` query. Values are clamped to `1..120` seconds. Longer-running queries are interrupted and return HTTP 408. |

---

## Rate Limiting

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_RATE_LIMIT_REQUESTS_PER_MINUTE` | `60` | Max requests per API key (or IP) per minute. |
| `MIDWICKET_RATE_LIMIT_BACKEND` | `memory` (dev) / `duckdb` (prod) | Backend for rate-limit state. `memory` — in-process, resets on restart. `duckdb` — persisted, survives restarts and works across workers. |
| `MIDWICKET_RATE_LIMIT_DB_PATH` | _(auto, inside MIDWICKET_DATA_DIR)_ | Absolute path to the DuckDB file used by the `duckdb` rate-limit backend. |
| `MIDWICKET_TRUSTED_PROXIES` | _(empty)_ | Comma-separated list of trusted reverse-proxy IPs or CIDRs (for example `127.0.0.1,10.0.0.0/8`). `X-Forwarded-For` is honored only when the direct peer matches this list. |

---

## Cache

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_CACHE_TTL` | `3600` | Query-result cache TTL in seconds. |

---

## Data Download

| Variable | Default | Description |
|---|---|---|
| `CRICSHEET_URL` | `https://cricsheet.org/downloads/ipl_json.zip` | Source URL for the Cricsheet IPL dataset. Override to point at a mirror or a locally-hosted copy. |
| `MIDWICKET_DOWNLOAD_TIMEOUT` | `60` | HTTP request timeout in seconds for the initial ZIP download. |
| `MIDWICKET_EXTRACT_TIMEOUT` | `120` | Timeout in seconds for ZIP extraction. |

---

## Win Probability Model

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_WIN_MODEL_MODE` | `default` | Model loading mode. `default` — use the bundled heuristic/trained model. `path` — load from `MIDWICKET_WIN_MODEL_PATH` (dev/staging only; blocked in `production`). |
| `MIDWICKET_WIN_MODEL_PATH` | _(none)_ | Absolute path to a `.joblib` or `.pkl` model file. Only used when `MIDWICKET_WIN_MODEL_MODE=path`. |
| `MIDWICKET_WIN_MODEL_SHA256` | _(none)_ | SHA-256 hex digest of the model file at `MIDWICKET_WIN_MODEL_PATH`. **Required** when `MIDWICKET_WIN_MODEL_MODE=path`. The file is rejected if the digest does not match. |

---

## Plugins

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_PLUGINS` | _(none)_ | Comma-separated plugin specs (`name:entry_point`) to auto-load on import. Example: `myplugin:myplugin.midwicket_plugin`. Has no effect in `production`. |
| `MIDWICKET_PLUGIN_ALLOWLIST` | _(none — plugins disabled)_ | Comma-separated top-level package prefixes that plugins are permitted to use. Example: `myplugin,trusted_analytics`. An empty value disables all plugin loading. |

---

## Quick-start `.env` for local development

```dotenv
MIDWICKET_ENV=development
MIDWICKET_API_KEY_REQUIRED=false
MIDWICKET_ANALYZE_ENABLED=true
MIDWICKET_DB_THREADS=4
MIDWICKET_DB_MEMORY=2GB
```

## Minimal production `.env`

```dotenv
MIDWICKET_ENV=production
MIDWICKET_SECRET_KEY=<64-char random hex>
MIDWICKET_API_KEYS=<key1>,<key2>
MIDWICKET_ALLOWED_HOSTS=api.example.com
MIDWICKET_CORS_ORIGINS=https://app.example.com
MIDWICKET_DB_THREADS=8
MIDWICKET_DB_MEMORY=4GB
```
