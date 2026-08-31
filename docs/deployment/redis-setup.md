# Redis Deployment Guide

This guide covers deploying Midwicket with Redis-backed rate limiting for production scaling across multiple workers.

## When to Use Redis

The default DuckDB backend works for single-instance deployments and moderate throughput. Switch to Redis when:

- Running multiple uvicorn workers (`--workers > 1`)
- Deploying across multiple containers/pods
- Need sub-millisecond rate-limit checks
- Require sliding-window precision under high load

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MIDWICKET_RATE_LIMIT_BACKEND` | Backend type: `redis`, `memory`, or `duckdb` | `memory` (dev), `redis` (prod) |
| `MIDWICKET_REDIS_URL` | Redis connection URL (e.g., `redis://host:6379/0`) | `redis://localhost:6379/0` |
| `MIDWICKET_RATE_LIMIT_REQUESTS_PER_MINUTE` | Per-client request limit | `60` |
| `MIDWICKET_ENV` | Environment indicator (`production` enables Redis by default) | `development` |

## Docker Compose Example

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3
    command: redis-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru

  midwicket-api:
    build: .
    restart: unless-stopped
    ports:
      - "8000:8000"
    environment:
      MIDWICKET_ENV: production
      MIDWICKET_RATE_LIMIT_BACKEND: redis
      MIDWICKET_REDIS_URL: redis://redis:6379/0
      MIDWICKET_RATE_LIMIT_REQUESTS_PER_MINUTE: "120"
      MIDWICKET_API_KEYS: ${MIDWICKET_API_KEYS}
      MIDWICKET_API_KEY_REQUIRED: "true"
      MIDWICKET_SECRET_KEY: ${MIDWICKET_SECRET_KEY}
      MIDWICKET_CORS_ORIGINS: ${MIDWICKET_CORS_ORIGINS}
    depends_on:
      redis:
        condition: service_healthy
    command: uvicorn midwicket.serve.main:app --host 0.0.0.0 --port 8000 --workers 4

volumes:
  redis_data:
```

## Kubernetes Deployment

### Redis Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: midwicket-redis
  labels:
    app: midwicket-redis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: midwicket-redis
  template:
    metadata:
      labels:
        app: midwicket-redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
        args:
          - --appendonly
          - "yes"
          - --maxmemory
          - "256mb"
          - --maxmemory-policy
          - allkeys-lru
        resources:
          requests:
            memory: "128Mi"
            cpu: "50m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          exec:
            command: ["redis-cli", "ping"]
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          exec:
            command: ["redis-cli", "ping"]
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: redis-data
          mountPath: /data
      volumes:
      - name: redis-data
        persistentVolumeClaim:
          claimName: midwicket-redis-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: midwicket-redis
spec:
  selector:
    app: midwicket-redis
  ports:
  - port: 6379
    targetPort: 6379
  type: ClusterIP
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: midwicket-redis-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
```

### Midwicket API Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: midwicket-api
  labels:
    app: midwicket-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: midwicket-api
  template:
    metadata:
      labels:
        app: midwicket-api
    spec:
      containers:
      - name: api
        image: midwicket-api:latest
        ports:
        - containerPort: 8000
        env:
        - name: MIDWICKET_ENV
          value: production
        - name: MIDWICKET_RATE_LIMIT_BACKEND
          value: redis
        - name: MIDWICKET_REDIS_URL
          value: redis://midwicket-redis:6379/0
        - name: MIDWICKET_RATE_LIMIT_REQUESTS_PER_MINUTE
          value: "120"
        - name: MIDWICKET_API_KEYS
          valueFrom:
            secretKeyRef:
              name: midwicket-secrets
              key: api-keys
        - name: MIDWICKET_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: midwicket-secrets
              key: secret-key
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
---
apiVersion: v1
kind: Service
metadata:
  name: midwicket-api
spec:
  selector:
    app: midwicket-api
  ports:
  - port: 80
    targetPort: 8000
  type: LoadBalancer
```

## Migration from DuckDB to Redis

If you're currently running with the DuckDB backend:

1. Deploy Redis service (Docker Compose or Kubernetes)
2. Set `MIDWICKET_RATE_LIMIT_BACKEND=redis` in your API config
3. Set `MIDWICKET_REDIS_URL` to point to your Redis instance
4. Restart API workers — rate limit state will reset (acceptable for sliding-window limiter)
5. Verify rate limit headers in responses (`X-RateLimit-Remaining`)

No data migration is needed; rate limit state is ephemeral.

## Health Checks

### Redis Connection Check

The API automatically falls back to memory/DuckDB if Redis is unavailable. Monitor Redis health:

```bash
# Inside container
redis-cli -h midwicket-redis ping
# Should return: PONG

# From host
docker exec -it midwicket-redis_1 redis-cli ping
```

### Rate Limit Verification

Check rate limit headers in API responses:

```bash
curl -i -H "Authorization: Bearer YOUR_KEY" http://localhost:8000/v1/health
# Response includes:
# X-RateLimit-Limit: 120
# X-RateLimit-Remaining: 119
# X-RateLimit-Reset: 1717250000
```

## Performance Tuning

### Redis Configuration

For high-throughput deployments, tune Redis:

```conf
# redis.conf
maxmemory 512mb
maxmemory-policy allkeys-lru
appendonly yes
appendfsync everysec
tcp-keepalive 300
timeout 0
```

### Connection Pooling

The Python redis client uses a connection pool by default. For very high concurrency, you may want to increase pool size by setting:

```bash
export MIDWICKET_REDIS_POOL_SIZE=20
```

### Sliding Window vs Fixed Window

The Redis backend uses a sliding window algorithm via Lua scripts for accurate rate limiting. This is more accurate than fixed-window limiting but has slightly higher overhead per request.

## Troubleshooting

### "Redis ConnectionError, falling back to memory"

- Verify `MIDWICKET_REDIS_URL` is correct
- Check network connectivity to Redis host
- Verify Redis is running: `redis-cli ping`
- Check Redis logs for errors

### Rate limits not enforced across workers

- Confirm `MIDWICKET_RATE_LIMIT_BACKEND=redis` is set
- Check API startup logs for "Rate limiter backend: redis"
- If you see "Falling back to memory", investigate Redis connectivity

### High Redis memory usage

- Set `maxmemory` and `maxmemory-policy allkeys-lru` in Redis config
- Reduce rate limit window if needed
- Monitor with: `redis-cli info memory`

## Security Considerations

- Run Redis on a private network (not exposed to public)
- Use Redis ACL or `requirepass` for authentication in shared environments
- Enable TLS for production: `rediss://...` URL scheme
- Restrict access via security groups / network policies
- Rotate Redis password periodically

### Redis with Authentication

```yaml
environment:
  MIDWICKET_REDIS_URL: rediss://user:password@redis.example.com:6380/0
```

## References

- [Redis Documentation](https://redis.io/docs/)
- [Sliding Window Rate Limiting](https://redis.io/learn/howtos/quick-start/sliding-window)
- Implementation: `midwicket/serve/rate_limit.py`
