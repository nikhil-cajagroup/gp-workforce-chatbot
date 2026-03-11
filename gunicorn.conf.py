# ═══════════════════════════════════════════════════════════════
# Gunicorn Configuration — GP Workforce Chatbot
# ═══════════════════════════════════════════════════════════════

import os

# ── Server ────────────────────────────────────────────────────
bind = "0.0.0.0:8000"

# ── Workers ───────────────────────────────────────────────────
# UvicornWorker for async FastAPI support (ASGI)
worker_class = "uvicorn.workers.UvicornWorker"

# 2 workers for a t3.micro (1 vCPU, 1GB RAM)
# Increase to 3-4 for t3.small (2 vCPU, 2GB RAM)
workers = int(os.getenv("GUNICORN_WORKERS", "2"))

# ── Timeouts ──────────────────────────────────────────────────
# Long timeout for SSE streaming — Bedrock + Athena queries can take 30-60s
timeout = 120
graceful_timeout = 30
keepalive = 5

# ── Logging ───────────────────────────────────────────────────
accesslog = "-"          # stdout
errorlog = "-"           # stderr
loglevel = os.getenv("LOG_LEVEL", "info").lower()

# ── Security ──────────────────────────────────────────────────
# Limit request sizes to prevent abuse
limit_request_line = 8190
limit_request_fields = 100
limit_request_field_size = 8190
