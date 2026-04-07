# ═══════════════════════════════════════════════════════════════
# Gunicorn Configuration — GP Workforce Chatbot
# ═══════════════════════════════════════════════════════════════

import os

# ── Server ────────────────────────────────────────────────────
bind = "0.0.0.0:8000"

# ── Workers ───────────────────────────────────────────────────
# UvicornWorker for async FastAPI support (ASGI)
worker_class = "uvicorn.workers.UvicornWorker"

# 1 worker because in-memory conversation state (ConversationMemory, SemanticCache)
# is not shared across workers. Multiple workers would cause follow-up context loss
# when requests land on different workers.
# For t3.micro (1 vCPU, 1GB RAM), 1 worker with async (uvicorn) handles concurrent
# requests via asyncio event loop — no need for multiple worker processes.
workers = 1

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
