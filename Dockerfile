# ═══════════════════════════════════════════════════════════════
# GP Workforce Chatbot Backend — Production Dockerfile
# ═══════════════════════════════════════════════════════════════
# Build:  docker build -t gp-chatbot-backend .
# Run:    docker run -p 8000:8000 --env-file .env gp-chatbot-backend
# ═══════════════════════════════════════════════════════════════

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ── Install dependencies first (cached if requirements.txt unchanged) ─
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy only the files the backend needs ─────────────────────
# Main application
COPY gp_workforce_chatbot_backend_agent_v5.py .
# Domain knowledge + schema files
COPY gp_workforce_domain_notes.md .
COPY schemas/ ./schemas/
# Gunicorn config
COPY gunicorn.conf.py .

# ── Expose port and run ───────────────────────────────────────
EXPOSE 8000

# Health check for ELB / Docker health monitoring
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run with gunicorn (production ASGI server)
CMD ["gunicorn", "-c", "gunicorn.conf.py", "gp_workforce_chatbot_backend_agent_v5:app"]
