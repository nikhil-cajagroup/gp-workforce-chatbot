# ═══════════════════════════════════════════════════════════════
# InsightsQI Assistant Backend — Production Dockerfile (v8)
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
# tabulate needed by pandas .to_markdown() used in awswrangler output formatting
RUN pip install --no-cache-dir tabulate==0.9.0

# ── Copy only the files the backend needs ─────────────────────
# Main application (v8 — Pattern E multi-dataset supervisor)
COPY gp_workforce_chatbot_backend_agent_v8.py .

# v8 helper modules (workforce + appointments + shared services)
COPY v8_appointments_query_helpers.py .
COPY v8_appointments_sql_helpers.py .
COPY v8_dataset_service_helpers.py .
COPY v8_entity_resolution_helpers.py .
COPY v8_followup_sql_helpers.py .
COPY v8_validation_helpers.py .
COPY v8_workforce_intent_helpers.py .
COPY v8_workforce_override_helpers.py .
COPY v8_workforce_sql_helpers.py .

# v9 semantic fast-path: parser, compiler, metric registry, entity aliases
# cache-bust: v24 speed — Athena result reuse, 1h cache TTLs, v9 hash cache, workforce signals
COPY v9_parser.py v9_compiler.py v9_metric_registry.py v9_semantic_types.py v9_entity_aliases.py ./

# Domain knowledge + schema files
COPY gp_workforce_domain_notes.md .
COPY gp_appointments_domain_notes.md .
COPY schemas/ ./schemas/

# Few-shot examples + learned examples (critical for SQL generation)
COPY few_shot_examples.json .
COPY few_shot_examples_appointments.json .
COPY learned_examples.json .

# Gunicorn config
COPY gunicorn.conf.py .

# ── Expose port and run ───────────────────────────────────────
EXPOSE 8000

# Health check for ELB / Docker health monitoring
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run with gunicorn (production ASGI server)
CMD ["gunicorn", "-c", "gunicorn.conf.py", "gp_workforce_chatbot_backend_agent_v8:app"]
