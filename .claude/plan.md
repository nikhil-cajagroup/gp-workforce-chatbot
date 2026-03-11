# Production Deployment Plan — GP Workforce Chatbot

## Code Review Summary

After thorough analysis of the entire codebase (4,579-line backend, React frontend, configs, tests), here's the production readiness assessment and deployment plan for your WordPress website on AWS.

---

## PART 1: Production Readiness Review

### ✅ What's Already Production-Quality (Good Work)

1. **SQL injection protection** — readonly enforcement, table whitelist, column validation, comment stripping
2. **Structured logging** with request IDs and tracing
3. **Request timeouts** (90s default) with async wrapping
4. **Bounded caches** (LRU) to prevent memory leaks
5. **Thread safety** — locks on shared state, dedicated thread pool
6. **Error sanitisation** — no raw stack traces exposed to users
7. **Input validation** — Pydantic models with constraints (maxLength=1000)
8. **SSE streaming** — real-time progress updates
9. **LangGraph checkpointing** — conversation state persistence
10. **Semantic caching** — reduces duplicate LLM calls and costs

### 🔴 Critical Issues (Must Fix Before Production)

| # | Issue | Current State | Risk |
|---|---|---|---|
| 1 | **No authentication** | API is open to anyone | Anyone can query your Athena data and rack up LLM costs |
| 2 | **No HTTPS** | Runs on plain HTTP | Data transmitted in clear text |
| 3 | **No rate limiting** | Unlimited requests per user | DDoS / cost explosion risk |
| 4 | **Dev mode uvicorn** | `reload=True` in production | Watches files, restarts on changes — unstable in prod |
| 5 | **CORS allows only localhost** | `http://localhost:5173` | Frontend on WordPress domain will be blocked |
| 6 | **AWS creds via shared file** | `~/.aws/credentials` | Should use IAM roles in production |
| 7 | **No Docker container** | Runs bare on OS | Not reproducible, hard to deploy |
| 8 | **Missing pip dependencies** | `sse-starlette`, `numpy`, `langgraph`, `langgraph-checkpoint-sqlite` not in requirement.txt | Install will fail on fresh server |
| 9 | **SQLite checkpoint DB** | 514MB local file, grows forever | Will fill disk, not scalable |
| 10 | **Frontend API URL hardcoded** | `VITE_API_BASE=http://localhost:8000` | Must point to production API |

### 🟡 Important Issues (Should Fix)

| # | Issue | Recommendation |
|---|---|---|
| 11 | No process manager (gunicorn/supervisor) | Use gunicorn with uvicorn workers |
| 12 | No reverse proxy (nginx/ALB) | Put behind ALB or nginx |
| 13 | No monitoring/alerting | Add CloudWatch metrics + alarms |
| 14 | No health check depth | `/health` doesn't check Athena/Bedrock connectivity |
| 15 | Debug panel in production UI | Hide or remove `Show Debug` button with SQL |
| 16 | Checkpoint DB cleanup | Add TTL/rotation for old sessions |
| 17 | `requirement.txt` filename | Rename to `requirements.txt` (setup.sh expects this) |
| 18 | No `.env.example` file | Team members won't know required env vars |
| 19 | learned_examples.json grows unbounded | Add max size limit or rotation |
| 20 | No CSRF protection | Add for form-based interactions |

---

## PART 2: Deployment Architecture on AWS

### Recommended Architecture

```
WordPress (existing)
    │
    ├── Chatbot page with embedded iframe/widget
    │       │
    │       ▼
    │   CloudFront CDN ──► S3 Bucket (React frontend static files)
    │       │
    │       ▼ (API calls)
    │   Application Load Balancer (HTTPS, SSL cert)
    │       │
    │       ▼
    │   ECS Fargate (Docker container)
    │   ┌─────────────────────────────┐
    │   │  FastAPI Backend (gunicorn) │
    │   │  - GP Workforce Agent v6.0  │
    │   │  - IAM Role for Athena/Bedrock │
    │   └─────────────────────────────┘
    │       │           │
    │       ▼           ▼
    │   AWS Athena   AWS Bedrock
    │   (GP data)    (Nova Pro / Sonnet)
    │
    └── Rest of WordPress site (unchanged)
```

### WordPress Integration Options

**Option A — iframe embed (Recommended, simplest)**
- Host chatbot on subdomain: `chatbot.yourwebsite.com`
- Embed in WordPress page with `<iframe>` or shortcode
- Zero impact on existing WordPress site
- Independent scaling and deployments

**Option B — WordPress plugin with shortcode**
- Create a custom shortcode `[gp_chatbot]`
- Injects the React app's JS/CSS into the WordPress page
- More integrated look, but more complex to maintain

**Option C — Separate page, linked from WordPress**
- Chatbot lives on its own URL
- WordPress just links to it
- Simplest, but less integrated

---

## PART 3: Step-by-Step Deployment Checklist

### Phase 1: Fix Critical Code Issues (1–2 days)

- [ ] **1.1** Fix `requirement.txt` → rename to `requirements.txt` and add missing deps:
  ```
  sse-starlette>=1.6.0
  numpy>=1.26.0
  langgraph>=0.2.0
  langgraph-checkpoint-sqlite>=1.0.0
  ```
- [ ] **1.2** Remove `reload=True` from uvicorn.run() — add production startup script
- [ ] **1.3** Make CORS configurable — update to accept WordPress domain:
  ```python
  CORS_ORIGINS = os.getenv("CORS_ORIGINS", "https://yourwebsite.com,https://chatbot.yourwebsite.com")
  ```
- [ ] **1.4** Add basic API key authentication:
  ```python
  API_KEY = os.getenv("API_KEY")  # Required in production
  # Middleware to check X-API-Key header or query param
  ```
- [ ] **1.5** Add rate limiting (e.g., `slowapi` — 30 requests/min per session)
- [ ] **1.6** Add deep health check (verify Athena + Bedrock connectivity)
- [ ] **1.7** Add checkpoint DB size management (auto-prune sessions older than 7 days)
- [ ] **1.8** Create `.env.example` with all required variables documented

### Phase 2: Containerise with Docker (1 day)

- [ ] **2.1** Create `Dockerfile` for backend:
  ```dockerfile
  FROM python:3.11-slim
  WORKDIR /app
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  COPY . .
  CMD ["gunicorn", "gp_workforce_chatbot_backend_agent_v5:app",
       "-k", "uvicorn.workers.UvicornWorker",
       "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "120"]
  ```
- [ ] **2.2** Create `.dockerignore` (exclude node_modules, __pycache__, .langgraph_checkpoints.db, tests, old backend versions)
- [ ] **2.3** Create `docker-compose.yml` for local testing
- [ ] **2.4** Build frontend for production:
  ```bash
  cd gp-chat-ui
  VITE_API_BASE=https://api.chatbot.yourwebsite.com npm run build
  ```
- [ ] **2.5** Test Docker container locally

### Phase 3: AWS Infrastructure Setup (1–2 days)

- [ ] **3.1** Create ECR repository for Docker image
- [ ] **3.2** Create ECS Fargate cluster + task definition + service
- [ ] **3.3** Create IAM role for ECS task with policies:
  - `AmazonAthenaFullAccess`
  - `AmazonS3ReadOnlyAccess` (for Athena results bucket)
  - `AmazonBedrockFullAccess` (or scoped to specific models)
- [ ] **3.4** Create Application Load Balancer with:
  - HTTPS listener (port 443)
  - ACM SSL certificate for your domain
  - Target group pointing to ECS service
  - Health check on `/health`
- [ ] **3.5** Create S3 bucket for React frontend static files
- [ ] **3.6** Create CloudFront distribution pointing to S3 + ALB
- [ ] **3.7** Set up Route53 DNS:
  - `chatbot.yourwebsite.com` → CloudFront (frontend)
  - `api.chatbot.yourwebsite.com` → ALB (backend API)
- [ ] **3.8** Store secrets in AWS Secrets Manager or Parameter Store:
  - `API_KEY`
  - `BEDROCK_CHAT_MODEL_ID`
  - Any other sensitive config

### Phase 4: CI/CD Pipeline (optional, 1 day)

- [ ] **4.1** GitHub Actions or AWS CodePipeline workflow:
  - On push to `main`: build Docker → push to ECR → deploy to ECS
  - On push to `main`: build frontend → sync to S3 → invalidate CloudFront cache
- [ ] **4.2** Set up staging environment (separate ECS service + ALB)
- [ ] **4.3** Run test suite as part of CI before deploy

### Phase 5: WordPress Integration (half day)

- [ ] **5.1** Add chatbot page to WordPress
- [ ] **5.2** Option A (iframe):
  ```html
  <iframe
    src="https://chatbot.yourwebsite.com"
    width="100%"
    height="700px"
    frameborder="0"
    style="border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);"
  ></iframe>
  ```
- [ ] **5.3** Option B (shortcode plugin — if preferred):
  - Create custom WordPress plugin that loads React app's JS/CSS
  - Register shortcode `[gp_chatbot]`
  - Outputs a `<div id="gp-chatbot-root">` and loads the React bundle
- [ ] **5.4** Test on WordPress — verify CORS, SSL, streaming all work

### Phase 6: Monitoring & Go-Live (half day)

- [ ] **6.1** Set up CloudWatch alarms:
  - ECS CPU/memory > 80%
  - ALB 5xx error rate > 5%
  - ALB response time > 30s
- [ ] **6.2** Enable CloudWatch Logs for ECS container
- [ ] **6.3** Set up cost alerts in AWS Billing (Bedrock spend)
- [ ] **6.4** Test with team (2 users), verify everything works
- [ ] **6.5** Roll out to 10–15 users
- [ ] **6.6** Monitor for 1 week, then remove debug panel from UI

---

## PART 4: Cost Estimate (AWS Infrastructure)

| Service | Monthly Cost (est.) |
|---|---|
| ECS Fargate (1 vCPU, 2GB RAM) | £15–£30 |
| Application Load Balancer | £15–£20 |
| CloudFront CDN | £1–£5 |
| S3 (frontend static files) | < £1 |
| Route53 DNS | £1 |
| ACM SSL Certificate | Free |
| Bedrock LLM (Nova Pro, 200 msgs/day) | £23/month |
| Athena queries | £5–£15 |
| **Total estimated** | **£60–£95/month** |

---

## PART 5: Timeline Summary

| Phase | Effort | Description |
|---|---|---|
| Phase 1 | 1–2 days | Fix critical code issues |
| Phase 2 | 1 day | Docker containerisation |
| Phase 3 | 1–2 days | AWS infrastructure (ECS, ALB, S3, CloudFront) |
| Phase 4 | 1 day | CI/CD pipeline (optional) |
| Phase 5 | 0.5 day | WordPress integration |
| Phase 6 | 0.5 day | Monitoring + go-live |
| **Total** | **5–7 working days** | |
