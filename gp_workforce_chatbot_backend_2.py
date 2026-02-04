import os
import re
import json
import time
from typing import Dict, Any, List, Tuple, Optional

import boto3
import pandas as pd
import awswrangler as wr
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_aws import ChatBedrockConverse


# =============================================================================
# FastAPI app
# =============================================================================
app = FastAPI(title="GP Workforce Athena Chatbot", version="3.2 (Client Ready)")

# ✅ Frontend access (Vite default)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# CONFIG
# =============================================================================
AWS_PROFILE = os.getenv("AWS_PROFILE", "default")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")

ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "test-gp-workforce")
ATHENA_OUTPUT_S3 = os.getenv("ATHENA_OUTPUT_S3", "s3://test-athena-results-fingertips/")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "")

BEDROCK_CHAT_MODEL_ID = os.getenv("BEDROCK_CHAT_MODEL_ID", "amazon.nova-pro-v1:0")

MAX_ROWS_RETURN = int(os.getenv("MAX_ROWS_RETURN", "200"))
MAX_REPAIR_ATTEMPTS = int(os.getenv("MAX_REPAIR_ATTEMPTS", "2"))
CTAS_APPROACH = os.getenv("ATHENA_CTAS_APPROACH", "true").lower() == "true"

ALLOWED_TABLES = {"practice_high", "individual", "practice_detailed"}

SCHEMA_TTL_SECONDS = int(os.getenv("SCHEMA_TTL_SECONDS", "3600"))
LATEST_TTL_SECONDS = int(os.getenv("LATEST_TTL_SECONDS", "600"))

DOMAIN_NOTES_PATH = os.getenv("DOMAIN_NOTES_PATH", "gp_workforce_domain_notes.md")
DOMAIN_NOTES_MAX_CHARS = int(os.getenv("DOMAIN_NOTES_MAX_CHARS", "6000"))


# =============================================================================
# AWS Session
# =============================================================================
boto_sess = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


# =============================================================================
# Request / Response models
# =============================================================================
class ChatRequest(BaseModel):
    session_id: str
    question: str


class ChatResponse(BaseModel):
    answer: str
    sql: str
    preview_markdown: str
    meta: Dict[str, Any]


# =============================================================================
# In-memory chat memory (simple)
# =============================================================================
CHAT_MEMORY: Dict[str, List[Dict[str, str]]] = {}


# =============================================================================
# Caches
# =============================================================================
_SCHEMA_CACHE: Dict[str, Tuple[float, List[Tuple[str, str]]]] = {}
_LATEST_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


# =============================================================================
# Helpers
# =============================================================================
# ✅ allow SELECT or WITH (CTE)
READONLY_SQL_REGEX = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)


def now() -> float:
    return time.time()


def athena_kwargs() -> Dict[str, Any]:
    kw = {
        "database": ATHENA_DATABASE,
        "s3_output": ATHENA_OUTPUT_S3,
        "boto3_session": boto_sess,
        "ctas_approach": CTAS_APPROACH,
    }
    if ATHENA_WORKGROUP.strip():
        kw["workgroup"] = ATHENA_WORKGROUP.strip()
    return kw


def run_athena_df(sql: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(sql=sql, **athena_kwargs())


def enforce_readonly(sql: str) -> str:
    sql_clean = sql.strip().rstrip(";")

    if not READONLY_SQL_REGEX.match(sql_clean):
        raise ValueError("Blocked: only SELECT/WITH read-only queries are allowed.")

    bad = ["insert", "update", "delete", "drop", "alter", "create", "grant", "revoke"]
    if any(re.search(rf"\b{k}\b", sql_clean, re.IGNORECASE) for k in bad):
        raise ValueError("Blocked: query contains non-read-only keywords.")

    return sql_clean


def enforce_table_whitelist(sql: str) -> None:
    """
    ✅ Handles CTEs correctly.
    Example:
      WITH base AS (...) SELECT * FROM base JOIN individual ...
    We allow:
      - ALLOWED_TABLES
      - CTE names (like base)
    """
    sql_low = sql.lower()

    # Capture CTE names:
    # WITH base AS (...), other AS (...)
    cte_names = set(re.findall(r"\bwith\s+([a-zA-Z0-9_]+)\s+as\s*\(", sql_low))
    cte_names.update(re.findall(r",\s*([a-zA-Z0-9_]+)\s+as\s*\(", sql_low))

    # Find table names after FROM/JOIN
    tables = re.findall(r"(?:from|join)\s+([a-zA-Z0-9_\.]+)", sql_low, flags=re.IGNORECASE)

    found = set()
    for t in tables:
        t = t.split(".")[-1].lower()  # remove schema prefix if any
        found.add(t)

    allowed = set(ALLOWED_TABLES) | cte_names
    illegal = [t for t in found if t not in allowed]

    if illegal:
        raise ValueError(f"Blocked: illegal tables referenced: {illegal}")


def add_limit(sql: str, limit: int = MAX_ROWS_RETURN) -> str:
    if re.search(r"\blimit\b", sql, re.IGNORECASE):
        return sql
    return f"{sql}\nLIMIT {limit}"


def safe_markdown(df: pd.DataFrame, head: int = 30) -> str:
    if df is None or df.empty:
        return "✅ Query ran successfully but returned 0 rows."
    return df.head(head).to_markdown(index=False)


def llm_client() -> ChatBedrockConverse:
    return ChatBedrockConverse(
        model=BEDROCK_CHAT_MODEL_ID,
        region_name=AWS_REGION,
        temperature=0,
        max_tokens=1600,
    )


# =============================================================================
# Domain notes (local file)
# =============================================================================
def load_domain_notes() -> str:
    try:
        with open(DOMAIN_NOTES_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


DOMAIN_NOTES_TEXT = load_domain_notes()


def retrieve_domain_notes(question: str, max_chars: int = DOMAIN_NOTES_MAX_CHARS) -> str:
    """
    Light retrieval:
    - Split by headings
    - Keyword overlap score
    - Send top chunks into prompt
    """
    if not DOMAIN_NOTES_TEXT.strip():
        return ""

    q = question.lower()
    chunks = re.split(r"\n(?=## )", DOMAIN_NOTES_TEXT)
    scored: List[Tuple[int, str]] = []

    keywords = set(re.findall(r"[a-zA-Z]{3,}", q))
    for c in chunks:
        c_low = c.lower()
        score = sum(1 for k in keywords if k in c_low)
        if score > 0:
            scored.append((score, c))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = "\n\n".join([c for _, c in scored[:4]])

    if not top:
        top = DOMAIN_NOTES_TEXT[:max_chars]

    return top[:max_chars]


# =============================================================================
# Schema + latest helpers
# =============================================================================
def get_table_schema(table: str) -> List[Tuple[str, str]]:
    table = table.lower()
    if table not in ALLOWED_TABLES:
        raise ValueError("Unknown table requested.")

    cached = _SCHEMA_CACHE.get(table)
    if cached and (now() - cached[0] < SCHEMA_TTL_SECONDS):
        return cached[1]

    sql = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{ATHENA_DATABASE}'
      AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    df = run_athena_df(sql)
    schema = list(zip(df["column_name"].tolist(), df["data_type"].tolist()))

    _SCHEMA_CACHE[table] = (now(), schema)
    return schema


def get_latest_year_month(table: str) -> Dict[str, Any]:
    table = table.lower()

    cached = _LATEST_CACHE.get(table)
    if cached and (now() - cached[0] < LATEST_TTL_SECONDS):
        return cached[1]

    sql = f"""
    SELECT year, month
    FROM {table}
    WHERE year IS NOT NULL AND month IS NOT NULL
    ORDER BY CAST(year AS INTEGER) DESC, CAST(month AS INTEGER) DESC
    LIMIT 1
    """
    df = run_athena_df(sql)

    if df.empty:
        latest = {"year": None, "month": None}
    else:
        latest = {"year": str(df.iloc[0]["year"]), "month": str(df.iloc[0]["month"])}

    _LATEST_CACHE[table] = (now(), latest)
    return latest


# =============================================================================
# INTENT OVERRIDES (critical domain metrics)
# =============================================================================
def detect_intent(question: str) -> Optional[str]:
    q = question.lower()

    # ICB-wise GP vs non-GP %
    if (
        ("icb" in q)
        and ("gp" in q)
        and ("non gp" in q or "non-gp" in q or "nongp" in q or "other staff" in q)
        and ("percentage" in q or "percent" in q or "%" in q or "split" in q or "share" in q)
    ):
        return "icb_percent_gp_vs_nongp"

    # ICB-wise support ratio = non-GP per GP
    if (
        ("icb" in q or "by icb" in q)
        and ("support" in q or "non gp" in q or "non-gp" in q or "nongp" in q)
        and ("per gp" in q or "ratio" in q)
    ):
        return "icb_support_ratio_non_gp_per_gp"

    # National GP vs non-GP %
    if (
        ("gp" in q)
        and ("non gp" in q or "non-gp" in q or "nongp" in q or "other staff" in q)
        and ("percentage" in q or "percent" in q or "%" in q or "split" in q or "share" in q)
    ):
        return "national_percent_gp_vs_nongp"
    
        # Admin per clinical ratio
    if ("admin" in q) and ("clinical" in q) and ("ratio" in q or "per" in q):
        return "icb_admin_per_clinical_ratio"

    return None

def sql_icb_admin_per_clinical_ratio_latest() -> str:
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in individual table.")

    return f"""
WITH base AS (
  SELECT
    icb_code,
    icb_name,
    SUM(
      CASE
        WHEN regexp_like(lower(staff_group), 'admin|non[- ]?clinical')
        THEN fte ELSE 0
      END
    ) AS admin_fte,
    SUM(
      CASE
        WHEN NOT regexp_like(lower(staff_group), 'admin|non[- ]?clinical')
        THEN fte ELSE 0
      END
    ) AS clinical_fte
  FROM individual
  WHERE year = '{y}' AND month = '{m}'
    AND icb_code IS NOT NULL
    AND icb_name IS NOT NULL
  GROUP BY icb_code, icb_name
)
SELECT
  icb_code,
  icb_name,
  ROUND(admin_fte / NULLIF(clinical_fte, 0), 4) AS admin_per_clinical_ratio,
  ROUND(admin_fte, 2) AS admin_fte,
  ROUND(clinical_fte, 2) AS clinical_fte,
  '{y}' AS year,
  '{m}' AS month
FROM base
WHERE clinical_fte > 0
ORDER BY admin_per_clinical_ratio DESC
LIMIT 10
""".strip()


def sql_percent_gp_vs_nongp_latest() -> str:
    """
    National split (FTE) using INDIVIDUAL.
    Non-GP = everything except staff_group='GP'
    """
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in individual table.")

    return f"""
SELECT
  ROUND(100.0 * SUM(CASE WHEN staff_group = 'GP' THEN fte ELSE 0 END) / NULLIF(SUM(fte), 0), 4) AS gp_percentage,
  ROUND(100.0 * SUM(CASE WHEN staff_group <> 'GP' THEN fte ELSE 0 END) / NULLIF(SUM(fte), 0), 4) AS non_gp_percentage,
  ROUND(SUM(CASE WHEN staff_group = 'GP' THEN fte ELSE 0 END), 2) AS gp_fte,
  ROUND(SUM(CASE WHEN staff_group <> 'GP' THEN fte ELSE 0 END), 2) AS non_gp_fte,
  ROUND(SUM(fte), 2) AS total_fte,
  '{y}' AS year,
  '{m}' AS month
FROM individual
WHERE year = '{y}' AND month = '{m}'
""".strip()


def sql_icb_percent_gp_vs_nongp_latest() -> str:
    """
    ICB-wise split (FTE).
    Returns TOP 10 ICBs by non-GP % in latest month.
    """
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in individual table.")

    return f"""
WITH base AS (
  SELECT
    icb_code,
    icb_name,
    SUM(fte) AS total_fte,
    SUM(CASE WHEN staff_group = 'GP' THEN fte ELSE 0 END) AS gp_fte,
    SUM(CASE WHEN staff_group <> 'GP' THEN fte ELSE 0 END) AS non_gp_fte
  FROM individual
  WHERE year = '{y}' AND month = '{m}'
    AND icb_code IS NOT NULL
    AND icb_name IS NOT NULL
  GROUP BY icb_code, icb_name
)
SELECT
  icb_code,
  icb_name,
  ROUND(100.0 * gp_fte / NULLIF(total_fte, 0), 2) AS gp_percentage,
  ROUND(100.0 * non_gp_fte / NULLIF(total_fte, 0), 2) AS non_gp_percentage,
  ROUND(gp_fte, 2) AS gp_fte,
  ROUND(non_gp_fte, 2) AS non_gp_fte,
  ROUND(total_fte, 2) AS total_fte,
  '{y}' AS year,
  '{m}' AS month
FROM base
ORDER BY non_gp_percentage DESC
LIMIT 10
""".strip()


def sql_icb_support_ratio_non_gp_per_gp_latest() -> str:
    """
    Support staff per GP = non-GP FTE / GP FTE (ICB level).
    Returns TOP 10 ICBs by this ratio.
    """
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in individual table.")

    return f"""
WITH base AS (
  SELECT
    icb_code,
    icb_name,
    SUM(CASE WHEN staff_group = 'GP' THEN fte ELSE 0 END) AS gp_fte,
    SUM(CASE WHEN staff_group <> 'GP' THEN fte ELSE 0 END) AS non_gp_fte,
    SUM(fte) AS total_fte
  FROM individual
  WHERE year = '{y}' AND month = '{m}'
    AND icb_code IS NOT NULL
    AND icb_name IS NOT NULL
  GROUP BY icb_code, icb_name
)
SELECT
  icb_code,
  icb_name,
  ROUND(non_gp_fte / NULLIF(gp_fte, 0), 4) AS support_staff_per_gp,
  ROUND(gp_fte, 2) AS gp_fte,
  ROUND(non_gp_fte, 2) AS non_gp_fte,
  ROUND(total_fte, 2) AS total_fte,
  '{y}' AS year,
  '{m}' AS month
FROM base
WHERE gp_fte > 0
ORDER BY support_staff_per_gp DESC
LIMIT 10
""".strip()


# =============================================================================
# PLANNER
# =============================================================================
PLANNER_SYSTEM = """
You are a GP Workforce planning assistant.
Pick the best table for the question:

1) practice_high:
  prac_code, prac_name, staff_group, detailed_staff_role, measure, value, year, month

2) individual:
  comm_region_*, icb_*, sub_icb_*, staff_group, staff_role, detailed_staff_role,
  gender, age_band, country_qualification_group, fte, snapshot_date, year, month

3) practice_detailed:
  Very wide, many numeric columns stored as strings.

Return STRICT JSON only:
{
  "table": "practice_high|individual|practice_detailed",
  "needs_latest": true|false,
  "grouping_level": "practice|sub_icb|icb|comm_region|none",
  "filters": {"staff_group": "...", "staff_role": "...", "detailed_staff_role": "...", "measure": "..."},
  "notes": "short reasoning"
}
"""


def plan(question: str) -> Dict[str, Any]:
    llm = llm_client()
    domain = retrieve_domain_notes(question)

    raw = llm.invoke([
        SystemMessage(content=PLANNER_SYSTEM + "\n\nDOMAIN NOTES:\n" + domain),
        HumanMessage(content=question),
    ]).content.strip()

    raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"```$", "", raw).strip()

    try:
        obj = json.loads(raw)
    except Exception:
        obj = {
            "table": "practice_high",
            "needs_latest": True,
            "grouping_level": "none",
            "filters": {},
            "notes": "Fallback plan (invalid JSON from model).",
        }

    table = str(obj.get("table", "practice_high")).lower()
    if table not in ALLOWED_TABLES:
        table = "practice_high"

    obj["table"] = table
    obj["needs_latest"] = bool(obj.get("needs_latest", True))
    obj["grouping_level"] = obj.get("grouping_level", "none")
    obj["filters"] = obj.get("filters", {})

    return obj


# =============================================================================
# SQL GENERATION
# =============================================================================
SQL_SYSTEM = """
You are an expert NHS GP Workforce SQL assistant for AWS Athena (Presto/Trino).

Rules:
- ONLY SELECT/WITH read-only queries.
- ONLY allowed tables: practice_high, individual, practice_detailed.
- Must use correct column names from schema.
- If "latest" use latest year/month filter.
- practice_high totals: SUM(value)
- individual totals: SUM(fte)
Return SQL only. No markdown.
"""


def generate_sql(question: str, plan_obj: Dict[str, Any]) -> str:
    table = plan_obj["table"]
    schema = get_table_schema(table)
    latest = get_latest_year_month(table) if plan_obj.get("needs_latest") else {"year": None, "month": None}

    schema_text = "\n".join([f"- {c} ({t})" for c, t in schema[:160]])
    domain = retrieve_domain_notes(question)

    context = f"""
DOMAIN NOTES:
{domain}

TABLE: {table}
LATEST: year={latest.get("year")} month={latest.get("month")}

SCHEMA:
{schema_text}

PLAN:
{json.dumps(plan_obj, ensure_ascii=False)}

QUESTION:
{question}
""".strip()

    llm = llm_client()
    sql = llm.invoke([
        SystemMessage(content=SQL_SYSTEM),
        HumanMessage(content=context),
    ]).content.strip()

    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE).strip()
    sql = re.sub(r"```$", "", sql).strip()

    return sql


# =============================================================================
# SQL REPAIR
# =============================================================================
REPAIR_SYSTEM = """
You fix Athena SQL for GP Workforce chatbot.
Return ONLY corrected SQL.
Rules:
- SELECT/WITH only
- allowed tables only: practice_high, individual, practice_detailed
- use correct schema columns
"""


def repair_sql(question: str, bad_sql: str, error: str, plan_obj: Dict[str, Any]) -> str:
    table = plan_obj["table"]
    schema = get_table_schema(table)
    schema_text = "\n".join([f"- {c} ({t})" for c, t in schema[:220]])
    domain = retrieve_domain_notes(question)

    prompt = f"""
DOMAIN NOTES:
{domain}

QUESTION:
{question}

FAILED SQL:
{bad_sql}

ERROR:
{error}

TABLE: {table}
SCHEMA:
{schema_text}

Return corrected SQL only.
""".strip()

    llm = llm_client()
    fixed = llm.invoke([
        SystemMessage(content=REPAIR_SYSTEM),
        HumanMessage(content=prompt),
    ]).content.strip()

    fixed = re.sub(r"^```sql\s*", "", fixed, flags=re.IGNORECASE).strip()
    fixed = re.sub(r"```$", "", fixed).strip()

    return fixed


# =============================================================================
# Result summarizer
# =============================================================================
SUMMARY_SYSTEM = """
You are a helpful NHS GP Workforce analyst.
Write a short answer (2-6 lines) based ONLY on the preview table.
If asked about "non-GP", explain it means all staff_group values except 'GP'.
Do NOT invent numbers.
"""


def summarize_answer(question: str, df: pd.DataFrame, sql: str) -> str:
    preview = safe_markdown(df, head=15)
    domain = retrieve_domain_notes(question)

    llm = llm_client()
    text = llm.invoke([
        SystemMessage(content=SUMMARY_SYSTEM + "\n\nDOMAIN NOTES:\n" + domain),
        HumanMessage(content=f"""
QUESTION:
{question}

SQL:
{sql}

RESULT PREVIEW:
{preview}
""".strip()),
    ]).content.strip()

    return text


# =============================================================================
# Main answer handler
# =============================================================================
def answer(question: str, session_id: str) -> Dict[str, Any]:
    CHAT_MEMORY.setdefault(session_id, []).append({"role": "user", "content": question})

    intent = detect_intent(question)

    # ✅ OVERRIDES (client-consistent answers)
    if intent == "icb_percent_gp_vs_nongp":
        sql = sql_icb_percent_gp_vs_nongp_latest()
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        df = run_athena_df(sql_safe)

        preview_md = safe_markdown(df, head=30)
        final_answer = summarize_answer(question, df, sql_safe)
        CHAT_MEMORY[session_id].append({"role": "assistant", "content": final_answer})

        return {
            "answer": final_answer,
            "sql": sql_safe,
            "preview_markdown": preview_md,
            "meta": {
                "intent_override": intent,
                "rows_returned": int(len(df)),
                "ctas_approach": CTAS_APPROACH,
                "attempts": 1,
                "last_error": None,
            },
        }

    if intent == "national_percent_gp_vs_nongp":
        sql = sql_percent_gp_vs_nongp_latest()
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        df = run_athena_df(sql_safe)

        preview_md = safe_markdown(df, head=30)
        final_answer = summarize_answer(question, df, sql_safe)
        CHAT_MEMORY[session_id].append({"role": "assistant", "content": final_answer})

        return {
            "answer": final_answer,
            "sql": sql_safe,
            "preview_markdown": preview_md,
            "meta": {
                "intent_override": intent,
                "rows_returned": int(len(df)),
                "ctas_approach": CTAS_APPROACH,
                "attempts": 1,
                "last_error": None,
            },
        }

    if intent == "icb_support_ratio_non_gp_per_gp":
        sql = sql_icb_support_ratio_non_gp_per_gp_latest()
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        df = run_athena_df(sql_safe)

        preview_md = safe_markdown(df, head=30)
        final_answer = summarize_answer(question, df, sql_safe)
        CHAT_MEMORY[session_id].append({"role": "assistant", "content": final_answer})

        return {
            "answer": final_answer,
            "sql": sql_safe,
            "preview_markdown": preview_md,
            "meta": {
                "intent_override": intent,
                "rows_returned": int(len(df)),
                "ctas_approach": CTAS_APPROACH,
                "attempts": 1,
                "last_error": None,
            },
        }
    if intent == "icb_admin_per_clinical_ratio":
        sql = sql_icb_admin_per_clinical_ratio_latest()
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        df = run_athena_df(sql_safe)

        preview_md = safe_markdown(df, head=30)
        final_answer = summarize_answer(question, df, sql_safe)

        CHAT_MEMORY[session_id].append({"role": "assistant", "content": final_answer})

        return {
            "answer": final_answer,
            "sql": sql_safe,
            "preview_markdown": preview_md,
            "meta": {
                "intent_override": intent,
                "rows_returned": int(len(df)),
                "ctas_approach": CTAS_APPROACH,
                "attempts": 1,
                "last_error": None,
            },
        }
    # ✅ NORMAL LLM path
    plan_obj = plan(question)
    sql = generate_sql(question, plan_obj)

    attempt = 0
    last_err = None
    df = None

    while attempt <= MAX_REPAIR_ATTEMPTS:
        try:
            sql_safe = enforce_readonly(sql)
            enforce_table_whitelist(sql_safe)
            sql_safe = add_limit(sql_safe, MAX_ROWS_RETURN)

            df = run_athena_df(sql_safe)
            sql = sql_safe
            break

        except Exception as e:
            last_err = str(e)
            if attempt == MAX_REPAIR_ATTEMPTS:
                raise
            sql = repair_sql(question, sql, last_err, plan_obj)
            attempt += 1

    preview_md = safe_markdown(df, head=30)
    final_answer = summarize_answer(question, df, sql)

    CHAT_MEMORY[session_id].append({"role": "assistant", "content": final_answer})

    return {
        "answer": final_answer,
        "sql": sql,
        "preview_markdown": preview_md,
        "meta": {
            "plan": plan_obj,
            "rows_returned": int(0 if df is None else len(df)),
            "ctas_approach": CTAS_APPROACH,
            "attempts": attempt + 1,
            "last_error": last_err,
        },
    }


# =============================================================================
# API Routes
# =============================================================================
@app.get("/health")
def health():
    return {
        "ok": True,
        "athena_db": ATHENA_DATABASE,
        "allowed_tables": sorted(list(ALLOWED_TABLES)),
        "ctas_approach": CTAS_APPROACH,
        "domain_notes_loaded": bool(DOMAIN_NOTES_TEXT.strip()),
        "domain_notes_path": DOMAIN_NOTES_PATH,
        "version": "3.2",
    }


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        out = answer(req.question, req.session_id)
        return ChatResponse(**out)
    except Exception as e:
        # ✅ No more ugly 500 stacktrace to frontend
        return JSONResponse(
            status_code=400,
            content={
                "error": str(e),
                "hint": "Check SQL safety rules, schema mismatch, or Athena permissions.",
            },
        )


@app.get("/schema/{table_name}")
def schema(table_name: str):
    table_name = table_name.lower()
    schema_list = get_table_schema(table_name)
    latest = get_latest_year_month(table_name)

    return {
        "table": table_name,
        "columns": [{"name": c, "type": t} for c, t in schema_list],
        "latest": latest,
    }


# =============================================================================
# Run locally
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("gp_workforce_chatbot_backend_3:app", host="0.0.0.0", port=8000, reload=True)
