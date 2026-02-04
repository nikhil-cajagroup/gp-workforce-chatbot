import os
import re
import json
import time
from typing import Dict, Any, List, Tuple, Optional, TypedDict

import boto3
import pandas as pd
import awswrangler as wr
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_aws import ChatBedrockConverse

from langgraph.graph import StateGraph, END


# =============================================================================
# FastAPI app
# =============================================================================
app = FastAPI(title="GP Workforce Athena Chatbot (Agent v4)", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
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
MAX_AGENT_LOOPS = int(os.getenv("MAX_AGENT_LOOPS", "3"))  # tries for empty/error recovery

CTAS_APPROACH = os.getenv("ATHENA_CTAS_APPROACH", "true").lower() == "true"

ALLOWED_TABLES = {"practice_high", "individual", "practice_detailed"}

SCHEMA_TTL_SECONDS = int(os.getenv("SCHEMA_TTL_SECONDS", "3600"))
LATEST_TTL_SECONDS = int(os.getenv("LATEST_TTL_SECONDS", "600"))
DISTINCT_TTL_SECONDS = int(os.getenv("DISTINCT_TTL_SECONDS", "1800"))

DOMAIN_NOTES_PATH = os.getenv("DOMAIN_NOTES_PATH", "gp_workforce_domain_notes.md")
DOMAIN_NOTES_MAX_CHARS = int(os.getenv("DOMAIN_NOTES_MAX_CHARS", "7000"))


# =============================================================================
# AWS Session
# =============================================================================
boto_sess = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


# =============================================================================
# API models
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
# Caches
# =============================================================================
_SCHEMA_CACHE: Dict[str, Tuple[float, List[Tuple[str, str]]]] = {}
_LATEST_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_DISTINCT_CACHE: Dict[str, Tuple[float, List[str]]] = {}  # key -> (ts, values)


# =============================================================================
# SQL Safety
# =============================================================================
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
    Allows:
      - your 3 base tables
      - any CTE names in WITH clause
    Blocks any other table references.
    """
    sql_low = sql.lower()

    # CTE names: WITH a AS (...), b AS (...)
    cte_names = set(re.findall(r"\bwith\s+([a-zA-Z0-9_]+)\s+as\s*\(", sql_low))
    cte_names.update(re.findall(r",\s*([a-zA-Z0-9_]+)\s+as\s*\(", sql_low))

    # FROM/JOIN targets
    tables = re.findall(r"(?:from|join)\s+([a-zA-Z0-9_\.]+)", sql_low, flags=re.IGNORECASE)

    found = set()
    for t in tables:
        t = t.split(".")[-1].lower()
        found.add(t)

    allowed = set(ALLOWED_TABLES) | cte_names
    illegal = [t for t in found if t not in allowed]
    if illegal:
        raise ValueError(f"Blocked: illegal tables referenced: {illegal}")


def add_limit(sql: str, limit: int = MAX_ROWS_RETURN) -> str:
    if re.search(r"\blimit\b", sql, re.IGNORECASE):
        return sql
    return f"{sql}\nLIMIT {limit}"


def safe_markdown(df: Optional[pd.DataFrame], head: int = 30) -> str:
    if df is None or df.empty:
        return "✅ Query ran successfully but returned 0 rows."
    return df.head(head).to_markdown(index=False)


# =============================================================================
# LLM
# =============================================================================
def llm_client() -> ChatBedrockConverse:
    return ChatBedrockConverse(
        model=BEDROCK_CHAT_MODEL_ID,
        region_name=AWS_REGION,
        temperature=0,
        max_tokens=1800,
    )


# =============================================================================
# Domain Notes (local file) + lightweight retrieval
# =============================================================================
def load_domain_notes() -> str:
    try:
        with open(DOMAIN_NOTES_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


DOMAIN_NOTES_TEXT = load_domain_notes()


def retrieve_domain_notes(question: str, max_chars: int = DOMAIN_NOTES_MAX_CHARS) -> str:
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
# Introspection tools (data-driven intelligence)
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


def list_distinct_values(
    table: str,
    column: str,
    where_sql: Optional[str] = None,
    limit: int = 200,
) -> List[str]:
    """
    Distinct value tool (used to stop hallucinations like 'GP staff').
    """
    key = f"{table}|{column}|{where_sql or ''}|{limit}"
    cached = _DISTINCT_CACHE.get(key)
    if cached and (now() - cached[0] < DISTINCT_TTL_SECONDS):
        return cached[1]

    where_clause = f"WHERE {where_sql}" if where_sql else ""
    sql = f"""
    SELECT DISTINCT {column} AS v
    FROM {table}
    {where_clause}
    ORDER BY v
    LIMIT {int(limit)}
    """
    df = run_athena_df(sql)
    values = [str(x) for x in df["v"].dropna().tolist()]
    _DISTINCT_CACHE[key] = (now(), values)
    return values


def search_best_name_match(
    table: str,
    name_col: str,
    query_text: str,
    year: Optional[str] = None,
    month: Optional[str] = None,
    limit: int = 8,
) -> List[str]:
    q = (query_text or "").strip()
    if not q:
        return []

    # Escape single quotes for SQL LIKE
    q_sql = q.replace("'", "''")

    filters = []
    if year is not None and month is not None:
        filters.append(f"year = '{year}' AND month = '{month}'")
    filters.append(f"{name_col} IS NOT NULL")
    filters.append(f"LOWER({name_col}) LIKE LOWER('%{q_sql}%')")

    where_sql = " AND ".join(filters)

    sql = f"""
    SELECT DISTINCT {name_col} AS v
    FROM {table}
    WHERE {where_sql}
    ORDER BY v
    LIMIT {int(limit)}
    """
    df = run_athena_df(sql)
    return [str(x) for x in df["v"].dropna().tolist()]

# =============================================================================
# LangGraph state
# =============================================================================
class AgentState(TypedDict, total=False):
    session_id: str
    question: str

    domain_notes: str

    latest_year: Optional[str]
    latest_month: Optional[str]

    staff_groups: List[str]
    staff_roles: List[str]
    detailed_staff_roles: List[str]

    resolved_entities: Dict[str, Any]  # e.g., {"icb_name": "...", "pcn_name": "...", ...}

    plan: Dict[str, Any]
    sql: str

    df_preview_md: str
    answer: str

    attempts: int
    last_error: Optional[str]
    needs_retry: bool


# =============================================================================
# Planner / SQL / Fixer prompts (agentic)
# =============================================================================
PLANNER_SYSTEM = """
You are a GP Workforce analytics assistant that plans how to answer questions using Athena.

You MUST:
- Decide if the question is IN SCOPE for GP Workforce data (FTE/headcount/workforce breakdowns).
- Identify the best table: individual (regional/demographic), practice_high (practice totals), practice_detailed (wide).
- Identify needed entities: icb_name, sub_icb_name, pcn_name, practice name, etc.
- Identify metric intent: totals, % split, ratio, trend, top-N, etc.

Return STRICT JSON ONLY:
{
  "in_scope": true|false,
  "table": "individual|practice_high|practice_detailed",
  "intent": "total|percent_split|ratio|trend|topn|lookup|unknown",
  "group_by": ["..."],
  "filters_needed": ["..."],
  "entities_to_resolve": ["icb_name|sub_icb_name|pcn_name|prac_name|prac_code"],
  "notes": "short"
}
"""

SQL_SYSTEM = """
You are an expert AWS Athena (Trino/Presto) SQL writer for GP Workforce.

Hard rules:
- Return ONLY SQL (no markdown, no explanation).
- Read-only only (SELECT or WITH ... SELECT).
- Allowed base tables: practice_high, individual, practice_detailed.
- Use latest year/month provided (do NOT guess dates).
- IMPORTANT: staff_group / staff_role / detailed_staff_role MUST use values from the provided lists (no hallucination).
- If user asks for "non-GP", interpret as staff_group <> 'GP' unless domain notes specify differently.
- If asked for a ratio: use NULLIF(denominator,0).

Goal: write correct SQL that matches the question.
"""

FIXER_SYSTEM = """
You fix SQL queries for GP Workforce Athena.

Rules:
- Return ONLY corrected SQL (no markdown).
- Keep it read-only (SELECT/WITH).
- Allowed base tables only: practice_high, individual, practice_detailed.
- Use provided staff_group/staff_role/detailed_staff_role lists.
- If previous SQL returned 0 rows, broaden:
  - remove overly strict filters,
  - use LIKE matching on names,
  - or pick the closest valid staff_group value from the list.
"""


SUMMARY_SYSTEM = """
You are a helpful NHS GP Workforce analyst.
Write a clear answer in 2-6 lines using ONLY the preview results and metadata.
If out of scope, say so clearly and suggest what the dataset CAN answer.
Do NOT invent numbers.
"""


# =============================================================================
# Graph nodes
# =============================================================================
def node_init(state: AgentState) -> AgentState:
    state["attempts"] = int(state.get("attempts", 0))
    state["needs_retry"] = False
    state["last_error"] = None
    state["domain_notes"] = retrieve_domain_notes(state["question"])
    return state


def node_fetch_latest_and_vocab(state: AgentState) -> AgentState:
    """
    Pull latest (year, month) and valid categorical vocab so the model can't hallucinate.
    """
    latest = get_latest_year_month("individual")
    state["latest_year"] = latest.get("year")
    state["latest_month"] = latest.get("month")

    y, m = state["latest_year"], state["latest_month"]
    where_latest = None
    if y and m:
        where_latest = f"year = '{y}' AND month = '{m}'"

    # Vocab lists (individual is best for national/regional workforce)
    state["staff_groups"] = list_distinct_values("individual", "staff_group", where_sql=where_latest, limit=300)
    state["staff_roles"] = list_distinct_values("individual", "staff_role", where_sql=where_latest, limit=400)
    state["detailed_staff_roles"] = list_distinct_values("individual", "detailed_staff_role", where_sql=where_latest, limit=600)

    return state


def node_plan(state: AgentState) -> AgentState:
    llm = llm_client()

    prompt = f"""
DOMAIN NOTES:
{state.get("domain_notes","")}

LATEST:
year={state.get("latest_year")} month={state.get("latest_month")}

QUESTION:
{state["question"]}
""".strip()

    raw = llm.invoke([
        SystemMessage(content=PLANNER_SYSTEM),
        HumanMessage(content=prompt),
    ]).content.strip()

    raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"```$", "", raw).strip()

    try:
        plan = json.loads(raw)
    except Exception:
        plan = {
            "in_scope": True,
            "table": "individual",
            "intent": "unknown",
            "group_by": [],
            "filters_needed": [],
            "entities_to_resolve": [],
            "notes": "fallback plan (invalid JSON from model)",
        }

    # normalize table
    t = str(plan.get("table", "individual")).lower()
    if t not in ALLOWED_TABLES:
        t = "individual"
    plan["table"] = t

    # normalize in_scope
    plan["in_scope"] = bool(plan.get("in_scope", True))

    state["plan"] = plan
    return state


def node_resolve_entities(state: AgentState) -> AgentState:
    """
    Resolve ICB/Sub-ICB/PCN/practice names if the question contains them.
    We keep it simple and only resolve when planner requests it.
    """
    plan = state.get("plan", {})
    need = plan.get("entities_to_resolve", []) or []

    y, m = state.get("latest_year"), state.get("latest_month")

    resolved: Dict[str, Any] = {}
    q = state["question"]

    # Very light extraction: the model will do the heavy lifting,
    # we just supply candidates for names.
    # Candidates returned are used later inside SQL prompts.
    if "icb_name" in need:
        resolved["icb_name_candidates"] = search_best_name_match(
            table="individual", name_col="icb_name", query_text=q, year=y, month=m, limit=8
        )
    if "sub_icb_name" in need:
        resolved["sub_icb_name_candidates"] = search_best_name_match(
            table="individual", name_col="sub_icb_name", query_text=q, year=y, month=m, limit=8
        )
    if "pcn_name" in need:
        # PCN is often in practice_detailed or practice_high; adjust if your schema differs
        # We'll try practice_high first (common for practice level data).
        # If your column name differs, update name_col.
        try:
            resolved["pcn_name_candidates"] = search_best_name_match(
                table="practice_high", name_col="pcn_name", query_text=q, year=y, month=m, limit=8
            )
        except Exception:
            resolved["pcn_name_candidates"] = []
    if "prac_name" in need:
        try:
            resolved["prac_name_candidates"] = search_best_name_match(
                table="practice_high", name_col="prac_name", query_text=q, year=y, month=m, limit=8
            )
        except Exception:
            resolved["prac_name_candidates"] = []

    state["resolved_entities"] = resolved
    return state


def node_generate_sql(state: AgentState) -> AgentState:
    plan = state.get("plan", {})
    if not plan.get("in_scope", True):
        state["sql"] = ""
        return state

    table = plan.get("table", "individual")
    schema = get_table_schema(table)

    schema_text = "\n".join([f"- {c} ({t})" for c, t in schema[:220]])

    y, m = state.get("latest_year"), state.get("latest_month")

    context = f"""
DOMAIN NOTES:
{state.get("domain_notes","")}

LATEST:
year={y} month={m}

TABLE:
{table}

SCHEMA:
{schema_text}

VALID VALUES (latest month):
- staff_group: {state.get("staff_groups", [])[:120]}
- staff_role: {state.get("staff_roles", [])[:120]}
- detailed_staff_role: {state.get("detailed_staff_roles", [])[:120]}

ENTITY CANDIDATES (if relevant):
{json.dumps(state.get("resolved_entities", {}), ensure_ascii=False)}

PLAN:
{json.dumps(plan, ensure_ascii=False)}

QUESTION:
{state["question"]}
""".strip()

    llm = llm_client()
    sql = llm.invoke([
        SystemMessage(content=SQL_SYSTEM),
        HumanMessage(content=context),
    ]).content.strip()

    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE).strip()
    sql = re.sub(r"```$", "", sql).strip()

    state["sql"] = sql
    return state


def node_run_sql(state: AgentState) -> AgentState:
    plan = state.get("plan", {})
    if not plan.get("in_scope", True):
        state["df_preview_md"] = ""
        return state

    sql = state.get("sql", "").strip()
    if not sql:
        raise ValueError("No SQL produced for an in-scope question.")

    try:
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        sql_safe = add_limit(sql_safe, MAX_ROWS_RETURN)

        df = run_athena_df(sql_safe)
        state["sql"] = sql_safe
        state["df_preview_md"] = safe_markdown(df, head=30)

        # store df size via meta-like fields
        state["_rows"] = int(len(df))
        state["_empty"] = bool(df.empty)

        state["last_error"] = None
        return state

    except Exception as e:
        state["last_error"] = str(e)
        state["_rows"] = 0
        state["_empty"] = True
        return state


def node_validate_or_fix(state: AgentState) -> AgentState:
    """
    If query errored or returned 0 rows (but should exist), attempt a smart repair.
    """
    plan = state.get("plan", {})
    if not plan.get("in_scope", True):
        state["needs_retry"] = False
        return state

    attempts = int(state.get("attempts", 0))
    last_error = state.get("last_error")
    empty = bool(state.get("_empty", False))

    # if ok + non-empty -> proceed
    if (not last_error) and (not empty):
        state["needs_retry"] = False
        return state

    # stop if too many loops
    if attempts >= MAX_AGENT_LOOPS:
        state["needs_retry"] = False
        return state

    # otherwise: call fixer LLM with real vocab & error/empty info
    llm = llm_client()

    table = plan.get("table", "individual")
    schema = get_table_schema(table)
    schema_text = "\n".join([f"- {c} ({t})" for c, t in schema[:250]])

    y, m = state.get("latest_year"), state.get("latest_month")

    fix_context = f"""
DOMAIN NOTES:
{state.get("domain_notes","")}

LATEST:
year={y} month={m}

TABLE:
{table}

SCHEMA:
{schema_text}

VALID VALUES (latest month):
- staff_group: {state.get("staff_groups", [])}
- staff_role: {state.get("staff_roles", [])}
- detailed_staff_role: {state.get("detailed_staff_roles", [])}

ENTITY CANDIDATES:
{json.dumps(state.get("resolved_entities", {}), ensure_ascii=False)}

QUESTION:
{state["question"]}

PREVIOUS SQL:
{state.get("sql","")}

ERROR (if any):
{last_error or ""}

RESULT EMPTY:
{empty}

Return corrected SQL only.
""".strip()

    fixed_sql = llm.invoke([
        SystemMessage(content=FIXER_SYSTEM),
        HumanMessage(content=fix_context),
    ]).content.strip()

    fixed_sql = re.sub(r"^```sql\s*", "", fixed_sql, flags=re.IGNORECASE).strip()
    fixed_sql = re.sub(r"```$", "", fixed_sql).strip()

    state["sql"] = fixed_sql
    state["attempts"] = attempts + 1
    state["needs_retry"] = True
    return state


def node_summarize(state: AgentState) -> AgentState:
    plan = state.get("plan", {})
    llm = llm_client()

    if not plan.get("in_scope", True):
        msg = f"""
QUESTION:
{state["question"]}

DOMAIN NOTES:
{state.get("domain_notes","")}

Explain clearly that this request is out of scope for GP Workforce dataset.
Then suggest what workforce questions ARE supported (FTE/headcount, ICB/sub-ICB/practice, demographics).
""".strip()
        ans = llm.invoke([
            SystemMessage(content=SUMMARY_SYSTEM),
            HumanMessage(content=msg),
        ]).content.strip()

        state["answer"] = ans
        state["df_preview_md"] = ""
        state["sql"] = ""
        return state

    msg = f"""
QUESTION:
{state["question"]}

SQL:
{state.get("sql","")}

RESULT PREVIEW:
{state.get("df_preview_md","")}

NOTES:
- latest year={state.get("latest_year")} month={state.get("latest_month")}
- if asked about non-GP: non-GP means staff_group <> 'GP'
""".strip()

    ans = llm.invoke([
        SystemMessage(content=SUMMARY_SYSTEM),
        HumanMessage(content=msg),
    ]).content.strip()

    # If still empty after all tries, give a helpful explanation
    if bool(state.get("_empty", False)) and ans:
        if "0 rows" not in ans.lower():
            ans += "\n\nNote: This returned 0 rows — likely due to filters not matching values in the dataset for that month."

    state["answer"] = ans
    return state


# =============================================================================
# Build the LangGraph
# =============================================================================
def build_graph():
    g = StateGraph(AgentState)

    g.add_node("init", node_init)
    g.add_node("latest_vocab", node_fetch_latest_and_vocab)
    g.add_node("plan", node_plan)
    g.add_node("resolve_entities", node_resolve_entities)
    g.add_node("generate_sql", node_generate_sql)
    g.add_node("run_sql", node_run_sql)
    g.add_node("validate_or_fix", node_validate_or_fix)
    g.add_node("summarize", node_summarize)

    g.set_entry_point("init")

    g.add_edge("init", "latest_vocab")
    g.add_edge("latest_vocab", "plan")
    g.add_edge("plan", "resolve_entities")
    g.add_edge("resolve_entities", "generate_sql")
    g.add_edge("generate_sql", "run_sql")
    g.add_edge("run_sql", "validate_or_fix")

    # Conditional loop: if needs_retry -> run again, else summarize
    def route_after_validate(state: AgentState) -> str:
        if state.get("needs_retry", False):
            return "run_sql"
        return "summarize"

    g.add_conditional_edges("validate_or_fix", route_after_validate, {
        "run_sql": "run_sql",
        "summarize": "summarize",
    })

    g.add_edge("summarize", END)

    return g.compile()


AGENT = build_graph()


# =============================================================================
# FastAPI routes
# =============================================================================
@app.get("/health")
def health():
    latest = get_latest_year_month("individual")
    return {
        "ok": True,
        "athena_db": ATHENA_DATABASE,
        "allowed_tables": sorted(list(ALLOWED_TABLES)),
        "ctas_approach": CTAS_APPROACH,
        "domain_notes_loaded": bool(DOMAIN_NOTES_TEXT.strip()),
        "domain_notes_path": DOMAIN_NOTES_PATH,
        "latest_individual": latest,
        "version": "4.0-agent",
    }


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        state: AgentState = {
            "session_id": req.session_id,
            "question": req.question,
            "attempts": 0,
        }

        out = AGENT.invoke(state)

        meta = {
            "plan": out.get("plan", {}),
            "resolved_entities": out.get("resolved_entities", {}),
            "attempts": int(out.get("attempts", 0)),
            "last_error": out.get("last_error"),
            "latest_year": out.get("latest_year"),
            "latest_month": out.get("latest_month"),
            "rows_returned": int(out.get("_rows", 0)),
        }

        return ChatResponse(
            answer=out.get("answer", ""),
            sql=out.get("sql", ""),
            preview_markdown=out.get("df_preview_md", ""),
            meta=meta,
        )

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={
                "error": str(e),
                "hint": "Check Athena permissions, column names in your tables, or DOMAIN_NOTES content.",
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


@app.get("/distinct/{table_name}/{column_name}")
def distinct(table_name: str, column_name: str):
    table_name = table_name.lower()
    column_name = column_name.strip()
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    where_latest = f"year = '{y}' AND month = '{m}'" if y and m else None
    vals = list_distinct_values(table_name, column_name, where_sql=where_latest, limit=500)
    return {"table": table_name, "column": column_name, "latest_filter": where_latest, "values": vals}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("gp_workforce_chatbot_backend_agent_v4:app", host="0.0.0.0", port=8000, reload=True)