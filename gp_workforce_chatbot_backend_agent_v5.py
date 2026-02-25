"""
GP Workforce Chatbot Backend — Agent v5.1
==========================================
Production-hardened version with:
  1. Conversation memory (per session, last N turns)
  2. Schema CSVs loaded + column dictionary for human-readable labels
  3. Fuzzy entity matching (difflib) — handles misspellings
  4. practice_high vocab (measure, staff_group, detailed_staff_role) fed to LLM
  5. Time-range resolver ("last 12 months", "year over year", "since 2020")
  6. Plan validation against actual schema before SQL generation
  7. Richer prompts with column dictionary context
  8. Structured answer formatting (key numbers highlighted)
  9. Suggested follow-up questions returned in response
 10. Better hard-intent detection (more patterns)
 11. Query result caching (normalised SQL key)
 12. [v5.1] SQL injection protection — strict input sanitisation
 13. [v5.1] Structured logging with request tracing
 14. [v5.1] Request timeouts with async wrapping
 15. [v5.1] Hardened SQL safety (comment stripping, expanded blocklist)
 16. [v5.1] Bounded caches to prevent memory leaks
 17. [v5.1] Sanitised error responses
 18. [v5.1] Input validation with Pydantic constraints
"""

import os
import re
import json
import time
import uuid
import hashlib
import difflib
import logging
import asyncio
from collections import OrderedDict
from typing import Dict, Any, List, Tuple, Optional, TypedDict
from functools import wraps

import boto3
import pandas as pd
import awswrangler as wr
from fastapi import FastAPI, Request
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_aws import ChatBedrockConverse

from langgraph.graph import StateGraph, END


# =============================================================================
# Logging
# =============================================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gp_workforce_chatbot")


# =============================================================================
# FastAPI app
# =============================================================================
app = FastAPI(title="GP Workforce Athena Chatbot (Agent v5.1)", version="5.1")

# CORS — configurable via env, defaults to localhost dev
_CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _CORS_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# Request timeout (seconds)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))


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
MAX_AGENT_LOOPS = int(os.getenv("MAX_AGENT_LOOPS", "3"))
CTAS_APPROACH = os.getenv("ATHENA_CTAS_APPROACH", "true").lower() == "true"

ALLOWED_TABLES = {"practice_high", "individual", "practice_detailed"}

SCHEMA_TTL_SECONDS = int(os.getenv("SCHEMA_TTL_SECONDS", "3600"))
LATEST_TTL_SECONDS = int(os.getenv("LATEST_TTL_SECONDS", "600"))
DISTINCT_TTL_SECONDS = int(os.getenv("DISTINCT_TTL_SECONDS", "1800"))
QUERY_CACHE_TTL = int(os.getenv("QUERY_CACHE_TTL", "300"))

DOMAIN_NOTES_PATH = os.getenv("DOMAIN_NOTES_PATH", "gp_workforce_domain_notes.md")
DOMAIN_NOTES_MAX_CHARS = int(os.getenv("DOMAIN_NOTES_MAX_CHARS", "12000"))

COLUMN_DICT_PATH = os.getenv("COLUMN_DICT_PATH", "./schemas/column_dictionary.json")

INDIVIDUAL_COLS_CSV = os.getenv("INDIVIDUAL_COLS_CSV", "./schemas/individual_cols.csv")
PRACTICE_DETAILED_COLS_CSV = os.getenv("PRACTICE_DETAILED_COLS_CSV", "./schemas/practice_detailed_cols.csv")
PRACTICE_HIGH_COLS_CSV = os.getenv("PRACTICE_HIGH_COLS_CSV", "./schemas/practice_high_cols.csv")

MEMORY_MAX_TURNS = int(os.getenv("MEMORY_MAX_TURNS", "6"))

# Max input length for user questions
MAX_QUESTION_LENGTH = int(os.getenv("MAX_QUESTION_LENGTH", "1000"))
# Max entity hint length for SQL interpolation
MAX_ENTITY_LENGTH = int(os.getenv("MAX_ENTITY_LENGTH", "100"))


# =============================================================================
# AWS Session
# =============================================================================
boto_sess = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


# =============================================================================
# API models
# =============================================================================
class ChatRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64, pattern=r'^[a-zA-Z0-9_\-]+$')
    question: str = Field(..., min_length=1, max_length=1000)


class ChatResponse(BaseModel):
    answer: str
    sql: str
    preview_markdown: str
    meta: Dict[str, Any]
    suggestions: List[str]


# =============================================================================
# Conversation Memory (per session)
# =============================================================================
class ConversationMemory:
    """Stores last N turns per session + last entity context for follow-ups."""

    def __init__(self, max_sessions: int = 200, max_turns: int = MEMORY_MAX_TURNS):
        self._store: OrderedDict[str, List[Dict[str, str]]] = OrderedDict()
        self._entity_context: Dict[str, Dict[str, Any]] = {}
        self._max_sessions = max_sessions
        self._max_turns = max_turns

    def get_history(self, session_id: str) -> List[Dict[str, str]]:
        return self._store.get(session_id, [])

    def get_entity_context(self, session_id: str) -> Dict[str, Any]:
        """Return the last entity context for this session (practice name, ICB, table, etc)."""
        return self._entity_context.get(session_id, {})

    def save_entity_context(self, session_id: str, context: Dict[str, Any]):
        """Save entity context from the current turn for follow-up resolution."""
        self._entity_context[session_id] = context

    def add_turn(self, session_id: str, question: str, answer: str, sql: str = "",
                 entity_context: Optional[Dict[str, Any]] = None):
        if session_id not in self._store:
            self._store[session_id] = []
            if len(self._store) > self._max_sessions:
                oldest_key = next(iter(self._store))
                self._store.pop(oldest_key)
                self._entity_context.pop(oldest_key, None)
        turns = self._store[session_id]
        turns.append({"role": "user", "content": question})
        summary = answer[:500]
        if sql:
            summary += f"\n[SQL used: {sql[:200]}]"
        turns.append({"role": "assistant", "content": summary})
        if len(turns) > self._max_turns * 2:
            self._store[session_id] = turns[-(self._max_turns * 2):]
        # Store entity context for follow-ups
        if entity_context:
            self.save_entity_context(session_id, entity_context)

    def format_for_prompt(self, session_id: str) -> str:
        history = self.get_history(session_id)
        if not history:
            return ""
        lines = []
        for turn in history:
            prefix = "User" if turn["role"] == "user" else "Assistant"
            lines.append(f"{prefix}: {turn['content']}")
        return "\n".join(lines)


MEMORY = ConversationMemory()


# =============================================================================
# Bounded TTL Cache (prevents memory leaks)
# =============================================================================
class BoundedTTLCache:
    """Simple TTL cache with a max-size eviction policy (LRU)."""

    def __init__(self, max_size: int = 100, ttl: float = 3600):
        self._store: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl

    def get(self, key: str) -> Optional[Tuple[float, Any]]:
        item = self._store.get(key)
        if item and (now() - item[0] < self._ttl):
            self._store.move_to_end(key)
            return item
        if item:
            self._store.pop(key, None)
        return None

    def set(self, key: str, value: Any):
        self._store[key] = (now(), value)
        self._store.move_to_end(key)
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)

    def __len__(self):
        return len(self._store)


# =============================================================================
# Caches (all bounded)
# =============================================================================
_SCHEMA_CACHE = BoundedTTLCache(max_size=20, ttl=SCHEMA_TTL_SECONDS)
_LATEST_CACHE = BoundedTTLCache(max_size=20, ttl=LATEST_TTL_SECONDS)
_DISTINCT_CACHE = BoundedTTLCache(max_size=200, ttl=DISTINCT_TTL_SECONDS)
_SCHEMA_OVERRIDE: Dict[str, List[str]] = {}
_QUERY_CACHE = BoundedTTLCache(max_size=50, ttl=QUERY_CACHE_TTL)


def now() -> float:
    return time.time()


# =============================================================================
# Column Dictionary
# =============================================================================
def load_column_dictionary() -> Dict[str, Any]:
    try:
        with open(COLUMN_DICT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


COLUMN_DICT = load_column_dictionary()


def get_column_labels(table: str) -> str:
    """Return a compact string of key column -> human label mappings for a table."""
    tdata = COLUMN_DICT.get(table, {})
    if not tdata:
        return ""
    lines = []
    for section_name, section in tdata.items():
        if section_name.startswith("_"):
            continue
        if isinstance(section, dict):
            for col, label in section.items():
                if isinstance(label, str):
                    lines.append(f"  {col} = {label}")
                elif isinstance(label, dict):
                    for subcol, sublabel in label.items():
                        lines.append(f"  {subcol} = {sublabel}")
        elif isinstance(section, str):
            lines.append(f"  {section_name} = {section}")
    return "\n".join(lines[:150])


# =============================================================================
# SQL Safety + Input Sanitisation
# =============================================================================
READONLY_SQL_REGEX = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)

# Whitelist pattern for entity names (practice names, ICB names, etc.)
# Allows: letters, digits, spaces, hyphens, apostrophes, commas, periods, ampersands, parentheses
_SAFE_ENTITY_PATTERN = re.compile(r"^[a-zA-Z0-9\s\-'.,&()/]+$")


def sanitise_entity_input(value: str, field_name: str = "entity") -> str:
    """
    Sanitise user-provided entity names before SQL interpolation.
    Raises ValueError if input looks malicious.
    """
    if not value or not value.strip():
        raise ValueError(f"Empty {field_name} provided.")
    value = value.strip()
    if len(value) > MAX_ENTITY_LENGTH:
        raise ValueError(f"{field_name} too long (max {MAX_ENTITY_LENGTH} chars).")
    if not _SAFE_ENTITY_PATTERN.match(value):
        raise ValueError(f"Invalid characters in {field_name}. Only letters, digits, spaces, hyphens, apostrophes, commas, periods, ampersands, and parentheses are allowed.")
    # Double-escape single quotes for SQL safety
    value = value.replace("'", "''")
    return value


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments that could hide malicious keywords."""
    # Remove single-line comments (-- ...)
    sql = re.sub(r'--[^\n]*', '', sql)
    # Remove multi-line comments (/* ... */)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql.strip()


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
    cache_key = hashlib.md5(sql.strip().lower().encode()).hexdigest()
    cached = _QUERY_CACHE.get(cache_key)
    if cached:
        logger.debug("Query cache HIT: %s", cache_key[:12])
        return cached[1].copy()
    t0 = time.time()
    df = wr.athena.read_sql_query(sql=sql, **athena_kwargs())
    elapsed = time.time() - t0
    logger.info("Athena query executed in %.2fs, rows=%d", elapsed, len(df))
    _QUERY_CACHE.set(cache_key, df.copy())
    return df


def enforce_readonly(sql: str) -> str:
    # Strip comments first so they can't hide dangerous keywords
    sql_clean = _strip_sql_comments(sql).rstrip(";")
    if not READONLY_SQL_REGEX.match(sql_clean):
        raise ValueError("Blocked: only SELECT/WITH read-only queries are allowed.")
    bad = [
        "insert", "update", "delete", "drop", "alter", "create",
        "grant", "revoke", "truncate", "merge", "copy", "unload",
        "explain", "call", "execute",
    ]
    # Also check for SELECT ... INTO
    if re.search(r"\bselect\b.+\binto\b", sql_clean, re.IGNORECASE | re.DOTALL):
        raise ValueError("Blocked: SELECT INTO is not allowed.")
    if any(re.search(rf"\b{k}\b", sql_clean, re.IGNORECASE) for k in bad):
        raise ValueError("Blocked: query contains non-read-only keywords.")
    return sql_clean


def enforce_table_whitelist(sql: str) -> None:
    sql_low = _strip_sql_comments(sql).lower()
    cte_names = set(re.findall(r"\bwith\s+([a-zA-Z0-9_]+)\s+as\s*\(", sql_low))
    cte_names.update(re.findall(r",\s*([a-zA-Z0-9_]+)\s+as\s*\(", sql_low))
    tables = re.findall(r"(?:from|join)\s+([a-zA-Z0-9_\.]+)", sql_low, flags=re.IGNORECASE)
    found = set()
    for t in tables:
        t = t.split(".")[-1].lower()
        found.add(t)
    allowed = set(ALLOWED_TABLES) | cte_names | {"information_schema"}
    illegal = [t for t in found if t not in allowed]
    if illegal:
        raise ValueError(f"Blocked: illegal tables referenced: {illegal}")


def add_limit(sql: str, limit: int = MAX_ROWS_RETURN) -> str:
    if re.search(r"\blimit\b", sql, re.IGNORECASE):
        return sql
    return f"{sql}\nLIMIT {limit}"


def safe_markdown(df: Optional[pd.DataFrame], head: int = 30) -> str:
    if df is None or df.empty:
        return ""
    return df.head(head).to_markdown(index=False)


# =============================================================================
# LLM
# =============================================================================
def llm_client() -> ChatBedrockConverse:
    return ChatBedrockConverse(
        model=BEDROCK_CHAT_MODEL_ID,
        region_name=AWS_REGION,
        temperature=0,
        max_tokens=2000,
    )


# =============================================================================
# Domain Notes
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
    top = "\n\n".join([c for _, c in scored[:6]])
    if not top:
        top = DOMAIN_NOTES_TEXT[:max_chars]
    return top[:max_chars]


# =============================================================================
# Schema overrides from CSVs
# =============================================================================
def _load_cols_from_csv(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    return [c.strip() for c in df.columns.tolist() if str(c).strip()]


def load_schema_overrides() -> Dict[str, List[str]]:
    overrides: Dict[str, List[str]] = {}
    overrides["individual"] = _load_cols_from_csv(INDIVIDUAL_COLS_CSV)
    overrides["practice_detailed"] = _load_cols_from_csv(PRACTICE_DETAILED_COLS_CSV)
    overrides["practice_high"] = _load_cols_from_csv(PRACTICE_HIGH_COLS_CSV)
    return overrides


_SCHEMA_OVERRIDE = load_schema_overrides()


# =============================================================================
# Introspection tools
# =============================================================================
def get_table_schema(table: str) -> List[Tuple[str, str]]:
    table = table.lower()
    if table not in ALLOWED_TABLES:
        raise ValueError("Unknown table requested.")
    cached = _SCHEMA_CACHE.get(table)
    if cached:
        return cached[1]
    try:
        sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{ATHENA_DATABASE}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        df = run_athena_df(sql)
        schema = list(zip(df["column_name"].tolist(), df["data_type"].tolist()))
        _SCHEMA_CACHE.set(table, schema)
        logger.info("Schema loaded for %s: %d columns", table, len(schema))
        return schema
    except Exception as e:
        logger.warning("Schema introspection failed for %s: %s — using override", table, e)
        cols = _SCHEMA_OVERRIDE.get(table, [])
        schema = [(c, "unknown") for c in cols]
        _SCHEMA_CACHE.set(table, schema)
        return schema


def get_latest_year_month(table: str) -> Dict[str, Any]:
    table = table.lower()
    cached = _LATEST_CACHE.get(table)
    if cached:
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
    _LATEST_CACHE.set(table, latest)
    logger.info("Latest year/month for %s: %s/%s", table, latest.get("year"), latest.get("month"))
    return latest


def list_distinct_values(
    table: str,
    column: str,
    where_sql: Optional[str] = None,
    limit: int = 200,
) -> List[str]:
    key = f"{table}|{column}|{where_sql or ''}|{limit}"
    cached = _DISTINCT_CACHE.get(key)
    if cached:
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
    _DISTINCT_CACHE.set(key, values)
    return values


# =============================================================================
# Fuzzy entity matching (improvement #3)
# =============================================================================
def fuzzy_match(query: str, candidates: List[str], threshold: float = 0.5, top_n: int = 5) -> List[Tuple[str, float]]:
    """Return candidates sorted by similarity score above threshold."""
    if not query or not candidates:
        return []
    q = query.lower().strip()
    scored = []
    for c in candidates:
        c_low = c.lower().strip()
        # exact substring match gets highest score
        if q in c_low:
            scored.append((c, 0.95))
            continue
        if c_low in q:
            scored.append((c, 0.9))
            continue
        ratio = difflib.SequenceMatcher(None, q, c_low).ratio()
        # also try matching individual words
        q_words = set(q.split())
        c_words = set(c_low.split())
        word_overlap = len(q_words & c_words) / max(len(q_words), 1)
        combined = max(ratio, word_overlap)
        if combined >= threshold:
            scored.append((c, combined))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_n]


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
    q_sql = sanitise_entity_input(q, "search_query")
    filters = []
    if year is not None and month is not None:
        filters.append(f"year = '{year}' AND month = '{month}'")
    filters.append(f"{name_col} IS NOT NULL")
    filters.append(f"LOWER(TRIM({name_col})) LIKE LOWER('%{q_sql}%')")
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


def resolve_entity_fuzzy(
    table: str,
    name_col: str,
    user_text: str,
    year: Optional[str] = None,
    month: Optional[str] = None,
) -> List[str]:
    """
    First try DB LIKE search. If empty, load distinct values and fuzzy match.
    """
    db_results = search_best_name_match(table, name_col, user_text, year, month, limit=10)
    if db_results:
        return db_results

    # Fuzzy fallback: get all distinct values for this column
    where_sql = None
    if year and month:
        where_sql = f"year = '{year}' AND month = '{month}'"
    all_values = list_distinct_values(table, name_col, where_sql=where_sql, limit=500)
    matches = fuzzy_match(user_text, all_values, threshold=0.45, top_n=8)
    return [m[0] for m in matches]


# =============================================================================
# Time range resolver (improvement #5)
# =============================================================================
def resolve_time_range(question: str, latest_year: str, latest_month: str) -> Optional[Dict[str, Any]]:
    """
    Parse relative time expressions from the question.
    Returns dict with start_year, start_month, end_year, end_month, description.
    """
    q = question.lower()
    ly = int(latest_year)
    lm = int(latest_month)

    def _months_back(n: int) -> Tuple[int, int]:
        total = ly * 12 + lm - n
        return total // 12, total % 12 or 12

    # "last N months"
    m = re.search(r"last\s+(\d+)\s+months?", q)
    if m:
        n = int(m.group(1))
        sy, sm = _months_back(n)
        return {
            "start_year": str(sy), "start_month": f"{sm:02d}",
            "end_year": latest_year, "end_month": latest_month,
            "description": f"Last {n} months",
        }

    # "since YYYY" / "from YYYY"
    m = re.search(r"(?:since|from)\s+(20\d{2})", q)
    if m:
        return {
            "start_year": m.group(1), "start_month": "01",
            "end_year": latest_year, "end_month": latest_month,
            "description": f"Since {m.group(1)}",
        }

    # "year over year" / "yoy" / "compared to last year" / "vs last year"
    if re.search(r"year\s*(over|on)\s*year|yoy|compared?\s*to\s*last\s*year|vs\.?\s*last\s*year", q):
        prev_y = ly - 1
        return {
            "start_year": str(prev_y), "start_month": latest_month,
            "end_year": latest_year, "end_month": latest_month,
            "description": f"Year-over-year ({prev_y} vs {ly})",
            "compare_years": [str(prev_y), latest_year],
        }

    # "last year" (not "vs last year" which is caught above)
    if re.search(r"\blast\s+year\b", q) and not re.search(r"vs|compared|over", q):
        return {
            "start_year": str(ly - 1), "start_month": "01",
            "end_year": str(ly - 1), "end_month": "12",
            "description": f"Last year ({ly - 1})",
        }

    return None


# =============================================================================
# Deterministic hard intents (expanded)
# =============================================================================
def detect_hard_intent(question: str) -> Optional[str]:
    q = question.lower().strip()

    # practice -> ICB lookup
    if ("icb" in q) and ("practice" in q or "prac" in q) and ("where" in q or "located" in q or "which" in q):
        return "practice_to_icb_lookup"

    # how many GP at a practice
    if ("how many" in q or "number of" in q or "no. of" in q) and ("gp" in q) and ("practice" in q or "prac" in q):
        return "practice_gp_count"

    # shorthand: "how many gp in keele"
    if ("how many" in q or "number of" in q) and ("gp" in q) and len(q.split()) <= 8:
        return "practice_gp_count_soft"

    # total patients at a practice
    if ("patients" in q or "patient" in q or "list size" in q or "registered" in q) and ("practice" in q or "prac" in q):
        return "practice_patient_count"

    # patients per GP ratio
    if ("patients per gp" in q or "patient to gp" in q or "gp ratio" in q or "patients per doctor" in q):
        return "patients_per_gp"

    # staff breakdown at a practice
    if ("staff" in q or "workforce" in q or "all staff" in q or "breakdown" in q) and ("practice" in q or "prac" in q):
        return "practice_staff_breakdown"

    return None


def is_follow_up(question: str) -> bool:
    """Detect if the question is a follow-up that refers to a previous entity."""
    q = question.lower().strip()
    # Short questions without specific entity names are likely follow-ups
    follow_up_signals = [
        r"^(what|how|show|give|tell|and|also|now)\b",
        r"\b(the same|this practice|that practice|this icb|that icb|same one|them)\b",
        r"\b(its|their|there)\b",
        r"^(patients per|ratio|trend|gender|age|breakdown|compare|demographic)",
        r"\bfor (this|that|it|them)\b",
    ]
    if any(re.search(p, q) for p in follow_up_signals):
        # Check there's no new entity name (no proper nouns / specific names)
        # If question is short and generic, it's a follow-up
        words = q.split()
        if len(words) <= 10:
            # Check if there's a capitalized proper noun suggesting a new entity
            original_words = question.strip().split()
            has_new_entity = False
            skip_words = {"What", "How", "Show", "Give", "Tell", "The", "And", "Also",
                          "Now", "Is", "Are", "Was", "Were", "In", "At", "For", "By",
                          "GP", "FTE", "ICB", "NHS", "PCN", "DPC"}
            for w in original_words:
                if w[0].isupper() and w not in skip_words and len(w) > 2:
                    has_new_entity = True
                    break
            if not has_new_entity:
                return True
    return False


def resolve_follow_up_context(
    question: str,
    session_id: str,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    If question is a follow-up, enrich it with the entity from the previous turn.
    Returns (enriched_question, previous_entity_context_or_None).
    """
    if not session_id:
        return question, None

    prev_ctx = MEMORY.get_entity_context(session_id)
    if not prev_ctx:
        return question, None

    if not is_follow_up(question):
        return question, None

    # We have previous context and this is a follow-up
    entity_name = prev_ctx.get("entity_name", "")
    entity_type = prev_ctx.get("entity_type", "")  # "practice", "icb", etc.
    table = prev_ctx.get("table", "")

    if entity_name:
        enriched = f"{question} (context: {entity_type} = {entity_name}, table = {table})"
        return enriched, prev_ctx

    return question, None


def extract_entity_hint(question: str) -> str:
    """Extract the entity name (practice/ICB/etc) from the question."""
    q = question.strip()
    # try "in <entity>" pattern
    m = re.search(r"\bin\s+(.+?)(?:\?|$)", q, flags=re.IGNORECASE)
    if m:
        hint = m.group(1).strip()
        # remove trailing common words
        hint = re.sub(r"\b(practice|icb|pcn|region|area)s?\b.*$", "", hint, flags=re.IGNORECASE).strip()
        if len(hint) > 2:
            return hint
    # try "at <entity>"
    m = re.search(r"\bat\s+(.+?)(?:\?|$)", q, flags=re.IGNORECASE)
    if m:
        hint = m.group(1).strip()
        hint = re.sub(r"\b(practice|icb|pcn)s?\b.*$", "", hint, flags=re.IGNORECASE).strip()
        if len(hint) > 2:
            return hint
    # try "for <entity>"
    m = re.search(r"\bfor\s+(.+?)(?:\?|$)", q, flags=re.IGNORECASE)
    if m:
        hint = m.group(1).strip()
        hint = re.sub(r"\b(practice|icb|pcn)s?\b.*$", "", hint, flags=re.IGNORECASE).strip()
        if len(hint) > 2:
            return hint
    # try before "practice"
    m = re.search(r"(.+?)\bpractice\b", q, flags=re.IGNORECASE)
    if m:
        hint = m.group(1).strip()
        hint = re.sub(r"^(how many|number of|no\.? of|what|which|the|is|are|gp|gps|staff|at|in)\s+", "", hint, flags=re.IGNORECASE).strip()
        if len(hint) > 2:
            return hint
    return q


def sql_practice_gp_count_latest(practice_like: str) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    p = sanitise_entity_input(practice_like, "practice_name")
    return f"""
SELECT
  prac_code, prac_name, pcn_name, sub_icb_name, icb_name,
  total_gp_hc, total_gp_fte,
  total_gp_extgl_hc AS gp_excl_trainees_locums_hc,
  total_gp_extgl_fte AS gp_excl_trainees_locums_fte,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND LOWER(TRIM(prac_name)) LIKE LOWER('%{p}%')
ORDER BY total_gp_hc DESC NULLS LAST
LIMIT 10
""".strip()


def sql_practice_to_icb_latest(practice_like: str) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    p = sanitise_entity_input(practice_like, "practice_name")
    return f"""
SELECT
  prac_code, prac_name, pcn_name,
  sub_icb_code, sub_icb_name,
  icb_code, icb_name,
  region_code, region_name,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND LOWER(TRIM(prac_name)) LIKE LOWER('%{p}%')
ORDER BY prac_name
LIMIT 10
""".strip()


def sql_practice_patient_count(practice_like: str) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    p = sanitise_entity_input(practice_like, "practice_name")
    return f"""
SELECT
  prac_code, prac_name, icb_name,
  total_patients, total_male, total_female,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND LOWER(TRIM(prac_name)) LIKE LOWER('%{p}%')
ORDER BY total_patients DESC NULLS LAST
LIMIT 10
""".strip()


def sql_patients_per_gp(practice_like: str) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    p = sanitise_entity_input(practice_like, "practice_name")
    return f"""
SELECT
  prac_code, prac_name, icb_name,
  total_patients, total_gp_fte,
  CASE WHEN total_gp_fte > 0
    THEN ROUND(CAST(total_patients AS DOUBLE) / total_gp_fte, 1)
    ELSE NULL
  END AS patients_per_gp_fte,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND LOWER(TRIM(prac_name)) LIKE LOWER('%{p}%')
ORDER BY patients_per_gp_fte DESC NULLS LAST
LIMIT 10
""".strip()


def sql_practice_staff_breakdown(practice_like: str) -> str:
    latest = get_latest_year_month("practice_detailed")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        raise ValueError("No latest year/month found in practice_detailed table.")
    p = sanitise_entity_input(practice_like, "practice_name")
    return f"""
SELECT
  prac_code, prac_name, icb_name,
  total_gp_hc, total_gp_fte,
  total_nurses_hc, total_nurses_fte,
  total_dpc_hc, total_dpc_fte,
  total_admin_hc, total_admin_fte,
  total_patients,
  '{y}' AS year, '{m}' AS month
FROM practice_detailed
WHERE year = '{y}' AND month = '{m}'
  AND prac_name IS NOT NULL
  AND LOWER(TRIM(prac_name)) LIKE LOWER('%{p}%')
ORDER BY prac_name
LIMIT 10
""".strip()


# =============================================================================
# Suggestions generator
# =============================================================================
def generate_suggestions(question: str, plan: Dict[str, Any], answer: str) -> List[str]:
    """Generate 2-3 contextual follow-up suggestions."""
    q = question.lower()
    suggestions = []
    table = (plan.get("table") or "").lower()

    if table == "individual" or "icb" in q or "national" in q:
        if "gender" not in q:
            suggestions.append("Show the same breakdown by gender")
        if "trend" not in q and "last" not in q:
            suggestions.append("Show the trend over the last 12 months")
        if "icb" not in q:
            suggestions.append("Break this down by ICB")

    if table in ("practice_detailed", "practice_high") or "practice" in q:
        if "patient" not in q:
            suggestions.append("How many patients are registered at this practice?")
        if "staff" not in q and "breakdown" not in q:
            suggestions.append("Show full staff breakdown for this practice")
        if "ratio" not in q and "per gp" not in q:
            suggestions.append("What is the patients-per-GP ratio?")

    if "gp" in q and "nurse" not in q:
        suggestions.append("Show the same for Nurses")
    if "nurse" in q and "gp" not in q:
        suggestions.append("Show the same for GPs")

    return suggestions[:3]


# =============================================================================
# Plan validation (improvement #6)
# =============================================================================
def validate_plan(plan: Dict[str, Any], question: str) -> Dict[str, Any]:
    """Validate and auto-correct the plan before SQL generation."""
    table = str(plan.get("table", "individual")).lower()
    if table not in ALLOWED_TABLES:
        plan["table"] = "individual"
        table = "individual"

    schema_cols = {c for c, _ in get_table_schema(table)}

    # Validate group_by columns exist
    group_by = plan.get("group_by", []) or []
    valid_group_by = [g for g in group_by if g.lower() in schema_cols]
    if group_by and not valid_group_by:
        plan["_group_by_warning"] = f"Requested group_by {group_by} not found in {table} schema"
    plan["group_by"] = valid_group_by

    # Auto-switch table for practice questions if using individual
    q = question.lower()
    if table == "individual" and any(kw in q for kw in ["practice", "prac_name", "prac_code"]):
        plan["table"] = "practice_detailed"
        plan["_table_switched"] = f"Switched from individual to practice_detailed (practice question)"

    # Auto-switch for practice ranking
    if table == "individual" and ("top" in q or "rank" in q or "highest" in q or "lowest" in q) and "practice" in q:
        plan["table"] = "practice_high"
        plan["_table_switched"] = f"Switched to practice_high for practice ranking"

    return plan


# =============================================================================
# LangGraph state
# =============================================================================
class AgentState(TypedDict, total=False):
    session_id: str
    question: str
    original_question: str  # before follow-up enrichment

    conversation_history: str
    domain_notes: str
    follow_up_context: Optional[Dict[str, Any]]  # previous entity context if follow-up

    latest_year: Optional[str]
    latest_month: Optional[str]
    time_range: Optional[Dict[str, Any]]

    staff_groups: List[str]
    staff_roles: List[str]
    detailed_staff_roles: List[str]
    practice_high_measures: List[str]
    practice_high_staff_groups: List[str]
    practice_high_detailed_roles: List[str]

    resolved_entities: Dict[str, Any]
    plan: Dict[str, Any]
    sql: str

    df_preview_md: str
    answer: str
    suggestions: List[str]

    attempts: int
    last_error: Optional[str]
    needs_retry: bool

    _rows: int
    _empty: bool
    _hard_intent: Optional[str]


# =============================================================================
# Prompts (enhanced)
# =============================================================================
PLANNER_SYSTEM = """You are a GP Workforce analytics planner. You decide how to answer questions using 3 Athena tables.

CRITICAL — FOLLOW-UP HANDLING:
If the user asks a follow-up (e.g. "now by gender", "the same for nurses", "What is the ratio?"),
you MUST use the conversation history AND the follow-up context to understand what entity they're
referring to. If the question contains "(context: practice = Keele)" or similar, apply that filter.
The follow-up context is injected automatically — USE IT to maintain the correct entity filter.
NEVER ignore the context and return national/all-practice results when the user was asking about
a specific entity in the previous turn.

TABLE SELECTION RULES:
- individual: 22 columns. Has staff_group, staff_role, detailed_staff_role, gender, age_band, country_qualification_group, fte.
  Geography: comm_region_name, icb_name, sub_icb_name.
  USE FOR: national/regional/ICB totals, demographics, SUM(fte), COUNT(DISTINCT unique_identifier).
- practice_high: 8 columns. Tidy format: prac_code, prac_name, staff_group, detailed_staff_role, measure, value, year, month.
  measure can be 'FTE' or 'Headcount'. Value is in the 'value' column.
  USE FOR: practice-level rankings, comparing practices.
- practice_detailed: 830+ columns. Wide format with pre-computed totals per practice.
  Has geography hierarchy (practice->PCN->Sub-ICB->ICB->Region), patient counts, all staff breakdowns.
  USE FOR: practice lookups, patient counts, patients-per-GP, detailed GP sub-type breakdowns.

IMPORTANT:
- For "total GP FTE nationally" -> individual, SUM(fte) WHERE staff_group = 'GP'
- For "top practices by GP FTE" -> practice_high WHERE staff_group = 'GP' AND measure = 'FTE'
- For "Keele practice GP count" -> practice_detailed, search by prac_name
- For "GP trend over 12 months" -> individual, GROUP BY year, month

Return STRICT JSON ONLY:
{
  "in_scope": true|false,
  "table": "individual|practice_high|practice_detailed",
  "intent": "total|percent_split|ratio|trend|topn|lookup|comparison|demographics|unknown",
  "group_by": ["..."],
  "filters_needed": ["..."],
  "entities_to_resolve": ["icb_name|sub_icb_name|pcn_name|prac_name"],
  "notes": "short explanation"
}
"""

SQL_SYSTEM = """You are an expert AWS Athena (Trino/Presto) SQL writer for GP Workforce data.

HARD RULES:
- Return ONLY SQL (no markdown, no explanation, no backticks).
- Read-only only (SELECT or WITH ... SELECT).
- Allowed base tables: practice_high, individual, practice_detailed.
- Use the exact latest year/month provided (do NOT guess dates).
- staff_group / staff_role / detailed_staff_role MUST use values from the provided vocabulary lists. Never invent values.
- String comparisons should use LOWER(TRIM(...)) for robustness.

FOLLOW-UP CONTEXT:
- If the question contains "(context: practice = <name>)" or similar, you MUST include
  a WHERE filter for that entity (e.g. LOWER(TRIM(prac_name)) LIKE LOWER('%name%')).
- NEVER produce a query without the entity filter when context is provided.

TABLE-SPECIFIC RULES:
- individual: FTE = SUM(fte). Headcount = COUNT(DISTINCT unique_identifier). Always filter by year + month.
- practice_high: value column is the numeric measure. Filter by measure = 'FTE' or measure = 'Headcount'.
  Use CAST(value AS DOUBLE) for numeric operations. Always filter by year + month.
- practice_detailed: pre-computed totals already exist as columns (total_gp_hc, total_gp_fte etc).
  Do NOT SUM these — they are already totals per practice row. Always filter by year + month.

COLUMN LABELS are provided to help you pick the right column name. Use the column NAME (not the label) in SQL.

TIME RANGES: If a time range is provided, use it in WHERE clause:
  (CAST(year AS INTEGER) * 100 + CAST(month AS INTEGER)) BETWEEN start AND end

ROUNDING: Round FTE values to 1 decimal place. Round percentages to 1 decimal place.

Return ONLY the SQL query.
"""

FIXER_SYSTEM = """You fix SQL queries for GP Workforce Athena tables.

RULES:
- Return ONLY corrected SQL (no markdown, no backticks).
- Keep it read-only (SELECT/WITH).
- Allowed tables: practice_high, individual, practice_detailed.

COMMON FIXES:
- 0 rows from name mismatch: use LOWER(TRIM(name)) LIKE LOWER('%partial%')
- Invalid staff_group: use values from the vocabulary provided
- Wrong table: if practice lookup returns 0 rows from individual, use practice_detailed
- practice_high: remember value is string, use CAST(value AS DOUBLE) for math
- Column not found: check the schema provided and use correct column name

Return corrected SQL only.
"""

SUMMARY_SYSTEM = """You are a helpful NHS GP Workforce analyst providing clear, well-formatted answers.

FORMATTING RULES:
- Lead with the key finding in bold: e.g. "**Total GP FTE is 27,453.2** across England as of August 2024."
- Use bullet points for multiple data points.
- If showing a comparison, state the difference clearly.
- If multiple matching practices/entities, mention all matches and ask user to clarify.
- For trends, describe the direction (increasing/decreasing/stable).
- Round numbers appropriately (FTE to 1 decimal, headcount to whole numbers, percentages to 1 decimal).
- Do NOT invent numbers — only use what's in the preview data.
- Keep answers concise: 2-6 lines for simple queries, up to 10 for complex ones.
- End with a brief note on data source/date if relevant.
"""


# =============================================================================
# Graph nodes
# =============================================================================
def node_init(state: AgentState) -> AgentState:
    state["attempts"] = int(state.get("attempts", 0))
    state["needs_retry"] = False
    state["last_error"] = None
    state["suggestions"] = []

    # Store original question before enrichment
    state["original_question"] = state["question"]

    sid = state.get("session_id", "")
    logger.info("node_init | session=%s | q='%s'", sid, state["question"][:120])

    # Load conversation history
    state["conversation_history"] = MEMORY.format_for_prompt(sid)

    # Follow-up resolution: detect if this is a follow-up and enrich with previous entity
    enriched_q, follow_ctx = resolve_follow_up_context(state["question"], sid)
    state["follow_up_context"] = follow_ctx
    if follow_ctx:
        logger.info("node_init | follow-up detected, entity=%s", follow_ctx.get("entity_name"))
        state["question"] = enriched_q

    state["domain_notes"] = retrieve_domain_notes(state["question"])
    state["_hard_intent"] = detect_hard_intent(state["original_question"])
    if state["_hard_intent"]:
        logger.info("node_init | hard_intent=%s", state["_hard_intent"])
    return state


def node_fetch_latest_and_vocab(state: AgentState) -> AgentState:
    latest = get_latest_year_month("individual")
    state["latest_year"] = latest.get("year")
    state["latest_month"] = latest.get("month")

    y, m = state["latest_year"], state["latest_month"]
    where_latest = f"year = '{y}' AND month = '{m}'" if y and m else None

    # Resolve time range from question
    if y and m:
        state["time_range"] = resolve_time_range(state["question"], y, m)

    # Individual table vocab
    state["staff_groups"] = list_distinct_values("individual", "staff_group", where_sql=where_latest, limit=300)
    try:
        state["staff_roles"] = list_distinct_values("individual", "staff_role", where_sql=where_latest, limit=400)
    except Exception:
        state["staff_roles"] = []
    try:
        state["detailed_staff_roles"] = list_distinct_values("individual", "detailed_staff_role", where_sql=where_latest, limit=600)
    except Exception:
        state["detailed_staff_roles"] = []

    # Practice_high vocab (improvement #4)
    latest_ph = get_latest_year_month("practice_high")
    yp, mp = latest_ph.get("year"), latest_ph.get("month")
    where_ph = f"year = '{yp}' AND month = '{mp}'" if yp and mp else None
    try:
        state["practice_high_measures"] = list_distinct_values("practice_high", "measure", where_sql=where_ph, limit=50)
    except Exception:
        state["practice_high_measures"] = ["FTE", "Headcount"]
    try:
        state["practice_high_staff_groups"] = list_distinct_values("practice_high", "staff_group", where_sql=where_ph, limit=50)
    except Exception:
        state["practice_high_staff_groups"] = []
    try:
        state["practice_high_detailed_roles"] = list_distinct_values("practice_high", "detailed_staff_role", where_sql=where_ph, limit=200)
    except Exception:
        state["practice_high_detailed_roles"] = []

    return state


def node_hard_override_sql(state: AgentState) -> AgentState:
    hi = state.get("_hard_intent")
    if not hi:
        return state

    hint = extract_entity_hint(state["original_question"])
    logger.info("node_hard_override | intent=%s hint='%s'", hi, hint[:60])

    # If extract_entity_hint returned the whole question (no entity found),
    # check follow-up context for the entity
    orig_q = state.get("original_question", "")
    if hint == orig_q or len(hint) > 50:
        follow_ctx = state.get("follow_up_context")
        if follow_ctx and follow_ctx.get("entity_name"):
            hint = follow_ctx["entity_name"]
        else:
            # No entity at all — for practice-specific intents, skip hard override
            # and let the LLM planner handle it (it might be a national question)
            if hi in ("practice_gp_count", "practice_gp_count_soft", "practice_patient_count",
                       "practice_staff_breakdown", "practice_to_icb_lookup"):
                # Check if there's really no entity — if the question is generic like
                # "patients per GP ratio" without a practice name, treat as national
                if hi == "patients_per_gp":
                    pass  # let it fall through to national
                else:
                    return state  # skip hard override, let LLM handle

    if hi in ("practice_gp_count", "practice_gp_count_soft"):
        state["plan"] = {"in_scope": True, "table": "practice_detailed", "intent": "lookup",
                         "notes": f"Hard override: GP count for '{hint}'"}
        state["sql"] = sql_practice_gp_count_latest(hint)
        return state

    if hi == "practice_to_icb_lookup":
        state["plan"] = {"in_scope": True, "table": "practice_detailed", "intent": "lookup",
                         "notes": f"Hard override: ICB lookup for '{hint}'"}
        state["sql"] = sql_practice_to_icb_latest(hint)
        return state

    if hi == "practice_patient_count":
        state["plan"] = {"in_scope": True, "table": "practice_detailed", "intent": "lookup",
                         "notes": f"Hard override: patient count for '{hint}'"}
        state["sql"] = sql_practice_patient_count(hint)
        return state

    if hi == "patients_per_gp":
        state["plan"] = {"in_scope": True, "table": "practice_detailed", "intent": "ratio",
                         "notes": f"Hard override: patients per GP for '{hint}'"}
        state["sql"] = sql_patients_per_gp(hint)
        return state

    if hi == "practice_staff_breakdown":
        state["plan"] = {"in_scope": True, "table": "practice_detailed", "intent": "lookup",
                         "notes": f"Hard override: staff breakdown for '{hint}'"}
        state["sql"] = sql_practice_staff_breakdown(hint)
        return state

    return state


def node_plan(state: AgentState) -> AgentState:
    if state.get("sql"):
        logger.debug("node_plan | skipped (SQL already set by hard_override)")
        return state

    logger.info("node_plan | invoking LLM planner")
    t0 = time.time()
    llm = llm_client()
    prompt = f"""
CONVERSATION HISTORY:
{state.get("conversation_history", "") or "(first question)"}

DOMAIN NOTES:
{state.get("domain_notes", "")}

LATEST AVAILABLE:
year={state.get("latest_year")} month={state.get("latest_month")}

TIME RANGE (if detected):
{json.dumps(state.get("time_range"), ensure_ascii=False) if state.get("time_range") else "None (use latest month)"}

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
        logger.warning("node_plan | LLM returned invalid JSON, using fallback plan")
        plan = {
            "in_scope": True, "table": "individual", "intent": "unknown",
            "group_by": [], "filters_needed": [], "entities_to_resolve": [],
            "notes": "fallback plan (invalid JSON from model)",
        }

    plan = validate_plan(plan, state["question"])
    plan["in_scope"] = bool(plan.get("in_scope", True))
    state["plan"] = plan
    logger.info("node_plan | table=%s intent=%s in_scope=%s (%.2fs)",
                plan.get("table"), plan.get("intent"), plan.get("in_scope"), time.time() - t0)
    return state


def node_resolve_entities(state: AgentState) -> AgentState:
    plan = state.get("plan", {}) or {}
    q = state["question"]
    resolved: Dict[str, Any] = {}

    y_i, m_i = state.get("latest_year"), state.get("latest_month")
    latest_pd = get_latest_year_month("practice_detailed")
    y_pd, m_pd = latest_pd.get("year"), latest_pd.get("month")

    q_lower = q.lower()

    # Always try practice candidates if question mentions practice-like things
    if any(kw in q_lower for kw in ["practice", "pcn", "prac", "surgery", "medical centre", "health centre"]):
        hint = extract_entity_hint(q)
        try:
            resolved["prac_name_candidates"] = resolve_entity_fuzzy(
                "practice_detailed", "prac_name", hint, y_pd, m_pd)
        except Exception:
            resolved["prac_name_candidates"] = []

    # ICB resolution (fuzzy)
    if any(kw in q_lower for kw in ["icb", "integrated care"]) or "icb_name" in (plan.get("entities_to_resolve") or []):
        hint = extract_entity_hint(q)
        try:
            resolved["icb_name_candidates"] = resolve_entity_fuzzy(
                "individual", "icb_name", hint, y_i, m_i)
        except Exception:
            resolved["icb_name_candidates"] = []

    # Sub-ICB resolution
    if "sub_icb_name" in (plan.get("entities_to_resolve") or []) or "sub-icb" in q_lower or "sub icb" in q_lower:
        hint = extract_entity_hint(q)
        try:
            resolved["sub_icb_name_candidates"] = resolve_entity_fuzzy(
                "individual", "sub_icb_name", hint, y_i, m_i)
        except Exception:
            resolved["sub_icb_name_candidates"] = []

    # Region resolution
    if "region" in q_lower:
        try:
            resolved["region_candidates"] = list_distinct_values(
                "individual", "comm_region_name",
                where_sql=f"year = '{y_i}' AND month = '{m_i}'" if y_i and m_i else None,
                limit=20)
        except Exception:
            resolved["region_candidates"] = []

    state["resolved_entities"] = resolved
    return state


def node_generate_sql(state: AgentState) -> AgentState:
    if state.get("sql"):
        return state

    plan = state.get("plan", {})
    if not plan.get("in_scope", True):
        state["sql"] = ""
        return state

    table = plan.get("table", "individual")
    schema = get_table_schema(table)

    # For practice_detailed with 830+ cols, only show key columns
    if table == "practice_detailed" and len(schema) > 100:
        key_prefixes = [
            "prac_", "pcn_", "sub_icb_", "icb_", "region_",
            "total_gp_", "total_nurses_", "total_dpc_", "total_admin_",
            "total_patients", "total_male", "total_female",
            "year", "month",
        ]
        schema_filtered = [(c, t) for c, t in schema if any(c.startswith(p) or c == p for p in key_prefixes)]
        schema_text = "\n".join([f"- {c} ({t})" for c, t in schema_filtered[:120]])
        schema_text += f"\n... ({len(schema)} total columns, showing key ones only)"
    else:
        schema_text = "\n".join([f"- {c} ({t})" for c, t in schema[:120]])

    # Column labels
    col_labels = get_column_labels(table)

    latest = get_latest_year_month(table)
    y, m = latest.get("year"), latest.get("month")

    time_range = state.get("time_range")
    time_range_text = json.dumps(time_range, ensure_ascii=False) if time_range else "None — use latest month only"

    context = f"""
CONVERSATION HISTORY:
{state.get("conversation_history", "") or "(first question)"}

DOMAIN NOTES:
{state.get("domain_notes", "")}

LATEST (for {table}):
year={y} month={m}

TIME RANGE:
{time_range_text}

TABLE: {table}

SCHEMA:
{schema_text}

COLUMN LABELS (column_name = human meaning):
{col_labels}

VALID VALUES (individual table):
- staff_group: {state.get("staff_groups", [])[:50]}
- staff_role: {state.get("staff_roles", [])[:50]}
- detailed_staff_role: {state.get("detailed_staff_roles", [])[:50]}

VALID VALUES (practice_high table):
- measure: {state.get("practice_high_measures", [])}
- staff_group: {state.get("practice_high_staff_groups", [])[:30]}
- detailed_staff_role: {state.get("practice_high_detailed_roles", [])[:50]}

ENTITY CANDIDATES:
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
        state["_rows"] = 0
        state["_empty"] = True
        return state

    sql = (state.get("sql") or "").strip()
    if not sql:
        raise ValueError("No SQL produced for an in-scope question.")

    logger.info("node_run_sql | executing query (%d chars)", len(sql))
    try:
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        sql_safe = add_limit(sql_safe, MAX_ROWS_RETURN)

        df = run_athena_df(sql_safe)

        state["sql"] = sql_safe
        state["df_preview_md"] = safe_markdown(df, head=30)
        state["_rows"] = int(len(df))
        state["_empty"] = bool(df.empty)
        state["last_error"] = None
        logger.info("node_run_sql | success, rows=%d", state["_rows"])
        return state

    except Exception as e:
        logger.warning("node_run_sql | error: %s", str(e)[:200])
        state["last_error"] = str(e)
        state["df_preview_md"] = ""
        state["_rows"] = 0
        state["_empty"] = True
        return state


def node_validate_or_fix(state: AgentState) -> AgentState:
    plan = state.get("plan", {}) or {}
    if not plan.get("in_scope", True):
        state["needs_retry"] = False
        return state

    attempts = int(state.get("attempts", 0))
    last_error = state.get("last_error")
    empty = bool(state.get("_empty", False))

    if (not last_error) and (not empty):
        state["needs_retry"] = False
        return state

    if attempts >= MAX_AGENT_LOOPS:
        state["needs_retry"] = False
        return state

    # Smart table switch for practice questions
    qlow = state["question"].lower()
    if empty and any(kw in qlow for kw in ["practice", "prac", "surgery", "medical centre"]) and plan.get("table") != "practice_detailed":
        hint = extract_entity_hint(state["question"])
        state["plan"]["table"] = "practice_detailed"
        state["plan"]["intent"] = "lookup"
        state["sql"] = sql_practice_gp_count_latest(hint)
        state["attempts"] = attempts + 1
        state["needs_retry"] = True
        state["last_error"] = None
        return state

    # LLM fixer
    llm = llm_client()

    table = plan.get("table", "individual")
    schema = get_table_schema(table)
    if table == "practice_detailed" and len(schema) > 100:
        key_prefixes = ["prac_", "pcn_", "sub_icb_", "icb_", "region_",
                        "total_gp_", "total_nurses_", "total_dpc_", "total_admin_",
                        "total_patients", "year", "month"]
        schema_filtered = [(c, t) for c, t in schema if any(c.startswith(p) or c == p for p in key_prefixes)]
        schema_text = "\n".join([f"- {c} ({t})" for c, t in schema_filtered[:120]])
    else:
        schema_text = "\n".join([f"- {c} ({t})" for c, t in schema[:200]])

    latest = get_latest_year_month(table)
    y, m = latest.get("year"), latest.get("month")

    fix_context = f"""
DOMAIN NOTES:
{state.get("domain_notes", "")}

LATEST (for {table}):
year={y} month={m}

TABLE: {table}

SCHEMA:
{schema_text}

COLUMN LABELS:
{get_column_labels(table)}

VALID VALUES (individual):
- staff_group: {state.get("staff_groups", [])}
- staff_role: {state.get("staff_roles", [])}

VALID VALUES (practice_high):
- measure: {state.get("practice_high_measures", [])}
- staff_group: {state.get("practice_high_staff_groups", [])}

ENTITY CANDIDATES:
{json.dumps(state.get("resolved_entities", {}), ensure_ascii=False)}

QUESTION:
{state["question"]}

PREVIOUS SQL:
{state.get("sql", "")}

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
    plan = state.get("plan", {}) or {}
    llm = llm_client()

    if not plan.get("in_scope", True):
        msg = f"""
QUESTION:
{state["question"]}

This question is OUT OF SCOPE for the GP Workforce dataset.
Explain clearly what this dataset covers (FTE/headcount for GP practices in England,
broken down by staff group, role, demographics, geography) and suggest what
workforce questions ARE supported.
""".strip()
        ans = llm.invoke([SystemMessage(content=SUMMARY_SYSTEM), HumanMessage(content=msg)]).content.strip()
        state["answer"] = ans
        state["df_preview_md"] = ""
        state["sql"] = ""
        state["suggestions"] = [
            "Show total GP FTE nationally in the latest month",
            "Top 10 ICBs by GP FTE",
            "Staff breakdown by gender",
        ]
        return state

    display_q = state.get("original_question", state["question"])
    follow_ctx = state.get("follow_up_context")
    context_note = ""
    if follow_ctx and follow_ctx.get("entity_name"):
        context_note = f"\nCONTEXT: This was a follow-up about {follow_ctx.get('entity_type','entity')} '{follow_ctx.get('entity_name','')}'"

    msg = f"""
QUESTION:
{display_q}{context_note}

SQL USED:
{state.get("sql", "")}

RESULT PREVIEW:
{state.get("df_preview_md", "")}

ROWS RETURNED: {state.get("_rows", 0)}

TABLE USED: {plan.get("table", "unknown")}

NOTES:
- Format key numbers in bold.
- If multiple rows match a practice/entity name search, list them and ask the user to clarify.
- If the result is empty (0 rows), suggest why and how to rephrase.
- If this is a follow-up, refer to the specific entity by name in your answer.
""".strip()

    ans = llm.invoke([SystemMessage(content=SUMMARY_SYSTEM), HumanMessage(content=msg)]).content.strip()

    if bool(state.get("_empty", False)):
        ans += "\n\n*Note: This query returned 0 rows. The name or filter may not match exactly. Try a different spelling or a broader search.*"

    state["answer"] = ans
    state["suggestions"] = generate_suggestions(state.get("original_question", state["question"]), plan, ans)

    # Extract entity context from this turn to enable follow-ups
    entity_context = _extract_entity_context_from_state(state)

    # Save to conversation memory (use original question for display, not enriched)
    MEMORY.add_turn(
        state.get("session_id", ""),
        state.get("original_question", state["question"]),
        ans,
        state.get("sql", ""),
        entity_context=entity_context,
    )

    return state


def _extract_entity_context_from_state(state: AgentState) -> Dict[str, Any]:
    """Extract the entity context from the current answer to enable follow-up questions."""
    plan = state.get("plan", {}) or {}
    table = plan.get("table", "")
    sql = state.get("sql", "")
    ctx: Dict[str, Any] = {"table": table}

    # NOTE: SQL patterns use LOWER(TRIM(col_name)) which produces col_name)) — two closing parens.
    # Use \)* to handle zero, one, or two closing parens before LIKE/=.

    # Try to extract practice name from SQL
    m = re.search(r"prac_name\)*\s*(?:LIKE|=)\s*(?:LOWER\s*\()?'%([^%]+)%'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "practice"
        ctx["entity_col"] = "prac_name"
        return ctx

    # Try to extract ICB name from SQL
    m = re.search(r"icb_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "icb"
        ctx["entity_col"] = "icb_name"
        return ctx

    # Try to extract sub-ICB name from SQL
    m = re.search(r"sub_icb_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "sub_icb"
        ctx["entity_col"] = "sub_icb_name"
        return ctx

    # Try to extract region name
    m = re.search(r"(?:comm_)?region_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "region"
        ctx["entity_col"] = "comm_region_name"
        return ctx

    # If follow_up_context was used, carry it forward
    follow_ctx = state.get("follow_up_context")
    if follow_ctx:
        return follow_ctx

    return ctx


# =============================================================================
# Build Graph
# =============================================================================
def build_graph():
    g = StateGraph(AgentState)

    g.add_node("init", node_init)
    g.add_node("latest_vocab", node_fetch_latest_and_vocab)
    g.add_node("hard_override", node_hard_override_sql)
    g.add_node("plan", node_plan)
    g.add_node("resolve_entities", node_resolve_entities)
    g.add_node("generate_sql", node_generate_sql)
    g.add_node("run_sql", node_run_sql)
    g.add_node("validate_or_fix", node_validate_or_fix)
    g.add_node("summarize", node_summarize)

    g.set_entry_point("init")

    g.add_edge("init", "latest_vocab")
    g.add_edge("latest_vocab", "hard_override")
    g.add_edge("hard_override", "plan")
    g.add_edge("plan", "resolve_entities")
    g.add_edge("resolve_entities", "generate_sql")
    g.add_edge("generate_sql", "run_sql")
    g.add_edge("run_sql", "validate_or_fix")

    def route_after_validate(state: AgentState) -> str:
        return "run_sql" if state.get("needs_retry", False) else "summarize"

    g.add_conditional_edges("validate_or_fix", route_after_validate, {
        "run_sql": "run_sql",
        "summarize": "summarize",
    })

    g.add_edge("summarize", END)
    return g.compile()


AGENT = build_graph()


# =============================================================================
# Helpers for error sanitisation
# =============================================================================
_SAFE_ERROR_MESSAGES = {
    "No latest year/month": "Could not determine the latest data period. The database may be temporarily unavailable.",
    "Blocked:": "Your query was blocked by our safety checks. Please rephrase your question.",
    "Unknown table": "The requested data table is not available.",
    "Empty": "No matching entity found. Please check the spelling and try again.",
    "Invalid characters": "Your search contains invalid characters. Please use only letters, numbers, spaces, and basic punctuation.",
    "too long": "Your input is too long. Please shorten your question or entity name.",
}


def _sanitise_error(e: Exception) -> str:
    """Return a user-safe error message — never expose internal details."""
    msg = str(e)
    for pattern, safe_msg in _SAFE_ERROR_MESSAGES.items():
        if pattern.lower() in msg.lower():
            return safe_msg
    return "Something went wrong processing your request. Please try rephrasing your question."


# =============================================================================
# FastAPI routes
# =============================================================================
@app.get("/health")
def health():
    """Public health check — minimal info only."""
    return {
        "ok": True,
        "version": "5.1-agent",
    }


@app.get("/health/detail")
def health_detail():
    """Detailed health check — for internal / admin use."""
    return {
        "ok": True,
        "athena_db": ATHENA_DATABASE,
        "allowed_tables": sorted(list(ALLOWED_TABLES)),
        "domain_notes_loaded": bool(DOMAIN_NOTES_TEXT.strip()),
        "column_dict_loaded": bool(COLUMN_DICT),
        "schema_override_loaded": {k: len(v) for k, v in _SCHEMA_OVERRIDE.items()},
        "memory_sessions": len(MEMORY._store),
        "query_cache_size": len(_QUERY_CACHE),
        "version": "5.1-agent",
    }


def _run_agent_sync(req: ChatRequest) -> ChatResponse:
    """Synchronous agent invocation — wrapped by async endpoint."""
    request_id = str(uuid.uuid4())[:8]
    logger.info("chat | rid=%s session=%s q='%s'", request_id, req.session_id, req.question[:120])
    t0 = time.time()

    state: AgentState = {
        "session_id": req.session_id,
        "question": req.question,
        "attempts": 0,
    }

    out = AGENT.invoke(state)

    elapsed = time.time() - t0
    logger.info("chat | rid=%s completed in %.2fs rows=%d",
                request_id, elapsed, int(out.get("_rows", 0)))

    meta = {
        "plan": out.get("plan", {}),
        "resolved_entities": out.get("resolved_entities", {}),
        "attempts": int(out.get("attempts", 0)),
        "last_error": out.get("last_error"),
        "latest_year": out.get("latest_year"),
        "latest_month": out.get("latest_month"),
        "time_range": out.get("time_range"),
        "rows_returned": int(out.get("_rows", 0)),
        "hard_intent": out.get("_hard_intent"),
        "follow_up_context": out.get("follow_up_context"),
        "request_id": request_id,
        "elapsed_seconds": round(elapsed, 2),
    }

    return ChatResponse(
        answer=out.get("answer", ""),
        sql=out.get("sql", ""),
        preview_markdown=out.get("df_preview_md", ""),
        meta=meta,
        suggestions=out.get("suggestions", []),
    )


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    try:
        # Run the synchronous agent in a thread pool with timeout
        result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _run_agent_sync, req),
            timeout=REQUEST_TIMEOUT,
        )
        return result

    except asyncio.TimeoutError:
        logger.error("chat | TIMEOUT after %ds for session=%s", REQUEST_TIMEOUT, req.session_id)
        return JSONResponse(
            status_code=504,
            content={"error": "Request timed out. The query took too long. Please try a simpler question."},
        )

    except ValueError as e:
        # Known validation errors — sanitise but can be more specific
        logger.warning("chat | ValueError: %s", str(e)[:200])
        return JSONResponse(
            status_code=400,
            content={"error": _sanitise_error(e)},
        )

    except Exception as e:
        # Unexpected errors — log full details, return generic message
        logger.exception("chat | unexpected error for session=%s", req.session_id)
        return JSONResponse(
            status_code=500,
            content={"error": _sanitise_error(e)},
        )


@app.get("/schema/{table_name}")
def schema_endpoint(table_name: str):
    table_name = table_name.lower()
    if table_name not in ALLOWED_TABLES:
        return JSONResponse(status_code=400, content={"error": "Unknown table."})
    try:
        schema_list = get_table_schema(table_name)
        latest = get_latest_year_month(table_name)
        return {
            "table": table_name,
            "columns": [{"name": c, "type": t} for c, t in schema_list],
            "latest": latest,
            "override_cols_count": len(_SCHEMA_OVERRIDE.get(table_name, [])),
            "column_labels": get_column_labels(table_name),
        }
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Failed to load schema."})


@app.get("/suggestions")
def suggestions():
    """Return starter suggestions for the UI."""
    return {
        "suggestions": [
            "Total GP FTE nationally in the latest month",
            "Top 10 ICBs by GP FTE",
            "GP headcount trend over the last 12 months",
            "Gender breakdown of GPs by region",
            "Staff breakdown at Keele Practice",
            "Patients per GP ratio across all practices",
            "Nurse FTE by ICB in the latest month",
            "How many pharmacists are there nationally?",
        ]
    }


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting GP Workforce Chatbot v5.1 on port 8000")
    uvicorn.run("gp_workforce_chatbot_backend_agent_v5:app", host="0.0.0.0", port=8000, reload=True)
