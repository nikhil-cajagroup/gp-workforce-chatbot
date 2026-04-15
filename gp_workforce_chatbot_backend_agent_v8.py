"""
InsightsQI Assistant Backend — Agent v8.0
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
 19. [v5.2] Knowledge-only answer path for methodology/scope/definition questions
 20. [v5.2] Expanded domain notes with full NHS Digital publication metadata
 21. [v5.3] Topic-change detection — prevents context bleeding between unrelated questions
 22. [v5.3] Improved follow-up detection — self-contained questions treated as fresh queries
 23. [v5.3] Multi-period comparison SQL fix (CASE WHEN conditional aggregation)
 24. [v5.3] Conversation history windowing (last 3 turns to reduce topic contamination)
 25. [v5.3] Business logic guidance: retirement, FTE ratio, sustainability, locums, trends
 26. [v5.3] Smarter follow-up: "Show me X data" with specific subjects → fresh query
 27. [v5.3] Smarter follow-up: "Compare A vs B" with named entities → fresh query
 28. [v5.3] Correction/refinement detection: "i dont want X, i want Y" → follow-up
 29. [v5.3] Expanded out-of-scope handling with contextual alternative suggestions
 30. [v5.3] Time range: "N years ago" and "this year vs last year" parsing
 31. [v5.3] FTE per GP ratio SQL guidance (practice-level, handles NA/zero)
 32. [v5.4] Dynamic few-shot examples — vector-retrieved proven SQL patterns
 33. [v5.4] Semantic query cache — embedding-based similarity matching
 34. [v5.4] Answer quality grading with confidence scoring
 35. [v5.5] Long-term memory store — auto-learns from high-confidence queries
 36. [v5.6] Adaptive query routing — LLM classifier with fast-path regex, data_simple skips planner
 37. [v5.7] Structured output with Pydantic — planner & classifier return typed models (no JSON parse errors)
 38. [v5.8] Multi-turn clarification — detects ambiguous queries, asks clarifying questions, merges answers
 39. [v5.9] Fix PCN grouping (explicit planner example), fix correction follow-ups (national-scope enrichment), UI streaming progress
 40. [v6.0] Thread safety (locks on shared state), SQL injection hardening, SSE non-blocking streaming,
     Bedrock client reuse, error sanitisation, LangGraph checkpointing, code quality cleanup
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
import sqlite3
from dataclasses import replace
from collections import OrderedDict
from typing import Callable, Dict, Any, List, Tuple, Optional, TypedDict, Literal, MutableMapping, cast

import threading
import atexit
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import boto3
import numpy as np
import pandas as pd
import awswrangler as wr
from fastapi import FastAPI, Request
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_aws import ChatBedrockConverse

from langgraph.graph import StateGraph, END
from v8_workforce_sql_helpers import (
    build_practice_lookup_filter,
    build_sql_pcn_gp_count,
    build_sql_patients_per_gp,
    build_sql_practice_gp_count_latest,
    build_sql_practice_patient_count,
    build_sql_practice_staff_breakdown,
    build_sql_practice_to_icb_latest,
)
from v8_workforce_override_helpers import (
    apply_workforce_benchmark_and_group_followups as external_apply_workforce_benchmark_and_group_followups,
    apply_workforce_clinical_staff_breakdown as external_apply_workforce_clinical_staff_breakdown,
    apply_workforce_demographic_overrides as external_apply_workforce_demographic_overrides,
    apply_workforce_followup_lookup_overrides as external_apply_workforce_followup_lookup_overrides,
    apply_workforce_geo_context_followups as external_apply_workforce_geo_context_followups,
    apply_workforce_geo_scoped_simple_queries as external_apply_workforce_geo_scoped_simple_queries,
    apply_workforce_grouped_comparison_overrides as external_apply_workforce_grouped_comparison_overrides,
    apply_workforce_large_practice_threshold as external_apply_workforce_large_practice_threshold,
    apply_workforce_misc_lookup_overrides as external_apply_workforce_misc_lookup_overrides,
    apply_workforce_partner_salaried_trend as external_apply_workforce_partner_salaried_trend,
    apply_workforce_verbose_national_total as external_apply_workforce_verbose_national_total,
    geo_filter_from_follow_context as external_geo_filter_from_follow_context,
)
from v8_workforce_intent_helpers import (
    apply_workforce_patients_per_gp_intent as external_apply_workforce_patients_per_gp_intent,
    apply_workforce_practice_gp_count_override as external_apply_workforce_practice_gp_count_override,
    apply_workforce_practice_lookup_intents as external_apply_workforce_practice_lookup_intents,
    apply_workforce_ratio_overrides as external_apply_workforce_ratio_overrides,
    build_national_patients_per_gp_yoy_override as external_build_national_patients_per_gp_yoy_override,
    resolve_workforce_override_hint as external_resolve_workforce_override_hint,
)
from v8_dataset_service_helpers import (
    appointments_semantic_issue_checker as external_appointments_semantic_issue_checker,
    dataset_schema_text as external_dataset_schema_text,
    dataset_valid_values_block as external_dataset_valid_values_block,
    default_semantic_issue_checker as external_default_semantic_issue_checker,
    load_simple_vocab_from_config as external_load_simple_vocab_from_config,
    load_workforce_latest_and_vocab as external_load_workforce_latest_and_vocab,
)
from v8_entity_resolution_helpers import (
    resolve_entities_via_config as external_resolve_entities_via_config,
)
from v8_followup_sql_helpers import (
    build_geo_compare_followup_sql as external_build_geo_compare_followup_sql,
    build_group_extreme_followup_sql as external_build_group_extreme_followup_sql,
    build_grouped_followup_sql as external_build_grouped_followup_sql,
    build_top_practices_followup_sql as external_build_top_practices_followup_sql,
    build_total_change_followup_sql as external_build_total_change_followup_sql,
    followup_group_dimension as external_followup_group_dimension,
    infer_staff_filter_from_state as external_infer_staff_filter_from_state,
)
from v8_validation_helpers import (
    detect_sql_semantic_issues as external_detect_sql_semantic_issues,
)
from v8_appointments_sql_helpers import (
    build_appointments_geo_filter,
    build_appointments_geo_hint_from_context,
    build_sql_appointments_dna_rate,
    build_sql_appointments_hcp_breakdown,
    build_sql_appointments_mode_breakdown,
    build_sql_appointments_top_practices,
    build_sql_appointments_total_latest,
    build_sql_appointments_trend,
)
from v8_appointments_query_helpers import (
    appointments_query_strategy as external_appointments_query_strategy,
    appointments_scope_table as external_appointments_scope_table,
    apply_appointments_dna_rate as external_apply_appointments_dna_rate,
    apply_appointments_hcp_breakdown as external_apply_appointments_hcp_breakdown,
    apply_appointments_mode_breakdown as external_apply_appointments_mode_breakdown,
    apply_appointments_top_practices as external_apply_appointments_top_practices,
    apply_appointments_total as external_apply_appointments_total,
    apply_appointments_trend as external_apply_appointments_trend,
    init_appointments_query_plan as external_init_appointments_query_plan,
    reset_appointments_query_fallthrough as external_reset_appointments_query_fallthrough,
)
from v9_compiler import compile_request as v9_compile_request
from v9_metric_registry import (
    APPOINTMENTS_LATEST as V9_APPOINTMENTS_LATEST,
    WORKFORCE_LATEST as V9_WORKFORCE_LATEST,
)
from v9_parser import (
    SUPPORTED_SEMANTIC_METRICS,
    derive_followup_semantic_request,
    parse_semantic_request_deterministic,
    semantic_request_to_dict,
)
from v9_semantic_types import (
    CompareSpec as V9CompareSpec,
    SemanticRequest as V9SemanticRequest,
    TimeScope as V9TimeScope,
    TransformSpec as V9TransformSpec,
)

try:
    from langgraph.checkpoint.sqlite import SqliteSaver
    _HAS_CHECKPOINTER = True
except ImportError:
    _HAS_CHECKPOINTER = False
    SqliteSaver = None  # type: ignore[assignment,misc]

try:
    from langgraph.types import interrupt, Command
    _HAS_LANGGRAPH_INTERRUPT = True
except ImportError:
    interrupt = None  # type: ignore[assignment]
    Command = None  # type: ignore[assignment]
    _HAS_LANGGRAPH_INTERRUPT = False


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
app = FastAPI(title="GP Analytics Chatbot (Agent v8.0)", version="8.0")

# Rate Limiting — prevents abuse (too many requests from one user)
# Configurable via env vars: RATE_LIMIT_CHAT, RATE_LIMIT_SUGGESTIONS, RATE_LIMIT_DEFAULT
_RATE_LIMIT_CHAT = os.getenv("RATE_LIMIT_CHAT", "10/minute")           # /chat and /chat/stream
_RATE_LIMIT_SUGGESTIONS = os.getenv("RATE_LIMIT_SUGGESTIONS", "30/minute")  # /suggestions
_RATE_LIMIT_DEFAULT = os.getenv("RATE_LIMIT_DEFAULT", "60/minute")      # everything else

limiter = Limiter(key_func=get_remote_address, default_limits=[_RATE_LIMIT_DEFAULT])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS — configurable via env
# LOCAL:      CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
# PRODUCTION: CORS_ORIGINS=https://insightsqi.cajagroup.com
_CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
_origins_list = [o.strip() for o in _CORS_ORIGINS.split(",") if o.strip()]
_cors_origin_regex = os.getenv(
    "CORS_ORIGIN_REGEX",
    r"https?://(localhost|127\.0\.0\.1)(:\d+)?$",
)

# Safety check: warn if wildcard "*" is used (allows any website to call your API)
if "*" in _origins_list:
    logger.warning("CORS_ORIGINS contains '*' — this allows ANY website to call your API. "
                    "Set CORS_ORIGINS to your actual domain in production.")

logger.info("CORS allowed origins: %s", _origins_list)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins_list,
    allow_origin_regex=_cors_origin_regex,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

# API Key Authentication — prevents unauthorised access
# Set API_KEY env var to enable. Leave blank to disable (local dev).
_API_KEY = os.getenv("API_KEY", "")
# Routes that DON'T need an API key (health checks for load balancers)
_PUBLIC_ROUTES = {"/health", "/docs", "/openapi.json"}

if _API_KEY:
    logger.info("API key authentication ENABLED (key length: %d chars)", len(_API_KEY))
else:
    logger.warning("API_KEY not set — authentication DISABLED. "
                    "Set API_KEY in production to protect your endpoints.")


@app.middleware("http")
async def verify_api_key(request: Request, call_next):
    """Check X-API-Key header on all requests except public routes."""
    # Skip if no API key configured (local dev)
    if not _API_KEY:
        return await call_next(request)

    # Skip public routes (health checks, docs)
    if request.url.path in _PUBLIC_ROUTES:
        return await call_next(request)

    # Skip CORS preflight requests (browser sends OPTIONS before POST)
    if request.method == "OPTIONS":
        return await call_next(request)

    # Check the API key
    provided_key = request.headers.get("X-API-Key", "")
    if provided_key != _API_KEY:
        logger.warning("auth | rejected request to %s from %s — invalid API key",
                       request.url.path, request.client.host if request.client else "unknown")
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized — invalid or missing API key."},
        )

    return await call_next(request)


# Request timeout (seconds)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "90"))

# Dedicated thread pool for agent execution (prevents default executor bottleneck)
_AGENT_EXECUTOR = ThreadPoolExecutor(
    max_workers=int(os.getenv("AGENT_MAX_WORKERS", "4")),
    thread_name_prefix="agent-worker",
)


# =============================================================================
# CONFIG
# =============================================================================
# AWS_PROFILE: set this on your laptop (e.g. "default" or "chatbot")
# Leave UNSET on AWS servers — the IAM Role provides credentials automatically
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")

ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "test-gp-workforce")
ATHENA_OUTPUT_S3 = os.getenv("ATHENA_OUTPUT_S3", "s3://test-athena-results-fingertips/")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "")
APPOINTMENTS_ATHENA_DATABASE = os.getenv("APPOINTMENTS_ATHENA_DATABASE", "test-gp-appointments")

BEDROCK_CHAT_MODEL_ID = os.getenv("BEDROCK_CHAT_MODEL_ID", "amazon.nova-pro-v1:0")
# BEDROCK_CHAT_MODEL_ID = os.getenv("BEDROCK_CHAT_MODEL_ID", "eu.anthropic.claude-sonnet-4-5-20250929-v1:0")

MAX_ROWS_RETURN = int(os.getenv("MAX_ROWS_RETURN", "200"))
MAX_AGENT_LOOPS = int(os.getenv("MAX_AGENT_LOOPS", "3"))
CTAS_APPROACH = os.getenv("ATHENA_CTAS_APPROACH", "true").lower() == "true"

ALLOWED_TABLES = {"practice_high", "individual", "practice_detailed"}

SCHEMA_TTL_SECONDS = int(os.getenv("SCHEMA_TTL_SECONDS", "3600"))
LATEST_TTL_SECONDS = int(os.getenv("LATEST_TTL_SECONDS", "3600"))    # 1h — data changes monthly
DISTINCT_TTL_SECONDS = int(os.getenv("DISTINCT_TTL_SECONDS", "3600"))  # 1h — vocab is stable
QUERY_CACHE_TTL = int(os.getenv("QUERY_CACHE_TTL", "300"))

DOMAIN_NOTES_PATH = os.getenv("DOMAIN_NOTES_PATH", "gp_workforce_domain_notes.md")
APPOINTMENTS_DOMAIN_NOTES_PATH = os.getenv("APPOINTMENTS_DOMAIN_NOTES_PATH", "gp_appointments_domain_notes.md")
DOMAIN_NOTES_MAX_CHARS = int(os.getenv("DOMAIN_NOTES_MAX_CHARS", "20000"))

COLUMN_DICT_PATH = os.getenv("COLUMN_DICT_PATH", "./schemas/column_dictionary.json")
APPOINTMENTS_COLUMN_DICT_PATH = os.getenv("APPOINTMENTS_COLUMN_DICT_PATH", "./schemas/appointments_column_dictionary.json")

INDIVIDUAL_COLS_CSV = os.getenv("INDIVIDUAL_COLS_CSV", "./schemas/individual_cols.csv")
PRACTICE_DETAILED_COLS_CSV = os.getenv("PRACTICE_DETAILED_COLS_CSV", "./schemas/practice_detailed_cols.csv")
PRACTICE_HIGH_COLS_CSV = os.getenv("PRACTICE_HIGH_COLS_CSV", "./schemas/practice_high_cols.csv")
APPOINTMENTS_PCN_SUBICB_COLS_CSV = os.getenv("APPOINTMENTS_PCN_SUBICB_COLS_CSV", "./schemas/appointments_pcn_subicb_cols.csv")
APPOINTMENTS_PRACTICE_COLS_CSV = os.getenv("APPOINTMENTS_PRACTICE_COLS_CSV", "./schemas/appointments_practice_cols.csv")
APPOINTMENTS_AGG_COLS_CSV = os.getenv("APPOINTMENTS_AGG_COLS_CSV", "./schemas/appointments_practice_agg_cols.csv")
APPOINTMENTS_FEW_SHOT_PATH = os.getenv("APPOINTMENTS_FEW_SHOT_PATH", "few_shot_examples_appointments.json")

MEMORY_MAX_TURNS = int(os.getenv("MEMORY_MAX_TURNS", "6"))

# Max input length for user questions
MAX_QUESTION_LENGTH = int(os.getenv("MAX_QUESTION_LENGTH", "1000"))
# Max entity hint length for SQL interpolation
MAX_ENTITY_LENGTH = int(os.getenv("MAX_ENTITY_LENGTH", "100"))

# Named constants (extracted from magic numbers)  [L5]
CLARIFICATION_TIMEOUT_SECONDS = 300       # How long clarifications stay valid
FUZZY_MATCH_THRESHOLD = 0.45              # Minimum similarity for fuzzy matching
LTM_RELEVANCE_THRESHOLD = 0.5            # Minimum similarity for LTM retrieval
PREVIEW_HEAD_ROWS = 30                    # Rows shown in dataframe preview
ANSWER_TRUNCATION_CHARS = 500             # Max chars for answer summary in memory
COLUMN_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')  # Valid SQL identifiers [C4]

# LangGraph Checkpointing path [H1]
CHECKPOINT_DB_PATH = os.getenv("CHECKPOINT_DB_PATH", ".langgraph_checkpoints.db")
USE_SEMANTIC_PATH = os.getenv("USE_SEMANTIC_PATH", "false").lower() == "true"


DatasetName = Literal["workforce", "appointments"]
StateData = MutableMapping[str, Any]


class RoutingDecision(TypedDict, total=False):
    value: str
    confidence: str
    source: str
    reason: str


class DatasetConfig(TypedDict, total=False):
    name: DatasetName
    label: str
    athena_database: str
    prompt_profile: str
    pipeline_kind: str
    query_strategy: str
    planning_mode: str
    entity_resolution_mode: str
    sql_generation_mode: str
    validation_mode: str
    default_table: str
    latest_table: str
    vocab_table: str
    vocab_columns: Dict[str, Dict[str, Any]]
    allowed_tables: List[str]
    domain_notes_path: str
    domain_notes_text: str
    column_dict_path: str
    few_shot_path: str
    schema_csvs: Dict[str, str]
    keywords: List[str]
    prompt_systems: Dict[str, str]
    valid_values_specs: List[Dict[str, Any]]
    schema_display: Dict[str, Dict[str, Any]]
    entity_resolution_specs: List[Dict[str, Any]]
    query_node_fn: Callable[[StateData], StateData]
    semantic_issue_checker: Callable[[StateData, List[str]], List[str]]


WORKFORCE_CONFIG: DatasetConfig = {
    "name": "workforce",
    "label": "GP workforce",
    "athena_database": ATHENA_DATABASE,
    "prompt_profile": "workforce",
    "pipeline_kind": "legacy_workforce",
    "query_strategy": "legacy_sql_chain",
    "planning_mode": "llm_plan",
    "entity_resolution_mode": "fuzzy_workforce",
    "sql_generation_mode": "llm_sql",
    "validation_mode": "fix_loop",
    "default_table": "individual",
    "latest_table": "individual",
    "vocab_table": "individual",
    "allowed_tables": ["individual", "practice_high", "practice_detailed"],
    "domain_notes_path": DOMAIN_NOTES_PATH,
    "column_dict_path": COLUMN_DICT_PATH,
    "few_shot_path": FEW_SHOT_PATH if "FEW_SHOT_PATH" in globals() else "few_shot_examples.json",
    "schema_csvs": {
        "individual": INDIVIDUAL_COLS_CSV,
        "practice_detailed": PRACTICE_DETAILED_COLS_CSV,
        "practice_high": PRACTICE_HIGH_COLS_CSV,
    },
    "keywords": ["gp", "fte", "headcount", "workforce", "nurse", "pharmacist", "trainee", "locum"],
    "valid_values_specs": [
        {"title": "VALID VALUES (individual table)", "items": [
            ("staff_group", "staff_groups", 50),
            ("staff_role", "staff_roles", 50),
            ("detailed_staff_role", "detailed_staff_roles", 50),
        ]},
        {"title": "VALID VALUES (practice_high table)", "items": [
            ("measure", "practice_high_measures", 200),
            ("staff_group", "practice_high_staff_groups", 30),
            ("detailed_staff_role", "practice_high_detailed_roles", 50),
        ]},
    ],
    "schema_display": {
        "practice_detailed": {
            "prefixes": [
                "prac_", "pcn_", "sub_icb_", "icb_", "region_",
                "total_gp_", "total_nurses_", "total_dpc_", "total_admin_",
                "total_patients", "total_male", "total_female", "year", "month",
            ],
            "max_columns": 120,
            "append_total_count": True,
        },
    },
    "entity_resolution_specs": [
        {
            "entity_type": "practice",
            "table": "practice_detailed",
            "column": "prac_name",
            "result_key": "prac_name_candidates",
            "latest_table": "practice_detailed",
            "plan_keys": ["prac_name"],
            "trigger_keywords": ["practice", "pcn", "prac", "surgery", "medical centre", "health centre"],
        },
        {
            "entity_type": "icb",
            "table": "individual",
            "column": "icb_name",
            "result_key": "icb_name_candidates",
            "latest_table": "individual",
            "plan_keys": ["icb_name"],
            "trigger_keywords": ["icb", "integrated care"],
            "accept_specific_icb_hint": True,
        },
        {
            "entity_type": "sub_icb",
            "table": "individual",
            "column": "sub_icb_name",
            "result_key": "sub_icb_name_candidates",
            "latest_table": "individual",
            "plan_keys": ["sub_icb_name"],
            "trigger_keywords": ["sub-icb", "sub icb"],
            "skip_if_specific_icb_hint": True,
        },
        {
            "entity_type": "region",
            "table": "individual",
            "column": "__region_column__",
            "region_table_hint": "individual",
            "result_key": "region_candidates",
            "latest_table": "individual",
            "plan_keys": [],
            "trigger_keywords": ["region"],
            "always_if_hint": True,
        },
    ],
}

APPOINTMENTS_CONFIG: DatasetConfig = {
    "name": "appointments",
    "label": "GP appointments",
    "athena_database": APPOINTMENTS_ATHENA_DATABASE,
    "prompt_profile": "appointments",
    "pipeline_kind": "rules_appointments",
    "query_strategy": "rules_query_node",
    "planning_mode": "llm_plan",
    "entity_resolution_mode": "fuzzy_appointments",
    "sql_generation_mode": "llm_sql",
    "validation_mode": "fix_loop",
    "default_table": "practice",
    "latest_table": "practice",
    "vocab_table": "practice",
    "vocab_columns": {
        "appt_modes": {"column": "appt_mode", "limit": 50},
        "appt_statuses": {"column": "appt_status", "limit": 50},
        "appt_hcp_types": {"column": "hcp_type", "limit": 50},
        "appt_categories": {"column": "national_category", "limit": 80},
        "appt_time_bands": {"column": "time_between_book_and_appt", "limit": 80},
    },
    "allowed_tables": ["pcn_subicb", "practice"],
    "domain_notes_path": APPOINTMENTS_DOMAIN_NOTES_PATH,
    "column_dict_path": APPOINTMENTS_COLUMN_DICT_PATH,
    "few_shot_path": APPOINTMENTS_FEW_SHOT_PATH,
    "schema_csvs": {
        "pcn_subicb": APPOINTMENTS_PCN_SUBICB_COLS_CSV,
        "practice": APPOINTMENTS_PRACTICE_COLS_CSV,
    },
    "keywords": [
        "appointment", "appointments", "dna", "did not attend", "face-to-face",
        "telephone", "video", "online", "home visit", "hcp", "consultation",
        "appt", "attended", "book and appt",
    ],
    "valid_values_specs": [
        {"title": "VALID VALUES (appointments)", "items": [
            ("appt_mode", "appt_modes", 50),
            ("appt_status", "appt_statuses", 50),
            ("hcp_type", "appt_hcp_types", 50),
            ("national_category", "appt_categories", 80),
            ("time_between_book_and_appt", "appt_time_bands", 80),
        ]},
    ],
    "schema_display": {},
    "entity_resolution_specs": [
        {
            "entity_type": "practice",
            "table": "practice",
            "column": "gp_name",
            "result_key": "gp_name_candidates",
            "latest_table": "practice",
            "plan_keys": ["gp_name"],
            "trigger_keywords": ["practice", "surgery", "medical centre", "health centre", "clinic"],
        },
        {
            "entity_type": "icb",
            "table": "pcn_subicb",
            "column": "icb_name",
            "result_key": "icb_name_candidates",
            "latest_table": "practice",
            "plan_keys": ["icb_name"],
            "trigger_keywords": ["icb", "integrated care"],
        },
        {
            "entity_type": "sub_icb",
            "table": "pcn_subicb",
            "column": "sub_icb_location_name",
            "result_key": "sub_icb_location_name_candidates",
            "latest_table": "practice",
            "plan_keys": ["sub_icb_location_name"],
            "trigger_keywords": ["sub icb", "sub-icb"],
        },
        {
            "entity_type": "region",
            "table": "pcn_subicb",
            "column": "region_name",
            "result_key": "region_name_candidates",
            "latest_table": "practice",
            "plan_keys": ["region_name"],
            "trigger_keywords": ["region"],
        },
        {
            "entity_type": "pcn",
            "table": "practice",
            "column": "pcn_name",
            "result_key": "pcn_name_candidates",
            "latest_table": "practice",
            "plan_keys": ["pcn_name"],
            "trigger_keywords": ["pcn"],
        },
    ],
}

DATASET_CONFIGS: Dict[DatasetName, DatasetConfig] = {
    "workforce": WORKFORCE_CONFIG,
    "appointments": APPOINTMENTS_CONFIG,
}


def _dataset_version() -> str:
    return "8.0-agent"


def _dataset_config(dataset: DatasetName) -> DatasetConfig:
    if dataset == "cross_dataset":
        return APPOINTMENTS_CONFIG
    return DATASET_CONFIGS[dataset]


def _dataset_allowed_tables(dataset: DatasetName) -> set[str]:
    return set(_dataset_config(dataset).get("allowed_tables") or [])


def _load_simple_vocab_from_config(state: MutableMapping[str, Any], config: DatasetConfig) -> MutableMapping[str, Any]:
    return external_load_simple_vocab_from_config(
        state,
        config,
        get_latest_year_month=get_latest_year_month,
        list_distinct_values=list_distinct_values,
    )


def _load_workforce_latest_and_vocab(state: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    return external_load_workforce_latest_and_vocab(
        state,
        get_latest_year_month=get_latest_year_month,
        resolve_time_range=resolve_time_range,
        list_distinct_values=list_distinct_values,
    )


def make_dataset_vocab_loader(config: DatasetConfig):
    """Small factory step toward per-dataset sub-pipelines."""
    dataset_name = config["name"]
    pipeline_kind = config.get("pipeline_kind", "")

    def _node(state: StateData) -> StateData:
        if pipeline_kind == "legacy_workforce":
            return _load_workforce_latest_and_vocab(state)
        return _load_simple_vocab_from_config(state, config)

    return _node


def make_dataset_sql_runner(config: DatasetConfig):
    dataset_name = config["name"]
    athena_db = config.get("athena_database")

    def _node(state: StateData) -> StateData:
        sql = (state.get("sql") or "").strip()
        if not sql:
            return state
        try:
            df = run_athena_df(sql, database=athena_db)
            state["_rows"] = int(len(df))
            state["_empty"] = bool(df.empty)
            state["df_preview_md"] = safe_markdown(df, head=PREVIEW_HEAD_ROWS)
            state["last_error"] = None
            logger.info("dataset_sql_runner[%s] | success rows=%d", dataset_name, len(df))
            return state
        except Exception as e:
            state["_rows"] = 0
            state["_empty"] = True
            state["last_error"] = str(e)
            state["answer"] = f"I hit a {dataset_name} query error: {_sanitise_error(e)}"
            logger.warning("dataset_sql_runner[%s] | error: %s", dataset_name, str(e)[:200])
            return state

    return _node


def _identity_state_node(state: StateData) -> StateData:
    return state


def _default_semantic_issue_checker(state: MutableMapping[str, Any], issues: List[str]) -> List[str]:
    return external_default_semantic_issue_checker(state, issues)


def _appointments_semantic_issue_checker(state: MutableMapping[str, Any], issues: List[str]) -> List[str]:
    return external_appointments_semantic_issue_checker(
        state,
        issues,
        extract_practice_code=extract_practice_code,
    )


def make_dataset_query_node(config: DatasetConfig):
    node_fn = config.get("query_node_fn")
    if callable(node_fn):
        inner_node = node_fn
    else:
        strategy = config.get("query_strategy", "")
        if strategy == "rules_query_node":
            inner_node = node_appointments_query
        else:
            inner_node = node_hard_override_sql

    def semantic_first_node(state: StateData) -> StateData:
        if _try_v9_semantic_path(state):
            logger.info("make_dataset_query_node | semantic fast-path hit")
            return state
        return inner_node(state)

    return semantic_first_node


def make_dataset_planner_node(config: DatasetConfig):
    mode = config.get("planning_mode", "")
    if mode == "rules_embedded":
        return _identity_state_node
    return node_plan


def make_dataset_entity_resolver_node(config: DatasetConfig):
    mode = config.get("entity_resolution_mode", "")
    if mode == "none":
        return _identity_state_node
    return node_resolve_entities


def make_dataset_sql_generator_node(config: DatasetConfig):
    mode = config.get("sql_generation_mode", "")
    if mode == "rules_embedded":
        return _identity_state_node
    return node_generate_sql


def make_dataset_validator_node(config: DatasetConfig):
    mode = config.get("validation_mode", "")
    if mode == "none":
        return _identity_state_node
    return node_validate_or_fix


WORKFORCE_LATEST_VOCAB_NODE = make_dataset_vocab_loader(WORKFORCE_CONFIG)
APPOINTMENTS_LATEST_VOCAB_NODE = make_dataset_vocab_loader(APPOINTMENTS_CONFIG)
APPOINTMENTS_RUN_SQL_NODE = make_dataset_sql_runner(APPOINTMENTS_CONFIG)

# [H3] LangSmith / Langfuse tracing — auto-enabled when env vars set
# Set LANGCHAIN_TRACING_V2=true and LANGCHAIN_API_KEY=<key> for LangSmith
# Set LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY for Langfuse
_TRACING_ENABLED = os.getenv("LANGCHAIN_TRACING_V2", "false").lower() == "true"
if _TRACING_ENABLED:
    _tracing_project = os.getenv("LANGCHAIN_PROJECT", "gp-workforce-chatbot")
    os.environ.setdefault("LANGCHAIN_PROJECT", _tracing_project)


# =============================================================================
# AWS Session
# =============================================================================
# On your laptop: uses AWS_PROFILE (reads ~/.aws/credentials)
# On AWS (EB/ECS/EC2): uses IAM Role automatically (no profile needed)
_aws_profile = os.getenv("AWS_PROFILE")  # None if not set → uses IAM role
if _aws_profile:
    boto_sess = boto3.Session(profile_name=_aws_profile, region_name=AWS_REGION)
    logger.info("AWS session created with profile '%s' in %s", _aws_profile, AWS_REGION)
else:
    boto_sess = boto3.Session(region_name=AWS_REGION)
    logger.info("AWS session created with IAM role / default credentials in %s", AWS_REGION)


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
# Structured Output Models (for LLM responses)
# =============================================================================
class QueryClassification(BaseModel):
    """Structured output for query classification."""
    category: Literal["knowledge", "data_simple", "data_complex", "out_of_scope"] = Field(
        description=(
            "knowledge = methodology/definition question (no SQL needed). "
            "data_simple = straightforward single-table aggregation. "
            "data_complex = trends, rankings, comparisons, multi-group queries. "
            "out_of_scope = unrelated to GP workforce data."
        )
    )


class DatasetClassification(BaseModel):
    """Structured output for dataset routing."""
    dataset: Literal["workforce", "appointments"] = Field(
        description="Which dataset should answer the user's question."
    )
    confidence: Literal["high", "medium", "low"] = Field(
        default="medium",
        description="Confidence in the dataset routing choice."
    )


class QueryRewriteDecision(BaseModel):
    """Structured output for the query rewriter node."""
    should_rewrite: bool = Field(
        default=False,
        description="Whether the user's query should be rewritten into a clearer analytic request."
    )
    rewritten_question: str = Field(
        default="",
        description="A clearer rewritten question that preserves user intent, scope, and metric."
    )
    notes: str = Field(
        default="",
        description="Brief note explaining why the rewrite helps."
    )


class QueryPlan(BaseModel):
    """Structured output for the query planner — replaces free-form JSON."""
    in_scope: bool = Field(
        description="Whether the question can be answered from the active dataset"
    )
    table: Literal["individual", "practice_high", "practice_detailed", "practice", "pcn_subicb"] = Field(
        default="individual",
        description="Which database table to query"
    )
    intent: Literal[
        "total", "percent_split", "ratio", "trend", "topn",
        "lookup", "comparison", "demographics", "unknown"
    ] = Field(
        default="unknown",
        description="The analytical intent of the query"
    )
    group_by: List[str] = Field(
        default_factory=list,
        description="Column names to GROUP BY in the SQL query"
    )
    filters_needed: List[str] = Field(
        default_factory=list,
        description="SQL WHERE clause fragments to apply"
    )
    entities_to_resolve: List[str] = Field(
        default_factory=list,
        description="Entity column names that need fuzzy matching (e.g. icb_name, prac_name)"
    )
    needs_clarification: bool = Field(
        default=False,
        description=(
            "Set to true ONLY if the question is genuinely ambiguous and cannot be "
            "reasonably interpreted without asking the user. Examples: 'show me the data' "
            "(which data?), 'compare them' (compare what?). Do NOT set true for questions "
            "that can be answered with reasonable defaults."
        )
    )
    clarification_question: str = Field(
        default="",
        description=(
            "If needs_clarification is true, a short friendly question to ask the user. "
            "Offer 2-3 specific options. Example: 'Would you like to see GP FTE, headcount, "
            "or both? And for which geography — national, by region, or by ICB?'"
        )
    )
    notes: str = Field(
        default="",
        description="Short explanation of the query plan and reasoning"
    )


# =============================================================================
# Conversation Memory (per session)
# =============================================================================
class ConversationMemory:
    """Thread-safe conversation memory: last N turns per session + entity context + pending clarifications."""

    def __init__(self, max_sessions: int = 200, max_turns: int = MEMORY_MAX_TURNS):
        self._store: OrderedDict[str, List[Dict[str, str]]] = OrderedDict()
        self._entity_context: Dict[str, Dict[str, Any]] = {}
        self._pending_clarification: Dict[str, Dict[str, Any]] = {}
        self._user_preferences: Dict[str, Dict[str, Any]] = {}
        self._max_sessions = max_sessions
        self._max_turns = max_turns
        self._lock = threading.Lock()  # [C1] Thread safety for concurrent requests

    @property
    def session_count(self) -> int:
        """Number of active sessions. [L9] Use this instead of accessing _store directly."""
        with self._lock:
            return len(self._store)

    def get_history(self, session_id: str) -> List[Dict[str, str]]:
        with self._lock:
            return list(self._store.get(session_id, []))

    def get_entity_context(self, session_id: str) -> Dict[str, Any]:
        """Return the last entity context for this session (practice name, ICB, table, etc)."""
        with self._lock:
            return self._entity_context.get(session_id, {}).copy()

    def get_user_preferences(self, session_id: str) -> Dict[str, Any]:
        """Return lightweight user preferences inferred from the session so far."""
        with self._lock:
            return self._user_preferences.get(session_id, {}).copy()

    def update_user_preferences(self, session_id: str, preferences: Dict[str, Any]):
        if not session_id or not preferences:
            return
        with self._lock:
            merged = self._user_preferences.get(session_id, {}).copy()
            for key, value in preferences.items():
                if value not in (None, "", [], {}):
                    merged[key] = value
            if merged:
                self._user_preferences[session_id] = merged

    def get_last_user_question(self, session_id: str) -> str:
        with self._lock:
            turns = self._store.get(session_id, [])
            for turn in reversed(turns):
                if turn["role"] == "user":
                    return turn["content"]
        return ""

    def save_entity_context(self, session_id: str, context: Dict[str, Any]):
        """Save entity context from the current turn for follow-up resolution.
        Evicts oldest entries if _entity_context exceeds max_sessions (safety cap).
        """
        with self._lock:
            self._entity_context[session_id] = context
            # Safety cap: prevent unbounded growth if save_entity_context is called
            # for sessions that bypass add_turn (shouldn't happen, but defensive)
            while len(self._entity_context) > self._max_sessions * 2:
                # Remove entries not in _store (orphaned contexts)
                orphans = [k for k in self._entity_context if k not in self._store]
                if orphans:
                    self._entity_context.pop(orphans[0], None)
                else:
                    break

    def set_pending_clarification(self, session_id: str, original_question: str,
                                   clarification_question: str, partial_plan: Optional[Dict] = None):
        """Store a pending clarification for this session — next user message = clarification answer."""
        with self._lock:
            self._pending_clarification[session_id] = {
                "original_question": original_question,
                "clarification_question": clarification_question,
                "partial_plan": partial_plan or {},
                "timestamp": time.time(),
            }

    def get_pending_clarification(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return pending clarification state, or None. Auto-expires after timeout."""
        with self._lock:
            pending = self._pending_clarification.get(session_id)
            if not pending:
                return None
            if time.time() - pending.get("timestamp", 0) > CLARIFICATION_TIMEOUT_SECONDS:  # [L5]
                self._pending_clarification.pop(session_id, None)
                return None
            return pending.copy()

    def clear_pending_clarification(self, session_id: str):
        """Clear the pending clarification after it's been consumed."""
        with self._lock:
            self._pending_clarification.pop(session_id, None)

    def add_turn(self, session_id: str, question: str, answer: str, sql: str = "",
                 entity_context: Optional[Dict[str, Any]] = None):
        with self._lock:
            if session_id not in self._store:
                self._store[session_id] = []
                if len(self._store) > self._max_sessions:
                    oldest_key = next(iter(self._store))
                    self._store.pop(oldest_key)
                    self._entity_context.pop(oldest_key, None)
                    self._pending_clarification.pop(oldest_key, None)  # [C1] Clean up on eviction
                    self._user_preferences.pop(oldest_key, None)
            turns = self._store[session_id]
            turns.append({"role": "user", "content": question})
            summary = answer[:ANSWER_TRUNCATION_CHARS]  # [L5]
            if sql:
                summary += f"\n[SQL used: {sql[:200]}]"
            turns.append({"role": "assistant", "content": summary})
            if len(turns) > self._max_turns * 2:
                self._store[session_id] = turns[-(self._max_turns * 2):]
            # Store entity context for follow-ups
            if entity_context:
                self._entity_context[session_id] = entity_context

    def format_for_prompt(self, session_id: str, max_recent_turns: int = 3) -> str:
        """Format conversation history for LLM prompt.
        Only includes the last `max_recent_turns` Q&A pairs to limit
        topic contamination from older unrelated conversations.
        """
        history = self.get_history(session_id)  # already thread-safe
        if not history:
            return ""
        # Limit to last N turns (each turn = 2 entries: user + assistant)
        max_entries = max_recent_turns * 2
        recent = history[-max_entries:] if len(history) > max_entries else history
        lines = []
        for turn in recent:
            prefix = "User" if turn["role"] == "user" else "Assistant"
            lines.append(f"{prefix}: {turn['content']}")
        return "\n".join(lines)


MEMORY = ConversationMemory()


def _infer_user_preferences(question: str, current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Persist only explicit style/metric preferences that should carry across turns."""
    prefs = dict(current or {})
    q = (question or "").lower()

    if re.search(r"\b(?:briefly|brief|short answer|in short|keep it short|quick answer|concise|one line)\b", q):
        prefs["response_style"] = "concise"
    elif re.search(r"\b(?:in detail|detailed|more detail|full breakdown|deep dive|step by step)\b", q):
        prefs["response_style"] = "detailed"

    if re.search(r"\bi\s+meant\s+headcount\b|\b(?:headcount|head count)\s+(?:not|instead|rather|only)\b|\bi\s+(?:want|need|prefer)\s+headcount\b", q):
        prefs["preferred_metric"] = "headcount"
    elif re.search(r"\bi\s+meant\s+fte\b|\bfte\s+(?:not|instead|rather|only)\b|\bi\s+(?:want|need|prefer)\s+fte\b", q):
        prefs["preferred_metric"] = "fte"

    return prefs


def _style_instruction_for_preferences(preferences: Optional[Dict[str, Any]], question: str,
                                       *, knowledge: bool = False) -> str:
    q = (question or "").lower()
    response_style = (preferences or {}).get("response_style", "")

    if re.search(r"\b(?:briefly|brief|short answer|in short|keep it short|quick answer|concise|one line)\b", q):
        response_style = "concise"
    elif re.search(r"\b(?:in detail|detailed|more detail|full breakdown|deep dive|step by step|explain)\b", q):
        response_style = "detailed"

    if knowledge:
        base = ("Use a direct first sentence and sound like a human analyst. "
                "Prefer short paragraphs over bullets unless you are listing distinct concepts or source systems.")
    else:
        base = ("Start with one direct answer sentence and sound like a human analyst. "
                "Prefer a short paragraph over repetitive bullets, but use bullets when there are clearly separate facts.")

    if response_style == "detailed":
        return base + " Give 1-2 brief lines of interpretation or context after the main answer."

    return base + " Keep it tight: usually one short paragraph, or a lead sentence plus up to 3 bullets."


def _polish_answer_text(answer: str) -> str:
    answer = (answer or "").strip()
    answer = re.sub(r"[ \t]+\n", "\n", answer)
    answer = re.sub(r"\n{3,}", "\n\n", answer)
    return answer


def _followup_intent(question: str) -> str:
    """Classify short conversational follow-ups that need special handling."""
    q = (question or "").lower().strip()
    if not q:
        return ""

    benchmark_patterns = [
        r"\bis\s+(?:that|this|it)\s+(?:high|low)\b",
        r"\bis\s+(?:that|this|it)\s+(?:above|below)\s+average\b",
        r"\bhow\s+does\s+(?:that|this|it)\s+compare\b",
        r"\bwhere\s+does\s+(?:that|this|it)\s+sit\b",
    ]
    if any(re.search(p, q) for p in benchmark_patterns):
        return "benchmark_probe"

    explanation_patterns = [
        r"^why\s+(?:is|was)\s+(?:that|this|it)\b",
        r"^why\s+(?:so\s+)?(?:high|low)\b",
        r"\bwhat\s+(?:might\s+)?explain\s+(?:that|this|it)\b",
        r"\bwhy\s+might\s+(?:that|this|it)\s+be\b",
        r"\bwhat\s+does\s+(?:that|this|it)\s+mean\b",
        r"\bis\s+(?:that|this|it)\s+good\s+or\s+bad\b",
        r"\bshould\s+i\s+be\s+concerned\b",
    ]
    if any(re.search(p, q) for p in explanation_patterns):
        return "explanation"

    return ""


# =============================================================================
# Bounded TTL Cache (prevents memory leaks)
# =============================================================================
class BoundedTTLCache:
    """Thread-safe TTL cache with a max-size eviction policy (LRU). [C1]"""

    def __init__(self, max_size: int = 100, ttl: float = 3600):
        self._store: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Tuple[float, Any]]:
        with self._lock:
            item = self._store.get(key)
            if item and (time.time() - item[0] < self._ttl):
                self._store.move_to_end(key)
                return item
            if item:
                self._store.pop(key, None)
            return None

    def set(self, key: str, value: Any):
        with self._lock:
            self._store[key] = (time.time(), value)
            self._store.move_to_end(key)
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)

    def clear(self):
        with self._lock:
            self._store.clear()

    def __len__(self):
        with self._lock:
            return len(self._store)


# =============================================================================
# Caches (all bounded)
# =============================================================================
_SCHEMA_CACHE = BoundedTTLCache(max_size=20, ttl=SCHEMA_TTL_SECONDS)
_LATEST_CACHE = BoundedTTLCache(max_size=20, ttl=LATEST_TTL_SECONDS)
_DISTINCT_CACHE = BoundedTTLCache(max_size=200, ttl=DISTINCT_TTL_SECONDS)
_QUERY_CACHE = BoundedTTLCache(max_size=50, ttl=QUERY_CACHE_TTL)


# =============================================================================
# Few-Shot Example Retriever (Vector Similarity)
# =============================================================================
FEW_SHOT_PATH = os.getenv("FEW_SHOT_PATH", "few_shot_examples.json")
FEW_SHOT_TOP_K = int(os.getenv("FEW_SHOT_TOP_K", "4"))
EMBED_MODEL_ID = os.getenv("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
EMBED_DIMENSIONS = int(os.getenv("EMBED_DIMENSIONS", "256"))
SEMANTIC_CACHE_THRESHOLD = float(os.getenv("SEMANTIC_CACHE_THRESHOLD", "0.92"))


# [C3] Module-level Bedrock runtime client — reused across all embedding calls
_bedrock_runtime = boto_sess.client("bedrock-runtime")


def _embed_text(text: str) -> np.ndarray:
    """Get embedding vector from Bedrock Titan Embed v2. Uses shared client [C3]."""
    resp = _bedrock_runtime.invoke_model(
        modelId=EMBED_MODEL_ID,
        body=json.dumps({"inputText": text, "dimensions": EMBED_DIMENSIONS}),
    )
    result = json.loads(resp["body"].read())
    return np.array(result["embedding"], dtype=np.float32)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two vectors."""
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 0.0
    return float(dot / norm)


class FewShotRetriever:
    """Loads golden Q→SQL examples, embeds them, and retrieves similar ones."""

    def __init__(self, path: str = FEW_SHOT_PATH, top_k: int = FEW_SHOT_TOP_K):
        self._examples: List[Dict[str, str]] = []
        self._embeddings: List[np.ndarray] = []
        self._top_k = top_k
        self._ready = False
        self._load(path)

    def _load(self, path: str):
        """Load examples and compute embeddings at startup."""
        if not os.path.exists(path):
            logger.warning("Few-shot examples file not found: %s", path)
            return
        try:
            with open(path, "r") as f:
                self._examples = json.load(f)
            logger.info("Embedding %d few-shot examples...", len(self._examples))
            for ex in self._examples:
                emb = _embed_text(ex["question"])
                self._embeddings.append(emb)
            self._ready = True
            logger.info("Few-shot retriever ready with %d examples (%d dims)",
                        len(self._examples), EMBED_DIMENSIONS)
        except Exception as e:
            logger.error("Failed to load few-shot examples: %s", e)
            self._ready = False

    def retrieve(self, question: str, top_k: int = None) -> List[Dict[str, str]]:
        """Return top-K most similar examples for the given question."""
        if not self._ready or not self._examples:
            return []
        k = top_k or self._top_k
        q_emb = _embed_text(question)
        scored = []
        for i, ex_emb in enumerate(self._embeddings):
            sim = _cosine_similarity(q_emb, ex_emb)
            scored.append((sim, i))
        scored.sort(reverse=True, key=lambda x: x[0])
        results = []
        for sim, idx in scored[:k]:
            ex = self._examples[idx].copy()
            ex["similarity"] = round(sim, 4)
            results.append(ex)
        return results

    def find_nearest(self, question: str) -> Tuple[float, Optional[Dict[str, str]]]:
        """Return the closest match and its similarity score (for semantic cache)."""
        if not self._ready or not self._examples:
            return 0.0, None
        q_emb = _embed_text(question)
        best_sim, best_idx = 0.0, -1
        for i, ex_emb in enumerate(self._embeddings):
            sim = _cosine_similarity(q_emb, ex_emb)
            if sim > best_sim:
                best_sim, best_idx = sim, i
        if best_idx >= 0:
            return best_sim, self._examples[best_idx]
        return 0.0, None

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def count(self) -> int:
        return len(self._examples)


def _dataset_few_shot_path(dataset: DatasetName) -> str:
    return str(_dataset_config(dataset).get("few_shot_path") or FEW_SHOT_PATH)


FEW_SHOT_RETRIEVERS: Dict[DatasetName, FewShotRetriever] = {
    "workforce": FewShotRetriever(_dataset_few_shot_path("workforce")),
    "appointments": FewShotRetriever(_dataset_few_shot_path("appointments")),
}

# Backward-compatible alias for legacy workforce-only references.
FEW_SHOT = FEW_SHOT_RETRIEVERS["workforce"]


def _few_shot_retriever_for_dataset(dataset: DatasetName) -> FewShotRetriever:
    return FEW_SHOT_RETRIEVERS.get(dataset, FEW_SHOT)


def _dataset_ltm_examples(question: str, dataset: DatasetName, top_k: int = 2) -> List[Dict[str, Any]]:
    examples = LONG_TERM_MEMORY.retrieve(question, top_k=top_k)
    allowed_tables = _dataset_allowed_tables(dataset)
    filtered: List[Dict[str, Any]] = []
    for ex in examples:
        table = str(ex.get("table") or "").strip().lower()
        if table and table in allowed_tables:
            filtered.append(ex)
    return filtered


# =============================================================================
# Semantic Answer Cache (embedding-based)
# =============================================================================
class SemanticCache:
    """Thread-safe semantic answer cache — 'How many GPs?' ≈ 'Total GP count?' [C1]"""

    def __init__(self, max_size: int = 100, ttl: float = 300.0,
                 threshold: float = SEMANTIC_CACHE_THRESHOLD):
        self._entries: List[Dict[str, Any]] = []  # {embedding, question, response, ts}
        self._max_size = max_size
        self._ttl = ttl
        self._threshold = threshold
        self._lock = threading.Lock()

    def get(self, question: str) -> Optional[Dict[str, Any]]:
        """Check if a semantically similar question was recently answered."""
        if not FEW_SHOT.ready:
            return None
        # Compute embedding outside lock (I/O bound)
        q_emb = _embed_text(question)
        with self._lock:
            self._evict_expired()
            if not self._entries:
                return None
            best_sim, best_entry = 0.0, None
            for entry in self._entries:
                sim = _cosine_similarity(q_emb, entry["embedding"])
                if sim > best_sim:
                    best_sim, best_entry = sim, entry
            if best_sim >= self._threshold and best_entry:
                logger.info("Semantic cache HIT (sim=%.4f): '%s' ≈ '%s'",
                            best_sim, question[:60], best_entry["question"][:60])
                return best_entry["response"]
            return None

    def put(self, question: str, response: Dict[str, Any]):
        """Store a successful response with its embedding."""
        if not FEW_SHOT.ready:
            return
        # Compute embedding outside lock (I/O bound)
        q_emb = _embed_text(question)
        with self._lock:
            self._evict_expired()
            if len(self._entries) >= self._max_size:
                self._entries.pop(0)  # remove oldest
            self._entries.append({
                "embedding": q_emb,
                "question": question,
                "response": response,
                "ts": time.time(),
            })

    def _evict_expired(self):
        """Must be called with self._lock held."""
        cutoff = time.time() - self._ttl
        self._entries = [e for e in self._entries if e["ts"] > cutoff]

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._entries)


_SEMANTIC_CACHE = SemanticCache(
    max_size=int(os.getenv("SEMANTIC_CACHE_MAX_SIZE", "100")),
    ttl=float(os.getenv("SEMANTIC_CACHE_TTL", "300")),
)


# =============================================================================
# Long-Term Memory Store (persistent, auto-learning)
# =============================================================================
LONG_TERM_MEMORY_PATH = os.getenv("LONG_TERM_MEMORY_PATH", "learned_examples.json")
LONG_TERM_MEMORY_MAX = int(os.getenv("LONG_TERM_MEMORY_MAX", "200"))
LONG_TERM_DEDUP_THRESHOLD = float(os.getenv("LONG_TERM_DEDUP_THRESHOLD", "0.90"))
LONG_TERM_MIN_CONFIDENCE = float(os.getenv("LONG_TERM_MIN_CONFIDENCE", "0.85"))


class LongTermMemory:
    """
    Persistent memory that auto-learns from high-confidence successful queries.

    Stores proven Q→SQL mappings to disk, deduplicates by embedding similarity,
    and provides retrieval for augmenting few-shot examples.

    Entries are stored as:
      { question, table, sql, confidence, learned_at, use_count }
    """

    def __init__(self, path: str = LONG_TERM_MEMORY_PATH,
                 max_entries: int = LONG_TERM_MEMORY_MAX,
                 dedup_threshold: float = LONG_TERM_DEDUP_THRESHOLD,
                 min_confidence: float = LONG_TERM_MIN_CONFIDENCE):
        self._path = path
        self._max_entries = max_entries
        self._dedup_threshold = dedup_threshold
        self._min_confidence = min_confidence
        self._entries: List[Dict[str, Any]] = []
        self._embeddings: List[np.ndarray] = []
        self._dirty = False  # track unsaved changes
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        """Load learned examples from disk at startup."""
        if not os.path.exists(self._path):
            logger.info("No long-term memory file found at %s — starting fresh", self._path)
            return
        try:
            with open(self._path, "r") as f:
                data = json.load(f)
            if not isinstance(data, list):
                logger.warning("Long-term memory file invalid format, starting fresh")
                return
            self._entries = data
            # Embed all entries (if embedding service is available)
            if FEW_SHOT.ready:
                logger.info("Embedding %d long-term memory entries...", len(self._entries))
                for entry in self._entries:
                    try:
                        emb = _embed_text(entry["question"])
                        self._embeddings.append(emb)
                    except Exception as e:
                        logger.warning("Failed to embed LTM entry: %s", e)
                        self._embeddings.append(np.zeros(EMBED_DIMENSIONS, dtype=np.float32))
                logger.info("Long-term memory loaded: %d entries", len(self._entries))
            else:
                logger.info("Long-term memory loaded: %d entries (embeddings deferred)", len(self._entries))
        except Exception as e:
            logger.error("Failed to load long-term memory: %s", e)

    def _save(self):
        """Persist entries to disk."""
        try:
            # Save without embeddings (they're recomputed on load)
            with open(self._path, "w") as f:
                json.dump(self._entries, f, indent=2, default=str)
            self._dirty = False
            logger.info("Long-term memory saved: %d entries to %s", len(self._entries), self._path)
        except Exception as e:
            logger.error("Failed to save long-term memory: %s", e)

    def _is_duplicate(self, question: str) -> bool:
        """Check if a semantically similar question already exists."""
        if not self._embeddings:
            return False
        try:
            q_emb = _embed_text(question)
            for ex_emb in self._embeddings:
                if _cosine_similarity(q_emb, ex_emb) >= self._dedup_threshold:
                    return True
        except Exception:
            pass
        return False

    def learn(self, question: str, table: str, sql: str, confidence: float):
        """
        Auto-learn a successful query if it meets quality thresholds.
        - Must have confidence >= min_confidence
        - Must not duplicate an existing entry
        - Must have valid SQL with a SELECT
        """
        if confidence < self._min_confidence:
            return False
        if not sql or "SELECT" not in sql.upper():
            return False
        # Don't learn template/hard-override queries (they're already in few-shot)
        if not table:
            return False

        with self._lock:
            if self._is_duplicate(question):
                logger.debug("LTM skip duplicate: '%s'", question[:60])
                return False

            # Build entry
            entry = {
                "question": question,
                "table": table,
                "sql": sql,
                "confidence": round(confidence, 2),
                "learned_at": datetime.now().isoformat(),
                "use_count": 0,
            }
            self._entries.append(entry)
            try:
                self._embeddings.append(_embed_text(question))
            except Exception:
                self._embeddings.append(np.zeros(EMBED_DIMENSIONS, dtype=np.float32))

            # Evict lowest-value entries if over capacity
            if len(self._entries) > self._max_entries:
                # Sort by (confidence desc, use_count desc) to keep the best
                n = len(self._entries)
                scored = sorted(range(n),
                                key=lambda i: (self._entries[i].get("confidence", 0),
                                               self._entries[i].get("use_count", 0)),
                                reverse=True)
                keep_indices = sorted(scored[:self._max_entries])  # preserve original order
                # Rebuild both lists using the SAME index set (prevents desync)
                self._entries = [self._entries[i] for i in keep_indices]
                self._embeddings = [self._embeddings[i] for i in keep_indices
                                    if i < len(self._embeddings)]
                # Safety: pad embeddings if they fell behind entries
                while len(self._embeddings) < len(self._entries):
                    logger.warning("LTM eviction: padding missing embedding (desync detected)")
                    self._embeddings.append(np.zeros(EMBED_DIMENSIONS, dtype=np.float32))

            self._dirty = True
            logger.info("LTM learned: '%s' (confidence=%.2f, total=%d)",
                        question[:60], confidence, len(self._entries))

            # Auto-save every 5 new entries
            if len(self._entries) % 5 == 0:
                self._save()

            return True

    def retrieve(self, question: str, top_k: int = 2) -> List[Dict[str, Any]]:
        """Retrieve top-K similar learned examples for the given question. Thread-safe [C1]."""
        if not self._embeddings or not self._entries:
            return []
        try:
            q_emb = _embed_text(question)
            with self._lock:  # [C1] Protect shared state mutation
                scored = []
                for i, ex_emb in enumerate(self._embeddings):
                    sim = _cosine_similarity(q_emb, ex_emb)
                    scored.append((sim, i))
                scored.sort(reverse=True, key=lambda x: x[0])

                results = []
                for sim, idx in scored[:top_k]:
                    if sim < LTM_RELEVANCE_THRESHOLD:  # [L5] named constant
                        break
                    ex = self._entries[idx].copy()
                    ex["similarity"] = round(sim, 4)
                    # Increment use count (now thread-safe under lock)
                    self._entries[idx]["use_count"] = self._entries[idx].get("use_count", 0) + 1
                    results.append(ex)
                return results
        except Exception as e:
            logger.warning("LTM retrieve error: %s", e)
            return []

    def flush(self):
        """Force save to disk."""
        with self._lock:
            if self._dirty:
                self._save()

    @property
    def count(self) -> int:
        return len(self._entries)

    @property
    def stats(self) -> Dict[str, Any]:
        """Return stats about the long-term memory store."""
        if not self._entries:
            return {"count": 0}
        avg_conf = sum(e.get("confidence", 0) for e in self._entries) / len(self._entries)
        total_uses = sum(e.get("use_count", 0) for e in self._entries)
        return {
            "count": len(self._entries),
            "avg_confidence": round(avg_conf, 2),
            "total_uses": total_uses,
            "oldest": self._entries[0].get("learned_at", ""),
            "newest": self._entries[-1].get("learned_at", "") if self._entries else "",
        }


# Initialise at module load (after FEW_SHOT)
LONG_TERM_MEMORY = LongTermMemory()

# Flush to disk on shutdown
atexit.register(lambda: LONG_TERM_MEMORY.flush())


# =============================================================================
# Column Dictionary
# =============================================================================
def load_column_dictionary(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


COLUMN_DICTS: Dict[DatasetName, Dict[str, Any]] = {
    "workforce": load_column_dictionary(COLUMN_DICT_PATH),
    "appointments": load_column_dictionary(APPOINTMENTS_COLUMN_DICT_PATH),
}
COLUMN_DICT = COLUMN_DICTS["workforce"]


def get_column_labels(table: str, dataset: Optional[DatasetName] = None) -> str:
    """Return a compact string of key column -> human label mappings for a table."""
    target_dataset = dataset or _dataset_for_table(table)
    dataset_dict = COLUMN_DICTS.get(target_dataset, {})
    tdata = dataset_dict.get(table, {})
    if not tdata and dataset_dict and all(isinstance(v, str) for v in dataset_dict.values()):
        tdata = dataset_dict
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
# Allows: letters, digits, spaces, hyphens, apostrophes, commas, periods, ampersands,
# parentheses, question marks, colons, semicolons, exclamation marks, and percent signs
_SAFE_ENTITY_PATTERN = re.compile(r"^[a-zA-Z0-9\s\-'.,&()/!?;:%]+$")


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


def athena_kwargs(database: Optional[str] = None) -> Dict[str, Any]:
    kw = {
        "database": database or ATHENA_DATABASE,
        "s3_output": ATHENA_OUTPUT_S3,
        "boto3_session": boto_sess,
        "ctas_approach": CTAS_APPROACH,
    }
    if ATHENA_WORKGROUP.strip():
        kw["workgroup"] = ATHENA_WORKGROUP.strip()
    return kw


def run_athena_df(sql: str, database: Optional[str] = None) -> pd.DataFrame:
    db = database or ATHENA_DATABASE
    cache_key = hashlib.md5(f"{db}::{sql.strip().lower()}".encode()).hexdigest()
    cached = _QUERY_CACHE.get(cache_key)
    if cached:
        logger.debug("Query cache HIT: %s", cache_key[:12])
        return cached[1].copy()
    t0 = time.time()
    df = wr.athena.read_sql_query(sql=sql, **athena_kwargs(database=db))
    elapsed = time.time() - t0
    logger.info("Athena query executed in %.2fs, rows=%d db=%s", elapsed, len(df), db)
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


def fix_multiperiod_or_bug(sql: str) -> str:
    """
    Fix the SQL precedence bug where multi-period OR conditions escape other filters.

    BAD:  WHERE staff_group='GP' AND (year='A' AND month='B') OR (year='C' AND month='D')
    GOOD: WHERE staff_group='GP' AND ((year='A' AND month='B') OR (year='C' AND month='D'))

    Also handles:
    - Multiple OR branches (3+ periods)
    - CAST(year AS ...) variants
    - year=... OR year=... without month (simple year-only OR)

    This bug causes the OR branch to bypass all preceding WHERE filters.
    We detect any unparenthesized OR at the top level of the WHERE clause and wrap it.
    """
    original = sql

    # Pattern 1: AND (year=... AND month=...) OR (year=... AND month=...) [+more OR branches]
    pattern1 = re.compile(
        r"(AND\s*\(\s*(?:CAST\s*\()?\s*year\b[^)]*\)\s*AND\s*(?:CAST\s*\()?\s*month\b[^)]*\)"
        r"(?:\s*OR\s*\(\s*(?:CAST\s*\()?\s*year\b[^)]*\)\s*AND\s*(?:CAST\s*\()?\s*month\b[^)]*\))+)",
        re.IGNORECASE,
    )
    # Pattern 2: AND (year='...' AND month='...') OR (year='...' AND month='...')
    # (original simpler pattern kept as fallback)
    pattern2 = re.compile(
        r"(AND\s*\(\s*year\s*=\s*'[^']+'\s*AND\s*month\s*=\s*'[^']+'\s*\)"
        r"(?:\s*OR\s*\(\s*year\s*=\s*'[^']+'\s*AND\s*month\s*=\s*'[^']+'\s*\))+)",
        re.IGNORECASE,
    )
    # Pattern 3: AND year='...' OR year='...' (simple year-only, no parens)
    pattern3 = re.compile(
        r"(AND\s+year\s*=\s*'[^']+'\s+OR\s+year\s*=\s*'[^']+')",
        re.IGNORECASE,
    )

    def _wrap(m: re.Match) -> str:
        inner = m.group(1)
        inner_no_and = re.sub(r"^AND\s*", "", inner, count=1, flags=re.IGNORECASE).strip()
        return f"AND ({inner_no_and})"

    for pattern in [pattern1, pattern2, pattern3]:
        sql = pattern.sub(_wrap, sql)

    if sql != original:
        logger.warning("fix_multiperiod_or_bug | SQL OR precedence bug detected and fixed")
    return sql


# ── Case-sensitivity fix for categorical columns ──────────────────────────
# Athena string comparison is case-sensitive.  The LLM sometimes generates
# gender='male' instead of gender='Male', causing 0-row results.  This
# function normalises known categorical values to their correct case.

_CATEGORICAL_CANONICAL: Dict[str, Dict[str, str]] = {
    "gender": {"male": "Male", "female": "Female", "other/unknown": "Other/Unknown",
               "other": "Other/Unknown", "unknown": "Unknown"},
    "staff_group": {"gp": "GP", "nurses": "Nurses", "admin/non-clinical": "Admin/Non-Clinical",
                    "direct patient care": "Direct Patient Care", "dpc": "Direct Patient Care"},
    "country_qualification_group": {"uk": "UK", "eea": "EEA", "elsewhere": "Elsewhere",
                                     "uk/europe": "UK/Europe", "unknown": "Unknown"},
}


def fix_categorical_case(sql: str) -> str:
    """Fix case-sensitivity issues for known categorical column values in SQL.

    Matches patterns like: gender = 'male'  or  gender='Male'
    and normalises the value to the canonical case from the data.
    """
    original = sql

    for col, mapping in _CATEGORICAL_CANONICAL.items():
        # Match col = 'value' or col='value' (with optional whitespace)
        def _fix_value(m: re.Match) -> str:
            prefix = m.group(1)  # column = '
            raw_val = m.group(2)  # the value
            suffix = m.group(3)   # closing '
            canonical = mapping.get(raw_val.lower())
            if canonical:
                return f"{prefix}{canonical}{suffix}"
            return m.group(0)  # no match in mapping, leave unchanged

        # Pattern: col <optional whitespace> = <optional whitespace> 'value'
        pattern = re.compile(
            rf"({col}\s*=\s*')([^']+?)(')",
            re.IGNORECASE,
        )
        sql = pattern.sub(_fix_value, sql)

        # Also handle IN ('val1', 'val2') patterns
        # Match individual quoted values within IN clauses after the column name
        in_pattern = re.compile(
            rf"({col}\s+(?:NOT\s+)?IN\s*\()([^)]+)(\))",
            re.IGNORECASE,
        )
        def _fix_in_values(m: re.Match) -> str:
            prefix = m.group(1)
            values_str = m.group(2)
            suffix = m.group(3)
            def _replace_val(vm: re.Match) -> str:
                raw = vm.group(1)
                canonical = mapping.get(raw.lower())
                return f"'{canonical}'" if canonical else f"'{raw}'"
            fixed = re.sub(r"'([^']+?)'", _replace_val, values_str)
            return f"{prefix}{fixed}{suffix}"

        sql = in_pattern.sub(_fix_in_values, sql)

    if sql != original:
        logger.info("fix_categorical_case | normalised categorical values in SQL")
    return sql


# ── Hyphenated place-name normaliser ─────────────────────────────────────
# Users type "stoke on trent" but Athena has "Stoke-on-Trent".
# For LIKE patterns on name columns, replace spaces in the search value
# with a wildcard that matches both spaces and hyphens.
_NAME_COLS_PATTERN = re.compile(
    r"((?:icb_name|sub_icb_location_name|(?:comm_)?region_name|practice_name)"
    r"\s*\)*\s*(?:LIKE|like)\s*(?:LOWER\s*\()?'%?)([^']+?)(%?')",
    re.IGNORECASE,
)

def fix_hyphenated_names(sql: str) -> str:
    """Replace spaces in LIKE values for name columns with [-\\s] wildcard patterns."""
    original = sql
    # Known hyphenated place fragments (lowercase)
    _HYPHENATED = {
        "stoke on trent": "stoke-on-trent",
        "on trent": "-on-trent",
        "upon avon": "-upon-avon",
        "on sea": "-on-sea",
        "by sea": "-by-sea",
        "in furness": "-in-furness",
        "on thames": "-on-thames",
        "upon hull": "-upon-hull",
        "le spring": "-le-spring",
        "next the sea": "-next-the-sea",
    }
    def _fix_like(m: re.Match) -> str:
        prefix = m.group(1)
        val = m.group(2)
        suffix = m.group(3)
        val_lower = val.lower()
        for spaced, hyphenated in _HYPHENATED.items():
            if spaced in val_lower:
                val = re.sub(re.escape(spaced), hyphenated, val, flags=re.IGNORECASE)
        return f"{prefix}{val}{suffix}"
    sql = _NAME_COLS_PATTERN.sub(_fix_like, sql)
    if sql != original:
        logger.info("fix_hyphenated_names | normalised place names in SQL")
    return sql


# ── Geographic column fixer ─────────────────────────────────────────────
# The LLM sometimes picks the wrong geographic column (e.g., region_name for
# an ICB name). This detects that and rewrites to the correct column.
_VALID_REGIONS = {
    "south west", "midlands", "london", "south east",
    "north west", "east of england", "north east and yorkshire", "north east",
}

_REGION_ALIASES = {
    "north east": "north east and yorkshire",
    "north east region": "north east and yorkshire",
    "north east and yorkshire region": "north east and yorkshire",
}

_NATIONAL_SCOPE_HINTS = {
    "england", "all england", "whole england", "national", "nationally",
    "across england", "for england", "in england",
}

_GENERIC_GROUP_SCOPE_HINTS = {
    "each icb", "all icbs", "every icb", "by icb",
    "each region", "all regions", "every region", "by region",
    "each pcn", "all pcns", "every pcn", "by pcn",
}


def _normalise_geo_text(text: str) -> str:
    return re.sub(r"[\s\-]+", " ", (text or "").strip().lower())


def _is_national_scope_hint(hint: str) -> bool:
    hint_norm = _normalise_geo_text(hint)
    return hint_norm in _NATIONAL_SCOPE_HINTS


def _is_generic_group_scope_hint(hint: str) -> bool:
    hint_norm = _normalise_geo_text(hint)
    return hint_norm in _GENERIC_GROUP_SCOPE_HINTS


def _is_known_icb_fragment_hint(hint: str) -> bool:
    if _is_national_scope_hint(hint):
        return False
    hint_norm = _normalise_geo_text(hint)
    for frag in _KNOWN_ICB_FRAGMENTS:
        frag_norm = _normalise_geo_text(frag)
        if hint_norm == frag_norm or hint_norm in frag_norm or frag_norm in hint_norm:
            return True
    return False


def _city_to_icb_for_hint(hint: str) -> str:
    hint_norm = _normalise_geo_text(hint)
    for city, icb in _CITY_TO_ICB.items():
        if hint_norm == _normalise_geo_text(city):
            return icb
    return ""


def _region_column_for_table(table_hint: str) -> str:
    table = (table_hint or "").strip().lower()
    if not table:
        return "region_name"
    try:
        schema_cols = {col for col, _ in get_table_schema(table)}
    except Exception:
        schema_cols = set(_SCHEMA_OVERRIDE.get(table, []))
    if "region_name" in schema_cols:
        return "region_name"
    if "comm_region_name" in schema_cols:
        return "comm_region_name"
    return "region_name" if table == "practice_detailed" else "comm_region_name"

def _is_valid_region(val: str) -> bool:
    """Check if a LIKE value fragment matches a real NHS region."""
    val_clean = val.strip().lower().strip("%")
    if not val_clean:
        return False
    for region in _VALID_REGIONS:
        if val_clean in region or region in val_clean:
            return True
    return False

def fix_wrong_geo_column(sql: str) -> str:
    """If region_name is used with a non-region value, switch to icb_name.
    Also broadens single-column searches to OR across geo columns for
    ambiguous place names."""
    original = sql
    # Pattern matches: [LOWER(TRIM(]region_name[))] LIKE [LOWER(]'%value%'[)]
    # or:              region_name = 'value'
    pattern = re.compile(
        r"(LOWER\s*\(\s*TRIM\s*\(\s*)?"
        r"((?:comm_)?region_name)"
        r"(\s*\)\s*\))?"
        r"(\s*(?:LIKE|=)\s*)"
        r"(?:LOWER\s*\()?"
        r"('(?:%?)([^']+?)(?:%?)')"
        r"(?:\))?",
        re.IGNORECASE,
    )
    def _check_and_fix(m: re.Match) -> str:
        func_open = m.group(1) or ""
        col = m.group(2)
        func_close = m.group(3) or ""
        op = m.group(4)
        full_val = m.group(5)   # includes quotes and %
        val_inner = m.group(6)  # just the value text
        if _is_valid_region(val_inner):
            return m.group(0)  # It's a valid region, leave it
        # Not a region → rewrite to icb_name
        logger.info("fix_wrong_geo_column | '%s' is not a region, switching to icb_name", val_inner)
        new_col = "icb_name"
        if func_open:
            return f"{func_open}{new_col}{func_close}{op}{full_val}"
        return f"{new_col}{op}{full_val}"
    sql = pattern.sub(_check_and_fix, sql)
    if sql != original:
        logger.info("fix_wrong_geo_column | rewrote region_name to icb_name")
    return sql


# ── Known ICB name fragments — used to avoid broadening when value already matches ──
_KNOWN_ICB_FRAGMENTS = {
    "birmingham", "solihull", "greater manchester", "manchester", "cornwall",
    "isles of scilly", "stoke-on-trent", "staffordshire", "nottingham",
    "derby", "leicester", "coventry", "warwick", "hereford", "worcester",
    "shropshire", "telford", "black country", "west yorkshire", "south yorkshire",
    "north east", "north cumbria", "cheshire", "merseyside", "lancashire",
    "south lancashire", "surrey", "sussex", "kent", "medway", "hampshire",
    "isle of wight", "dorset", "bath", "somerset", "wiltshire", "swindon",
    "devon", "bristol", "north somerset", "south gloucestershire", "norfolk",
    "waveney", "suffolk", "north east essex", "mid and south essex",
    "hertfordshire", "west essex", "bedfordshire", "luton", "milton keynes",
    "cambridgeshire", "peterborough", "buckinghamshire", "oxfordshire",
    "berkshire", "frimley", "humber", "north yorkshire", "north lincolnshire",
    "north east lincolnshire", "lincolnshire", "london", "north west london",
    "north central london", "north east london", "south east london",
    "south west london",
}

# ── City-to-ICB mapping for cities whose names aren't in ICB names ──
_CITY_TO_ICB = {
    "stoke on trent": "staffordshire and stoke-on-trent",
    "stoke-on-trent": "staffordshire and stoke-on-trent",
    "leeds": "west yorkshire",
    "bradford": "west yorkshire",
    "wakefield": "west yorkshire",
    "huddersfield": "west yorkshire",
    "halifax": "west yorkshire",
    "liverpool": "cheshire and merseyside",
    "newcastle": "north east and north cumbria",
    "sunderland": "north east and north cumbria",
    "gateshead": "north east and north cumbria",
    "middlesbrough": "north east and north cumbria",
    "sheffield": "south yorkshire",
    "doncaster": "south yorkshire",
    "barnsley": "south yorkshire",
    "rotherham": "south yorkshire",
    "preston": "lancashire",
    "blackpool": "lancashire",
    "blackburn": "lancashire",
    "burnley": "lancashire",
    "bolton": "greater manchester",
    "oldham": "greater manchester",
    "rochdale": "greater manchester",
    "salford": "greater manchester",
    "stockport": "greater manchester",
    "tameside": "greater manchester",
    "trafford": "greater manchester",
    "wigan": "greater manchester",
    "bury": "greater manchester",
    "barking": "north east london",
    "dagenham": "north east london",
    "hackney": "north east london",
    "tower hamlets": "north east london",
    "newham": "north east london",
    "waltham forest": "north east london",
    "redbridge": "north east london",
    "havering": "north east london",
    "camden": "north central london",
    "islington": "north central london",
    "barnet": "north central london",
    "enfield": "north central london",
    "haringey": "north central london",
    "greenwich": "south east london",
    "lewisham": "south east london",
    "bromley": "south east london",
    "bexley": "south east london",
    "lambeth": "south east london",
    "southwark": "south east london",
    "wandsworth": "south west london",
    "richmond": "south west london",
    "kingston": "south west london",
    "croydon": "south west london",
    "merton": "south west london",
    "sutton": "south west london",
    "ealing": "north west london",
    "hounslow": "north west london",
    "hillingdon": "north west london",
    "brent": "north west london",
    "harrow": "north west london",
    "hammersmith": "north west london",
    "brighton": "sussex",
    "eastbourne": "sussex",
    "worthing": "sussex",
    "hastings": "sussex",
    "reading": "buckinghamshire",
    "slough": "frimley",
    "oxford": "buckinghamshire",
    "norwich": "norfolk",
    "ipswich": "suffolk",
    "cambridge": "cambridgeshire",
    "gloucester": "gloucestershire",
    "cheltenham": "gloucestershire",
    "exeter": "devon",
    "plymouth": "devon",
    "torbay": "devon",
    "york": "humber and north yorkshire",
    "scarborough": "humber and north yorkshire",
    "hull": "humber and north yorkshire",
    "grimsby": "north east lincolnshire",
    "scunthorpe": "north lincolnshire",
    "lincoln": "lincolnshire",
    "wolverhampton": "black country",
    "dudley": "black country",
    "sandwell": "black country",
    "walsall": "black country",
    "worcester": "herefordshire and worcestershire",
    "hereford": "herefordshire and worcestershire",
    "shrewsbury": "shropshire",
    "telford": "shropshire",
    "portsmouth": "hampshire",
    "southampton": "hampshire",
    "winchester": "hampshire",
    "canterbury": "kent",
    "maidstone": "kent",
    "medway": "kent",
    "bath": "bath",
    "taunton": "somerset",
    "swindon": "bath",
    "bristol": "bristol",
    "peterborough": "cambridgeshire",
    "luton": "bedfordshire",
    "watford": "hertfordshire",
    "stevenage": "hertfordshire",
    "chelmsford": "mid and south essex",
    "southend": "mid and south essex",
    "colchester": "north east essex",
    "basildon": "mid and south essex",
}


def fix_geo_broadening(sql: str) -> str:
    """Broaden icb_name LIKE '%city%' to also search sub_icb_name when the
    city isn't a known ICB fragment. Also rewrites prac_name geo searches
    to icb_name + sub_icb_name. Uses city-to-ICB mapping when available."""
    original = sql

    # ── Pattern 1: icb_name LIKE '%value%' ──
    # Use negative lookbehind to avoid matching "icb_name" inside "sub_icb_name"
    icb_pattern = re.compile(
        r"(LOWER\s*\(\s*(?:TRIM\s*\(\s*)?)?"
        r"(?<!sub_)(?<!sub_icb_location_)(icb_name)"
        r"(\s*\)\s*(?:\)\s*)?)?"
        r"(\s*(?:LIKE|=)\s*)"
        r"(?:LOWER\s*\()?"
        r"('(%?)([^']+?)(%?)')"
        r"(?:\))?",
        re.IGNORECASE,
    )

    def _broaden_icb(m: re.Match) -> str:
        func_open = m.group(1) or ""
        col = m.group(2)
        func_close = m.group(3) or ""
        op = m.group(4)
        full_val = m.group(5)
        pct_l = m.group(6)
        val_inner = m.group(7)
        pct_r = m.group(8)
        val_lower = val_inner.strip().lower()

        # Check if value already matches a known ICB fragment — no broadening needed
        if _is_known_icb_fragment_hint(val_lower):
            return m.group(0)

        # Check city-to-ICB mapping
        mapped_icb = _city_to_icb_for_hint(val_lower)
        if mapped_icb:
            # Rewrite to use mapped ICB name and also search sub_icb_name
            logger.info("fix_geo_broadening | '%s' mapped to ICB '%s' + sub_icb_name", val_inner, mapped_icb)
            icb_val = f"'{pct_l}{mapped_icb}{pct_r}'"
            sub_val = f"'{pct_l}{val_inner}{pct_r}'"
            if func_open:
                icb_expr = f"{func_open}icb_name{func_close}{op}{icb_val}"
                sub_expr = f"{func_open}sub_icb_name{func_close}{op}{sub_val}"
            else:
                icb_expr = f"icb_name{op}{icb_val}"
                sub_expr = f"sub_icb_name{op}{sub_val}"
            return f"({icb_expr} OR {sub_expr})"

        # No mapping — broaden to OR sub_icb_name
        logger.info("fix_geo_broadening | '%s' not a known ICB, broadening to sub_icb_name", val_inner)
        if func_open:
            icb_expr = f"{func_open}icb_name{func_close}{op}{full_val}"
            sub_expr = f"{func_open}sub_icb_name{func_close}{op}{full_val}"
        else:
            icb_expr = f"icb_name{op}{full_val}"
            sub_expr = f"sub_icb_name{op}{full_val}"
        return f"({icb_expr} OR {sub_expr})"

    sql = icb_pattern.sub(_broaden_icb, sql)

    # ── Pattern 2: prac_name LIKE '%city%' used as area search ──
    # When the query has no other geo filter (icb_name, region_name, sub_icb_name)
    # and prac_name LIKE contains a known city, rewrite to icb/sub_icb search
    # Check only WHERE clause for geo filters (SELECT may include geo columns as output)
    _where_clause = sql
    _where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
    if _where_match:
        _where_clause = sql[_where_match.start():]
    has_geo_filter = bool(re.search(
        r'\b(?:icb_name|sub_icb_name|(?:comm_)?region_name)\s*(?:\)|LIKE|=)',
        _where_clause, re.IGNORECASE))
    if not has_geo_filter:
        prac_pattern = re.compile(
            r"(LOWER\s*\(\s*(?:TRIM\s*\(\s*)?)?"
            r"(prac_name)"
            r"(\s*\)\s*(?:\)\s*)?)?"
            r"(\s*(?:LIKE|=)\s*)"
            r"(?:LOWER\s*\()?"
            r"('(%?)([^']+?)(%?)')"
            r"(?:\))?",
            re.IGNORECASE,
        )

        def _fix_prac_geo(m: re.Match) -> str:
            func_open = m.group(1) or ""
            func_close = m.group(3) or ""
            op = m.group(4)
            pct_l = m.group(6)
            val_inner = m.group(7)
            pct_r = m.group(8)
            val_lower = val_inner.strip().lower()

            # Only rewrite if the value matches a known city or ICB fragment
            mapped_icb = _city_to_icb_for_hint(val_lower)
            is_known_icb = _is_known_icb_fragment_hint(val_lower)

            if not mapped_icb and not is_known_icb:
                return m.group(0)  # Not a city — leave prac_name search as-is

            logger.info("fix_geo_broadening | prac_name '%s' is a geographic area, rewriting to icb/sub_icb", val_inner)
            if mapped_icb:
                icb_val = f"'{pct_l}{mapped_icb}{pct_r}'"
                sub_val = f"'{pct_l}{val_inner}{pct_r}'"
                if func_open:
                    return f"({func_open}icb_name{func_close}{op}{icb_val} OR {func_open}sub_icb_name{func_close}{op}{sub_val})"
                return f"(icb_name{op}{icb_val} OR sub_icb_name{op}{sub_val})"
            else:
                full_val = f"'{pct_l}{val_inner}{pct_r}'"
                if func_open:
                    return f"({func_open}icb_name{func_close}{op}{full_val} OR {func_open}sub_icb_name{func_close}{op}{full_val})"
                return f"(icb_name{op}{full_val} OR sub_icb_name{op}{full_val})"

        sql = prac_pattern.sub(_fix_prac_geo, sql)

    if sql != original:
        logger.info("fix_geo_broadening | broadened geo search in SQL")
    return sql


def fix_missing_follow_up_geo(sql: str, follow_ctx: Optional[Dict[str, Any]]) -> str:
    """Post-processing safety net: if follow_up_context has a geographic entity
    but the LLM-generated SQL has NO geo filter in the WHERE clause, inject one.
    This prevents the common failure mode where Nova Pro ignores the context annotation
    and returns national data for a geo-scoped follow-up."""
    if not follow_ctx or not sql:
        return sql
    entity_name = (follow_ctx.get("entity_name") or "").strip().lower()
    entity_type = follow_ctx.get("entity_type", "")
    entity_col = follow_ctx.get("entity_col", "")
    if not entity_name or entity_type not in ("icb", "sub_icb", "region", "practice", "city"):
        return sql

    # Check if SQL already contains a filter on any geo column
    where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
    if not where_match:
        return sql  # No WHERE clause — don't mess with it
    where_clause = sql[where_match.start():]

    geo_cols = ["icb_name", "sub_icb_name", "comm_region_name", "region_name", "prac_name"]
    has_geo_filter = any(gc in where_clause.lower() for gc in geo_cols)
    if has_geo_filter:
        return sql  # Already has a geo filter — trust LLM

    # No geo filter found — inject one based on entity context
    logger.info("fix_missing_follow_up_geo | injecting geo filter: %s = '%s' (type=%s)",
                entity_col, entity_name, entity_type)

    # Determine what table is being queried to pick the right column
    sql_lower = sql.lower()

    if entity_type == "city":
        # City queries need the dual ICB + sub_icb filter
        mapped_icb = _city_to_icb_for_hint(entity_name)
        if mapped_icb:
            geo_clause = (f"(LOWER(TRIM(icb_name)) LIKE '%{mapped_icb}%'"
                          f" OR LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%')")
        else:
            geo_clause = f"(LOWER(TRIM(icb_name)) LIKE '%{entity_name}%' OR LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%')"
    elif entity_type == "icb":
        geo_clause = f"LOWER(TRIM(icb_name)) LIKE '%{entity_name}%'"
    elif entity_type == "sub_icb":
        geo_clause = f"LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%'"
    elif entity_type == "region":
        region_col = _region_column_for_table("practice_detailed" if "practice_detailed" in sql_lower else "individual")
        geo_clause = f"LOWER(TRIM({region_col})) LIKE '%{entity_name}%'"
    elif entity_type == "practice":
        geo_clause = f"LOWER(TRIM(prac_name)) LIKE '%{entity_name}%'"
    else:
        return sql

    # Inject right after WHERE keyword (before existing conditions)
    sql = re.sub(
        r'(\bWHERE\b\s+)',
        rf'\1{geo_clause} AND ',
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    logger.info("fix_missing_follow_up_geo | injected: %s", geo_clause)
    return sql


def safe_markdown(df: Optional[pd.DataFrame], head: int = PREVIEW_HEAD_ROWS) -> str:  # [L5]
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
def _load_notes_from_path(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def load_domain_notes() -> str:
    return _load_notes_from_path(DOMAIN_NOTES_PATH)


WORKFORCE_DOMAIN_NOTES_TEXT = load_domain_notes()
APPOINTMENTS_DOMAIN_NOTES_TEXT = _load_notes_from_path(APPOINTMENTS_DOMAIN_NOTES_PATH)
DOMAIN_NOTES_TEXT = WORKFORCE_DOMAIN_NOTES_TEXT
WORKFORCE_CONFIG["domain_notes_text"] = WORKFORCE_DOMAIN_NOTES_TEXT
APPOINTMENTS_CONFIG["domain_notes_text"] = APPOINTMENTS_DOMAIN_NOTES_TEXT
KNOWLEDGE_RETRIEVERS: Dict[DatasetName, "NotesRetriever"] = {}


def _dataset_for_table(table: str) -> DatasetName:
    t = str(table or "").strip().lower()
    if t in set(APPOINTMENTS_CONFIG["allowed_tables"]):
        return "appointments"
    return "workforce"


_APPOINTMENTS_DATASET_SIGNALS = [
    "appointment", "appointments", "dna", "did not attend", "appointment mode",
    "appt_status", "appt mode", "hcp type", "consultation", "book and appt",
    "time between booking and appointment", "face-to-face", "telephone", "video", "home visit",
]
_WORKFORCE_DATASET_SIGNALS = [
    "gp workforce", "fte", "headcount", "patients-per-gp", "patients per gp",
    "nurse", "nurses", "dpc", "direct patient care", "admin", "non-clinical",
    "partner", "salaried", "trainee", "locum", "retainer", "age distribution", "gender breakdown",
]
_VAGUE_FOLLOWUP_PATTERNS = [
    r"^\s*what\s+about\b",
    r"^\s*and\s+",
    r"^\s*same\b",
    r"^\s*here\b",
    r"^\s*this\b",
    r"^\s*that\b",
    r"^\s*break\s+(?:this|that)\s+down\b",
    r"^\s*show\s+(?:this|that)\b",
    r"^\s*compare\s+(?:this|that)\b",
]
_DATASET_CLASSIFIER_SYSTEM = """You classify user questions for an NHS analytics chatbot into exactly one dataset.

Choose:
- workforce: GP workforce staffing questions like GP headcount, FTE, nurses, DPC staff, patients-per-GP, age, gender, partners, salaried, trainees.
- appointments: GP appointments activity questions like total appointments, DNA, appointment mode, HCP type, booking lead time, attended appointments.

Use the previous conversation context when provided. If the user asks a vague follow-up like "what about DNA rate?" and the previous dataset was appointments, keep it on appointments unless the new wording clearly switches domain.

Return only the structured dataset and confidence.
"""


def _dataset_from_follow_context(follow_ctx: Optional[Dict[str, Any]]) -> str:
    if not follow_ctx:
        return ""
    dataset = str(follow_ctx.get("dataset") or "").strip().lower()
    if dataset in {"workforce", "appointments"}:
        return dataset
    table = str(follow_ctx.get("table") or "").strip()
    if table:
        return _dataset_for_table(table)
    semantic_state = follow_ctx.get("semantic_state") or {}
    dataset = str(semantic_state.get("dataset") or "").strip().lower()
    if dataset in {"workforce", "appointments"}:
        return dataset
    return ""


def _has_any_signal(question: str, signals: List[str]) -> bool:
    q = (question or "").lower()
    return any(signal in q for signal in signals)


def _has_strong_workforce_signal(question: str) -> bool:
    q = (question or "").lower()
    if _has_any_signal(q, _WORKFORCE_DATASET_SIGNALS):
        return True
    return bool(
        re.search(r"\b(?:gps?|gp)\s+(?:fte|headcount|partners?|salaried|locums?|retainers?|trainees?)\b", q)
        or re.search(r"\bhow\s+many\s+gps?\b", q)
        or re.search(r"\bgps?\s+(?:are\s+there|nationally|in\s+)\b", q)
    )


def _has_strong_appointments_signal(question: str) -> bool:
    q = (question or "").lower()
    if _has_any_signal(q, _APPOINTMENTS_DATASET_SIGNALS):
        return True
    return bool(re.search(r"\bappointments?\s+(?:total|trend|nationally|in)\b", q))


def _looks_like_vague_followup(question: str) -> bool:
    q = (question or "").strip().lower()
    return any(re.search(pattern, q) for pattern in _VAGUE_FOLLOWUP_PATTERNS)


def _classify_dataset_with_llm(question: str, follow_ctx: Optional[Dict[str, Any]] = None) -> DatasetName:
    q = str(question or "").strip()
    if not q:
        return "workforce"
    prev_dataset = _dataset_from_follow_context(follow_ctx)
    prev_entity = ""
    if follow_ctx:
        prev_entity = str(follow_ctx.get("entity_name") or "").strip()

    prompt = f"""
QUESTION:
{q}

PREVIOUS DATASET:
{prev_dataset or "(none)"}

PREVIOUS ENTITY:
{prev_entity or "(none)"}
""".strip()

    try:
        llm = llm_client()
        structured_llm = llm.with_structured_output(DatasetClassification)
        result = structured_llm.invoke([
            SystemMessage(content=_DATASET_CLASSIFIER_SYSTEM),
            HumanMessage(content=prompt),
        ])
        dataset = cast(DatasetName, result.dataset)
        logger.info(
            "detect_dataset | llm fallback chose dataset=%s confidence=%s for %r",
            dataset,
            getattr(result, "confidence", "unknown"),
            q[:100],
        )
        return dataset
    except Exception as exc:
        logger.warning("detect_dataset | llm fallback failed (%s), defaulting conservatively", str(exc)[:120])
        return cast(DatasetName, prev_dataset or "workforce")


def _decide_dataset_route(question: str, follow_ctx: Optional[Dict[str, Any]] = None) -> RoutingDecision:
    q = str(question or "").strip()
    q_lower = q.lower()
    previous_dataset = _dataset_from_follow_context(follow_ctx)

    appointments_signal = _has_strong_appointments_signal(q_lower)
    workforce_signal = _has_strong_workforce_signal(q_lower)

    if previous_dataset and _looks_like_vague_followup(q_lower):
        if previous_dataset == "appointments" and not workforce_signal:
            return {
                "value": "appointments",
                "confidence": "high",
                "source": "follow_context_rule",
                "reason": "vague follow-up inherited from prior appointments context",
            }
        if previous_dataset == "workforce" and not appointments_signal:
            return {
                "value": "workforce",
                "confidence": "high",
                "source": "follow_context_rule",
                "reason": "vague follow-up inherited from prior workforce context",
            }

    if previous_dataset:
        if previous_dataset == "appointments" and appointments_signal and not workforce_signal:
            return {
                "value": "appointments",
                "confidence": "high",
                "source": "follow_context_rule",
                "reason": "appointments signal matches prior dataset context",
            }
        if previous_dataset == "workforce" and workforce_signal and not appointments_signal:
            return {
                "value": "workforce",
                "confidence": "high",
                "source": "follow_context_rule",
                "reason": "workforce signal matches prior dataset context",
            }

    if appointments_signal and not workforce_signal:
        return {
            "value": "appointments",
            "confidence": "high",
            "source": "deterministic_signal",
            "reason": "strong appointments-only signal detected",
        }
    if workforce_signal and not appointments_signal:
        return {
            "value": "workforce",
            "confidence": "high",
            "source": "deterministic_signal",
            "reason": "strong workforce-only signal detected",
        }

    if previous_dataset and not appointments_signal and not workforce_signal:
        return {
            "value": previous_dataset,
            "confidence": "medium",
            "source": "follow_context_rule",
            "reason": "no fresh dataset signal, inheriting prior dataset context",
        }

    if any(keyword in q_lower for keyword in APPOINTMENTS_CONFIG["keywords"]):
        return {
            "value": "appointments",
            "confidence": "medium",
            "source": "keyword_rule",
            "reason": "appointments keyword fallback matched",
        }

    dataset = _classify_dataset_with_llm(q, follow_ctx)
    return {
        "value": dataset,
        "confidence": "medium" if previous_dataset else "low",
        "source": "llm_fallback",
        "reason": "deterministic routing was uncertain, used semantic classifier fallback",
    }


def detect_dataset(question: str, follow_ctx: Optional[Dict[str, Any]] = None) -> DatasetName:
    return cast(DatasetName, _decide_dataset_route(question, follow_ctx).get("value") or "workforce")


def retrieve_dataset_domain_notes(question: str, dataset: DatasetName,
                                  max_chars: int = DOMAIN_NOTES_MAX_CHARS,
                                  max_chunks: int = 8) -> str:
    notes_text = str(_dataset_config(dataset).get("domain_notes_text") or "")
    if not notes_text.strip():
        return ""
    q = question.lower()
    chunks = re.split(r"\n(?=## )", notes_text)
    scored: List[Tuple[int, str]] = []
    keywords = set(re.findall(r"[a-zA-Z]{3,}", q))
    for c in chunks:
        c_low = c.lower()
        score = sum(1 for k in keywords if k in c_low)
        if score > 0:
            scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = "\n\n".join([c for _, c in scored[:max_chunks]])
    if not top:
        top = notes_text[:max_chars]
    return top[:max_chars]


def retrieve_domain_notes(question: str, max_chars: int = DOMAIN_NOTES_MAX_CHARS,
                          max_chunks: int = 8) -> str:
    """Retrieve the most relevant domain-notes sections for a question.
    For knowledge-only questions, call with higher max_chunks/max_chars for richer context.
    """
    return retrieve_dataset_domain_notes(question, "workforce", max_chars=max_chars, max_chunks=max_chunks)


class NotesRetriever:
    """Lightweight local RAG over dataset domain notes."""

    def __init__(self, dataset: DatasetName, notes_text: str) -> None:
        self.dataset = dataset
        self.notes_text = str(notes_text or "")
        self._chunks = self._chunk_notes(self.notes_text)
        self._embeddings: List[List[float]] = []
        self._ready = False

    @staticmethod
    def _chunk_notes(notes_text: str, max_chars: int = 1400) -> List[str]:
        if not notes_text.strip():
            return []
        sections = re.split(r"\n(?=## )", notes_text)
        chunks: List[str] = []
        for section in sections:
            section = section.strip()
            if not section:
                continue
            if len(section) <= max_chars:
                chunks.append(section)
                continue
            paragraphs = [p.strip() for p in re.split(r"\n\s*\n", section) if p.strip()]
            current = ""
            for para in paragraphs:
                candidate = f"{current}\n\n{para}".strip() if current else para
                if len(candidate) <= max_chars:
                    current = candidate
                else:
                    if current:
                        chunks.append(current)
                    current = para
            if current:
                chunks.append(current)
        return chunks

    @property
    def ready(self) -> bool:
        return self._ready and bool(self._embeddings)

    def _ensure_ready(self) -> None:
        if self.ready or not self._chunks:
            return
        try:
            logger.info("Embedding %d knowledge chunks for %s notes...", len(self._chunks), self.dataset)
            self._embeddings = [get_embedding_vector(chunk) for chunk in self._chunks]
            self._ready = True
            logger.info("Knowledge retriever ready for %s with %d chunks", self.dataset, len(self._chunks))
        except Exception as exc:
            logger.warning("Knowledge retriever init failed for %s: %s", self.dataset, exc)
            self._embeddings = []
            self._ready = False

    def retrieve(self, question: str, top_k: int = 4, max_chars: int = 6000) -> str:
        if not self.notes_text.strip():
            return ""
        self._ensure_ready()
        if not self.ready:
            return retrieve_dataset_domain_notes(question, self.dataset, max_chars=max_chars, max_chunks=top_k)

        try:
            q_vec = get_embedding_vector(question)
            scored: List[Tuple[float, str]] = []
            for emb, chunk in zip(self._embeddings, self._chunks):
                scored.append((cosine_similarity(q_vec, emb), chunk))
            scored.sort(key=lambda item: item[0], reverse=True)
            selected: List[str] = []
            total_chars = 0
            for _, chunk in scored[: max(top_k * 2, top_k)]:
                if chunk in selected:
                    continue
                if total_chars + len(chunk) > max_chars and selected:
                    break
                selected.append(chunk)
                total_chars += len(chunk)
                if len(selected) >= top_k:
                    break
            context = "\n\n".join(selected).strip()
            return context[:max_chars] if context else retrieve_dataset_domain_notes(
                question, self.dataset, max_chars=max_chars, max_chunks=top_k
            )
        except Exception as exc:
            logger.warning("Knowledge retriever lookup failed for %s: %s", self.dataset, exc)
            return retrieve_dataset_domain_notes(question, self.dataset, max_chars=max_chars, max_chunks=top_k)


KNOWLEDGE_RETRIEVERS = {
    "workforce": NotesRetriever("workforce", WORKFORCE_DOMAIN_NOTES_TEXT),
    "appointments": NotesRetriever("appointments", APPOINTMENTS_DOMAIN_NOTES_TEXT),
}


# =============================================================================
# Schema overrides from CSVs
# =============================================================================
def _load_cols_from_csv(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    df = pd.read_csv(path, nrows=0)  # [L7] Only read headers, not entire file
    return [c.strip() for c in df.columns.tolist() if str(c).strip()]


def load_schema_overrides() -> Dict[str, List[str]]:
    overrides: Dict[str, List[str]] = {}
    overrides["individual"] = _load_cols_from_csv(INDIVIDUAL_COLS_CSV)
    overrides["practice_detailed"] = _load_cols_from_csv(PRACTICE_DETAILED_COLS_CSV)
    overrides["practice_high"] = _load_cols_from_csv(PRACTICE_HIGH_COLS_CSV)
    overrides["pcn_subicb"] = _load_cols_from_csv(APPOINTMENTS_PCN_SUBICB_COLS_CSV)
    overrides["practice"] = _load_cols_from_csv(APPOINTMENTS_PRACTICE_COLS_CSV)
    return overrides


_SCHEMA_OVERRIDE = load_schema_overrides()


# =============================================================================
# Introspection tools
# =============================================================================
def get_table_schema(table: str, database: Optional[str] = None) -> List[Tuple[str, str]]:
    table = table.lower()
    allowed = set(ALLOWED_TABLES) | set(APPOINTMENTS_CONFIG["allowed_tables"])
    if table not in allowed:
        raise ValueError("Unknown table requested.")
    db = database or (_dataset_config(_dataset_for_table(table)).get("athena_database") or ATHENA_DATABASE)
    cache_key = f"{db}::{table}"
    cached = _SCHEMA_CACHE.get(cache_key)
    if cached:
        return cached[1]
    try:
        sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{db}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        df = run_athena_df(sql, database=db)
        schema = list(zip(df["column_name"].tolist(), df["data_type"].tolist()))
        _SCHEMA_CACHE.set(cache_key, schema)
        logger.info("Schema loaded for %s (%s): %d columns", table, db, len(schema))
        return schema
    except Exception as e:
        logger.warning("Schema introspection failed for %s: %s — using override", table, e)
        cols = _SCHEMA_OVERRIDE.get(table, [])
        schema = [(c, "unknown") for c in cols]
        _SCHEMA_CACHE.set(cache_key, schema)
        return schema


def _validate_column_name(column: str) -> str:
    """[C4] Validate SQL column name to prevent injection. Returns sanitised name or raises."""
    col = column.strip()
    if not col or not COLUMN_NAME_PATTERN.match(col):
        raise ValueError(f"Invalid column name: {col!r}")
    return col


def _validate_table_name(table: str) -> str:
    """[C4] Validate table name against allow-list."""
    t = table.lower().strip()
    allowed = set(ALLOWED_TABLES) | set(APPOINTMENTS_CONFIG["allowed_tables"])
    if t not in allowed:
        raise ValueError(f"Unknown table: {t!r}")
    return t


def get_latest_year_month(table: str, database: Optional[str] = None) -> Dict[str, Any]:
    table = _validate_table_name(table)  # [C4] Validate table
    db = database or (_dataset_config(_dataset_for_table(table)).get("athena_database") or ATHENA_DATABASE)
    cache_key = f"{db}::{table}"
    cached = _LATEST_CACHE.get(cache_key)
    if cached:
        return cached[1]
    sql = f"""
    SELECT year, month
    FROM {table}
    WHERE year IS NOT NULL AND month IS NOT NULL
    ORDER BY CAST(year AS INTEGER) DESC, CAST(month AS INTEGER) DESC
    LIMIT 1
    """
    df = run_athena_df(sql, database=db)
    if df.empty:
        latest = {"year": None, "month": None}
    else:
        latest = {"year": str(df.iloc[0]["year"]), "month": str(df.iloc[0]["month"])}
    _LATEST_CACHE.set(cache_key, latest)
    logger.info("Latest year/month for %s (%s): %s/%s", table, db, latest.get("year"), latest.get("month"))
    return latest


def list_distinct_values(
    table: str,
    column: str,
    where_sql: Optional[str] = None,
    limit: int = 200,
    database: Optional[str] = None,
) -> List[str]:
    table = _validate_table_name(table)    # [C4]
    column = _validate_column_name(column)  # [C4] Prevent SQL injection via column name
    db = database or ATHENA_DATABASE
    key = f"{db}|{table}|{column}|{where_sql or ''}|{limit}"
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
    df = run_athena_df(sql, database=db)
    values = [str(x) for x in df["v"].dropna().tolist()]
    _DISTINCT_CACHE.set(key, values)
    return values


# =============================================================================
# Fuzzy entity matching (improvement #3)
# =============================================================================
def fuzzy_match(query: str, candidates: List[str], threshold: float = FUZZY_MATCH_THRESHOLD, top_n: int = 5) -> List[Tuple[str, float]]:  # [L5]
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
    table = _validate_table_name(table)       # [C4]
    name_col = _validate_column_name(name_col)  # [C4] Prevent SQL injection via column name
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
) -> List[Dict[str, Any]]:
    """
    First try DB LIKE search. If empty, load distinct values and fuzzy match.
    Returns scored candidates so downstream prompts and confidence grading can
    distinguish exact-ish matches from weaker fuzzy suggestions.
    """
    user_text = (user_text or "").strip()
    user_text_low = user_text.lower()

    db_results = search_best_name_match(table, name_col, user_text, year, month, limit=10)
    if db_results:
        scored_results: List[Dict[str, Any]] = []
        for candidate in db_results:
            cand_low = candidate.lower().strip()
            if cand_low == user_text_low:
                score = 1.0
                match_type = "exact"
            elif user_text_low and (user_text_low in cand_low or cand_low in user_text_low):
                score = 0.97
                match_type = "db_like"
            else:
                ratio = difflib.SequenceMatcher(None, user_text_low, cand_low).ratio()
                q_words = set(user_text_low.split())
                c_words = set(cand_low.split())
                word_overlap = len(q_words & c_words) / max(len(q_words), 1)
                score = max(0.88, ratio, word_overlap)
                match_type = "db_like"
            scored_results.append({
                "value": candidate,
                "score": round(min(score, 1.0), 4),
                "match_type": match_type,
            })
        scored_results.sort(key=lambda x: x.get("score", 0), reverse=True)
        return scored_results

    # Fuzzy fallback: get all distinct values for this column
    where_sql = None
    if year and month:
        where_sql = f"year = '{year}' AND month = '{month}'"
    all_values = list_distinct_values(table, name_col, where_sql=where_sql, limit=500)
    matches = fuzzy_match(user_text, all_values, threshold=0.45, top_n=8)
    return [
        {"value": candidate, "score": round(score, 4), "match_type": "fuzzy"}
        for candidate, score in matches
    ]


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

    # "last N years" / "past N years" / "over the last N years" / "in the last N years"
    m = re.search(r"(?:last|past|previous)\s+(\d+)\s+years?", q)
    if m:
        n_years = int(m.group(1))
        start_y = ly - n_years + 1  # "last 3 years" from 2025 = 2023,2024,2025
        return {
            "start_year": str(start_y), "start_month": "01",
            "end_year": latest_year, "end_month": latest_month,
            "description": f"Last {n_years} years ({start_y}-{ly})",
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

    # "N years ago" / "this year vs N years ago" / "versus N years ago"
    m = re.search(r"(\d+)\s+years?\s+ago", q)
    if m:
        n_years = int(m.group(1))
        past_y = ly - n_years
        # If the question is a comparison ("vs", "versus", "compared", "compare")
        if re.search(r"\bvs\.?\b|versus|compar", q):
            return {
                "start_year": str(past_y), "start_month": latest_month,
                "end_year": latest_year, "end_month": latest_month,
                "description": f"Comparison: {past_y} vs {ly}",
                "compare_years": [str(past_y), latest_year],
            }
        else:
            # Just "N years ago" without comparison
            return {
                "start_year": str(past_y), "start_month": "01",
                "end_year": str(past_y), "end_month": "12",
                "description": f"{n_years} years ago ({past_y})",
            }

    # "last year" (not "vs last year" which is caught above)
    if re.search(r"\blast\s+year\b", q) and not re.search(r"vs|compared|over", q):
        return {
            "start_year": str(ly - 1), "start_month": "01",
            "end_year": str(ly - 1), "end_month": "12",
            "description": f"Last year ({ly - 1})",
        }

    # "this year vs last year" / "current vs previous"
    if re.search(r"this\s+year\s+(?:vs\.?|versus|compared?\s+to)\s+(?:last|previous)\s+year", q):
        return {
            "start_year": str(ly - 1), "start_month": latest_month,
            "end_year": latest_year, "end_month": latest_month,
            "description": f"This year vs last year ({ly - 1} vs {ly})",
            "compare_years": [str(ly - 1), latest_year],
        }

    return None


# =============================================================================
# Deterministic hard intents (expanded)
# =============================================================================
def _is_explicit_definition_question(question: str) -> bool:
    q_lower = (question or "").strip().lower()
    if not re.search(r"\b(?:what\s+does|what\s+is|define|definition\s+of|meaning\s+of)\b", q_lower):
        return False
    if any(term in q_lower for term in [
        "dna rate", "rate of dna", "percentage", "proportion", "count", "number of",
        "how many", "trend", "latest", "current", "nationally", "by icb", "by region", "in nhs ",
    ]):
        return False
    definition_terms = {
        "dna", "did not attend", "appointment mode", "hcp type", "health care professional",
        "national category", "time between book and appt", "time from booking",
        "fte", "headcount", "arrs",
    }
    return any(term in q_lower for term in definition_terms)


def detect_hard_intent(question: str) -> Optional[str]:
    q = question.lower().strip()
    if _is_explicit_definition_question(question):
        return None
    practice_code = extract_practice_code(question)
    practice_hint = _specific_entity_hint(question, "practice")
    has_specific_practice_target = bool(practice_code or practice_hint)
    generic_practice_scope = _has_generic_scope_reference(question, "practice")
    is_practice_ranking_query = any(term in q for term in [
        "which practice", "top practice", "top practices", "highest practice",
        "lowest practice", "most patients", "highest patients", "largest practice",
        "biggest practice", "most gps", "highest patients per gp", "worst patients per gp",
    ])

    if (
        any(term in q for term in ["salaried", "partner"])
        and any(term in q for term in ["trend", "over the years", "over years", "over time"])
        and "gp" in q
    ):
        return "partner_salaried_trend"

    if (
        any(term in q for term in ["trainee", "trainees", "training", "registrar", "registrars"])
        and any(term in q for term in ["how many", "number of", "count", "currently", "current", "workforce", "nationally", "across england", "in england"])
    ):
        return "trainee_gp_count"

    if (
        any(term in q for term in ["retire", "retirement", "eligible for retirement"])
        and "gp" in q
        and any(term in q for term in ["proportion", "percentage", "how many", "number of", "count"])
    ):
        return "retirement_eligible"

    if practice_code:
        if ("icb" in q) and ("where" in q or "located" in q or "which" in q):
            return "practice_to_icb_lookup"
        if any(term in q for term in ["patients per gp", "patient to gp", "gp ratio", "patients per doctor"]):
            return "patients_per_gp"
        if any(term in q for term in ["staff", "workforce", "all staff", "breakdown"]):
            return "practice_staff_breakdown"
        if any(term in q for term in ["patients", "patient", "list size", "registered"]):
            return "practice_patient_count"
        if "gp" in q:
            return "practice_gp_count"

    # practice -> ICB lookup
    if (
        has_specific_practice_target
        and ("icb" in q)
        and ("practice" in q or "practise" in q or "prac" in q)
        and ("where" in q or "located" in q or "which" in q)
    ):
        return "practice_to_icb_lookup"

    # how many GP at a practice
    if (
        has_specific_practice_target
        and not generic_practice_scope
        and ("how many" in q or "number of" in q or "no. of" in q)
        and ("gp" in q)
        and ("practice" in q or "practise" in q or "prac" in q)
    ):
        return "practice_gp_count"

    # shorthand: "how many gp in keele" — but NOT if it mentions a region, ICB, city, or country
    _REGION_KEYWORDS = {"london", "midlands", "north east", "north west", "south east",
                        "south west", "east of england", "england", "region", "icb",
                        "nationally", "national", "country", "uk", "scotland", "wales",
                        "northern ireland"}
    soft_gp_count_exclusions = [
        "practice", "practices", "trainee", "trainees", "nurse", "nurses", "admin", "clerical",
        "dpc", "pharmacist", "pharmacists", "paramedic", "paramedics",
        "physiotherapist", "physiotherapists", "partner", "partners",
        "salaried", "locum", "retainer",
    ]
    if (
        ("how many" in q or "number of" in q)
        and ("gp" in q)
        and len(q.split()) <= 8
        and not any(term in q for term in soft_gp_count_exclusions)
    ):
        if not any(rk in q for rk in _REGION_KEYWORDS):
            return "practice_gp_count_soft"

    # total patients at a practice
    if (
        has_specific_practice_target
        and not generic_practice_scope
        and not is_practice_ranking_query
        and ("patients" in q or "patient" in q or "list size" in q or "registered" in q)
        and ("practice" in q or "practise" in q or "prac" in q)
    ):
        return "practice_patient_count"

    # patients per GP ratio
    if ("patients per gp" in q or "patients-per-gp" in q or "patient to gp" in q or "gp ratio" in q or "patients per doctor" in q):
        return "patients_per_gp"

    # staff breakdown at a practice
    if (
        has_specific_practice_target
        and not generic_practice_scope
        and not is_practice_ranking_query
        and ("staff" in q or "workforce" in q or "all staff" in q or "breakdown" in q)
        and ("practice" in q or "practise" in q or "prac" in q)
    ):
        return "practice_staff_breakdown"

    # GP numbers grouped by PCN (national or filtered by ICB/region)
    if "pcn" in q and ("group" in q or "by pcn" in q or "per pcn" in q or "each pcn" in q or "all pcn" in q):
        return "pcn_gp_count"

    return None


def _extract_topic_keywords(question: str) -> set:
    """Extract meaningful topic keywords from a question for topic-change detection."""
    q = question.lower().strip()
    # Remove common stop words, question words, AND domain-generic words that appear
    # in nearly every GP workforce question (they don't help distinguish topics)
    stop_words = {
        # General English stop words
        "what", "how", "show", "give", "tell", "me", "the", "a", "an", "is", "are",
        "was", "were", "in", "at", "for", "by", "of", "and", "or", "to", "this",
        "that", "it", "its", "their", "there", "can", "you", "do", "does", "did",
        "i", "we", "my", "with", "from", "on", "be", "been", "has", "have", "had",
        "will", "would", "could", "should", "may", "might", "please", "also", "now",
        "vs", "versus", "compared", "comparison", "between", "many", "much", "per",
        "total", "number", "count", "average", "proportion", "percentage", "ratio",
        "same", "similar", "different", "side", "both", "all", "each", "every",
        "data", "table", "query", "practice", "practices", "year", "years", "month",
        "months", "latest", "current", "last", "next", "ago", "since", "want", "dont",
        "actually", "actual", "type", "types", "show", "give", "list",
        # Domain-generic words (appear in most GP workforce questions)
        "england", "nhs", "nationally", "national", "workforce", "gps", "general",
        "digital", "across", "currently", "available", "based", "over", "change",
        "changed", "time", "trend", "trends", "breakdown", "break", "down",
    }
    words = re.findall(r"[a-z]+", q)
    return {w for w in words if w not in stop_words and len(w) > 2}


def _is_topic_change(new_question: str, session_id: str) -> bool:
    """
    Detect if the new question is about a fundamentally different topic
    compared to the previous turn(s). Returns True if topic has changed.
    """
    history = MEMORY.get_history(session_id)
    if not history:
        return False

    # Get last user question
    last_user_q = ""
    for turn in reversed(history):
        if turn["role"] == "user":
            last_user_q = turn["content"]
            break
    if not last_user_q:
        return False

    new_kw = _extract_topic_keywords(new_question)
    old_kw = _extract_topic_keywords(last_user_q)

    if not new_kw or not old_kw:
        return False

    overlap = new_kw & old_kw
    # If there's very little overlap, it's a topic change
    # Use Jaccard similarity: overlap / union
    union = new_kw | old_kw
    similarity = len(overlap) / len(union) if union else 0

    # Also check for specific topic-defining words that signal a clear shift
    # NOTE: fte/headcount are measurement types, not topics — they appear alongside any topic
    # so they are NOT included as a topic group
    topic_defining_groups = [
        {"trainee", "trainees", "training", "registrar", "registrars"},
        {"retire", "retirement", "retiring", "pension"},
        {"nurse", "nurses", "nursing"},
        {"admin", "administrative", "administration"},
        {"locum", "locums"},
        {"patient", "patients"},
        {"gender", "male", "female"},
        {"age", "ages", "young", "old", "elderly"},
        {"joiner", "joiners", "leaver", "leavers", "turnover", "attrition"},
        {"salary", "pay", "earnings", "income"},
        {"region", "regional", "geography", "geographic"},
        {"qualified", "qualification", "eligible"},
    ]

    new_topics = set()
    old_topics = set()
    for i, group in enumerate(topic_defining_groups):
        if new_kw & group:
            new_topics.add(i)
        if old_kw & group:
            old_topics.add(i)

    # If the new question has very few meaningful keywords, it's likely a follow-up / reformatting
    # request (e.g. "show me side by side", "now by region") — NOT a topic change
    if len(new_kw) <= 2:
        return False

    # If the topic-defining groups are completely different, it's a topic change
    # Both must have identifiable topic groups for this to trigger
    if new_topics and old_topics and not (new_topics & old_topics):
        logger.info("_is_topic_change | topic shift detected: old_groups=%s new_groups=%s (sim=%.2f)",
                    old_topics, new_topics, similarity)
        return True

    # Low overall similarity with different topic groups = topic change
    # But only if the new question has enough substance (5+ keywords) to judge
    if similarity < 0.15 and new_topics and old_topics and new_topics != old_topics and len(new_kw) >= 3:
        logger.info("_is_topic_change | low similarity %.2f, treating as topic change", similarity)
        return True

    return False


def is_follow_up(question: str) -> bool:
    """Detect if the question is a follow-up that refers to a previous entity."""
    q = question.lower().strip()

    # ---- Self-contained question check ----
    # A question that has its own clear subject (not a pronoun) is NOT a follow-up
    # even if it starts with "What" or "How"
    self_contained_patterns = [
        # "What proportion of qualified GPs..." — new full question
        r"^what\s+(?:proportion|percentage|number|fraction|share)\s+of\s+(?!the\s+same|this|that|them)",
        # "How many <specific noun> are/were/..." — new full question
        r"^how\s+many\s+\w+\s+(?:are|were|is|was|do|did|have|had|will|would)\b",
        # "What is the <specific topic>..." — new full question (but NOT "what is the same")
        r"^what\s+is\s+the\s+(?!same\b)(?:average|total|proportion|distribution|breakdown|trend|ratio|number)\b",
        # "How does/do <noun>..." — new standalone question
        # EXCEPTIONS: "how has THIS/THAT/IT changed" = follow-up pronoun reference
        #             "how has the ratio/trend/count changed" = generic follow-up
        r"^how\s+(?:does|do|did|has|have|will|would|can|could)\s+(?!(?:the\s+)?(?:ratio|trend|rate|count|total|number|average|proportion|breakdown)\b)(?!(?:this|that|it)\b)\w{3,}",
        # "Which <noun>..." — new question
        r"^which\s+\w{3,}\s+(?:are|is|were|was|have|has|do|does|can|could)\b",
        # "Are there..." / "Is there..." — new question
        r"^(?:are|is)\s+there\s+",
    ]
    # ---- Pronoun / deictic reference check (overrides self-contained) ----
    # If question contains "this practice", "that ICB", "this region", etc.
    # it MUST be a follow-up regardless of word count — it references a prior entity.
    pronoun_entity_refs = [
        r"\b(?:this|that)\s+(?:practice|surgery|icb|region|area|pcn|sub.?icb|place)\b",
        r"\b(?:my|our)\s+practice\b",
        r"\b(?:at|in|for|of)\s+(?:this|that)\s+(?:practice|surgery|icb|region|area|pcn)\b",
        r"\b(?:at|in|for|of)\s+(?:my|our)\s+practice\b",
        r"\b(?:the\s+same)\s+(?:practice|surgery|icb|region|area|pcn)\b",
    ]
    if any(re.search(p, q) for p in pronoun_entity_refs):
        return True

    if any(re.search(p, q) for p in self_contained_patterns):
        # Additional check: if question has 8+ words, it's likely self-contained
        if len(q.split()) >= 8:
            return False

    # ---- Explicit follow-up signals ----
    # These are strong signals that the user is referring back to a previous result
    strong_follow_up_signals = [
        r"\b(the same|this practice|that practice|my practice|our practice|this icb|that icb|same one|them)\b",
        r"\b(?:for|of)\s+(this|that|it|them)\b",
        r"^(and |also |now )",
        r"^(?:same|the\s+same|same\s+thing)(?:\s+but)?\s+(?:for|in)\b",
        r"^same\s+but\s+(?:for|in)\b",
        r"^(show me|can you show|give me)\s+(?:the\s+)?(?:same|that|this|it|a comparison|side by side)",
        r"^show\s+(?:the\s+)?trend\b",
        r"\bside\s+by\s+side\b",
        r"^(patients per|ratio|trend|gender|age|breakdown|demographic)\b",
        r"^break\s+(?:this|that|it)\s+down\b",
        r"^split\s+(?:this|that|it)\s+by\b",
        r"^i\s+meant\b",
        r"\b(its|their)\s+\w",
        # "how has this/the <metric> changed" — refers to previously discussed entity
        r"^how\s+(has|have|did|does|is|was|were)\s+(this|that|it|the)\b",
        # "what is the X ratio/count/total?" without specifying an entity — follow-up if context exists
        r"^what\s+is\s+the\s+(?:[\w\-/]+\s+)+(ratio|rate|count|total|average|proportion)\??$",
        # "what is the patients-per-GP ratio" style
        r"\bpatients[\s\-]per[\s\-](?:gp|doctor|practitioner)\b",
        # Correction / refinement: user is changing the previous request
        r"\b(i\s+)?don'?t\s+want\b.*\bi\s+want\b",
        r"^(not\s+that|no\s*,?\s*(i\s+)?(want|need|mean))",
        r"^(instead|rather)\b",
        r"^(actually|but)\s+(i\s+)?(want|need|show|give)",
        r"^actually\b.*\b(show|give|use|want|need|switch|headcount|fte)\b",
        r"\b(?:instead\s+of|rather\s+than|not)\s+(?:fte|headcount|head\s+count)\b",
        r"\b(?:headcount|fte)\s+instead\b",
    ]
    # Special handling for "compare": only a follow-up if it does NOT specify
    # the entities being compared (e.g. "compare GPs vs nurses" is a NEW question,
    # but "compare them" or "compare the same" is a follow-up)
    if re.search(r"^compare\b", q):
        # If the compare question has specific entities (vs/with/and/to + noun), it's NEW
        if re.search(r"\b(vs|versus|with|against|to|and)\s+\w{3,}", q) and len(q.split()) >= 5:
            pass  # Skip — this is a self-contained comparison, not a follow-up
        else:
            # Short "compare them" / "compare the same" → treat as follow-up
            strong_follow_up_signals.append(r"^compare\b")
    if any(re.search(p, q) for p in strong_follow_up_signals):
        words = q.split()
        if len(words) <= 12:
            # Check if there's a capitalized proper noun suggesting a new entity
            original_words = question.strip().split()
            has_new_entity = False
            skip_words = {"What", "How", "Show", "Give", "Tell", "The", "And", "Also",
                          "Now", "Is", "Are", "Was", "Were", "In", "At", "For", "By",
                          "GP", "GPs", "FTE", "ICB", "NHS", "PCN", "DPC", "HCP", "DNA",
                          "Break", "Split", "Compare", "Instead", "Actually", "Not",
                          "Rather", "Same", "This", "That", "But", "Yes", "No"}
            for w in original_words:
                w_clean = w.rstrip(",.;:!?'\"")  # strip trailing punctuation
                if w_clean and w_clean[0].isupper() and w_clean not in skip_words and len(w_clean) > 2:
                    has_new_entity = True
                    break
            if not has_new_entity:
                return True

    # ---- Weak signals: short generic questions ----
    # "what about X" / "how about X" are ALWAYS follow-ups (asking about another slice)
    if re.search(r"^(what about|how about)\b", q):
        words = q.split()
        if len(words) <= 6:
            return True

    # "What is the ratio/total/trend?" — short generic follow-up (with or without qualifier)
    if re.search(r"^what\s+is\s+the\s+(?:[\w\-/]+\s+)*(ratio|total|trend|breakdown|split|difference|rate|count)\??$", q):
        return True

    # "show/give/tell me X" — follow-up ONLY if it doesn't name a specific data subject
    if re.search(r"^(show|give|tell)\b", q):
        words = q.split()
        if len(words) <= 6:
            # Exception: if the question names a SPECIFIC workforce role/data subject,
            # it's a fresh new query, not a follow-up.
            # E.g. "Show me locum GPs data" → new query about locums
            # E.g. "Show me trainee GPs data" → new query about trainees
            # But "Show me the same data" → follow-up (pronoun/same reference)
            # But "show me this" → follow-up (pronoun)
            specific_data_subjects = {
                "locum", "locums", "trainee", "trainees", "training",
                "nurse", "nurses", "nursing", "admin", "administrative",
                "partner", "partners", "senior", "registrar", "registrars",
                "pharmacist", "pharmacists", "paramedic", "paramedics",
                "practitioner", "practitioners", "dispenser", "dispensers",
                "physiotherapist", "physiotherapists", "retired", "retirement",
                "salaried", "contractor", "qualified",
            }
            q_words = set(re.findall(r"[a-z]+", q))
            if q_words & specific_data_subjects:
                pass  # Has specific subject → NOT a follow-up, skip
            else:
                return True

    return False


def resolve_follow_up_context(
    question: str,
    session_id: str,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    If question is a follow-up, enrich it with the entity from the previous turn.
    Returns (enriched_question, previous_entity_context_or_None).

    CRITICAL: If the topic has changed (e.g. trainees -> retirement), we do NOT
    carry forward the previous context, even if the question looks like a follow-up.
    """
    if not session_id:
        return question, None

    prev_ctx = MEMORY.get_entity_context(session_id)
    logger.info("resolve_follow_up_context | session=%s prev_ctx=%s", session_id, prev_ctx)
    if not prev_ctx:
        logger.info("resolve_follow_up_context | no prev_ctx for session=%s, returning early", session_id)
        return question, None

    # Check follow-up FIRST — if it's a strong follow-up signal, skip topic change detection
    # (e.g. "break this down by gender" is NOT a topic change, even though "gender" is a new topic word)
    _is_definite_follow_up = is_follow_up(question)
    q_low = (question or "").lower().strip()
    if not _is_definite_follow_up and prev_ctx:
        generic_contextual_patterns = [
            r"^(?:which|what)\s+(region|icb|pcn)\s+has\s+(?:the\s+)?(?:most|highest|largest|fewest|lowest|least)\b",
            r"^(?:what\s+about|how\s+about|and)\s+(?:the\s+)?(?:most|highest|largest|fewest|lowest|least)\b",
            r"^(?:has|how\s+has)\s+(?:the\s+)?(?:total|count|number|practice count|headcount|fte|it|that|this)\b.*\bchanged\b",
            r"^compare(?:\s+(?:this|that|it))?\s+(?:with|to|against)\s+.+$",
        ]
        if any(re.search(pattern, q_low) for pattern in generic_contextual_patterns):
            _is_definite_follow_up = True

    # ---- Topic change detection ----
    # Only check topic change if the question is NOT a definite follow-up
    # (follow-ups like "break this down by X" reference the previous result, not a new topic)
    if not _is_definite_follow_up and _is_topic_change(question, session_id):
        logger.info("resolve_follow_up_context | topic change detected, clearing entity context")
        MEMORY.save_entity_context(session_id, {})
        return question, None

    if not _is_definite_follow_up:
        return question, None

    active_ctx = prev_ctx
    if _should_prefer_parent_scope(question, prev_ctx):
        parent_scope_ctx = _parent_scope_context(prev_ctx)
        if parent_scope_ctx:
            active_ctx = parent_scope_ctx

    # We have previous context and this is a follow-up on the same topic
    entity_name = active_ctx.get("entity_name", "")
    entity_type = active_ctx.get("entity_type", "")  # "practice", "icb", etc.
    table = active_ctx.get("table", "")

    # Build optional metric suffix for context annotation
    _metric_suffix = ""
    prev_metric = active_ctx.get("previous_metric", "")
    if prev_metric:
        _metric_desc_map = {
            "headcount": "headcount (COUNT DISTINCT unique_identifier)",
            "fte": "FTE (SUM(fte))",
            "patients_per_gp": "patients-per-GP ratio",
        }
        _metric_suffix = f", metric = {_metric_desc_map.get(prev_metric, prev_metric)} — use the SAME metric type"
    _staff_suffix = ""
    prev_staff_group = str(active_ctx.get("previous_staff_group") or "").strip()
    if prev_staff_group:
        _staff_suffix = f", staff_group = {prev_staff_group} — keep the SAME staff group unless the user changes it"
    _grain_suffix = ""
    prev_grain = active_ctx.get("previous_grain", "")
    if prev_grain:
        _grain_suffix = f", grain = {prev_grain}"

    rewritten_q, stripped_ctx = _rewrite_same_but_follow_up(question, active_ctx)
    if rewritten_q:
        context_bits = []
        if table:
            context_bits.append(f"table = {table}")
        if prev_grain:
            context_bits.append(f"grain = {prev_grain}")
        if prev_metric:
            metric_desc_map = {
                "headcount": "headcount",
                "fte": "fte",
                "patients_per_gp": "patients_per_gp",
            }
            context_bits.append(f"metric = {metric_desc_map.get(prev_metric, prev_metric)}")
        if prev_staff_group:
            context_bits.append(f"staff_group = {prev_staff_group}")
        enriched = f"{rewritten_q} (context: {', '.join(context_bits)})" if context_bits else rewritten_q
        return enriched, stripped_ctx

    if entity_name:
        # Replace "this/that ICB", "this/that practice" etc. with the actual entity name
        # so the SQL generator uses the correct name instead of interpreting "this" literally
        q_enriched = re.sub(
            r"\b(?:this|that|my|our)\s+(?:icb|practice|surgery|region|area|pcn|sub[\s-]?icb|place)\b",
            entity_name,
            question,
            flags=re.IGNORECASE,
        )
        # For city-type entities, include both city and ICB info in context
        if entity_type == "city":
            mapped_icb = prev_ctx.get("mapped_icb", _city_to_icb_for_hint(entity_name.lower()))
            city_ctx = f"city = {entity_name}, icb = {mapped_icb}" if mapped_icb else f"city = {entity_name}"
            if q_enriched != question:
                enriched = f"{q_enriched} (context: {city_ctx}, table = {table}{_grain_suffix}{_metric_suffix}{_staff_suffix})"
            else:
                enriched = f"{question} (context: {city_ctx}, table = {table}{_grain_suffix}{_metric_suffix}{_staff_suffix})"
        elif q_enriched != question:
            enriched = f"{q_enriched} (context: {entity_type} = {entity_name}, table = {table}{_grain_suffix}{_metric_suffix}{_staff_suffix})"
        else:
            enriched = f"{question} (context: {entity_type} = {entity_name}, table = {table}{_grain_suffix}{_metric_suffix}{_staff_suffix})"
        return enriched, active_ctx

    # National-level follow-up: no specific entity, but still carry table/scope context
    # This handles corrections like "i dont want FTE, i want headcount" after a national query
    if table:
        enriched = f"{question} (context: table = {table}, scope = national{_grain_suffix}{_metric_suffix}{_staff_suffix})"
        return enriched, active_ctx

    return question, None


_FOLLOW_UP_SUBJECT_ONLY = {
    "age", "ages", "gender", "male", "female", "trend", "trends", "ratio", "rate",
    "count", "counts", "headcount", "headcounts", "fte", "breakdown", "split",
    "distribution", "nurse", "nurses", "gp", "gps", "doctor", "doctors", "admin",
    "administrative", "dpc", "locum", "locums", "trainee", "trainees", "registrar",
    "registrars", "partner", "partners", "salaried", "pharmacist", "pharmacists",
    "paramedic", "paramedics", "physiotherapist", "physiotherapists", "practice",
    "practices", "icb", "icbs", "region", "regions", "pcn", "pcns",
    "dna", "dna rate", "appointment mode", "appointment modes", "mode breakdown",
    "hcp", "hcp type", "hcp types", "booking lead time", "lead time",
    "time between booking and appointment",
}


def _follow_up_metric_phrase(prev_ctx: Dict[str, Any]) -> str:
    view = str(prev_ctx.get("previous_view") or "").lower()
    metric = str(prev_ctx.get("previous_metric") or "").lower()
    staff_group = str(prev_ctx.get("previous_staff_group") or "").lower()
    aggregation = str(prev_ctx.get("previous_aggregation") or "").lower()

    if view == "age_distribution":
        return "the GP age distribution"
    if view == "gender_breakdown":
        return "the gender breakdown"
    if view == "practice_staff_breakdown":
        return "the practice staff breakdown"
    if view == "sub_icb_breakdown":
        return "the sub-ICB breakdown"
    if view == "appointment_mode_breakdown":
        return "the appointment mode breakdown"
    if view == "hcp_type_breakdown":
        return "appointments by HCP type"
    if view == "booking_lead_time_breakdown":
        return "appointments by time between booking and appointment"
    if view == "appointments_trend":
        return "the appointments trend"

    if metric == "patients_per_gp":
        return "the patients-per-GP ratio"
    if metric == "dna_rate":
        return "the DNA rate"
    if metric == "appointments_total":
        if staff_group:
            return f"{staff_group} appointments"
        return "total appointments"

    subject = {
        "gp": "GP",
        "nurses": "nurse",
        "admin": "admin staff",
        "dpc": "direct patient care staff",
    }.get(staff_group, "")

    if subject:
        if metric == "fte":
            return f"{subject} FTE"
        if aggregation == "average":
            return f"average {subject} headcount"
        return f"{subject} headcount"

    if metric == "fte":
        return "FTE"
    if metric == "headcount":
        return "headcount"
    return "the same measure"


def _looks_like_subject_only_follow_up(target: str) -> bool:
    target_low = _clean_entity_hint(target).lower()
    if not target_low:
        return True
    if target_low in {"this", "that", "it", "them", "same", "the same"}:
        return True
    if re.fullmatch(r"(?:the\s+)?(?:most|highest|largest|fewest|lowest|least|best|worst)", target_low):
        return True
    if target_low in _FOLLOW_UP_SUBJECT_ONLY:
        return True
    if re.fullmatch(r"(?:by|for|in)\s+(?:age|gender|region|icb|pcn|practice)s?", target_low):
        return True
    return False


def _rewrite_same_but_follow_up(question: str, prev_ctx: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Rephrase casual follow-ups like "what about London?" into a more explicit query
    while carrying forward metric/table context but dropping the previous entity.
    """
    q = (question or "").strip()
    if not q:
        return "", None

    patterns = [
        r"^(?:what\s+about|how\s+about)\s+(.+?)[\?!.]?$",
        r"^(?:same|the\s+same|same\s+thing)(?:\s+but)?\s+(?:for|in)\s+(.+?)[\?!.]?$",
        r"^(?:same\s+but)\s+(?:for|in)\s+(.+?)[\?!.]?$",
        r"^(?:and|now)\s+(?:for|in)\s+(.+?)[\?!.]?$",
        r"^compare\s+(?:with|to|against)\s+(.+?)[\?!.]?$",
    ]
    target = ""
    for pattern in patterns:
        m = re.match(pattern, q, flags=re.IGNORECASE)
        if m:
            target = _clean_entity_hint(m.group(1))
            target = re.sub(r"^(?:in|for|within|across)\s+", "", target, flags=re.IGNORECASE).strip()
            break
    if not target or _looks_like_subject_only_follow_up(target):
        placeholder_m = re.match(
            r"^(?:what\s+about|how\s+about|and)\s+(?:the\s+)?(most|highest|largest|fewest|lowest|least)\b",
            q,
            flags=re.IGNORECASE,
        )
        if placeholder_m:
            stripped_ctx = {
                k: v for k, v in prev_ctx.items()
                if k not in {"entity_name", "entity_type", "entity_col", "mapped_icb"}
            }
            return q, stripped_ctx
        return "", None

    compare_m = re.match(r"^compare\s+(?:with|to|against)\s+(.+?)[\?!.]?$", q, flags=re.IGNORECASE)
    if compare_m:
        metric_phrase = _follow_up_metric_phrase(prev_ctx)
        current_entity = str(prev_ctx.get("entity_name") or "").strip()
        if current_entity:
            rewritten = f"Compare {metric_phrase} in {current_entity} with {target}"
        else:
            rewritten = f"Compare {metric_phrase} with {target}"
        return rewritten, prev_ctx

    metric_phrase = _follow_up_metric_phrase(prev_ctx)
    rewritten = f"Show {metric_phrase} for {target}"
    stripped_ctx = {
        k: v for k, v in prev_ctx.items()
        if k not in {"entity_name", "entity_type", "entity_col", "mapped_icb"}
    }
    return rewritten, stripped_ctx


def _parent_scope_context(prev_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    parent_name = str(prev_ctx.get("parent_scope_entity_name") or "").strip()
    parent_type = str(prev_ctx.get("parent_scope_entity_type") or "").strip()
    if not parent_name or not parent_type:
        return None

    scope_ctx = dict(prev_ctx)
    scope_ctx["entity_name"] = parent_name
    scope_ctx["entity_type"] = parent_type
    scope_ctx["entity_col"] = prev_ctx.get("parent_scope_entity_col") or prev_ctx.get("entity_col")
    if prev_ctx.get("parent_scope_mapped_icb"):
        scope_ctx["mapped_icb"] = prev_ctx.get("parent_scope_mapped_icb")
    if prev_ctx.get("parent_scope_grain"):
        scope_ctx["previous_grain"] = prev_ctx.get("parent_scope_grain")
    return scope_ctx


def _should_prefer_parent_scope(question: str, prev_ctx: Dict[str, Any]) -> bool:
    if str(prev_ctx.get("entity_type") or "") != "practice":
        return False
    if not prev_ctx.get("parent_scope_entity_name"):
        return False

    q = (question or "").lower().strip()
    if not q:
        return False

    explicit_practice_terms = [
        "this practice", "that practice", "my practice", "our practice",
        "at this practice", "at that practice", "at my practice", "at our practice",
        "their ", "its ", "registered at", "staff breakdown", "arrs staff",
        "patient list", "list size", "practice code",
    ]
    if any(term in q for term in explicit_practice_terms):
        return False

    # Benchmark or "here" follow-ups should stay anchored to the current
    # practice. Promoting them to the parent ICB causes wrong comparisons like
    # benchmarking the whole ICB instead of the practice the user just asked
    # about.
    if _parse_benchmark_request(q):
        return False
    if any(term in q for term in [
        " national average", "national avg", "average practice", "average icb",
        "average region", "average pcn", "pcn average", "national total",
        "ratio here", "here?", " here ",
    ]):
        return False

    ranking_terms = ["top", "highest", "lowest", "most", "least", "fewest", "largest", "biggest", "worst", "best"]
    regroup_patterns = [
        r"\bby\s+(?:icb|region|sub[\s-]?icb|pcn|practice)\b",
        r"\bbreak\s+(?:this|that|it)\s+down\b",
        r"\bin total\b",
    ]
    broad_metric_terms = [
        "patients per gp", "patient to gp", "gp ratio", "ratio", "headcount", "fte",
        "gaps", "count", "numbers", "breakdown", "trend",
    ]

    if any(re.search(pattern, q) for pattern in regroup_patterns):
        return True
    if re.search(r"\bcompare\s+(?:with|to|against)\b", q):
        target_geo = _extract_geo_scope_hint(question)
        if target_geo and not _is_national_scope_hint(target_geo):
            return True
        if any(token in q for token in [" by region", " by icb", " by pcn", " by sub-icb", " by sub icb"]):
            return True
        return False
    if any(term in q for term in ranking_terms) and any(term in q for term in broad_metric_terms):
        return True
    return False


def extract_entity_hint(question: str) -> str:
    """Extract the entity name (practice/ICB/etc) from the question."""
    q = question.strip()

    def _clean_candidate(raw_hint: str, trailing_pattern: str) -> str:
        hint = (raw_hint or "").strip()
        hint = re.sub(
            r"\s+(?:in\s+)?(?:the\s+)?(?:latest|current|last)\s+(?:month|year|quarter)\b.*$",
            "",
            hint,
            flags=re.IGNORECASE,
        ).strip()
        hint = re.sub(
            r"\s+(?:over|during)\s+the\s+(?:past|last)\s+\d+\s+(?:month|months|year|years|quarter|quarters)\b.*$",
            "",
            hint,
            flags=re.IGNORECASE,
        ).strip()
        hint = re.sub(trailing_pattern, "", hint, flags=re.IGNORECASE).strip()
        hint = _clean_entity_hint(hint)
        if len(hint) <= 2:
            return ""
        if _looks_like_time_only_hint(hint):
            return ""
        return hint

    patterns = [
        (r"\bat\s+(.+?)(?:\?|$)", r"\b(icb|pcn)s?\b.*$"),
        (r"\bfor\s+(.+?)(?:\?|$)", r"\b(icb|pcn)s?\b.*$"),
        (r"\bin\s+(.+?)(?:\?|$)", r"\b(practi[cs]e|icb|pcn|region|area)s?\b.*$"),
    ]
    for pattern, trailing_pattern in patterns:
        m = re.search(pattern, q, flags=re.IGNORECASE)
        if not m:
            continue
        hint = _clean_candidate(m.group(1), trailing_pattern)
        if hint:
            return hint

    # try before "practice"
    m = re.search(r"(.+?)\bpracti[cs]e\b", q, flags=re.IGNORECASE)
    if m:
        hint = m.group(1).strip()
        hint = re.sub(r"^(how many|number of|no\.? of|what|which|the|is|are|gp|gps|staff|work|works|at|in)\s+", "", hint, flags=re.IGNORECASE).strip()
        if len(hint) > 2:
            return hint
    return q


_PRACTICE_CODE_RE = re.compile(r"\b([A-Za-z]\d{5})\b")


def extract_practice_code(question: str) -> str:
    """Extract a six-character practice code like P82001 if present."""
    m = _PRACTICE_CODE_RE.search(question or "")
    return m.group(1).upper() if m else ""


def _clean_entity_hint(hint: str) -> str:
    hint = re.sub(r"^[\s,;:()'\"-]+|[\s,;:()'\"-]+$", "", hint or "").strip()
    hint = re.sub(r"\s+", " ", hint)
    return hint


def _strip_trailing_filler(text: str) -> str:
    text = text or ""
    text = re.sub(r"\b(?:mate|please|pls|thanks|thank you|cheers)\b.*$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\b(?:have|has|with|where|by|over|under|more than|less than)\b.*$", "", text, flags=re.IGNORECASE).strip()
    return _clean_entity_hint(text)


def _extract_geo_scope_hint(question: str) -> str:
    q = question or ""
    m = re.search(r"\b(?:in|within|across)\s+(.+?)(?:\?|$)", q, flags=re.IGNORECASE)
    if not m:
        return ""
    hint = _strip_trailing_filler(m.group(1))
    return re.sub(r"^the\s+", "", hint, flags=re.IGNORECASE).strip()


def _looks_like_time_only_hint(hint: str) -> bool:
    h = (hint or "").strip().lower()
    if not h:
        return False
    time_phrases = [
        "latest month", "latest year", "current month", "current year",
        "past year", "last year", "last month", "last 12 months",
        "over time", "month by month", "quarter by quarter",
    ]
    if h in time_phrases:
        return True
    if re.fullmatch(r"(?:the\s+)?(?:latest|current|last)\s+(?:month|year|quarter)", h):
        return True
    if re.fullmatch(r"(?:the\s+)?past\s+\d+\s+(?:month|months|year|years|quarter|quarters)", h):
        return True
    return False


def _extract_appointments_geo_hint(question: str) -> str:
    hint = _extract_geo_scope_hint(question)
    if not hint:
        return ""
    hint = re.sub(
        r"\s+(?:in\s+)?(?:the\s+)?(?:latest|current|last)\s+(?:month|year|quarter)\b.*$",
        "",
        hint,
        flags=re.IGNORECASE,
    )
    hint = re.sub(
        r"\s+(?:over|during)\s+the\s+(?:past|last)\s+\d+\s+(?:month|months|year|years|quarter|quarters)\b.*$",
        "",
        hint,
        flags=re.IGNORECASE,
    )
    hint = hint.strip(" ,.")
    if _looks_like_time_only_hint(hint):
        return ""
    if _is_national_scope_hint(hint):
        return ""
    return hint


def _appointments_hcp_filter(question: str) -> Optional[str]:
    q = (question or "").lower()
    gp_patterns = (
        r"\bappointments?\s+(?:with|by|from)\s+(?:a\s+)?gp\b",
        r"\b(?:with|by|from|seen by|handled by)\s+(?:a\s+)?gp\b",
        r"\bappointments?\s+with\s+(?:a\s+)?general practitioner\b",
        r"\bgp\s+hcp\b",
    )
    if any(re.search(pattern, q) for pattern in gp_patterns):
        return "GP"
    nurse_patterns = (
        r"\bnurse appointments?\b",
        r"\bappointments?\s+(?:with|by|from)\s+nurses?\b",
        r"\b(?:with|by|from|seen by)\s+nurses?\b",
    )
    if any(re.search(pattern, q) for pattern in nurse_patterns):
        return "Nurse"
    pharmacist_patterns = (
        r"\bpharmacist appointments?\b",
        r"\bappointments?\s+(?:with|by|from)\s+pharmacists?\b",
        r"\b(?:with|by|from|seen by)\s+pharmacists?\b",
    )
    if any(re.search(pattern, q) for pattern in pharmacist_patterns):
        return "Other Practice staff"
    return None


def _normalise_region_name(hint: str) -> str:
    hint_low = _clean_entity_hint(hint).lower()
    if hint_low in _REGION_ALIASES:
        return _REGION_ALIASES[hint_low]
    for region in sorted(_VALID_REGIONS, key=len, reverse=True):
        if hint_low == region or hint_low in region or region in hint_low:
            return _REGION_ALIASES.get(region, region)
    return hint_low


def _age_55_plus_filter() -> str:
    return "age_band IN ('55-59', '60-64', '65 and over')"


def _age_60_plus_filter() -> str:
    return "age_band IN ('60-64', '65 and over')"


def _geo_filter_from_hint_text(question: str, hint: str, table_hint: str = "individual") -> Optional[str]:
    """
    Build a geography filter from raw question text for deterministic overrides.
    Supports regions, ICBs, and common city names.
    """
    raw_hint = _clean_entity_hint(hint)
    if not raw_hint:
        return None
    if _is_national_scope_hint(raw_hint):
        return None
    if _is_generic_group_scope_hint(raw_hint):
        return None

    hint_low = raw_hint.lower()
    q_low = (question or "").lower()
    region_col = _region_column_for_table(table_hint)

    mapped_icb = _city_to_icb_for_hint(hint_low)
    explicit_icb_request = (
        hint_low.startswith("nhs ")
        or hint_low.endswith(" icb")
        or " icb" in hint_low
        or " icb" in q_low
        or "integrated care" in q_low
    )
    if mapped_icb and not explicit_icb_request:
        mapped_icb = sanitise_entity_input(mapped_icb, "icb_name")
        city_hint = sanitise_entity_input(hint_low, "sub_icb_name")
        if table_hint == "practice_detailed" and "practice" in q_low:
            return f"LOWER(TRIM(sub_icb_name)) LIKE LOWER('%{city_hint}%')"
        return (
            f"(LOWER(TRIM(icb_name)) LIKE LOWER('%{mapped_icb}%') "
            f"OR LOWER(TRIM(sub_icb_name)) LIKE LOWER('%{city_hint}%'))"
        )

    if explicit_icb_request or _looks_like_specific_icb_hint(question, raw_hint):
        icb_name = sanitise_entity_input(raw_hint, "icb_name")
        return f"LOWER(TRIM(icb_name)) LIKE LOWER('%{icb_name}%')"

    if "region" in q_low or _is_valid_region(raw_hint):
        region_name = _normalise_region_name(raw_hint)
        region_name = sanitise_entity_input(region_name, "region_name")
        return f"LOWER(TRIM({region_col})) = '{region_name.lower()}'"

    return None


def _has_generic_scope_reference(question: str, entity_type: str) -> bool:
    q = (question or "").lower().strip()
    if not q:
        return False

    generic_patterns = {
        "practice": [
            r"\bgeneral practice\b",
            r"\bprimary care\b",
            r"\bprimary care workforce\b",
            r"\bprimary care setting\b",
            r"\bgp practices?\b",
            r"\ball practices?\b",
            r"\beach practice\b",
            r"\bper practice\b",
            r"\bpractices\s+with\b",
            r"\bpractices\s+by\b",
            r"\bpractices\s+in\b",
            r"\bpractices?\s+are there\b",
            r"\bpractice count\b",
            r"\bpractice size\b",
            r"\btop\s+(?:\d+\s+)?practices?\b",
            r"\bhighest\s+practice\b",
            r"\blowest\s+practice\b",
            r"\baverage .* per practice\b",
            r"\bwork in general practice\b",
            r"\bin general practice\b",
            r"\bmy practice\b",
            r"\bour practice\b",
        ],
        "icb": [
            r"\bby icb\b",
            r"\beach icb\b",
            r"\bper icb\b",
            r"\ball icbs?\b",
            r"\bacross all icbs?\b",
            r"\bwhich icb\b",
            r"\bcompare .* icbs?\b",
        ],
        "sub_icb": [
            r"\bby sub[\s-]?icb\b",
            r"\beach sub[\s-]?icb\b",
            r"\bper sub[\s-]?icb\b",
            r"\ball sub[\s-]?icbs?\b",
        ],
        "region": [
            r"\bby region\b",
            r"\beach region\b",
            r"\bper region\b",
            r"\ball regions\b",
            r"\bacross all regions\b",
            r"\bwhich region\b",
            r"\bcompare .* regions?\b",
        ],
        "pcn": [
            r"\bby pcn\b",
            r"\beach pcn\b",
            r"\bper pcn\b",
            r"\ball pcns?\b",
            r"\bacross all pcns?\b",
            r"\bwhich pcn\b",
            r"\bcompare .* pcns?\b",
        ],
    }
    return any(re.search(p, q) for p in generic_patterns.get(entity_type, []))


def _looks_like_named_practice_hint(hint: str) -> bool:
    hint_low = _clean_entity_hint(hint).lower()
    if not hint_low:
        return False
    if hint_low in {"practice", "my practice", "our practice", "this practice", "that practice"}:
        return False
    named_practice_tokens = (
        " practice",
        " surgery",
        " clinic",
        " medical centre",
        " medical center",
        " health centre",
        " health center",
        " medical practice",
        " group practice",
        " partnership",
    )
    return any(token in hint_low for token in named_practice_tokens)


def _specific_entity_hint(question: str, entity_type: str) -> str:
    """
    Return a cleaned specific entity hint, or empty string when the question is
    talking about a generic scope like "all practices" / "by ICB".
    """
    q = (question or "").strip()
    if not q:
        return ""

    if entity_type == "practice":
        code = extract_practice_code(q)
        if code:
            return code

    hint = _clean_entity_hint(extract_entity_hint(q))
    if not hint or hint.lower() == q.lower():
        return ""

    if _has_generic_scope_reference(q, entity_type):
        if entity_type != "practice" or not _looks_like_named_practice_hint(hint):
            return ""

    hint = re.sub(r"^the\s+", "", hint, flags=re.IGNORECASE).strip()

    # Ranking phrases ("lowest 5", "top 10", "bottom 3", etc.) are never
    # entity names — reject them for any entity_type.
    if re.fullmatch(
        r"(?:top|bottom|lowest|highest|fewest|most|least|worst|best|largest|biggest|smallest)(?:\s+\d+)?",
        hint.lower(),
    ):
        return ""

    hint_low = hint.lower()
    q_low = q.lower()
    generic_hints = {
        "practice": {
            "practice", "practices", "general", "general practice", "gp practice", "gp practices", "all",
            "primary care", "primary care workforce", "primary care setting",
            "which", "what", "who", "where", "top", "highest", "lowest", "most", "least",
            "largest", "biggest", "smallest", "best", "worst",
            "this", "that", "these", "those", "they", "them", "it", "there", "total",
            "my practice", "our practice", "this practice", "that practice", "here",
        },
        "icb": {"icb", "icbs", "all icbs", "integrated care board"},
        "sub_icb": {"sub icb", "sub-icb", "all sub icbs", "all sub-icbs"},
        "region": {"region", "regions", "all regions"},
        "pcn": {"pcn", "pcns", "all pcns", "primary care network"},
    }
    if hint_low in generic_hints.get(entity_type, set()):
        return ""

    if entity_type == "practice":
        practice_like_tokens = ["practice", "surgery", "medical centre", "health centre", "clinic"]
        noise_patterns = [
            r"\btrend\b",
            r"\bover (?:the )?years?\b",
            r"\bover time\b",
            r"\bcompare\b",
            r"\bcomparison\b",
            r"\bbreakdown\b",
            r"\baverage\b",
            r"\bratio\b",
            r"\bheadcount\b",
            r"\bfte\b",
            r"\bpartner\b",
            r"\bsalaried\b",
            r"\blocum\b",
            r"\bretainer\b",
            r"\bnurses?\b",
            r"\badmin\b",
            r"\bdpc\b",
            r"\bpharmacists?\b",
            r"\bparamedics?\b",
            r"\bphysiotherapists?\b",
            r"\btrainees?\b",
            r"\bregistrars?\b",
            r"\bgender\b",
            r"\bage\b",
            r"\bmonth\b",
            r"\byear\b",
        ]
        if hint_low in _CITY_TO_ICB or hint_low in _VALID_REGIONS:
            return ""
        if _looks_like_specific_icb_hint(question, hint):
            return ""
        if " vs " in f" {hint_low} " or " versus " in f" {hint_low} ":
            return ""
        if any(re.search(pattern, hint_low, re.IGNORECASE) for pattern in noise_patterns):
            return ""
        if len(hint.split()) > 5 and not any(token in hint_low for token in practice_like_tokens):
            return ""

    if _is_national_scope_hint(hint_low):
        return ""

    # Directional NHS regions such as "North East" or "South West" should not
    # be treated as specific ICB names unless the user explicitly asks for an ICB.
    if (
        entity_type == "icb"
        and hint_low in _VALID_REGIONS
        and not any(token in q_low for token in [" icb", "nhs ", "integrated care"])
    ):
        return ""

    if entity_type == "sub_icb" and hint_low in _VALID_REGIONS:
        return ""

    return hint


_AMBIGUOUS_REGION_HINTS = {
    "north east", "north west", "south east", "south west",
    "midlands", "london", "east of england",
}


def _looks_like_specific_icb_hint(question: str, hint: str) -> bool:
    """
    Decide whether a hint should be treated as an ICB rather than a region.
    This avoids clarifying "North East region" as if it were an ambiguous ICB.
    """
    q_low = (question or "").lower()
    hint_low = (hint or "").strip().lower()
    if not hint_low:
        return False
    if _is_national_scope_hint(hint_low):
        return False

    if any(token in q_low for token in [" icb", "nhs ", "integrated care"]):
        return True

    if "region" in q_low and hint_low in _VALID_REGIONS:
        return False

    if hint_low in _AMBIGUOUS_REGION_HINTS:
        return False

    return _is_known_icb_fragment_hint(hint_low)


_SECONDARY_CARE_TERMS = {
    "hospital", "consultant", "consultants", "secondary care", "acute", "ward",
    "surgery department", "ed", "a&e", "inpatient", "outpatient",
}


def _question_has_explicit_metric(question: str) -> bool:
    q = (question or "").lower()
    metric_terms = [
        "headcount", "head count", "fte", "patients per gp", "patients-per-gp",
        "patient to gp", "gp ratio", "ratio", "trend", "age", "gender",
        "nurse", "nurses", "admin", "dpc", "trainee", "registrar",
        "pharmacist", "pharmacists", "physiotherapist", "physiotherapists",
        "paramedic", "paramedics", "clinical staff", "staff group", "staff groups",
    ]
    if any(term in q for term in metric_terms):
        return True
    role_terms = [
        "gp", "gps", "nurse", "nurses", "pharmacist", "pharmacists",
        "physiotherapist", "physiotherapists", "paramedic", "paramedics",
        "admin", "clinical staff", "staff groups", "workforce",
    ]
    measure_terms = ["number", "numbers", "count", "total", "size", "workforce size", "split", "breakdown", "proportion", "percentage"]
    return any(role in q for role in role_terms) and any(measure in q for measure in measure_terms)


def _rescue_or_clarify_question(question: str) -> Dict[str, Any]:
    """
    Recover from vague but common GP workforce phrasing before the planner sees it.
    Returns optional keys:
      - rewritten_question
      - clarification_question
      - notes
    """
    original = (question or "").strip()
    q_low = original.lower()
    if not original:
        return {}

    rescued_question = original

    typo_normalisations = {
        r"\bhw\b": "how",
        r"\bnurss\b": "nurses",
        r"\bnurss\b": "nurses",
        r"\bnursee?s\b": "nurses",
    }
    for pattern, replacement in typo_normalisations.items():
        rescued_question = re.sub(pattern, replacement, rescued_question, flags=re.IGNORECASE)
    q_low = rescued_question.lower()

    if "gp" not in q_low and "general practitioner" not in q_low and not any(term in q_low for term in _SECONDARY_CARE_TERMS):
        if re.search(r"\bdoctors?\b", q_low) and any(term in q_low for term in [
            "england", "region", "icb", "practice", "patients", "workforce", "stoke", "london", "kent"
        ]):
            rescued_question = re.sub(
                r"\bdoctors\b", "GPs",
                re.sub(r"\bdoctor\b", "GP", rescued_question, flags=re.IGNORECASE),
                flags=re.IGNORECASE,
            )
            q_low = rescued_question.lower()

    primary_care_rewrites = [
        (r"\btotal nursing staff\b.*\bprimary care\b", "How many nurses are there in general practice?"),
        (r"\bpharmacists?\b.*\bprimary care\b", "How many pharmacists are there in general practice?"),
        (r"\bphysiotherapists?\b.*\bprimary care\b", "How many physiotherapists are there in general practice?"),
        (r"\bparamedics?\b.*\bprimary care\b", "How many paramedics are there in general practice?"),
        (r"\badmin\b.*\bprimary care\b", "How many admin staff are there in general practice?"),
        (r"\bnon-clinical\b.*\bprimary care\b", "How many admin staff are there in general practice?"),
        (r"\ball staff groups?\b.*\bprimary care\b", "Show staff counts by staff group in general practice"),
        (r"\bbreakdown of all staff groups?\b.*\bprimary care\b", "Show staff counts by staff group in general practice"),
        (r"\bmale to female ratio of nurses?\b.*\bprimary care\b", "Show the male and female nurse counts in general practice"),
        (r"\bclinical staff\b.*\bfte\b", "Show total FTE by staff group for clinical staff in general practice"),
        (r"\bfte\b.*\bclinical staff\b", "Show total FTE by staff group for clinical staff in general practice"),
    ]
    for pattern, rewritten in primary_care_rewrites:
        if re.search(pattern, q_low):
            return {
                "rewritten_question": rewritten,
                "notes": "Normalized broad primary care wording into a GP workforce query",
            }

    # Workforce pressure / strain / coverage wording → patients-per-GP ratio
    pressure_terms = ["pressure", "pressured", "under pressure", "stretched", "strain", "overstretched"]
    coverage_terms = ["coverage", "capacity", "understaffed", "short staffed", "short-staffed", "demand pressure"]
    if (
        any(term in q_low for term in pressure_terms + coverage_terms)
        and "patients per gp" not in q_low
        and "patients-per-gp" not in q_low
    ):
        geo_hint = _extract_geo_scope_hint(rescued_question)
        if re.search(r"\b(?:which|what)\s+(?:areas?|regions?|icbs?)\b.*\b(?:understaffed|short[\s-]?staffed|stretched|pressure)\b", q_low):
            if "icb" in q_low:
                return {
                    "rewritten_question": "Show the ICBs with the highest patients-per-GP ratio",
                    "notes": "Rescued understaffed-area wording into patients-per-GP by ICB",
                }
            return {
                "rewritten_question": "Show the regions with the highest patients-per-GP ratio",
                "notes": "Rescued understaffed-area wording into patients-per-GP by region",
            }
        if re.search(r"\b(?:bad|worst|most pressured|most under pressure|struggling)\s+(?:regions?|areas?|icbs?)\b", q_low):
            if "icb" in q_low:
                return {
                    "rewritten_question": "Show the ICBs with the highest patients-per-GP ratio",
                    "notes": "Rescued vague pressure wording into patients-per-GP by ICB",
                }
            return {
                "rewritten_question": "Show the regions with the highest patients-per-GP ratio",
                "notes": "Rescued vague pressure wording into patients-per-GP by region",
            }
        if re.search(r"\b(?:best|least pressured|least under pressure)\s+(?:regions?|areas?|icbs?)\b", q_low):
            if "icb" in q_low:
                return {
                    "rewritten_question": "Show the ICBs with the lowest patients-per-GP ratio",
                    "notes": "Rescued vague pressure wording into patients-per-GP by ICB",
                }
            return {
                "rewritten_question": "Show the regions with the lowest patients-per-GP ratio",
                "notes": "Rescued vague pressure wording into patients-per-GP by region",
            }
        if geo_hint:
            return {
                "rewritten_question": f"Show the patients-per-GP ratio in {geo_hint}",
                "notes": f"Rescued pressure wording into patients-per-GP for {geo_hint}",
            }
        return {
            "clarification_question": (
                "If you mean workforce pressure, the best measure here is usually patients-per-GP ratio. "
                "Do you want that nationally, by region, or by ICB?"
            ),
            "notes": "Clarify vague pressure wording",
        }

    if (
        any(term in q_low for term in ["good", "bad", "better", "worse", "healthy", "unhealthy", "performing"])
        and any(term in q_low for term in ["gp", "gps", "workforce", "staffing"])
        and not _question_has_explicit_metric(q_low)
    ):
        return {
            "clarification_question": (
                "To judge whether an area looks stronger or weaker, do you want GP headcount, GP FTE, "
                "or the patients-per-GP ratio? The ratio is usually the best pressure measure."
            ),
            "notes": "Clarify evaluative wording into a supported GP workforce metric",
        }

    # Very short compare prompts often omit the metric entirely.
    compare_m = re.match(r"^compare\s+(.+?)[\?!.]?$", q_low)
    if compare_m and not any(token in q_low for token in [" vs ", " versus ", " against ", " to "]) and not _question_has_explicit_metric(q_low):
        compare_target = _clean_entity_hint(compare_m.group(1))
        if compare_target and len(compare_target.split()) <= 5:
            target_label = compare_target.title() if compare_target == compare_target.lower() else compare_target
            return {
                "clarification_question": (
                    f"What would you like to compare for {target_label}: GP headcount, GP FTE, or patients-per-GP ratio?"
                ),
                "notes": "Clarify comparison metric for vague compare prompt",
            }

    # Rescue loose prompts like "bad regions" / "worst areas" even without pressure wording.
    if re.search(r"\b(?:bad|worst)\s+(?:regions?|areas?)\b", q_low):
        return {
            "rewritten_question": "Show the regions with the highest patients-per-GP ratio",
            "notes": "Rescued vague 'bad regions' wording into patients-per-GP by region",
        }
    if re.search(r"\b(?:best)\s+(?:regions?|areas?)\b", q_low):
        return {
            "rewritten_question": "Show the regions with the lowest patients-per-GP ratio",
            "notes": "Rescued vague 'best regions' wording into patients-per-GP by region",
        }

    if rescued_question != original:
        return {
            "rewritten_question": rescued_question,
            "notes": "Normalized GP workforce wording",
        }

    return {}


def _deterministic_table_choice(question: str, plan: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], str]:
    """
    Choose the safest table from explicit domain heuristics.
    Returns (table_or_none, reason).
    """
    q = (question or "").lower()
    intent = str((plan or {}).get("intent") or "").lower()
    practice_hint = _specific_entity_hint(question, "practice")
    practice_code = extract_practice_code(question)

    if any(term in q for term in ["patients per gp", "patients-per-gp", "patient to gp", "gp ratio", "patient list", "registered patients", "list size"]):
        return "practice_detailed", "patient list / ratio metrics live in practice_detailed"

    if any(term in q for term in ["pressure", "pressured", "under pressure", "stretched", "strain"]):
        return "practice_detailed", "pressure-style questions map to patients-per-GP ratio in practice_detailed"

    if practice_code:
        return "practice_detailed", "practice code lookups belong in practice_detailed"

    if "per practice" in q or re.search(r"\baverage .* per practice\b", q):
        return "practice_detailed", "per-practice averages are best served from practice_detailed"

    if any(term in q for term in ["pcn", "sub-icb", "sub icb"]):
        return "practice_detailed", "PCN and sub-ICB rollups come from practice_detailed"

    if practice_hint or any(term in q for term in ["practice", "surgery", "medical centre", "health centre"]):
        ranking_terms = ["top", "highest", "lowest", "most", "least", "rank"]
        practice_detail_terms = ["patients", "patient", "ratio", "staff", "workforce", "breakdown", "more than", "less than", "registered"]
        if any(term in q for term in ranking_terms) and not any(term in q for term in practice_detail_terms):
            return "practice_high", "practice ranking questions fit practice_high"
        return "practice_detailed", "practice-level lookup/aggregation belongs in practice_detailed"

    individual_terms = [
        "age", "gender", "male", "female", "demographic", "qualification",
        "qualified", "retire", "retirement", "training", "trainee", "registrar",
        "partner", "salaried", "locum", "nurse", "admin", "dpc", "pharmacist",
        "physiotherapist", "paramedic",
    ]
    if intent in {"demographics", "trend", "percent_split"} or any(term in q for term in individual_terms):
        return "individual", "people-level workforce slices are best served from individual"

    if intent in {"comparison", "topn"} and not any(term in q for term in ["practice", "pcn", "sub-icb", "sub icb"]):
        return "individual", "non-practice comparisons and rankings default to individual"

    return None, ""


def _preview_first_row(df_md: str) -> Dict[str, str]:
    """Parse the first data row from a markdown preview table."""
    if not df_md:
        return {}
    lines = [line.strip() for line in df_md.strip().splitlines() if line.strip()]
    if len(lines) < 3:
        return {}
    headers = [h.strip() for h in lines[0].split("|") if h.strip()]
    values = [v.strip() for v in lines[2].split("|") if v.strip()]
    if not headers or not values:
        return {}
    return dict(zip(headers, values))


def _coerce_preview_number(value: str) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(text)
    except Exception:
        return None


def _ranking_entity_from_answer(answer: str, entity_type: str) -> str:
    """
    Extract the leading ranked entity from the answer text itself. This is
    useful when the SQL filtered a geography but the answer promoted a specific
    top practice/ICB from the result rows.
    """
    spans = re.findall(r"\*\*(.+?)\*\*", answer or "")
    for span in spans:
        raw = re.sub(r"\s+", " ", span).strip(" -*")
        if not re.search(r"[A-Za-z]", raw):
            continue

        if entity_type == "practice":
            text = raw
            if re.search(r"\btop practice\b", raw, re.IGNORECASE) and re.search(r"\bis\b", raw, re.IGNORECASE):
                text = re.split(r"\bis\b", raw, maxsplit=1, flags=re.IGNORECASE)[1].strip()
                text = re.split(r"\s+(?:with|in|for|as of)\b", text, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            elif " are tied" in raw.lower():
                text = re.split(r"\s+are\s+tied\b", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            else:
                text = re.split(r"\s+(?:has|have|is|are|with)\b", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip()

            if "practice" == text.lower():
                continue
            if " and " in text.lower() or "," in text:
                text = re.split(r",|\sand\s", text, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if text and any(ch.isalpha() for ch in text):
                return text
        if entity_type == "icb":
            icb_matches = re.findall(r"(NHS .*? ICB)", raw, flags=re.IGNORECASE)
            if icb_matches:
                return icb_matches[0].strip()
            text = re.split(r"\s+(?:has|have|is|are|with)\b", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if "icb" in text.lower() or text.lower().startswith("nhs "):
                return text
        if entity_type == "region":
            text = re.split(r"\s+(?:has|have|is|are|with)\b", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            return text
    return ""


def _practice_lookup_filter(practice_like: str) -> str:
    return build_practice_lookup_filter(practice_like, extract_practice_code, sanitise_entity_input)


def sql_practice_gp_count_latest(practice_like: str) -> str:
    return build_sql_practice_gp_count_latest(
        practice_like,
        get_latest_year_month,
        extract_practice_code,
        sanitise_entity_input,
    )


def sql_practice_to_icb_latest(practice_like: str) -> str:
    return build_sql_practice_to_icb_latest(
        practice_like,
        get_latest_year_month,
        extract_practice_code,
        sanitise_entity_input,
    )


def sql_practice_patient_count(practice_like: str) -> str:
    return build_sql_practice_patient_count(
        practice_like,
        get_latest_year_month,
        extract_practice_code,
        sanitise_entity_input,
    )


def sql_patients_per_gp(practice_like: str) -> str:
    return build_sql_patients_per_gp(
        practice_like,
        get_latest_year_month,
        extract_practice_code,
        sanitise_entity_input,
    )


def sql_practice_staff_breakdown(practice_like: str) -> str:
    return build_sql_practice_staff_breakdown(
        practice_like,
        get_latest_year_month,
        extract_practice_code,
        sanitise_entity_input,
    )


def sql_pcn_gp_count() -> str:
    """GP headcount and FTE grouped by PCN — national summary."""
    return build_sql_pcn_gp_count(get_latest_year_month)


def _appointments_geo_filter(question: str, hint: str, table: str) -> Optional[str]:
    return build_appointments_geo_filter(
        question,
        hint,
        table,
        clean_entity_hint=_clean_entity_hint,
        looks_like_time_only_hint=_looks_like_time_only_hint,
        is_national_scope_hint=_is_national_scope_hint,
        extract_practice_code=extract_practice_code,
        sanitise_entity_input=sanitise_entity_input,
        is_valid_region=_is_valid_region,
        normalise_region_name=_normalise_region_name,
        city_to_icb_for_hint=_city_to_icb_for_hint,
    )


def _appointments_geo_hint_from_context(follow_ctx: Dict[str, Any]) -> str:
    return build_appointments_geo_hint_from_context(follow_ctx)


def sql_appointments_total_latest(question: str, geo_hint: str = "", hcp_type: Optional[str] = None) -> str:
    return build_sql_appointments_total_latest(
        question,
        geo_hint,
        hcp_type,
        specific_entity_hint=_specific_entity_hint,
        get_latest_year_month=get_latest_year_month,
        appointments_db=APPOINTMENTS_ATHENA_DATABASE,
        sanitise_entity_input=sanitise_entity_input,
        extract_practice_code=extract_practice_code,
        appointments_geo_filter=_appointments_geo_filter,
    )


def sql_appointments_trend(question: str, geo_hint: str = "", months_back: int = 12, hcp_type: Optional[str] = None) -> str:
    return build_sql_appointments_trend(
        question,
        geo_hint,
        months_back,
        hcp_type,
        specific_entity_hint=_specific_entity_hint,
        sanitise_entity_input=sanitise_entity_input,
        extract_practice_code=extract_practice_code,
        appointments_geo_filter=_appointments_geo_filter,
    )


def sql_appointments_mode_breakdown(question: str, geo_hint: str = "", hcp_type: Optional[str] = None) -> str:
    return build_sql_appointments_mode_breakdown(
        question,
        geo_hint,
        hcp_type,
        specific_entity_hint=_specific_entity_hint,
        get_latest_year_month=get_latest_year_month,
        appointments_db=APPOINTMENTS_ATHENA_DATABASE,
        sanitise_entity_input=sanitise_entity_input,
        appointments_geo_filter=_appointments_geo_filter,
    )


def sql_appointments_hcp_breakdown(question: str, geo_hint: str = "") -> str:
    return build_sql_appointments_hcp_breakdown(
        question,
        geo_hint,
        specific_entity_hint=_specific_entity_hint,
        get_latest_year_month=get_latest_year_month,
        appointments_db=APPOINTMENTS_ATHENA_DATABASE,
        extract_practice_code=extract_practice_code,
        appointments_geo_filter=_appointments_geo_filter,
    )


def sql_appointments_dna_rate(question: str, geo_hint: str = "", hcp_type: Optional[str] = None) -> str:
    return build_sql_appointments_dna_rate(
        question,
        geo_hint,
        hcp_type,
        get_latest_year_month=get_latest_year_month,
        appointments_db=APPOINTMENTS_ATHENA_DATABASE,
        specific_entity_hint=_specific_entity_hint,
        sanitise_entity_input=sanitise_entity_input,
        appointments_geo_filter=_appointments_geo_filter,
    )


def sql_appointments_top_practices(question: str, limit: int = 10) -> str:
    return build_sql_appointments_top_practices(
        limit,
        get_latest_year_month=get_latest_year_month,
        appointments_db=APPOINTMENTS_ATHENA_DATABASE,
    )


# =============================================================================
# Suggestions generator
# =============================================================================
def generate_suggestions(question: str, plan: Dict[str, Any], answer: str,
                         sql: str = "", entity_context: Optional[Dict[str, Any]] = None) -> List[str]:
    """Generate 2-3 contextual follow-up suggestions based on the ANSWER, not just the question.

    If entity_context is provided (region, ICB, practice etc.), suggestions are
    made specific to that entity so follow-ups carry the correct scope.
    """
    q = question.lower()
    a = answer.lower()
    suggestions = []
    table = (plan.get("table") or "").lower()
    intent = (plan.get("intent") or "").lower()
    ctx = entity_context or {}
    ctx_metric = str(ctx.get("previous_metric") or "").lower()
    ctx_staff_group = str(ctx.get("previous_staff_group") or "").lower()
    ctx_entity_type = str(ctx.get("entity_type") or "").lower()
    comparison_basis = str(ctx.get("comparison_basis") or "").strip()
    priority_suggestions: List[str] = []

    # ── Build a metric descriptor for clearer follow-ups ──
    # Instead of "How has this changed..." → "How has the GP headcount changed..."
    _metric_desc = ""
    if "nurse" in q:
        _metric_desc = "the nurse count"
    elif "dpc" in q or "direct patient care" in q:
        _metric_desc = "the DPC count"
    elif "doctor" in q or "gp" in q:
        if "fte" in q:
            _metric_desc = "the GP FTE"
        else:
            _metric_desc = "the GP headcount"
    elif "staff" in q or "workforce" in q:
        _metric_desc = "the total workforce"
    elif "trainee" in q or "registrar" in q:
        _metric_desc = "the trainee count"
    elif "patient" in q and ("ratio" in q or "per gp" in q):
        _metric_desc = "the patients-per-GP ratio"
    elif "partner" in q or "salaried" in q:
        _metric_desc = "the partner/salaried split"
    elif ctx_metric == "patients_per_gp":
        _metric_desc = "the patients-per-GP ratio"
    elif ctx_metric == "fte":
        if ctx_staff_group in {"nurses", "nurse"}:
            _metric_desc = "the nurse FTE"
        elif ctx_staff_group in {"dpc", "direct patient care"}:
            _metric_desc = "the DPC FTE"
        else:
            _metric_desc = "the GP FTE"
    elif ctx_metric == "headcount":
        if ctx_staff_group in {"nurses", "nurse"}:
            _metric_desc = "the nurse headcount"
        elif ctx_staff_group in {"dpc", "direct patient care"}:
            _metric_desc = "the DPC headcount"
        else:
            _metric_desc = "the GP headcount"

    # ── Extract scope qualifier from entity context or SQL ──
    # This turns generic "Break this down by ICB" into "Break this down by ICB within Midlands"
    _scope_qualifier = ""  # e.g. " in Midlands", " for NHS Birmingham and Solihull ICB"
    _scope_entity_type = ""
    _scope_entity_name = ""
    if ctx.get("entity_name"):
        _scope_entity_type = ctx.get("entity_type", "")
        _scope_entity_name = ctx.get("entity_name", "")
    elif sql:
        # Try to extract region/ICB from SQL WHERE clause
        m = re.search(r"(?:comm_)?region_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
        if m:
            _scope_entity_type = "region"
            _scope_entity_name = m.group(1).strip()
        else:
            m = re.search(r"icb_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
            if m:
                _scope_entity_type = "icb"
                _scope_entity_name = m.group(1).strip()

    # Title-case entity name if it came from SQL (may be lowercased by LOWER())
    if _scope_entity_name and _scope_entity_name == _scope_entity_name.lower():
        _scope_entity_name = _scope_entity_name.title()

    if _scope_entity_name:
        if _scope_entity_type == "region":
            _scope_qualifier = f" in {_scope_entity_name}"
        elif _scope_entity_type == "icb":
            _scope_qualifier = f" for {_scope_entity_name}"
        elif _scope_entity_type == "practice":
            _scope_qualifier = f" at {_scope_entity_name}"
        elif _scope_entity_type == "sub_icb":
            _scope_qualifier = f" in {_scope_entity_name}"

    # --- Detect what the QUESTION is about (primary intent) ---
    is_about_age = any(w in q for w in ["age", "over 55", "over 60", "age band", "age distribution", "retirement", "retiring"])
    is_about_gender = any(w in q for w in ["gender", "male", "female", "men", "women"])
    is_about_trend = any(w in q for w in ["trend", "changed", "change", "over the past", "over the last", "year-on-year", "over time"])
    is_about_trainee = any(w in q for w in ["trainee", "training", "registrar", "st1", "st2", "st3"])
    is_about_icb = "icb" in q or ctx_entity_type == "icb"
    is_about_region = "region" in q or ctx_entity_type == "region"
    is_about_practice = any(w in q for w in ["practice", "surgery", "medical centre"]) or table in ("practice_detailed", "practice_high")
    # "how many practices" or "total practices" is a national aggregate, not practice-specific
    _practice_aggregate_words = ["how many practice", "total practice", "number of practice", "practices are there"]
    is_practice_aggregate = any(w in q for w in _practice_aggregate_words)
    is_specific_practice = is_about_practice and not is_practice_aggregate and (intent == "lookup" or ("top" not in q and "most" not in q and "least" not in q))
    is_about_nurse = any(w in q for w in ["nurse", "nursing"]) or ctx_staff_group in {"nurses", "nurse"}
    is_about_dpc = any(w in q for w in ["dpc", "direct patient care", "pharmacist", "physiotherapist"]) or ctx_staff_group in {"dpc", "direct patient care"}
    is_about_gp = ("gp" in q and not is_about_nurse and not is_about_dpc) or ctx_staff_group == "gp"
    # "nurses in GP practices" is a national query about nurses, not a practice-specific query
    if (is_about_nurse or is_about_dpc) and is_about_practice and not any(w in q for w in ["at ", "in the ", "at the "]):
        is_about_practice = False
        is_specific_practice = False
    is_about_partner_salaried = any(w in q for w in ["salaried", "partner", "locum", "retainer"])
    is_about_ratio = any(w in q for w in ["ratio", "per gp", "patients per"]) or ctx_metric == "patients_per_gp"
    is_list_practices = is_about_practice and any(w in q for w in ["top", "most", "least", "which practice", "practices with"])
    is_national = not is_about_icb and not is_specific_practice and not is_about_region

    if comparison_basis:
        priority_suggestions.append("Why might that be?")
        if not is_about_trend:
            priority_suggestions.append(f"Show the trend over the last year{_scope_qualifier}")
        if ctx_entity_type in {"icb", "region", "sub_icb", "city"}:
            priority_suggestions.append("Break this down by practice")

    # --- Generate contextual suggestions based on what the answer covered ---

    # After ratio data → suggest drill-down by area or trend (high priority)
    if is_about_ratio:
        if "worst" not in q and "highest" not in q and "most" not in q:
            suggestions.append("Which area has the worst patients-per-GP ratio?")
        if is_about_practice and not is_about_trend:
            suggestions.append(f"How has {_metric_desc or 'this'} changed over the past year{_scope_qualifier}?")
        elif not is_about_icb:
            suggestions.append("How does this vary by ICB?")
        if not is_about_trend:
            suggestions.append("How has the ratio changed over time?")

    # After listing practices → suggest deeper dive into specific practice
    elif is_list_practices:
        suggestions.append("How many patients are registered at this practice?")
        suggestions.append("What is the patients-per-GP ratio?")
        if "staff" not in q:
            suggestions.append("Show the full staff breakdown for this practice")

    # After national nurse/DPC query → suggest comparison, not practice-specific
    elif (is_about_nurse or is_about_dpc) and is_national:
        if is_about_nurse:
            suggestions.append(f"How does this compare to GP numbers{_scope_qualifier}?")
            suggestions.append(f"Break this down by ICB{_scope_qualifier}")
            if not is_about_trend:
                suggestions.append(f"How has {_metric_desc or 'this'} changed over the past 3 years{_scope_qualifier}?")
        else:
            suggestions.append("Show the nurse breakdown separately")
            suggestions.append(f"Break this down by ICB{_scope_qualifier}")

    # After age distribution → suggest retirement or gender cross-tab
    elif is_about_age and not is_about_gender:
        suggestions.append("How does this vary between male and female GPs?")
        if "55" not in q and "retirement" not in q:
            suggestions.append("How many GPs are over 55 and approaching retirement?")
        if not is_about_icb:
            suggestions.append("Which ICBs have the oldest GP workforce?")

    # After gender breakdown → suggest age or role type cross-tab
    elif is_about_gender and not is_about_age:
        suggestions.append("What is the age distribution of GPs?")
        if not is_about_partner_salaried:
            suggestions.append("Are there more female salaried or female partners?")

    # After trainee data → suggest pipeline or comparison
    elif is_about_trainee:
        if "grade" not in q and "st1" not in q:
            suggestions.append("Break this down by training grade (ST1, ST2, ST3)")
        if not is_about_trend:
            suggestions.append("How has the trainee count changed over the past 3 years?")
        if not is_about_icb:
            suggestions.append("Which ICBs have the most trainees?")

    # After ICB-specific data → suggest comparison or deeper dive
    elif is_about_icb:
        if "most" not in q and "least" not in q and "top" not in q:
            suggestions.append("Which ICB has the highest number?")
        if not is_about_trend:
            suggestions.append(f"How has {_metric_desc or 'this'} changed over the past year{_scope_qualifier}?")
        if not is_about_practice:
            suggestions.append("Show the top practices in this ICB")

    # After partner/salaried → suggest trend or gender
    elif is_about_partner_salaried:
        if not is_about_trend:
            suggestions.append("How has the salaried vs partner split changed over 3 years?")
        if not is_about_gender:
            suggestions.append("What is the gender breakdown for each role type?")
        if not is_about_icb:
            suggestions.append(f"Break this down by ICB{_scope_qualifier}")

    # After practice-aggregate data (e.g. "how many practices") → suggest national metrics
    elif is_practice_aggregate:
        suggestions.append("What is the average number of GPs per practice?")
        suggestions.append("Which practices have the most GPs?")
        if not is_about_trend:
            suggestions.append("How has the number of practices changed over time?")

    # After practice-specific data → suggest related practice metrics
    elif is_specific_practice:
        if not is_about_ratio:
            suggestions.append("What is the patients-per-GP ratio?")
        if "staff" not in q and "breakdown" not in q:
            suggestions.append("Show the full staff breakdown for this practice")
        if "patient" not in q and not is_about_ratio:
            suggestions.append("How many patients are registered at this practice?")

    # After trend data → suggest snapshot or comparison
    elif is_about_trend:
        if not is_about_gender:
            suggestions.append(f"What is the gender split{_scope_qualifier}?")
        if not is_about_icb:
            suggestions.append("Which ICBs saw the biggest change?")
        if not is_about_age:
            suggestions.append("What is the age distribution?")

    # After region-level data → suggest ICB drill-down or comparison
    if is_about_region and len(suggestions) < 3:
        if not is_about_icb:
            suggestions.append(f"Break this down further by ICB{_scope_qualifier}")
        if not is_about_trend:
            suggestions.append(f"How has {_metric_desc or 'this'} changed over the past year{_scope_qualifier}?")
        if not is_about_gender:
            suggestions.append("What is the gender split by region?")

    # After national data (not practice, not ICB) → suggest drill-down
    if is_national and not is_about_practice and not is_about_trainee and len(suggestions) < 3:
        if not is_about_icb and not any("Break this down" in s and "ICB" in s for s in suggestions):
            suggestions.append(f"Break this down by ICB{_scope_qualifier}")
        if not is_about_trend and not any("changed" in s or "trend" in s or "over" in s for s in suggestions):
            suggestions.append(f"How has {_metric_desc or 'this'} changed over the past 3 years{_scope_qualifier}?")

    # Cross-staff-group suggestions (fill remaining slots)
    if is_about_gp and not is_about_nurse and not is_about_dpc and len(suggestions) < 3:
        suggestions.append(f"Show the same for nurses and DPC staff{_scope_qualifier}")
    if is_about_nurse and not is_about_gp and len(suggestions) < 3:
        suggestions.append(f"How does this compare to GP numbers{_scope_qualifier}?")
    if is_about_dpc and not is_about_nurse and len(suggestions) < 3:
        suggestions.append("Show the nurse breakdown separately")

    if ctx and len(suggestions) < 3:
        if ctx.get("entity_name") and not any("Compare this with national average" == s for s in suggestions):
            suggestions.append("Compare this with national average")
        if not any("Why might that be?" == s for s in suggestions):
            suggestions.append("Why might that be?")

    # Deduplicate and limit
    seen = set()
    unique = []
    for s in priority_suggestions + suggestions:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique[:3]


# =============================================================================
# Plan validation (improvement #6)
# =============================================================================
def validate_plan(plan: Dict[str, Any], question: str, dataset: DatasetName = "workforce") -> Dict[str, Any]:
    """Validate and auto-correct the plan before SQL generation."""
    allowed_tables = _dataset_allowed_tables(dataset)
    default_table = str(_dataset_config(dataset).get("default_table") or ("practice" if dataset == "appointments" else "individual"))
    table = str(plan.get("table", default_table)).lower()
    if table not in allowed_tables:
        plan["table"] = default_table
        table = default_table

    deterministic_table, deterministic_reason = _deterministic_table_choice_for_dataset(question, plan, dataset)
    if deterministic_table and deterministic_table != table:
        plan["table"] = deterministic_table
        plan["_table_switched"] = deterministic_reason
        table = deterministic_table

    schema_cols = {c for c, _ in get_table_schema(table)}

    # Validate group_by columns exist
    group_by = plan.get("group_by", []) or []
    valid_group_by = [g for g in group_by if g.lower() in schema_cols]
    if group_by and not valid_group_by:
        plan["_group_by_warning"] = f"Requested group_by {group_by} not found in {table} schema"
    plan["group_by"] = valid_group_by

    # Auto-switch table for practice questions if using individual
    q = question.lower()
    if dataset == "workforce" and table == "individual" and any(kw in q for kw in ["practice", "prac_name", "prac_code"]):
        plan["table"] = "practice_detailed"
        plan["_table_switched"] = f"Switched from individual to practice_detailed (practice question)"

    # Auto-switch for practice ranking
    if dataset == "workforce" and table == "individual" and ("top" in q or "rank" in q or "highest" in q or "lowest" in q) and "practice" in q:
        plan["table"] = "practice_high"
        plan["_table_switched"] = f"Switched to practice_high for practice ranking"

    entities = [str(e) for e in (plan.get("entities_to_resolve", []) or []) if str(e).strip()]
    practice_hint = _specific_entity_hint(question, "practice")
    icb_hint = _specific_entity_hint(question, "icb")
    sub_icb_hint = _specific_entity_hint(question, "sub_icb")
    if dataset == "workforce":
        if practice_hint and plan.get("table") in {"practice_detailed", "practice_high"} and "prac_name" not in entities:
            entities.append("prac_name")
        if icb_hint and _looks_like_specific_icb_hint(question, icb_hint) and "icb_name" not in entities:
            entities.append("icb_name")
        if sub_icb_hint and "sub_icb_name" not in entities:
            entities.append("sub_icb_name")
        if "prac_name" in entities and not _specific_entity_hint(question, "practice"):
            entities = [e for e in entities if e != "prac_name"]
        if "icb_name" in entities and not icb_hint:
            entities = [e for e in entities if e != "icb_name"]
        if "icb_name" in entities and icb_hint and not _looks_like_specific_icb_hint(question, icb_hint):
            entities = [e for e in entities if e != "icb_name"]
        if icb_hint and _looks_like_specific_icb_hint(question, icb_hint):
            entities = [e for e in entities if e != "sub_icb_name"]
        if "sub_icb_name" in entities and not sub_icb_hint:
            entities = [e for e in entities if e != "sub_icb_name"]
    elif dataset == "appointments":
        region_hint = _specific_entity_hint(question, "region")
        pcn_hint = _specific_entity_hint(question, "pcn")
        if practice_hint and "gp_name" not in entities:
            entities.append("gp_name")
        if icb_hint and "icb_name" not in entities:
            entities.append("icb_name")
        if sub_icb_hint and "sub_icb_location_name" not in entities:
            entities.append("sub_icb_location_name")
        if region_hint and "region_name" not in entities:
            entities.append("region_name")
        if pcn_hint and "pcn_name" not in entities:
            entities.append("pcn_name")
    plan["entities_to_resolve"] = entities

    return plan


# =============================================================================
# LangGraph state
# =============================================================================
class DatasetPipelineState(TypedDict, total=False):
    """
    Dataset-subgraph state.

    This keeps the fields needed by the reusable SQL pipelines and is the type
    used by `make_sql_pipeline(config)`. The supervisor graph may carry some
    additional top-level response/UX fields around the pipeline state.
    """
    session_id: str
    dataset: DatasetName
    question: str
    original_question: str  # before follow-up enrichment
    rewritten_question: str
    rewrite_notes: str

    conversation_history: str
    domain_notes: str
    follow_up_context: Optional[Dict[str, Any]]  # previous entity context if follow-up
    user_preferences: Dict[str, Any]
    semantic_state: Dict[str, Any]
    viz_plan: Dict[str, Any]

    latest_year: Optional[str]
    latest_month: Optional[str]
    time_range: Optional[Dict[str, Any]]

    staff_groups: List[str]
    staff_roles: List[str]
    detailed_staff_roles: List[str]
    practice_high_measures: List[str]
    practice_high_staff_groups: List[str]
    practice_high_detailed_roles: List[str]
    appt_modes: List[str]
    appt_statuses: List[str]
    appt_hcp_types: List[str]
    appt_categories: List[str]
    appt_time_bands: List[str]

    resolved_entities: Dict[str, Any]
    plan: Dict[str, Any]
    sql: str
    candidate_tables: List[str]
    schema_narrowing_notes: str
    narrowed_schema_text: str
    semantic_request_v9: Dict[str, Any]
    semantic_path: Dict[str, Any]

    df_preview_md: str
    answer: str
    suggestions: List[str]

    attempts: int
    last_error: Optional[str]
    needs_retry: bool

    _rows: int
    _empty: bool
    _hard_intent: Optional[str]
    _is_knowledge: bool  # True = answer from domain notes only, skip SQL
    _query_route: str  # adaptive routing: "knowledge" | "data_simple" | "data_complex" | "out_of_scope"
    _query_routing: RoutingDecision
    _dataset_routing: RoutingDecision
    _few_shot_best_sim: float  # best similarity score from few-shot retrieval
    _confidence: Dict[str, Any]  # confidence grading result
    _needs_clarification: bool  # True = query is ambiguous, ask user for more info
    _clarification_question: str  # the clarification question to present to the user
    _clarification_resolved: bool  # True = this query was enriched from a clarification answer
    _followup_intent: str  # conversational follow-up type e.g. benchmark_probe / explanation


class SupervisorState(DatasetPipelineState, total=False):
    """
    Top-level graph state.

    The supervisor orchestrates dataset routing, clarification, summarisation,
    grading, and API-facing metadata on top of the dataset pipeline fields.
    """
    answer: str
    suggestions: List[str]
    supervisor_mode: str
    worker_plan: Dict[str, Any]
    data_worker_answer: str
    knowledge_worker_answer: str


# Backwards-compatible alias while the rest of the file is migrated to the
# explicit supervisor/pipeline split.
class AgentState(SupervisorState, total=False):
    pass


class SemanticFrame(TypedDict, total=False):
    metric: str
    staff_group: str
    entity_type: str
    entity_name: str
    entity_code: str
    table: str
    view: str
    aggregation: str
    grain: str
    group_dim: str
    comparison_basis: str
    mapped_icb: str
    parent_scope_entity_type: str
    parent_scope_entity_name: str


# =============================================================================
# Prompts (enhanced)
# =============================================================================
PLANNER_SYSTEM = """You are a GP Workforce analytics planner. You decide how to answer questions using 3 Athena tables.

CRITICAL — FOLLOW-UP HANDLING:
If the user asks a follow-up (e.g. "now by gender", "the same for nurses", "What is the ratio?"),
you MUST use the conversation history AND the follow-up context to understand what entity they're
referring to.
- If the question contains "(context: practice = <name>)" → filter by prac_name LIKE '%<name>%'
- If the question contains "(context: icb = <name>)" → filter by icb_name LIKE '%<name>%'
- If the question contains "(context: region = <name>)" → filter by the table's region column from schema.
  In the bundled schemas this is comm_region_name on individual and region_name on practice_detailed.
- If the question contains "(context: sub_icb = <name>)" → filter by sub_icb_name LIKE '%<name>%'
- If the question contains "(context: city = <city>, icb = <icb>)" → filter by (icb_name LIKE '%<icb>%' OR sub_icb_name LIKE '%<city>%')
- If the question contains "(context: table = <name>)" → prefer that table for the follow-up
- If the question contains "(context: grain = <grain>)" → preserve the SAME comparison grain unless the user explicitly changes it.
  Example grains: practice_total, icb_total, region_total, city_total, national_total.
- If the question contains "(context: metric = patients_per_gp)" → compute patients-per-GP ratio
- If the question contains "(context: metric = fte)" → use SUM(fte) as the metric
- If the question contains "(context: metric = headcount)" → use COUNT(DISTINCT unique_identifier) as the metric
  using SUM(total_patients)/SUM(total_gp_fte) from practice_detailed
The follow-up context is injected automatically — USE IT to maintain the correct entity filter.
NEVER ignore the context and return national/all-practice results when the user was asking about
a specific entity in the previous turn.

CRITICAL — FOLLOW-UP METRIC INFERENCE:
ALWAYS use the conversation history to identify what metric was being discussed.
- If context includes "metric = patients_per_gp" OR the previous question explicitly mentioned "patients per GP":
  → compute SUM(total_patients)/SUM(total_gp_fte) from practice_detailed.
- If previous question was about FTE per GP (total_gp_fte / total_gp_hc ≈ 2.0): stay on FTE/GP metric.
- If previous question was about headcount/FTE ratio: stay on that metric.
- NEVER switch to patients-per-GP unless the previous question explicitly mentioned "patients".

CRITICAL — TOPIC CHANGE DETECTION:
If the user's NEW question is about a COMPLETELY DIFFERENT topic from the conversation history,
you MUST treat it as a FRESH question and NOT carry forward filters/entities from the old topic.
Examples of topic changes:
- Previous: "average trainees per practice" → New: "how many GPs eligible to retire" — DIFFERENT topic
- Previous: "nurse FTE trend" → New: "What proportion of GPs are male" — DIFFERENT topic
- Previous: "top 10 practices by GP FTE" → New: "show me by region instead" — SAME topic (reformatting)
When the topic changes, plan the query based ONLY on the new question. Do NOT inherit
staff_group filters, demographic filters, or training grade filters from previous turns.

TABLE SELECTION RULES:
- individual: 22 columns. Has staff_group, staff_role, detailed_staff_role, gender, age_band, country_qualification_group, fte.
  Geography: region column from schema, icb_name, sub_icb_name.
  USE FOR: national/regional/ICB totals, demographics, SUM(fte), COUNT(DISTINCT unique_identifier).
- practice_high: 8 columns. Tidy format: prac_code, prac_name, staff_group, detailed_staff_role, measure, value, year, month.
  measure can be 'FTE' or 'Headcount'. Value is in the 'value' column.
  USE FOR: practice-level rankings, comparing practices.
- practice_detailed: 830+ columns. Wide format with pre-computed totals per practice.
  Has geography hierarchy (practice->PCN->Sub-ICB->ICB->Region), patient counts, all staff breakdowns.
  USE FOR: practice lookups, patient counts, patients-per-GP, detailed GP sub-type breakdowns.

IMPORTANT EXAMPLES:
- "total GP FTE nationally" -> individual, SUM(fte) WHERE staff_group = 'GP'
- "top practices by GP FTE" -> practice_high WHERE staff_group = 'GP' AND measure = 'FTE'
- "Keele practice GP count" -> practice_detailed, search by prac_name
- "GP trend over 12 months" -> individual, GROUP BY year, month
- "proportion of GPs in training" -> individual, intent="percent_split", COUNT trainees / COUNT all GPs
- "qualified GPs eligible to retire" -> individual, intent="demographics", filter age_band 55+, EXCLUDE trainees/locums
- "trainee pipeline / eligible for FTE" -> individual, intent="total", count all trainees by training grade
- "which practices have most locums" -> practice_high, intent="topn", WHERE detailed_staff_role LIKE '%Locum%'
- "percentage of headcount that are FTE" -> individual, intent="ratio", SUM(fte) / COUNT(DISTINCT unique_identifier)
- "GP numbers grouped by PCN" -> practice_detailed, intent="total", GROUP BY pcn_name, SUM(total_gp_hc) or SUM(total_gp_fte)
- "FTE proportion per PCN within an ICB" -> practice_detailed, intent="percent_split", GROUP BY pcn_name
- "practice sustainability" -> practice_detailed, intent="ratio", total_gp_fte / total_gp_hc
- "GPs vs advanced practitioners" -> individual, intent="comparison", compare staff_group GP vs DPC advanced roles
- "loss of qualified GPs over time" -> individual, intent="trend", qualified GP count over months (exclude trainees/locums)
- "patients-per-GP ratio nationally" -> practice_detailed, intent="ratio", SUM(total_patients)/SUM(total_gp_fte)
- "patients-per-GP trend for NHS Kent and Medway ICB" -> practice_detailed, intent="trend", GROUP BY year+month, WHERE icb_name LIKE '%kent%', SUM(total_patients)/SUM(total_gp_fte)
- "how has the patients-per-GP ratio changed over time for Kent and Medway" -> practice_detailed, intent="trend"
- "average nurses per practice" -> practice_detailed, intent="total", AVG(total_nurses_hc)
- "GP age distribution" -> individual, intent="demographics", GROUP BY age_band
- "male vs female GP trainees" -> individual, intent="percent_split", GROUP BY gender WHERE staff_role LIKE '%Training%'
- "GPs in North East" -> individual, intent="total", WHERE the region column LIKE '%North East%'

CRITICAL COLUMN USAGE:
- TRAINEES: Filter by staff_role LIKE '%Training%' (NOT detailed_staff_role). The value is 'GPs in Training Grades'.
- REGIONS: Use the region column exposed by the schema for that table.
  In many deployments this is comm_region_name on individual and region_name on practice_detailed.
  Actual regions: East of England, London, Midlands, North East and Yorkshire, North West, South East, South West.
- REGIONS (practice_detailed table): Use region_name (NOT comm_region_name).

OUT OF SCOPE (set in_scope=false with explanation):
- Patient wait times, appointment data, GP Patient Survey data
- Prescribing data, QOF data, patient satisfaction scores
- Real-time data, today's staffing levels
- Individual staff names, identifiable personal data
- Salary, pay, earnings data (not in this dataset)
- Data from non-England countries (Scotland, Wales, NI)

MULTI-PERIOD COMPARISONS:
- Questions like "this year vs 3 years ago", "compare 2022 and 2025", "how has X changed" -> intent = "comparison"
- Use the SAME table as you would for a single-period query, but note BOTH periods in filters_needed.
- Example: "average trainees this year vs 3 years ago" -> practice_detailed, intent="comparison",
  filters_needed=["year='2025' month='12'", "year='2022' month='12'"], notes="two-period comparison"

CORRECTION / REFINEMENT FOLLOW-UPS:
- If the user says "i dont want FTE, i want headcount" or "not by region, by ICB" or "actually show me nurses instead",
  this is a CORRECTION of their previous question. Use the conversation history to understand what they originally asked,
  then re-plan with the correction applied.
- If the question contains "(context: table = ..., scope = national)", use that to understand the previous query scope.
- Example: prev="Show me GP FTE nationally" + current="i dont want FTE, i want headcount"
  → individual table, intent="total", COUNT(DISTINCT unique_identifier) WHERE staff_group='GP', scope=national
- ALWAYS generate SQL for corrections — they are valid data queries, NOT out-of-scope.

BENCHMARK / AVERAGE FOLLOW-UPS:
- If the user says "compare this with national average" or similar after an entity-level result,
  you MUST compare against the average for the SAME unit type, not the England total.
- Examples:
  * ICB total -> compare with average ICB
  * Region total -> compare with average region
  * Practice total -> compare with average practice
- NEVER compare a local total against the England total and call it an "average".
- If the previous result's unit type is ambiguous (for example a city that is not a formal reporting grain),
  ask a clarification instead of guessing.

FOLLOW-UP HANDLING FOR COMPARISONS:
- If user says "show me side by side" / "compare" / "show both" after a previous query,
  this is a FORMATTING request, not a scope change.
- Keep the SAME table, SAME granularity (national stays national), SAME intent.
- Do NOT expand to per-practice breakdown unless the user explicitly asks for it.
- Set intent = "comparison" and note both time periods.

AMBIGUITY & CLARIFICATION:
- If the question is genuinely ambiguous and you cannot make a reasonable assumption, set needs_clarification=true.
- Provide a short, friendly clarification_question with 2-3 specific options.
- ONLY flag clarification for truly ambiguous queries. These NEED clarification:
  * "show me the data" (what data? which metric? what geography?)
  * "compare them" (compare what entities/metrics?)
  * "how is it going" (what metric? what timeframe?)
- These do NOT need clarification (use reasonable defaults):
  * "GP FTE" → assume national total, latest month (clear enough)
  * "top practices" → assume by GP FTE, top 10 (standard default)
  * "GP trend" → assume FTE trend, last 12 months (common request)
  * "staff breakdown" → assume by staff_group, national (reasonable)
- When in doubt, prefer answering with reasonable defaults over asking for clarification.
- NEVER ask for clarification on queries that already specify a metric, geography, or entity.

Your output will be parsed as a structured object with these fields:
- in_scope (bool): Whether the question can be answered from GP workforce data
- table (string): "individual", "practice_high", or "practice_detailed"
- intent (string): "total", "percent_split", "ratio", "trend", "topn", "lookup", "comparison", "demographics", or "unknown"
- group_by (list of strings): Column names to GROUP BY
- filters_needed (list of strings): SQL WHERE clause fragments
- entities_to_resolve (list of strings): Entity column names needing fuzzy matching (e.g. "icb_name", "prac_name")
- needs_clarification (bool): True ONLY if the query is genuinely ambiguous (see rules above)
- clarification_question (string): If needs_clarification is true, a short question with 2-3 options
- notes (string): Short explanation of the query plan

Fill in ALL fields. If no filters/groups/entities are needed, use empty lists.
"""

SQL_SYSTEM = """You are an expert AWS Athena (Trino/Presto) SQL writer for GP Workforce data.

HARD RULES:
- Return ONLY SQL (no markdown, no explanation, no backticks).
- Read-only only (SELECT or WITH ... SELECT).
- Allowed base tables: practice_high, individual, practice_detailed.
- Use the exact latest year/month provided (do NOT guess dates).
- staff_group / staff_role / detailed_staff_role MUST use values from the provided vocabulary lists. Never invent values.
- For role filtering, ALWAYS use LIKE with partial matching (e.g. staff_role LIKE '%Salaried%'), NEVER use exact match (= 'Salaried GP').
  The actual detailed_staff_role values are things like 'Salaried By Practice', 'Partner/Provider', 'Senior Partner' — NOT 'Salaried GP' or 'GP Partner'.
- For TRAINEE queries, use staff_role LIKE '%Training%' (NOT detailed_staff_role LIKE '%trainee%').
  The actual staff_role value for trainees is 'GPs in Training Grades'. detailed_staff_role does NOT contain 'trainee'.
- For REGION filtering, ALWAYS use LIKE with partial matching on the region column shown in the schema.
  The actual region names are: 'East of England', 'London', 'Midlands', 'North East and Yorkshire',
  'North West', 'South East', 'South West'. Note: "North East" alone won't match — the region is called
  "North East and Yorkshire". ALWAYS use LIKE '%North East%' instead of = 'North East'.
  On practice_detailed the column is usually region_name. On individual, follow the schema provided in context.
- String comparisons should use LOWER(TRIM(...)) for robustness.
- If ENTITY RESOLUTION GUIDANCE provides a clear top match, use that exact resolved value in SQL instead of the raw user wording.

FOLLOW-UP CONTEXT:
- If the question contains "(context: practice = <name>)" you MUST include:
  WHERE LOWER(TRIM(prac_name)) LIKE LOWER('%<name>%')
- If the question contains "(context: icb = <name>)" you MUST include:
  WHERE LOWER(TRIM(icb_name)) LIKE LOWER('%<name>%')
- If the question contains "(context: region = <name>)" you MUST include:
  WHERE LOWER(TRIM(<region_column_from_schema>)) LIKE LOWER('%<name>%')
  Use the region column shown in the schema for the chosen table.
- If the question contains "(context: sub_icb = <name>)" you MUST include:
  WHERE LOWER(TRIM(sub_icb_name)) LIKE LOWER('%<name>%')
- If the question contains "(context: city = <city>, icb = <icb>)" you MUST include:
  WHERE (LOWER(TRIM(icb_name)) LIKE '%<icb>%' OR LOWER(TRIM(sub_icb_name)) LIKE '%<city>%')
  Example: "(context: city = leeds, icb = west yorkshire)" → WHERE (LOWER(TRIM(icb_name)) LIKE '%west yorkshire%' OR LOWER(TRIM(sub_icb_name)) LIKE '%leeds%')
- If the question contains "(context: grain = <grain>)" you MUST preserve the same reporting grain unless the user explicitly changes it.
  Example: if grain = icb_total and the user asks for a benchmark, compare with the average ICB, not the England total.
- If the question contains "(context: metric = patients_per_gp)" you MUST compute:
  ROUND(SUM(total_patients) / NULLIF(SUM(total_gp_fte), 0), 1) AS patients_per_gp
  using the practice_detailed table (which has total_patients and total_gp_fte columns).
- If the question contains "(context: metric = fte)" use SUM(fte) as the metric.
- If the question contains "(context: metric = headcount)" use COUNT(DISTINCT unique_identifier) as the metric.
- NEVER produce a query without the entity filter when context is provided.
- NEVER use LIKE '%this%' or LIKE '%that%' literally — always substitute the real entity name.
- If the user asks for an "average" benchmark, the SQL must actually compute an average (for example AVG(...) or an averaged grouped benchmark).
- NEVER compare a local total with the England total and describe that as an average.

FOLLOW-UP CONTEXT EXAMPLES (MANDATORY — study these carefully):

Example 1: "What about nurses? (context: city = leeds, icb = west yorkshire, table = individual, metric = headcount)"
→ SELECT COUNT(DISTINCT unique_identifier) AS nurse_count, ROUND(SUM(fte), 1) AS nurse_fte
  FROM individual WHERE year = '2025' AND month = '12'
  AND staff_group = 'Nurses'
  AND (LOWER(TRIM(icb_name)) LIKE '%west yorkshire%' OR LOWER(TRIM(sub_icb_name)) LIKE '%leeds%')

Example 2: "Break this down by gender (context: city = leeds, icb = west yorkshire, table = individual, metric = headcount)"
→ SELECT gender, COUNT(DISTINCT unique_identifier) AS count, ROUND(SUM(fte), 1) AS fte
  FROM individual WHERE year = '2025' AND month = '12'
  AND staff_group = 'Nurses'
  AND (LOWER(TRIM(icb_name)) LIKE '%west yorkshire%' OR LOWER(TRIM(sub_icb_name)) LIKE '%leeds%')
  GROUP BY gender

Example 3: "I meant FTE not headcount (context: icb = cheshire and merseyside, table = individual, metric = headcount)"
→ SELECT ROUND(SUM(fte), 1) AS gp_fte
  FROM individual WHERE year = '2025' AND month = '12'
  AND staff_group = 'GP'
  AND LOWER(TRIM(icb_name)) LIKE '%cheshire and merseyside%'

CRITICAL: In ALL examples above, the geo filter from the context is ALWAYS present. NEVER omit it.

TOPIC CHANGE:
- If the CURRENT QUESTION asks about a different subject than the CONVERSATION HISTORY
  (e.g. history is about trainees but new question asks about retirement/age),
  write the SQL for the NEW question only. Do NOT carry forward WHERE filters
  (e.g. training grade filters) from previous unrelated questions.

TABLE-SPECIFIC RULES:
- individual: FTE = SUM(fte). Headcount = COUNT(DISTINCT unique_identifier). Always filter by year + month.
- practice_high: value column is the numeric measure. Filter by measure = 'FTE' or measure = 'Headcount'.
  Use CAST(value AS DOUBLE) for numeric operations. Always filter by year + month.
- practice_detailed: pre-computed totals already exist as columns (total_gp_hc, total_gp_fte etc).
  For a SINGLE practice lookup, use the columns directly from that row.
  For aggregation across MULTIPLE practices (national / region / ICB / PCN), SUM or AVG those per-practice totals as appropriate.
  Always filter by year + month.

COLUMN LABELS are provided to help you pick the right column name. Use the column NAME (not the label) in SQL.

TIME RANGES: If a time range is provided, use it in WHERE clause:
  (CAST(year AS INTEGER) * 100 + CAST(month AS INTEGER)) BETWEEN start AND end

MULTI-PERIOD COMPARISONS (CRITICAL):
When comparing two time periods (e.g. "this year vs 3 years ago", "2025 vs 2022", "compare", "side by side"):
- NEVER mix rows from different years in the same AVG/SUM/COUNT without separating them.
- Use conditional aggregation with CASE WHEN to produce separate columns per period:
    COUNT(DISTINCT CASE WHEN year = '2025' AND month = '12' THEN unique_identifier END) AS hc_2025,
    COUNT(DISTINCT CASE WHEN year = '2022' AND month = '12' THEN unique_identifier END) AS hc_2022
- OR use two CTEs / subqueries — one per period — joined on the grouping key.
- Always include a difference or change column:  (val_2025 - val_2022) AS change
- If the original question asked for a national average, keep it national (no GROUP BY practice).
- If the user says "side by side" or "compare" for a follow-up, maintain the SAME granularity
  as the previous query (national stays national, practice-level stays practice-level)
  unless the user explicitly asks to drill down.

MULTI-PERIOD WHERE CLAUSE (CRITICAL — SQL PRECEDENCE BUG):
When using CASE WHEN for multi-period, the WHERE clause must NOT add year/month OR conditions.
BAD (SQL precedence bug — returns wrong data):
  WHERE staff_group = 'GP' AND staff_role LIKE '%Training%'
    AND (year = '2025' AND month = '12') OR (year = '2022' AND month = '12')
GOOD — just filter to the shared month, let CASE WHEN separate the years:
  WHERE staff_group = 'GP' AND staff_role LIKE '%Training%' AND month = '12'
OR GOOD — wrap OR in double parentheses:
  WHERE staff_group = 'GP' AND staff_role LIKE '%Training%'
    AND ((year = '2025' AND month = '12') OR (year = '2022' AND month = '12'))
Rule: any OR combining year+month conditions MUST be wrapped in outer parentheses.

FOLLOW-UP QUERIES:
- "Show me side by side" / "compare" / "show both" on a previous result means:
  reformat the SAME data with clear period labels, NOT expand to per-practice.
- Maintain the same GROUP BY level as the previous query unless user explicitly changes scope.

NULLIF HANDLING:
- practice_detailed columns may contain 'NA' strings. Always use NULLIF(col, 'NA') before CAST.
- Example: CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE)

ROUNDING: Round FTE values to 1 decimal place. Round percentages to 1 decimal place.

BUSINESS LOGIC & COMMON QUERY PATTERNS:

1. QUALIFIED GPs (exclude trainees and locums):
   - On individual table: WHERE staff_group = 'GP' AND staff_role NOT LIKE '%Training%' AND staff_role NOT LIKE '%Locum%'
   - On practice_detailed: use total_gp_extgl_hc/fte columns (GPs excluding trainees and locums)

2. RETIREMENT ELIGIBILITY:
   - NHS pension age is typically 60 (old scheme) or State Pension Age (new scheme)
   - For "eligible to retire in next 5 years": filter age_band IN ('55-59', '60-64') from individual table
   - MUST exclude trainees and locums from the count
   - Calculate as: COUNT(age 55+) / COUNT(all qualified GPs) * 100

3. FTE / HEADCOUNT RATIO:
   - FTE per head = SUM(fte) / COUNT(DISTINCT unique_identifier) (on individual table)
   - Percentage: multiply by 100
   - A value < 1.0 means average staff work part-time
   - For practice_detailed: total_gp_fte / CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE)

4. PRACTICE SUSTAINABILITY:
   - Higher proportion of non-FTE (part-time) staff = potentially less sustainable
   - Sustainability ratio = total_gp_fte / total_gp_hc (closer to 1.0 = more sustainable)
   - Can also compare: GP FTE to patient count ratio (patients per GP FTE)

5. TRAINEE / REGISTRAR PIPELINE:
   - On individual table: trainees/registrars have staff_role LIKE '%Training%'
   - IMPORTANT: Use the VALID VALUES list provided for exact role names. Do NOT invent values.
   - Common roles: look for values containing 'Training' or 'Registrar' in the staff_role or detailed_staff_role lists
   - All GP trainees (ST1-ST3, F1/F2) will eventually be eligible for qualified GP roles
   - ST3 trainees are closest to completion (~1 year away)
   - ST1/ST2 are 2-3 years away
   - Proportion = COUNT(trainees) / COUNT(all GPs) * 100
   - On practice_detailed: use total_gp_reg_hc/fte (GP registrars) columns if available

6. LOCUM QUERIES:
   - On practice_high: WHERE staff_group = 'GP' AND detailed_staff_role LIKE '%Locum%'
   - On practice_detailed: use total_gp_locum_vac_hc, total_gp_locum_abs_hc, total_gp_locum_oth_hc
   - For "most locums": ORDER BY value DESC on practice_high

7. GPs vs ADVANCED PRACTITIONERS:
   - GPs: staff_group = 'GP' (on individual)
   - Advanced practitioners: staff_group = 'DPC' with detailed_staff_role LIKE 'Advanced%'
     or specific roles like 'Physician Associate', 'Paramedic', etc.

8. TREND / LOSS OVER TIME:
   - For "loss of GPs over time": use individual table, GROUP BY year, month, count qualified GPs
   - Show month-over-month or year-over-year change
   - Always exclude trainees and locums unless specifically asked about

8b. TRAINEE TREND OVER MULTIPLE YEARS:
   - MUST use individual table with: staff_role LIKE '%%Training%%'
   - DO NOT use detailed_staff_role for trainee filtering — use staff_role
   - DO NOT guess role names — just use LIKE '%%Training%%' which catches all trainee roles
   - For year-over-year trend: use the SAME month from each year (e.g. month = '12') for consistent comparison
   - COPY THIS EXACT SQL PATTERN:
     SELECT year, COUNT(DISTINCT unique_identifier) AS trainee_hc, ROUND(SUM(fte), 1) AS trainee_fte
     FROM individual
     WHERE staff_group = 'GP' AND staff_role LIKE '%%Training%%'
       AND month = '12'
     GROUP BY year ORDER BY year
   - On practice_detailed: SUM the total_gp_trn_gr_st1_hc through total_gp_trn_gr_f1_2_hc columns
   - Available years in the data: 2022, 2023, 2024, 2025

8c. PARTNER vs SALARIED GP TREND:
   - On individual: staff_role contains 'Partner' or 'Provider' for partners; 'Salaried' for salaried
   - CRITICAL: NEVER use exact match (= 'Salaried GP' or = 'GP Partner') — these values do NOT exist!
     The actual values are 'Salaried/Other GPs', 'GP Providers/Partners', etc. ALWAYS use LIKE patterns.
   - Example for salaried vs partner split (ALWAYS follow this pattern):
     SELECT
       COUNT(DISTINCT CASE WHEN staff_role LIKE '%Partner%' OR staff_role LIKE '%Provider%' THEN unique_identifier END) AS partner_hc,
       COUNT(DISTINCT CASE WHEN staff_role LIKE '%Salaried%' THEN unique_identifier END) AS salaried_hc,
       ROUND(SUM(CASE WHEN staff_role LIKE '%Partner%' OR staff_role LIKE '%Provider%' THEN fte ELSE 0 END), 1) AS partner_fte,
       ROUND(SUM(CASE WHEN staff_role LIKE '%Salaried%' THEN fte ELSE 0 END), 1) AS salaried_fte
     FROM individual
     WHERE staff_group = 'GP' AND year = '2025' AND month = '12'
   - Example for trend:
     SELECT year,
       COUNT(DISTINCT CASE WHEN staff_role LIKE '%Partner%' OR staff_role LIKE '%Provider%' THEN unique_identifier END) AS partner_hc,
       COUNT(DISTINCT CASE WHEN staff_role LIKE '%Salaried%' THEN unique_identifier END) AS salaried_hc
     FROM individual
     WHERE staff_group = 'GP' AND month = '12'
     GROUP BY year ORDER BY year
   - On practice_detailed: use total_gp_ptnr_prov_hc/fte and total_gp_sal_by_prac_hc/fte columns

9. PERCENTAGE FILTERING (CRITICAL):
   - When the user asks "practices where more than X% of GPs are..." or "ICBs where at least X%...":
   - You MUST use a HAVING clause or subquery to filter on the computed percentage.
   - Example: "practices where more than 50% of GPs are over 55":
     SELECT prac_name, gp_over_55 / total_gp * 100 AS pct_over_55
     FROM (
       SELECT prac_name,
         CAST(NULLIF(total_gp_hc_55to59, 'NA') AS DOUBLE) + ... AS gp_over_55,
         CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) AS total_gp
       FROM practice_detailed WHERE year='2025' AND month='12'
     ) sub
     WHERE total_gp > 0 AND (gp_over_55 / total_gp * 100) > 50
     ORDER BY pct_over_55 DESC
   - NEVER return all rows and expect the user to filter — always apply the threshold in SQL.
   - Use NULLIF to avoid division by zero: NULLIF(total_gp, 0)

10. DIVISION BY ZERO PROTECTION:
    - Always wrap divisors in NULLIF(..., 0) to prevent division by zero errors.
    - Example: col_a / NULLIF(col_b, 0)
    - For practice_detailed, also handle 'NA' strings: NULLIF(CAST(NULLIF(col, 'NA') AS DOUBLE), 0)

11a. PATIENTS PER GP RATIO (CRITICAL — numerator and denominator order):
    - ALWAYS compute as: SUM(total_patients) / SUM(total_gp_fte)  ← patients ÷ FTE
    - NEVER invert: SUM(total_gp_fte) / SUM(total_patients) gives ~0.001, which is WRONG.
    - On practice_detailed (national/ICB/practice):
        ROUND(SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
              NULLIF(SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)), 0), 1) AS patients_per_gp
    - On individual table (per-person): use COUNT(DISTINCT unique_identifier) as denominator.
    - Typical valid range: 1,000–4,000 patients per GP. A result near 0 means numerator/denominator are swapped.

11b. FTE PER GP RATIO (practice-level):
    - On practice_detailed: ROUND(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) / NULLIF(CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE), 0), 3) AS fte_per_gp
    - For "lowest FTE per GP ratio" → ORDER BY fte_per_gp ASC
    - For "highest FTE per GP ratio" → ORDER BY fte_per_gp DESC
    - MUST filter out NULL/zero headcount: WHERE total_gp_hc IS NOT NULL AND total_gp_hc != 'NA' AND CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) > 0
    - Always include prac_name, icb_name for context
    - Example:
      SELECT prac_name, icb_name,
        CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) AS gp_fte,
        CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) AS gp_hc,
        ROUND(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) / NULLIF(CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE), 0), 3) AS fte_per_gp
      FROM practice_detailed
      WHERE year = '2025' AND month = '12'
        AND total_gp_hc IS NOT NULL AND total_gp_hc != 'NA'
        AND CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) > 0
      ORDER BY fte_per_gp ASC
      LIMIT 20

Return ONLY the SQL query.
"""

FIXER_SYSTEM = """You fix SQL queries for GP Workforce Athena tables.

RULES:
- Return ONLY corrected SQL (no markdown, no backticks).
- Keep it read-only (SELECT/WITH).
- Allowed tables: practice_high, individual, practice_detailed.

COMMON FIXES:
- 0 rows from name mismatch: use LOWER(TRIM(name)) LIKE LOWER('%partial%')
- If ENTITY RESOLUTION GUIDANCE provides a clear top match, use that exact resolved entity value in the corrected SQL.
- Invalid staff_group: use values from the vocabulary provided
- Wrong table: if practice lookup returns 0 rows from individual, use practice_detailed
- practice_high: remember value is string, use CAST(value AS DOUBLE) for math
- Column not found: check the schema provided and use correct column name
- Trend query missing time grouping: add GROUP BY year/month as appropriate.
- Multi-period comparison returns identical values for both periods:
  This means the two periods were MIXED in the same aggregate (AVG/SUM).
  FIX: use CASE WHEN year = 'YYYY' ... END inside the aggregate to separate periods,
  OR use two CTEs joined on the grouping key.
- Multi-period WHERE OR without parentheses (SQL precedence bug — 2022 returns 192,075 instead of ~9,448):
  SYMPTOM: one period returns a hugely inflated number (total of all staff rather than filtered subset).
  FIX: wrap OR conditions in outer parentheses: AND ((year='A' AND month='B') OR (year='C' AND month='D'))
  OR: remove the year-specific OR from WHERE entirely and use `AND month = '12'` with CASE WHEN.
- practice_detailed columns may contain 'NA': always use NULLIF(col, 'NA') before CAST.
- If user asked for "more than X%" or "at least X%" but the query has no HAVING or WHERE to filter:
  Add a HAVING clause or wrap in a subquery with WHERE to enforce the percentage threshold.
- Division by zero: wrap divisors in NULLIF(..., 0).
- FTE per GP ratio returning 0 rows: ensure WHERE filters out NULL and 'NA' headcount values:
  WHERE total_gp_hc IS NOT NULL AND total_gp_hc != 'NA' AND CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) > 0
- Patients-per-GP ratio returning ~0.001: numerator and denominator are INVERTED.
  Fix: ROUND(SUM(total_patients) / NULLIF(SUM(total_gp_fte), 0), 1) — patients ÷ FTE, NOT fte ÷ patients.
- practice_detailed aggregation across multiple practices: SUM or AVG the per-practice total_* columns as appropriate.
  For a single-practice lookup, use the row values directly.
- Multi-period (N years ago) returning 0 rows: check that the year values match actual data.
  Available years typically include 2019-2025. Verify both period years exist in the data.
- Trainee query returning 0 rows: DO NOT use detailed_staff_role with guessed values like
  'GPs in Training Grades' or 'Trainee GPs'. Instead use: staff_role LIKE '%%Training%%'
  This catches all trainee roles reliably.

Return corrected SQL only.
"""

KNOWLEDGE_SYSTEM = """You are a knowledgeable NHS GP Workforce analyst. Answer the user's question
using ONLY the domain notes provided below. Do NOT make up information.

RULES:
- Use the domain notes to answer questions about definitions, methodology, data sources,
  scope, exclusions, comparability, publication details, and similar non-data questions.
- If the domain notes contain the answer, provide it clearly and concisely.
- If the domain notes do NOT contain enough information, say so honestly and suggest
  the user check the official NHS England Digital publication page.
- Sound like a human analyst, not a system message.
- Lead with a direct answer sentence. Use bullets only when listing distinct items.
- Bold key terms when helpful, but do not over-format.
- Keep answers concise: 2-8 lines.
- If the question asks for actual numbers/statistics from the database, say:
  "This question requires querying the database. Please rephrase to ask for specific data,
  e.g. 'Show total GP FTE nationally in the latest month'."
- NEVER invent statistics, dates, or figures not in the domain notes.
- End with a brief suggestion of what the user could ask next.
"""

INTERPRETATION_SYSTEM = """You are a thoughtful NHS GP Workforce analyst helping a user understand a previous result.

RULES:
- Sound natural, calm, and ChatGPT-like rather than mechanical.
- Lead with the practical takeaway.
- If benchmark context is available, explain whether the result is above, below, or around the benchmark.
- Do NOT invent a local causal story. Use cautious wording such as "could reflect", "might indicate", or
  "on its own this does not prove why".
- If the metric is patients-per-GP, explain that lower usually means fewer patients per GP FTE and therefore
  relatively stronger staffing coverage, while higher means more pressure per GP FTE.
- If the metric is raw headcount or FTE, explain that high/low is hard to judge without a benchmark or denominator.
- Keep it concise: usually one short paragraph or a lead sentence plus up to 3 bullets.
"""

SUMMARY_SYSTEM = """You are a helpful NHS GP Workforce analyst providing clear, well-formatted answers.

FORMATTING RULES:
- Lead with the key finding in bold: e.g. "**Total GP FTE is 27,453.2** across England as of August 2024."
- Sound like a human analyst rather than a report template.
- Prefer a short paragraph to repetitive bullets. Use bullets only when there are clearly separate facts.
- For a simple one-number answer, prefer one compact paragraph over a heading plus bullets.
- If showing a comparison, state the difference clearly with direction and magnitude:
  e.g. "**ST3 trainees increased from 0.65 to 0.81 per practice (+24.6%)**"
- For period comparisons (e.g. 2022 vs 2025), always show:
  1. The value in each period
  2. The absolute change (increase/decrease)
  3. The percentage change where meaningful
- If ALL values show zero change across periods, flag this as suspicious and suggest the user
  verify the query — real workforce data rarely shows zero change over multiple years.
- If multiple matching practices/entities, mention all matches and ask user to clarify.
- For trends, describe the direction (increasing/decreasing/stable).
- Round numbers appropriately (FTE to 1 decimal, headcount to whole numbers, percentages to 1 decimal).
- Do NOT invent numbers — only use what's in the preview data.
- NEVER call a total a "national average" unless the preview or SQL explicitly contains an average/benchmark value.
- For benchmark answers, state what the benchmark means, e.g. "average practice", "average ICB", or "average region".
- Keep answers concise: 2-6 lines for simple queries, up to 10 for complex ones.
- Mention the data date naturally in the sentence when you can.
- Only add a separate data source/date note when it genuinely adds clarity.
"""


APPOINTMENTS_PLANNER_SYSTEM = """You are an NHS GP Appointments analytics planner.

You are planning answers for the GP appointments dataset using only these base tables:
- practice
- pcn_subicb

TABLE RULES:
- practice: use for national totals, practice-level totals, top practices, practice-code lookups,
  national trends, national appointment mode breakdowns, and national DNA rate.
- pcn_subicb: use for region, ICB, and sub-ICB geography questions, including totals,
  DNA rate, appointment mode breakdown, and trends.

DOMAIN RULES:
- The core metric is count_of_appointments.
- DNA rate = appointments where appt_status = 'DNA' divided by total appointments in the same scope.
- Appointment mode questions use appt_mode.
- HCP questions use hcp_type.
- Booking lead-time questions use time_between_book_and_appt.
- Keep geography aligned to the user request: national, region, ICB, sub-ICB, or practice.
- Do not use workforce concepts like FTE or headcount here.

AMBIGUITY:
- If geography is genuinely ambiguous, set needs_clarification=true.
- Otherwise prefer a reasonable default and answer.

Output fields:
- in_scope
- table
- intent
- group_by
- filters_needed
- entities_to_resolve
- needs_clarification
- clarification_question
- notes
"""


APPOINTMENTS_SQL_SYSTEM = """You are an expert AWS Athena SQL writer for NHS GP appointments data.

HARD RULES:
- Return SQL only.
- Read-only only.
- Allowed tables: practice, pcn_subicb.
- Use the provided latest year/month exactly.
- Core metric is count_of_appointments.
- For DNA rate, numerator must filter appt_status = 'DNA' and denominator must be total appointments in the same scope.
- For appointment mode breakdowns, group by appt_mode.
- For geography totals:
  - national -> usually practice
  - region / ICB / sub-ICB -> pcn_subicb
  - practice lookup -> practice
- Use LOWER(TRIM(...)) for robust string matching.
- If entity resolution guidance provides a clear top match, use it.
- Do not use workforce-only columns or tables.
"""


APPOINTMENTS_FIXER_SYSTEM = """You fix SQL queries for NHS GP appointments Athena tables.

RULES:
- Return corrected SQL only.
- Keep it read-only.
- Allowed tables: practice, pcn_subicb.

COMMON FIXES:
- Use count_of_appointments, not workforce metrics.
- For geography-specific queries, use pcn_subicb.
- For national or practice-level queries, use practice.
- If a geography filter returns 0 rows, prefer LOWER(TRIM(...)) LIKE with the provided resolved name.
- If the user asked for DNA rate, ensure numerator filters appt_status = 'DNA' and denominator is total appointments.
- If a trend query is missing time grouping, add GROUP BY year, month.
- If a query is about appointment mode, group by appt_mode.
- If a query is about HCP type, filter or group by hcp_type.
"""


APPOINTMENTS_KNOWLEDGE_SYSTEM = """You are a knowledgeable NHS GP Appointments analyst.

Use only the provided domain notes. Answer clearly and concisely.
- Do not invent figures.
- If the notes do not contain the answer, say so.
- Explain appointments concepts such as DNA, appointment mode, HCP type, and geography coverage in plain English.
"""


APPOINTMENTS_SUMMARY_SYSTEM = """You are a helpful NHS GP Appointments analyst providing clear, concise answers.

RULES:
- Lead with the key result in bold.
- Use count/rate language correctly.
- Never call a total an average unless the SQL explicitly calculates an average.
- For DNA rate, make clear it is a percentage of appointments.
- For mode breakdowns, mention the dominant mode and key supporting values.
- If the question is scoped to a geography or practice, explicitly name that geography or practice in the first sentence.
- Keep the answer natural and concise.
"""

WORKFORCE_CONFIG["prompt_systems"] = {
    "planner": PLANNER_SYSTEM,
    "sql": SQL_SYSTEM,
    "fixer": FIXER_SYSTEM,
    "knowledge": KNOWLEDGE_SYSTEM,
    "summary": SUMMARY_SYSTEM,
}

APPOINTMENTS_CONFIG["prompt_systems"] = {
    "planner": APPOINTMENTS_PLANNER_SYSTEM,
    "sql": APPOINTMENTS_SQL_SYSTEM,
    "fixer": APPOINTMENTS_FIXER_SYSTEM,
    "knowledge": APPOINTMENTS_KNOWLEDGE_SYSTEM,
    "summary": APPOINTMENTS_SUMMARY_SYSTEM,
}


def _prompt_profile_for_dataset(dataset: DatasetName) -> str:
    return _dataset_config(dataset).get("prompt_profile", dataset)


def _dataset_label(dataset: DatasetName) -> str:
    if dataset == "cross_dataset":
        return "Cross-dataset GP analytics"
    return str(_dataset_config(dataset).get("label") or ("GP appointments" if dataset == "appointments" else "GP workforce"))


def _appointments_table_choice(question: str, plan: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    q = (question or "").lower()
    group_by = [str(g).lower() for g in ((plan or {}).get("group_by") or [])]
    if extract_practice_code(question):
        return "practice", "practice code lookup belongs in appointments practice table"
    if any(col in group_by for col in ["region_name", "icb_name", "sub_icb_name", "sub_icb_location_name", "pcn_name"]):
        return "pcn_subicb", "grouped appointments geography is best served from pcn_subicb"
    if any(term in q for term in [" icb", "integrated care", "sub-icb", "sub icb", "region", "pcn"]):
        return "pcn_subicb", "appointments geography filter points to pcn_subicb"
    return "practice", "national and practice-level appointments queries default to practice"


def _deterministic_table_choice_for_dataset(question: str, plan: Optional[Dict[str, Any]], dataset: DatasetName) -> Tuple[Optional[str], str]:
    if dataset == "appointments":
        return _appointments_table_choice(question, plan)
    return _deterministic_table_choice(question, plan)


def _planner_system_for_dataset(dataset: DatasetName) -> str:
    return str((_dataset_config(dataset).get("prompt_systems") or {}).get("planner") or PLANNER_SYSTEM)


def _sql_system_for_dataset(dataset: DatasetName) -> str:
    return str((_dataset_config(dataset).get("prompt_systems") or {}).get("sql") or SQL_SYSTEM)


def _fixer_system_for_dataset(dataset: DatasetName) -> str:
    return str((_dataset_config(dataset).get("prompt_systems") or {}).get("fixer") or FIXER_SYSTEM)


def _knowledge_system_for_dataset(dataset: DatasetName) -> str:
    return str((_dataset_config(dataset).get("prompt_systems") or {}).get("knowledge") or KNOWLEDGE_SYSTEM)


def _summary_system_for_dataset(dataset: DatasetName) -> str:
    return str((_dataset_config(dataset).get("prompt_systems") or {}).get("summary") or SUMMARY_SYSTEM)


def _dataset_schema_text(table: str, dataset: DatasetName, max_columns: Optional[int] = None) -> str:
    return external_dataset_schema_text(
        table,
        _dataset_config(dataset),
        get_table_schema=get_table_schema,
        max_columns=max_columns,
    )


def _resolve_entity_fuzzy_with_city_fallback(
    table: str,
    name_col: str,
    user_text: str,
    year: Optional[str] = None,
    month: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Wrapper around resolve_entity_fuzzy that tries city-to-ICB mapping first.

    When the user says "NHS Leeds ICB", the hint is "Leeds" which doesn't appear
    in any icb_name value.  If the raw hint matches a city in _CITY_TO_ICB, try
    the mapped ICB name first (e.g. "west yorkshire") for a much better match.
    """
    results = resolve_entity_fuzzy(table, name_col, user_text, year, month)

    # If we got a strong match (score >= 0.85), no need for fallback
    if results and results[0].get("score", 0) >= 0.85:
        return results

    # Try city-to-ICB fallback for icb_name columns
    if name_col.lower() in ("icb_name",):
        hint_low = (user_text or "").strip().lower()
        # Strip common ICB prefixes to get the city name
        city_key = re.sub(r"^nhs\s+", "", hint_low).strip()
        city_key = re.sub(r"\s+icb$", "", city_key).strip()
        city_key = re.sub(r"\s+integrated\s+care\s+board$", "", city_key).strip()
        mapped_icb = _CITY_TO_ICB.get(city_key) or _CITY_TO_ICB.get(hint_low)
        if mapped_icb:
            mapped_results = resolve_entity_fuzzy(table, name_col, mapped_icb, year, month)
            if mapped_results and mapped_results[0].get("score", 0) > (results[0].get("score", 0) if results else 0):
                logger.info("entity_resolver | city-to-ICB fallback: '%s' → '%s' (score %.2f)",
                            user_text, mapped_results[0].get("value"), mapped_results[0].get("score", 0))
                return mapped_results

    return results


def _resolve_entities_via_config(dataset: DatasetName, question: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    return external_resolve_entities_via_config(
        dataset,
        question,
        plan,
        _dataset_config(dataset),
        specific_entity_hint=_specific_entity_hint,
        looks_like_specific_icb_hint=_looks_like_specific_icb_hint,
        region_column_for_table=_region_column_for_table,
        get_latest_year_month=get_latest_year_month,
        resolve_entity_fuzzy=_resolve_entity_fuzzy_with_city_fallback,
    )


def _dataset_valid_values_block(state: MutableMapping[str, Any], dataset: DatasetName) -> str:
    return external_dataset_valid_values_block(state, _dataset_config(dataset))


# =============================================================================
# Graph nodes
# =============================================================================
_QUERY_REWRITE_HINTS = [
    r"\bgp\s+pressure\b",
    r"\bunderstaffed\b",
    r"\bbad\s+regions?\b",
    r"\bgood\s+or\s+bad\b",
    r"\bhow\s+many\s+does\s+this\s+practice\s+have\b",
    r"\bshow\s+the\s+bad\s+areas\b",
]


def _should_try_query_rewrite(state: StateData) -> bool:
    if state.get("_needs_clarification", False):
        return False
    if state.get("follow_up_context"):
        return False
    q = str(state.get("question") or "").strip()
    if not q:
        return False
    q_low = q.lower()
    if _parse_cross_dataset_request(q):
        return False
    if re.search(r"^(?:show|what about|how about|compare|break|split)\s+(?:this|that|it)\b", q_low):
        return False
    return any(re.search(pattern, q_low) for pattern in _QUERY_REWRITE_HINTS)


def node_query_rewriter(state: StateData) -> StateData:
    """
    Lightweight rewrite step before dataset routing.

    Follows LangGraph's custom-workflow guidance: only rewrite when it helps,
    and keep the original user question in state.
    """
    state["rewritten_question"] = ""
    state["rewrite_notes"] = ""
    if not _should_try_query_rewrite(state):
        return state

    q = str(state.get("question") or "").strip()
    deterministic = _rescue_or_clarify_question(q)
    rewritten = str(deterministic.get("rewritten_question") or "").strip()
    if rewritten and rewritten.lower() != q.lower():
        state["question"] = rewritten
        state["rewritten_question"] = rewritten
        state["rewrite_notes"] = str(deterministic.get("notes") or "deterministic rewrite").strip()
        logger.info("node_query_rewriter | deterministic rewrite -> %r", rewritten[:140])
        return state

    llm = llm_client()
    follow_ctx = state.get("follow_up_context") or {}
    prompt = f"""
CONVERSATION HISTORY:
{state.get("conversation_history", "") or "(first question)"}

USER QUESTION:
{q}

FOLLOW-UP CONTEXT:
{json.dumps(follow_ctx, ensure_ascii=False) if follow_ctx else "None"}

Rewrite the user's question only if it is vague and a clearer analytic request would improve routing or planning.
Preserve the same intended metric, geography, time scope, and dataset.
Do not answer the question. Do not introduce facts that were not implied by the user.
If the question is already clear enough, return should_rewrite=false.
""".strip()

    try:
        decision = llm.with_structured_output(QueryRewriteDecision).invoke([
            SystemMessage(
                content=(
                    "You rewrite NHS analytics questions into clearer forms for downstream routing. "
                    "Only rewrite when it materially improves precision. Preserve user intent exactly."
                )
            ),
            HumanMessage(content=prompt),
        ])
        rewritten = str(decision.rewritten_question or "").strip()
        if decision.should_rewrite and rewritten and rewritten.lower() != q.lower():
            state["question"] = rewritten
            state["rewritten_question"] = rewritten
            state["rewrite_notes"] = str(decision.notes or "llm rewrite").strip()
            logger.info("node_query_rewriter | llm rewrite -> %r", rewritten[:140])
    except Exception as e:
        logger.debug("node_query_rewriter | rewrite skipped after LLM error: %s", str(e)[:120])

    return state


def _narrow_candidate_tables(state: StateData) -> Tuple[List[str], str]:
    dataset = cast(DatasetName, str(state.get("dataset") or "workforce"))
    config = _dataset_config(dataset)
    allowed = list(config.get("allowed_tables") or [])
    plan = state.get("plan") or {}
    question = str(state.get("question") or state.get("original_question") or "")
    preferred, reason = _deterministic_table_choice_for_dataset(question, plan, dataset)

    candidates: List[str] = []
    if preferred and preferred in allowed:
        candidates.append(preferred)

    q_low = question.lower()
    if dataset == "workforce":
        if any(term in q_low for term in ["age", "gender", "ethnicity", "country qualification", "partner", "salaried", "trainee", "trend"]):
            if "individual" in allowed and "individual" not in candidates:
                candidates.append("individual")
        if any(term in q_low for term in ["patients", "practice", "pcn", "sub-icb", "sub icb", "ratio", "benchmark", "compare"]):
            if "practice_detailed" in allowed and "practice_detailed" not in candidates:
                candidates.append("practice_detailed")
        if any(term in q_low for term in ["top", "highest", "lowest", "most", "least", "rank"]) and "practice_high" in allowed:
            if "practice_high" not in candidates:
                candidates.append("practice_high")
    else:
        if any(term in q_low for term in ["region", "icb", "sub-icb", "sub icb", "pcn"]) and "pcn_subicb" in allowed:
            if "pcn_subicb" not in candidates:
                candidates.append("pcn_subicb")
        if any(term in q_low for term in ["practice", "gp code", "top practices", "national", "latest month"]) and "practice" in allowed:
            if "practice" not in candidates:
                candidates.append("practice")

    for table in allowed:
        if table not in candidates:
            candidates.append(table)
        if len(candidates) >= 2:
            break

    return candidates[:2], reason


def get_metric_table_hint_for_semantic_metric(metric_key: str, dataset: str, grain: str = "") -> str:
    if metric_key in {"gp_headcount", "gp_fte", "nurse_fte"}:
        return "individual"
    if metric_key in {"patients_per_gp", "registered_patients"}:
        return "practice_detailed"
    if metric_key in {"appointments_per_gp_fte", "appointments_per_gp_headcount", "appointments_per_nurse_fte", "appointments_per_patient"} or dataset == "cross":
        return "cross_dataset_join"
    if metric_key in {
        "total_appointments",
        "dna_rate",
        "face_to_face_appointments",
        "face_to_face_share",
        "telephone_appointments",
        "telephone_share",
        "video_online_appointments",
        "video_online_share",
        "home_visit_appointments",
        "home_visit_share",
        "within_2_weeks_appointments",
        "within_2_weeks_share",
        "over_2_weeks_appointments",
        "over_2_weeks_share",
    }:
        if grain in {"region", "icb", "sub_icb", "pcn"}:
            return "pcn_subicb"
        return "practice"
    return "individual" if dataset == "workforce" else "practice"


def _has_unresolved_practice_placeholder(question: str) -> bool:
    q_low = str(question or "").lower()
    return any(
        token in q_low
        for token in ("my practice", "our practice", "this practice", "that practice")
    )


def _practice_resolution_placeholder_question() -> str:
    return "Which practice do you mean? Share the exact practice name or ODS code."


def _practice_resolution_source(dataset_hint: str, metric_key: str) -> Dict[str, str]:
    dataset = str(dataset_hint or "").strip().lower()
    appointment_metrics = {
        "total_appointments",
        "dna_rate",
        "face_to_face_appointments",
        "face_to_face_share",
        "telephone_appointments",
        "telephone_share",
        "video_online_appointments",
        "video_online_share",
        "home_visit_appointments",
        "home_visit_share",
        "within_2_weeks_appointments",
        "within_2_weeks_share",
        "over_2_weeks_appointments",
        "over_2_weeks_share",
    }
    if dataset == "appointments" or metric_key in appointment_metrics:
        return {
            "table": "practice",
            "name_col": "gp_name",
            "code_col": "gp_code",
            "pcn_col": "pcn_name",
            "area_col": "sub_icb_location_name",
            "database": APPOINTMENTS_ATHENA_DATABASE,
        }
    return {
        "table": "practice_detailed",
        "name_col": "prac_name",
        "code_col": "prac_code",
        "pcn_col": "pcn_name",
        "area_col": "sub_icb_name",
        "database": ATHENA_DATABASE,
    }


def _practice_candidate_label(candidate: Dict[str, Any]) -> str:
    name = str(candidate.get("name") or "").strip()
    code = str(candidate.get("code") or "").strip().upper()
    area = str(candidate.get("area_name") or "").strip()
    pcn = str(candidate.get("pcn_name") or "").strip()
    extras = [item for item in (area, pcn, code) if item]
    if extras:
        return f"{name} ({', '.join(extras)})"
    return name or code


def _join_readable_options(options: List[str]) -> str:
    cleaned = [str(option).strip() for option in options if str(option).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} or {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, or {cleaned[-1]}"


def _resolve_v9_practice_reference(question: str, dataset_hint: str, metric_key: str) -> Dict[str, Any]:
    question = str(question or "").strip()
    if not question:
        return {"status": "none"}

    explicit_code = extract_practice_code(question)
    if explicit_code:
        return {"status": "resolved", "code": explicit_code.upper(), "name": "", "candidates": []}

    if _has_unresolved_practice_placeholder(question):
        return {
            "status": "placeholder",
            "code": "",
            "name": "",
            "candidates": [],
            "clarification_question": _practice_resolution_placeholder_question(),
        }

    practice_hint = _specific_entity_hint(question, "practice")
    if not practice_hint:
        return {"status": "none"}

    source = _practice_resolution_source(dataset_hint, metric_key)
    table = source["table"]
    name_col = source["name_col"]
    code_col = source["code_col"]
    pcn_col = source["pcn_col"]
    area_col = source["area_col"]
    database = source["database"]

    latest = get_latest_year_month(table, database=database)
    year = latest.get("year")
    month = latest.get("month")

    base_filters = [f"{name_col} IS NOT NULL"]
    if year and month:
        base_filters.insert(0, f"year = '{year}' AND month = '{month}'")

    safe_exact_hint = sanitise_entity_input(practice_hint, name_col)
    candidate_sql = f"""
    SELECT DISTINCT
      {code_col} AS code,
      {name_col} AS name,
      {pcn_col} AS pcn_name,
      {area_col} AS area_name
    FROM {table}
    WHERE {{where_clause}}
    ORDER BY name, area_name, pcn_name, code
    LIMIT 8
    """

    exact_where = " AND ".join(base_filters + [f"LOWER(TRIM({name_col})) = LOWER('{safe_exact_hint}')"])
    exact_df = run_athena_df(candidate_sql.format(where_clause=exact_where), database=database)
    exact_candidates = exact_df.to_dict(orient="records") if not exact_df.empty else []
    if len(exact_candidates) == 1:
        candidate = exact_candidates[0]
        return {
            "status": "resolved",
            "code": str(candidate.get("code") or "").strip().upper(),
            "name": str(candidate.get("name") or "").strip(),
            "candidates": exact_candidates,
        }
    if len(exact_candidates) > 1:
        labels = [_practice_candidate_label(candidate) for candidate in exact_candidates[:3]]
        return {
            "status": "ambiguous",
            "code": "",
            "name": practice_hint,
            "candidates": exact_candidates,
            "clarification_question": f"I found multiple matching practices. Did you mean {_join_readable_options(labels)}?",
        }

    safe_hint = sanitise_entity_input(practice_hint, "search_query")
    like_where = " AND ".join(base_filters + [f"LOWER(TRIM({name_col})) LIKE LOWER('%{safe_hint}%')"])
    like_df = run_athena_df(candidate_sql.format(where_clause=like_where), database=database)
    like_candidates = like_df.to_dict(orient="records") if not like_df.empty else []
    if len(like_candidates) == 1:
        candidate = like_candidates[0]
        return {
            "status": "resolved",
            "code": str(candidate.get("code") or "").strip().upper(),
            "name": str(candidate.get("name") or "").strip(),
            "candidates": like_candidates,
        }
    if len(like_candidates) > 1:
        labels = [_practice_candidate_label(candidate) for candidate in like_candidates[:3]]
        return {
            "status": "ambiguous",
            "code": "",
            "name": practice_hint,
            "candidates": like_candidates,
            "clarification_question": f"I found multiple possible practice matches. Did you mean {_join_readable_options(labels)}?",
        }

    where_sql = " AND ".join(base_filters)
    all_names = list_distinct_values(table, name_col, where_sql=where_sql, limit=2000, database=database)
    fuzzy_matches = fuzzy_match(practice_hint, all_names, threshold=0.45, top_n=5)
    if not fuzzy_matches:
        return {
            "status": "unresolved",
            "code": "",
            "name": practice_hint,
            "candidates": [],
            "clarification_question": "I couldn't confidently match that practice name. Please share the exact practice name or ODS code.",
        }

    best_name = str(fuzzy_matches[0][0] or "").strip()
    best_score = float(fuzzy_matches[0][1] or 0.0)
    second_score = float(fuzzy_matches[1][1] or 0.0) if len(fuzzy_matches) > 1 else 0.0
    safe_best_name = sanitise_entity_input(best_name, name_col)
    best_where = " AND ".join(base_filters + [f"LOWER(TRIM({name_col})) = LOWER('{safe_best_name}')"])
    best_df = run_athena_df(candidate_sql.format(where_clause=best_where), database=database)
    best_candidates = best_df.to_dict(orient="records") if not best_df.empty else []
    if len(best_candidates) == 1 and best_score >= 0.9 and (best_score - second_score) >= 0.05:
        candidate = best_candidates[0]
        return {
            "status": "resolved",
            "code": str(candidate.get("code") or "").strip().upper(),
            "name": str(candidate.get("name") or "").strip(),
            "candidates": best_candidates,
        }

    if best_candidates and best_score >= 0.72:
        labels = [_practice_candidate_label(candidate) for candidate in best_candidates[:3]]
        return {
            "status": "ambiguous",
            "code": "",
            "name": practice_hint,
            "candidates": best_candidates,
            "clarification_question": f"I found a few likely practice matches. Did you mean {_join_readable_options(labels)}?",
        }

    return {
        "status": "unresolved",
        "code": "",
        "name": practice_hint,
        "candidates": [],
        "clarification_question": "I couldn't confidently match that practice name. Please share the exact practice name or ODS code.",
    }


def _practice_code_exists_in_latest(table: str, code_col: str, code: str, database: str) -> bool:
    latest = get_latest_year_month(table, database=database)
    year = str(latest.get("year") or "").strip()
    month = str(latest.get("month") or "").strip()
    if not year or not month or not code:
        return False
    safe_code = sanitise_entity_input(str(code).strip().upper(), "practice_code")
    sql = f"""
    SELECT COUNT(*) AS n
    FROM {table}
    WHERE year = '{year}' AND month = '{month}'
      AND UPPER(TRIM({code_col})) = '{safe_code}'
    """
    df = run_athena_df(sql, database=database)
    if df.empty:
        return False
    try:
        return int(df.iloc[0]["n"]) > 0
    except Exception:
        return False


def _cross_practice_code_is_linked(practice_code: str) -> bool:
    code = str(practice_code or "").strip().upper()
    if not code:
        return False
    appointments_ok = _practice_code_exists_in_latest("practice", "gp_code", code, APPOINTMENTS_ATHENA_DATABASE)
    workforce_ok = _practice_code_exists_in_latest("practice_detailed", "prac_code", code, ATHENA_DATABASE)
    return appointments_ok and workforce_ok


def _resolve_v9_practice_code(question: str, dataset_hint: str, metric_key: str) -> Optional[str]:
    resolution = _resolve_v9_practice_reference(question, dataset_hint, metric_key)
    if resolution.get("status") != "resolved":
        return None
    code = str(resolution.get("code") or "").strip().upper()
    return code or None


def _v9_semantic_request_from_dict(data: Optional[Dict[str, Any]]) -> Optional[V9SemanticRequest]:
    if not isinstance(data, dict):
        return None
    metrics = [str(m).strip() for m in (data.get("metrics") or []) if str(m).strip()]
    if not metrics:
        return None
    try:
        time_data = data.get("time") or {}
        time_scope = V9TimeScope(
            mode=str(time_data.get("mode") or "latest"),
            year=time_data.get("year"),
            month=time_data.get("month"),
        )
        transforms = [
            V9TransformSpec(
                type=str(t.get("type") or "topn"),
                n=t.get("n"),
                order=str(t.get("order") or "desc"),
                scope=t.get("scope"),
            )
            for t in (data.get("transforms") or [])
            if isinstance(t, dict) and t.get("type")
        ]
        compare_data = data.get("compare")
        compare = None
        if isinstance(compare_data, dict) and compare_data.get("dimension"):
            compare = V9CompareSpec(
                dimension=str(compare_data.get("dimension")),
                values=list(compare_data.get("values") or []),
            )
        return V9SemanticRequest(
            metrics=metrics,
            entity_filters=dict(data.get("entity_filters") or {}),
            group_by=list(data.get("group_by") or []),
            time=time_scope,
            transforms=transforms,
            compare=compare,
            clarification_needed=bool(data.get("clarification_needed", False)),
            confidence=str(data.get("confidence") or "medium"),
        )
    except Exception as exc:
        logger.debug("_v9_semantic_request_from_dict | skipped reconstruction: %s", exc)
        return None


def _derive_v9_followup_compiled(
    question: str,
    prior_dict: Optional[Dict[str, Any]],
    dataset_hint: str,
) -> Optional[tuple[V9SemanticRequest, Any]]:
    """Try to merge the current turn with a prior v9 semantic request and compile it."""
    if not USE_SEMANTIC_PATH:
        return None
    prior_request = _v9_semantic_request_from_dict(prior_dict)
    if prior_request is None:
        return None
    merged = derive_followup_semantic_request(
        question,
        prior=prior_request,
        dataset_hint=dataset_hint,
    )
    if merged is None or not merged.metrics:
        return None
    try:
        compiled = v9_compile_request(merged)
    except Exception as exc:
        logger.info("v9_followup | compile failed: %s", exc)
        return None
    return merged, compiled


def _compile_v9_semantic_request(question: str, dataset_hint: str = "") -> tuple[Any, Any] | None:
    question = str(question or "").strip()
    if not USE_SEMANTIC_PATH or not question:
        return None
    semantic_request = parse_semantic_request_deterministic(
        question,
        dataset_hint=dataset_hint,
        practice_name_resolver=_resolve_v9_practice_code,
    )
    if semantic_request is None or not semantic_request.metrics:
        return None
    if any(metric not in SUPPORTED_SEMANTIC_METRICS for metric in semantic_request.metrics):
        return None
    try:
        compiled = v9_compile_request(semantic_request)
    except Exception as exc:
        logger.debug("v9 semantic compile skipped: %s", str(exc)[:160])
        return None
    compiled = _refresh_v9_compiled_latest_periods(compiled)
    return semantic_request, compiled


def _refresh_v9_compiled_latest_periods(compiled: Any) -> Any:
    sql_text = str(getattr(compiled, "sql", "") or "")
    sql_low = sql_text.lower()
    notes = dict(getattr(compiled, "notes", {}) or {})

    has_appt = f'"{APPOINTMENTS_ATHENA_DATABASE}".' in sql_low
    has_workforce = f'"{ATHENA_DATABASE}".' in sql_low

    if has_appt:
        appt_table = "pcn_subicb" if f'"{APPOINTMENTS_ATHENA_DATABASE}".pcn_subicb' in sql_low else "practice"
        latest_appt = get_latest_year_month(appt_table, database=APPOINTMENTS_ATHENA_DATABASE)
        ay, am = latest_appt.get("year"), latest_appt.get("month")
        if ay and am:
            sql_text = re.sub(
                rf"year\s*=\s*'{re.escape(V9_APPOINTMENTS_LATEST['year'])}'\s+AND\s+month\s*=\s*'{re.escape(V9_APPOINTMENTS_LATEST['month'])}'",
                f"year = '{ay}' AND month = '{am}'",
                sql_text,
                flags=re.IGNORECASE,
            )
            notes["appointments_year"] = str(ay)
            notes["appointments_month"] = str(am)
            if getattr(compiled, "dataset", "") == "appointments":
                notes["year"] = str(ay)
                notes["month"] = str(am)

    if has_workforce:
        workforce_table = "practice_detailed" if f'"{ATHENA_DATABASE}".practice_detailed' in sql_low else "individual"
        latest_wf = get_latest_year_month(workforce_table)
        wy, wm = latest_wf.get("year"), latest_wf.get("month")
        if wy and wm:
            sql_text = re.sub(
                rf"year\s*=\s*'{re.escape(V9_WORKFORCE_LATEST['year'])}'\s+AND\s+month\s*=\s*'{re.escape(V9_WORKFORCE_LATEST['month'])}'",
                f"year = '{wy}' AND month = '{wm}'",
                sql_text,
                flags=re.IGNORECASE,
            )
            notes["workforce_year"] = str(wy)
            notes["workforce_month"] = str(wm)
            if getattr(compiled, "dataset", "") == "workforce":
                notes["year"] = str(wy)
                notes["month"] = str(wm)

    if sql_text == getattr(compiled, "sql", "") and notes == dict(getattr(compiled, "notes", {}) or {}):
        return compiled
    return replace(compiled, sql=sql_text, notes=notes)


def _semantic_clarification_question(state: StateData, dataset: str, semantic_request: Any | None = None) -> Optional[str]:
    question = str(state.get("original_question") or state.get("question") or "").strip()
    if not question or dataset not in {"appointments", "workforce"}:
        return None

    follow_ctx = state.get("follow_up_context") or {}
    if _has_unresolved_practice_placeholder(question):
        if str(follow_ctx.get("entity_type") or "").strip().lower() == "practice":
            return None
        return _practice_resolution_placeholder_question()

    if semantic_request is not None:
        entity_filters = dict(getattr(semantic_request, "entity_filters", {}) or {})
        practice_code = str(entity_filters.get("practice_code") or "").strip().upper()
        metric_keys = list(getattr(semantic_request, "metrics", []) or [])
        compiled_dataset = ""
        if metric_keys and metric_keys[0] in {
            "appointments_per_gp_fte",
            "appointments_per_gp_headcount",
            "appointments_per_nurse_fte",
            "appointments_per_patient",
        }:
            compiled_dataset = "cross"
        if compiled_dataset == "cross" and practice_code and not _cross_practice_code_is_linked(practice_code):
            return (
                "I can't calculate that cross-dataset practice metric for this practice in the latest snapshots "
                "because it doesn't have linked records in both datasets. Try another practice code or ask at ICB or region level."
            )

    practice_hint = _specific_entity_hint(question, "practice")
    if not practice_hint:
        return None

    # Guard: only fire practice clarification when the hint actually looks like
    # a practice reference. `_specific_entity_hint` can extract stray words like
    # "retirement" or "eligibility" from general workforce questions; those
    # should never trigger an "I couldn't match that practice" clarification.
    hint_low = practice_hint.strip().lower()
    is_practice_code = bool(re.fullmatch(r"[a-z]\d{5}", hint_low))
    practice_like_tokens = ("practice", "surgery", "medical centre", "health centre", "clinic")
    if not is_practice_code and not any(tok in hint_low for tok in practice_like_tokens):
        return None

    metric_key = ""
    if semantic_request is not None:
        metric_key = str((getattr(semantic_request, "metrics", None) or [""])[0] or "").strip()
    resolution = _resolve_v9_practice_reference(question, dataset, metric_key)
    clarification_question = str(resolution.get("clarification_question") or "").strip()
    if resolution.get("status") in {"placeholder", "ambiguous", "unresolved"} and clarification_question:
        return clarification_question
    return None


def _set_semantic_clarification(state: StateData, clarification_question: str, notes: str) -> None:
    state["_needs_clarification"] = True
    state["_clarification_question"] = clarification_question
    state["plan"] = {
        "in_scope": True,
        "table": None,
        "intent": "clarify",
        "notes": notes,
        "group_by": [],
        "filters_needed": [],
        "entities_to_resolve": [],
    }


def _apply_v9_semantic_result_to_state(state: StateData, semantic_request: Any, compiled: Any) -> None:
    semantic_dataset = str(compiled.dataset or "").strip().lower()
    public_dataset = "cross_dataset" if semantic_dataset == "cross" else semantic_dataset
    entity_filters = dict(getattr(semantic_request, "entity_filters", {}) or {})
    entity_type = ""
    entity_name = ""
    entity_code = ""
    entity_col = ""
    if "practice_code" in entity_filters:
        entity_type = "practice"
        entity_name = str(entity_filters.get("practice_code") or "").strip()
        entity_code = entity_name.upper()
        entity_col = "practice_code"
    elif "icb_name" in entity_filters:
        entity_type = "icb"
        entity_name = str(entity_filters.get("icb_name") or "").strip()
        entity_col = "icb_name"
    elif "region_name" in entity_filters:
        entity_type = "region"
        entity_name = str(entity_filters.get("region_name") or "").strip()
        entity_col = "region_name"
    elif "pcn_name" in entity_filters:
        entity_type = "pcn"
        entity_name = str(entity_filters.get("pcn_name") or "").strip()
        entity_col = "pcn_name"
    elif "sub_icb_name" in entity_filters or "sub_icb_location_name" in entity_filters:
        entity_type = "sub_icb"
        entity_name = str(entity_filters.get("sub_icb_name") or entity_filters.get("sub_icb_location_name") or "").strip()
        entity_col = "sub_icb_name"
    state["sql"] = compiled.sql
    state["dataset"] = public_dataset
    state["semantic_request_v9"] = semantic_request_to_dict(semantic_request)
    state["semantic_path"] = {
        "used": True,
        "compiler": "v9",
        "dataset": compiled.dataset,
        "grain": compiled.grain,
        "metric_keys": compiled.metric_keys,
        "confidence": getattr(semantic_request, "confidence", ""),
    }
    state["plan"] = {
        "in_scope": True,
        "table": get_metric_table_hint_for_semantic_metric(compiled.metric_keys[0], semantic_dataset, str(compiled.grain)),
        "intent": "semantic_metric",
        "group_by": list(semantic_request.group_by),
        "filters_needed": [],
        "entities_to_resolve": [],
        "notes": "SQL compiled via v9 semantic metric path",
    }
    notes = compiled.notes or {}
    group_by = list(getattr(semantic_request, "group_by", []) or [])
    semantic_view = ""
    if group_by == ["appt_mode"]:
        semantic_view = "appointment_mode_breakdown"
    elif group_by == ["hcp_type"]:
        semantic_view = "hcp_type_breakdown"
    elif group_by == ["time_between_book_and_appt"]:
        semantic_view = "booking_lead_time_breakdown"
    elif any(t.type == "trend" for t in getattr(semantic_request, "transforms", []) or []):
        semantic_view = "appointments_trend" if public_dataset == "appointments" else "trend"
    state["latest_year"] = notes.get("year") or notes.get("workforce_year") or notes.get("appointments_year")
    state["latest_month"] = notes.get("month") or notes.get("workforce_month") or notes.get("appointments_month")
    state["semantic_state"] = {
        "dataset": public_dataset,
        "metric": compiled.metric_keys[0],
        "entity_type": entity_type,
        "entity_name": entity_name,
        "entity_code": entity_code,
        "entity_col": entity_col,
        "view": semantic_view,
        "grain": compiled.grain,
        "group_by": group_by,
        "entity_filters": entity_filters,
        "comparison_type": "benchmark" if any(t.type == "benchmark" for t in semantic_request.transforms) else "",
        "view_type": "semantic_metric",
        "semantic_path": "v9",
    }


def _v9_gate_reject_reason(
    state: StateData,
    semantic_request: Any,
    dataset: str,
    compiled: Any | None = None,
) -> str:
    if bool(getattr(semantic_request, "clarification_needed", False)):
        return "clarification_needed"

    confidence = str(getattr(semantic_request, "confidence", "medium") or "medium").strip().lower()
    if confidence != "high":
        return f"confidence_{confidence}"

    question = str(state.get("question") or "").strip()
    if len(re.findall(r"\b[\w-]+\b", question)) > 15:
        return "question_too_long"

    if state.get("follow_up_context"):
        return "follow_up_context_present"

    entity_filters = dict(getattr(semantic_request, "entity_filters", {}) or {})
    for key, value in entity_filters.items():
        if not str(value or "").strip():
            return f"empty_{key}"

    if dataset == "appointments" and "practice_code" not in entity_filters:
        q_low = question.lower()
        if any(token in q_low for token in ("medical centre", "health centre", "surgery", "clinic", "practice")) and " by practice" not in q_low:
            return "unresolved_practice_name"

    compiled_dataset = str(getattr(compiled, "dataset", "") or "").strip().lower()
    if compiled_dataset == "cross":
        practice_code = str(entity_filters.get("practice_code") or "").strip().upper()
        if practice_code and not _cross_practice_code_is_linked(practice_code):
            return "cross_practice_not_linked"
        has_explicit_grain = bool(getattr(semantic_request, "group_by", [])) or bool(entity_filters) or getattr(semantic_request, "compare", None) is not None
        if not has_explicit_grain:
            return "cross_dataset_requires_explicit_grain"

    return ""


def _prior_v9_semantic_request_dict(state: StateData) -> Optional[Dict[str, Any]]:
    follow_ctx = state.get("follow_up_context") or {}
    prior = follow_ctx.get("v9_semantic_request") if isinstance(follow_ctx, dict) else None
    if isinstance(prior, dict) and prior.get("metrics"):
        return prior
    return None


def _try_v9_followup_merge(state: StateData, dataset: str) -> bool:
    """If this turn is a follow-up of a prior v9 turn, merge and compile."""
    prior_dict = _prior_v9_semantic_request_dict(state)
    if prior_dict is None:
        return False
    question = str(state.get("original_question") or state.get("question") or "").strip()
    if not question:
        return False
    merged = _derive_v9_followup_compiled(question, prior_dict, dataset_hint=dataset)
    if merged is None:
        return False
    semantic_request, compiled = merged
    compiled_dataset = str(getattr(compiled, "dataset", "") or "").strip().lower()
    if compiled_dataset not in {"workforce", "appointments", "cross"}:
        return False
    # For the single-worker fast-path, only accept same-dataset follow-ups; cross
    # follow-ups are handled by the cross-dataset branch in node_supervisor_decide.
    if dataset in {"workforce", "appointments"} and compiled_dataset != dataset and compiled_dataset != "cross":
        return False
    if dataset in {"workforce", "appointments"} and compiled_dataset == "cross":
        return False
    _apply_v9_semantic_result_to_state(state, semantic_request, compiled)
    logger.info(
        "v9 semantic follow-up | dataset=%s metric=%s grain=%s prior_metric=%s",
        compiled.dataset,
        compiled.metric_keys[0],
        compiled.grain,
        (prior_dict.get("metrics") or [""])[0],
    )
    return True


def _try_v9_semantic_path(state: StateData) -> bool:
    if not USE_SEMANTIC_PATH:
        return False
    if state.get("sql"):
        return True
    dataset = str(state.get("dataset") or "workforce").strip().lower()
    if dataset not in {"workforce", "appointments"}:
        return False

    question = str(state.get("question") or "").strip()
    if not question:
        return False

    # Follow-up merge: if the prior turn was a high-confidence v9 request, try
    # inheriting its filters instead of rejecting outright on follow_up_context.
    if state.get("follow_up_context") and _try_v9_followup_merge(state, dataset):
        return True

    compiled_request = _compile_v9_semantic_request(question, dataset_hint=dataset)
    if compiled_request is None:
        clarification_question = _semantic_clarification_question(state, dataset)
        if clarification_question:
            _set_semantic_clarification(state, clarification_question, "semantic practice clarification")
            logger.info("v9_gate_clarify | dataset=%s question=%r", dataset, question[:100])
            return True
        logger.info("v9_gate_reject | dataset=%s reason=parse_or_compile_failed", dataset)
        return False
    semantic_request, compiled = compiled_request

    reject_reason = _v9_gate_reject_reason(state, semantic_request, dataset, compiled)
    if reject_reason:
        clarification_question = _semantic_clarification_question(state, dataset, semantic_request=semantic_request)
        if clarification_question:
            _set_semantic_clarification(state, clarification_question, f"semantic gate rejected: {reject_reason}")
            logger.info("v9_gate_clarify | dataset=%s reason=%s", dataset, reject_reason)
            return True
        logger.info("v9_gate_reject | dataset=%s reason=%s", dataset, reject_reason)
        return False

    if compiled.dataset != dataset:
        logger.info(
            "v9_gate_reject | dataset=%s reason=dataset_mismatch compiled=%s",
            dataset,
            compiled.dataset,
        )
        return False

    _apply_v9_semantic_result_to_state(state, semantic_request, compiled)
    logger.info(
        "v9 semantic path | dataset=%s metric=%s grain=%s",
        compiled.dataset,
        compiled.metric_keys[0],
        compiled.grain,
    )
    return True


def node_schema_narrow(state: StateData) -> StateData:
    if state.get("sql") or state.get("_needs_clarification", False):
        return state

    candidates, notes = _narrow_candidate_tables(state)
    state["candidate_tables"] = candidates
    state["schema_narrowing_notes"] = notes
    bundle_parts = []
    for table in candidates:
        bundle_parts.append(f"TABLE: {table}\n{_dataset_schema_text(table, cast(DatasetName, str(state.get('dataset') or 'workforce')), max_columns=80)}")
    state["narrowed_schema_text"] = "\n\n".join(bundle_parts).strip()
    logger.info("node_schema_narrow | dataset=%s candidates=%s", state.get("dataset"), candidates)
    return state


def _infer_viz_plan(state: StateData) -> Dict[str, Any]:
    if state.get("_is_knowledge", False) or int(state.get("_rows", 0) or 0) < 2:
        return {"recommended": False, "reason": "not enough structured data for visualization"}

    plan = dict(state.get("plan") or {})
    sql = str(state.get("sql") or "").lower()
    group_by = [str(g).lower() for g in (plan.get("group_by") or [])]
    semantic_state = dict(state.get("semantic_state") or {})
    metric = str(semantic_state.get("metric") or plan.get("intent") or "").strip()

    if any(g in {"year", "month"} for g in group_by) or re.search(r"group by\s+year|group by\s+month", sql):
        return {
            "recommended": True,
            "chart_type": "line",
            "x": "time",
            "y": metric or "value",
            "reason": "trend/time-series result",
        }

    if semantic_state.get("group_dim") in {"icb", "region"}:
        return {
            "recommended": True,
            "chart_type": "bar",
            "x": str(semantic_state.get("group_dim")),
            "y": metric or "value",
            "reason": "grouped cross-dataset comparison",
        }

    if group_by:
        return {
            "recommended": True,
            "chart_type": "bar",
            "x": group_by[0],
            "y": metric or "value",
            "reason": "grouped categorical comparison",
        }

    return {"recommended": False, "reason": "single-value or ranking answer is better as text/table"}


def node_visualization_plan(state: StateData) -> StateData:
    state["viz_plan"] = _infer_viz_plan(state)
    logger.info("node_visualization_plan | %s", state.get("viz_plan"))
    return state


def node_init(state: StateData) -> StateData:
    # Clear transient run outputs so a durable LangGraph thread can be reused
    # across turns without carrying forward stale SQL, plans, or results.
    state["resolved_entities"] = {}
    state["plan"] = {}
    state["sql"] = ""
    state["df_preview_md"] = ""
    state["answer"] = ""
    state["latest_year"] = None
    state["latest_month"] = None
    state["time_range"] = None
    state["_rows"] = 0
    state["_empty"] = False
    state["attempts"] = int(state.get("attempts", 0))
    state["needs_retry"] = False
    state["last_error"] = None
    state["suggestions"] = []
    state["semantic_state"] = {}
    state["supervisor_mode"] = ""
    state["worker_plan"] = {}
    state["data_worker_answer"] = ""
    state["knowledge_worker_answer"] = ""
    state["rewritten_question"] = ""
    state["rewrite_notes"] = ""
    state["candidate_tables"] = []
    state["schema_narrowing_notes"] = ""
    state["narrowed_schema_text"] = ""
    state["viz_plan"] = {}
    state["semantic_request_v9"] = {}
    state["semantic_path"] = {}
    state["_dataset_routing"] = {}
    state["_query_routing"] = {}
    state["_needs_clarification"] = False
    state["_clarification_question"] = ""
    state["_clarification_resolved"] = False
    state["_followup_intent"] = ""

    # Store original question before enrichment
    state["original_question"] = state["question"]

    sid = state.get("session_id", "")
    logger.info("node_init | session=%s | q='%s'", sid, state["question"][:120])

    current_prefs = MEMORY.get_user_preferences(sid)
    inferred_prefs = _infer_user_preferences(state["question"], current_prefs)
    if inferred_prefs:
        MEMORY.update_user_preferences(sid, inferred_prefs)
    state["user_preferences"] = MEMORY.get_user_preferences(sid)

    # Legacy fallback for clarification resumes created before LangGraph interrupts
    # were introduced, or when interrupts are unavailable.
    pending = MEMORY.get_pending_clarification(sid)
    if pending and not _HAS_LANGGRAPH_INTERRUPT:
        original_q = pending["original_question"]
        clarification_answer = state["question"]  # current message is the user's answer
        # Merge: original question + clarification answer
        merged_q = f"{original_q} ({clarification_answer})"
        logger.info(
            "node_init | clarification resolved: original='%s' answer='%s' merged='%s'",
            original_q[:80], clarification_answer[:80], merged_q[:120],
        )
        state["question"] = merged_q
        state["original_question"] = merged_q
        state["_clarification_resolved"] = True
        MEMORY.clear_pending_clarification(sid)

    # Load conversation history
    state["conversation_history"] = MEMORY.format_for_prompt(sid)

    # Follow-up resolution: detect if this is a follow-up and enrich with previous entity
    # (skip follow-up enrichment if we just resolved a clarification — already enriched)
    if not state["_clarification_resolved"]:
        enriched_q, follow_ctx = resolve_follow_up_context(state["question"], sid)
        state["follow_up_context"] = follow_ctx
        if follow_ctx:
            logger.info("node_init | follow-up detected, entity=%s metric=%s enriched=%r",
                        follow_ctx.get("entity_name"), follow_ctx.get("previous_metric"), enriched_q[:120])
            state["question"] = enriched_q
    else:
        state["follow_up_context"] = None

    dataset_decision = _decide_dataset_route(state.get("original_question", state["question"]), state.get("follow_up_context"))
    state["dataset"] = cast(DatasetName, dataset_decision.get("value") or "workforce")
    state["_dataset_routing"] = dataset_decision

    if state.get("follow_up_context"):
        state["semantic_state"] = _semantic_state_from_context(state["follow_up_context"])

    rescue = _rescue_or_clarify_question(state["question"])
    if rescue.get("rewritten_question"):
        state["question"] = str(rescue["rewritten_question"]).strip()
        logger.info("node_init | rescued question -> %r (%s)", state["question"][:120], rescue.get("notes", ""))
    if rescue.get("clarification_question") and not state.get("_clarification_resolved", False):
        state["_needs_clarification"] = True
        state["_clarification_question"] = str(rescue["clarification_question"]).strip()
        state["plan"] = {
            "in_scope": True,
            "table": None,
            "intent": "clarify",
            "notes": rescue.get("notes", "early clarification"),
            "group_by": [],
            "filters_needed": [],
            "entities_to_resolve": [],
        }
        logger.info("node_init | early clarification triggered: %s", state["_clarification_question"][:120])

    if state.get("follow_up_context"):
        state["_followup_intent"] = _followup_intent(state.get("original_question", ""))

    state["domain_notes"] = retrieve_dataset_domain_notes(state["question"], state["dataset"])
    state["_hard_intent"] = detect_hard_intent(state["original_question"])
    if state["_hard_intent"]:
        logger.info("node_init | hard_intent=%s", state["_hard_intent"])
    state["_is_knowledge"] = False  # default; knowledge_check may override
    return state


def node_dataset_classify(state: StateData) -> StateData:
    decision = _decide_dataset_route(
        state.get("question", state.get("original_question", "")),
        state.get("follow_up_context"),
    )
    dataset = cast(DatasetName, decision.get("value") or "workforce")
    state["dataset"] = dataset
    state["_dataset_routing"] = decision
    state["domain_notes"] = retrieve_dataset_domain_notes(state.get("question", ""), dataset)
    logger.info(
        "node_dataset_classify | dataset=%s confidence=%s source=%s for '%s'",
        dataset,
        decision.get("confidence"),
        decision.get("source"),
        state.get("question", "")[:120],
    )
    return state


_SUPERVISOR_KNOWLEDGE_TERMS = [
    "what does",
    "definition",
    "meaning",
    "policy",
    "methodology",
    "how is",
    "how are",
    "why ",
]


def _extract_top_n(question: str, default: int = 10, cap: int = 25) -> int:
    m = re.search(r"\b(?:top|bottom|lowest|highest|fewest|most)\s+(\d+)\b", question.lower())
    if not m:
        return default
    try:
        return max(1, min(int(m.group(1)), cap))
    except Exception:
        return default


def _parse_cross_dataset_request(question: str, follow_ctx: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    q = str(question or "").strip()
    # Strip follow-up context annotations so entity hint extraction doesn't
    # misinterpret them as entity names (e.g. "(context: table = practice)" is
    # NOT an ICB hint).
    q_clean = re.sub(r"\s*\(context:.*$", "", q).strip() or q
    q_low = q_clean.lower()
    follow_ctx = dict(follow_ctx or {})
    semantic_state = dict(follow_ctx.get("semantic_state") or {})
    is_cross_follow_up = str(semantic_state.get("dataset") or follow_ctx.get("dataset") or "").strip().lower() == "cross_dataset"

    mentions_practice = "practice" in q_low or "practices" in q_low
    appointments_signal = _has_strong_appointments_signal(q_low)
    workforce_signal = _has_strong_workforce_signal(q_low) or bool(re.search(r"\bgps?\b|\bper\s+gp\b", q_low))

    top_n = _extract_top_n(q_clean, default=10)
    order_desc = not any(w in q_low for w in ["lowest", "fewest", "least", "bottom", "worst"])
    gp_basis = "headcount" if any(term in q_low for term in ["headcount", "gp count", "gp headcount"]) else "fte"
    icb_hint = _specific_entity_hint(q_clean, "icb")
    region_hint = _specific_entity_hint(q_clean, "region")
    wants_group_by_icb = bool(re.search(r"\b(?:by|break(?:\s+that|\s+this)?\s+down\s+by|show\s+this\s+by)\s+icb\b", q_low) or re.search(r"\bwhich\s+icbs?\b", q_low))
    wants_group_by_region = bool(re.search(r"\b(?:by|break(?:\s+that|\s+this)?\s+down\s+by|show\s+this\s+by)\s+region\b", q_low) or re.search(r"\bwhich\s+regions?\b", q_low))
    wants_national_average = bool(re.search(r"\bnational\s+(?:average|avg)\b", q_low))

    if not is_cross_follow_up:
        direct_cross_ok = appointments_signal and workforce_signal and (
            mentions_practice or wants_group_by_icb or wants_group_by_region or ("appointments per gp" in q_low or "appointments-per-gp" in q_low)
        )
        if not direct_cross_ok:
            return None

    if (wants_group_by_icb or wants_group_by_region) and (appointments_signal and workforce_signal or is_cross_follow_up):
        group_dim = "region" if wants_group_by_region else "icb"
        previous_kind = str(semantic_state.get("metric") or follow_ctx.get("previous_metric") or "").strip()
        if previous_kind == "appointments_and_gp_count_ranking":
            return {
                "kind": f"appointments_and_gp_count_by_{group_dim}",
                "top_n": top_n,
                "icb_hint": icb_hint,
                "region_hint": region_hint,
            }
        return {
            "kind": f"appointments_per_gp_by_{group_dim}",
            "top_n": top_n,
            "gp_basis": gp_basis if not is_cross_follow_up else str(follow_ctx.get("gp_basis") or semantic_state.get("gp_basis") or gp_basis),
            "icb_hint": icb_hint,
            "region_hint": region_hint,
        }

    if "appointments per gp" in q_low or "appointments-per-gp" in q_low:
        return {
            "kind": "appointments_per_gp_ranking",
            "top_n": top_n,
            "order": "DESC" if order_desc else "ASC",
            "gp_basis": gp_basis,
            "icb_hint": icb_hint,
        }

    if "most appointments" in q_low and any(term in q_low for term in ["fewest gps", "fewest gp", "least gps", "least gp"]):
        return {
            "kind": "appointments_and_gp_count_ranking",
            "top_n": top_n,
            "appointments_order": "DESC",
            "gp_order": "ASC",
            "icb_hint": icb_hint,
        }

    if any(term in q_low for term in ["highest appointments", "top practices"]) and "gp" in q_low:
        gp_order = "ASC" if any(term in q_low for term in ["fewest gp", "least gp", "lowest gp"]) else "DESC"
        return {
            "kind": "appointments_and_gp_count_ranking",
            "top_n": top_n,
            "appointments_order": "DESC",
            "gp_order": gp_order,
            "icb_hint": icb_hint,
        }

    if "appointments" in q_low and any(term in q_low for term in ["gps", "gp count", "gp headcount"]) and any(term in q_low for term in ["compare", "rank", "ranking", "top", "highest", "lowest", "fewest", "most"]):
        return {
            "kind": "appointments_and_gp_count_ranking",
            "top_n": top_n,
            "appointments_order": "DESC" if not any(term in q_low for term in ["fewest appointments", "lowest appointments", "least appointments"]) else "ASC",
            "gp_order": "ASC" if any(term in q_low for term in ["fewest gp", "lowest gp", "least gp"]) else "DESC",
            "icb_hint": icb_hint,
        }

    if is_cross_follow_up:
        previous_kind = str(semantic_state.get("metric") or follow_ctx.get("previous_metric") or "").strip()
        previous_top_n = int(follow_ctx.get("previous_limit") or semantic_state.get("top_n") or top_n or 10)
        previous_gp_basis = str(follow_ctx.get("gp_basis") or semantic_state.get("gp_basis") or "fte").strip().lower() or "fte"
        previous_icb_hint = str(follow_ctx.get("parent_scope_entity_name") or semantic_state.get("parent_scope_entity_name") or icb_hint or "").strip()
        previous_region_hint = str(follow_ctx.get("parent_scope_region_name") or semantic_state.get("parent_scope_region_name") or region_hint or "").strip()
        previous_entity_code = str(follow_ctx.get("previous_entity_code") or semantic_state.get("entity_code") or "").strip().upper()
        previous_group_dim = str(follow_ctx.get("previous_group_dim") or semantic_state.get("group_dim") or "").strip().lower()
        spec: Dict[str, Any]
        if previous_kind == "appointments_per_gp_ranking":
            if wants_national_average and previous_entity_code:
                return {
                    "kind": "appointments_per_gp_benchmark",
                    "practice_code": previous_entity_code,
                    "practice_name": str(follow_ctx.get("entity_name") or semantic_state.get("entity_name") or "").strip(),
                    "gp_basis": previous_gp_basis,
                }
            if wants_group_by_icb:
                return {
                    "kind": "appointments_per_gp_by_icb",
                    "top_n": previous_top_n,
                    "gp_basis": previous_gp_basis,
                    "icb_hint": icb_hint or previous_icb_hint,
                }
            if wants_group_by_region:
                return {
                    "kind": "appointments_per_gp_by_region",
                    "top_n": previous_top_n,
                    "gp_basis": previous_gp_basis,
                    "region_hint": region_hint or previous_region_hint,
                }
            order = str(follow_ctx.get("order") or semantic_state.get("order") or "DESC").upper()
            if any(term in q_low for term in ["lowest", "fewest", "least", "bottom", "worst"]):
                order = "ASC"
            elif any(term in q_low for term in ["highest", "most", "top", "best"]):
                order = "DESC"
            if "headcount" in q_low:
                previous_gp_basis = "headcount"
            elif "fte" in q_low:
                previous_gp_basis = "fte"
            spec = {
                "kind": "appointments_per_gp_ranking",
                "top_n": previous_top_n,
                "order": order,
                "gp_basis": previous_gp_basis,
                "icb_hint": icb_hint or previous_icb_hint,
            }
            if re.search(r"\b(?:top|bottom|lowest|highest|fewest|most)\s+(\d+)\b", q_low):
                spec["top_n"] = _extract_top_n(q, default=previous_top_n)
            return spec

        if previous_kind == "appointments_and_gp_count_ranking":
            if wants_group_by_icb:
                return {
                    "kind": "appointments_and_gp_count_by_icb",
                    "top_n": previous_top_n,
                    "icb_hint": icb_hint or previous_icb_hint,
                }
            if wants_group_by_region:
                return {
                    "kind": "appointments_and_gp_count_by_region",
                    "top_n": previous_top_n,
                    "region_hint": region_hint or previous_region_hint,
                }
            appt_order = str(follow_ctx.get("appointments_order") or semantic_state.get("appointments_order") or "DESC").upper()
            gp_order = str(follow_ctx.get("gp_order") or semantic_state.get("gp_order") or "ASC").upper()
            if any(term in q_low for term in ["lowest", "fewest", "least", "bottom", "worst"]):
                appt_order = "ASC"
                gp_order = "DESC"
            elif any(term in q_low for term in ["highest", "most", "top", "best"]):
                appt_order = "DESC"
                gp_order = "ASC"
            if any(term in q_low for term in ["most gps", "highest gp", "largest gp"]):
                gp_order = "DESC"
            elif any(term in q_low for term in ["fewest gp", "least gp", "lowest gp"]):
                gp_order = "ASC"
            if any(term in q_low for term in ["fewest appointments", "least appointments", "lowest appointments"]):
                appt_order = "ASC"
            elif any(term in q_low for term in ["most appointments", "highest appointments", "top appointments"]):
                appt_order = "DESC"
            spec = {
                "kind": "appointments_and_gp_count_ranking",
                "top_n": previous_top_n,
                "appointments_order": appt_order,
                "gp_order": gp_order,
                "icb_hint": icb_hint or previous_icb_hint,
            }
            if re.search(r"\b(?:top|bottom|lowest|highest|fewest|most)\s+(\d+)\b", q_low):
                spec["top_n"] = _extract_top_n(q, default=previous_top_n)
            return spec

        if previous_kind in {"appointments_per_gp_by_icb", "appointments_per_gp_by_region"}:
            group_dim = "region" if previous_kind.endswith("_region") else "icb"
            previous_entity_name = str(follow_ctx.get("entity_name") or semantic_state.get("entity_name") or "").strip()
            if wants_national_average and previous_entity_name:
                return {
                    "kind": "appointments_per_gp_group_benchmark",
                    "group_dim": group_dim,
                    "entity_name": previous_entity_name,
                    "gp_basis": previous_gp_basis,
                    "icb_hint": icb_hint or previous_icb_hint,
                    "region_hint": region_hint or previous_region_hint,
                }
            order = str(follow_ctx.get("order") or semantic_state.get("order") or "DESC").upper()
            if any(term in q_low for term in ["lowest", "fewest", "least", "bottom", "worst"]):
                order = "ASC"
            elif any(term in q_low for term in ["highest", "most", "top", "best"]):
                order = "DESC"
            return {
                "kind": previous_kind,
                "top_n": _extract_top_n(q, default=previous_top_n),
                "gp_basis": previous_gp_basis,
                "order": order,
                "icb_hint": icb_hint or previous_icb_hint,
                "region_hint": region_hint or previous_region_hint,
            }

        if previous_kind in {"appointments_and_gp_count_by_icb", "appointments_and_gp_count_by_region"}:
            order = str(follow_ctx.get("appointments_order") or semantic_state.get("appointments_order") or "DESC").upper()
            if any(term in q_low for term in ["lowest", "fewest", "least", "bottom", "worst"]):
                order = "ASC"
            elif any(term in q_low for term in ["highest", "most", "top", "best"]):
                order = "DESC"
            return {
                "kind": previous_kind,
                "top_n": _extract_top_n(q, default=previous_top_n),
                "appointments_order": order,
                "gp_order": "ASC" if order == "DESC" else "DESC",
                "icb_hint": icb_hint or previous_icb_hint,
                "region_hint": region_hint or previous_region_hint,
            }

    return None


def _build_cross_dataset_sql(spec: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    appt_latest = get_latest_year_month("practice", database=APPOINTMENTS_ATHENA_DATABASE)
    wf_latest = get_latest_year_month("practice_detailed", database=ATHENA_DATABASE)
    appt_year, appt_month = appt_latest.get("year"), appt_latest.get("month")
    wf_year, wf_month = wf_latest.get("year"), wf_latest.get("month")
    limit_n = int(spec.get("top_n") or 10)

    common_ctes = f"""
WITH appt AS (
  SELECT
    UPPER(TRIM(gp_code)) AS practice_code,
    MAX(TRIM(gp_name)) AS practice_name,
    MAX(TRIM(sub_icb_location_name)) AS sub_icb_name,
    SUM(CAST(count_of_appointments AS DOUBLE)) AS appointments_total
  FROM "{APPOINTMENTS_ATHENA_DATABASE}".practice
  WHERE year = '{appt_year}' AND month = '{appt_month}'
  GROUP BY 1
),
wf AS (
  SELECT
    UPPER(TRIM(prac_code)) AS practice_code,
    TRIM(prac_name) AS workforce_practice_name,
    TRIM(icb_name) AS workforce_icb_name,
    TRIM(region_name) AS workforce_region_name,
    CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) AS gp_headcount,
    CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) AS gp_fte
  FROM "{ATHENA_DATABASE}".practice_detailed
  WHERE year = '{wf_year}' AND month = '{wf_month}'
)
""".strip()
    scope_where = ""
    icb_hint = str(spec.get("icb_hint") or "").strip()
    region_hint = str(spec.get("region_hint") or "").strip()
    if icb_hint:
        icb_safe = sanitise_entity_input(icb_hint, "icb_name")
        scope_where = f"\n  AND LOWER(TRIM(wf.workforce_icb_name)) LIKE LOWER('%{icb_safe}%')"
    elif region_hint:
        region_safe = sanitise_entity_input(region_hint, "region_name")
        scope_where = f"\n  AND LOWER(TRIM(wf.workforce_region_name)) LIKE LOWER('%{region_safe}%')"

    kind = str(spec.get("kind") or "")
    gp_basis = str(spec.get("gp_basis") or "fte").strip().lower()
    ratio_denominator = "wf.gp_headcount" if gp_basis == "headcount" else "wf.gp_fte"
    ratio_alias = "appointments_per_gp_headcount" if gp_basis == "headcount" else "appointments_per_gp_fte"
    joined_cte = f"""
    joined AS (
      SELECT
        appt.practice_code,
        COALESCE(appt.practice_name, wf.workforce_practice_name) AS practice_name,
        wf.workforce_icb_name AS icb_name,
        wf.workforce_region_name AS region_name,
        appt.appointments_total,
        wf.gp_headcount,
        wf.gp_fte,
        ROUND(appt.appointments_total / NULLIF({ratio_denominator}, 0), 1) AS {ratio_alias}
      FROM appt
  JOIN wf ON appt.practice_code = wf.practice_code
  WHERE appt.appointments_total IS NOT NULL
    AND {ratio_denominator} IS NOT NULL
    AND {ratio_denominator} > 0
)
""".strip()
    if kind == "appointments_per_gp_ranking":
        order = str(spec.get("order") or "DESC")
        sql = f"""
{common_ctes},
{joined_cte}
SELECT
  practice_code,
  practice_name,
  icb_name,
  ROUND(appointments_total, 0) AS appointments_total,
  ROUND(gp_headcount, 1) AS gp_headcount,
  ROUND(gp_fte, 1) AS gp_fte,
  {ratio_alias}
FROM joined
WHERE 1=1{scope_where.replace("wf.workforce_icb_name", "icb_name")}
ORDER BY {ratio_alias} {order}, appointments_total DESC
LIMIT {limit_n}
""".strip()
    elif kind == "appointments_per_gp_benchmark":
        practice_code = sanitise_entity_input(str(spec.get("practice_code") or "").strip().upper(), "practice_code")
        sql = f"""
{common_ctes},
{joined_cte},
current_practice AS (
  SELECT *
  FROM joined
  WHERE practice_code = '{practice_code}'
  LIMIT 1
),
benchmark AS (
  SELECT
    AVG({ratio_alias}) AS national_average
  FROM joined
)
SELECT
  current_practice.practice_code,
  current_practice.practice_name,
  current_practice.icb_name,
  ROUND(current_practice.appointments_total, 0) AS appointments_total,
  ROUND(current_practice.gp_headcount, 1) AS gp_headcount,
  ROUND(current_practice.gp_fte, 1) AS gp_fte,
  current_practice.{ratio_alias} AS current_value,
  ROUND(benchmark.national_average, 1) AS national_average,
  ROUND(current_practice.{ratio_alias} - benchmark.national_average, 1) AS difference,
  ROUND(
    100.0 * (current_practice.{ratio_alias} - benchmark.national_average) /
    NULLIF(benchmark.national_average, 0),
    1
  ) AS pct_difference,
  'average linked practice' AS comparison_basis
FROM current_practice
CROSS JOIN benchmark
""".strip()
    elif kind == "appointments_per_gp_by_icb":
        sql = f"""
{common_ctes},
{joined_cte}
SELECT
  icb_name,
  ROUND(SUM(appointments_total), 0) AS appointments_total,
  ROUND(SUM(gp_headcount), 1) AS gp_headcount,
  ROUND(SUM(gp_fte), 1) AS gp_fte,
  ROUND(SUM(appointments_total) / NULLIF(SUM({ratio_denominator.replace('wf.', '')}), 0), 1) AS {ratio_alias}
FROM joined
WHERE icb_name IS NOT NULL
  AND TRIM(icb_name) != ''{scope_where.replace("wf.workforce_icb_name", "icb_name")}
GROUP BY icb_name
ORDER BY {ratio_alias} {str(spec.get("order") or "DESC")}, appointments_total DESC
LIMIT {limit_n}
""".strip()
    elif kind == "appointments_per_gp_by_region":
        sql = f"""
{common_ctes},
{joined_cte}
SELECT
  region_name,
  ROUND(SUM(appointments_total), 0) AS appointments_total,
  ROUND(SUM(gp_headcount), 1) AS gp_headcount,
  ROUND(SUM(gp_fte), 1) AS gp_fte,
  ROUND(SUM(appointments_total) / NULLIF(SUM({ratio_denominator.replace('wf.', '')}), 0), 1) AS {ratio_alias}
FROM joined
WHERE region_name IS NOT NULL
  AND TRIM(region_name) != ''{scope_where.replace("wf.workforce_region_name", "region_name")}
GROUP BY region_name
ORDER BY {ratio_alias} {str(spec.get("order") or "DESC")}, appointments_total DESC
LIMIT {limit_n}
""".strip()
    elif kind == "appointments_and_gp_count_by_icb":
        sql = f"""
{common_ctes},
joined AS (
  SELECT
    appt.practice_code,
    COALESCE(appt.practice_name, wf.workforce_practice_name) AS practice_name,
    wf.workforce_icb_name AS icb_name,
    wf.workforce_region_name AS region_name,
    appt.appointments_total,
    wf.gp_headcount,
    wf.gp_fte
  FROM appt
  JOIN wf ON appt.practice_code = wf.practice_code
  WHERE appt.appointments_total IS NOT NULL
    AND wf.gp_headcount IS NOT NULL
    AND wf.gp_headcount > 0
)
SELECT
  icb_name,
  ROUND(SUM(appointments_total), 0) AS appointments_total,
  ROUND(SUM(gp_headcount), 1) AS gp_headcount,
  ROUND(SUM(gp_fte), 1) AS gp_fte
FROM joined
WHERE icb_name IS NOT NULL
  AND TRIM(icb_name) != ''{scope_where.replace("wf.workforce_icb_name", "icb_name")}
GROUP BY icb_name
ORDER BY appointments_total {str(spec.get("appointments_order") or "DESC")}, gp_headcount {str(spec.get("gp_order") or "ASC")}
LIMIT {limit_n}
""".strip()
    elif kind == "appointments_and_gp_count_by_region":
        sql = f"""
{common_ctes},
joined AS (
  SELECT
    appt.practice_code,
    COALESCE(appt.practice_name, wf.workforce_practice_name) AS practice_name,
    wf.workforce_icb_name AS icb_name,
    wf.workforce_region_name AS region_name,
    appt.appointments_total,
    wf.gp_headcount,
    wf.gp_fte
  FROM appt
  JOIN wf ON appt.practice_code = wf.practice_code
  WHERE appt.appointments_total IS NOT NULL
    AND wf.gp_headcount IS NOT NULL
    AND wf.gp_headcount > 0
)
SELECT
  region_name,
  ROUND(SUM(appointments_total), 0) AS appointments_total,
  ROUND(SUM(gp_headcount), 1) AS gp_headcount,
  ROUND(SUM(gp_fte), 1) AS gp_fte
FROM joined
WHERE region_name IS NOT NULL
  AND TRIM(region_name) != ''{scope_where.replace("wf.workforce_region_name", "region_name")}
GROUP BY region_name
ORDER BY appointments_total {str(spec.get("appointments_order") or "DESC")}, gp_headcount {str(spec.get("gp_order") or "ASC")}
LIMIT {limit_n}
""".strip()
    elif kind == "appointments_per_gp_group_benchmark":
        group_dim = str(spec.get("group_dim") or "icb").strip().lower()
        if group_dim == "region":
            group_col = "region_name"
            comparison_basis = "average region"
        else:
            group_col = "icb_name"
            comparison_basis = "average ICB"
        entity_name = sanitise_entity_input(str(spec.get("entity_name") or "").strip(), group_col)
        sql = f"""
{common_ctes},
{joined_cte},
grouped AS (
  SELECT
    {group_col},
    ROUND(SUM(appointments_total), 0) AS appointments_total,
    ROUND(SUM(gp_headcount), 1) AS gp_headcount,
    ROUND(SUM(gp_fte), 1) AS gp_fte,
    ROUND(SUM(appointments_total) / NULLIF(SUM({ratio_denominator.replace('wf.', '')}), 0), 1) AS {ratio_alias}
  FROM joined
  WHERE {group_col} IS NOT NULL
    AND TRIM({group_col}) != ''{scope_where.replace("wf.workforce_icb_name", "icb_name").replace("wf.workforce_region_name", "region_name")}
  GROUP BY {group_col}
),
current_group AS (
  SELECT *
  FROM grouped
  WHERE LOWER(TRIM({group_col})) = LOWER('{entity_name}')
  LIMIT 1
),
benchmark AS (
  SELECT AVG({ratio_alias}) AS national_average
  FROM grouped
)
SELECT
  current_group.{group_col},
  current_group.appointments_total,
  current_group.gp_headcount,
  current_group.gp_fte,
  current_group.{ratio_alias} AS current_value,
  ROUND(benchmark.national_average, 1) AS national_average,
  ROUND(current_group.{ratio_alias} - benchmark.national_average, 1) AS difference,
  ROUND(
    100.0 * (current_group.{ratio_alias} - benchmark.national_average) /
    NULLIF(benchmark.national_average, 0),
    1
  ) AS pct_difference,
  '{comparison_basis}' AS comparison_basis
FROM current_group
CROSS JOIN benchmark
""".strip()
    else:
        appointments_order = str(spec.get("appointments_order") or "DESC")
        gp_order = str(spec.get("gp_order") or "ASC")
        sql = f"""
{common_ctes}
SELECT
  appt.practice_code,
  COALESCE(appt.practice_name, wf.workforce_practice_name) AS practice_name,
  wf.workforce_icb_name AS icb_name,
  ROUND(appt.appointments_total, 0) AS appointments_total,
  ROUND(wf.gp_headcount, 1) AS gp_headcount,
  ROUND(wf.gp_fte, 1) AS gp_fte
FROM appt
JOIN wf ON appt.practice_code = wf.practice_code
WHERE appt.appointments_total IS NOT NULL
  AND wf.gp_headcount IS NOT NULL
  AND wf.gp_headcount > 0{scope_where}
ORDER BY appointments_total {appointments_order}, gp_headcount {gp_order}
LIMIT {limit_n}
""".strip()

    return sql, {
        "appointments_year": str(appt_year or ""),
        "appointments_month": str(appt_month or ""),
        "workforce_year": str(wf_year or ""),
        "workforce_month": str(wf_month or ""),
    }


def _render_cross_dataset_answer(question: str, df: pd.DataFrame, spec: Dict[str, Any], periods: Dict[str, str]) -> str:
    if df.empty:
        return (
            "**No linked cross-dataset results were found for that question.**\n\n"
            "I could not find practices that matched between the appointments and workforce datasets for the latest periods."
        )

    top = df.iloc[0].to_dict()
    practice_name = str(top.get("practice_name") or "the top matched practice").strip()
    icb_name = str(top.get("icb_name") or "").strip()
    appointments_total = str(top.get("appointments_total") or "").strip()
    gp_headcount = str(top.get("gp_headcount") or "").strip()
    gp_fte = str(top.get("gp_fte") or "").strip()
    appointments_period = f"{periods.get('appointments_month')}/{periods.get('appointments_year')}"
    workforce_period = f"{periods.get('workforce_month')}/{periods.get('workforce_year')}"

    kind = str(spec.get("kind") or "")
    if kind == "appointments_per_gp_benchmark":
        gp_basis = str(spec.get("gp_basis") or "fte").strip().lower()
        basis_label = "GP headcount" if gp_basis == "headcount" else "GP FTE"
        current_value = str(top.get("current_value") or "").strip()
        national_average = str(top.get("national_average") or "").strip()
        pct_difference = str(top.get("pct_difference") or "").strip()
        lead = f"**{practice_name} can be compared with the national average for appointments per {basis_label}.**"
        detail = (
            f"It recorded **{current_value} appointments per {basis_label}** versus a "
            f"**national average of {national_average}** across linked practices."
        )
        if pct_difference:
            detail += f" That is **{pct_difference}%** above or below the national average depending on direction."
    elif kind == "appointments_per_gp_by_icb":
        gp_basis = str(spec.get("gp_basis") or "fte").strip().lower()
        basis_label = "GP headcount" if gp_basis == "headcount" else "GP FTE"
        ratio_key = "appointments_per_gp_headcount" if gp_basis == "headcount" else "appointments_per_gp_fte"
        ratio = str(top.get(ratio_key) or "").strip()
        lead = f"**Here is the cross-dataset appointments-per-{basis_label.lower()} view by ICB.**"
        detail = (
            f"The top ICB in this result is **{icb_name}** with **{appointments_total} appointments** and "
            f"**{ratio} appointments per {basis_label}**."
        )
    elif kind == "appointments_per_gp_by_region":
        gp_basis = str(spec.get("gp_basis") or "fte").strip().lower()
        basis_label = "GP headcount" if gp_basis == "headcount" else "GP FTE"
        ratio_key = "appointments_per_gp_headcount" if gp_basis == "headcount" else "appointments_per_gp_fte"
        ratio = str(top.get(ratio_key) or "").strip()
        region_name = str(top.get("region_name") or "").strip()
        lead = f"**Here is the cross-dataset appointments-per-{basis_label.lower()} view by region.**"
        detail = (
            f"The top region in this result is **{region_name}** with **{appointments_total} appointments** and "
            f"**{ratio} appointments per {basis_label}**."
        )
    elif kind == "appointments_and_gp_count_by_icb":
        lead = f"**Here is the cross-dataset appointments and GP staffing view by ICB.**"
        detail = (
            f"The top ICB in this result is **{icb_name}** with **{appointments_total} appointments**, "
            f"**{gp_headcount} GPs**, and **{gp_fte} GP FTE**."
        )
    elif kind == "appointments_and_gp_count_by_region":
        region_name = str(top.get("region_name") or "").strip()
        lead = f"**Here is the cross-dataset appointments and GP staffing view by region.**"
        detail = (
            f"The top region in this result is **{region_name}** with **{appointments_total} appointments**, "
            f"**{gp_headcount} GPs**, and **{gp_fte} GP FTE**."
        )
    elif kind == "appointments_per_gp_group_benchmark":
        gp_basis = str(spec.get("gp_basis") or "fte").strip().lower()
        basis_label = "GP headcount" if gp_basis == "headcount" else "GP FTE"
        group_dim = str(spec.get("group_dim") or "icb").strip().lower()
        group_name = str(top.get(f"{group_dim}_name") or top.get("icb_name") or top.get("region_name") or "").strip()
        current_value = str(top.get("current_value") or "").strip()
        national_average = str(top.get("national_average") or "").strip()
        pct_difference = str(top.get("pct_difference") or "").strip()
        lead = f"**{group_name} can be compared with the average {group_dim} for appointments per {basis_label}.**"
        detail = (
            f"It recorded **{current_value} appointments per {basis_label}** versus an "
            f"**average {group_dim} value of {national_average}**."
        )
        if pct_difference:
            detail += f" That is **{pct_difference}%** above or below the grouped average depending on direction."
    elif kind == "appointments_per_gp_ranking":
        gp_basis = str(spec.get("gp_basis") or "fte").strip().lower()
        ratio_key = "appointments_per_gp_headcount" if gp_basis == "headcount" else "appointments_per_gp_fte"
        ratio = str(top.get(ratio_key) or "").strip()
        direction = "lowest" if str(spec.get("order") or "DESC").upper() == "ASC" else "highest"
        basis_label = "GP headcount" if gp_basis == "headcount" else "GP FTE"
        lead = f"**{practice_name} has the {direction} appointments-per-{basis_label.lower()} ratio in the linked practice data.**"
        detail = (
            f"It recorded **{appointments_total} appointments** in {appointments_period} and "
            f"**{gp_headcount if gp_basis == 'headcount' else gp_fte} {basis_label}** in {workforce_period}, "
            f"which works out to **{ratio} appointments per {basis_label}**."
        )
    else:
        appt_dir = "high" if str(spec.get("appointments_order") or "DESC").upper() == "DESC" else "low"
        gp_dir = "low" if str(spec.get("gp_order") or "ASC").upper() == "ASC" else "high"
        lead = f"**{practice_name} is the top linked match for {appt_dir} appointments with relatively {gp_dir} GP staffing.**"
        detail = (
            f"It recorded **{appointments_total} appointments** in {appointments_period} alongside **{gp_headcount} GPs** "
            f"and **{gp_fte} GP FTE** in the {workforce_period} workforce snapshot."
        )

    tail = f" It is in **{icb_name}**." if icb_name and kind in {"appointments_per_gp_ranking", "appointments_and_gp_count_ranking"} else ""
    note = (
        f"\n\nThis join uses the latest appointments month (**{appointments_period}**) and the latest workforce month "
        f"(**{workforce_period}**) and matches practices by practice code."
    )
    return f"{lead}\n\n{detail}{tail}{note}"


def _split_supervisor_question(question: str) -> tuple[str, str]:
    q = (question or "").strip()
    parts = re.split(r"\s+(?:and|also)\s+", q, maxsplit=1)
    if len(parts) == 2:
        left, right = parts[0].strip(), parts[1].strip()
        left_low, right_low = left.lower(), right.lower()
        if any(term in right_low for term in _SUPERVISOR_KNOWLEDGE_TERMS):
            return left, right
        if any(term in left_low for term in _SUPERVISOR_KNOWLEDGE_TERMS):
            return right, left
    return q, ""


def _needs_multi_worker_supervision(state: StateData) -> bool:
    route = str(state.get("_query_route") or "")
    if route in {"greeting", "out_of_scope"}:
        return False
    q = str(state.get("original_question") or state.get("question") or "").strip()
    data_q, knowledge_q = _split_supervisor_question(q)
    return bool(data_q and knowledge_q)


def node_supervisor_decide(state: StateData) -> StateData:
    original_q = str(state.get("question") or state.get("original_question") or "").strip()
    state["supervisor_mode"] = "single_worker"
    state["worker_plan"] = {}
    # Phase C: follow-up merge for same-dataset follow-ups of a prior v9 turn.
    if (
        state.get("follow_up_context")
        and not state.get("sql")
        and _try_v9_followup_merge(state, str(state.get("dataset") or "workforce").strip().lower())
    ):
        logger.info(
            "node_supervisor_decide | v9 follow-up merged metric=%s grain=%s",
            (state.get("semantic_state") or {}).get("metric"),
            (state.get("semantic_state") or {}).get("grain"),
        )
        return state
    if not state.get("follow_up_context"):
        semantic_compiled = _compile_v9_semantic_request(
            original_q,
            dataset_hint=str(state.get("dataset") or ""),
        )
        if semantic_compiled is not None:
            semantic_request, compiled = semantic_compiled
            gate_reason = _v9_gate_reject_reason(
                state,
                semantic_request,
                str(state.get("dataset") or ""),
                compiled,
            )
            if gate_reason:
                logger.info(
                    "node_supervisor_decide | semantic precompile rejected reason=%s question=%r",
                    gate_reason,
                    original_q[:100],
                )
            else:
                _apply_v9_semantic_result_to_state(state, semantic_request, compiled)
                if str(compiled.dataset or "").strip().lower() == "cross":
                    state["supervisor_mode"] = "semantic_cross_dataset"
                    state["worker_plan"] = {
                        "full_question": original_q,
                        "semantic_cross_dataset": True,
                    }
                    logger.info(
                        "node_supervisor_decide | mode=semantic_cross_dataset metric=%s grain=%s question=%r",
                        compiled.metric_keys[0],
                        compiled.grain,
                        original_q[:100],
                    )
                    return state
                logger.info(
                    "node_supervisor_decide | precompiled semantic metric=%s dataset=%s grain=%s",
                    compiled.metric_keys[0],
                    compiled.dataset,
                    compiled.grain,
                )
    cross_spec = _parse_cross_dataset_request(original_q, state.get("follow_up_context"))
    if cross_spec:
        state["supervisor_mode"] = "cross_dataset"
        state["worker_plan"] = {
            "cross_dataset_spec": cross_spec,
            "full_question": original_q,
        }
        logger.info(
            "node_supervisor_decide | mode=cross_dataset kind=%s question=%r",
            cross_spec.get("kind"),
            original_q[:100],
        )
        return state
    if not _needs_multi_worker_supervision(state):
        return state

    data_q, knowledge_q = _split_supervisor_question(original_q)
    state["supervisor_mode"] = "multi_worker"
    state["worker_plan"] = {
        "primary_dataset": state.get("dataset", "workforce"),
        "full_question": original_q,
        "data_question": data_q or original_q,
        "knowledge_question": knowledge_q,
        "knowledge_worker": "knowledge_rag_scaffold",
    }
    state["question"] = data_q or original_q
    logger.info(
        "node_supervisor_decide | mode=multi_worker dataset=%s data_q=%r knowledge_q=%r",
        state.get("dataset", "workforce"),
        (data_q or original_q)[:100],
        knowledge_q[:100],
    )
    return state


# =============================================================================
# Knowledge-only detection (conservative)
# =============================================================================
# These patterns indicate the user is asking about methodology, definitions,
# scope, data sources, or publication metadata — NOT requesting data from the DB.
_KNOWLEDGE_KEYWORDS = [
    # Definitions & methodology
    r"\bwhat\s+is\s+(?:an?\s+)?(?:fte|headcount|snapshot|sub.?icb|dpc|direct\s+patient\s+care|"
    r"ad.?hoc\s+locum|partial\s+estimate|nwrs|wmds|pcn|primary\s+care\s+network)\b",
    r"\bwhat\s+does\s+(?:fte|nwrs|wmds|dpc|pcn)\s+(?:stand\s+for|mean)\b",
    r"\bwhat\s+(?:is|are)\s+(?:the\s+)?(?:difference|distinction)\s+between\s+(?:fte|headcount|"
    r"pcn|practice|locum|collected|estimated)\b",
    r"\bdefin(?:e|ition)\s+(?:of\s+)?(?:fte|headcount|snapshot|dpc|locum)\b",
    r"\bhow\s+(?:many|often)\s+(?:hours|is)\b.*\b(?:fte|full.?time)\b",
    r"\bwhat\s+(?:number|amount)\s+of\s+hours\b",
    r"\bwhat\s+is\s+meant\s+by\b",
    r"\bwhy\s+can\s+headcount\s+be\s+(?:higher|greater|more)\b",
    # How is X calculated/measured/derived
    r"\bhow\s+(?:is|are)\s+(?:the\s+)?(?:fte|headcount|data|staff\s+numbers?|estimates?)"
    r"\s+(?:calculated|measured|derived|estimated|determined|worked\s+out|computed)\b",
    r"\bhow\s+(?:is|are)\s+(?:fte|headcount)\s+(?:calculated|measured|defined|determined|computed)\b",
    # Chatbot capabilities & available data
    r"\bwhat\s+(?:data|information|topics?|questions?)\s+(?:is|are)\s+(?:available|supported|covered)\b",
    r"\bwhat\s+(?:can|does)\s+(?:this|the)\s+(?:chatbot|bot|tool|system)\s+(?:do|answer|help|cover|show)\b",
    r"\bwhat\s+(?:kind|type)s?\s+of\s+(?:data|questions?|information)\b.*\b(?:available|ask|answer|have|cover)\b",
    r"\bwhat\s+(?:can\s+i|should\s+i)\s+(?:ask|find|search|look\s+for|query)\b",
    r"\bwhat\s+tables?\s+(?:are|is)\s+(?:available|there|used|in\s+the)\b",
    r"\b(?:which|what)\s+tables?\s+(?:does|do|can)\s+(?:this|the|i)\b",
    # Scope & exclusions
    r"\b(?:does|do|is|are)\s+(?:this|the)\s+(?:publication|dataset|data|series)\s+(?:include|cover|contain)\b",
    r"\b(?:include|exclude|cover|included|excluded|covered)\b.*\b(?:prison|army|walk.?in|hospital|"
    r"dental|pcn|appointments?|prescri|satisfaction)\b",
    r"\b(?:prison|army\s+base|walk.?in\s+cent|urgent\s+treatment)\b.*\b(?:include|exclude|cover|in\s+this)\b",
    r"\bwhat\s+(?:is|are)\s+(?:not\s+)?(?:included|excluded|covered|in\s+scope|out\s+of\s+scope)\b",
    r"\bscope\s+(?:of\s+)?(?:this|the)\s+(?:publication|dataset|data)\b",
    r"\bare\s+(?:walk.?in|prison|army)\b",
    # Data sources & provenance
    r"\bwhat\s+(?:is|are)\s+(?:the\s+)?(?:data\s+source|source\s+of|nwrs|wmds|tis)\b",
    r"\bwhat\s+data\s+source\b",
    r"\bwhere\s+(?:does|do)\s+(?:the|this)\s+data\s+come\s+from\b",
    r"\bwhat\s+(?:system|tool)\s+(?:is|do)\s+(?:used|practices\s+use)\b",
    r"\bwhat\s+(?:data\s+)?sources?\s+(?:feed|go|are\s+used|inform|contribute)\b",
    r"\bwhat\s+feeds?\s+(?:into|the)\s+(?:this|the|gp)\b",
    # Comparability & time series
    r"\b(?:can\s+i|is\s+it\s+possible\s+to)\s+compare\b.*\b(?:before|pre|prior|2012|2013|2014|"
    r"earlier|previous|old)\b",
    r"\bwhen\s+(?:did|does)\s+(?:the\s+)?(?:time\s+series|comparable|series)\s+(?:begin|start)\b",
    r"\bwhy\s+(?:were|are|can.?t|should)\b.*\b(?:comparable|compare|comparison|misleading)\b",
    r"\b(?:quarterly|monthly)\s+(?:to|vs|versus)\s+(?:monthly|quarterly)\b",
    r"\bwhy\s+(?:does|should)\s+(?:nhs|one)\s+recommend\s+year.?(?:on|over).?year\b",
    r"\brecommend\b.*\byear.?(?:on|over).?year\b",
    # Seasonality / fluctuation explanations
    r"\bwhy\s+(?:do|does|are|is)\s+(?:gp\s+)?(?:trainee|registrar|nurse|staff)\s+(?:numbers?|figures?|counts?)\s+(?:fluctuate|vary|change|go\s+up\s+and\s+down)\b",
    r"\bwhy\s+(?:do|does)\s+(?:the\s+)?(?:numbers?|figures?|data)\s+(?:fluctuate|vary|change)\s+(?:so\s+much\s+)?(?:month|between)\b",
    r"\bseasonal(?:ity|ly|\s+(?:pattern|variation|effect|adjustment))\b.*\b(?:workforce|gp|trainee|data)\b",
    r"\b(?:workforce|gp|trainee|data)\b.*\bseasonal(?:ity|ly|\s+(?:pattern|variation|effect))\b",
    r"\bfrom\s+which\s+(?:point|date|period)\b.*\b(?:time\s+series|comparable|series)\b",
    r"\bmonth.?(?:to|over|vs).?month\b.*\b(?:misleading|recommend|year.?over)\b",
    r"\byear.?(?:on|over).?year\b.*\b(?:recommend|better|instead)\b",
    # Publication metadata & files
    r"\bwhat\s+files?\s+(?:are|is)\s+(?:available|included|published|released)\b",
    r"\bis\s+there\s+(?:an?\s+)?(?:individual|practice|interactive|dashboard|csv|excel)\b",
    r"\bwhat\s+(?:file|csv|data)\s+should\s+i\s+use\b",
    r"\b(?:release|publication)\s+(?:date|frequency|schedule)\b",
    r"\bhow\s+often\s+(?:is\s+(?:it|this|the\s+data)\s+)?(?:published|released|updated)\b",
    # Joiners & leavers methodology
    r"\b(?:does|do)\s+(?:the|this)\s+(?:publication|data)\s+(?:provide|have|include|show|contain)"
    r"\b.*\b(?:joiners?|leavers?|leaving|turnover)\b",
    r"\bwhat\s+(?:kinds?|types?)\s+of\s+(?:leaving|leaver|departure)\b",
    r"\breason\s+for\s+leaving\b",
    # Estimation methodology
    r"\bwhat\s+(?:is|are)\s+(?:a\s+)?partial\s+estimate\b",
    r"\bwhat\s+are\s+partial\s+estimates\b",
    r"\bhow\s+are\s+(?:zero.?hours?|missing\s+data|(?:partial\s+)?estimates?|fte)\b.*\b(?:handled|calculated|treated|computed|derived)\b",
    r"\bzero.?hours?\s+contracts?\b.*\b(?:handled|calculated|treated|counted|included)\b",
    r"\bwhy\s+can.?t?\s+(?:i\s+)?(?:just\s+)?(?:sum|add\s+up)\s+practice\b",
    r"\bsum\s+practice.?level\s+headcount\b",
    # General "about this publication" questions
    r"\bwhat\s+is\s+(?:the\s+)?general\s+practice\s+workforce\s+(?:publication|series|data)\b",
    r"\bwhat\s+geographic\s+(?:areas?|coverage|regions?|levels?)\b.*\b(?:does|do|is|covered)\b",
    r"\bwhat\s+workforce\s+(?:groups?|categories|staff)\s+(?:are|is)\s+(?:included|covered)\b",
    r"\bwhat\s+(?:date|day)\s+does\s+(?:the\s+)?(?:monthly\s+)?snapshot\s+represent\b",
    r"\bgeographic\s+hierarchy\b",
    # GP appointments definitions and scope
    r"\bwhat\s+does\s+(?:dna|hcp)\s+(?:stand\s+for|mean)\b",
    r"\bwhat\s+is\s+(?:an?\s+)?(?:dna|appointment\s+mode|hcp\s+type|national\s+category)\b",
    r"\bdefin(?:e|ition)\s+(?:of\s+)?(?:dna|appointment\s+mode|hcp\s+type|national\s+category)\b",
    r"\bwhat\s+does\s+time\s+between\s+book(?:ing)?\s+and\s+appt\b.*\bmean\b",
    r"\bhow\s+is\s+(?:the\s+)?dna\s+rate\s+calculated\b",
    r"\bwhat\s+(?:appointment\s+)?statuses?\s+(?:are|is)\s+(?:used|available|included)\b",
]
_KNOWLEDGE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _KNOWLEDGE_KEYWORDS]

# These patterns mean the user DEFINITELY wants data — never classify as knowledge.
# IMPORTANT: Keep these tight to avoid blocking legitimate knowledge questions.
_DATA_SIGNALS = [
    r"\b(?:total|sum|count|number\s+of|how\s+many)\s+(?:gps?|nurses?|dpc|admin|staff|pharmacists?|trainees?|locums?|registrars?|retainers?|physiotherapists?|paramedics?|practitioners?)\b",
    r"\b(?:current|latest)\s+(?:number|count|total)\s+(?:of\s+)?(?:pharmacists?|nurses?|trainees?|locums?|registrars?|gps?|dpc|admin|staff|physiotherapists?|paramedics?)\b",
    r"\btop\s+\d+\b",
    r"\btrend\b",
    r"\brank\b",
    r"\bcompare\s+(?:\w+\s+){0,3}(?:fte|headcount|practice|icb)\b",
    r"\b(?:show|give|list|display|get)\s+(?:me\s+)?(?:the\s+)?(?:total|all|every|data|numbers?|figures?|stats?)\b",
    r"\b(?:total|how\s+many|number\s+of|count)\s+(?:appointments?|consultations?)\b",
    r"\b(?:dna|did\s+not\s+attend)\b",
    r"\b(?:face[\s-]?to[\s-]?face|telephone|video|online|home\s+visit)\b.*\bappointments?\b",
    r"\b(?:latest|current|last)\s+(?:month|year|quarter)\b.*\b(?:fte|headcount|gp|nurse|staff|dpc|admin)\b",
    # "FTE for/at/by X" (data request), but NOT "FTE in this publication" (knowledge)
    r"\b(?:fte|headcount)\s+(?:for|at|by)\s+(?!this\b)",
    r"\b(?:fte|headcount)\s+in\s+(?!this\b)(?!the\s+(?:publication|series|dataset|data)\b)",
    r"\b(?:breakdown|split|distribution)\s+(?:of|by|for)\b",
    r"\bpractice\s+(?:called|named|like)\b",
    # "at/for [a specific] practice" (data request), but NOT "in the General Practice Workforce" (knowledge)
    r"\b(?:at|for)\s+(?:\w+\s+){0,2}(?:practice|surgery|medical\s+centre|health\s+centre)\b",
    r"\bin\s+(?!the\s+general\s+practice\s+workforce)(?:\w+\s+){0,2}(?:surgery|medical\s+centre|health\s+centre)\b",
    # Correction/refinement follow-ups: user changing metric (FTE→headcount, etc.) — always a data request
    r"\b(?:show|give|use|switch\s+to)\s+(?:me\s+)?(?:the\s+)?(?:headcount|fte|head\s+count)\s+(?:instead|rather|not)\b",
    r"\b(?:instead\s+of|rather\s+than)\s+(?:fte|headcount|head\s+count)\b",
    r"\bheadcount\s+(?:instead|not\s+fte)\b",
    r"\bfte\s+(?:instead|not\s+headcount)\b",
    # Context-enriched follow-ups (from resolve_follow_up_context) — always data
    r"\(context:\s*(?:practice|icb|region|sub_icb|table)\s*=",
]
_DATA_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _DATA_SIGNALS]


def _is_knowledge_only_question(question: str) -> bool:
    """
    Conservative check: returns True ONLY if the question matches knowledge patterns
    AND does NOT match any data-request signals. When in doubt, returns False (→ SQL path).
    """
    q = question.strip()
    q_lower = q.lower()

    if _is_explicit_definition_question(q):
        return True

    if any(term in q_lower for term in [
        "dna rate", "rate of dna", "what is the rate", "percentage of dna",
        "number of appointments", "total appointments", "appointments nationally",
        "appointments in ", "dna in nhs ", "dna in ",
    ]):
        return False

    # Explicit terminology / definition questions should stay on the knowledge path
    # even if they mention terms like DNA, appointment mode, or HCP type.
    if re.search(r"\b(?:what\s+does|what\s+is|define|meaning\s+of)\b", q_lower):
        if any(term in q_lower for term in [
            "dna", "did not attend", "appointment mode", "hcp type", "health care professional",
            "national category", "time between book and appt", "time from booking",
            "fte", "headcount", "arrs",
        ]):
            return True

    # If ANY data signal matches, it's a data question — never knowledge-only
    if any(p.search(q) for p in _DATA_PATTERNS):
        return False

    # If it's a follow-up (very short, or "what about...", "and nurses?"), it needs SQL context
    # BUT: short knowledge questions like "What tables are available" should still pass through
    if len(q.split()) <= 4 and not q.lower().startswith(("what is", "what does", "what are",
                                                          "how is", "how are", "what can",
                                                          "what data", "what table")):
        return False

    # Check knowledge patterns
    if any(p.search(q) for p in _KNOWLEDGE_PATTERNS):
        return True

    return False


def _render_interpretive_followup_answer(question: str, follow_ctx: Dict[str, Any],
                                         preferences: Optional[Dict[str, Any]] = None) -> Tuple[str, List[str]]:
    """
    Deterministic interpretation layer for questions like "why is that?" after a result.
    This keeps the bot conversational and grounded without inventing local causes.
    """
    q_low = (question or "").lower()
    entity_name = str(follow_ctx.get("entity_name") or "").strip()
    entity_type = str(follow_ctx.get("entity_type") or "result").strip()
    metric = str(follow_ctx.get("previous_metric") or "").lower()
    comparison_basis = str(follow_ctx.get("comparison_basis") or "").strip()
    benchmark_value = str(follow_ctx.get("benchmark_value") or "").strip()
    pct_difference = str(follow_ctx.get("pct_difference") or "").strip()
    result_direction = str(follow_ctx.get("result_direction") or "").strip()

    subject = entity_name or f"this {entity_type}".strip()
    response_style = (preferences or {}).get("response_style", "")
    wants_detail = response_style == "detailed" or any(term in q_low for term in ["why", "explain", "good or bad"])

    if metric == "patients_per_gp":
        metric_label = "patients-per-GP ratio"
        polarity_guidance = (
            "For this metric, a lower value usually means fewer patients per GP FTE and therefore relatively stronger staffing coverage, "
            "while a higher value usually points to more pressure per GP FTE."
        )
    elif metric == "fte":
        metric_label = "GP FTE"
        polarity_guidance = (
            "For raw FTE, high or low is hard to judge on its own because population size and service model matter too."
        )
    elif metric == "headcount":
        metric_label = "GP headcount"
        polarity_guidance = (
            "For raw headcount, high or low is hard to judge on its own because population size, practice mix, and local demand matter too."
        )
    else:
        metric_label = "this measure"
        polarity_guidance = "On its own, the number is usually most useful when you compare it with a benchmark or trend."

    if comparison_basis and result_direction:
        direction_map = {
            "above": "above",
            "below": "below",
            "around": "around",
        }
        direction_phrase = direction_map.get(result_direction, result_direction)
        lead = f"**{subject} looks {direction_phrase} the {comparison_basis} on this {metric_label}.**"
        detail_bits = []
        if benchmark_value:
            detail_bits.append(f"The benchmark in the last comparison was **{benchmark_value}**.")
        if pct_difference and pct_difference not in {"0", "0.0", "0.00"}:
            detail_bits.append(f"The gap was **{pct_difference}%**.")
        explanation = (
            f"{polarity_guidance} Even so, that comparison tells us the position, not the cause. "
            "Possible explanations can include local population size, practice mix, recruitment and retention, part-time working patterns, and how services are organised."
        )
        if wants_detail and detail_bits:
            ans = f"{lead} {' '.join(detail_bits)}\n\n{explanation}"
        else:
            ans = f"{lead}\n\n{explanation}"
        suggestions = [
            "Compare this with national average",
            "Show the trend over the last year",
            "Break this down by practice",
        ]
        return ans, suggestions

    lead = f"**That result tells you the level for {subject}, but not the reason on its own.**"
    explanation = (
        f"{polarity_guidance} The next best step is usually to compare it with a benchmark or look at the trend over time."
    )
    if metric in {"fte", "headcount"}:
        explanation += " If you want a stronger judgement, patients-per-GP is often a better pressure indicator than a raw count."
    suggestions = [
        "Compare this with national average",
        "Show the trend over the last year",
        "Show patients-per-GP ratio",
    ]
    if wants_detail:
        explanation += " I can also break it down by geography or staff mix to help explain the pattern."
    return f"{lead}\n\n{explanation}", suggestions


# =============================================================================
# Adaptive Query Routing — LLM-based classifier
# =============================================================================
# Complexity signals for simple vs complex data queries
_SIMPLE_QUERY_SIGNALS = [
    r"^\s*(?:total|how\s+many|number\s+of|count)\s+(?:gps?|nurses?|dpc|admin|staff|trainees?|pharmacists?|practices?|physiotherapists?|paramedics?)",
    r"^\s*(?:show|get|give|list|what\s+is)\s+(?:me\s+)?(?:the\s+)?(?:total|national|overall|latest)\b",
    r"^\s*gp\s+(?:fte|headcount)\s+(?:nationally|in\s+(?:the\s+)?latest)\b",
    r"^\s*(?:total|how\s+many)\s+(?:gp\s+)?practices?\b",
    # "What's the current/total/latest number of pharmacists/nurses/GPs..."
    r"\b(?:current|total|latest)\s+(?:number|count|total)\s+(?:of\s+)?(?:pharmacists?|nurses?|trainees?|locums?|gps?|dpc|admin|staff|physiotherapists?|paramedics?)\b",
]
_SIMPLE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _SIMPLE_QUERY_SIGNALS]

_COMPLEX_QUERY_SIGNALS = [
    r"\btrend\b|\bover\s+(?:the\s+)?last\s+\d+\s+months?\b|\byear\s+over\s+year\b",
    r"\btop\s+\d+\b|\branking?\b|\bworst\b|\bhighest\b|\blowest\b",
    r"\bcompare\b|\bvs\b|\bversus\b|\bratio\b|\bper\s+(?:10k?|capita|patient|gp)\b",
    r"\b(?:group|split|breakdown|distribution)\s+by\s+(?:\w+\s+)?(?:and|,)\s+",
    r"\bpractices?\s+(?:where|with|that\s+have|over|under)\b",
    r"\bcross\b|\bjoin\b|\bcombine\b|\bmerge\b",
    r"\b(?:percentage|proportion|share)\s+(?:of|by)\b",
    r"\b(?:more|fewer|greater|less)\s+than\s+\d+\b",
]
_COMPLEX_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _COMPLEX_QUERY_SIGNALS]

_CLASSIFIER_SYSTEM = """You are a query classifier for a GP Workforce analytics chatbot.
Given a user question across GP workforce and GP appointments data, classify it into EXACTLY one category:

1. "knowledge" — Asks about methodology, definitions, data sources, scope, publication info.
   Does NOT need database queries. Example: "What is FTE?", "How often is the data published?"

2. "data_simple" — A straightforward data question needing ONE table, ONE aggregation, minimal filters.
   Example: "Total GP FTE nationally", "How many practices are there?", "GP headcount in the latest month"

3. "data_complex" — A complex data question needing multiple groupings, comparisons, trends over time,
   rankings, ratios, cross-tabulations, or entity-specific lookups.
   Example: "Top 10 ICBs by GP FTE", "GP headcount trend over 12 months", "Gender breakdown by age band"

4. "out_of_scope" — The question is clearly NOT about GP workforce data (e.g. weather, recipes, salary info, wait times).
   Example: "What is the weather?", "How much do GPs earn?", "Football scores"

Respond with ONLY the category name (one word or two words with underscore). Nothing else.
"""


def _classify_query_route(question: str, hard_intent: Optional[str] = None,
                          is_followup: bool = False) -> str:
    """
    Classify a question into one of: knowledge, data_simple, data_complex, out_of_scope.

    Uses a tiered approach:
      1. Regex fast-path for high-confidence classifications (saves ~1-2s)
      2. Falls back to lightweight LLM classification for ambiguous cases
    """
    q = question.strip()
    q_lower = q.lower()

    # Fast-path 0a: Non-England countries → out_of_scope (data is England-only)
    _NON_ENGLAND = [
        r"\b(?:scotland|scottish|wales|welsh|northern\s+ireland|ni\b)",
        r"\b(?:edinburgh|glasgow|cardiff|belfast|aberdeen|dundee|swansea|newport)\b",
    ]
    if any(re.search(p, q_lower) for p in _NON_ENGLAND):
        # Check there's no other dominant England-focused intent (e.g. "compare England vs Scotland")
        if not any(w in q_lower for w in ["england", "english", "london", "nhs england"]):
            return "out_of_scope"

    # Fast-path 0b: Greetings, thanks, and social niceties → special "greeting" route
    _GREETING_PATTERNS = [
        r"^(?:hi|hello|hey|hiya|howdy|good\s+(?:morning|afternoon|evening))(?:\s|!|\.|,|$)",
        r"^(?:thanks?(?:\s+you)?|thank\s+you|cheers|ta|much\s+appreciated)(?:\s|!|\.|,|$)",
        r"^(?:bye|goodbye|see\s+you|have\s+a\s+good|have\s+a\s+nice)(?:\s|!|\.|,|$)",
        r"^(?:you'?re\s+welcome|no\s+(?:worries|problem)|ok(?:ay)?\s*(?:thanks?|cheers)?)(?:\s|!|\.|,|$)",
        r"^(?:how\s+are\s+you|what'?s\s+up|sup)(?:\s*\??\s*$)",
    ]
    # Only match if the message is short (social niceties are brief)
    if len(q.split()) <= 8 and any(re.search(p, q_lower) for p in _GREETING_PATTERNS):
        return "greeting"

    # Fast-path 1: Hard intents are always simple data queries (template SQL)
    if hard_intent:
        return "data_simple"

    # Fast-path 2: Follow-ups default to data_complex (need context resolution)
    if is_followup:
        return "data_complex"

    # Fast-path 3: Regex knowledge detection (already proven reliable)
    if _is_knowledge_only_question(q):
        return "knowledge"

    # Fast-path 4: Clear simple query patterns
    if any(p.search(q) for p in _SIMPLE_PATTERNS) and not any(p.search(q) for p in _COMPLEX_PATTERNS):
        return "data_simple"

    # Fast-path 5: Clear complex query patterns
    if any(p.search(q) for p in _COMPLEX_PATTERNS):
        return "data_complex"

    # Fast-path 6: Clear out-of-scope patterns (no GP/workforce/NHS signals at all)
    _DOMAIN_WORDS = {"gp", "nurse", "doctor", "practice", "fte", "headcount", "staff",
                     "workforce", "nhs", "icb", "pcn", "dpc", "trainee", "locum",
                     "pharmacist", "admin", "patient", "region", "sub-icb", "sub icb",
                     "physiotherapist", "paramedic", "registrar", "retainer",
                     "physician associate", "clinical pharmacist", "social prescriber",
                     "appointment", "appointments", "consultation", "consultations",
                     "dna", "attended", "telephone", "face-to-face", "online", "video",
                     "health and wellbeing", "care coordinator", "first contact"}
    if not any(dw in q_lower for dw in _DOMAIN_WORDS) and len(q.split()) >= 3:
        # No domain keywords and not super short — likely OOS but let LLM confirm
        pass

    # Fast-path 7: Data signal patterns override — if the question contains a clear
    # data-requesting pattern (e.g. "how many paramedics"), force data route even
    # if LLM might classify it as out_of_scope.  This protects DPC sub-roles
    # that exist in the data but the LLM might not know about.
    if any(p.search(q) for p in _DATA_PATTERNS):
        # Determine simple vs complex based on grouping/comparison signals
        if any(p.search(q) for p in _COMPLEX_PATTERNS):
            return "data_complex"
        return "data_simple"

    # LLM classification for ambiguous cases — using structured output
    try:
        llm = llm_client()
        structured_llm = llm.with_structured_output(QueryClassification)
        classification = structured_llm.invoke([
            SystemMessage(content=_CLASSIFIER_SYSTEM),
            HumanMessage(content=f"Question: {q}"),
        ])
        result = classification.category
        logger.debug("_classify_query_route | structured LLM classified '%s' as %s", q[:60], result)
        return result
    except Exception as e:
        logger.warning("_classify_query_route | structured LLM call failed (%s), defaulting to data_complex", str(e)[:100])
        return "data_complex"


def _classify_query_route_decision(question: str, hard_intent: Optional[str] = None,
                                   is_followup: bool = False) -> RoutingDecision:
    q = question.strip()
    q_lower = q.lower()

    _NON_ENGLAND = [
        r"\b(?:scotland|scottish|wales|welsh|northern\s+ireland|ni\b)",
        r"\b(?:edinburgh|glasgow|cardiff|belfast|aberdeen|dundee|swansea|newport)\b",
    ]
    if any(re.search(p, q_lower) for p in _NON_ENGLAND) and not any(w in q_lower for w in ["england", "english", "london", "nhs england"]):
        return {"value": "out_of_scope", "confidence": "high", "source": "deterministic_rule", "reason": "non-England geography detected"}

    _GREETING_PATTERNS = [
        r"^(?:hi|hello|hey|hiya|howdy|good\s+(?:morning|afternoon|evening))(?:\s|!|\.|,|$)",
        r"^(?:thanks?(?:\s+you)?|thank\s+you|cheers|ta|much\s+appreciated)(?:\s|!|\.|,|$)",
        r"^(?:bye|goodbye|see\s+you|have\s+a\s+good|have\s+a\s+nice)(?:\s|!|\.|,|$)",
        r"^(?:you'?re\s+welcome|no\s+(?:worries|problem)|ok(?:ay)?\s*(?:thanks?|cheers)?)(?:\s|!|\.|,|$)",
        r"^(?:how\s+are\s+you|what'?s\s+up|sup)(?:\s*\??\s*$)",
    ]
    if len(q.split()) <= 8 and any(re.search(p, q_lower) for p in _GREETING_PATTERNS):
        return {"value": "greeting", "confidence": "high", "source": "deterministic_rule", "reason": "social greeting pattern matched"}

    if hard_intent:
        return {"value": "data_simple", "confidence": "high", "source": "deterministic_rule", "reason": "hard intent matched template path"}

    if is_followup:
        return {"value": "data_complex", "confidence": "high", "source": "deterministic_rule", "reason": "follow-up questions need context-aware routing"}

    if _is_knowledge_only_question(q):
        return {"value": "knowledge", "confidence": "high", "source": "deterministic_rule", "reason": "knowledge-only pattern matched"}

    if any(p.search(q) for p in _SIMPLE_PATTERNS) and not any(p.search(q) for p in _COMPLEX_PATTERNS):
        return {"value": "data_simple", "confidence": "high", "source": "deterministic_rule", "reason": "simple data pattern matched without complex modifiers"}

    if any(p.search(q) for p in _COMPLEX_PATTERNS):
        return {"value": "data_complex", "confidence": "high", "source": "deterministic_rule", "reason": "complex comparison/trend/ranking pattern matched"}

    _DOMAIN_WORDS = {"gp", "nurse", "doctor", "practice", "fte", "headcount", "staff",
                     "workforce", "nhs", "icb", "pcn", "dpc", "trainee", "locum",
                     "pharmacist", "admin", "patient", "region", "sub-icb", "sub icb",
                     "physiotherapist", "paramedic", "registrar", "retainer",
                     "physician associate", "clinical pharmacist", "social prescriber",
                     "appointment", "appointments", "consultation", "consultations",
                     "dna", "attended", "telephone", "face-to-face", "online", "video",
                     "health and wellbeing", "care coordinator", "first contact"}

    if any(p.search(q) for p in _DATA_PATTERNS):
        if any(p.search(q) for p in _COMPLEX_PATTERNS):
            return {"value": "data_complex", "confidence": "high", "source": "deterministic_rule", "reason": "clear data signal plus complex modifier matched"}
        return {"value": "data_simple", "confidence": "high", "source": "deterministic_rule", "reason": "clear data signal matched"}

    if not any(dw in q_lower for dw in _DOMAIN_WORDS) and len(q.split()) >= 3:
        # still let LLM arbitrate, but mark this as low-confidence domain routing
        pass

    route = _classify_query_route(q, hard_intent=hard_intent, is_followup=is_followup)
    return {
        "value": route,
        "confidence": "medium" if any(dw in q_lower for dw in _DOMAIN_WORDS) else "low",
        "source": "llm_fallback",
        "reason": "deterministic route was uncertain, used semantic classifier fallback",
    }


def _should_use_fast_simple_route(state: StateData) -> bool:
    """
    Keep the planner skip only for truly obvious national aggregate questions.
    If a question looks entity-scoped, geography-scoped, or otherwise likely to
    benefit from fuzzy matching / clarification, send it through the full path.
    """
    if state.get("follow_up_context") or state.get("_hard_intent"):
        return False

    question = state.get("original_question", state.get("question", ""))
    q_lower = question.lower().strip()
    if not q_lower:
        return False

    scope_keywords = [
        "practice", "prac", "surgery", "medical centre", "health centre",
        "pcn", "icb", "integrated care", "sub icb", "sub-icb", "region",
    ]
    if any(keyword in q_lower for keyword in scope_keywords):
        return False

    if re.search(r"\b(?:by|for|in|at|within|across)\b", q_lower) and "national" not in q_lower:
        return False

    hint = extract_entity_hint(question)
    if hint and hint.strip().lower() != question.strip().lower():
        return False

    obvious_national_terms = {
        "national", "nationally", "england", "overall", "latest", "current", "total",
    }
    if any(term in q_lower for term in obvious_national_terms):
        return True

    return len(q_lower.split()) <= 8


def _candidate_score(candidate: Any) -> float:
    if isinstance(candidate, dict):
        return float(candidate.get("score", candidate.get("similarity", 0.0)) or 0.0)
    return 0.0


def _candidate_value(candidate: Any) -> str:
    if isinstance(candidate, dict):
        return str(candidate.get("value", candidate.get("name", "")) or "")
    return str(candidate or "")


def _format_entity_candidates_for_prompt(resolved_entities: Dict[str, Any], limit: int = 3) -> str:
    """Render entity candidates into a compact human-readable prompt section."""
    if not resolved_entities:
        return "(none)"

    lines = []
    for key, candidates in resolved_entities.items():
        if not isinstance(candidates, list) or not candidates:
            continue
        formatted = []
        for candidate in candidates[:limit]:
            value = _candidate_value(candidate)
            if not value:
                continue
            score = _candidate_score(candidate)
            match_type = candidate.get("match_type", "candidate") if isinstance(candidate, dict) else "candidate"
            if score > 0:
                formatted.append(f"{value} (score={score:.2f}, type={match_type})")
            else:
                formatted.append(f"{value} ({match_type})")
        if formatted:
            lines.append(f"- {key}: " + "; ".join(formatted))
    return "\n".join(lines) if lines else "(none)"


def _clear_entity_matches(resolved_entities: Dict[str, Any]) -> Dict[str, str]:
    """
    Return the best unambiguous entity matches.
    Criteria are intentionally conservative so we only steer SQL generation when
    the top candidate is meaningfully better than the alternatives.
    """
    clear_matches: Dict[str, str] = {}
    for key, candidates in resolved_entities.items():
        if not isinstance(candidates, list) or not candidates:
            continue
        top = candidates[0]
        top_value = _candidate_value(top)
        top_score = _candidate_score(top)
        if not top_value:
            continue

        next_score = _candidate_score(candidates[1]) if len(candidates) > 1 else 0.0
        top_match_type = top.get("match_type", "") if isinstance(top, dict) else ""
        is_clear = (
            top_match_type == "exact"
            or top_score >= 0.98
            or (top_score >= 0.93 and (top_score - next_score) >= 0.04)
        )
        if is_clear:
            clear_matches[key] = top_value
    return clear_matches


def _entity_resolution_guidance(resolved_entities: Dict[str, Any]) -> str:
    summary = _format_entity_candidates_for_prompt(resolved_entities)
    clear_matches = _clear_entity_matches(resolved_entities)
    if clear_matches:
        clear_lines = "\n".join([f"- {key}: use '{value}'" for key, value in clear_matches.items()])
    else:
        clear_lines = "(none)"
    return (
        "Candidate summary:\n"
        f"{summary}\n\n"
        "Clear matches to prefer in SQL if relevant:\n"
        f"{clear_lines}"
    )


def _entity_clarification_question(state: StateData) -> Optional[str]:
    """Ask for clarification when entity matching is weak or genuinely ambiguous."""
    if state.get("_clarification_resolved", False):
        return None

    question = state.get("original_question", state.get("question", ""))
    q_lower = question.lower()
    plan = state.get("plan", {}) or {}
    resolved = state.get("resolved_entities", {}) or {}

    specs = [
        ("prac_name_candidates", "practice", ["practice", "prac", "surgery", "medical centre", "health centre"]),
        ("icb_name_candidates", "ICB", ["icb", "integrated care"]),
        ("sub_icb_name_candidates", "sub-ICB", ["sub icb", "sub-icb"]),
        ("gp_name_candidates", "practice", ["practice", "prac", "surgery", "medical centre", "health centre"]),
        ("sub_icb_location_name_candidates", "sub-ICB", ["sub icb", "sub-icb"]),
        ("region_name_candidates", "region", ["region"]),
        ("pcn_name_candidates", "PCN", ["pcn", "primary care network"]),
    ]
    specific_hints = {
        "prac_name_candidates": _specific_entity_hint(question, "practice"),
        "icb_name_candidates": _specific_entity_hint(question, "icb"),
        "sub_icb_name_candidates": _specific_entity_hint(question, "sub_icb"),
        "gp_name_candidates": _specific_entity_hint(question, "practice"),
        "sub_icb_location_name_candidates": _specific_entity_hint(question, "sub_icb"),
        "region_name_candidates": _specific_entity_hint(question, "region"),
        "pcn_name_candidates": _specific_entity_hint(question, "pcn"),
    }

    for key, label, keywords in specs:
        candidates = resolved.get(key)
        if not isinstance(candidates, list) or not candidates:
            continue

        entity_hint_requested = (
            bool(specific_hints.get(key))
            or (
                key.replace("_candidates", "") in (plan.get("entities_to_resolve") or [])
                and not _has_generic_scope_reference(question, label.lower().replace("-", "_"))
            )
        )
        if not entity_hint_requested:
            continue

        top = candidates[0]
        top_value = _candidate_value(top)
        top_score = _candidate_score(top)
        second_score = _candidate_score(candidates[1]) if len(candidates) > 1 else 0.0

        options = [f"'{_candidate_value(candidate)}'" for candidate in candidates[:3] if _candidate_value(candidate)]
        if not options:
            continue

        ambiguous = len(candidates) > 1 and top_score < 0.98 and abs(top_score - second_score) <= 0.03
        low_confidence = top_score < 0.62

        if ambiguous:
            joined = ", ".join(options[:-1]) + f", or {options[-1]}" if len(options) > 2 else " or ".join(options)
            return f"I found multiple possible {label} matches. Did you mean {joined}?"

        if low_confidence and top_value:
            joined = ", ".join(options[:-1]) + f", or {options[-1]}" if len(options) > 2 else " or ".join(options)
            return f"I couldn't confidently match that {label} name. Did you mean {joined}?"

    return None


def _parse_benchmark_request(question: str) -> Optional[str]:
    q = (question or "").lower().strip()
    if not q:
        return None
    if any(p in q for p in ["national average", "england average", "average nationally"]):
        return "national_average"
    if any(p in q for p in ["national total", "england total", "total nationally"]):
        return "national_total"
    if any(p in q for p in ["average practice", "practice average", "average per practice"]):
        return "practice_average"
    if any(p in q for p in ["average icb", "icb average", "average per icb"]):
        return "icb_average"
    if any(p in q for p in ["regional average", "average region", "average per region"]):
        return "region_average"
    if any(p in q for p in ["pcn average", "average pcn", "average per pcn", "compare to pcn",
                             "compare pcn", "compare to the pcn", "compare with pcn",
                             "compare with the pcn"]):
        return "pcn_average"
    return None


def _metric_and_staff_from_context(follow_ctx: Dict[str, Any]) -> Tuple[str, str]:
    metric = str(follow_ctx.get("previous_metric") or "headcount")
    if metric in {"appointments_total", "dna_rate"}:
        staff_group = str(follow_ctx.get("previous_staff_group") or "")
    else:
        staff_group = str(follow_ctx.get("previous_staff_group") or "GP")
    return metric, staff_group


def _geo_filter_from_context(follow_ctx: Dict[str, Any], table_hint: str = "individual") -> Optional[str]:
    entity_name = str(follow_ctx.get("entity_name") or "").strip().lower()
    entity_type = str(follow_ctx.get("entity_type") or "")
    entity_code = str(follow_ctx.get("previous_entity_code") or "").strip().upper()
    if not entity_name:
        return None

    if entity_type == "city":
        mapped_icb = follow_ctx.get("mapped_icb", _city_to_icb_for_hint(entity_name))
        if mapped_icb:
            return (f"(LOWER(TRIM(icb_name)) LIKE '%{mapped_icb}%'"
                    f" OR LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%')")
        return f"(LOWER(TRIM(icb_name)) LIKE '%{entity_name}%' OR LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%')"
    if entity_type == "icb":
        return f"LOWER(TRIM(icb_name)) LIKE '%{entity_name}%'"
    if entity_type == "sub_icb":
        return f"LOWER(TRIM(sub_icb_name)) LIKE '%{entity_name}%'"
    if entity_type == "region":
        region_col = _region_column_for_table(table_hint)
        return f"LOWER(TRIM({region_col})) LIKE '%{entity_name}%'"
    if entity_type == "practice":
        if entity_code:
            return f"UPPER(TRIM(prac_code)) = '{entity_code}'"
        return f"LOWER(TRIM(prac_name)) LIKE '%{entity_name}%'"
    return None


def _effective_scope_context(follow_ctx: Dict[str, Any], question: str = "") -> Dict[str, Any]:
    if _should_prefer_parent_scope(question, follow_ctx):
        return _parent_scope_context(follow_ctx) or follow_ctx
    return follow_ctx


def _benchmark_basis_for_grain(grain_base: str) -> str:
    mapping = {
        "practice": "average practice",
        "icb": "average ICB",
        "region": "average region",
        "sub_icb": "average sub-ICB",
    }
    return mapping.get(grain_base, "national benchmark")


def _build_benchmark_followup(state: StateData, benchmark_request: str) -> Optional[Dict[str, Any]]:
    follow_ctx = state.get("follow_up_context") or {}
    if not follow_ctx:
        return None

    metric, staff_group = _metric_and_staff_from_context(follow_ctx)
    if not metric or metric == "headcount":
        preferred_metric = str((state.get("user_preferences") or {}).get("preferred_metric") or "").lower()
        if preferred_metric in {"fte", "headcount"}:
            metric = preferred_metric
    grain = str(follow_ctx.get("previous_grain") or "")
    grain_base = grain.split("_", 1)[0] if "_" in grain else grain
    entity_name = str(follow_ctx.get("entity_name") or "")
    entity_type = str(follow_ctx.get("entity_type") or "")

    if benchmark_request == "national_total":
        # Let planner/LLM handle explicit total-vs-total if needed.
        return None

    if benchmark_request in {"practice_average", "icb_average", "region_average"}:
        requested_base = benchmark_request.replace("_average", "")
        if grain_base and requested_base != grain_base:
            readable_current = _benchmark_basis_for_grain(grain_base)
            readable_requested = _benchmark_basis_for_grain(requested_base)
            return {
                "clarification_question": (
                    f"Do you want to compare this {entity_type or 'result'} with the {readable_current}, "
                    f"or instead switch to the {readable_requested}?"
                ),
            }

    # PCN average — only meaningful for practice-level entities
    if benchmark_request == "pcn_average" and grain_base != "practice":
        return {
            "clarification_question": (
                f"PCN average comparison is only available for individual practices. "
                f"Would you like to compare with the national average instead?"
            ),
        }

    if grain_base == "city":
        return {
            "clarification_question": (
                f"To compare {entity_name.title()} with a benchmark, would you like the national total, "
                f"the average sub-ICB area, or the average ICB?"
            ),
        }

    if grain_base not in {"practice", "icb", "region", "sub_icb"}:
        return None

    latest = get_latest_year_month("practice_detailed" if metric == "patients_per_gp" or grain_base == "practice" else "individual")
    y, m = latest.get("year"), latest.get("month")
    if not y or not m:
        return None

    comparison_basis = _benchmark_basis_for_grain(grain_base)

    if grain_base == "practice":
        entity_filter = _geo_filter_from_context(follow_ctx, table_hint="practice_detailed")
        if not entity_filter:
            return None

        if metric == "patients_per_gp":
            current_value_expr = (
                "ROUND(CAST(NULLIF(total_patients, 'NA') AS DOUBLE) / "
                "NULLIF(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE), 0), 1)"
            )
            avg_expr = (
                "AVG(CAST(NULLIF(total_patients, 'NA') AS DOUBLE) / "
                "NULLIF(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE), 0))"
            )
            valid_filter = (
                "total_patients IS NOT NULL AND total_patients != 'NA' AND "
                "total_gp_fte IS NOT NULL AND total_gp_fte != 'NA' AND "
                "CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE) > 0"
            )
            decimals = 1
            label = "patients_per_gp"
        elif metric == "fte":
            current_value_expr = "CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)"
            avg_expr = "AVG(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE))"
            valid_filter = "total_gp_fte IS NOT NULL AND total_gp_fte != 'NA'"
            decimals = 1
            label = "gp_fte"
        else:
            current_value_expr = "CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE)"
            avg_expr = "AVG(CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE))"
            valid_filter = "total_gp_hc IS NOT NULL AND total_gp_hc != 'NA'"
            decimals = 0
            label = "gp_headcount"

        # PCN-scoped benchmark: filter benchmark CTE to same PCN as the entity
        if benchmark_request == "pcn_average":
            pcn_filter = (
                f"pcn_name = (SELECT pcn_name FROM practice_detailed "
                f"WHERE year = '{y}' AND month = '{m}' AND {entity_filter} LIMIT 1)"
            )
            benchmark_where = f"{valid_filter}\n    AND {pcn_filter}"
            comparison_basis = "PCN average"
            avg_label = "pcn_average"
        else:
            benchmark_where = valid_filter
            avg_label = "national_average"

        sql = f"""
WITH current_entity AS (
  SELECT {current_value_expr} AS current_value
  FROM practice_detailed
  WHERE year = '{y}' AND month = '{m}'
    AND {entity_filter}
  LIMIT 1
),
benchmark AS (
  SELECT {avg_expr} AS benchmark_value
  FROM practice_detailed
  WHERE year = '{y}' AND month = '{m}'
    AND {benchmark_where}
)
SELECT
  ROUND(current_value, {decimals}) AS current_value,
  ROUND(benchmark_value, {decimals}) AS {avg_label},
  ROUND(current_value - benchmark_value, {decimals}) AS difference,
  ROUND((current_value - benchmark_value) / NULLIF(benchmark_value, 0) * 100, 1) AS pct_difference,
  '{comparison_basis}' AS comparison_basis,
  '{label}' AS metric
FROM current_entity
CROSS JOIN benchmark
""".strip()
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "comparison",
                "notes": f"Hard override: benchmark comparison for {entity_name} vs {comparison_basis}",
            },
            "sql": sql,
        }

    # ICB / region / sub-ICB comparisons
    group_col_map = {
        "icb": "icb_name",
        "region": _region_column_for_table("individual"),
        "sub_icb": "sub_icb_name",
    }
    group_col = group_col_map.get(grain_base)
    if not group_col:
        return None

    if metric == "patients_per_gp":
        pd_group_col = _region_column_for_table("practice_detailed") if grain_base == "region" else group_col
        entity_filter = _geo_filter_from_context(follow_ctx, table_hint="practice_detailed")
        if not entity_filter:
            return None
        sql = f"""
WITH current_entity AS (
  SELECT
    ROUND(SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
          NULLIF(SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)), 0), 1) AS current_value
  FROM practice_detailed
  WHERE year = '{y}' AND month = '{m}'
    AND {entity_filter}
),
benchmark AS (
  SELECT AVG(metric_value) AS benchmark_value
  FROM (
    SELECT {pd_group_col},
      SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
      NULLIF(SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)), 0) AS metric_value
    FROM practice_detailed
    WHERE year = '{y}' AND month = '{m}'
      AND {pd_group_col} IS NOT NULL AND TRIM({pd_group_col}) != ''
    GROUP BY {pd_group_col}
  ) grouped
)
SELECT
  ROUND(current_value, 1) AS current_value,
  ROUND(benchmark_value, 1) AS national_average,
  ROUND(current_value - benchmark_value, 1) AS difference,
  ROUND((current_value - benchmark_value) / NULLIF(benchmark_value, 0) * 100, 1) AS pct_difference,
  '{comparison_basis}' AS comparison_basis,
  'patients_per_gp' AS metric
FROM current_entity
CROSS JOIN benchmark
""".strip()
        return {
            "plan": {
                "in_scope": True,
                "table": "practice_detailed",
                "intent": "comparison",
                "notes": f"Hard override: benchmark comparison for {entity_name} vs {comparison_basis}",
            },
            "sql": sql,
        }

    entity_filter = _geo_filter_from_context(follow_ctx, table_hint="individual")
    if not entity_filter:
        return None
    agg_expr = "ROUND(SUM(fte), 1)" if metric == "fte" else "COUNT(DISTINCT unique_identifier)"
    benchmark_agg_expr = "SUM(fte)" if metric == "fte" else "COUNT(DISTINCT unique_identifier)"
    decimals = 1 if metric == "fte" else 0
    metric_label = "gp_fte" if metric == "fte" else "gp_headcount"
    sql = f"""
WITH current_entity AS (
  SELECT {agg_expr} AS current_value
  FROM individual
  WHERE year = '{y}' AND month = '{m}'
    AND staff_group = '{staff_group}'
    AND {entity_filter}
),
benchmark AS (
  SELECT AVG(metric_value) AS benchmark_value
  FROM (
    SELECT {group_col},
      {benchmark_agg_expr} AS metric_value
    FROM individual
    WHERE year = '{y}' AND month = '{m}'
      AND staff_group = '{staff_group}'
      AND {group_col} IS NOT NULL AND TRIM({group_col}) != ''
    GROUP BY {group_col}
  ) grouped
)
SELECT
  ROUND(current_value, {decimals}) AS current_value,
  ROUND(benchmark_value, {decimals}) AS national_average,
  ROUND(current_value - benchmark_value, {decimals}) AS difference,
  ROUND((current_value - benchmark_value) / NULLIF(benchmark_value, 0) * 100, 1) AS pct_difference,
  '{comparison_basis}' AS comparison_basis,
  '{metric_label}' AS metric
FROM current_entity
CROSS JOIN benchmark
""".strip()
    return {
        "plan": {
            "in_scope": True,
            "table": "individual",
            "intent": "comparison",
            "notes": f"Hard override: benchmark comparison for {entity_name} vs {comparison_basis}",
        },
        "sql": sql,
    }


def _detect_sql_semantic_issues(state: MutableMapping[str, Any]) -> List[str]:
    return external_detect_sql_semantic_issues(
        state,
        dataset_config=_dataset_config,
        clear_entity_matches=_clear_entity_matches,
    )


def node_knowledge_check(state: StateData) -> StateData:
    """
    Adaptive routing node: classifies the query and sets _query_route + _is_knowledge.
    Routes:
      - greeting      → friendly response node (skip SQL entirely)
      - knowledge     → knowledge_answer node (skip SQL entirely)
      - data_simple   → streamlined SQL path (skip planner + entity resolution)
      - data_complex  → full planning pipeline
      - out_of_scope  → handled by planner marking in_scope=False → summarize
    """
    q = state.get("question") or state.get("original_question", "")
    followup_intent = state.get("_followup_intent", "")
    follow_ctx = state.get("follow_up_context") or {}

    if _needs_multi_worker_supervision(state):
        state["_query_route"] = "data_complex"
        state["_query_routing"] = {
            "value": "data_complex",
            "confidence": "high",
            "source": "supervisor_rule",
            "reason": "question requires coordinated multi-worker handling",
        }
        state["_is_knowledge"] = False
        logger.info("node_knowledge_check | route=supervisor_multi_worker for '%s'", q[:120])
        return state

    if follow_ctx and followup_intent == "explanation":
        state["_query_route"] = "knowledge"
        state["_query_routing"] = {
            "value": "knowledge",
            "confidence": "high",
            "source": "followup_rule",
            "reason": "interpretive follow-up should stay on the knowledge/explanation path",
        }
        state["_is_knowledge"] = True
        logger.info("node_knowledge_check | route=knowledge for interpretive follow-up '%s'", q[:120])
        return state

    if follow_ctx and followup_intent == "benchmark_probe" and str(follow_ctx.get("previous_grain") or "").startswith("national_"):
        state["_query_route"] = "knowledge"
        state["_query_routing"] = {
            "value": "knowledge",
            "confidence": "high",
            "source": "followup_rule",
            "reason": "national benchmark probe is interpretive rather than a fresh SQL request",
        }
        state["_is_knowledge"] = True
        logger.info("node_knowledge_check | route=knowledge for national benchmark probe '%s'", q[:120])
        return state

    # Classify using adaptive routing
    is_followup = bool(follow_ctx)
    route_decision = _classify_query_route_decision(q, hard_intent=state.get("_hard_intent"), is_followup=is_followup)
    route = str(route_decision.get("value") or "data_complex")

    state["_query_route"] = route
    state["_query_routing"] = route_decision
    state["_is_knowledge"] = (route in ("knowledge", "greeting"))

    # If the classifier confidently detected out_of_scope (e.g. Scotland, weather),
    # pre-set the plan so the planner doesn't accidentally generate SQL for it
    if route == "out_of_scope":
        state["plan"] = {
            "in_scope": False,
            "table": None,
            "intent": "out_of_scope",
            "notes": "Classified as out-of-scope by fast-path classifier",
            "group_by": [],
            "filters_needed": [],
            "entities_to_resolve": [],
        }

    logger.info(
        "node_knowledge_check | route=%s confidence=%s source=%s for '%s'",
        route,
        route_decision.get("confidence"),
        route_decision.get("source"),
        q[:120],
    )
    return state


def node_knowledge_answer(state: StateData) -> StateData:
    """
    Answer a knowledge/methodology question directly from domain notes.
    No SQL is executed. The LLM uses the domain notes as its knowledge base.
    Also handles greetings/thanks with friendly responses (no LLM call needed).
    """
    q = state.get("original_question", state["question"])
    dataset = state.get("dataset", "workforce")
    dataset_label = _dataset_label(dataset)
    route = state.get("_query_route", "")
    followup_intent = state.get("_followup_intent", "")
    follow_ctx = state.get("follow_up_context") or {}

    # ── Greeting / thanks fast-path: no LLM call needed ──
    if route == "greeting":
        logger.info("node_knowledge_answer | greeting detected, responding with friendly message")
        q_lower = q.lower().strip()

        if any(w in q_lower for w in ["thank", "cheers", "ta ", "appreciated"]):
            ans = ("You're welcome! I'm here whenever you need GP workforce data. "
                   "Feel free to ask another question anytime.")
            sugg = [
                "Show total GP FTE nationally",
                "Top 10 practices by GP headcount",
                "GP age distribution breakdown",
            ]
        elif any(w in q_lower for w in ["bye", "goodbye", "see you"]):
            ans = "Goodbye! Feel free to come back anytime you need GP workforce insights."
            sugg = []
        else:
            # Generic greeting (hello, hi, hey, good morning, etc.)
            ans = ("Hello! I'm the NHS primary care data chatbot. I can help you explore "
                   "GP workforce and GP appointments data across England, including staff numbers, "
                   "appointments activity, geography breakdowns, and trends over time.\n\n"
                   "Try asking me something like:")
            sugg = [
                "How many GPs are there in England?",
                "Show total appointments nationally in the latest month",
                "Show GP age distribution",
            ]

        state["answer"] = _polish_answer_text(ans)
        state["sql"] = ""
        state["df_preview_md"] = ""
        state["plan"] = {"in_scope": True, "table": None, "intent": "greeting", "notes": "social greeting/thanks"}
        state["_rows"] = 0
        state["_empty"] = False
        state["suggestions"] = sugg

        MEMORY.add_turn(state.get("session_id", ""), q, ans, sql="", entity_context=None)
        return state

    # ── Interpretive follow-up path: explain a previous result without inventing new data ──
    if follow_ctx and followup_intent in {"explanation", "benchmark_probe"}:
        ans, sugg = _render_interpretive_followup_answer(
            q,
            follow_ctx,
            preferences=state.get("user_preferences"),
        )
        state["answer"] = _polish_answer_text(ans)
        state["sql"] = ""
        state["df_preview_md"] = ""
        state["plan"] = {"in_scope": True, "table": None, "intent": "knowledge", "notes": "interpretive follow-up"}
        state["_rows"] = 0
        state["_empty"] = False
        state["suggestions"] = sugg
        MEMORY.add_turn(state.get("session_id", ""), q, state["answer"], sql="", entity_context=follow_ctx)
        logger.info("node_knowledge_answer | answered via interpretive follow-up path")
        return state

    # ── Knowledge question path ──
    logger.info("node_knowledge_answer | answering from domain notes")

    q_lower = q.lower().strip()
    if "data sources" in q_lower or ("what sources" in q_lower and "chatbot" in q_lower):
        if dataset == "appointments":
            ans = (
                "**This chatbot uses NHS England GP appointments activity data.**\n\n"
                "- The current appointments pipeline is grounded in the **GP Appointments in General Practice** dataset.\n"
                "- In `v8`, appointments answers are generated from the main **practice** and **pcn_subicb** tables.\n"
                "- Those tables support national, regional, ICB, sub-ICB, PCN, and practice-level appointment activity questions."
            )
            suggestions = [
                "What does DNA mean in the appointments data?",
                "Show total appointments nationally in the latest month",
                "Show appointment mode breakdown nationally",
            ]
        else:
            ans = (
                "**This chatbot uses NHS England GP workforce source data from several linked systems.**\n\n"
                "- **NWRS** and **wMDS** provide the core workforce source data.\n"
                "- **TIS** supports trainee information.\n"
                "- **GMC Register** adds registration and qualification context.\n"
                "- **ODS** provides organisational reference data."
            )
            suggestions = [
                "How is FTE calculated in the workforce data?",
                "What time period does the data cover?",
                "What does ARRS stand for?",
            ]
        state["answer"] = _polish_answer_text(ans)
        state["sql"] = ""
        state["df_preview_md"] = ""
        state["plan"] = {"in_scope": True, "table": None, "intent": "knowledge", "notes": "data sources fast-path"}
        state["_rows"] = 0
        state["_empty"] = False
        state["suggestions"] = suggestions
        MEMORY.add_turn(state.get("session_id", ""), q, ans, sql="", entity_context=None)
        logger.info("node_knowledge_answer | answered via data-sources fast-path")
        return state

    if "arrs" in q_lower:
        ans = (
            "**ARRS stands for Additional Roles Reimbursement Scheme.**\n\n"
            "It funds additional multidisciplinary roles in primary care rather than traditional GP posts.\n\n"
            "- Roles can include clinical pharmacists, pharmacy technicians, physiotherapists, paramedics, care coordinators, "
            "social prescribing link workers, health and wellbeing coaches, and other ARRS-funded roles.\n"
            "- In this chatbot, ARRS questions are usually answered from the Direct Patient Care workforce data."
        )
        state["answer"] = _polish_answer_text(ans)
        state["sql"] = ""
        state["df_preview_md"] = ""
        state["plan"] = {"in_scope": True, "table": None, "intent": "knowledge", "notes": "ARRS definition fast-path"}
        state["_rows"] = 0
        state["_empty"] = False
        state["suggestions"] = [
            "How many ARRS roles are filled across England?",
            "Show the DPC staff breakdown nationally",
            "How many pharmacists work in primary care?",
        ]
        MEMORY.add_turn(state.get("session_id", ""), q, ans, sql="", entity_context=None)
        logger.info("node_knowledge_answer | answered via ARRS fast-path")
        return state

    llm = llm_client()

    # For knowledge questions, retrieve more context than default
    domain_notes = retrieve_dataset_domain_notes(
        q,
        state.get("dataset", "workforce"),
        max_chars=DOMAIN_NOTES_MAX_CHARS,
        max_chunks=12,
    )
    conversation_history = state.get("conversation_history", "")
    style_instruction = _style_instruction_for_preferences(
        state.get("user_preferences"), q, knowledge=True
    )

    prompt = f"""
CONVERSATION HISTORY:
{conversation_history or "(first question)"}

DOMAIN NOTES (your knowledge base — answer ONLY from this):
{domain_notes}

USER QUESTION:
{q}

ANSWER STYLE:
{style_instruction}

Provide a clear, well-formatted answer using ONLY the domain notes above.
""".strip()

    ans = llm.invoke([
        SystemMessage(content=_knowledge_system_for_dataset(dataset)),
        HumanMessage(content=prompt),
    ]).content.strip()

    state["answer"] = _polish_answer_text(ans)
    state["sql"] = ""
    state["df_preview_md"] = ""
    state["plan"] = {"in_scope": True, "table": None, "intent": "knowledge", "notes": "answered from domain notes"}
    state["_rows"] = 0
    state["_empty"] = False
    state["suggestions"] = _knowledge_suggestions_for_dataset(dataset, q)

    # Save to conversation memory
    MEMORY.add_turn(
        state.get("session_id", ""),
        q,
        ans,
        sql="",
        entity_context=None,
    )

    logger.info("node_knowledge_answer | answered successfully")
    return state


def _answer_from_knowledge_retriever(
    *,
    question: str,
    dataset: DatasetName,
    conversation_history: str = "",
    user_preferences: Optional[Dict[str, Any]] = None,
    max_chars: int = DOMAIN_NOTES_MAX_CHARS,
) -> str:
    retriever = KNOWLEDGE_RETRIEVERS.get(dataset)
    if retriever is None:
        context = retrieve_dataset_domain_notes(question, dataset, max_chars=max_chars, max_chunks=6)
    else:
        context = retriever.retrieve(question, top_k=4, max_chars=max_chars)

    llm = llm_client()
    style_instruction = _style_instruction_for_preferences(
        user_preferences or {}, question, knowledge=True
    )
    prompt = f"""
CONVERSATION HISTORY:
{conversation_history or "(first question)"}

RETRIEVED KNOWLEDGE CONTEXT:
{context}

USER QUESTION:
{question}

ANSWER STYLE:
{style_instruction}

Use ONLY the retrieved knowledge context above.
Answer directly and concisely.
""".strip()

    return llm.invoke([
        SystemMessage(content=_knowledge_system_for_dataset(dataset)),
        HumanMessage(content=prompt),
    ]).content.strip()


def node_knowledge_rag_worker(state: StateData) -> StateData:
    """
    Local knowledge-RAG worker for multi-worker supervision.
    """
    worker_plan = dict(state.get("worker_plan") or {})
    knowledge_q = str(worker_plan.get("knowledge_question") or "").strip()
    if not knowledge_q:
        state["knowledge_worker_answer"] = ""
        return state

    data_answer = str(state.get("data_worker_answer") or state.get("answer") or "").strip()
    dataset = str(state.get("dataset") or worker_plan.get("primary_dataset") or "workforce")
    state["data_worker_answer"] = data_answer
    state["knowledge_worker_answer"] = _polish_answer_text(
        _answer_from_knowledge_retriever(
            question=knowledge_q,
            dataset=cast(DatasetName, dataset),
            conversation_history=str(state.get("conversation_history") or ""),
            user_preferences=cast(Optional[Dict[str, Any]], state.get("user_preferences")),
            max_chars=min(DOMAIN_NOTES_MAX_CHARS, 7000),
        )
    )
    logger.info("node_knowledge_rag_worker | retrieved knowledge answer generated")
    return state


def _dataset_pipeline(dataset: DatasetName):
    return APPOINTMENTS_SQL_PIPELINE if dataset == "appointments" else WORKFORCE_SQL_PIPELINE


def _copy_dataset_pipeline_result(state: StateData, result: StateData) -> None:
    preserve = {
        "session_id",
        "conversation_history",
        "user_preferences",
        "worker_plan",
        "supervisor_mode",
        "knowledge_worker_answer",
        "data_worker_answer",
    }
    for key, value in result.items():
        if key in preserve:
            continue
        state[key] = value


def node_multi_worker_dispatch(state: StateData) -> StateData:
    worker_plan = dict(state.get("worker_plan") or {})
    dataset = cast(DatasetName, str(state.get("dataset") or worker_plan.get("primary_dataset") or "workforce"))
    data_q = str(worker_plan.get("data_question") or state.get("question") or "").strip()
    knowledge_q = str(worker_plan.get("knowledge_question") or "").strip()

    def run_data_worker() -> StateData:
        pipeline_state: StateData = dict(state)
        pipeline_state["question"] = data_q
        pipeline_state["original_question"] = data_q
        return cast(StateData, _dataset_pipeline(dataset).invoke(pipeline_state))

    def run_knowledge_worker() -> str:
        if not knowledge_q:
            return ""
        return _polish_answer_text(
            _answer_from_knowledge_retriever(
                question=knowledge_q,
                dataset=dataset,
                conversation_history=str(state.get("conversation_history") or ""),
                user_preferences=cast(Optional[Dict[str, Any]], state.get("user_preferences")),
                max_chars=min(DOMAIN_NOTES_MAX_CHARS, 7000),
            )
        )

    data_future = _AGENT_EXECUTOR.submit(run_data_worker)
    knowledge_future = _AGENT_EXECUTOR.submit(run_knowledge_worker) if knowledge_q else None

    data_result = cast(StateData, data_future.result(timeout=REQUEST_TIMEOUT))
    knowledge_answer = str(knowledge_future.result(timeout=min(REQUEST_TIMEOUT, 45))) if knowledge_future else ""

    _copy_dataset_pipeline_result(state, data_result)
    state["knowledge_worker_answer"] = knowledge_answer.strip()
    state["data_worker_answer"] = str(state.get("answer") or "").strip()
    logger.info(
        "node_multi_worker_dispatch | dataset=%s data_q=%r knowledge_q=%r rows=%s",
        dataset,
        data_q[:100],
        knowledge_q[:100],
        state.get("_rows", 0),
    )
    return state


def node_cross_dataset_query(state: StateData) -> StateData:
    semantic_path = dict(state.get("semantic_path") or {})
    if semantic_path.get("used") and str(semantic_path.get("dataset") or "").strip().lower() == "cross" and state.get("sql"):
        df = run_athena_df(str(state.get("sql") or ""), database=APPOINTMENTS_ATHENA_DATABASE)
        state["df_preview_md"] = safe_markdown(df, head=10)
        state["_rows"] = int(len(df))
        state["_empty"] = bool(df.empty)
        state["last_error"] = None
        state["dataset"] = "cross_dataset"
        state["plan"] = {
            "in_scope": True,
            "table": "cross_dataset_join",
            "intent": "semantic_metric",
            "notes": "Cross-dataset SQL compiled via v9 semantic metric path",
        }
        state["answer"] = ""
        state["suggestions"] = [
            "Compare this with national average",
            "Show this by ICB",
            "Show this by region",
        ]
        logger.info(
            "node_cross_dataset_query | semantic metric=%s rows=%d",
            (semantic_path.get("metric_keys") or [""])[0],
            state["_rows"],
        )
        return state

    worker_plan = dict(state.get("worker_plan") or {})
    spec = dict(worker_plan.get("cross_dataset_spec") or {})
    if not spec:
        state["answer"] = "I couldn't determine a supported cross-dataset join pattern for that question."
        state["sql"] = ""
        state["df_preview_md"] = ""
        state["_rows"] = 0
        state["_empty"] = True
        state["plan"] = {"in_scope": False, "table": None, "intent": "cross_dataset_join", "notes": "missing cross-dataset spec"}
        return state

    sql, periods = _build_cross_dataset_sql(spec)
    df = run_athena_df(sql, database=APPOINTMENTS_ATHENA_DATABASE)
    state["sql"] = add_limit(sql, MAX_ROWS_RETURN)
    state["df_preview_md"] = safe_markdown(df, head=10)
    state["_rows"] = int(len(df))
    state["_empty"] = bool(df.empty)
    state["last_error"] = None
    state["dataset"] = "cross_dataset"
    state["plan"] = {
        "in_scope": True,
        "table": "cross_dataset_practice_join",
        "intent": "cross_dataset_join",
        "notes": f"Cross-dataset practice join: {spec.get('kind')}",
    }
    state["answer"] = _polish_answer_text(
        _render_cross_dataset_answer(
            str(state.get("original_question") or state.get("question") or ""),
            df,
            spec,
            periods,
        )
    )
    kind = str(spec.get("kind") or "")
    if kind == "appointments_per_gp_benchmark":
        state["suggestions"] = [
            "Show this by ICB",
            "Show this by region",
            "Top 10 practices by appointments per GP",
            "Show this with GP headcount instead of FTE",
        ]
    elif kind in {"appointments_per_gp_by_icb", "appointments_and_gp_count_by_icb"}:
        state["suggestions"] = [
            "Top 10 practices by appointments per GP",
            "Show this by region",
            "What about the lowest 5 practices?",
            "Compare the top result with national average",
        ]
    elif kind in {"appointments_per_gp_by_region", "appointments_and_gp_count_by_region"}:
        state["suggestions"] = [
            "Show this by ICB",
            "What about the lowest regions?",
            "Compare the top result with national average",
        ]
    elif kind == "appointments_per_gp_group_benchmark":
        state["suggestions"] = [
            "Show this by region",
            "Show this by ICB",
            "Top 10 practices by appointments per GP",
        ]
    else:
        state["suggestions"] = [
            "What about the lowest 5?",
            "Compare this with national average",
            "Show this by ICB",
        ]
    state["semantic_state"] = {
        "dataset": "cross_dataset",
        "metric": str(spec.get("kind") or "cross_dataset_join"),
        "entity_type": "practice",
        "table": "cross_dataset_practice_join",
        "aggregation": "ranking",
        "grain": "practice_join",
        "top_n": str(spec.get("top_n") or ""),
        "order": str(spec.get("order") or ""),
        "appointments_order": str(spec.get("appointments_order") or ""),
        "gp_order": str(spec.get("gp_order") or ""),
        "gp_basis": str(spec.get("gp_basis") or ""),
        "parent_scope_entity_name": str(spec.get("icb_hint") or ""),
        "parent_scope_region_name": str(spec.get("region_hint") or ""),
        "group_dim": "icb" if "_by_icb" in kind else ("region" if "_by_region" in kind or spec.get("group_dim") == "region" else ""),
    }
    entity_context = _extract_entity_context_from_state(state)
    if spec.get("kind") == "appointments_per_gp_ranking":
        entity_context["order"] = str(spec.get("order") or "DESC").upper()
        entity_context["gp_basis"] = str(spec.get("gp_basis") or "fte").lower()
    elif spec.get("kind") in {"appointments_per_gp_by_icb", "appointments_per_gp_by_region"}:
        entity_context["order"] = str(spec.get("order") or "DESC").upper()
        entity_context["gp_basis"] = str(spec.get("gp_basis") or "fte").lower()
        entity_context["previous_group_dim"] = "icb" if spec.get("kind") == "appointments_per_gp_by_icb" else "region"
    else:
        entity_context["appointments_order"] = str(spec.get("appointments_order") or "DESC").upper()
        entity_context["gp_order"] = str(spec.get("gp_order") or "ASC").upper()
        if spec.get("kind") in {"appointments_and_gp_count_by_icb", "appointments_and_gp_count_by_region"}:
            entity_context["previous_group_dim"] = "icb" if spec.get("kind") == "appointments_and_gp_count_by_icb" else "region"
    entity_context["previous_limit"] = str(spec.get("top_n") or "")
    if spec.get("icb_hint"):
        entity_context["parent_scope_entity_name"] = str(spec.get("icb_hint"))
        entity_context["parent_scope_entity_type"] = "icb"
        entity_context["parent_scope_entity_col"] = "icb_name"
    if spec.get("region_hint"):
        entity_context["parent_scope_region_name"] = str(spec.get("region_hint"))
    MEMORY.add_turn(
        state.get("session_id", ""),
        str(state.get("original_question") or state.get("question") or ""),
        state["answer"],
        state.get("sql", ""),
        entity_context=entity_context,
    )
    logger.info(
        "node_cross_dataset_query | kind=%s rows=%d",
        spec.get("kind"),
        state["_rows"],
    )
    return state


def node_multi_worker_merge(state: StateData) -> StateData:
    data_answer = str(state.get("data_worker_answer") or state.get("answer") or "").strip()
    knowledge_answer = str(state.get("knowledge_worker_answer") or "").strip()
    if not knowledge_answer:
        state["answer"] = data_answer
        return state

    if not data_answer:
        state["answer"] = knowledge_answer
        return state

    state["answer"] = f"{data_answer}\n\nContext:\n{knowledge_answer}".strip()
    suggestions = list(state.get("suggestions") or [])
    dataset = cast(DatasetName, str(state.get("dataset") or "workforce"))
    worker_plan = dict(state.get("worker_plan") or {})
    knowledge_q = str(worker_plan.get("knowledge_question") or state.get("original_question") or "")
    for suggestion in _knowledge_suggestions_for_dataset(dataset, knowledge_q):
        if suggestion not in suggestions:
            suggestions.append(suggestion)
    if "Explain the methodology behind this figure" not in suggestions and dataset == "workforce":
        suggestions.append("Explain the methodology behind this figure")
    state["suggestions"] = suggestions[:3]
    logger.info("node_multi_worker_merge | merged data and knowledge worker answers")
    return state


def _knowledge_suggestions(question: str) -> List[str]:
    """Generate relevant follow-up suggestions after a knowledge answer."""
    q_lower = question.lower()
    suggestions = []

    if "fte" in q_lower or "headcount" in q_lower:
        suggestions.append("Show total GP FTE nationally in the latest month")
        suggestions.append("What is the difference between FTE and headcount?")
    elif "locum" in q_lower:
        suggestions.append("How are ad-hoc locums different from regular locums?")
        suggestions.append("Show total GP FTE excluding locums")
    elif "source" in q_lower or "nwrs" in q_lower or "wmds" in q_lower:
        suggestions.append("What staff groups are covered in this publication?")
        suggestions.append("Show national totals by staff group")
    elif "scope" in q_lower or "include" in q_lower or "exclude" in q_lower:
        suggestions.append("What geographic areas does this publication cover?")
        suggestions.append("Show GP FTE by region in the latest month")
    elif "compar" in q_lower or "time series" in q_lower:
        suggestions.append("Show GP FTE trend over the last 12 months")
        suggestions.append("When did the series move from quarterly to monthly?")
    elif "file" in q_lower or "csv" in q_lower or "dashboard" in q_lower:
        suggestions.append("What is the individual-level CSV used for?")
        suggestions.append("Show total GP FTE nationally")
    elif "joiner" in q_lower or "leaver" in q_lower:
        suggestions.append("What leaving-related fields does NWRS collect?")
        suggestions.append("Show GP headcount trend over the last 12 months")
    else:
        suggestions.append("Show total GP FTE nationally in the latest month")
        suggestions.append("What data sources are used in this publication?")
        suggestions.append("What staff groups are included in this publication?")

    # Always add one data-oriented suggestion to guide users to the SQL path
    if not any("Show" in s or "total" in s for s in suggestions):
        suggestions.append("Show total GP FTE nationally in the latest month")

    return suggestions[:3]


def _knowledge_suggestions_for_dataset(dataset: DatasetName, question: str) -> List[str]:
    if dataset == "appointments":
        q_lower = question.lower()
        suggestions: List[str] = []
        if "dna" in q_lower or "did not attend" in q_lower:
            suggestions = [
                "What is the DNA rate nationally?",
                "Show DNA rate by ICB in the latest month",
                "Show appointment mode breakdown nationally",
            ]
        elif "appointment mode" in q_lower or "mode" in q_lower:
            suggestions = [
                "Show appointment mode breakdown nationally",
                "Show appointment mode breakdown in NHS Greater Manchester ICB",
                "Show GP appointments trend over the past year",
            ]
        elif "hcp" in q_lower or "health care professional" in q_lower:
            suggestions = [
                "Show total GP appointments nationally in the latest month",
                "Show total nurse appointments nationally in the latest month",
                "Compare GP and nurse appointments nationally",
            ]
        else:
            suggestions = [
                "Show total appointments nationally in the latest month",
                "What is the DNA rate nationally?",
                "Show top practices by appointments",
            ]
        return suggestions[:3]
    return _knowledge_suggestions(question)


def node_fetch_latest_and_vocab(state: StateData) -> StateData:
    return WORKFORCE_LATEST_VOCAB_NODE(state)


def node_fetch_latest_and_vocab_appointments(state: StateData) -> StateData:
    return APPOINTMENTS_LATEST_VOCAB_NODE(state)


def _init_appointments_query_plan(state: StateData, hcp_type: Optional[str]) -> None:
    external_init_appointments_query_plan(state, hcp_type)


def _reset_appointments_query_fallthrough(state: StateData) -> StateData:
    return external_reset_appointments_query_fallthrough(
        state,
        log_info=lambda msg: logger.info(msg),
    )


def _appointments_scope_table(practice_hint: str, geo_hint: str) -> str:
    return external_appointments_scope_table(practice_hint, geo_hint)


def _apply_appointments_top_practices(state: StateData, question: str) -> StateData:
    return external_apply_appointments_top_practices(
        state,
        question,
        sql_appointments_top_practices=sql_appointments_top_practices,
    )


def _apply_appointments_dna_rate(state: StateData, question: str, geo_hint: str,
                                 practice_hint: str, hcp_type: Optional[str]) -> StateData:
    return external_apply_appointments_dna_rate(
        state,
        question,
        geo_hint,
        practice_hint,
        hcp_type,
        sql_appointments_dna_rate=sql_appointments_dna_rate,
    )


def _apply_appointments_mode_breakdown(state: StateData, question: str, geo_hint: str,
                                       practice_hint: str, hcp_type: Optional[str]) -> StateData:
    return external_apply_appointments_mode_breakdown(
        state,
        question,
        geo_hint,
        practice_hint,
        hcp_type,
        sql_appointments_mode_breakdown=sql_appointments_mode_breakdown,
    )


def _apply_appointments_hcp_breakdown(state: StateData, question: str, geo_hint: str,
                                      practice_hint: str) -> StateData:
    return external_apply_appointments_hcp_breakdown(
        state,
        question,
        geo_hint,
        practice_hint,
        sql_appointments_hcp_breakdown=sql_appointments_hcp_breakdown,
    )


def _apply_appointments_trend(state: StateData, question: str, geo_hint: str,
                              practice_hint: str, hcp_type: Optional[str]) -> StateData:
    return external_apply_appointments_trend(
        state,
        question,
        geo_hint,
        practice_hint,
        hcp_type,
        sql_appointments_trend=sql_appointments_trend,
    )


def _apply_appointments_total(state: StateData, question: str, geo_hint: str,
                              practice_hint: str, hcp_type: Optional[str]) -> StateData:
    return external_apply_appointments_total(
        state,
        question,
        geo_hint,
        practice_hint,
        hcp_type,
        sql_appointments_total_latest=sql_appointments_total_latest,
    )


def _appointments_query_strategy(state: StateData) -> StateData:
    return external_appointments_query_strategy(
        state,
        extract_appointments_geo_hint=_extract_appointments_geo_hint,
        appointments_geo_hint_from_context=_appointments_geo_hint_from_context,
        appointments_hcp_filter=_appointments_hcp_filter,
        specific_entity_hint=_specific_entity_hint,
        sql_appointments_top_practices=sql_appointments_top_practices,
        sql_appointments_dna_rate=sql_appointments_dna_rate,
        sql_appointments_mode_breakdown=sql_appointments_mode_breakdown,
        sql_appointments_hcp_breakdown=sql_appointments_hcp_breakdown,
        sql_appointments_trend=sql_appointments_trend,
        sql_appointments_total_latest=sql_appointments_total_latest,
        log_info=lambda msg: logger.info(msg),
    )


def node_appointments_query(state: StateData) -> StateData:
    return _appointments_query_strategy(state)


def _workforce_query_strategy(state: StateData) -> StateData:
    return node_hard_override_sql(state)


APPOINTMENTS_CONFIG["query_node_fn"] = _appointments_query_strategy
APPOINTMENTS_CONFIG["semantic_issue_checker"] = _appointments_semantic_issue_checker


def _apply_workforce_trainee_gp_count(state: StateData, hi: str) -> bool:
    if hi != "trainee_gp_count":
        return False
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False
    state["plan"] = {
        "in_scope": True,
        "table": "individual",
        "intent": "total",
        "notes": "Hard override: national trainee GP count",
    }
    state["sql"] = f"""
SELECT
  COUNT(DISTINCT unique_identifier) AS trainee_gp_count,
  ROUND(SUM(fte), 1) AS trainee_gp_fte
FROM individual
WHERE year = '{y}' AND month = '{m}'
  AND staff_group = 'GP'
  AND staff_role LIKE '%Training%'
LIMIT 200
""".strip()
    logger.info("node_hard_override | trainee GP count")
    return True


def _apply_workforce_retirement_eligible(state: StateData, hi: str) -> bool:
    if hi != "retirement_eligible":
        return False
    latest = get_latest_year_month("individual")
    y, m = latest.get("year"), latest.get("month")
    if not (y and m):
        return False
    age_filter = _age_55_plus_filter()
    state["plan"] = {
        "in_scope": True,
        "table": "individual",
        "intent": "demographics",
        "notes": "Hard override: retirement-eligible qualified GP proportion",
    }
    state["sql"] = f"""
SELECT
  COUNT(DISTINCT CASE
    WHEN staff_group = 'GP'
     AND staff_role NOT LIKE '%Training%'
     AND staff_role NOT LIKE '%Locum%'
     AND {age_filter}
    THEN unique_identifier END
  ) AS retirement_eligible_gp_count,
  COUNT(DISTINCT CASE
    WHEN staff_group = 'GP'
     AND staff_role NOT LIKE '%Training%'
     AND staff_role NOT LIKE '%Locum%'
    THEN unique_identifier END
  ) AS qualified_gp_count,
  ROUND(
    100.0 * COUNT(DISTINCT CASE
      WHEN staff_group = 'GP'
       AND staff_role NOT LIKE '%Training%'
       AND staff_role NOT LIKE '%Locum%'
       AND {age_filter}
      THEN unique_identifier END
    ) / NULLIF(COUNT(DISTINCT CASE
      WHEN staff_group = 'GP'
       AND staff_role NOT LIKE '%Training%'
       AND staff_role NOT LIKE '%Locum%'
      THEN unique_identifier END
    ), 0),
    1
  ) AS retirement_eligible_pct
FROM individual
WHERE year = '{y}' AND month = '{m}'
LIMIT 200
""".strip()
    logger.info("node_hard_override | retirement eligibility")
    return True


def _build_national_patients_per_gp_yoy_override(state: StateData) -> bool:
    return external_build_national_patients_per_gp_yoy_override(
        state,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_ratio_overrides(
    state: AgentState,
    orig_q: str,
    follow_ctx: Optional[Dict[str, Any]],
) -> bool:
    return external_apply_workforce_ratio_overrides(
        state,
        orig_q,
        follow_ctx,
        get_latest_year_month=get_latest_year_month,
        build_national_patients_per_gp_yoy_override_fn=_build_national_patients_per_gp_yoy_override,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_followup_lookup_overrides(
    state: AgentState,
    orig_q: str,
    follow_ctx: Optional[Dict[str, Any]],
) -> bool:
    return external_apply_workforce_followup_lookup_overrides(
        state,
        orig_q,
        follow_ctx,
        sql_practice_staff_breakdown=sql_practice_staff_breakdown,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_demographic_overrides(state: StateData, orig_q: str, geo_hint: str) -> bool:
    return external_apply_workforce_demographic_overrides(
        state,
        orig_q,
        geo_hint,
        geo_filter_from_hint_text=_geo_filter_from_hint_text,
        get_latest_year_month=get_latest_year_month,
        age_60_plus_filter=_age_60_plus_filter,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_grouped_comparison_overrides(state: StateData, orig_q: str) -> bool:
    return external_apply_workforce_grouped_comparison_overrides(
        state,
        orig_q,
        get_latest_year_month=get_latest_year_month,
        region_column_for_table=_region_column_for_table,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_misc_lookup_overrides(state: StateData, orig_q: str) -> bool:
    return external_apply_workforce_misc_lookup_overrides(
        state,
        orig_q,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_partner_salaried_trend(state: StateData, orig_q: str, hi: str) -> bool:
    return external_apply_workforce_partner_salaried_trend(
        state,
        orig_q,
        hi,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_benchmark_and_group_followups(
    state: AgentState,
    orig_q: str,
    follow_ctx: Optional[Dict[str, Any]],
    followup_intent: str,
) -> bool:
    return external_apply_workforce_benchmark_and_group_followups(
        state,
        orig_q,
        follow_ctx,
        followup_intent,
        build_benchmark_followup=_build_benchmark_followup,
        parse_benchmark_request=_parse_benchmark_request,
        build_geo_compare_followup_sql=_build_geo_compare_followup_sql,
        build_grouped_followup_sql=_build_grouped_followup_sql,
        build_group_extreme_followup_sql=_build_group_extreme_followup_sql,
        build_total_change_followup_sql=_build_total_change_followup_sql,
        build_national_patients_per_gp_yoy_override=_build_national_patients_per_gp_yoy_override,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_large_practice_threshold(state: StateData, orig_q: str) -> bool:
    return external_apply_workforce_large_practice_threshold(
        state,
        orig_q,
        extract_geo_scope_hint=_extract_geo_scope_hint,
        geo_filter_from_hint_text=_geo_filter_from_hint_text,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_geo_scoped_simple_queries(state: StateData, orig_q: str, geo_hint: str) -> bool:
    return external_apply_workforce_geo_scoped_simple_queries(
        state,
        orig_q,
        geo_hint,
        is_national_scope_hint=_is_national_scope_hint,
        geo_filter_from_hint_text=_geo_filter_from_hint_text,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_verbose_national_total(state: StateData, orig_q: str) -> bool:
    return external_apply_workforce_verbose_national_total(
        state,
        orig_q,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _geo_filter_from_follow_context(entity_name: str, entity_type: str, follow_ctx: Optional[Dict[str, Any]] = None) -> Optional[str]:
    return external_geo_filter_from_follow_context(
        entity_name,
        entity_type,
        follow_ctx,
        city_to_icb_for_hint=_city_to_icb_for_hint,
        region_column_for_table=_region_column_for_table,
    )


def _apply_workforce_geo_context_followups(
    state: AgentState,
    orig_q: str,
    follow_ctx: Optional[Dict[str, Any]],
) -> bool:
    return external_apply_workforce_geo_context_followups(
        state,
        orig_q,
        follow_ctx,
        build_top_practices_followup_sql=_build_top_practices_followup_sql,
        staff_group_map=_STAFF_GROUP_MAP,
        get_latest_year_month=get_latest_year_month,
        geo_filter_from_follow_context_fn=_geo_filter_from_follow_context,
        region_column_for_table=_region_column_for_table,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_clinical_staff_breakdown(state: StateData, effective_q: str) -> bool:
    return external_apply_workforce_clinical_staff_breakdown(
        state,
        effective_q,
        get_latest_year_month=get_latest_year_month,
        log_info=lambda msg: logger.info(msg),
    )


def _resolve_workforce_override_hint(
    state: AgentState,
    hi: str,
    follow_ctx: Optional[Dict[str, Any]],
    geo_hint: str,
) -> Tuple[str, str, set[str]]:
    return external_resolve_workforce_override_hint(
        state,
        hi,
        follow_ctx,
        geo_hint,
        specific_entity_hint=_specific_entity_hint,
        extract_entity_hint=extract_entity_hint,
        extract_practice_code=extract_practice_code,
        log_info=lambda msg: logger.info(msg),
    )


def _apply_workforce_practice_gp_count_override(state: StateData, hi: str, hint: str) -> bool:
    return external_apply_workforce_practice_gp_count_override(
        state,
        hi,
        hint,
        city_to_icb_for_hint=_city_to_icb_for_hint,
        is_known_icb_fragment_hint=_is_known_icb_fragment_hint,
        is_national_scope_hint=_is_national_scope_hint,
        get_latest_year_month=get_latest_year_month,
        build_sql_practice_gp_count_latest=sql_practice_gp_count_latest,
    )


def _apply_workforce_practice_lookup_intents(state: StateData, hi: str, hint: str) -> bool:
    return external_apply_workforce_practice_lookup_intents(
        state,
        hi,
        hint,
        build_sql_pcn_gp_count=sql_pcn_gp_count,
        build_sql_practice_to_icb_latest=sql_practice_to_icb_latest,
        build_sql_practice_patient_count=sql_practice_patient_count,
        build_sql_practice_staff_breakdown=sql_practice_staff_breakdown,
    )


def _apply_workforce_patients_per_gp_intent(state: StateData, hi: str, hint: str, raw_q: str) -> bool:
    return external_apply_workforce_patients_per_gp_intent(
        state,
        hi,
        hint,
        raw_q,
        get_latest_year_month=get_latest_year_month,
        geo_filter_from_hint_text=_geo_filter_from_hint_text,
        build_sql_patients_per_gp=sql_patients_per_gp,
    )


def node_run_sql_appointments(state: StateData) -> StateData:
    return APPOINTMENTS_RUN_SQL_NODE(state)


_STAFF_GROUP_MAP = {
    "nurse": "Nurses", "nurses": "Nurses", "nursing": "Nurses",
    "gp": "GP", "gps": "GP", "doctor": "GP", "doctors": "GP",
    "admin": "Admin", "administrative": "Admin", "administration": "Admin",
    "dpc": "DPC", "direct patient care": "DPC",
    "pharmacist": "DPC", "pharmacists": "DPC",
    "paramedic": "DPC", "paramedics": "DPC",
    "physiotherapist": "DPC", "physiotherapists": "DPC",
}


def _infer_staff_filter_from_state(state: MutableMapping[str, Any], follow_ctx: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    return external_infer_staff_filter_from_state(state, follow_ctx)


def _build_grouped_followup_sql(state: MutableMapping[str, Any], dim: str) -> Optional[Dict[str, Any]]:
    return external_build_grouped_followup_sql(
        state,
        dim,
        get_latest_year_month=get_latest_year_month,
        region_column_for_table=_region_column_for_table,
        infer_staff_filter_from_state_fn=_infer_staff_filter_from_state,
    )


def _followup_group_dimension(follow_ctx: Dict[str, Any]) -> str:
    return external_followup_group_dimension(follow_ctx)


def _build_group_extreme_followup_sql(state: MutableMapping[str, Any], extreme: str) -> Optional[Dict[str, Any]]:
    return external_build_group_extreme_followup_sql(
        state,
        extreme,
        get_latest_year_month=get_latest_year_month,
        region_column_for_table=_region_column_for_table,
        followup_group_dimension_fn=_followup_group_dimension,
        infer_staff_filter_from_state_fn=_infer_staff_filter_from_state,
    )


def _build_total_change_followup_sql(state: MutableMapping[str, Any]) -> Optional[Dict[str, Any]]:
    return external_build_total_change_followup_sql(
        state,
        get_latest_year_month=get_latest_year_month,
    )


def _build_geo_compare_followup_sql(state: MutableMapping[str, Any], target_hint: str) -> Optional[Dict[str, Any]]:
    return external_build_geo_compare_followup_sql(
        state,
        target_hint,
        clean_entity_hint=_clean_entity_hint,
        normalise_region_name=_normalise_region_name,
        sanitise_entity_input=sanitise_entity_input,
        region_column_for_table=_region_column_for_table,
        get_latest_year_month=get_latest_year_month,
        infer_staff_filter_from_state_fn=_infer_staff_filter_from_state,
    )


def _build_top_practices_followup_sql(state: MutableMapping[str, Any]) -> Optional[Dict[str, Any]]:
    return external_build_top_practices_followup_sql(
        state,
        effective_scope_context=_effective_scope_context,
        geo_filter_from_context=_geo_filter_from_context,
        get_latest_year_month=get_latest_year_month,
    )


def node_hard_override_sql(state: StateData) -> StateData:
    follow_ctx = state.get("follow_up_context")
    orig_q = (state.get("original_question") or "").lower().strip()
    followup_intent = state.get("_followup_intent", "")
    hi = state.get("_hard_intent")

    geo_hint = _extract_geo_scope_hint(state.get("original_question", ""))

    if _apply_workforce_trainee_gp_count(state, hi):
        return state

    if _apply_workforce_retirement_eligible(state, hi):
        return state

    if _apply_workforce_ratio_overrides(state, orig_q, follow_ctx):
        return state

    if _apply_workforce_misc_lookup_overrides(state, orig_q):
        return state

    if _apply_workforce_followup_lookup_overrides(state, orig_q, follow_ctx):
        return state

    if _apply_workforce_demographic_overrides(state, orig_q, geo_hint):
        return state

    if _apply_workforce_grouped_comparison_overrides(state, orig_q):
        return state

    if _apply_workforce_partner_salaried_trend(state, orig_q, hi):
        return state

    if _apply_workforce_benchmark_and_group_followups(state, orig_q, follow_ctx, followup_intent):
        return state

    if _apply_workforce_large_practice_threshold(state, orig_q):
        return state

    if _apply_workforce_geo_scoped_simple_queries(state, orig_q, geo_hint):
        return state

    if _apply_workforce_verbose_national_total(state, orig_q):
        return state

    if _apply_workforce_geo_context_followups(state, orig_q, follow_ctx):
        return state

    effective_q = " ".join([
        str(state.get("original_question") or ""),
        str(state.get("question") or ""),
    ]).lower()

    if _apply_workforce_clinical_staff_breakdown(state, effective_q):
        return state

    if not hi:
        return state

    # If classifier already determined this is out_of_scope (e.g. Scotland, Wales),
    # do NOT apply hard override — let the OOS handler deal with it
    if state.get("_query_route") == "out_of_scope":
        logger.info("node_hard_override | skipped (route=out_of_scope, overrides hard_intent=%s)", hi)
        return state

    hint, raw_orig_q, _ = _resolve_workforce_override_hint(state, hi, follow_ctx, geo_hint)
    if not hint and hi in ("practice_gp_count", "practice_gp_count_soft", "practice_patient_count", "practice_staff_breakdown", "practice_to_icb_lookup"):
        return state

    if _apply_workforce_practice_gp_count_override(state, hi, hint):
        return state

    if _apply_workforce_practice_lookup_intents(state, hi, hint):
        return state

    if _apply_workforce_patients_per_gp_intent(state, hi, hint, raw_orig_q.lower()):
        return state

    return state


WORKFORCE_CONFIG["query_node_fn"] = _workforce_query_strategy
WORKFORCE_CONFIG["semantic_issue_checker"] = _default_semantic_issue_checker


def node_plan(state: StateData) -> StateData:
    if state.get("sql"):
        logger.debug("node_plan | skipped (SQL already set by hard_override)")
        return state

    # If plan is already pre-set (e.g. by classifier-detected OOS), skip LLM planner
    existing_plan = state.get("plan")
    if existing_plan and existing_plan.get("intent") == "out_of_scope":
        logger.info("node_plan | skipped (plan pre-set as out_of_scope by classifier)")
        return state

    if _try_v9_semantic_path(state):
        logger.info("node_plan | semantic compiler path hit")
        return state

    logger.info("node_plan | invoking LLM planner")
    t0 = time.time()
    llm = llm_client()
    dataset = state.get("dataset", "workforce")
    candidate_tables = list(state.get("candidate_tables") or [])
    narrowed_schema_text = str(state.get("narrowed_schema_text") or "").strip()
    narrowing_notes = str(state.get("schema_narrowing_notes") or "").strip()
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

PREFERRED TABLE CANDIDATES:
{", ".join(candidate_tables) if candidate_tables else "(not narrowed)"}

SCHEMA NARROWING NOTES:
{narrowing_notes or "(none)"}

NARROWED SCHEMA CONTEXT:
{narrowed_schema_text or "(planner may consider all allowed tables if needed)"}
""".strip()

    # Use structured output for guaranteed valid plan schema
    try:
        structured_llm = llm.with_structured_output(QueryPlan)
        plan_obj = structured_llm.invoke([
            SystemMessage(content=_planner_system_for_dataset(dataset)),
            HumanMessage(content=prompt),
        ])
        plan = plan_obj.model_dump()
        logger.debug("node_plan | structured output parsed successfully")
    except Exception as e:
        logger.warning("node_plan | structured output failed (%s), trying free-form JSON", str(e)[:120])
        # Fallback: try free-form JSON from unstructured LLM
        try:
            raw = llm.invoke([
                SystemMessage(content=_planner_system_for_dataset(dataset)),
                HumanMessage(content=prompt),
            ]).content.strip()
            raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE).strip()
            raw = re.sub(r"```$", "", raw).strip()
            plan = json.loads(raw)
        except Exception:
            logger.warning("node_plan | free-form JSON also failed, using fallback plan")
            plan = {
                "in_scope": True, "table": "individual", "intent": "unknown",
                "group_by": [], "filters_needed": [], "entities_to_resolve": [],
                "notes": "fallback plan (structured output + JSON both failed)",
            }

    plan = validate_plan(plan, state["question"], dataset=dataset)
    plan["in_scope"] = bool(plan.get("in_scope", True))
    if candidate_tables and len(candidate_tables) == 1 and plan.get("table") not in candidate_tables:
        original_table = plan.get("table")
        plan["table"] = candidate_tables[0]
        plan["notes"] = f"{plan.get('notes', '')} Preferred narrowed table applied instead of {original_table}.".strip()

    # ── Multi-turn clarification: if planner flagged ambiguity, short-circuit ──
    # Don't ask for clarification if:
    #   - This turn already resolved a previous clarification (prevent infinite loops)
    #   - The question is out of scope (handle via OOS path instead)
    #   - The planner didn't actually provide a clarification question
    if (plan.get("needs_clarification", False)
        and not state.get("_clarification_resolved", False)
        and plan.get("in_scope", True)
        and plan.get("clarification_question", "").strip()
    ):
        state["_needs_clarification"] = True
        state["_clarification_question"] = plan["clarification_question"]
        state["plan"] = plan
        logger.info(
            "node_plan | CLARIFICATION NEEDED: '%s' (%.2fs)",
            plan["clarification_question"][:120], time.time() - t0,
        )
        return state

    state["plan"] = plan
    logger.info("node_plan | table=%s intent=%s in_scope=%s (%.2fs)",
                plan.get("table"), plan.get("intent"), plan.get("in_scope"), time.time() - t0)
    return state


def node_resolve_entities(state: StateData) -> StateData:
    if state.get("sql"):
        state["resolved_entities"] = state.get("resolved_entities", {}) or {}
        return state

    plan = state.get("plan", {}) or {}
    if not plan.get("in_scope", True):
        state["resolved_entities"] = {}
        return state

    q = state["question"]
    dataset = state.get("dataset", "workforce")
    resolved: Dict[str, Any] = _resolve_entities_via_config(dataset, q, plan)
    state["resolved_entities"] = resolved

    clarification_q = _entity_clarification_question(state)
    if clarification_q:
        state["_needs_clarification"] = True
        state["_clarification_question"] = clarification_q
        logger.info("node_resolve_entities | %s clarification needed: %s", dataset, clarification_q[:120])
    return state


def node_generate_sql(state: StateData) -> StateData:
    if state.get("sql"):
        return state

    plan = state.get("plan", {})
    dataset = state.get("dataset", "workforce")

    # ── data_simple fast-path: auto-generate lightweight plan if planner was skipped ──
    if not plan and state.get("_query_route") == "data_simple":
        auto_table, table_reason = _deterministic_table_choice(state["question"])
        if not auto_table:
            auto_table = "individual"
            table_reason = "defaulted to individual for simple aggregate"
        plan = {
            "in_scope": True,
            "table": auto_table,
            "intent": "simple_aggregate",
            "group_by": [],
            "filters_needed": [],
            "entities_to_resolve": [],
            "notes": f"Auto-generated plan for data_simple route ({table_reason})",
        }
        state["plan"] = plan
        logger.info("node_generate_sql | data_simple auto-plan: table=%s", auto_table)

    if not plan.get("in_scope", True):
        state["sql"] = ""
        return state

    table = plan.get("table", "individual")
    schema_text = _dataset_schema_text(table, dataset, max_columns=120)

    # Column labels
    col_labels = get_column_labels(table, dataset)

    latest = get_latest_year_month(table)
    y, m = latest.get("year"), latest.get("month")

    time_range = state.get("time_range")
    if time_range:
        time_range_text = json.dumps(time_range, ensure_ascii=False)
        # If compare_years, make it extra explicit which year+month pairs to use
        if time_range.get("compare_years"):
            cy = time_range["compare_years"]
            em = time_range.get("end_month", m)
            time_range_text += (
                f"\nCOMPARISON PERIODS: Use year='{cy[0]}' month='{em}' vs year='{cy[-1]}' month='{em}'. "
                f"The latest available month for this table is month='{m}' — use this month for BOTH periods."
            )
    else:
        time_range_text = "None — use latest month only"

    # ── Retrieve similar few-shot examples (golden + learned) ──
    few_shot_text = ""
    dataset_retriever = _few_shot_retriever_for_dataset(dataset)
    if dataset_retriever.ready:
        examples = dataset_retriever.retrieve(state["question"], top_k=FEW_SHOT_TOP_K)
        # Also retrieve from long-term learned memory
        ltm_examples = _dataset_ltm_examples(state["question"], dataset, top_k=2)
        if ltm_examples:
            # Merge, avoiding duplicates by question similarity
            existing_qs = {ex["question"].lower().strip() for ex in examples}
            for ltm_ex in ltm_examples:
                if ltm_ex["question"].lower().strip() not in existing_qs:
                    ltm_ex["source"] = "learned"
                    examples.append(ltm_ex)
        if examples:
            parts = []
            for i, ex in enumerate(examples, 1):
                source_tag = " [learned]" if ex.get("source") == "learned" else ""
                parts.append(
                    f"Example {i} (similarity={ex['similarity']}{source_tag}):\n"
                    f"  Q: {ex['question']}\n"
                    f"  Table: {ex['table']}\n"
                    f"  SQL: {ex['sql']}"
                )
            few_shot_text = "\n\n".join(parts)
            state["_few_shot_best_sim"] = examples[0]["similarity"]
            logger.debug("Few-shot: injected %d examples (%d golden, %d learned, best sim=%.4f)",
                         len(examples), len(examples) - len([e for e in examples if e.get("source") == "learned"]),
                         len([e for e in examples if e.get("source") == "learned"]),
                         examples[0]["similarity"])

    entity_resolution_text = _entity_resolution_guidance(state.get("resolved_entities", {}))
    valid_values_text = _dataset_valid_values_block(state, dataset)
    candidate_tables = list(state.get("candidate_tables") or [])
    narrowing_notes = str(state.get("schema_narrowing_notes") or "").strip()

    context = f"""
CONVERSATION HISTORY:
{state.get("conversation_history", "") or "(first question)"}

SIMILAR PROVEN QUERIES (use these as patterns — adapt, don't copy blindly):
{few_shot_text or "(no similar examples found)"}

DOMAIN NOTES:
{state.get("domain_notes", "")}

LATEST (for {table}):
year={y} month={m}

TIME RANGE:
{time_range_text}

TABLE: {table}

PREFERRED TABLE CANDIDATES:
{", ".join(candidate_tables) if candidate_tables else table}

SCHEMA NARROWING NOTES:
{narrowing_notes or "(none)"}

SCHEMA:
{schema_text}

COLUMN LABELS (column_name = human meaning):
{col_labels}

{valid_values_text}

ENTITY RESOLUTION GUIDANCE:
If a clear top entity match is listed below, use that exact value in SQL instead of the raw user text.
{entity_resolution_text}

ENTITY CANDIDATES (raw):
{json.dumps(state.get("resolved_entities", {}), ensure_ascii=False)}

PLAN:
{json.dumps(plan, ensure_ascii=False)}

QUESTION:
{state["question"]}
""".strip()

    llm = llm_client()
    sql = llm.invoke([
        SystemMessage(content=_sql_system_for_dataset(dataset)),
        HumanMessage(content=context),
    ]).content.strip()

    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE).strip()
    sql = re.sub(r"```$", "", sql).strip()

    state["sql"] = sql
    return state


def node_run_sql(state: StateData) -> StateData:
    plan = state.get("plan", {})
    if not plan.get("in_scope", True):
        state["df_preview_md"] = ""
        state["_rows"] = 0
        state["_empty"] = True
        return state

    sql = (state.get("sql") or "").strip()
    if not sql:
        raise ValueError("No SQL produced for an in-scope question.")

    # If LLM generated comment-only SQL (no actual SELECT), treat as knowledge question
    sql_no_comments = re.sub(r"--.*$", "", sql, flags=re.MULTILINE).strip()
    if not sql_no_comments or not re.search(r"\bSELECT\b", sql_no_comments, re.IGNORECASE):
        logger.warning("node_run_sql | comment-only SQL detected, no SELECT found — treating as knowledge")
        state["df_preview_md"] = ""
        state["_rows"] = 0
        state["_empty"] = True
        state["last_error"] = None
        return state

    logger.info("node_run_sql | executing query (%d chars)", len(sql))
    try:
        sql_safe = enforce_readonly(sql)
        enforce_table_whitelist(sql_safe)
        sql_safe = fix_multiperiod_or_bug(sql_safe)
        sql_safe = fix_categorical_case(sql_safe)
        sql_safe = fix_hyphenated_names(sql_safe)
        sql_safe = fix_wrong_geo_column(sql_safe)
        sql_safe = fix_geo_broadening(sql_safe)
        sql_safe = fix_missing_follow_up_geo(sql_safe, state.get("follow_up_context"))
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


def node_validate_or_fix(state: StateData) -> StateData:
    plan = state.get("plan", {}) or {}
    dataset = state.get("dataset", "workforce")
    if not plan.get("in_scope", True):
        state["needs_retry"] = False
        return state

    attempts = int(state.get("attempts", 0))
    last_error = state.get("last_error")
    empty = bool(state.get("_empty", False))
    semantic_issues = _detect_sql_semantic_issues(state)
    if semantic_issues:
        semantic_error = " ; ".join(semantic_issues)
        logger.warning("node_validate_or_fix | semantic issues: %s", semantic_error)
        last_error = f"{last_error} ; {semantic_error}" if last_error else semantic_error

    if (not last_error) and (not empty):
        state["needs_retry"] = False
        return state

    if attempts >= MAX_AGENT_LOOPS:
        state["last_error"] = last_error
        state["needs_retry"] = False
        return state

    clarification_question = _semantic_clarification_question(state, dataset)
    if clarification_question:
        _set_semantic_clarification(state, clarification_question, "validation requested practice clarification")
        state["needs_retry"] = False
        state["last_error"] = last_error
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
    schema_text = _dataset_schema_text(table, dataset, max_columns=200)

    latest = get_latest_year_month(table)
    y, m = latest.get("year"), latest.get("month")

    # ── Retrieve similar few-shot examples for fixer ──
    fixer_few_shot = ""
    dataset_retriever = _few_shot_retriever_for_dataset(dataset)
    if dataset_retriever.ready:
        fix_examples = dataset_retriever.retrieve(state["question"], top_k=3)
        if fix_examples:
            parts = []
            for i, ex in enumerate(fix_examples, 1):
                parts.append(f"Example {i}: Q: {ex['question']}\n  SQL: {ex['sql']}")
            fixer_few_shot = "\n\n".join(parts)

    entity_resolution_text = _entity_resolution_guidance(state.get("resolved_entities", {}))
    valid_values_text = _dataset_valid_values_block(state, dataset)

    fix_context = f"""
SIMILAR PROVEN QUERIES (reference patterns):
{fixer_few_shot or "(none)"}

DOMAIN NOTES:
{state.get("domain_notes", "")}

LATEST (for {table}):
year={y} month={m}

TABLE: {table}

SCHEMA:
{schema_text}

COLUMN LABELS:
{get_column_labels(table, dataset)}

{valid_values_text}

ENTITY RESOLUTION GUIDANCE:
If a clear top entity match is listed below, use that exact value in the corrected SQL instead of the raw user text.
{entity_resolution_text}

ENTITY CANDIDATES (raw):
{json.dumps(state.get("resolved_entities", {}), ensure_ascii=False)}

QUESTION:
{state["question"]}

PREVIOUS SQL:
{state.get("sql", "")}

    ERROR / SEMANTIC ISSUES (if any):
    {last_error or ""}

RESULT EMPTY:
{empty}

Return corrected SQL only.
""".strip()

    fixed_sql = llm.invoke([
        SystemMessage(content=_fixer_system_for_dataset(dataset)),
        HumanMessage(content=fix_context),
    ]).content.strip()

    fixed_sql = re.sub(r"^```sql\s*", "", fixed_sql, flags=re.IGNORECASE).strip()
    fixed_sql = re.sub(r"```$", "", fixed_sql).strip()

    state["sql"] = fixed_sql
    state["attempts"] = attempts + 1
    state["needs_retry"] = True
    return state


def node_summarize(state: StateData) -> StateData:
    plan = state.get("plan", {}) or {}
    llm = llm_client()
    user_prefs = state.get("user_preferences", {})
    dataset = state.get("dataset", "workforce")
    dataset_label = _dataset_label(dataset)
    worker_plan = state.get("worker_plan") or {}
    summary_question = (
        str(worker_plan.get("data_question") or "").strip()
        if state.get("supervisor_mode") == "multi_worker"
        else ""
    ) or state.get("original_question", state["question"])
    memory_question = (
        str(worker_plan.get("full_question") or "").strip()
        if state.get("supervisor_mode") == "multi_worker"
        else ""
    ) or state.get("original_question", state["question"])

    if not plan.get("in_scope", True):
        plan_notes = plan.get("notes", "")
        domain_notes = state.get("domain_notes", "")
        style_instruction = _style_instruction_for_preferences(
            user_prefs, summary_question
        )
        msg = f"""
QUESTION:
{state["question"]}

PLANNER NOTES:
{plan_notes}

DOMAIN NOTES (for context on what IS available):
{domain_notes[:4000]}

This question is OUT OF SCOPE for the {dataset_label} dataset.
Explain clearly and helpfully:
1. Why this specific question cannot be answered from the {dataset_label} data
2. What related data IS available (e.g. if they ask about wait times, mention we have patients-per-GP ratio as a proxy)
3. If applicable, mention the correct NHS dataset that WOULD have this data
4. Suggest 2-3 related questions the user CAN ask with this chatbot
Keep it concise (4-8 lines). Be helpful, not just rejecting.

ANSWER STYLE:
{style_instruction}
""".strip()
        ans = llm.invoke([SystemMessage(content=_summary_system_for_dataset(dataset)), HumanMessage(content=msg)]).content.strip()
        ans = _polish_answer_text(ans)
        state["answer"] = ans
        state["df_preview_md"] = ""
        state["sql"] = ""
        # Generate contextual suggestions based on the question
        q_lower = state["question"].lower()
        if any(w in q_lower for w in ["scotland", "scottish", "wales", "welsh",
                                       "northern ireland", "belfast", "edinburgh",
                                       "glasgow", "cardiff", "aberdeen"]):
            # Override the LLM answer with a clearer non-England message
            ans = ("**This data covers England only.** The GP Workforce Statistics publication "
                   "from NHS England does not include data for Scotland, Wales, or Northern Ireland.\n\n"
                   "- **Scotland**: Check NHS Education for Scotland (NES) or ISD Scotland\n"
                   "- **Wales**: Check Health Education and Improvement Wales (HEIW)\n"
                   "- **Northern Ireland**: Check the Northern Ireland Statistics and Research Agency (NISRA)\n\n"
                   "I can help you explore the **England** GP workforce data instead.")
            state["answer"] = ans
            state["suggestions"] = [
                "Total GP FTE in England",
                "GP headcount by region in England",
                "GP age distribution in England",
            ]
            return state
        elif any(w in q_lower for w in ["wait", "appointment", "seen", "time"]):
            state["suggestions"] = [
                "Show patients per GP ratio by practice",
                "Which practices have the fewest GPs per patient?",
                "GP FTE trend over the last 12 months",
            ]
        elif any(w in q_lower for w in ["salary", "pay", "earn", "income"]):
            state["suggestions"] = [
                "Show GP FTE vs headcount ratio nationally",
                "Compare GP types by FTE (salaried vs partner)",
                "Total GP FTE by region",
            ]
        elif any(w in q_lower for w in ["turnover", "leaver", "leaving", "convert"]):
            state["suggestions"] = [
                "Show trainee numbers by training grade",
                "Trend of qualified GP headcount over 12 months",
                "Compare trainee numbers this year vs 3 years ago",
            ]
        else:
            state["suggestions"] = [
                "Show total GP FTE nationally in the latest month",
                "Top 10 practices by GP FTE",
                "GP age distribution breakdown",
            ]
        return state

    display_q = summary_question
    follow_ctx = state.get("follow_up_context")
    display_q = _semantic_followup_display_question(display_q, follow_ctx)
    style_instruction = _style_instruction_for_preferences(user_prefs, display_q)
    context_note = ""
    if follow_ctx and follow_ctx.get("entity_name"):
        view_note = f", view = {follow_ctx.get('previous_view')}" if follow_ctx.get("previous_view") else ""
        metric_note = f", metric = {follow_ctx.get('previous_metric')}" if follow_ctx.get("previous_metric") else ""
        context_note = f"\nCONTEXT: This was a follow-up about {follow_ctx.get('entity_type','entity')} '{follow_ctx.get('entity_name','')}'{view_note}{metric_note}"

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

ANSWER STYLE:
{style_instruction}
""".strip()

    ans = llm.invoke([SystemMessage(content=_summary_system_for_dataset(dataset)), HumanMessage(content=msg)]).content.strip()

    # Stabilise trend follow-up wording so the answer explicitly reads like a
    # change over time, even if the LLM falls back to a row-by-row bullet list.
    ans_low = ans.lower()
    display_q_low = display_q.lower()
    if (
        follow_ctx
        and follow_ctx.get("entity_name")
        and plan.get("intent") == "trend"
        and "how has this changed" in display_q_low
        and not any(word in ans_low for word in ["changed", "increase", "increased", "decrease", "decreased"])
    ):
        entity_name = str(follow_ctx.get("entity_name") or "").strip()
        metric_label = "the metric"
        prev_metric = str(follow_ctx.get("previous_metric") or "").lower()
        if prev_metric == "patients_per_gp":
            metric_label = "the patients-per-GP ratio"
        elif prev_metric == "fte":
            metric_label = "the FTE per GP measure"
        ans = f"**{metric_label.capitalize()} has changed over the past year for {entity_name}.**\n\n{ans}"

    semantic_path = dict(state.get("semantic_path") or {})
    if semantic_path.get("used") and str(semantic_path.get("dataset") or "").strip().lower() == "cross":
        grain = str(semantic_path.get("grain") or "").strip().lower()
        if grain in {"region", "icb"} and f"by {grain}" not in ans.lower():
            ans = f"**Here is the cross-dataset view by {grain}.**\n\n{ans}"

    if bool(state.get("_empty", False)):
        ans += "\n\n*Note: This query returned 0 rows. The name or filter may not match exactly. Try a different spelling or a broader search.*"

    state["answer"] = _polish_answer_text(ans)
    # Extract entity context from this turn to enable follow-ups
    entity_context = _extract_entity_context_from_state(state)
    logger.info("node_summarize | entity_context=%s session=%s", entity_context, state.get("session_id", "")[:30])

    state["suggestions"] = generate_suggestions(
        display_q, plan, ans,
        sql=state.get("sql", ""), entity_context=entity_context,
    )

    # Save to conversation memory (use original question for display, not enriched)
    MEMORY.add_turn(
        state.get("session_id", ""),
        memory_question,
        ans,
        state.get("sql", ""),
        entity_context=entity_context,
    )

    return state


def _extract_entity_context_from_state(state: StateData) -> Dict[str, Any]:
    """Extract the entity context from the current answer to enable follow-up questions."""
    plan = state.get("plan", {}) or {}
    state_semantic = dict(state.get("semantic_state") or {})
    dataset = str(state_semantic.get("dataset") or state.get("dataset") or "workforce")
    table = plan.get("table", "")
    sql = state.get("sql", "")
    ctx: Dict[str, Any] = {"table": table, "dataset": dataset}
    # Carry forward the full v9 SemanticRequest dict so follow-up turns can
    # re-parse with inherited entity_filters, group_by, time scope and transforms.
    v9_request_dict = state.get("semantic_request_v9")
    if v9_request_dict:
        ctx["v9_semantic_request"] = dict(v9_request_dict)
    elif isinstance((state.get("follow_up_context") or {}).get("v9_semantic_request"), dict):
        ctx["v9_semantic_request"] = dict(state["follow_up_context"]["v9_semantic_request"])
    orig_q = (state.get("original_question") or state.get("question") or "").lower()
    follow_ctx = state.get("follow_up_context") or {}
    rows = int(state.get("_rows", 0) or 0)
    last_error = str(state.get("last_error") or "").strip()

    def _with_semantic_state(context: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(context or {})
        semantic_state = _semantic_state_from_context(enriched)
        if semantic_state:
            enriched["semantic_state"] = semantic_state
            state["semantic_state"] = semantic_state
        return enriched

    group_by = plan.get("group_by") or []
    group_dim = ""
    if group_by:
        first_group = str(group_by[0]).lower()
        if "region" in first_group:
            group_dim = "region"
        elif "icb_name" in first_group:
            group_dim = "icb"
        elif "pcn_name" in first_group:
            group_dim = "pcn"

    if rows <= 0 or last_error:
        if follow_ctx:
            preserved = dict(follow_ctx)
            if group_dim:
                preserved["previous_group_dim"] = group_dim
            if table and not preserved.get("table"):
                preserved["table"] = table
            return _with_semantic_state(preserved)
        return {}

    if state_semantic:
        if state_semantic.get("entity_name"):
            ctx["entity_name"] = str(state_semantic.get("entity_name") or "").strip()
        if state_semantic.get("entity_type"):
            ctx["entity_type"] = str(state_semantic.get("entity_type") or "").strip()
        if state_semantic.get("entity_col"):
            ctx["entity_col"] = str(state_semantic.get("entity_col") or "").strip()
        if state_semantic.get("entity_code"):
            ctx["previous_entity_code"] = str(state_semantic.get("entity_code") or "").strip().upper()
        if state_semantic.get("metric"):
            ctx["previous_metric"] = str(state_semantic.get("metric") or "").strip()
        if state_semantic.get("view"):
            ctx["previous_view"] = str(state_semantic.get("view") or "").strip()
        if state_semantic.get("grain"):
            ctx["previous_grain"] = str(state_semantic.get("grain") or "").strip()

    def _carry_parent_scope(context: Dict[str, Any]) -> Dict[str, Any]:
        if context.get("entity_type") != "practice":
            return context

        if follow_ctx:
            if str(follow_ctx.get("entity_type") or "") in {"city", "icb", "sub_icb", "region"}:
                context["parent_scope_entity_name"] = follow_ctx.get("entity_name")
                context["parent_scope_entity_type"] = follow_ctx.get("entity_type")
                context["parent_scope_entity_col"] = follow_ctx.get("entity_col")
                if follow_ctx.get("mapped_icb"):
                    context["parent_scope_mapped_icb"] = follow_ctx.get("mapped_icb")
                if follow_ctx.get("previous_grain"):
                    context["parent_scope_grain"] = follow_ctx.get("previous_grain")
            elif follow_ctx.get("parent_scope_entity_name"):
                context["parent_scope_entity_name"] = follow_ctx.get("parent_scope_entity_name")
                context["parent_scope_entity_type"] = follow_ctx.get("parent_scope_entity_type")
                context["parent_scope_entity_col"] = follow_ctx.get("parent_scope_entity_col")
                if follow_ctx.get("parent_scope_mapped_icb"):
                    context["parent_scope_mapped_icb"] = follow_ctx.get("parent_scope_mapped_icb")
                if follow_ctx.get("parent_scope_grain"):
                    context["parent_scope_grain"] = follow_ctx.get("parent_scope_grain")
        elif preview_row.get("icb_name"):
            context["parent_scope_entity_name"] = preview_row.get("icb_name")
            context["parent_scope_entity_type"] = "icb"
            context["parent_scope_entity_col"] = "icb_name"
        return context

    def _finalise_context(context: Dict[str, Any]) -> Dict[str, Any]:
        sql_lower = sql.lower()
        plan_intent = str(plan.get("intent") or "").lower()
        explicit_metric_correction = ""
        if re.search(r"\bi\s+meant\s+fte\b|\bfte\s+(?:not|instead)\b|\bwant\s+fte\b|\bshow\s+fte\b|\buse\s+fte\b", orig_q):
            explicit_metric_correction = "fte"
        elif re.search(r"\bi\s+meant\s+headcount\b|\bheadcount\s+(?:not|instead)\b|\bwant\s+headcount\b|\bshow\s+headcount\b|\buse\s+headcount\b", orig_q):
            explicit_metric_correction = "headcount"

        if dataset == "cross_dataset":
            metric_name = str((state.get("semantic_state") or {}).get("metric") or "").strip() or "cross_dataset_join"
            context["previous_subject"] = "cross_dataset_practice_ranking"
            context["previous_metric"] = metric_name
            context["previous_view"] = "cross_dataset_ranking"
            context["previous_aggregation"] = "ranking"
            gp_basis = str((state.get("semantic_state") or {}).get("gp_basis") or "").strip()
            if gp_basis:
                context["gp_basis"] = gp_basis
            top_n = str((state.get("semantic_state") or {}).get("top_n") or "").strip()
            if top_n:
                context["previous_limit"] = top_n
            order = str((state.get("semantic_state") or {}).get("order") or "").strip()
            if order:
                context["order"] = order
            appointments_order = str((state.get("semantic_state") or {}).get("appointments_order") or "").strip()
            if appointments_order:
                context["appointments_order"] = appointments_order
            gp_order = str((state.get("semantic_state") or {}).get("gp_order") or "").strip()
            if gp_order:
                context["gp_order"] = gp_order
            if str((state.get("semantic_state") or {}).get("parent_scope_entity_name") or "").strip():
                context["parent_scope_entity_name"] = str((state.get("semantic_state") or {}).get("parent_scope_entity_name")).strip()
                context["parent_scope_entity_type"] = "icb"
                context["parent_scope_entity_col"] = "icb_name"
            aggregation = "ranking"
        elif dataset == "appointments":
            if "count(distinct prac_code)" in sql_lower:
                context["previous_subject"] = "practice_count"
            elif "count_of_appointments" in sql_lower:
                context["previous_subject"] = "appointments"

            if "appt_status = 'dna'" in sql_lower or " did not attend" in f" {orig_q} " or re.search(r"\bdna\b", orig_q):
                context["previous_metric"] = "dna_rate"
            elif "count_of_appointments" in sql_lower:
                context["previous_metric"] = "appointments_total"
            elif follow_ctx.get("previous_metric"):
                context["previous_metric"] = follow_ctx.get("previous_metric")

            if "hcp_type = 'gp'" in sql_lower or re.search(r"\bgp appointments?\b", orig_q):
                context["previous_staff_group"] = "GP"
            elif "hcp_type = 'nurse'" in sql_lower or re.search(r"\bnurse appointments?\b", orig_q):
                context["previous_staff_group"] = "Nurse"
            elif follow_ctx.get("previous_staff_group"):
                context["previous_staff_group"] = follow_ctx.get("previous_staff_group")

            previous_view = ""
            if "appt_mode" in sql_lower or "mode breakdown" in orig_q or "appointment mode" in orig_q:
                previous_view = "appointment_mode_breakdown"
            elif "hcp_type" in sql_lower or "by hcp type" in orig_q:
                previous_view = "hcp_type_breakdown"
            elif "time_between_book_and_appt" in sql_lower or "time between booking and appointment" in orig_q:
                previous_view = "booking_lead_time_breakdown"
            elif "group by year, month" in sql_lower or plan_intent == "trend" or any(term in orig_q for term in ["trend", "over time", "past year", "last 12 months", "month by month"]):
                previous_view = "appointments_trend"
            elif "sub_icb_location_name" in sql_lower and ("sub-icb" in orig_q or "sub icb" in orig_q):
                previous_view = "sub_icb_breakdown"
            elif follow_ctx.get("previous_view"):
                previous_view = str(follow_ctx.get("previous_view"))
            if previous_view:
                context["previous_view"] = previous_view

            aggregation = "ratio" if context.get("previous_metric") == "dna_rate" else "total"
        else:
            if "count(distinct prac_code)" in sql_lower or any(term in orig_q for term in [
                "how many practices", "number of practices", "gp practices", "practice count",
            ]):
                context["previous_subject"] = "practice_count"

            if explicit_metric_correction:
                context["previous_metric"] = explicit_metric_correction
            elif "patient" in orig_q and ("per gp" in orig_q or "per-gp" in orig_q or "ratio" in orig_q):
                context["previous_metric"] = "patients_per_gp"
            elif "patients_per_gp" in sql_lower or (plan_intent == "ratio" and "total_patients" in sql_lower and "total_gp_fte" in sql_lower):
                context["previous_metric"] = "patients_per_gp"
            elif "headcount" in orig_q or "head count" in orig_q or "count(distinct" in sql_lower:
                context["previous_metric"] = "headcount"
            elif "fte" in orig_q or "sum(fte" in sql_lower:
                context["previous_metric"] = "fte"
            elif follow_ctx.get("previous_metric"):
                context["previous_metric"] = follow_ctx.get("previous_metric")

            if "staff_group = 'nurses'" in sql_lower or "'nurses'" in sql_lower:
                context["previous_staff_group"] = "Nurses"
            elif "staff_group = 'admin'" in sql_lower or "'admin'" in sql_lower:
                context["previous_staff_group"] = "Admin"
            elif "staff_group = 'dpc'" in sql_lower or "'dpc'" in sql_lower:
                context["previous_staff_group"] = "DPC"
            elif "staff_group = 'gp'" in sql_lower or "'gp'" in sql_lower:
                context["previous_staff_group"] = "GP"
            elif follow_ctx.get("previous_staff_group"):
                context["previous_staff_group"] = follow_ctx.get("previous_staff_group")

            previous_view = ""
            if "age_band" in sql_lower or "age distribution" in orig_q or re.search(r"\bage\b", orig_q):
                previous_view = "age_distribution"
            elif "gender" in sql_lower or "gender breakdown" in orig_q or re.search(r"\bgender\b", orig_q):
                previous_view = "gender_breakdown"
            elif "sub_icb_name" in sql_lower and ("sub-icb" in orig_q or "sub icb" in orig_q):
                previous_view = "sub_icb_breakdown"
            elif table == "practice_detailed" and all(token in sql_lower for token in ["total_gp_hc", "total_nurses_hc", "total_admin_hc"]):
                previous_view = "practice_staff_breakdown"
            elif follow_ctx.get("previous_view"):
                previous_view = str(follow_ctx.get("previous_view"))
            if previous_view:
                context["previous_view"] = previous_view

            if "avg(" in sql_lower or " average " in f" {orig_q} ":
                aggregation = "average"
            elif context.get("previous_metric") == "patients_per_gp":
                aggregation = "ratio"
            else:
                aggregation = "total"
        context["previous_aggregation"] = aggregation

        if preview_row:
            current_value = _coerce_preview_number(preview_row.get("current_value", ""))
            benchmark_value = _coerce_preview_number(preview_row.get("national_average", ""))
            difference_value = _coerce_preview_number(preview_row.get("difference", ""))
            pct_difference = _coerce_preview_number(preview_row.get("pct_difference", ""))
            comparison_basis = str(preview_row.get("comparison_basis", "")).strip()
            if current_value is not None:
                context["result_value"] = str(preview_row.get("current_value", "")).strip()
            if benchmark_value is not None:
                context["benchmark_value"] = str(preview_row.get("national_average", "")).strip()
            if comparison_basis:
                context["comparison_basis"] = comparison_basis
            if pct_difference is not None:
                context["pct_difference"] = str(preview_row.get("pct_difference", "")).strip()
            if difference_value is not None:
                if abs(difference_value) <= 0.05:
                    context["result_direction"] = "around"
                elif difference_value > 0:
                    context["result_direction"] = "above"
                else:
                    context["result_direction"] = "below"

        grain_base = context.get("entity_type") or "national"
        if grain_base == "city":
            context["previous_grain"] = "city_total"
        else:
            context["previous_grain"] = f"{grain_base}_{aggregation}"
        if group_dim:
            context["previous_group_dim"] = group_dim
        return _carry_parent_scope(context)

    preview_row = _preview_first_row(state.get("df_preview_md", ""))
    prefers_rank_winner = (
        plan.get("intent") == "topn"
        or any(term in orig_q for term in ["top practice", "top practices", "which practice", "highest practice",
                                           "lowest practice", "top icb", "which icb", "highest icb", "lowest icb"])
    )

    if prefers_rank_winner:
        if preview_row.get("practice_name") and table == "cross_dataset_practice_join":
            ctx["entity_name"] = preview_row["practice_name"]
            ctx["entity_type"] = "practice"
            ctx["entity_col"] = "practice_name"
            if preview_row.get("practice_code"):
                ctx["previous_entity_code"] = str(preview_row["practice_code"]).strip().upper()
            return _with_semantic_state(_finalise_context(ctx))
        if preview_row.get("gp_name") and "practice" in orig_q:
            ctx["entity_name"] = preview_row["gp_name"]
            ctx["entity_type"] = "practice"
            ctx["entity_col"] = "gp_name"
            if preview_row.get("gp_code"):
                ctx["previous_entity_code"] = str(preview_row["gp_code"]).strip().upper()
            return _with_semantic_state(_finalise_context(ctx))
        if preview_row.get("prac_name") and "practice" in orig_q:
            ctx["entity_name"] = preview_row["prac_name"]
            ctx["entity_type"] = "practice"
            ctx["entity_col"] = "prac_name"
            if preview_row.get("prac_code"):
                ctx["previous_entity_code"] = str(preview_row["prac_code"]).strip().upper()
            return _with_semantic_state(_finalise_context(ctx))
        ranked_practice = _ranking_entity_from_answer(state.get("answer", ""), "practice")
        if ranked_practice and "practice" in orig_q:
            ctx["entity_name"] = ranked_practice
            ctx["entity_type"] = "practice"
            ctx["entity_col"] = "prac_name"
            return _with_semantic_state(_finalise_context(ctx))
        if preview_row.get("icb_name") and "icb" in orig_q:
            ctx["entity_name"] = preview_row["icb_name"]
            ctx["entity_type"] = "icb"
            ctx["entity_col"] = "icb_name"
            return _with_semantic_state(_finalise_context(ctx))
        if preview_row.get("region_name") and "region" in orig_q:
            ctx["entity_name"] = preview_row["region_name"]
            ctx["entity_type"] = "region"
            ctx["entity_col"] = "region_name"
            return _with_semantic_state(_finalise_context(ctx))
        ranked_icb = _ranking_entity_from_answer(state.get("answer", ""), "icb")
        if ranked_icb and "icb" in orig_q:
            ctx["entity_name"] = ranked_icb
            ctx["entity_type"] = "icb"
            ctx["entity_col"] = "icb_name"
            return _with_semantic_state(_finalise_context(ctx))
        if preview_row.get("sub_icb_name") and "sub-icb" in orig_q:
            ctx["entity_name"] = preview_row["sub_icb_name"]
            ctx["entity_type"] = "sub_icb"
            ctx["entity_col"] = "sub_icb_name"
            return _with_semantic_state(_finalise_context(ctx))

    # NOTE: SQL patterns use LOWER(TRIM(col_name)) which produces col_name)) — two closing parens.
    # Use \)* to handle zero, one, or two closing parens before LIKE/=.

    # Try to extract practice name from SQL
    m = re.search(r"UPPER\s*\(\s*TRIM\s*\(\s*prac_code\s*\)\s*\)\s*=\s*'([A-Za-z]\d{5})'", sql, re.IGNORECASE)
    if m:
        ctx["previous_entity_code"] = m.group(1).upper()
        ctx["entity_name"] = (preview_row.get("prac_name", "") or ctx["previous_entity_code"]).strip()
        ctx["entity_type"] = "practice"
        ctx["entity_col"] = "prac_code"
        return _with_semantic_state(_finalise_context(ctx))

    m = re.search(r"UPPER\s*\(\s*TRIM\s*\(\s*gp_code\s*\)\s*\)\s*=\s*'([A-Za-z]\d{5})'", sql, re.IGNORECASE)
    if m:
        ctx["previous_entity_code"] = m.group(1).upper()
        ctx["entity_name"] = (preview_row.get("gp_name", "") or ctx["previous_entity_code"]).strip()
        ctx["entity_type"] = "practice"
        ctx["entity_col"] = "gp_code"
        return _with_semantic_state(_finalise_context(ctx))

    m = re.search(r"appt\.practice_code\s*=\s*wf\.practice_code", sql, re.IGNORECASE)
    if m and preview_row.get("practice_code"):
        ctx["previous_entity_code"] = str(preview_row.get("practice_code") or "").strip().upper()
        ctx["entity_name"] = str(preview_row.get("practice_name") or ctx["previous_entity_code"]).strip()
        ctx["entity_type"] = "practice"
        ctx["entity_col"] = "practice_code"
        return _with_semantic_state(_finalise_context(ctx))

    m = re.search(r"prac_name\)*\s*(?:LIKE|=)\s*(?:LOWER\s*\()?'%([^%]+)%'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "practice"
        ctx["entity_col"] = "prac_name"
        return _with_semantic_state(_finalise_context(ctx))

    m = re.search(r"gp_name\)*\s*(?:LIKE|=)\s*(?:LOWER\s*\()?'%([^%]+)%'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "practice"
        ctx["entity_col"] = "gp_name"
        return _with_semantic_state(_finalise_context(ctx))

    # Try to detect city query pattern: (icb_name LIKE '%mapped_icb%' OR sub_icb_name LIKE '%city%')
    # This fires for city queries generated by hard_override or fix_geo_broadening
    city_m = re.search(r"icb_name\)*\s*(?:LIKE|=)\s*(?:LOWER\s*\()?'%?([^%']+)%?'.*?sub_icb_name\)*\s*(?:LIKE|=)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if city_m:
        icb_val = city_m.group(1).strip().lower()
        city_val = city_m.group(2).strip().lower()
        # If the sub_icb value is a known city in our mapping, save as "city" type
        if city_val in _CITY_TO_ICB:
            ctx["entity_name"] = city_val
            ctx["entity_type"] = "city"
            ctx["entity_col"] = "icb_name"
            ctx["mapped_icb"] = icb_val
            # Metric detection still needed — falls through below
            if "patient" in orig_q and ("per gp" in orig_q or "per-gp" in orig_q or "ratio" in orig_q):
                ctx["previous_metric"] = "patients_per_gp"
            elif "patients_per_gp" in sql.lower() or (str(plan.get("intent") or "").lower() == "ratio" and "total_patients" in sql.lower() and "total_gp_fte" in sql.lower()):
                ctx["previous_metric"] = "patients_per_gp"
            elif "headcount" in orig_q or "head count" in orig_q or "count(distinct" in sql.lower():
                ctx["previous_metric"] = "headcount"
            elif "fte" in orig_q or "sum(fte" in sql.lower():
                ctx["previous_metric"] = "fte"
            return _with_semantic_state(_finalise_context(ctx))

    # Try to extract ICB name from SQL
    m = re.search(r"icb_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "icb"
        ctx["entity_col"] = "icb_name"
        return _with_semantic_state(_finalise_context(ctx))

    # Try to extract sub-ICB name from SQL
    m = re.search(r"sub_icb_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "sub_icb"
        ctx["entity_col"] = "sub_icb_name"
        return _with_semantic_state(_finalise_context(ctx))

    m = re.search(r"sub_icb_location_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "sub_icb"
        ctx["entity_col"] = "sub_icb_location_name"
        return _with_semantic_state(_finalise_context(ctx))

    m = re.search(r"pcn_name\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(1).strip()
        ctx["entity_type"] = "pcn"
        ctx["entity_col"] = "pcn_name"
        return _with_semantic_state(_finalise_context(ctx))

    # Try to extract region name
    m = re.search(r"((?:comm_)?region_name)\)*\s*(?:=|LIKE)\s*(?:LOWER\s*\()?'%?([^%']+)%?'", sql, re.IGNORECASE)
    if m:
        ctx["entity_name"] = m.group(2).strip()
        ctx["entity_type"] = "region"
        ctx["entity_col"] = m.group(1).strip()
        return _with_semantic_state(_finalise_context(ctx))

    # For queries where entity is in the result but not in SQL WHERE (e.g. GROUP BY / top-1 queries),
    # extract entity from the first data row of df_preview_md
    if not ctx.get("entity_name"):
        if preview_row:
            if ("gp_name" in preview_row and preview_row["gp_name"]
                and (table == "practice" or "practice" in orig_q or preview_row.get("gp_code"))):
                ctx["entity_name"] = preview_row["gp_name"]
                ctx["entity_type"] = "practice"
                ctx["entity_col"] = "gp_name"
                if preview_row.get("gp_code"):
                    ctx["previous_entity_code"] = str(preview_row["gp_code"]).strip().upper()
            elif ("prac_name" in preview_row and preview_row["prac_name"]
                and (table == "practice_detailed" or "practice" in orig_q or preview_row.get("prac_code"))):
                ctx["entity_name"] = preview_row["prac_name"]
                ctx["entity_type"] = "practice"
                ctx["entity_col"] = "prac_name"
                if preview_row.get("prac_code"):
                    ctx["previous_entity_code"] = str(preview_row["prac_code"]).strip().upper()
            elif ("practice_name" in preview_row and preview_row["practice_name"]
                and table == "cross_dataset_practice_join"):
                ctx["entity_name"] = preview_row["practice_name"]
                ctx["entity_type"] = "practice"
                ctx["entity_col"] = "practice_name"
                if preview_row.get("practice_code"):
                    ctx["previous_entity_code"] = str(preview_row["practice_code"]).strip().upper()
            elif "sub_icb_location_name" in preview_row and preview_row["sub_icb_location_name"]:
                ctx["entity_name"] = preview_row["sub_icb_location_name"]
                ctx["entity_type"] = "sub_icb"
                ctx["entity_col"] = "sub_icb_location_name"
            elif "pcn_name" in preview_row and preview_row["pcn_name"]:
                ctx["entity_name"] = preview_row["pcn_name"]
                ctx["entity_type"] = "pcn"
                ctx["entity_col"] = "pcn_name"
            elif "icb_name" in preview_row and preview_row["icb_name"]:
                ctx["entity_name"] = preview_row["icb_name"]
                ctx["entity_type"] = "icb"
                ctx["entity_col"] = "icb_name"
            elif "region_name" in preview_row and preview_row["region_name"]:
                ctx["entity_name"] = preview_row["region_name"]
                ctx["entity_type"] = "region"
                ctx["entity_col"] = "region_name"

    # If follow_up_context was used, carry it forward
    if follow_ctx and not ctx.get("entity_name"):
        carried = dict(follow_ctx)
        if group_dim:
            carried["previous_group_dim"] = group_dim
        if table and not carried.get("table"):
            carried["table"] = table
        return _with_semantic_state(_finalise_context(carried))

    return _with_semantic_state(_finalise_context(ctx))


def _semantic_followup_display_question(question: str, follow_ctx: Optional[Dict[str, Any]]) -> str:
    q = (question or "").strip()
    if not q or not follow_ctx:
        return q

    entity_name = str(follow_ctx.get("entity_name") or "").strip()
    entity_code = str(follow_ctx.get("previous_entity_code") or "").strip()
    entity_type = str(follow_ctx.get("entity_type") or "").strip().lower()
    metric_phrase = _follow_up_metric_phrase(follow_ctx)

    if re.match(r"^compare(?:\s+(?:this|that|it))?\s+(?:with|to|against)\s+national average\b", q, flags=re.IGNORECASE):
        if entity_name:
            return f"Compare {metric_phrase} for {entity_name} with the national average"
        return f"Compare {metric_phrase} with the national average"

    if re.match(r"^compare(?:\s+(?:this|that|it))?\s+(?:to|with)\s+national average\b", q, flags=re.IGNORECASE):
        if entity_name:
            return f"Compare {metric_phrase} for {entity_name} with the national average"
        return f"Compare {metric_phrase} with the national average"

    if re.match(r"^what\s+is\s+the\s+patients?\s+per\s+gp\s+ratio\s+here\b", q, flags=re.IGNORECASE):
        if entity_type == "practice" and (entity_name or entity_code):
            label = entity_name or entity_code
            return f"What is the patients-per-GP ratio at practice {label}?"
        if entity_name:
            return f"What is the patients-per-GP ratio in {entity_name}?"

    if re.match(r"^(?:what\s+about|how\s+about)\s+(?:in|for)\s+.+", q, flags=re.IGNORECASE):
        target = re.sub(r"^(?:what\s+about|how\s+about)\s+", "", q, flags=re.IGNORECASE).strip(" ?!.")
        target = re.sub(r"^(?:in|for|within|across)\s+", "", target, flags=re.IGNORECASE).strip()
        if target:
            return f"Show {metric_phrase} for {target}"

    return q


# =============================================================================
# Answer Grading & Confidence Scoring
# =============================================================================
def _compute_confidence(state: StateData) -> Dict[str, Any]:
    """
    Compute confidence score from heuristic signals.
    Returns { "score": 0.0-1.0, "level": "high"|"medium"|"low", "signals": [...] }
    """
    signals = []
    score = 1.0  # start at maximum, deduct for issues

    plan = state.get("plan", {}) or {}
    answer = state.get("answer", "")
    rows = int(state.get("_rows", 0))
    attempts = int(state.get("attempts", 0))
    sql = state.get("sql", "")
    last_error = state.get("last_error")
    is_knowledge = state.get("_is_knowledge", False)
    empty = bool(state.get("_empty", False))
    semantic_path = dict(state.get("semantic_path") or {})

    # Knowledge questions — typically high confidence
    if is_knowledge:
        signals.append("knowledge_answer")
        return {"score": 0.9, "level": "high", "signals": signals}

    # Out-of-scope questions — medium (we're confident it's OOS, but answer is a suggestion)
    if not plan.get("in_scope", True):
        signals.append("out_of_scope")
        return {"score": 0.7, "level": "medium", "signals": signals}

    # Track query route for observability
    route = state.get("_query_route", "unknown")
    signals.append(f"route:{route}")

    # ── Positive signals ──
    if rows > 0:
        signals.append(f"data_returned ({rows} rows)")
    else:
        score -= 0.4
        signals.append("no_data_returned")

    if attempts == 0:
        signals.append("first_attempt_success")
    elif attempts == 1:
        score -= 0.1
        signals.append("needed_1_retry")
    else:
        score -= 0.2
        signals.append(f"needed_{attempts}_retries")

    # Hard intent (template SQL) = very reliable
    if state.get("_hard_intent"):
        score = min(score + 0.1, 1.0)
        signals.append("template_sql_used")

    # Few-shot example match boosts confidence
    if state.get("_few_shot_best_sim", 0) > 0.85:
        score = min(score + 0.05, 1.0)
        signals.append(f"strong_few_shot_match ({state.get('_few_shot_best_sim', 0):.2f})")

    if semantic_path.get("used"):
        score = min(score + 0.05, 1.0)
        signals.append("semantic_fast_path")
        if str(semantic_path.get("dataset") or "").strip().lower() == "cross":
            score -= 0.05
            signals.append("cross_dataset_join")

    if state.get("_clarification_resolved", False):
        score = min(score + 0.05, 1.0)
        signals.append("clarification_resolved")

    # Entity fuzzy matching = slight uncertainty
    resolved = state.get("resolved_entities", {})
    if resolved:
        for key, vals in resolved.items():
            if isinstance(vals, list) and vals:
                top_candidate = vals[0]
                if not isinstance(top_candidate, dict):
                    continue
                match_score = top_candidate.get("score", top_candidate.get("similarity", 1.0))
                match_type = top_candidate.get("match_type", "")
                if match_type == "fuzzy" and match_score < 0.85:
                    score -= 0.1
                    signals.append(f"fuzzy_entity_match ({key})")
                if len(vals) > 1:
                    second_candidate = vals[1] if len(vals) > 1 and isinstance(vals[1], dict) else {}
                    second_score = float(second_candidate.get("score", second_candidate.get("similarity", 0.0)) or 0.0)
                    if abs(float(match_score or 0.0) - second_score) <= 0.03:
                        score -= 0.1
                        signals.append(f"ambiguous_entity_match ({key})")

    # Empty result despite having SQL = uncertain
    if empty and sql:
        score -= 0.15
        signals.append("sql_returned_empty")

    # Residual error = problem
    if last_error:
        score -= 0.25
        signals.append("unresolved_error")

    # Answer length sanity
    if answer and len(answer.strip()) < 30:
        score -= 0.1
        signals.append("very_short_answer")

    # Clamp
    score = max(0.1, min(1.0, score))

    # Level
    if score >= 0.8:
        level = "high"
    elif score >= 0.5:
        level = "medium"
    else:
        level = "low"

    return {"score": round(score, 2), "level": level, "signals": signals}


def node_grade_answer(state: StateData) -> StateData:
    """
    Post-summarise node: computes confidence score and adds a confidence
    note to the answer. [L8] Emoji badges removed — let frontend handle visual styling.
    """
    confidence = _compute_confidence(state)
    state["_confidence"] = confidence

    answer = state.get("answer", "")
    level = confidence["level"]

    # Only add low/medium confidence note — high confidence needs no disclaimer
    if level == "low" and answer:
        state["answer"] = answer + "\n\n*Confidence: Low — results may be incomplete or approximate. Try rephrasing for better accuracy.*"
    elif level == "medium" and answer:
        state["answer"] = answer + "\n\n*Confidence: Medium — results should be broadly correct but may have minor gaps.*"

    logger.info("Answer grade: %s (score=%.2f, signals=%s)",
                level, confidence["score"], confidence["signals"])

    return state


# =============================================================================
# Multi-Turn Clarification Node
# =============================================================================
def _clarification_suggestions(original_q: str, clarification_q: str, plan: Dict[str, Any]) -> List[str]:
    """Generate targeted suggestions for a clarification turn."""
    suggestions: List[str] = []
    q_low = original_q.lower()
    clarification_q_low = clarification_q.lower()
    if all(term in clarification_q_low for term in ["national total", "average sub-icb", "average icb"]):
        suggestions = [
            "Compare with national total",
            "Compare with average sub-ICB area",
            "Compare with average ICB",
        ]
    elif "gp headcount, gp fte, or patients-per-gp ratio" in clarification_q_low:
        suggestions = [
            "Compare GP headcount",
            "Compare GP FTE",
            "Compare patients-per-GP ratio",
        ]
    elif "patients-per-gp ratio" in clarification_q_low and "nationally, by region, or by icb" in clarification_q_low:
        suggestions = [
            "Patients-per-GP ratio nationally",
            "Patients-per-GP ratio by region",
            "Patients-per-GP ratio by ICB",
        ]
    if not suggestions and "compare " in q_low and not _question_has_explicit_metric(q_low):
        suggestions = [
            "Compare GP headcount",
            "Compare GP FTE",
            "Compare patients-per-GP ratio",
        ]
    elif not suggestions and any(term in q_low for term in ["pressure", "pressured", "strain", "stretched"]):
        suggestions = [
            "Patients-per-GP ratio nationally",
            "Patients-per-GP ratio by region",
            "Patients-per-GP ratio by ICB",
        ]
    elif not suggestions and ("region" in q_low or "icb" in q_low):
        suggestions = [
            "Show by region",
            "Show by ICB",
            "Show national total",
        ]
    elif not suggestions and ("trend" in q_low or "over time" in q_low):
        suggestions = [
            "Last 12 months",
            "Last 3 years",
            "Year over year comparison",
        ]
    elif not suggestions and "practice" in q_low:
        suggestions = [
            "All practices nationally",
            "Top 10 practices by GP FTE",
            "A specific practice (name it)",
        ]
    elif not suggestions:
        suggestions = [
            "GP FTE nationally",
            "GP headcount by region",
            "Staff breakdown by role",
        ]
    return suggestions


def node_clarify(state: StateData) -> StateData:
    """
    When the planner detects an ambiguous query, this node short-circuits
    the pipeline and returns a clarification question to the user.

    The original question + partial plan are saved in ConversationMemory
    so the next user message can be merged with the original question.
    """
    sid = state.get("session_id", "")
    original_q = state.get("original_question", state["question"])
    clarification_q = state.get("_clarification_question", "")
    plan = state.get("plan", {}) or {}

    suggestions = _clarification_suggestions(original_q, clarification_q, plan)

    if _HAS_LANGGRAPH_INTERRUPT and interrupt is not None and Command is not None:
        MEMORY.set_pending_clarification(
            sid,
            original_question=original_q,
            clarification_question=clarification_q,
            partial_plan=plan,
        )
        prompt_payload = {
            "kind": "clarification",
            "question": clarification_q,
            "message": f"I'd like to make sure I give you the right data. {clarification_q}",
            "suggestions": suggestions,
            "original_question": original_q,
            "plan": {
                "intent": plan.get("intent"),
                "table": plan.get("table"),
                "notes": plan.get("notes"),
            },
            "semantic_state": state.get("semantic_state", {}),
        }
        logger.info("node_clarify | interrupting with question='%s' suggestions=%s", clarification_q[:120], suggestions)
        clarification_answer = interrupt(prompt_payload)
        MEMORY.clear_pending_clarification(sid)
        merged_q = f"{original_q} ({str(clarification_answer).strip()})"
        logger.info("node_clarify | resumed with merged question=%r", merged_q[:140])
        return Command(
            update={
                "question": merged_q,
                "original_question": merged_q,
                "_clarification_resolved": True,
                "_needs_clarification": False,
                "_clarification_question": "",
                "answer": "",
                "sql": "",
                "df_preview_md": "",
                "_rows": 0,
                "_empty": False,
                "needs_retry": False,
                "last_error": None,
                "resolved_entities": {},
                "plan": {},
                "suggestions": [],
            },
            goto="knowledge_check",
        )

    # Fallback path when LangGraph interrupts are unavailable.
    MEMORY.set_pending_clarification(
        sid,
        original_question=original_q,
        clarification_question=clarification_q,
        partial_plan=plan,
    )
    state["answer"] = f"I'd like to make sure I give you the right data. {clarification_q}"
    state["sql"] = ""
    state["df_preview_md"] = ""
    state["_rows"] = 0
    state["_empty"] = False
    state["suggestions"] = suggestions
    MEMORY.add_turn(sid, original_q, state["answer"], sql="")
    logger.info("node_clarify | fallback asked: '%s' | suggestions=%s", clarification_q[:120], suggestions)
    return state


def _pipeline_done(state: DatasetPipelineState) -> DatasetPipelineState:
    return state


def make_sql_pipeline(config: DatasetConfig):
    """
    Compile a dataset-specific SQL subgraph.

    We keep one supervisor-level checkpointer and invoke these as subgraphs so
    their state is captured within the parent thread/checkpoint namespace.
    """
    dataset_name = config["name"]
    query_strategy = config.get("query_strategy", "")
    planning_mode = config.get("planning_mode", "")
    entity_resolution_mode = config.get("entity_resolution_mode", "")
    sql_generation_mode = config.get("sql_generation_mode", "")
    validation_mode = config.get("validation_mode", "")

    latest_vocab_node = make_dataset_vocab_loader(config)
    query_node = make_dataset_query_node(config)
    schema_narrow_node = node_schema_narrow
    planner_node = make_dataset_planner_node(config)
    resolver_node = make_dataset_entity_resolver_node(config)
    sql_generator_node = make_dataset_sql_generator_node(config)
    sql_runner_node = make_dataset_sql_runner(config) if dataset_name == "appointments" else node_run_sql
    validator_node = make_dataset_validator_node(config)

    g = StateGraph(DatasetPipelineState)

    g.add_node("done", _pipeline_done)
    g.add_node("latest_vocab", latest_vocab_node)
    g.add_node("query", query_node)
    g.add_node("schema_narrow", schema_narrow_node)
    g.add_node("plan", planner_node)
    g.add_node("resolve_entities", resolver_node)
    g.add_node("generate_sql", sql_generator_node)
    g.add_node("run_sql", sql_runner_node)
    g.add_node("validate_or_fix", validator_node)

    g.set_entry_point("latest_vocab")
    g.add_edge("latest_vocab", "query")

    def route_after_query(state: DatasetPipelineState) -> str:
        if state.get("_needs_clarification", False):
            return "done"
        if query_strategy == "rules_query_node":
            if state.get("sql"):
                return "run_sql"
            return "schema_narrow"
        if state.get("sql"):
            return "run_sql"
        return "schema_narrow"

    g.add_conditional_edges("query", route_after_query, {
        "done": "done",
        "schema_narrow": "schema_narrow",
        "run_sql": "run_sql",
    })

    def route_after_schema_narrow(state: DatasetPipelineState) -> str:
        route = state.get("_query_route", "data_complex")
        if route == "data_simple" and _should_use_fast_simple_route(state):
            return "generate_sql" if planning_mode != "rules_embedded" else "plan"
        return "plan"

    g.add_conditional_edges("schema_narrow", route_after_schema_narrow, {
        "plan": "plan",
        "generate_sql": "generate_sql",
    })

    def route_after_plan(state: DatasetPipelineState) -> str:
        if not (state.get("plan") or {}).get("in_scope", True):
            return "done"
        if state.get("_needs_clarification", False):
            return "done"
        if planning_mode == "rules_embedded":
            return "run_sql" if state.get("sql") else "done"
        if entity_resolution_mode == "none":
            if sql_generation_mode == "rules_embedded":
                return "run_sql" if state.get("sql") else "done"
            return "generate_sql"
        return "resolve_entities"

    g.add_conditional_edges("plan", route_after_plan, {
        "done": "done",
        "resolve_entities": "resolve_entities",
        "generate_sql": "generate_sql",
        "run_sql": "run_sql",
    })

    def route_after_resolve_entities(state: DatasetPipelineState) -> str:
        if not (state.get("plan") or {}).get("in_scope", True):
            return "done"
        if state.get("_needs_clarification", False):
            return "done"
        if sql_generation_mode == "rules_embedded":
            return "run_sql" if state.get("sql") else "done"
        return "generate_sql"

    g.add_conditional_edges("resolve_entities", route_after_resolve_entities, {
        "done": "done",
        "generate_sql": "generate_sql",
        "run_sql": "run_sql",
    })

    if sql_generation_mode != "rules_embedded":
        g.add_edge("generate_sql", "run_sql")
    else:
        g.add_edge("generate_sql", "done")

    def route_after_validate(state: DatasetPipelineState) -> str:
        return "run_sql" if state.get("needs_retry", False) else "done"

    if validation_mode == "none":
        g.add_edge("run_sql", "done")
    else:
        g.add_edge("run_sql", "validate_or_fix")
        g.add_conditional_edges("validate_or_fix", route_after_validate, {
            "run_sql": "run_sql",
            "done": "done",
        })

    g.add_edge("done", END)
    return g.compile()


WORKFORCE_SQL_PIPELINE = make_sql_pipeline(WORKFORCE_CONFIG)
APPOINTMENTS_SQL_PIPELINE = make_sql_pipeline(APPOINTMENTS_CONFIG)


# =============================================================================
# Build Graph (Adaptive Routing + Multi-Turn Clarification)
# =============================================================================
def build_graph():
    g = StateGraph(SupervisorState)

    g.add_node("init", node_init)
    g.add_node("query_rewriter", node_query_rewriter)
    g.add_node("dataset_classify", node_dataset_classify)
    g.add_node("knowledge_check", node_knowledge_check)
    g.add_node("supervisor_decide", node_supervisor_decide)
    g.add_node("multi_worker_dispatch", node_multi_worker_dispatch)
    g.add_node("cross_dataset_query", node_cross_dataset_query)
    g.add_node("knowledge_answer", node_knowledge_answer)
    g.add_node("workforce_pipeline", WORKFORCE_SQL_PIPELINE)
    g.add_node("appointments_pipeline", APPOINTMENTS_SQL_PIPELINE)
    g.add_node("knowledge_rag_worker", node_knowledge_rag_worker)
    g.add_node("multi_worker_merge", node_multi_worker_merge)
    g.add_node("clarify", node_clarify)
    g.add_node("summarize", node_summarize)
    g.add_node("visualization_plan", node_visualization_plan)
    g.add_node("grade_answer", node_grade_answer)

    g.set_entry_point("init")

    # init → query_rewriter → dataset_classify → knowledge_check
    g.add_edge("init", "query_rewriter")
    g.add_edge("query_rewriter", "dataset_classify")
    g.add_edge("dataset_classify", "knowledge_check")

    # knowledge_check → route based on adaptive classification
    def route_after_knowledge_check(state: SupervisorState) -> str:
        if state.get("_query_route") == "out_of_scope":
            return "summarize"
        if state.get("_is_knowledge", False):
            return "knowledge_answer"
        return "supervisor_decide"

    g.add_conditional_edges("knowledge_check", route_after_knowledge_check, {
        "knowledge_answer": "knowledge_answer",
        "supervisor_decide": "supervisor_decide",
        "summarize": "summarize",
    })

    def route_after_supervisor_decide(state: SupervisorState) -> str:
        if state.get("supervisor_mode") == "cross_dataset":
            return "cross_dataset_query"
        if state.get("supervisor_mode") == "semantic_cross_dataset":
            return "cross_dataset_query"
        if state.get("supervisor_mode") == "multi_worker":
            return "multi_worker_dispatch"
        return "appointments_pipeline" if state.get("dataset") == "appointments" else "workforce_pipeline"

    g.add_conditional_edges("supervisor_decide", route_after_supervisor_decide, {
        "cross_dataset_query": "cross_dataset_query",
        "semantic_cross_dataset": "cross_dataset_query",
        "multi_worker_dispatch": "multi_worker_dispatch",
        "workforce_pipeline": "workforce_pipeline",
        "appointments_pipeline": "appointments_pipeline",
    })

    # knowledge_answer → grade_answer → END
    g.add_edge("knowledge_answer", "grade_answer")

    def route_after_dataset_pipeline(state: SupervisorState) -> str:
        if state.get("_needs_clarification", False):
            return "clarify"
        if state.get("supervisor_mode") == "cross_dataset":
            return "visualization_plan"
        if not (state.get("plan") or {}).get("in_scope", True):
            return "grade_answer" if state.get("answer") else "summarize"
        if state.get("sql") or state.get("df_preview_md") or state.get("_rows", 0):
            return "summarize"
        return "grade_answer"

    g.add_conditional_edges("workforce_pipeline", route_after_dataset_pipeline, {
        "clarify": "clarify",
        "summarize": "summarize",
        "visualization_plan": "visualization_plan",
        "grade_answer": "grade_answer",
    })
    g.add_conditional_edges("appointments_pipeline", route_after_dataset_pipeline, {
        "clarify": "clarify",
        "summarize": "summarize",
        "visualization_plan": "visualization_plan",
        "grade_answer": "grade_answer",
    })
    g.add_conditional_edges("multi_worker_dispatch", route_after_dataset_pipeline, {
        "clarify": "clarify",
        "summarize": "summarize",
        "visualization_plan": "visualization_plan",
        "grade_answer": "grade_answer",
    })
    g.add_conditional_edges("cross_dataset_query", route_after_dataset_pipeline, {
        "clarify": "clarify",
        "summarize": "summarize",
        "visualization_plan": "visualization_plan",
        "grade_answer": "grade_answer",
    })

    # clarify → END (return clarification question to user; pipeline resumes next turn)
    g.add_edge("clarify", END)

    def route_after_summarize(state: SupervisorState) -> str:
        if state.get("supervisor_mode") == "multi_worker" and state.get("knowledge_worker_answer"):
            return "multi_worker_merge"
        worker_plan = state.get("worker_plan") or {}
        if state.get("supervisor_mode") == "multi_worker" and worker_plan.get("knowledge_question"):
            return "knowledge_rag_worker"
        return "visualization_plan"

    g.add_conditional_edges("summarize", route_after_summarize, {
        "multi_worker_merge": "multi_worker_merge",
        "knowledge_rag_worker": "knowledge_rag_worker",
        "visualization_plan": "visualization_plan",
    })
    g.add_edge("knowledge_rag_worker", "multi_worker_merge")
    g.add_edge("multi_worker_merge", "visualization_plan")
    g.add_edge("visualization_plan", "grade_answer")
    g.add_edge("grade_answer", END)

    # [H1] LangGraph checkpointing — enables durable state for interrupted runs
    if _HAS_CHECKPOINTER:
        try:
            _checkpoint_conn = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=False)
            checkpointer = SqliteSaver(_checkpoint_conn)
            logger.info("LangGraph SqliteSaver checkpointer enabled at %s", CHECKPOINT_DB_PATH)
            return g.compile(checkpointer=checkpointer)
        except Exception as e:
            logger.warning("Failed to initialise checkpointer: %s — running without", e)
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
        "version": _dataset_version(),
    }


@app.get("/health/detail")
def health_detail():
    """Detailed health check — for internal / admin use."""
    return {
        "ok": True,
        "athena_db": ATHENA_DATABASE,
        "appointments_athena_db": APPOINTMENTS_ATHENA_DATABASE,
        "allowed_tables": {
            "workforce": sorted(list(ALLOWED_TABLES)),
            "appointments": sorted(list(APPOINTMENTS_CONFIG["allowed_tables"])),
        },
        "domain_notes_loaded": bool(DOMAIN_NOTES_TEXT.strip()),
        "appointments_domain_notes_loaded": bool(APPOINTMENTS_DOMAIN_NOTES_TEXT.strip()),
        "column_dict_loaded": {name: bool(data) for name, data in COLUMN_DICTS.items()},
        "schema_override_loaded": {k: len(v) for k, v in _SCHEMA_OVERRIDE.items()},
        "memory_sessions": MEMORY.session_count,  # [L9] Use property instead of _store
        "query_cache_size": len(_QUERY_CACHE),
        "few_shot_examples": {name: retriever.count for name, retriever in FEW_SHOT_RETRIEVERS.items()},
        "few_shot_ready": {name: retriever.ready for name, retriever in FEW_SHOT_RETRIEVERS.items()},
        "semantic_cache_size": _SEMANTIC_CACHE.size,
        "long_term_memory": LONG_TERM_MEMORY.stats,
        "checkpointer_enabled": _HAS_CHECKPOINTER,
        "version": _dataset_version(),
    }


@app.post("/admin/clear-cache")
def clear_cache():
    """Clear the semantic cache — useful after deployments with SQL fixes."""
    with _SEMANTIC_CACHE._lock:
        count = len(_SEMANTIC_CACHE._entries)
        _SEMANTIC_CACHE._entries.clear()
    _QUERY_CACHE.clear()
    return {"cleared": True, "semantic_entries_removed": count, "query_cache_cleared": True}


@app.get("/memory")
def memory_endpoint():
    """Inspect the long-term memory store."""
    return {
        "stats": LONG_TERM_MEMORY.stats,
        "entries": [
            {
                "question": e["question"],
                "table": e["table"],
                "confidence": e.get("confidence"),
                "learned_at": e.get("learned_at"),
                "use_count": e.get("use_count", 0),
            }
            for e in LONG_TERM_MEMORY._entries
        ],
    }


@app.post("/memory/flush")
def memory_flush_endpoint():
    """Force save long-term memory to disk."""
    LONG_TERM_MEMORY.flush()
    return {"ok": True, "saved_count": LONG_TERM_MEMORY.count}


# =============================================================================
# Shared helpers for /chat and /chat/stream  [H5]
# =============================================================================
_FOLLOWUP_SIGNALS = (
    # Deictic references — "this/that/the" + entity type
    "this practice", "this icb", "this region", "this area",
    "that practice", "that icb", "that region", "that area",
    "the same practice", "the same icb", "the same region",
    "the above", "same ", "these practices",
    # Pronoun references
    "for them", "for it", "about it", "about them",
    # Implicit references — short follow-ups
    "how has this changed", "how has it changed", "how has that changed",
    "what about", "and by ", "now show", "now break",
    "break it down", "break this down", "drill down",
    # Correction patterns
    "i meant", "i mean ", "instead of", "not that",
)


def _is_followup(question: str) -> bool:
    """Check if question looks like a follow-up (skip semantic cache).
    Uses a broader set of signals to prevent incorrect cache hits for
    context-dependent questions."""
    q = question.lower().strip()
    # Short questions (< 5 words) are likely follow-ups
    if len(q.split()) <= 3 and not q.endswith("?"):
        return True
    if re.search(r"^how\s+(?:has|have|did|does)\s+(?:this|that|it)\s+changed\b", q):
        return True
    if re.search(r"^how\s+(?:has|have)\s+(?:this|that|it)\s+changed\s+over\b", q):
        return True
    if re.search(r"^what\s+is\s+the\s+(?:[\w\-/]+\s+)*(?:ratio|rate|trend|breakdown|count|total|average|proportion)\??$", q):
        return True
    if re.search(r"^show\s+(?:me\s+)?(?:the\s+)?top\s+(?:\d+\s+)?practices?\b", q):
        return True
    return any(w in q for w in _FOLLOWUP_SIGNALS)


def _should_skip_semantic_cache(question: str, session_id: str = "") -> bool:
    """
    Be conservative with semantic cache on context-sensitive prompts. Reusing a
    standalone cached answer for a follow-up like "What is the patients-per-GP
    ratio?" is worse than missing a cache hit.
    """
    if _is_followup(question) or is_follow_up(question):
        return True

    q = (question or "").lower().strip()
    if _parse_cross_dataset_request(question):
        return True

    follow_ctx: Dict[str, Any] = {}
    if session_id:
        follow_ctx = dict(MEMORY.get_entity_context(session_id) or {})
        if _parse_cross_dataset_request(question, follow_ctx):
            return True

        # Entity-scoped seed questions are often the first turn in a multi-turn
        # chain. Recomputing them is safer than reusing a cached answer whose
        # reconstructed context may be too thin for downstream follow-ups.
        if (
            extract_practice_code(question)
            or re.search(r"\b(?:in|within|across)\s+.+", q)
            or any(token in q for token in [" icb", "region", "sub-icb", "sub icb", "practice p"])
        ):
            return True

    if follow_ctx:
        follow_dataset = str(
            (follow_ctx.get("semantic_state") or {}).get("dataset")
            or follow_ctx.get("dataset")
            or ""
        ).strip().lower()
        if follow_dataset == "cross_dataset":
            if re.search(r"^compare(?:\s+(?:this|that|it))?\s+(?:with|to|against)\s+.+", q):
                return True
            if re.search(r"^show\s+(?:this|that|it)\s+by\s+.+", q):
                return True
            if re.search(r"^(?:what\s+about|how\s+about)\s+the\s+(?:lowest|highest|fewest|most|least).+", q):
                return True

        if re.search(r"^what\s+is\s+the\s+(?:[\w\-/]+\s+)*(?:ratio|rate|trend|breakdown|count|total|average|proportion)\??$", q):
            return True
        if re.search(r"^(?:what\s+about|how\s+about)\s+.+", q):
            return True
        if re.search(r"^(?:break|split|show)\s+(?:this|that|it)\s+down\b", q):
            return True
        if re.search(r"^show\s+(?:me\s+)?(?:the\s+)?top\s+(?:\d+\s+)?practices?\b", q):
            return True
        if re.search(r"^show\s+the\s+full\s+staff\s+breakdown\b", q):
            return True

    return False


def _looks_like_clarification_answer(question: str) -> bool:
    """
    Heuristic for deciding whether an incoming turn is probably the user's
    short answer to a clarification prompt rather than a brand-new question.
    """
    q = (question or "").strip()
    if not q:
        return False
    q_low = q.lower()
    words = re.findall(r"[a-z0-9&'-]+", q_low)
    if not words or len(words) > 6 or q_low.endswith("?"):
        return False
    if re.match(r"^(how|what|which|who|when|where|why)\b", q_low):
        return False
    if re.match(r"^(show|give|compare|break|split|list|tell)\b", q_low):
        return False
    return True


def _build_meta(out: Dict[str, Any], request_id: str, elapsed: float,
                cache_hit: bool = False) -> Dict[str, Any]:
    """Build response metadata dict from agent output. [H5] Single source of truth."""
    return {
        "dataset": out.get("dataset", "workforce"),
        "plan": out.get("plan", {}),
        "resolved_entities": out.get("resolved_entities", {}),
        "semantic_state": out.get("semantic_state", {}),
        "semantic_request_v9": out.get("semantic_request_v9", {}),
        "semantic_path": out.get("semantic_path", {}),
        "rewritten_question": out.get("rewritten_question", ""),
        "rewrite_notes": out.get("rewrite_notes", ""),
        "candidate_tables": out.get("candidate_tables", []),
        "schema_narrowing_notes": out.get("schema_narrowing_notes", ""),
        "viz_plan": out.get("viz_plan", {}),
        "attempts": int(out.get("attempts", 0)),
        "last_error": out.get("last_error"),
        "latest_year": out.get("latest_year"),
        "latest_month": out.get("latest_month"),
        "time_range": out.get("time_range"),
        "rows_returned": int(out.get("_rows", 0)),
        "hard_intent": out.get("_hard_intent"),
        "follow_up_context": out.get("follow_up_context"),
        "query_route": out.get("_query_route", "unknown"),
        "dataset_routing": out.get("_dataset_routing", {}),
        "query_routing": out.get("_query_routing", {}),
        "request_id": request_id,
        "elapsed_seconds": round(elapsed, 2),
        "semantic_cache_hit": cache_hit,
        "confidence": out.get("_confidence", {}),
        "needs_clarification": bool(out.get("_needs_clarification", False)),
        "clarification_resolved": bool(out.get("_clarification_resolved", False)),
        "clarification_question": out.get("_clarification_question", ""),
    }


def _build_result_dict(out: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """Build the result dict used by both endpoints. [H5]"""
    return {
        "answer": out.get("answer", ""),
        "sql": out.get("sql", ""),
        "preview_markdown": out.get("df_preview_md", ""),
        "meta": meta,
        "suggestions": out.get("suggestions", []),
    }


def _langgraph_thread_id(session_id: str) -> str:
    """Stable LangGraph thread id per chat session."""
    sid = str(session_id or "").strip()
    return f"session::{sid}" if sid else f"anon::{uuid.uuid4().hex[:8]}"


def _graph_config_for_thread(thread_id: str) -> Dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


def _get_pending_graph_interrupts(thread_id: str):
    """Return any pending LangGraph interrupts for this session thread."""
    if not (_HAS_CHECKPOINTER and _HAS_LANGGRAPH_INTERRUPT):
        return ()
    try:
        snapshot = AGENT.get_state(_graph_config_for_thread(thread_id))
        return tuple(getattr(snapshot, "interrupts", ()) or ())
    except Exception as e:
        logger.debug("pending interrupt lookup failed for %s: %s", thread_id, str(e)[:160])
        return ()


def _semantic_state_from_context(context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Compact typed semantic frame carried between turns and exposed in meta."""
    ctx = dict(context or {})
    if not ctx:
        return {}
    frame: SemanticFrame = {}
    mapping = {
        "dataset": "dataset",
        "metric": "previous_metric",
        "staff_group": "previous_staff_group",
        "entity_type": "entity_type",
        "entity_name": "entity_name",
        "entity_code": "previous_entity_code",
        "table": "table",
        "view": "previous_view",
        "aggregation": "previous_aggregation",
        "grain": "previous_grain",
        "group_dim": "previous_group_dim",
        "comparison_basis": "comparison_basis",
        "mapped_icb": "mapped_icb",
        "parent_scope_entity_type": "parent_scope_entity_type",
        "parent_scope_entity_name": "parent_scope_entity_name",
    }
    for target, source in mapping.items():
        value = ctx.get(source)
        if value not in (None, "", [], {}):
            frame[target] = str(value)
    return dict(frame)


def _interrupt_result_from_output(out: Dict[str, Any], request_id: str, elapsed: float) -> Optional[ChatResponse]:
    """Convert a LangGraph interrupt payload into the normal API response shape."""
    interrupts = out.get("__interrupt__") or ()
    if not interrupts:
        return None
    first = interrupts[0]
    payload = getattr(first, "value", {}) or {}
    if not isinstance(payload, dict):
        payload = {"message": str(payload)}
    meta = _build_meta(out, request_id, elapsed)
    meta["needs_clarification"] = True
    meta["clarification_question"] = str(payload.get("question", "")).strip()
    meta["langgraph_interrupt"] = True
    answer = str(payload.get("message") or "I need a bit more detail before I answer that.").strip()
    return ChatResponse(
        answer=answer,
        sql="",
        preview_markdown="",
        meta=meta,
        suggestions=list(payload.get("suggestions") or []),
    )


def _entity_context_from_cached_response(question: str, cached_resp: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct follow-up context for semantic-cache hits."""
    cached_meta = dict((cached_resp.get("meta") or {}))
    cached_semantic = dict(cached_meta.get("semantic_state", {}) or {})
    pseudo_state: StateData = {
        "question": question,
        "original_question": question,
        "sql": str(cached_resp.get("sql", "") or ""),
        "df_preview_md": str(cached_resp.get("preview_markdown", "") or ""),
        "plan": dict(cached_meta.get("plan", {}) or {}),
        "dataset": str(cached_semantic.get("dataset") or cached_meta.get("dataset") or "workforce"),
        "_rows": int(cached_meta.get("rows_returned", 0) or 0),
        "_empty": bool(int(cached_meta.get("rows_returned", 0) or 0) == 0),
        "last_error": cached_meta.get("last_error"),
    }
    return _extract_entity_context_from_state(pseudo_state)


def _post_process(question: str, out: Dict[str, Any], result: Dict[str, Any],
                   is_followup: bool) -> None:
    """Semantic cache put + LTM auto-learn. [H5] Shared by both endpoints."""
    needs_clarification = bool(out.get("_needs_clarification", False))
    rows = int(out.get("_rows", 0))

    # Store in semantic cache (only successful data responses, not clarifications)
    if not is_followup and not needs_clarification and rows > 0:
        _SEMANTIC_CACHE.put(question, result)

    # Auto-learn to long-term memory (high-confidence first-attempt successes)
    confidence = out.get("_confidence", {})
    conf_score = confidence.get("score", 0)
    plan = out.get("plan", {}) or {}
    if (not is_followup
        and not needs_clarification
        and rows > 0
        and int(out.get("attempts", 0)) == 0
        and plan.get("in_scope", False)
        and plan.get("table")
        and out.get("sql")
        and not out.get("_hard_intent")
    ):
        LONG_TERM_MEMORY.learn(
            question=question,
            table=plan["table"],
            sql=out["sql"],
            confidence=conf_score,
        )


def _run_agent_sync(req: ChatRequest) -> ChatResponse:
    """Synchronous agent invocation — uses shared helpers [H5]."""
    request_id = str(uuid.uuid4())[:8]
    logger.info("chat | rid=%s session=%s q='%s'", request_id, req.session_id, req.question[:120])
    t0 = time.time()
    thread_id = _langgraph_thread_id(req.session_id)
    config = _graph_config_for_thread(thread_id)
    pending_interrupts = _get_pending_graph_interrupts(thread_id)
    pending_clarification = MEMORY.get_pending_clarification(req.session_id)
    effective_question = req.question
    resume_interrupt = bool(pending_interrupts) and (
        pending_clarification is not None or _looks_like_clarification_answer(req.question)
    )

    if pending_interrupts and not resume_interrupt:
        logger.info(
            "chat | rid=%s ignoring stale LangGraph interrupt for session=%s question=%r",
            request_id, req.session_id, req.question[:120],
        )

    if resume_interrupt and Command is not None:
        logger.info("chat | rid=%s resuming LangGraph interrupt for session=%s", request_id, req.session_id)
        if pending_clarification:
            MEMORY.clear_pending_clarification(req.session_id)
        out = AGENT.invoke(Command(resume=req.question), config=config)
        elapsed = time.time() - t0
        interrupt_response = _interrupt_result_from_output(out, request_id, elapsed)
        if interrupt_response:
            return interrupt_response
        meta = _build_meta(out, request_id, elapsed)
        result = _build_result_dict(out, meta)
        _post_process(req.question, out, result, True)
        return ChatResponse(
            answer=result["answer"],
            sql=result["sql"],
            preview_markdown=result["preview_markdown"],
            meta=result["meta"],
            suggestions=result["suggestions"],
        )

    if pending_clarification and not pending_interrupts:
        original_q = str(pending_clarification.get("original_question") or "").strip()
        effective_question = f"{original_q} ({req.question})" if original_q else req.question
        MEMORY.clear_pending_clarification(req.session_id)
        logger.info("chat | rid=%s resuming clarification via merged question=%r", request_id, effective_question[:140])

    skip_semantic_cache = bool(pending_clarification) or _should_skip_semantic_cache(effective_question, req.session_id)

    # ── Semantic cache check ──
    if not skip_semantic_cache:
        cached_resp = _SEMANTIC_CACHE.get(req.question)
        if cached_resp:
            elapsed = time.time() - t0
            logger.info("chat | rid=%s SEMANTIC CACHE HIT in %.4fs", request_id, elapsed)
            cached_meta = dict(cached_resp.get("meta", {}))
            cached_meta["request_id"] = request_id
            cached_meta["elapsed_seconds"] = round(elapsed, 2)
            cached_meta["semantic_cache_hit"] = True
            cached_entity_context = _entity_context_from_cached_response(req.question, cached_resp)
            cached_meta["semantic_state"] = _semantic_state_from_context(cached_entity_context)
            MEMORY.add_turn(
                req.session_id,
                effective_question,
                cached_resp.get("answer", ""),
                cached_resp.get("sql", ""),
                entity_context=cached_entity_context,
            )
            return ChatResponse(
                answer=cached_resp.get("answer", ""),
                sql=cached_resp.get("sql", ""),
                preview_markdown=cached_resp.get("preview_markdown", ""),
                meta=cached_meta,
                suggestions=cached_resp.get("suggestions", []),
            )

    state: StateData = {
        "session_id": req.session_id,
        "question": effective_question,
        "attempts": 0,
    }

    out = AGENT.invoke(state, config=config)

    elapsed = time.time() - t0
    interrupt_response = _interrupt_result_from_output(out, request_id, elapsed)
    if interrupt_response:
        return interrupt_response
    logger.info("chat | rid=%s completed in %.2fs rows=%d",
                request_id, elapsed, int(out.get("_rows", 0)))

    meta = _build_meta(out, request_id, elapsed)
    result = _build_result_dict(out, meta)

    response = ChatResponse(
        answer=result["answer"],
        sql=result["sql"],
        preview_markdown=result["preview_markdown"],
        meta=result["meta"],
        suggestions=result["suggestions"],
    )

    _post_process(effective_question, out, result, skip_semantic_cache)

    return response


@app.post("/chat", response_model=ChatResponse)
@limiter.limit(_RATE_LIMIT_CHAT)
async def chat(request: Request, req: ChatRequest):
    try:
        # Run the synchronous agent in a thread pool with timeout
        result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(_AGENT_EXECUTOR, _run_agent_sync, req),
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


# =============================================================================
# Streaming SSE endpoint (/chat/stream)
# =============================================================================
# Maps LangGraph node names to user-friendly progress descriptions
_NODE_PROGRESS = {
    "init": {"step": 1, "label": "Initialising", "detail": "Preparing your query..."},
    "knowledge_check": {"step": 2, "label": "Routing", "detail": "Classifying query type and selecting optimal route..."},
    "knowledge_answer": {"step": 3, "label": "Answering", "detail": "Generating answer from domain knowledge..."},
    "latest_vocab": {"step": 3, "label": "Loading data", "detail": "Loading latest vocabulary..."},
    "hard_override": {"step": 4, "label": "Checking patterns", "detail": "Checking for known query patterns..."},
    "plan": {"step": 5, "label": "Planning", "detail": "Planning the query strategy..."},
    "clarify": {"step": 6, "label": "Clarifying", "detail": "Need more information from you..."},
    "resolve_entities": {"step": 6, "label": "Resolving entities", "detail": "Matching names to database values..."},
    "generate_sql": {"step": 7, "label": "Generating SQL", "detail": "Writing the database query..."},
    "run_sql": {"step": 8, "label": "Running query", "detail": "Executing query on Athena..."},
    "validate_or_fix": {"step": 9, "label": "Validating", "detail": "Checking query results..."},
    "summarize": {"step": 10, "label": "Summarising", "detail": "Generating natural language summary..."},
    "grade_answer": {"step": 11, "label": "Grading", "detail": "Assessing answer quality..."},
}
_TOTAL_STEPS = 11


def _stream_agent(req: ChatRequest):
    """Generator that yields SSE events — uses shared helpers [H5]."""
    request_id = str(uuid.uuid4())[:8]
    logger.info("stream | rid=%s session=%s q='%s'", request_id, req.session_id, req.question[:120])
    t0 = time.time()
    thread_id = _langgraph_thread_id(req.session_id)
    config = _graph_config_for_thread(thread_id)
    pending_interrupts = _get_pending_graph_interrupts(thread_id)
    pending_clarification = MEMORY.get_pending_clarification(req.session_id)
    effective_question = req.question
    resume_interrupt = bool(pending_interrupts) and (
        pending_clarification is not None or _looks_like_clarification_answer(req.question)
    )

    if pending_interrupts and not resume_interrupt:
        logger.info(
            "stream | rid=%s ignoring stale LangGraph interrupt for session=%s question=%r",
            request_id, req.session_id, req.question[:120],
        )

    if pending_clarification and not pending_interrupts:
        original_q = str(pending_clarification.get("original_question") or "").strip()
        effective_question = f"{original_q} ({req.question})" if original_q else req.question
        MEMORY.clear_pending_clarification(req.session_id)
        logger.info("stream | rid=%s resuming clarification via merged question=%r", request_id, effective_question[:140])

    followup = bool(pending_clarification) or _should_skip_semantic_cache(effective_question, req.session_id)

    # ── Semantic cache check ──
    if not followup:
        cached_resp = _SEMANTIC_CACHE.get(req.question)
        if cached_resp:
            elapsed = time.time() - t0
            cached_meta = dict(cached_resp.get("meta", {}))
            cached_meta["request_id"] = request_id
            cached_meta["elapsed_seconds"] = round(elapsed, 2)
            cached_meta["semantic_cache_hit"] = True
            cached_entity_context = _entity_context_from_cached_response(req.question, cached_resp)
            cached_meta["semantic_state"] = _semantic_state_from_context(cached_entity_context)
            MEMORY.add_turn(
                req.session_id,
                effective_question,
                cached_resp.get("answer", ""),
                cached_resp.get("sql", ""),
                entity_context=cached_entity_context,
            )
            result = {
                "answer": cached_resp.get("answer", ""),
                "sql": cached_resp.get("sql", ""),
                "preview_markdown": cached_resp.get("preview_markdown", ""),
                "meta": cached_meta,
                "suggestions": cached_resp.get("suggestions", []),
            }
            yield {"event": "progress", "data": json.dumps({"step": 1, "total": _TOTAL_STEPS,
                    "label": "Cache hit", "detail": "Found similar question in cache!"})}
            yield {"event": "complete", "data": json.dumps(result, default=str)}
            return

    state: StateData = {
        "session_id": req.session_id,
        "question": effective_question,
        "attempts": 0,
    }

    # Stream through graph nodes
    out = {}
    try:
        if resume_interrupt and Command is not None and pending_clarification:
            MEMORY.clear_pending_clarification(req.session_id)
        graph_input = Command(resume=req.question) if resume_interrupt and Command is not None else state
        for node_output in AGENT.stream(graph_input, stream_mode="updates", config=config):
            if "__interrupt__" in node_output:
                out["__interrupt__"] = node_output["__interrupt__"]
                payload = getattr(node_output["__interrupt__"][0], "value", {}) if node_output["__interrupt__"] else {}
                if not isinstance(payload, dict):
                    payload = {"message": str(payload)}
                yield {
                    "event": "progress",
                    "data": json.dumps({
                        "step": 6,
                        "total": _TOTAL_STEPS,
                        "label": "Clarifying",
                        "detail": "Waiting for your clarification...",
                        "elapsed": round(time.time() - t0, 1),
                        "node": "clarify",
                    }),
                }
                elapsed = time.time() - t0
                interrupt_response = _interrupt_result_from_output(out, request_id, elapsed)
                if interrupt_response:
                    yield {"event": "complete", "data": interrupt_response.model_dump_json()}
                return
            for node_name, node_state in node_output.items():
                out.update(node_state)
                progress = _NODE_PROGRESS.get(node_name, {
                    "step": 5, "label": node_name, "detail": f"Processing {node_name}..."
                })
                elapsed_so_far = round(time.time() - t0, 1)
                yield {
                    "event": "progress",
                    "data": json.dumps({
                        "step": progress["step"],
                        "total": _TOTAL_STEPS,
                        "label": progress["label"],
                        "detail": progress["detail"],
                        "elapsed": elapsed_so_far,
                        "node": node_name,
                    }),
                }
    except Exception as e:
        logger.exception("stream | error during graph execution")
        yield {"event": "error", "data": json.dumps({"error": _sanitise_error(e)})}
        return

    elapsed = time.time() - t0
    interrupt_response = _interrupt_result_from_output(out, request_id, elapsed)
    if interrupt_response:
        yield {"event": "complete", "data": interrupt_response.model_dump_json()}
        return
    logger.info("stream | rid=%s completed in %.2fs rows=%d",
                request_id, elapsed, int(out.get("_rows", 0)))

    meta = _build_meta(out, request_id, elapsed)  # [H5]
    result = _build_result_dict(out, meta)         # [H5]
    _post_process(effective_question, out, result, followup)  # [H5]

    yield {"event": "complete", "data": json.dumps(result, default=str)}


@app.post("/chat/stream")
@limiter.limit(_RATE_LIMIT_CHAT)
async def chat_stream(request: Request, req: ChatRequest):
    """Streaming chat endpoint using Server-Sent Events (SSE).

    [C2] Uses asyncio.to_thread to avoid blocking the event loop.
    [C5] Uses _sanitise_error for safe error responses.

    Events:
      - progress: { step, total, label, detail, elapsed, node }
      - complete: { answer, sql, preview_markdown, meta, suggestions }
      - error:    { error }
    """
    async def async_event_generator():
        """Bridge sync generator to async via queue + thread. [C2]"""
        import queue
        q: queue.Queue = queue.Queue()
        _SENTINEL = object()

        def _run_sync():
            try:
                for event in _stream_agent(req):
                    q.put(event)
            except Exception as e:
                logger.exception("chat_stream | unexpected error")
                q.put({"event": "error", "data": json.dumps({"error": _sanitise_error(e)})})  # [C5]
            finally:
                q.put(_SENTINEL)

        # Start the sync generator in a background thread
        loop = asyncio.get_event_loop()
        loop.run_in_executor(_AGENT_EXECUTOR, _run_sync)

        # Yield events as they arrive without blocking the event loop
        while True:
            try:
                item = await asyncio.to_thread(q.get, timeout=REQUEST_TIMEOUT)
            except Exception:
                yield {"event": "error", "data": json.dumps({"error": "Request timed out."})}
                break
            if item is _SENTINEL:
                break
            yield item

    return EventSourceResponse(async_event_generator())


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
@limiter.limit(_RATE_LIMIT_SUGGESTIONS)
def suggestions(request: Request):
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
    logger.info("Starting InsightsQI Assistant v8.0 on port 8000")
    uvicorn.run("gp_workforce_chatbot_backend_agent_v8:app", host="0.0.0.0", port=8000, reload=True)
