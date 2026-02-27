"""
GP Workforce Chatbot Backend — Agent v5.9
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
from typing import Dict, Any, List, Tuple, Optional, TypedDict, Literal
from functools import wraps

import math
import threading
import atexit
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
app = FastAPI(title="GP Workforce Athena Chatbot (Agent v5.9)", version="5.9")

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
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "90"))


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
DOMAIN_NOTES_MAX_CHARS = int(os.getenv("DOMAIN_NOTES_MAX_CHARS", "20000"))

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


class QueryPlan(BaseModel):
    """Structured output for the query planner — replaces free-form JSON."""
    in_scope: bool = Field(
        description="Whether the question can be answered from the GP workforce dataset"
    )
    table: Literal["individual", "practice_high", "practice_detailed"] = Field(
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
    """Stores last N turns per session + last entity context for follow-ups + pending clarifications."""

    def __init__(self, max_sessions: int = 200, max_turns: int = MEMORY_MAX_TURNS):
        self._store: OrderedDict[str, List[Dict[str, str]]] = OrderedDict()
        self._entity_context: Dict[str, Dict[str, Any]] = {}
        self._pending_clarification: Dict[str, Dict[str, Any]] = {}
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

    def set_pending_clarification(self, session_id: str, original_question: str,
                                   clarification_question: str, partial_plan: Optional[Dict] = None):
        """Store a pending clarification for this session — next user message = clarification answer."""
        self._pending_clarification[session_id] = {
            "original_question": original_question,
            "clarification_question": clarification_question,
            "partial_plan": partial_plan or {},
            "timestamp": time.time(),
        }

    def get_pending_clarification(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return pending clarification state, or None. Auto-expires after 5 minutes."""
        pending = self._pending_clarification.get(session_id)
        if not pending:
            return None
        # Expire after 5 minutes of inactivity
        if time.time() - pending.get("timestamp", 0) > 300:
            self._pending_clarification.pop(session_id, None)
            return None
        return pending

    def clear_pending_clarification(self, session_id: str):
        """Clear the pending clarification after it's been consumed."""
        self._pending_clarification.pop(session_id, None)

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

    def format_for_prompt(self, session_id: str, max_recent_turns: int = 3) -> str:
        """Format conversation history for LLM prompt.
        Only includes the last `max_recent_turns` Q&A pairs to limit
        topic contamination from older unrelated conversations.
        """
        history = self.get_history(session_id)
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
# Few-Shot Example Retriever (Vector Similarity)
# =============================================================================
FEW_SHOT_PATH = os.getenv("FEW_SHOT_PATH", "few_shot_examples.json")
FEW_SHOT_TOP_K = int(os.getenv("FEW_SHOT_TOP_K", "4"))
EMBED_MODEL_ID = os.getenv("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
EMBED_DIMENSIONS = int(os.getenv("EMBED_DIMENSIONS", "256"))
SEMANTIC_CACHE_THRESHOLD = float(os.getenv("SEMANTIC_CACHE_THRESHOLD", "0.92"))


def _embed_text(text: str) -> np.ndarray:
    """Get embedding vector from Bedrock Titan Embed v2."""
    client = boto_sess.client("bedrock-runtime")
    resp = client.invoke_model(
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


# Initialise at module load
FEW_SHOT = FewShotRetriever()


# =============================================================================
# Semantic Answer Cache (embedding-based)
# =============================================================================
class SemanticCache:
    """Cache answers by semantic similarity — 'How many GPs?' ≈ 'Total GP count?'"""

    def __init__(self, max_size: int = 100, ttl: float = 300.0,
                 threshold: float = SEMANTIC_CACHE_THRESHOLD):
        self._entries: List[Dict[str, Any]] = []  # {embedding, question, response, ts}
        self._max_size = max_size
        self._ttl = ttl
        self._threshold = threshold

    def get(self, question: str) -> Optional[Dict[str, Any]]:
        """Check if a semantically similar question was recently answered."""
        if not FEW_SHOT.ready:
            return None
        self._evict_expired()
        if not self._entries:
            return None
        q_emb = _embed_text(question)
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
        self._evict_expired()
        if len(self._entries) >= self._max_size:
            self._entries.pop(0)  # remove oldest
        q_emb = _embed_text(question)
        self._entries.append({
            "embedding": q_emb,
            "question": question,
            "response": response,
            "ts": time.time(),
        })

    def _evict_expired(self):
        cutoff = time.time() - self._ttl
        self._entries = [e for e in self._entries if e["ts"] > cutoff]

    @property
    def size(self) -> int:
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

            # Evict oldest if over capacity (keep highest confidence)
            if len(self._entries) > self._max_entries:
                # Sort by confidence (desc), then by learned_at (newest first)
                scored = sorted(range(len(self._entries)),
                                key=lambda i: (self._entries[i].get("confidence", 0),
                                               self._entries[i].get("use_count", 0)),
                                reverse=True)
                keep_indices = set(scored[:self._max_entries])
                self._entries = [self._entries[i] for i in range(len(self._entries)) if i in keep_indices]
                self._embeddings = [self._embeddings[i] for i in range(len(self._embeddings)) if i in keep_indices]

            self._dirty = True
            logger.info("LTM learned: '%s' (confidence=%.2f, total=%d)",
                        question[:60], confidence, len(self._entries))

            # Auto-save every 5 new entries
            if len(self._entries) % 5 == 0:
                self._save()

            return True

    def retrieve(self, question: str, top_k: int = 2) -> List[Dict[str, Any]]:
        """Retrieve top-K similar learned examples for the given question."""
        if not self._embeddings or not self._entries:
            return []
        try:
            q_emb = _embed_text(question)
            scored = []
            for i, ex_emb in enumerate(self._embeddings):
                sim = _cosine_similarity(q_emb, ex_emb)
                scored.append((sim, i))
            scored.sort(reverse=True, key=lambda x: x[0])

            results = []
            for sim, idx in scored[:top_k]:
                if sim < 0.5:  # minimum relevance threshold
                    break
                ex = self._entries[idx].copy()
                ex["similarity"] = round(sim, 4)
                # Increment use count
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


def retrieve_domain_notes(question: str, max_chars: int = DOMAIN_NOTES_MAX_CHARS,
                          max_chunks: int = 8) -> str:
    """Retrieve the most relevant domain-notes sections for a question.
    For knowledge-only questions, call with higher max_chunks/max_chars for richer context.
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
    top = "\n\n".join([c for _, c in scored[:max_chunks]])
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


def _extract_topic_keywords(question: str) -> set:
    """Extract meaningful topic keywords from a question for topic-change detection."""
    q = question.lower().strip()
    # Remove common stop words and question words
    stop_words = {
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
        r"^how\s+(?:does|do|did|has|have|will|would|can|could)\s+\w{3,}",
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
        r"\b(?:at|in|for|of)\s+(?:this|that)\s+(?:practice|surgery|icb|region|area|pcn)\b",
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
        r"\b(the same|this practice|that practice|this icb|that icb|same one|them)\b",
        r"\b(?:for|of)\s+(this|that|it|them)\b",
        r"^(and |also |now )",
        r"^(show me|can you show|give me)\s+(?:the\s+)?(?:same|that|this|it|a comparison|side by side)",
        r"\bside\s+by\s+side\b",
        r"^(patients per|ratio|trend|gender|age|breakdown|demographic)\b",
        r"\b(its|their)\s+\w",
        # Correction / refinement: user is changing the previous request
        r"\b(i\s+)?don'?t\s+want\b.*\bi\s+want\b",
        r"^(not\s+that|no\s*,?\s*(i\s+)?(want|need|mean))",
        r"^(instead|rather)\b",
        r"^(actually|but)\s+(i\s+)?(want|need|show|give)",
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
                          "GP", "GPs", "FTE", "ICB", "NHS", "PCN", "DPC"}
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

    # "What is the ratio/total/trend?" — short generic follow-up
    if re.search(r"^what\s+is\s+the\s+(ratio|total|trend|breakdown|split|difference)\??$", q):
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
    if not prev_ctx:
        return question, None

    # ---- Topic change detection ----
    # If user switched topics entirely, clear entity context and treat as fresh question
    if _is_topic_change(question, session_id):
        logger.info("resolve_follow_up_context | topic change detected, clearing entity context")
        MEMORY.save_entity_context(session_id, {})
        return question, None

    if not is_follow_up(question):
        return question, None

    # We have previous context and this is a follow-up on the same topic
    entity_name = prev_ctx.get("entity_name", "")
    entity_type = prev_ctx.get("entity_type", "")  # "practice", "icb", etc.
    table = prev_ctx.get("table", "")

    if entity_name:
        enriched = f"{question} (context: {entity_type} = {entity_name}, table = {table})"
        return enriched, prev_ctx

    # National-level follow-up: no specific entity, but still carry table/scope context
    # This handles corrections like "i dont want FTE, i want headcount" after a national query
    if table:
        enriched = f"{question} (context: table = {table}, scope = national)"
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
    _is_knowledge: bool  # True = answer from domain notes only, skip SQL
    _query_route: str  # adaptive routing: "knowledge" | "data_simple" | "data_complex" | "out_of_scope"
    _few_shot_best_sim: float  # best similarity score from few-shot retrieval
    _confidence: Dict[str, Any]  # confidence grading result
    _needs_clarification: bool  # True = query is ambiguous, ask user for more info
    _clarification_question: str  # the clarification question to present to the user
    _clarification_resolved: bool  # True = this query was enriched from a clarification answer


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
  Geography: comm_region_name, icb_name, sub_icb_name.
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
- "average nurses per practice" -> practice_detailed, intent="total", AVG(total_nurses_hc)
- "GP age distribution" -> individual, intent="demographics", GROUP BY age_band

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
- String comparisons should use LOWER(TRIM(...)) for robustness.

FOLLOW-UP CONTEXT:
- If the question contains "(context: practice = <name>)" or similar, you MUST include
  a WHERE filter for that entity (e.g. LOWER(TRIM(prac_name)) LIKE LOWER('%name%')).
- NEVER produce a query without the entity filter when context is provided.

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
  Do NOT SUM these — they are already totals per practice row. Always filter by year + month.

COLUMN LABELS are provided to help you pick the right column name. Use the column NAME (not the label) in SQL.

TIME RANGES: If a time range is provided, use it in WHERE clause:
  (CAST(year AS INTEGER) * 100 + CAST(month AS INTEGER)) BETWEEN start AND end

MULTI-PERIOD COMPARISONS (CRITICAL):
When comparing two time periods (e.g. "this year vs 3 years ago", "2025 vs 2022", "compare", "side by side"):
- NEVER mix rows from different years in the same AVG/SUM/COUNT without separating them.
- Use conditional aggregation with CASE WHEN to produce separate columns per period:
    AVG(CASE WHEN year = '2025' AND month = '12' THEN CAST(NULLIF(col, 'NA') AS DOUBLE) END) AS col_2025,
    AVG(CASE WHEN year = '2022' AND month = '12' THEN CAST(NULLIF(col, 'NA') AS DOUBLE) END) AS col_2022
- OR use two CTEs / subqueries — one per period — joined on the grouping key.
- Always include a difference or change column:  (val_2025 - val_2022) AS change
- If the original question asked for a national average, keep it national (no GROUP BY practice).
- If the user says "side by side" or "compare" for a follow-up, maintain the SAME granularity
  as the previous query (national stays national, practice-level stays practice-level)
  unless the user explicitly asks to drill down.

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

11. FTE PER GP RATIO (practice-level):
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
- Invalid staff_group: use values from the vocabulary provided
- Wrong table: if practice lookup returns 0 rows from individual, use practice_detailed
- practice_high: remember value is string, use CAST(value AS DOUBLE) for math
- Column not found: check the schema provided and use correct column name
- Multi-period comparison returns identical values for both periods:
  This means the two periods were MIXED in the same aggregate (AVG/SUM).
  FIX: use CASE WHEN year = 'YYYY' ... END inside the aggregate to separate periods,
  OR use two CTEs joined on the grouping key.
- practice_detailed columns may contain 'NA': always use NULLIF(col, 'NA') before CAST.
- If user asked for "more than X%" or "at least X%" but the query has no HAVING or WHERE to filter:
  Add a HAVING clause or wrap in a subquery with WHERE to enforce the percentage threshold.
- Division by zero: wrap divisors in NULLIF(..., 0).
- FTE per GP ratio returning 0 rows: ensure WHERE filters out NULL and 'NA' headcount values:
  WHERE total_gp_hc IS NOT NULL AND total_gp_hc != 'NA' AND CAST(NULLIF(total_gp_hc, 'NA') AS DOUBLE) > 0
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
- Use bullet points for clarity. Bold key terms.
- Keep answers concise: 2-8 lines.
- If the question asks for actual numbers/statistics from the database, say:
  "This question requires querying the database. Please rephrase to ask for specific data,
  e.g. 'Show total GP FTE nationally in the latest month'."
- NEVER invent statistics, dates, or figures not in the domain notes.
- End with a brief suggestion of what the user could ask next.
"""

SUMMARY_SYSTEM = """You are a helpful NHS GP Workforce analyst providing clear, well-formatted answers.

FORMATTING RULES:
- Lead with the key finding in bold: e.g. "**Total GP FTE is 27,453.2** across England as of August 2024."
- Use bullet points for multiple data points.
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
    state["_needs_clarification"] = False
    state["_clarification_question"] = ""
    state["_clarification_resolved"] = False

    # Store original question before enrichment
    state["original_question"] = state["question"]

    sid = state.get("session_id", "")
    logger.info("node_init | session=%s | q='%s'", sid, state["question"][:120])

    # ── Multi-turn clarification: check if previous turn asked a clarification question ──
    pending = MEMORY.get_pending_clarification(sid)
    if pending:
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
            logger.info("node_init | follow-up detected, entity=%s", follow_ctx.get("entity_name"))
            state["question"] = enriched_q
    else:
        state["follow_up_context"] = None

    state["domain_notes"] = retrieve_domain_notes(state["question"])
    state["_hard_intent"] = detect_hard_intent(state["original_question"])
    if state["_hard_intent"]:
        logger.info("node_init | hard_intent=%s", state["_hard_intent"])
    state["_is_knowledge"] = False  # default; knowledge_check may override
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
]
_KNOWLEDGE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _KNOWLEDGE_KEYWORDS]

# These patterns mean the user DEFINITELY wants data — never classify as knowledge.
# IMPORTANT: Keep these tight to avoid blocking legitimate knowledge questions.
_DATA_SIGNALS = [
    r"\b(?:total|sum|count|number\s+of|how\s+many)\s+(?:gp|nurse|dpc|admin|staff)\b",
    r"\btop\s+\d+\b",
    r"\btrend\b",
    r"\brank\b",
    r"\bcompare\s+(?:\w+\s+){0,3}(?:fte|headcount|practice|icb)\b",
    r"\b(?:show|give|list|display|get)\s+(?:me\s+)?(?:the\s+)?(?:total|all|every|data|numbers?|figures?|stats?)\b",
    r"\b(?:latest|current|last)\s+(?:month|year|quarter)\b.*\b(?:fte|headcount|gp|nurse|staff|dpc|admin)\b",
    # "FTE for/at/by X" (data request), but NOT "FTE in this publication" (knowledge)
    r"\b(?:fte|headcount)\s+(?:for|at|by)\s+(?!this\b)",
    r"\b(?:fte|headcount)\s+in\s+(?!this\b)(?!the\s+(?:publication|series|dataset|data)\b)",
    r"\b(?:breakdown|split|distribution)\s+(?:of|by|for)\b",
    r"\bpractice\s+(?:called|named|like)\b",
    # "at/for [a specific] practice" (data request), but NOT "in the General Practice Workforce" (knowledge)
    r"\b(?:at|for)\s+(?:\w+\s+){0,2}(?:practice|surgery|medical\s+centre|health\s+centre)\b",
    r"\bin\s+(?!the\s+general\s+practice\s+workforce)(?:\w+\s+){0,2}(?:surgery|medical\s+centre|health\s+centre)\b",
]
_DATA_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _DATA_SIGNALS]


def _is_knowledge_only_question(question: str) -> bool:
    """
    Conservative check: returns True ONLY if the question matches knowledge patterns
    AND does NOT match any data-request signals. When in doubt, returns False (→ SQL path).
    """
    q = question.strip()

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


# =============================================================================
# Adaptive Query Routing — LLM-based classifier
# =============================================================================
# Complexity signals for simple vs complex data queries
_SIMPLE_QUERY_SIGNALS = [
    r"^\s*(?:total|how\s+many|number\s+of|count)\s+(?:gp|nurse|dpc|admin|staff|trainee|pharmacist|practice)",
    r"^\s*(?:show|get|give|list|what\s+is)\s+(?:me\s+)?(?:the\s+)?(?:total|national|overall|latest)\b",
    r"^\s*gp\s+(?:fte|headcount)\s+(?:nationally|in\s+(?:the\s+)?latest)\b",
    r"^\s*(?:total|how\s+many)\s+(?:gp\s+)?practices?\b",
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
Given a user question, classify it into EXACTLY one category:

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
                     "pharmacist", "admin", "patient", "region", "sub-icb", "sub icb"}
    if not any(dw in q_lower for dw in _DOMAIN_WORDS) and len(q.split()) >= 3:
        # No domain keywords and not super short — likely OOS but let LLM confirm
        pass

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


def node_knowledge_check(state: AgentState) -> AgentState:
    """
    Adaptive routing node: classifies the query and sets _query_route + _is_knowledge.
    Routes:
      - knowledge     → knowledge_answer node (skip SQL entirely)
      - data_simple   → streamlined SQL path (skip planner + entity resolution)
      - data_complex  → full planning pipeline
      - out_of_scope  → handled by planner marking in_scope=False → summarize
    """
    q = state.get("original_question", state["question"])

    # Classify using adaptive routing
    is_followup = bool(state.get("follow_up_context"))
    route = _classify_query_route(q, hard_intent=state.get("_hard_intent"), is_followup=is_followup)

    state["_query_route"] = route
    state["_is_knowledge"] = (route == "knowledge")

    logger.info("node_knowledge_check | route=%s for '%s'", route, q[:120])
    return state


def node_knowledge_answer(state: AgentState) -> AgentState:
    """
    Answer a knowledge/methodology question directly from domain notes.
    No SQL is executed. The LLM uses the domain notes as its knowledge base.
    """
    logger.info("node_knowledge_answer | answering from domain notes")
    llm = llm_client()

    q = state.get("original_question", state["question"])
    # For knowledge questions, retrieve more context than default
    domain_notes = retrieve_domain_notes(q, max_chars=DOMAIN_NOTES_MAX_CHARS, max_chunks=12)
    conversation_history = state.get("conversation_history", "")

    prompt = f"""
CONVERSATION HISTORY:
{conversation_history or "(first question)"}

DOMAIN NOTES (your knowledge base — answer ONLY from this):
{domain_notes}

USER QUESTION:
{q}

Provide a clear, well-formatted answer using ONLY the domain notes above.
""".strip()

    ans = llm.invoke([
        SystemMessage(content=KNOWLEDGE_SYSTEM),
        HumanMessage(content=prompt),
    ]).content.strip()

    state["answer"] = ans
    state["sql"] = ""
    state["df_preview_md"] = ""
    state["plan"] = {"in_scope": True, "table": None, "intent": "knowledge", "notes": "answered from domain notes"}
    state["_rows"] = 0
    state["_empty"] = False
    state["suggestions"] = _knowledge_suggestions(q)

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

    # If extract_entity_hint returned the whole question, a pronoun, or junk,
    # check follow-up context for the entity
    _PRONOUNS = {"this", "that", "it", "them", "they", "these", "those", "the same", "same"}
    orig_q = state.get("original_question", "")
    if hint == orig_q or len(hint) > 50 or hint.lower().strip() in _PRONOUNS:
        follow_ctx = state.get("follow_up_context")
        if follow_ctx and follow_ctx.get("entity_name"):
            hint = follow_ctx["entity_name"]
        else:
            # No entity at all — for practice-specific intents, skip hard override
            # and let the LLM planner handle it (it might be a national question)
            if hi in ("practice_gp_count", "practice_gp_count_soft", "practice_patient_count",
                       "practice_staff_breakdown", "practice_to_icb_lookup",
                       "patients_per_gp"):
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

    # Use structured output for guaranteed valid plan schema
    try:
        structured_llm = llm.with_structured_output(QueryPlan)
        plan_obj = structured_llm.invoke([
            SystemMessage(content=PLANNER_SYSTEM),
            HumanMessage(content=prompt),
        ])
        plan = plan_obj.model_dump()
        logger.debug("node_plan | structured output parsed successfully")
    except Exception as e:
        logger.warning("node_plan | structured output failed (%s), trying free-form JSON", str(e)[:120])
        # Fallback: try free-form JSON from unstructured LLM
        try:
            raw = llm.invoke([
                SystemMessage(content=PLANNER_SYSTEM),
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

    plan = validate_plan(plan, state["question"])
    plan["in_scope"] = bool(plan.get("in_scope", True))

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

    # ── data_simple fast-path: auto-generate lightweight plan if planner was skipped ──
    if not plan and state.get("_query_route") == "data_simple":
        q_lower = state["question"].lower()
        # Infer table from question keywords
        if any(kw in q_lower for kw in ["practice", "pcn", "surgery", "patients per"]):
            auto_table = "practice_detailed"
        elif any(kw in q_lower for kw in ["measure", "staff_group", "practice_high"]):
            auto_table = "practice_high"
        else:
            auto_table = "individual"
        plan = {
            "in_scope": True,
            "table": auto_table,
            "intent": "simple_aggregate",
            "group_by": [],
            "filters_needed": [],
            "entities_to_resolve": [],
            "notes": "Auto-generated plan for data_simple route (planner skipped)",
        }
        state["plan"] = plan
        logger.info("node_generate_sql | data_simple auto-plan: table=%s", auto_table)

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
    if FEW_SHOT.ready:
        examples = FEW_SHOT.retrieve(state["question"], top_k=FEW_SHOT_TOP_K)
        # Also retrieve from long-term learned memory
        ltm_examples = LONG_TERM_MEMORY.retrieve(state["question"], top_k=2)
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

    # ── Retrieve similar few-shot examples for fixer ──
    fixer_few_shot = ""
    if FEW_SHOT.ready:
        fix_examples = FEW_SHOT.retrieve(state["question"], top_k=3)
        if fix_examples:
            parts = []
            for i, ex in enumerate(fix_examples, 1):
                parts.append(f"Example {i}: Q: {ex['question']}\n  SQL: {ex['sql']}")
            fixer_few_shot = "\n\n".join(parts)

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
        plan_notes = plan.get("notes", "")
        domain_notes = state.get("domain_notes", "")
        msg = f"""
QUESTION:
{state["question"]}

PLANNER NOTES:
{plan_notes}

DOMAIN NOTES (for context on what IS available):
{domain_notes[:4000]}

This question is OUT OF SCOPE for the GP Workforce dataset.
Explain clearly and helpfully:
1. Why this specific question cannot be answered from the GP Workforce data
2. What related data IS available (e.g. if they ask about wait times, mention we have patients-per-GP ratio as a proxy)
3. If applicable, mention the correct NHS dataset that WOULD have this data
4. Suggest 2-3 related questions the user CAN ask with this chatbot
Keep it concise (4-8 lines). Be helpful, not just rejecting.
""".strip()
        ans = llm.invoke([SystemMessage(content=SUMMARY_SYSTEM), HumanMessage(content=msg)]).content.strip()
        state["answer"] = ans
        state["df_preview_md"] = ""
        state["sql"] = ""
        # Generate contextual suggestions based on the question
        q_lower = state["question"].lower()
        if any(w in q_lower for w in ["wait", "appointment", "seen", "time"]):
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
# Answer Grading & Confidence Scoring
# =============================================================================
def _compute_confidence(state: AgentState) -> Dict[str, Any]:
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

    # Entity fuzzy matching = slight uncertainty
    resolved = state.get("resolved_entities", {})
    if resolved:
        for key, vals in resolved.items():
            if isinstance(vals, list) and vals:
                match_score = vals[0].get("score", 1.0) if isinstance(vals[0], dict) else 1.0
                if match_score < 0.85:
                    score -= 0.1
                    signals.append(f"fuzzy_entity_match ({key})")

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


def node_grade_answer(state: AgentState) -> AgentState:
    """
    Post-summarise node: computes confidence score and optionally
    adds a confidence note to the answer.
    """
    confidence = _compute_confidence(state)
    state["_confidence"] = confidence

    # Add visual confidence indicator to answer
    answer = state.get("answer", "")
    level = confidence["level"]

    if level == "high":
        badge = "🟢"
    elif level == "medium":
        badge = "🟡"
    else:
        badge = "🔴"

    # Only add low/medium confidence note — high confidence needs no disclaimer
    if level == "low" and answer:
        state["answer"] = answer + f"\n\n{badge} *Confidence: Low — results may be incomplete or approximate. Try rephrasing for better accuracy.*"
    elif level == "medium" and answer:
        state["answer"] = answer + f"\n\n{badge} *Confidence: Medium — results should be broadly correct but may have minor gaps.*"

    logger.info("Answer grade: %s (score=%.2f, signals=%s)",
                level, confidence["score"], confidence["signals"])

    return state


# =============================================================================
# Multi-Turn Clarification Node
# =============================================================================
def node_clarify(state: AgentState) -> AgentState:
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

    # Save pending clarification so next turn can resolve it
    MEMORY.set_pending_clarification(
        sid,
        original_question=original_q,
        clarification_question=clarification_q,
        partial_plan=plan,
    )

    # Format the clarification answer
    state["answer"] = f"I'd like to make sure I give you the right data. {clarification_q}"
    state["sql"] = ""
    state["df_preview_md"] = ""
    state["_rows"] = 0
    state["_empty"] = False

    # Generate helpful suggestions based on the clarification context
    intent = plan.get("intent", "")
    table = plan.get("table", "individual")
    suggestions = []
    if "region" in original_q.lower() or "icb" in original_q.lower():
        suggestions = [
            "Show by region",
            "Show by ICB",
            "Show national total",
        ]
    elif "trend" in original_q.lower() or "over time" in original_q.lower():
        suggestions = [
            "Last 12 months",
            "Last 3 years",
            "Year over year comparison",
        ]
    elif "practice" in original_q.lower():
        suggestions = [
            "All practices nationally",
            "Top 10 practices by GP FTE",
            "A specific practice (name it)",
        ]
    else:
        suggestions = [
            "GP FTE nationally",
            "GP headcount by region",
            "Staff breakdown by role",
        ]
    state["suggestions"] = suggestions

    # Save this turn to conversation memory so history shows the clarification exchange
    MEMORY.add_turn(sid, original_q, state["answer"], sql="")

    logger.info("node_clarify | asked: '%s' | suggestions=%s", clarification_q[:120], suggestions)
    return state


# =============================================================================
# Build Graph (Adaptive Routing + Multi-Turn Clarification)
# =============================================================================
def build_graph():
    g = StateGraph(AgentState)

    g.add_node("init", node_init)
    g.add_node("knowledge_check", node_knowledge_check)
    g.add_node("knowledge_answer", node_knowledge_answer)
    g.add_node("latest_vocab", node_fetch_latest_and_vocab)
    g.add_node("hard_override", node_hard_override_sql)
    g.add_node("plan", node_plan)
    g.add_node("clarify", node_clarify)
    g.add_node("resolve_entities", node_resolve_entities)
    g.add_node("generate_sql", node_generate_sql)
    g.add_node("run_sql", node_run_sql)
    g.add_node("validate_or_fix", node_validate_or_fix)
    g.add_node("summarize", node_summarize)
    g.add_node("grade_answer", node_grade_answer)

    g.set_entry_point("init")

    # init → knowledge_check (always — performs adaptive classification)
    g.add_edge("init", "knowledge_check")

    # knowledge_check → route based on adaptive classification
    def route_after_knowledge_check(state: AgentState) -> str:
        return "knowledge_answer" if state.get("_is_knowledge", False) else "latest_vocab"

    g.add_conditional_edges("knowledge_check", route_after_knowledge_check, {
        "knowledge_answer": "knowledge_answer",
        "latest_vocab": "latest_vocab",
    })

    # knowledge_answer → grade_answer → END
    g.add_edge("knowledge_answer", "grade_answer")

    # SQL pipeline — latest_vocab → hard_override (always)
    g.add_edge("latest_vocab", "hard_override")

    # ── ADAPTIVE ROUTING after hard_override ──
    # data_simple: skip planner & entity resolution → generate_sql directly
    # data_complex / out_of_scope / fallback: full pipeline → plan → resolve → generate_sql
    def route_after_hard_override(state: AgentState) -> str:
        # If hard_override already set SQL, always go to plan (it will skip itself)
        if state.get("sql"):
            return "plan"
        route = state.get("_query_route", "data_complex")
        if route == "data_simple":
            return "generate_sql"
        return "plan"

    g.add_conditional_edges("hard_override", route_after_hard_override, {
        "plan": "plan",
        "generate_sql": "generate_sql",
    })

    # ── MULTI-TURN CLARIFICATION after plan ──
    # If planner flagged ambiguity → clarify node (short-circuits pipeline)
    # Otherwise → continue to resolve_entities as normal
    def route_after_plan(state: AgentState) -> str:
        if state.get("_needs_clarification", False):
            return "clarify"
        return "resolve_entities"

    g.add_conditional_edges("plan", route_after_plan, {
        "clarify": "clarify",
        "resolve_entities": "resolve_entities",
    })

    # clarify → END (return clarification question to user; pipeline resumes next turn)
    g.add_edge("clarify", END)

    # Full pipeline continues: resolve_entities → generate_sql
    g.add_edge("resolve_entities", "generate_sql")

    # generate_sql → run_sql → validate_or_fix
    g.add_edge("generate_sql", "run_sql")
    g.add_edge("run_sql", "validate_or_fix")

    # validate_or_fix → retry or summarize
    def route_after_validate(state: AgentState) -> str:
        return "run_sql" if state.get("needs_retry", False) else "summarize"

    g.add_conditional_edges("validate_or_fix", route_after_validate, {
        "run_sql": "run_sql",
        "summarize": "summarize",
    })

    g.add_edge("summarize", "grade_answer")
    g.add_edge("grade_answer", END)
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
        "version": "5.9-agent",
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
        "few_shot_examples": FEW_SHOT.count,
        "few_shot_ready": FEW_SHOT.ready,
        "semantic_cache_size": _SEMANTIC_CACHE.size,
        "long_term_memory": LONG_TERM_MEMORY.stats,
        "version": "5.9-agent",
    }


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


def _run_agent_sync(req: ChatRequest) -> ChatResponse:
    """Synchronous agent invocation — wrapped by async endpoint."""
    request_id = str(uuid.uuid4())[:8]
    logger.info("chat | rid=%s session=%s q='%s'", request_id, req.session_id, req.question[:120])
    t0 = time.time()

    # ── Semantic cache check ──
    # Only for non-follow-up, standalone questions (follow-ups depend on session context)
    q_lower = req.question.lower().strip()
    is_likely_followup = any(w in q_lower for w in ["this practice", "that icb", "same ", "the above"])
    if not is_likely_followup:
        cached_resp = _SEMANTIC_CACHE.get(req.question)
        if cached_resp:
            elapsed = time.time() - t0
            logger.info("chat | rid=%s SEMANTIC CACHE HIT in %.4fs", request_id, elapsed)
            # Return cached response with updated meta
            cached_meta = cached_resp.get("meta", {})
            cached_meta["request_id"] = request_id
            cached_meta["elapsed_seconds"] = round(elapsed, 2)
            cached_meta["semantic_cache_hit"] = True
            return ChatResponse(
                answer=cached_resp.get("answer", ""),
                sql=cached_resp.get("sql", ""),
                preview_markdown=cached_resp.get("preview_markdown", ""),
                meta=cached_meta,
                suggestions=cached_resp.get("suggestions", []),
            )

    state: AgentState = {
        "session_id": req.session_id,
        "question": req.question,
        "attempts": 0,
    }

    out = AGENT.invoke(state)

    elapsed = time.time() - t0
    logger.info("chat | rid=%s completed in %.2fs rows=%d",
                request_id, elapsed, int(out.get("_rows", 0)))

    needs_clarification = bool(out.get("_needs_clarification", False))

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
        "query_route": out.get("_query_route", "unknown"),
        "request_id": request_id,
        "elapsed_seconds": round(elapsed, 2),
        "semantic_cache_hit": False,
        "confidence": out.get("_confidence", {}),
        "needs_clarification": needs_clarification,
        "clarification_resolved": bool(out.get("_clarification_resolved", False)),
    }

    response = ChatResponse(
        answer=out.get("answer", ""),
        sql=out.get("sql", ""),
        preview_markdown=out.get("df_preview_md", ""),
        meta=meta,
        suggestions=out.get("suggestions", []),
    )

    # ── Store in semantic cache (only successful data responses, not clarifications) ──
    if not is_likely_followup and not needs_clarification and int(out.get("_rows", 0)) > 0:
        _SEMANTIC_CACHE.put(req.question, {
            "answer": response.answer,
            "sql": response.sql,
            "preview_markdown": response.preview_markdown,
            "meta": meta,
            "suggestions": response.suggestions,
        })

    # ── Auto-learn to long-term memory (high-confidence first-attempt successes) ──
    confidence = out.get("_confidence", {})
    conf_score = confidence.get("score", 0)
    plan = out.get("plan", {}) or {}
    if (not is_likely_followup
        and not needs_clarification
        and int(out.get("_rows", 0)) > 0
        and int(out.get("attempts", 0)) == 0
        and plan.get("in_scope", False)
        and plan.get("table")
        and out.get("sql")
        and not out.get("_hard_intent")  # skip template queries
    ):
        LONG_TERM_MEMORY.learn(
            question=req.question,
            table=plan["table"],
            sql=out["sql"],
            confidence=conf_score,
        )

    return response


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
    """Generator that yields SSE events as the agent processes a query."""
    request_id = str(uuid.uuid4())[:8]
    logger.info("stream | rid=%s session=%s q='%s'", request_id, req.session_id, req.question[:120])
    t0 = time.time()

    # ── Semantic cache check ──
    q_lower = req.question.lower().strip()
    is_likely_followup = any(w in q_lower for w in ["this practice", "that icb", "same ", "the above"])
    if not is_likely_followup:
        cached_resp = _SEMANTIC_CACHE.get(req.question)
        if cached_resp:
            elapsed = time.time() - t0
            cached_meta = cached_resp.get("meta", {})
            cached_meta["request_id"] = request_id
            cached_meta["elapsed_seconds"] = round(elapsed, 2)
            cached_meta["semantic_cache_hit"] = True
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

    state: AgentState = {
        "session_id": req.session_id,
        "question": req.question,
        "attempts": 0,
    }

    # Stream through graph nodes
    out = {}
    try:
        for node_output in AGENT.stream(state, stream_mode="updates"):
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
    needs_clarification = bool(out.get("_needs_clarification", False))
    logger.info("stream | rid=%s completed in %.2fs rows=%d clarification=%s",
                request_id, elapsed, int(out.get("_rows", 0)), needs_clarification)

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
        "query_route": out.get("_query_route", "unknown"),
        "request_id": request_id,
        "elapsed_seconds": round(elapsed, 2),
        "semantic_cache_hit": False,
        "confidence": out.get("_confidence", {}),
        "needs_clarification": needs_clarification,
        "clarification_resolved": bool(out.get("_clarification_resolved", False)),
    }

    result = {
        "answer": out.get("answer", ""),
        "sql": out.get("sql", ""),
        "preview_markdown": out.get("df_preview_md", ""),
        "meta": meta,
        "suggestions": out.get("suggestions", []),
    }

    # Store in semantic cache (skip clarification responses)
    if not is_likely_followup and not needs_clarification and int(out.get("_rows", 0)) > 0:
        _SEMANTIC_CACHE.put(req.question, result)

    # Auto-learn to long-term memory (skip clarification responses)
    confidence = out.get("_confidence", {})
    conf_score = confidence.get("score", 0)
    plan = out.get("plan", {}) or {}
    if (not is_likely_followup
        and not needs_clarification
        and int(out.get("_rows", 0)) > 0
        and int(out.get("attempts", 0)) == 0
        and plan.get("in_scope", False)
        and plan.get("table")
        and out.get("sql")
        and not out.get("_hard_intent")
    ):
        LONG_TERM_MEMORY.learn(
            question=req.question,
            table=plan["table"],
            sql=out["sql"],
            confidence=conf_score,
        )

    yield {"event": "complete", "data": json.dumps(result, default=str)}


@app.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    """Streaming chat endpoint using Server-Sent Events (SSE).

    Emits progress events as the agent processes through each node,
    followed by a 'complete' event with the full ChatResponse payload.

    Events:
      - progress: { step, total, label, detail, elapsed, node }
      - complete: { answer, sql, preview_markdown, meta, suggestions }
      - error:    { error }
    """
    def event_generator():
        try:
            yield from _stream_agent(req)
        except Exception as e:
            logger.exception("chat_stream | unexpected error")
            yield {"event": "error", "data": json.dumps({"error": str(e)[:200]})}

    return EventSourceResponse(event_generator())


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
    logger.info("Starting GP Workforce Chatbot v5.9 on port 8000")
    uvicorn.run("gp_workforce_chatbot_backend_agent_v5:app", host="0.0.0.0", port=8000, reload=True)
