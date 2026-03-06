# GP Workforce Chatbot — Full Project Context

> Give this file to Claude at the start of any session to restore full project understanding.

---

## 1. What This Project Is

A **production chatbot** that answers natural-language questions about NHS England GP workforce data. It sits on top of AWS Athena (S3 + Parquet) and uses an LLM (Amazon Bedrock — Nova Pro by default) to plan and generate SQL, then formats the results into readable answers.

**Current file**: `gp_workforce_chatbot_backend_agent_v5.py` (internally versioned as v6.0-agent)
**Test file**: `test_docx_questions.py` (26 tests, all passing as of last session)
**Domain notes**: `gp_workforce_domain_notes.md`
**Working directory**: `/Users/CajaLtd/Chatbot/`

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| API framework | FastAPI + uvicorn |
| LLM | Amazon Bedrock — `amazon.nova-pro-v1:0` via `langchain-aws` |
| Agent framework | LangGraph (StateGraph) with SqliteSaver checkpointing |
| Database | AWS Athena (awswrangler) — `test-gp-workforce` database |
| Embeddings | `sentence-transformers/all-MiniLM-L6-v2` (local, HuggingFace) |
| Fuzzy matching | Python `difflib` |

**Key packages** (`requirement.txt`):
```
fastapi, uvicorn, boto3, awswrangler, pandas, pydantic, langchain, langchain-aws,
langgraph, sentence-transformers, tabulate
```

---

## 3. Data — Athena Tables

Three tables in `test-gp-workforce` database:

### `individual`
- **22 columns**, row-per-staff-member per month snapshot
- Key columns: `unique_identifier`, `staff_group`, `staff_role`, `detailed_staff_role`, `gender`, `age_band`, `fte`, `year`, `month`
- Geography: `comm_region_name`, `icb_name`, `sub_icb_name`
- **USE FOR**: national/regional/ICB totals, demographics, trends, SUM(fte), COUNT(DISTINCT unique_identifier)
- FTE = `SUM(fte)`, Headcount = `COUNT(DISTINCT unique_identifier)`

### `practice_high`
- **8 columns**, tidy format: `prac_code`, `prac_name`, `staff_group`, `detailed_staff_role`, `measure`, `value`, `year`, `month`
- `measure` is `'FTE'` or `'Headcount'`. `value` is a string — use `CAST(value AS DOUBLE)` for math
- **USE FOR**: practice-level rankings, top-N practices by FTE/headcount

### `practice_detailed`
- **830+ columns**, wide format with pre-computed totals per practice per month
- Key columns: `prac_code`, `prac_name`, `pcn_name`, `sub_icb_name`, `icb_name`, `region_name`, `total_patients`, `total_gp_fte`, `total_gp_hc`, `year`, `month`
- Pre-computed totals — **do NOT SUM these**, they are already totals per practice row
- **USE FOR**: practice lookups, patient counts, patients-per-GP ratio, detailed GP sub-type breakdowns
- Note: columns may contain `'NA'` strings — always use `NULLIF(col, 'NA')` before CAST

**Latest data**: December 2025 (year='2025', month='12')

---

## 4. Architecture — LangGraph Agent

The agent is a `StateGraph` with these nodes:

```
START → init → classifier
  → knowledge_check → knowledge_answer → END   (for methodology/scope questions)
  → plan → resolve_entities → generate_sql → run_sql → validate_or_fix
      ↑____________retry if error/empty_______________|
  → format_answer → END
```

**State type** (`AgentState` TypedDict):
- `session_id`, `question`, `original_question`, `answer`, `sql`
- `plan` (dict), `follow_up_context` (dict), `conversation_history` (str)
- `df_preview_md` (str — markdown table of query results)
- `_rows`, `_empty`, `last_error`, `attempts`, `needs_retry`
- `suggestions` (list), `time_range` (dict), `_hard_intent`, `_is_knowledge`

**Thread-ID pattern**: Each request uses a unique thread_id = `{session_id}_{uuid4[:8]}` to avoid LangGraph checkpoint contamination between requests.

---

## 5. Memory System (3 Layers)

### Layer 1: Conversation Memory (`ConversationMemory` class)
- Per-session turn history (last 6 turns = `MEMORY_MAX_TURNS`)
- Stores entity context: `entity_name`, `entity_type` (`"icb"`, `"practice"`, etc.), `entity_col`, `table`, `previous_metric`
- `MEMORY.get_entity_context(session_id)` → used for follow-up enrichment
- `MEMORY.add_turn(...)` → saves turn + entity context after each request

### Layer 2: Few-Shot Retriever (`FewShotRetriever`)
- 39 pre-loaded question→SQL examples embedded with sentence-transformers
- Vector similarity search (cosine) returns top-3 similar examples
- Fed into the SQL generation prompt as few-shot examples
- Loaded from `few_shot_examples.json`

### Layer 3: Long-Term Memory (`LongTermMemory`)
- 130 entries learned from high-confidence (≥0.85) past queries
- Auto-learns new successful queries; persists to `learned_examples.json`
- Retrieved by cosine similarity, threshold 0.5
- Fed into the SQL generation prompt

---

## 6. Key Functions & Their Roles

### Follow-up Detection & Enrichment

**`is_follow_up(question: str) -> bool`** (line ~1451)
- Returns True if question refers to a previous entity (pronoun, "this practice", etc.)
- **Self-contained pattern** (returns False for 8+ word questions unless pronoun detected):
  - Does NOT match when next word after modal is `this/that/it` (pronoun = follow-up)
  - Does NOT match when next word is `ratio/trend/count/number/etc.` (generic follow-up)
- **Strong follow-up signals**: "how has this/the...", "patients-per-GP ratio", "for this ICB", etc.
- Critical fix: `(?!(?:this|that|it)\b)` in self-contained pattern so "how has **this** changed" correctly → True

**`resolve_follow_up_context(question, session_id)`** (line ~1546)
- Enriches follow-up questions with entity context from previous turn
- Replaces "this ICB / this practice" pronouns with actual entity name
- Returns enriched question like: `"How has this changed? (context: icb = NHS Kent and Medway ICB, table = practice_detailed, metric = patients_per_gp)"`

**`_extract_entity_context_from_state(state)`** (line ~3602)
- Called after each query to extract entity for next follow-up
- Priority: SQL WHERE clause → df_preview_md first row → follow_up_context
- Stores `previous_metric = "patients_per_gp"` when query was about patients-per-GP
- Parses `df_preview_md` markdown table to get entity from GROUP BY / top-N queries

### SQL Safety

**`fix_multiperiod_or_bug(sql)`** (line ~927)
- Pre-execution fix for SQL precedence bug
- Detects: `AND (year='A' AND month='B') OR (year='C' AND month='D')`
- Fixes to: `AND ((year='A' AND month='B') OR (year='C' AND month='D'))`
- Without this, the OR bypasses all preceding WHERE filters → inflated results (192,075 trainees instead of ~9,448)

**`enforce_readonly(sql)`** — blocks non-SELECT statements
**`enforce_table_whitelist(sql)`** — only `practice_high`, `individual`, `practice_detailed` allowed
**`add_limit(sql, n)`** — adds LIMIT 200 if not present

---

## 7. LLM Prompts

### `PLANNER_SYSTEM`
- Decides which table, intent, filters, entities to resolve
- Key rules:
  - `(context: icb = <name>)` → filter by `icb_name LIKE '%<name>%'`
  - `(context: practice = <name>)` → filter by `prac_name LIKE '%<name>%'`
  - `(context: metric = patients_per_gp)` → use `SUM(total_patients)/SUM(total_gp_fte)` from `practice_detailed`
  - `(context: table = ...)` → prefer that table for follow-up
  - Use conversation history to infer metric — NEVER switch to patients-per-GP unless previous question explicitly mentioned "patients"
- Examples: national FTE → individual; practice rankings → practice_high; patient counts, patients-per-GP → practice_detailed

### `SQL_SYSTEM`
- Generates the actual SQL
- Critical rules:
  - Patients-per-GP: `ROUND(SUM(total_patients) / NULLIF(SUM(total_gp_fte), 0), 1)` — NEVER invert
  - Multi-period CASE WHEN: do NOT add OR year conditions to WHERE (use CASE WHEN instead)
  - ICB filter: `LOWER(TRIM(icb_name)) LIKE LOWER('%name%')`
  - Practice filter: `LOWER(TRIM(prac_name)) LIKE LOWER('%name%')`
  - `practice_detailed`: use `NULLIF(col, 'NA')` before any CAST
  - Trainees: `staff_role LIKE '%Training%'` (NOT `detailed_staff_role`)
  - Regions (individual): `comm_region_name LIKE '%North East%'`
  - Regions (practice_detailed): `region_name` (different column name!)

### `FIXER_SYSTEM`
- Only triggers when SQL returns error or empty results
- Fixes: name mismatches (fuzzy LIKE), wrong table, missing parentheses, inverted ratio

### `ANSWER_SYSTEM`
- Formats query results into structured markdown with key numbers bolded

---

## 8. API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | `{"ok": true, "version": "6.0-agent"}` |
| GET | `/health/detail` | Full system status |
| POST | `/chat` | Main chat endpoint |
| GET | `/stream` | SSE streaming endpoint |
| GET | `/memory` | Long-term memory contents |
| POST | `/memory/flush` | Force save LTM to disk |

**Chat request**:
```json
{"session_id": "user123", "question": "How many GPs are in Manchester?"}
```

**Chat response**:
```json
{
  "answer": "There are 1,234 GPs in Manchester...",
  "sql": "SELECT ...",
  "suggestions": ["Show age breakdown", "Compare to last year"],
  "meta": {
    "rows_returned": 1,
    "confidence": {"score": 0.92, "level": "high"},
    "session_id": "user123"
  }
}
```

**Server start**: `python gp_workforce_chatbot_backend_agent_v5.py` → runs on port 8000

---

## 9. Environment Variables

```bash
AWS_PROFILE=default
AWS_REGION=eu-west-2
ATHENA_DATABASE=test-gp-workforce
ATHENA_OUTPUT_S3=s3://test-athena-results-fingertips/
ATHENA_WORKGROUP=          # optional
BEDROCK_CHAT_MODEL_ID=amazon.nova-pro-v1:0
MAX_ROWS_RETURN=200
MAX_AGENT_LOOPS=3
MEMORY_MAX_TURNS=6
DOMAIN_NOTES_PATH=gp_workforce_domain_notes.md
COLUMN_DICT_PATH=./schemas/column_dictionary.json
CHECKPOINT_DB_PATH=.langgraph_checkpoints.db
LOG_LEVEL=INFO
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
REQUEST_TIMEOUT=90
```

---

## 10. Known Bugs Fixed (History)

| Bug | Symptom | Fix |
|---|---|---|
| LangGraph 400 error | All `/chat` calls fail | `config={"configurable": {"thread_id": ...}}` required for SqliteSaver |
| LangGraph state contamination | Previous request state bleeds in | Unique thread_id per request: `{session_id}_{uuid4[:8]}` |
| SQL OR precedence bug | `AND (year='A') OR (year='B')` returns 192,075 trainees instead of ~9,448 | `fix_multiperiod_or_bug()` auto-fixes before execution |
| Inverted patients-per-GP | Returns ~0.001 | Rule: `SUM(total_patients)/SUM(total_gp_fte)`, never inverted |
| "this ICB" literal in SQL | `LIKE '%this%'` | Replace pronouns with entity name in `resolve_follow_up_context` |
| Entity not extracted from GROUP BY queries | No entity in follow-up SQL | Parse `df_preview_md` first data row for icb_name/prac_name |
| "how has this changed" not detected as follow-up | Returns national data | Fix: `(?!(?:this|that|it)\b)` in self-contained pattern |
| ICB context annotation ignored | `(context: icb = ...)` not used | Added explicit instruction for `icb` type (not just `practice`) |
| FTE/GP follow-up switches to patients-per-GP | Wrong metric after my fix | PLANNER: "NEVER switch to patients-per-GP unless previous question explicitly mentioned patients" |

---

## 11. Test Suite (`test_docx_questions.py`)

**26 tests, all PASS** as of last session.

Sections:
1. **FTE & Headcount** (S1T1–S1T4): Proportions, percentages, ratios
2. **Demographics** (S2T5–S2T8): Age distribution, trainee pipeline, year-over-year
3. **Trends** (S3T9–S3T10): Time-series, GP change
4. **Follow-up chains** (S4T11–S4T18): 8 multi-turn entity memory tests

Key helper functions in the test file:
- `has_any(*words)` — answer contains any word
- `sql_has_icb_filter()` — SQL has `icb_name` + `LIKE`/`=`
- `sql_has_practice_filter()` — SQL has `prac_name` + `LIKE`/`=`
- `sql_balanced_parens()` — balanced `(`/`)`
- `ratio_correct(lo, hi)` — number in answer within range
- `rows_gt(n)` — rows_returned > n
- `has_rows()` — rows > 0 or SQL exists

Run: `python test_docx_questions.py`

---

## 12. Common Patterns & SQL Examples

### Patients-per-GP ratio (practice level)
```sql
SELECT prac_name, icb_name,
  ROUND(SUM(CAST(NULLIF(total_patients, 'NA') AS DOUBLE)) /
        NULLIF(SUM(CAST(NULLIF(total_gp_fte, 'NA') AS DOUBLE)), 0), 1) AS patients_per_gp
FROM practice_detailed
WHERE year = '2025' AND month = '12'
  AND LOWER(TRIM(prac_name)) LIKE LOWER('%keele%')
```

### Patients-per-GP trend for an ICB
```sql
SELECT year, month,
  ROUND(SUM(CAST(NULLIF(total_patients,'NA') AS DOUBLE)) /
        NULLIF(SUM(CAST(NULLIF(total_gp_fte,'NA') AS DOUBLE)),0), 1) AS patients_per_gp
FROM practice_detailed
WHERE LOWER(TRIM(icb_name)) LIKE LOWER('%kent and medway%')
GROUP BY year, month
ORDER BY year, month
```

### Multi-period comparison (correct CASE WHEN approach)
```sql
SELECT
  COUNT(DISTINCT CASE WHEN year='2025' AND month='12' THEN unique_identifier END) AS hc_2025,
  COUNT(DISTINCT CASE WHEN year='2022' AND month='12' THEN unique_identifier END) AS hc_2022
FROM individual
WHERE staff_group = 'GP' AND staff_role LIKE '%Training%'
  AND month = '12'  -- NO OR here — CASE WHEN handles year separation
```

### FTE per GP ratio by ICB
```sql
SELECT icb_name,
  ROUND(CAST(NULLIF(total_gp_fte,'NA') AS DOUBLE) /
        NULLIF(CAST(NULLIF(total_gp_hc,'NA') AS DOUBLE), 0), 3) AS fte_per_gp
FROM practice_detailed
WHERE year='2025' AND month='12'
  AND LOWER(TRIM(icb_name)) LIKE LOWER('%herefordshire%')
GROUP BY icb_name
ORDER BY fte_per_gp DESC
```

### Trainees (always use staff_role, NOT detailed_staff_role)
```sql
SELECT COUNT(DISTINCT unique_identifier) AS trainee_hc
FROM individual
WHERE staff_group = 'GP'
  AND staff_role LIKE '%Training%'  -- correct column
  AND year = '2025' AND month = '12'
```

---

## 13. Directory Structure

```
/Users/CajaLtd/Chatbot/
├── gp_workforce_chatbot_backend_agent_v5.py  ← MAIN FILE (v6.0)
├── gp_workforce_domain_notes.md              ← domain knowledge for LLM
├── test_docx_questions.py                    ← 26-test suite (all pass)
├── few_shot_examples.json                    ← 39 question→SQL examples
├── learned_examples.json                     ← auto-learned LTM entries
├── .langgraph_checkpoints.db                 ← LangGraph checkpoint DB
├── schemas/
│   ├── column_dictionary.json
│   ├── individual_cols.csv
│   ├── practice_detailed_cols.csv
│   └── practice_high_cols.csv
├── gp-chat-ui/                               ← React frontend
├── requirement.txt
└── PROJECT_CONTEXT.md                        ← THIS FILE
```

---

## 14. Frontend

React app in `gp-chat-ui/`. Talks to the FastAPI backend at port 8000. The backend exposes SSE streaming at `/stream` for real-time token delivery.

---

## 15. Running the Project

```bash
# Start backend
cd /Users/CajaLtd/Chatbot
python gp_workforce_chatbot_backend_agent_v5.py

# Check health
curl http://localhost:8000/health

# Run tests
python test_docx_questions.py

# Start frontend (separate terminal)
cd gp-chat-ui
npm run dev
```

---

## 16. Important Gotchas

1. **`practice_detailed` columns contain `'NA'` strings** — always `NULLIF(col, 'NA')` before `CAST`
2. **Region column name differs**: `individual` → `comm_region_name`, `practice_detailed` → `region_name`
3. **Trainees**: use `staff_role LIKE '%Training%'` not `detailed_staff_role`
4. **`practice_detailed` pre-computes totals** — don't SUM `total_gp_fte` across practices unless aggregating by ICB/region
5. **Multi-period WHERE OR**: always wrap `(year=A AND month=B) OR (year=C AND month=D)` in outer parens, or use CASE WHEN
6. **LangGraph thread_id**: must use unique per-request ID to avoid state bleed between sessions
7. **Fixer only triggers on error/empty** — wrong-but-valid SQL (e.g. correct rows but wrong numbers) won't be auto-fixed by the fixer node
8. **`is_follow_up` returns False** for 8+ word self-contained questions UNLESS: contains pronoun like "this/that/it" as 2nd word after modal, or matches pronoun_entity_refs patterns

---

*Last updated: March 2026 — 26/26 tests passing*
