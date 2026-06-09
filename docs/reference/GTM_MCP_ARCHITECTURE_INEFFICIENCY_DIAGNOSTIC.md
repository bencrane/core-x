# gtm-mcp Architecture & Inefficiency Diagnostic

> Read-only audit of `apps/gtm_mcp` (14 files, ~4,281 LoC, 8 `register()` modules, ~40 tools).
> Method: 7 files read first-hand + a 13-agent adversarial workflow (9 deep-read finders → 4 skeptic
> verifiers, all four returned **refuted: False**) + cross-reference against the two governing prior
> diagnostics: `GTM_MCP_RECALL_ARCHITECTURE_DIAGNOSTIC.md` and `GOVCON_90DAY_TRIGGER_DIAGNOSTIC.md`.
> No code was modified. Every claim carries a `file:line` anchor.

---

## 0. Verdict summary (read this first)

| Directive question | Verdict | One-line basis |
|---|---|---|
| Re-opening Lance/boto3/S3/DuckDB connections from scratch on every tool call? | **NO** | Four process-lifetime singletons/caches; every tool routes through them. |
| Re-embedding the same semantic queries (LRU gap)? | **NO — N/A** | There is **zero** embedding code in the server; nothing to cache or re-compute. |
| Blindly re-fetching the same `notice_id`/enrichment payload per session? | **NO** | Award data is warm-cache + BTREE from Lance; enrichment runs in the downstream Modal worker, not the server. |
| Legacy/corrupted-cache paths needing a repoint to the 90-day mirror? | **NONE** | Registry is dynamic discovery over `active/*`; zero `sam_attachment`/`govcon_scope`/`_90day` references exist in the app. |
| Is the server aligned with the 90-day reconciled mirror? | **PARTIALLY — one structural gap** | The mirrors auto-register by name, but **no consumer/vector-search tool exists** to use `govcon_scope_vectors_90day` for its intended ANN retrieval. |

**Bottom line.** The "stateless teardown" hypothesis is false — and not by accident. This exact concern
(per-call re-open of Lance index state) was diagnosed by a prior 8-agent workflow and **remediated in the
PR that shipped `GTM_MCP_RECALL_ARCHITECTURE_DIAGNOSTIC.md`.** Every fix it prescribed is live in the code
today. The most likely reason the flaw was suspected again is a **stale module docstring**
(`database.py:55-59`) that still describes the *old, pre-fix* "opened per call, never cached" contract —
the precise text the prior diagnostic flagged as the flaw. The real remaining work is the **secondary
objective**: there is no tool that consumes the 90-day vector mirror.

---

## 1. The Compute Teardown — strict Yes/No

### 1.1 Connection pooling — **NOT torn down per request**

`database.py` holds four process-lifetime state objects; every one of the ~40 tools reaches the data plane
through them. A name-anchored grep confirms **no** `lance.dataset(` / `duckdb.connect(` / `boto3.client(`
construction exists anywhere under `src/tools/` — all are confined to `database.py` inside the
singleton/cache functions.

| State object | What | Lifetime | Anchor |
|---|---|---|---|
| `_con` | single in-memory DuckDB connection (R2 S3 secret + `hqx` Postgres ATTACH configured once) | process (double-checked lazy singleton) | [`database.py:125,641-658`](apps/gtm_mcp/src/database.py:641) |
| `_registry` | discovered `name → s3://uri` map | process singleton, 30-min TTL self-refresh | [`database.py:129,360-374`](apps/gtm_mcp/src/database.py:360) |
| `_tls.s3` | boto3 R2 client | per-thread, built once per thread | [`database.py:131,243-248`](apps/gtm_mcp/src/database.py:243) |
| `_handle_cache` | warm, index-resident `LanceDataset` handles, per-URI | process, per-dataset TTL tiered by mutation cadence (`_ttl_for`) | [`database.py:151,508-531`](apps/gtm_mcp/src/database.py:508) |

- Per-query work runs on a `con.cursor()` that is closed in a `finally` ([`database.py:706-721`](apps/gtm_mcp/src/database.py:706)); the **connection**, the R2 secret, and the `hqx` ATTACH are never torn down. The only `.close()` calls in the whole tree are on cursors ([`provider360.py:215`](apps/gtm_mcp/src/tools/provider360.py), `database.py:721`), never on `_con`. No `_con = None` reset, no `_handle_cache.clear()` in the request path, no `atexit` hook.
- Both DuckDB extension installs and the 740-ms Postgres ATTACH are **warmed once at boot** ([`main.py:143-149`](apps/gtm_mcp/main.py:143)), so the first real request pays neither.
- Point lookups reuse the resident scalar index across calls (the entire win of the prior remediation): cold open ~1.5–3.5 s vs warm ~0.14–0.29 s (`RECALL_ARCHITECTURE_DIAGNOSTIC §2`).

**One real-but-cold inconsistency (info-severity):** `get_object_bytes` builds a **fresh boto3 client** via
`_s3_client()` instead of the per-thread `_thread_s3()` — [`database.py:251-259`](apps/gtm_mcp/src/database.py:251). Its
only caller is `catalog._catalog_schema()` ([`catalog.py:58`](apps/gtm_mcp/src/tools/catalog.py:58)), which is memoized behind
`_schema_cache`, so it fires **at most once per process** (re-arming only on `catalog.invalidate()`). A
client-construction inconsistency, not a hot-path teardown.

### 1.2 Embedding inefficiency — **no embedding exists; the question is N/A**

There is **no query-embedding step anywhere in the server**, so it cannot re-embed anything.

- Grep across `apps/gtm_mcp` for `embed|embedding|voyage|cohere|openai|sentence_transformer|text-embedding|knn|nearest|cosine|l2_distance|metric_type|hnsw|nprobe` → **zero hits.** The only `.encode(` calls are `str.encode("utf-8")` for HTTP bodies / bearer tokens ([`main.py:91`](apps/gtm_mcp/main.py:91), [`parallel.py:140`](apps/gtm_mcp/src/tools/parallel.py:140), [`hydration.py:76`](apps/gtm_mcp/src/tools/hydration.py:76)).
- `requirements.txt` carries no embedding/vector dependency (mcp, uvicorn, duckdb, pylance, pyarrow, boto3, psycopg only).
- The word "semantic" in [`provider360.py:5`](apps/gtm_mcp/src/tools/provider360.py:5) / `README.md` means *business-named parameterized tools*, **not** vector search. Every read is Lance BTREE/BITMAP scalar-index pushdown (`.scanner(filter=…)`) or DuckDB ANSI SQL over Arrow-bridged Lance relations.

There is **no LRU cache to add today** because there is nothing being embedded. (When the vector tool is
built — §3.2 — the embedding cache becomes a build-time requirement; specified there.)

### 1.3 Network waste — **no blind re-fetch of the same key on the read hot path**

The directive's premise — "USAspending / Prime Award enrichment blindly hitting the network for the same
`notice_id`" — is **misattributed to the wrong process.**

- The award read, `lookup_awards_by_uei` ([`audience.py:113-137`](apps/gtm_mcp/src/tools/audience.py:113)), is served from `database.open_dataset("awards")` (→ `contractor_award_summary`) through the warm `_handle_cache` with a BTREE pushdown on `recipient_uei`. Re-querying the same UEI in a session reuses the resident handle — **no external API, no cold reopen.** Same for company/people domain lookups.
- USAspending / Prime-award **enrichment** is not performed by the MCP server at all. `parallel.py` and `hydration.py` are **control-plane signal tools**: they fire a single idempotency-keyed Trigger.dev REST dispatch ([`parallel.py:128-155`](apps/gtm_mcp/src/tools/parallel.py:128), [`hydration.py:66-92`](apps/gtm_mcp/src/tools/hydration.py:66)) to a Modal worker, which is the only thing that calls the external API and writes the dataset. There is no enrichment payload in the server to cache.

**The genuine — but bounded — per-call overhead (the only "waste" found):**

| Item | What repeats per call | Severity | Anchor |
|---|---|---|---|
| `corex` DDL re-exec | full `COREX_DDL` (~20 `CREATE/INDEX/ALTER … IF NOT EXISTS`) on **every** `_cursor()` entry; 3 tools open `_cursor()` twice/call → 2× DDL | **med** | [`corex.py:265`](apps/gtm_mcp/src/tools/corex.py:265); double at `:433/:439`, `:505/:525`, `:628/:670` |
| `parallel` spec DDL re-exec | `_SPECS_DDL` re-run on every spec read/write | low | [`parallel.py:110,422`](apps/gtm_mcp/src/tools/parallel.py:110) |
| per-call psycopg connect | fresh `psycopg.connect` per write/validate call (no client-side pool) | low | [`ops.py:134`](apps/gtm_mcp/src/tools/ops.py:134), [`hydration.py:49`](apps/gtm_mcp/src/tools/hydration.py:49), [`corex.py:262`](apps/gtm_mcp/src/tools/corex.py:262), [`parallel.py:90,108,421`](apps/gtm_mcp/src/tools/parallel.py:90) |
| `corex` enroll loop | per-contact SELECT + 2 upserts, row-by-row (≤~3,000 stmts for a 1,000-contact audience) on one connection | low | [`corex.py:526-555`](apps/gtm_mcp/src/tools/corex.py:526) |

Mitigating fact: every per-call `psycopg.connect` targets `HQX_DB_URL_POOLED` — the **Supavisor
transaction pooler** ([`database.py:553-563`](apps/gtm_mcp/src/database.py:553)) — so connection cost is amortized
server-side; the only avoidable client cost is the TCP+TLS+auth handshake. The DuckDB-attach read path is
**already** the efficient surface (`ops.list_postgres_tables` / `get_postgres_schema` ride the shared
singleton, [`ops.py:189-258`](apps/gtm_mcp/src/tools/ops.py:189)). The psycopg split is the *documented* control-plane
write/validate idiom ([`ops.py:21-34`](apps/gtm_mcp/src/tools/ops.py:21)), not an oversight — DuckDB's Postgres writer
lacks a clean `ON CONFLICT`.

---

## 2. The Schema Audit — legacy data paths

**Result: there are no legacy data paths to repoint.** A repository-wide grep
(`sam_attachment | govcon_scope | scope_vector | _90day | attachment_files`, over all `.py`/`.sql`/`.md`,
`__pycache__` excluded) returns **zero matches** anywhere in `apps/gtm_mcp`.

### 2.1 Why no repoint is needed (the dynamic registry)

The catalog is **not a hardcoded list.** `discover_datasets()` lists `s3://data-sink/active/*` and registers
every prefix carrying the `_versions` Lance marker by its on-disk path-relative name
([`database.py:289-320`](apps/gtm_mcp/src/database.py:289); the register line is `out[rel] = f"{ACTIVE_URI}/{rel}/"` at
[`database.py:311`](apps/gtm_mcp/src/database.py:311)). Therefore:

- `active/sam_attachment_files_90day/` → auto-registers as dataset **`sam_attachment_files_90day`**
- `active/govcon_scope_vectors_90day/` → auto-registers as dataset **`govcon_scope_vectors_90day`**

…on the next registry TTL refresh (≤30 min) or restart, with **no code change.**

### 2.2 The only hardcoded path literals (all correct, none legacy)

| `file:line` | Literal | Classification | Repoint? |
|---|---|---|---|
| [`database.py:82`](apps/gtm_mcp/src/database.py:82) | `ALIASES = {"awards": "contractor_award_summary"}` | back-compat alias | No |
| [`database.py:339-346`](apps/gtm_mcp/src/database.py:339) | resilience seed + env overrides: `companies` / `people` / `contractor_award_summary` | discovery floor (only fires if R2 list perm is absent) | No |
| [`database.py:75-77`](apps/gtm_mcp/src/database.py:75) | `BUCKET="data-sink"`, `ACTIVE_URI=…/active` | sink coordinates | No |
| [`catalog.py:28`](apps/gtm_mcp/src/tools/catalog.py:28) | `active/catalog.json` | maintained manifest (not a dataset) | No |
| [`parallel.py:52-53`](apps/gtm_mcp/src/tools/parallel.py:52) | `…/active/enrichment`, `…/active/parallel_research/` | **write** targets for Parallel materialize (unrelated feeds) | No |
| [`pe_thesis_query.py:88-90`](apps/gtm_mcp/pe_thesis_query.py:88) | `cms_general_payments` / `cms_research_payments` / `nppes_provider` | **offline script**, not in the request path (see §2.3) | No |

The seed/alias/env-override set is exhaustively `companies` / `people` / `contractor_award_summary` — **none
names the SAM/govcon/scope/attachment family**, so none can shadow a `*_90day` variant.

### 2.3 `pe_thesis_query.py` — not in the request path

`pe_thesis_query.py` is an **ad-hoc/importable offline script** (`python3 -m apps.gtm_mcp.pe_thesis_query`,
[`:31-37`](apps/gtm_mcp/pe_thesis_query.py:31)). It is **not** imported by `main.py` or any tool (grep confirms only a
docstring mention in `provider360.py:7`). Its own `duckdb.connect()` ([`:197`](apps/gtm_mcp/pe_thesis_query.py:197)) is
therefore irrelevant to per-request cost. Minor drift worth noting: its `DEFAULT_SNAPSHOT="2026-05"`
([`:51`](apps/gtm_mcp/pe_thesis_query.py:51)) lags `provider360.DEFAULT_SNAPSHOT="2026-06"` ([`provider360.py:54`](apps/gtm_mcp/src/tools/provider360.py:54)) — cosmetic, since both resolve dynamically when run live.

---

## 3. The Remediation Plan

Two tiers. Tier A is housekeeping on an already-sound architecture. Tier B is the actual capability the
90-day mirror was built for and that the gateway does not yet expose.

### 3.1 Tier A — correctness & micro-efficiency (low blast radius)

| # | Fix | Where | Why |
|---|---|---|---|
| A1 | **Rewrite the stale module docstring** to describe the warm-handle cache (the shipped behavior), deleting the "opened per call, never cached" contract. | [`database.py:55-59`](apps/gtm_mcp/src/database.py:55) | This text is the source of the recurring "stateless flaw" suspicion; it directly contradicts `_handle_cache` two screens below it and misleads every future auditor. **Do this first.** |
| A2 | **Hoist `COREX_DDL` to a once-per-process guard** (module flag set after first successful run) instead of re-executing on every `_cursor()`. | [`corex.py:252-266`](apps/gtm_mcp/src/tools/corex.py:252) | Highest-frequency avoidable work in the codebase (~20 DDL stmts × every tool call, 2× on three tools). Same pattern for `_SPECS_DDL` ([`parallel.py`](apps/gtm_mcp/src/tools/parallel.py:110)) and `OPS_DDL` ([`ops.py`](apps/gtm_mcp/src/tools/ops.py)). |
| A3 | **Point `get_object_bytes` at `_thread_s3()`** instead of constructing a fresh `_s3_client()`. | [`database.py:256`](apps/gtm_mcp/src/database.py:256) | Removes the lone divergence from the per-thread-client pattern. Cosmetic (cold path), but trivial and correct. |
| A4 | *(optional)* Collapse the `corex` enroll loop to a set-based CTE upsert; move `hydration._audience_exists` onto the shared DuckDB `hqx` attach (it is a pure read). | [`corex.py:526-555`](apps/gtm_mcp/src/tools/corex.py:526), [`hydration.py:49`](apps/gtm_mcp/src/tools/hydration.py:49) | Removes N round-trips / one per-call psycopg handshake. Low value given the pooled DSN; do only if these tools become hot. |

A client-side psycopg pool is **not** recommended: the DSN is already a server-side transaction pooler;
adding a client pool duplicates pooling and complicates the stateless-HTTP multi-instance story
(`main.py:54`). A2 captures ~all the real win.

### 3.2 Tier B — close the 90-day mirror gap (the actual objective)

The mirrors **auto-register** (§2.1), so today they are reachable **only as scalar relations** via
`execute_audience_query` (raw SQL, BTREE-filterable on `notice_id`/`naics`/`psc`, returns `text`) and as
schema via `describe_dataset`. The `embedding` column of `govcon_scope_vectors_90day` can be *projected*
but **never searched by similarity** — its entire purpose (hybrid filter-then-ANN retrieval, per
`GOVCON_90DAY_TRIGGER_DIAGNOSTIC §5.5`) is unreachable through the gateway. This is an **additive gap**, not
a mis-pointed consumer.

**B1 — Add a query-embedding helper with an LRU/TTL cache.** This is where the directive's "embedding
inefficiency" concern correctly lands — as a *build-time requirement of the new tool*, not a retrofit of
existing code:

```python
# new: src/embeddings.py  (model + dim MUST match the writer in the govcon scope-vector pipeline)
import functools

@functools.lru_cache(maxsize=2048)          # query text → vector; bounded, process-resident
def embed_query(text: str) -> tuple[float, ...]:
    # single embedding-API call; identical scope queries within a session hit the cache.
    ...
```

Add the embedding dependency to `requirements.txt` (pin the exact model that produced the stored vectors —
a dimension/model mismatch silently returns garbage neighbors).

**B2 — Add a hybrid filter-then-ANN search tool** over `govcon_scope_vectors_90day`, mounted on FastMCP like
every other tool. It must push the scalar predicate **and** the vector query into one Lance scanner (a
DuckDB scan over a registered relation engages neither the scalar index nor ANN — `RECALL §6.1`,
`GOVCON §6.1`):

```python
def search_govcon_scopes(query: str, naics: list[str] | None = None,
                         psc: list[str] | None = None, k: int = 20) -> dict:
    """Semantic scope search: filter the 90-day govcon corpus by NAICS/PSC, then rank by
    similarity to `query`. Returns notice_id, naics, psc, text chunk, distance."""
    qv = embed_query(query)
    ds = database.open_dataset("govcon_scope_vectors_90day")     # warm _handle_cache (free reuse)
    pred = _and(_in("naics", naics), _in("psc", psc))            # BTREE prefilter
    tbl = ds.scanner(
        prefilter=True, filter=pred or None,
        nearest={"column": "embedding", "q": qv, "k": k, "nprobes": 20},
        columns=["notice_id", "naics", "psc", "text"],
    ).to_table()
    return {"matches": tbl.to_pylist()}
```

This rides the existing warm-handle cache for free (B-tier inherits Tier-A's connection reuse). Register it
in a new `src/tools/govcon.py` and add `govcon.register(mcp)` to `main.py`.

**B3 — (optional) Typed attachment lookup** over `sam_attachment_files_90day` — a thin `get_attachment_text(notice_id)`
point-lookup (BTREE on `notice_id`) for retrieving extracted PWS/SOW text. Lower priority: this dataset is
already usefully reachable via `execute_audience_query`; the vector tool (B2) is the one that is structurally
impossible without new code.

**B4 — Verification gate.** After B1–B2, confirm with a live query inside Render (co-located, not laptop→R2)
that (a) `list_datasets` shows both `*_90day` datasets, (b) `search_govcon_scopes("SCIF / sensitive
compartmented information facility", naics=["236220"])` returns the security-intent subset that keyword
targeting cannot (`GOVCON §4.3` — the 126 PSC-N063/C1AZ rows hidden inside 2,208 border-barrier false
positives), and (c) `EXPLAIN`/scanner stats show the BTREE prefilter + ANN both fired (not a full scan).

### 3.3 Sequencing

```
A1 (docstring)  ──► removes the false-alarm surface; do immediately, zero risk
A2 (DDL hoist)  ──► the only material per-call win
A3 (s3 client)  ──► trivial cleanup
        │
        ▼
B1 (embed+LRU) ─► B2 (hybrid ANN tool) ─► B4 (verify in-region) ─► [B3 attachment lookup, optional]
```

Tier A is independent and shippable on its own. Tier B is the capability work; it does **not** depend on A
beyond inheriting the warm-handle cache that is already in place.

---

## 4. What was checked (provenance)

- **Read first-hand:** `database.py`, `main.py`, `catalog.py`, `audience.py`, `parallel.py`, `hydration.py`, `provider360.py`; both governing diagnostics.
- **Workflow finders (file:line):** `corex.py`, `ops.py`, `dmaas.py`, `pe_thesis_query.py`, cross-cutting path + vector/embedding audits.
- **Adversarial verifiers (4):** connection-teardown, embedding-recompute, network-refetch, path-alignment — **all four returned `refuted: False`** (could not break the verdicts above); each independently re-read source rather than trusting the finders.
- `dmaas.py` confirmed a pure Lob **stub** (`status: not_implemented`) — no datasets, network, or embeddings; out of scope.
