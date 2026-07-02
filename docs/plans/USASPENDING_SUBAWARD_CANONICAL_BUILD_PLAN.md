# USAspending Subaward Canonical Table — Build Plan (of record)

Plan of record for **`usaspending_subaward_canonical`** — a single typed-v2 Lance system-of-record that
reconciles the two USAspending **contract-subaward** feeds into ONE composite-PK-grained table under
`s3://data-sink/active/`. The SUBAWARD counterpart of the FPDS prime spine
([`usaspending_fpds_canonical.py`](../../pipelines/usaspending/usaspending_fpds_canonical.py)) — a subaward
is a CHILD of a prime award (up to ~23k subawards per prime), so it is a SEPARATE canonical, never folded
into the prime PK-grained table.

- **Pipeline / worker:** [`usaspending_subaward_canonical.py`](../../pipelines/usaspending/usaspending_subaward_canonical.py) (+ co-located `ops_usaspending_subaward_canonical_runs.sql`)
- **Control plane:** [`src/trigger/usaspending_subaward_canonical.ts`](../../src/trigger/usaspending_subaward_canonical.ts) (daily 20:00 UTC → dispatcher → `refresh_fn` → waitpoint callback)
- **Output URI:** `s3://data-sink/active/usaspending_subaward_canonical/` · Sample: `.../_sample/usaspending_subaward_canonical_sample/`
- **Shape:** LOCAL-CLI (doppler+uv) OR Modal. DIRECT-R2 write + DIRECT-R2 index (non-giant). `data_storage_version="2.1"`, overwrite read-model rebuild.
- **Columns:** 91 typed (7 key · 64 core · 18 enrich · 2 prov). Reference: [`SUBAWARD_CANONICAL_FIELD_DICTIONARY.md`](../reference/SUBAWARD_CANONICAL_FIELD_DICTIONARY.md) + [`SUBAWARD_SPINE_OMISSIONS_ADVERSARIAL_REVIEW.md`](../reference/SUBAWARD_SPINE_OMISSIONS_ADVERSARIAL_REVIEW.md).

---

## 1. Objective + locked decisions

**Objective.** Collapse the two contract-subaward feeds into one canonical, PK-unique, typed subaward table
keyed on the composite `(prime_award_unique_key, subaward_number)`, carrying the sub-grain facts + the
denormalized prime-award context serving needs (eliminating the subaward→award join), plus provenance.

**Locked decisions (do not relitigate):**
1. **Typed v2, not all-VARCHAR.** `subaward_amount`/officer-comp `double`, dates `date32`, mod-frontier +
   `built_at` naive `timestamp[us]`, FY/report `int64`. BULK rpt.* is TYPED (pass through / TRY_CAST the
   VARCHAR rpt cols); FRESH all-VARCHAR → `TRY_CAST(s(x) AS T)`.
2. **Canonical vocabulary = FRESH / bulk_download subaward names.** BULK `subaward_search` rpt.* is
   crosswalked in; BULK-only / FRESH-only enrichment keeps its native name.
3. **Contract-only scope** (`prime_award_group='procurement'`), mirroring FRESH. Grant subawards (7.16M,
   BULK-only) are a separate future canonical (§8).
4. **Separate canonical, not folded into the prime spine.** Child grain (fan-out ≤ 22,969 subs/prime).
5. **Overwrite read-model rebuild.** Derived/rebuildable → overwrite each run.

---

## 2. The reconciliation probe (the single most load-bearing fact)

Unlike the prime spine (shared PK proven byte-for-byte), the two subaward feeds have NO shared native PK
(BULK `broker_subaward_id` int64 vs FRESH `subaward_sam_report_id` string — different id spaces). The
composite PK was PROVEN by a live probe before any code was written:

| Question | Live result |
|---|---|
| Contract discriminator | `prime_award_group` ∈ {grant 7,158,222 · **procurement 2,643,501**} |
| BULK native PK 1:1 | `broker_subaward_id` 9,801,723 = distinct → 1:1 |
| FRESH native PK 1:1 | `subaward_sam_report_id` 321,204 rows / **226,928 distinct → NOT 1:1** (94,276 daily-re-pull dups → FRESH needs a collapse) |
| Composite grain | `(prime,subno)` ~2.0 BULK / 2.18 FRESH rows per composite → collapse latest-per-composite |
| Cross-source match | **90.27% FRESH containment in BULK**; 14,322 FRESH-only tail; 1,168,453 BULK-only body |
| Fan-out | 196,101 primes · **max 22,969 subs/prime** · avg 6.6 → child grain confirmed |

**rows_out centerline = 1,301,358 ∪ 14,322 = 1,315,680.** NULL-prime BULK rows (37,078 / 1.4%) drop.

---

## 3. Merge design (TWO-SOURCE per-composite argmax — ONE physical artifact)

- **PK (structural post-collapse):** `(prime_award_unique_key, subaward_number)`. A synthesized single-col
  `subaward_unique_key = prime|subno` is carried for BTREE point-lookup; the fail-closed gate uses the
  TUPLE + a `subaward_unique_key`-distinct == composite-distinct collision gate.
- **Two collapses** (`bulk_latest`, `fresh_latest`), each `row_number()=1` per composite, `ORDER
  subaward_last_modified_date DESC, <native surrogate> DESC` (BULK `broker_subaward_id` / FRESH
  `subaward_sam_report_id`). PK-uniqueness structural.
- **`subaward_last_modified_date`** = the UNIFIED mod-frontier (BULK `broker_updated_at` ⊕ FRESH parsed
  SAM mtime) = the argmax driver. Cross-clock; FRESH generally ≥ BULK and tie→FRESH → FRESH is the
  freshness overlay by construction.
- **core_winner** = ONE flat 2-way window over `core_union` (fresh rank 1, bulk rank 2), argmax on
  `subaward_last_modified_date`, tie→FRESH via `source_rank`.
- **Enrichment (single-source, independent of core winner):** BULK-only cols from `bulk_latest` (b.*),
  FRESH-only from `fresh_latest` (f.*). No COALESCE. `canonical_source` derived once = winner's src.
- **NO monthly/delta/tombstone** (a subaward is superseded, never deleted) — strictly simpler than the
  4-feed prime spine.
- **Fail-closed gates BEFORE publish:** PK-unique on the composite + synthesized-key collision.

Every projection / enrichment block / column order / index list is PROGRAM-GENERATED from `COLUMN_SPEC`.

---

## 4. CLI + run commands

```
python -m pipelines.usaspending.usaspending_subaward_canonical init_ops
python -m pipelines.usaspending.usaspending_subaward_canonical build  [--since YYYY-MM-DD] [--target-uri URI]
python -m pipelines.usaspending.usaspending_subaward_canonical index  [--target-uri URI]
python -m pipelines.usaspending.usaspending_subaward_canonical verify [--target-uri URI]
python -m pipelines.usaspending.usaspending_subaward_canonical print_merge_sql
```
`--since` pushes `subaward_action_date >=` into BOTH data scanners (BULK date32 / FRESH lexical ISO-10);
NEVER the contract-scope filter. `build`/`index` are split for blast-radius isolation.

**Full on-box build (the sanctioned path at this scale):**
```
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 -m pipelines.usaspending.usaspending_subaward_canonical build   # then: index ; then: verify
```
**Cadence (Modal):** `refresh_fn` (build→index→verify + waitpoint callback), dispatched daily by the
Trigger task. Manual: `modal run pipelines/usaspending/usaspending_subaward_canonical.py --cmd refresh`.

---

## 5. Box routing

| Run | Box | Why |
|---|---|---|
| SAMPLE / FULL | **On-box** (doppler+uv) | ~1.3M rows — FAR under the ~100M "giant" threshold; the collapses, 2-way argmax, and the composite BTREE sort are all small. DIRECT-R2 write + index (no local-stage/boto3 machinery). Verified: full build ~3.5 min, index+verify a few min. |
| Cadence | **Modal** (`refresh_fn`, 64 GiB) | Isolated, sized, waitpoint-callback wired — the fleet cadence substrate. No `ephemeral_disk` giant needed. |

**Hard rule:** the on-box path writes the SAMPLE URI unless building the prod canonical deliberately.

---

## 6. Disciplines (fleet rules)
- `LANCE_BYPASS_SPILLING="true"` module-top before any `import lance`.
- DIRECT-R2 write + index (non-giant; proven by `contractor_award_summary.py`). `data_storage_version="2.1"`.
- `built_at` = ONE injected naive-UTC literal (NOT now()). `subaward_last_modified_date` via
  `replace(...,'+00','')+TRY_CAST` (NO strptime).
- FSRS sentinels carried RAW (faithful SoR): `subaward_amount` 1.0e18, `subaward_action_date` 1900/2106;
  the ledger max-date is clamped; consumers clamp for display.
- NO auto-retries; overwrite idempotency. Programmatic schema-identity gate on the two collapses before union.

---

## 7. Verify gates (verified prod, 2026-07-02)

| Metric | Value |
|---|---|
| `rows_out` | **1,315,680** (= probe centerline) |
| `pk_unique` / `subaward_unique_key` 1:1 | **TRUE / TRUE** (0 dupes, 0 collisions) |
| `canonical_source` | bulk 1,300,059 · fresh 15,621 (= 14,322 FRESH-only + 1,299 corrections) |
| `fresh_corrections_applied` | 1,299 (> 0 gate) |
| `null_key_dropped` | 37,078 |
| `max(subaward_action_date)` | 2026-06-29 (clamped) |
| indices | 30 (11 BTREE + 19 BITMAP) |
| `built_at_distinct` | 1 |

Ledger `ops.usaspending_subaward_canonical_runs` records rows_in/out, dedup, tail/body, corrections,
null-key-dropped, max-date, indices, status, timestamps (psycopg, `HQX_DB_URL_POOLED`, WARN-only on failure).

---

## 8. Deferred scope
- **Grant subawards** (7.16M, BULK-only — no FRESH overlay): a separate `usaspending_subaward_grant_canonical`
  (grant vocabulary differs; FRESH is procurement-only).
- **Downstream repoint:** the 10 existing subaward consumers still read the raw feeds; repointing them at the
  canonical (eliminating their subaward→award joins via the on-spine prime context) is a separate track.
- **The ~120-column BULK/FRESH tail** (dead-in-contract-scope, `_desc`/name/FIPS/DUNS twins, internal/ETL) —
  recoverable in a v2 widening (append + re-index, no rewrite) if a consumer need emerges.
