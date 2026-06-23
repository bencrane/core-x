# GovCon v2-freeform-labor extraction — current state & handoff

**Last updated:** 2026-06-23
**Owning workstream:** GovCon LLM lane (Phase 2 of `GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md`)
**Active staging dir:** `/tmp/v2svc_stage/`
**Prompt version on the floor:** `v2-freeform-labor` (hash `f3567fc823dadf6aa1cc5f7f8699467cd77f79cb6fd107751ca54ddfe7148866`)
**Current script prompt hash:** `1798fde01d56...` (newer — staging is one revision behind, lands via `--allow-prompt-hash <hash>`)

---

## TL;DR for the next agent

10,339 SAM solicitation attachments are being extracted by the LLM lane into
`govcon_award_requirements` + `govcon_doc_scope` (Lance v2.1, R2). The v2 prompt
revision drops the controlled-vocab constraint on `labor_category` rows so the
extractor preserves raw job titles ("Senior Java Developer", "Site Superintendent",
"Cybersecurity Analyst", etc.) verbatim — quotability for outreach, at the cost
of cardinality blow-up downstream.

**81% landed** (8,375 / 10,339 docs). **1,964 still pending**, concentrated in
shards 4 / 7 / 17 / 18 / 23 — these are LLM lanes that crashed or were never
dispatched in the current wave. Yesterday's 7,086 landed under the regex
extract run; today's wave gap-filled 1,292 more.

All three downstream Lance products are **freshly rebuilt** off this state:
`govcon_award_capability_profiles` (35,726), `govcon_subawardee_capability_profiles`
(6,586), `govcon_sub_targeting` (192,747 edges).

---

## Phase status

| Phase | Status | Notes |
|---|---|---|
| 1. `extract` (regex lane) | ✅ done | Full corpus pass; `govcon_award_requirements` v14354 = 242,641 rows including all lanes' history |
| 2. `bracket` (decision A) | ✅ done | Marked resources `excluded_marked` in ledger; never enter LLM context |
| 3. `select` (stage 10,339 task files under prompt hash `f3567f`) | ✅ done | Files in `/tmp/v2svc_stage/tasks/` |
| 4. Agent extraction (LLM lane) | 🟡 **81%** | 8,375 / 10,339; 1,964 task files have no `.result.json` |
| 5. `ingest` (validate + land into Lance) | ✅ **caught up** to staged results | Reentrant; next ingest waits for more results |
| 6. Downstream materializations (profiles, targeting) | ✅ rebuilt 2026-06-23 | Snapshot pinned: reqs:v14354, scope:v13971, manifest:v6 |
| Future: labor title canonicalization | ⏳ not started | See [Next architectural step](#next-architectural-step--labor-title-canonicalization) |

---

## Landings tally

| Wave | Date | Docs landed | Rows into `govcon_award_requirements` | Notes |
|---|---|---:|---:|---|
| 1 | 2026-06-22 14:56 UTC | 7,086 | (per prior report) | Auto-passed 98% gate cleanly |
| 2 | 2026-06-23 04:25 EDT | 1,292 | 3,018 inserted + 87 updated | `--force-land`; pass_rate 0.9758 (below gate, see [Validator fix](#validator-fix-2026-06-23)) |
| **Total in Lance** | — | **8,375** | — | 81.0% coverage |

Remaining (1,964 docs) by shard (using `awk 'NR % 24'` over alphabetical task list):

| Shard | Remaining | Likely lane status |
|---|---:|---|
| 4 | 431 | full shard untouched |
| 7 | 339 | partial (~92 done) |
| 17 | 359 | partial (~72 done) |
| 18 | 427 | full shard nearly untouched |
| 23 | 367 | partial (~63 done) |
| 0/2/5/12/15/19 | 41 combined | stragglers, near-complete shards |

---

## Critical architecture facts

### URI naming convention (one-time burn class)

The plan originally suffixed every Lance dataset with `_90day` (window-suffix
table-per-window). Operator naming decision 2026-06-14 (commit
[`0b1efba`](https://github.com/bencrane/core-x/commit/0b1efba)) reversed this:
**the window is DATA** (carried as `award_last_modified_date` /
`award_action_date` / `built_at` columns), not a table name suffix. Widening the
window is now an append, not a new table.

**All current Lance datasets are at the non-`_90day` paths**:
- `s3://data-sink/active/govcon_award_requirements/` (NOT `..._90day/`)
- `s3://data-sink/active/govcon_doc_scope/`
- `s3://data-sink/active/govcon_labor_demand/`
- `s3://data-sink/active/govcon_requirements_extract_ledger/`
- `s3://data-sink/active/govcon_teaming_edges/`
- `s3://data-sink/active/govcon_sub_targeting/`
- `s3://data-sink/active/govcon_award_capability_profiles/`
- `s3://data-sink/active/govcon_subawardee_capability_profiles/`

The `_90day` paths **do not exist on R2** — confirmed via boto3 enumeration. If a
script you find still references them, it's stale.

### Lance dataset → builder map

| Dataset | URI | Built by | Idempotent? |
|---|---|---|---|
| `govcon_award_requirements` | s3://data-sink/active/govcon_award_requirements/ | regex: `sam_labor_demand_extract_90day.py --phase extract` · LLM: `--phase ingest` | merge_insert by `requirement_id` |
| `govcon_doc_scope` | s3://data-sink/active/govcon_doc_scope/ | LLM: `--phase ingest` | merge_insert by `resource_id` |
| `govcon_labor_demand` | s3://data-sink/active/govcon_labor_demand/ | regex: `sam_labor_demand_extract_90day.py --phase extract` | merge by `demand_id` |
| `govcon_requirements_extract_ledger` | s3://data-sink/active/govcon_requirements_extract_ledger/ | `--phase ingest` (status updates) | merge by `resource_id` |
| `govcon_award_capability_profiles` | s3://data-sink/active/govcon_award_capability_profiles/ | [build_award_capability_profiles.py](pipelines/sam_gov/build_award_capability_profiles.py) | overwrite snapshot |
| `govcon_subawardee_capability_profiles` | s3://data-sink/active/govcon_subawardee_capability_profiles/ | [build_subawardee_capability_profiles.py](pipelines/sam_gov/build_subawardee_capability_profiles.py) | overwrite snapshot |
| `govcon_sub_targeting` | s3://data-sink/active/govcon_sub_targeting/ | [materialize_sub_targeting.py](pipelines/serving/materialize_sub_targeting.py) | overwrite snapshot |

### Schema vocabulary (the staged task file embeds the full vocab)

- `requirement_types` (closed enum): `certification`, `clearance`, `labor_category`, `standard_compliance`, `license`, `equipment_capability`, `past_performance`, `deliverable`, `insurance_bonding`, `staffing_constraint`, `vehicle_constraint`
- `clearance_levels` (closed enum): `PUBLIC_TRUST`, `CONFIDENTIAL`, `SECRET`, `TOP_SECRET`, `TS_SCI`
- `capability_tags`: 76 controlled tags (construction_general, hvac_mechanical, it_services, cybersecurity_services, …) — agents MUST tag from this list only; OOV is rejected at ingest
- `labor_categories` (36 tokens): **DEPRECATED as a constraint** under v2-freeform-labor — kept in vocab for backwards-compat / value_norm_hints lookup, but labor rows are now free-form raw titles
- `value_norm_hints`: normalized values for certification (ISO/CMMC), clearance (`clearance:secret`), standard_compliance (`davis_bacon`, `nist-800-171`, `em-385-1-1`), license, insurance_bonding, staffing_constraint, vehicle_constraint, deliverable

### CUI egress invariant (anti-pattern #10 — never break this)

- `evidence_quote` and `requirement_detail` are **NULL at write** for resources where `marked_resource = true` (chunk-level `content_marking` is non-empty)
- `govcon_doc_scope` has zero marked rows by construction (marked docs are bracketed out of the LLM lane in decision A)
- `build_*_capability_profiles.py` and `materialize_sub_targeting.py` all assert these invariants and refuse to write on violation
- Capability profiles carry **NO verbatim chunk text** — `scope_summary` sourced only from doc_scope (unmarked); requirements rollups are normalized values only

---

## Today's session (2026-06-23) — what happened

### Validator fix (2026-06-23)

**Defect.** `sam_labor_demand_extract_90day.py:validate_result()` was rejecting
free-form labor titles as `labor_category_out_of_vocab` even when the staged task
file declared `prompt_version: "v2-freeform-labor"`. The script HAD an
`LLM_FREEFORM_LABOR` env-flag gate (commit
[`baa636f`](https://github.com/bencrane/core-x/commit/baa636f)) but it required
manual opt-in (`GOVCON_LLM_FREEFORM_LABOR=1`) every run — operator-hostile, easy
to forget.

**Fix landed in PR [#640](https://github.com/bencrane/core-x/pull/640):** validator
now reads `task["prompt_version"]` and auto-honors v2-freeform-labor without the
env flag. Env flag retained for forcing free-form behavior under other prompt
versions.

```python
# pipelines/sam_gov/sam_labor_demand_extract_90day.py:1798
freeform_labor = task.get("prompt_version") == "v2-freeform-labor"
# … at line 1837:
if (rtype == "labor_category" and value_norm not in labor_set
        and not LLM_FREEFORM_LABOR and not freeform_labor):
    reject(f"labor_category_out_of_vocab:{value_norm}")
```

**Quantified impact.** Wave 2's ingest run pass rate went from **0.7586 → 0.9758**
with no other changes. 707 valid free-form labor rows that would have been
rejected are now retained.

### Today's ingest (the actual landing)

```bash
cd /Users/benjamincrane/core-x && PATH=/Users/benjamincrane/core-x/.venv/bin:$PATH \
  doppler run -- python pipelines/sam_gov/sam_labor_demand_extract_90day.py \
  --phase ingest \
  --staging-dir /tmp/v2svc_stage \
  --allow-prompt-hash f3567fc823dadf6aa1cc5f7f8699467cd77f79cb6fd107751ca54ddfe7148866 \
  --requirements-uri s3://data-sink/active/govcon_award_requirements/ \
  --doc-scope-uri s3://data-sink/active/govcon_doc_scope/ \
  --ledger-uri s3://data-sink/active/govcon_requirements_extract_ledger/ \
  --labor-uri s3://data-sink/active/govcon_labor_demand/ \
  --force-land
```

**Result:** 7 batches, all landed. 1,236 docs pass + 56 partial = 1,292 total
docs. 3,105 valid requirement rows. 77 rows dropped at validation (46
quote_mismatch, 30 bad_mandatory_type, 1 bad_requirement_type — real agent
output bugs, validator correctly drops).

`--force-land` was used because run_pass_rate (0.9758) is just below the 98%
gate. Inspection confirmed the 77 rejects are agent-side malformed output, not
infrastructure — the right call was to land the 3,105 good rows. Future waves
should hit ≥ 98% organically; if not, investigate before forcing.

### Downstream rebuilds (all clean)

```bash
doppler run -- python pipelines/sam_gov/build_award_capability_profiles.py build
# → 35,726 rows · txn_resolution=1.0000 · 37s

doppler run -- python pipelines/sam_gov/build_subawardee_capability_profiles.py build
# → 6,586 rows · 54s

doppler run -- python pipelines/serving/materialize_sub_targeting.py build
# → 192,747 edges · 6,954 awards × 15,367 subs · 75s
```

`sub_targeting` edge breakdown:
- `direct_subaward` (sub previously sub-let by this prime on this award): 5,594
- `teaming_history` (sub teamed with this prime over 5y corpus): 281,846
- `capability_match` (4-digit NAICS family equality + labor-token hit on
  subaward_description): 45,428
- `poc_available` (POC in `sam_pocs`): 185,455 (96.2%)

---

## How to close out Phase 4 (the remaining 1,964)

1. **Re-dispatch agent fleet** for the 5 unfinished shards (4, 7, 17, 18, 23).
   The original dispatch prompt is the embedded `instructions` in each task
   file; the agent shard pattern is:

   ```bash
   ls /tmp/v2svc_stage/tasks/*.task.json | awk 'NR % 24 == <SHARD>'
   ```

   Agents read the task, write to the EXACT `result_path` in the task JSON
   (`/tmp/v2svc_stage/results/<rid>.result.json`). Skip if result file already
   exists (`test -f`).

2. **Re-run ingest** with the same `--allow-prompt-hash` + URI overrides above.
   After PR [#640](https://github.com/bencrane/core-x/pull/640) merged, the URI
   overrides are redundant on `main` (the schemas + builders already point at the
   non-`_90day` paths). The `--allow-prompt-hash` flag IS still required because
   the staged tasks are on prompt hash `f3567f` while the script's current hash
   is newer.

3. **Rebuild the three downstream products** with the same three `build`
   commands above. Each is idempotent snapshot-overwrite — re-running with the
   same upstream state is provably zero-delta (verify with
   `<builder> verify --content-hash`).

**Expected close-out tally:** 10,339 docs landed, ~24,500 requirement rows in
the v2-freeform-labor cohort, capability profiles uplift by ~19% on
`has_extracted_scope`, sub_targeting `capability_match` edges uplift by a
similar magnitude.

---

## Next architectural step — labor title canonicalization

(This is the question the operator surfaced at end-of-session. Decision NOT
yet made; this section captures the framing.)

**Why.** v2-freeform-labor deliberately PRESERVED raw titles for evidence
quotability. The downstream consequence is cardinality blow-up: "Senior Java
Developer", "Sr. Java Developer", "Java Developer (Senior)", "Java Dev III" all
land as distinct rows. Direct impact:

- `top_labor_categories` rollups in `govcon_award_capability_profiles` fragment
  across spelling variants
- `capability_match` edges in `sub_targeting` token-match on
  `subaward_description` — fires under-frequently when titles diverge from
  description token surface form
- Cross-award labor demand aggregation (which roles are in demand, where, by how
  much) can't be done cleanly off the raw column

**Pattern — Path B sidecar.** Same shape as the existing
`govcon_sub_self_reported_tags` sidecar that `build_subawardee_capability_profiles`
joins (desc_sha → tags). For labor:

| Dataset (proposed) | Grain | Columns |
|---|---|---|
| `govcon_labor_title_canonical` | `title_sha` = sha256(normalized raw title) | `raw_title`, `canonical_title`, `function` (e.g. software_engineering, security, construction_trades, healthcare_clinical), `specialty` (e.g. java, network_security, electrician, registered_nurse), `seniority` (junior/mid/senior/lead/principal), `is_management` (bool), `is_clearance_bearing` (bool), `soc_code` (BLS SOC mapping, nullable), `version`, `built_at` |

Profile + sub_targeting builders read `canonical_title` for rollups and matching;
keep `raw_title` in `evidence_quote` for citation.

**Two design choices to make first:**

1. **Engine.** Options:
   - **Deterministic** (regex/lookup for seniority + curated function dictionary). Fastest, explainable, but tail recall is mediocre on weird titles (acronyms, military-domain, very long descriptive titles).
   - **LLM clustering** via the existing extract → stage → ingest infrastructure (`--phase llm-canonicalize-labor` would mirror `--phase extract`). Best recall, validator-gated, but cost scales with cardinality.
   - **Embedding clusters** + small calibration set. Middle ground; can power
     fuzzy search at query time too.

   **Recommendation:** start deterministic (covers ~80% of the head), layer LLM
   only for the long tail (titles seen < N times that didn't match a
   deterministic rule). Hybrid avoids paying LLM cost on the obvious 10k
   variants of "Software Engineer".

2. **Vocabulary anchor.**
   - **BLS SOC codes** (~840 codes, federal-standard, mapped to government job
     series and wage data) — free interoperability with public labor stats, but
     SOC is too coarse for some govcon constructs (e.g. clearance-bearing
     roles, "FSO", "PWS author" — none have a SOC code).
   - **Custom taxonomy** — agile, but now an owned asset that needs
     maintenance.

   **Recommendation:** SOC as the spine + custom enrichments where SOC is too
   coarse. SOC code becomes a join key to the BLS OEWS wage tables; custom
   columns (`is_clearance_bearing`, `is_management`) live in the sidecar.

**Same pattern wanted for two adjacent vocabularies (worth scoping together):**

- `capability_tags` — already controlled vocab (76 tokens), no normalization
  needed; but a `capability_tag → broader_function` mapping would help with
  rollups.
- `certification` — `value_norm_hints` lists 13 normalized forms (iso_9001,
  cmmc_l2, etc.) but extracted rows beyond those land verbatim. Same sidecar
  approach: `cert_sha → canonical_cert + family + issuing_body`.

If the operator wants this, scope all three behind one builder /
`govcon_*_canonical` sidecar pattern. Don't build labor in isolation if
capability_tags and certification will need the same shape three months later.

---

## Known code debt (surfaced this session, status varies)

| File | Issue | Status |
|---|---|---|
| `sam_labor_demand_extract_90day.py:1837` | v2-freeform-labor required env-flag opt-in | ✅ fixed in PR [#640](https://github.com/bencrane/core-x/pull/640) |
| `sam_labor_demand_extract_90day.py:1174` | `LLM_PROMPT_VERSION = "v1"` constant doesn't match staged `v2-freeform-labor` — forces `--allow-prompt-hash <hex>` on every ingest | Open. Whether to bump the script constant to `v2-freeform-labor` depends on whether v2 is the going-forward default (recommend yes; it's been on the floor for the live wave). |
| `govcon_gtm_schemas.py:11-30` | Docstring still references `..._90day` table names that don't exist | Open. Cosmetic; URIs and code are correct. |
| Staged tasks list `engine: "session-fable"` but were extracted by other sessions | Cosmetic; ledger records the actual engine via `extractor` column | Open. No functional impact. |

---

## Useful commands cheat-sheet

```bash
# How many tasks landed in Lance (LLM lane) so far?
doppler run -- python -c "
import lance; from pipelines.sam_gov.govcon_gtm_schemas import EXTRACT_LEDGER_URI, _r2_storage_options
ds = lance.dataset(EXTRACT_LEDGER_URI, storage_options=_r2_storage_options())
import duckdb; con = duckdb.connect(':memory:')
con.register('l', ds.scanner(columns=['resource_id', 'llm_state']).to_table())
print(con.execute('SELECT llm_state, count(*) FROM l GROUP BY 1 ORDER BY 2 DESC').fetchall())
"

# How many free-form labor titles exist? (post-PR #640 landing)
doppler run -- python -c "
import lance; from pipelines.sam_gov.govcon_gtm_schemas import REQUIREMENTS_URI, _r2_storage_options
ds = lance.dataset(REQUIREMENTS_URI, storage_options=_r2_storage_options())
import duckdb; con = duckdb.connect(':memory:')
con.register('r', ds.scanner(columns=['requirement_type', 'requirement_value', 'extractor_version'],
    filter=\"requirement_type = 'labor_category' AND extractor_version = 'v2-freeform-labor'\").to_table())
print('rows:', con.execute('SELECT count(*) FROM r').fetchone())
print('distinct titles:', con.execute('SELECT count(DISTINCT requirement_value) FROM r').fetchone())
print('top 20:')
for row in con.execute('SELECT requirement_value, count(*) c FROM r GROUP BY 1 ORDER BY 2 DESC LIMIT 20').fetchall():
    print(f'  {row[1]:>4}  {row[0]}')
"

# Re-run downstream rebuild after a future ingest wave
doppler run -- python pipelines/sam_gov/build_award_capability_profiles.py build
doppler run -- python pipelines/sam_gov/build_subawardee_capability_profiles.py build
doppler run -- python pipelines/serving/materialize_sub_targeting.py build

# Verify zero-delta idempotency of any rebuilder
doppler run -- python pipelines/sam_gov/build_award_capability_profiles.py verify --content-hash
```

---

## File references

- Extract/ingest engine: [pipelines/sam_gov/sam_labor_demand_extract_90day.py](pipelines/sam_gov/sam_labor_demand_extract_90day.py) (validator at :1788, ingest phase at :2017)
- Schemas + URIs (single source of truth): [pipelines/sam_gov/govcon_gtm_schemas.py](pipelines/sam_gov/govcon_gtm_schemas.py)
- Award capability profile builder: [pipelines/sam_gov/build_award_capability_profiles.py](pipelines/sam_gov/build_award_capability_profiles.py)
- Subawardee capability profile builder: [pipelines/sam_gov/build_subawardee_capability_profiles.py](pipelines/sam_gov/build_subawardee_capability_profiles.py)
- Sub-targeting materializer: [pipelines/serving/materialize_sub_targeting.py](pipelines/serving/materialize_sub_targeting.py)
- Underlying build plan (authority for column/type tables): [docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md](docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md)
- Related handoff (marked-lane local extraction, sibling workstream): [docs/plans/GOVCON_MARKED_LANE_LOCAL_EXTRACTION_HANDOFF.md](docs/plans/GOVCON_MARKED_LANE_LOCAL_EXTRACTION_HANDOFF.md)

---

## Insights (worth carrying forward)

1. **The `--force-land` decision is not casual.** Today's 97.58% < 98% gate
   meant 77 rows would be rejected by the validator. Inspection showed 100% of
   the rejects were agent-output bugs (malformed quotes, wrong type for
   `mandatory` — agent emitted string `"true"` instead of bool). The validator
   was doing its job; forcing was the right call because the 1,292 docs with
   3,105 good rows were genuinely good. Pattern: never force a land without
   first reading `reject_reasons` and confirming the failures are agent-side
   noise, not infrastructure regression.

2. **Reentrancy is the design.** The ingest phase is reentrant — every new
   wave of staged results just calls `--phase ingest` again. The Lance
   datasets merge_insert by content hash (`requirement_id` =
   `sha256(resource_id|requirement_type|value_norm)[:24]`). Re-extracting the
   same resource produces zero net change unless the content actually changed.
   Same for the downstream builders — they're snapshot-overwrite and the
   content-hash check makes "did this change anything?" a deterministic
   question.

3. **The cardinality cost of v2-freeform-labor is real and material.** The 36-token
   v1 labor vocabulary lost a lot of govcon-specific roles (Cybersecurity
   Analyst, Systems Administrator, Site Safety and Health Officer, FSO, etc. —
   none of which mapped). v2 fixed that, at the cost of needing a real
   normalization layer to make downstream matching work. Without
   canonicalization, `sub_targeting.capability_match` recall is bounded by the
   probability that the sub's `subaward_description` happens to contain the
   exact surface form the solicitation used. With canonicalization, recall is
   bounded by the much weaker condition that they share a canonical title.

4. **Single-source URI registry is load-bearing.** Three separate builder
   scripts redefined their OWN local copies of `REQUIREMENTS_URI` /
   `DOC_SCOPE_URI` / `TEAMING_URI` instead of importing from
   `govcon_gtm_schemas.py`. When the `_90day` → no-suffix rename happened in
   [`0b1efba`](https://github.com/bencrane/core-x/commit/0b1efba), the central
   constants were updated but the local copies in builder scripts were missed.
   Result: builders silently read empty datasets and would have overwritten
   downstream products with nothing. Recommended hygiene: all URI constants
   live in `govcon_gtm_schemas.py`; builders import them, never redefine.

5. **Per-shard agent-fleet visibility is missing.** When 21 of 24 lanes
   appeared empty mid-session, the apparent state ("only 3 of 24 shards done")
   was actually wrong — 7,086 docs from prior waves had been landed into Lance
   and the staging dir cleared. There's no operator-facing reconciliation
   between (a) which shards have agent activity, (b) which result files exist
   on disk, (c) which records are landed in the ledger. A dashboard or
   `--phase status` subcommand reading the ledger would prevent the next
   operator from drawing the same wrong conclusion.
