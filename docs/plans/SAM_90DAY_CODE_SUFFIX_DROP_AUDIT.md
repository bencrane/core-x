# SAM / govcon `_90day` CODE-suffix drop — AUDIT + SAFE-DROP PLAN

**Date:** 2026-06-26 · **Status:** AUDIT ONLY (read-only investigation; no code changed, no git mv, no migration run).
**Scope:** dropping `_90day` from the **CODE layer** (module filenames, ops SQL filenames, FEED/env/identifier strings, ops table+index names, docs) — *after* the R2 dataset + ledger rename already executed in [#542](https://github.com/bencrane/core-x/pull/542) (`docs/plans/SAM_GOVCON_90DAY_RENAME_MIGRATION.md`) and the operator's **WINDOW-AS-DATA** naming decision (2026-06-14, `pipelines/sam_gov/govcon_gtm_schemas.py:20,182,299`).

**Headline (verified):** A naive "strip the suffix" is **unsafe and partly wrong**. The two collision pairs are NOT "stale name vs current name" — for `download`/`reconcile` the **`_90day` file is the CURRENT/canonical worker and the suffix-free file is the SUPERSEDED legacy** (the inverse of the intuitive read). The migration precedent doc itself rules module-file renames "**pure churn with import-path blast radius; do it last, separately, if at all**" (`SAM_GOVCON_90DAY_RENAME_MIGRATION.md:68-69`). No `_90day` module is a Modal app whose name derives from its filename — so no Modal app gets orphaned by a file rename. The live ops tables were **already** renamed off `_90day` in prod (#542); only **index names** and one **docstring** still carry the suffix in code (cosmetic).

---

## Inventory (verified by `find`)

**9 Python modules** carrying `_90day`:
- `pipelines/sam_gov/sam_attachment_download_90day.py` (31 KB)
- `pipelines/sam_gov/sam_attachment_embed_90day.py` (15 KB)
- `pipelines/sam_gov/sam_attachment_extract_90day.py` (111 KB)
- `pipelines/sam_gov/sam_attachment_gtm_scope_90day.py` (19 KB)
- `pipelines/sam_gov/sam_attachment_reconcile_90day.py` (10 KB)
- `pipelines/sam_gov/sam_labor_demand_extract_90day.py` (121 KB)
- `pipelines/sam_gov/sam_marking_fullbody_90day.py` (28 KB)
- `pipelines/sam_gov/sam_opps_attachment_manifest_90day_winners.py` (20 KB)
- `pipelines/usaspending/govcon_teaming_edges_90day.py` (15 KB)

**3 ops SQL files:** `pipelines/sam_gov/ops_sam_attachment_download_90day_runs.sql`, `ops_sam_attachment_gtm_scope_90day_runs.sql`, `ops_sam_extraction_90day_runs.sql`.

**8 docs:** `docs/plans/SAM_GOVCON_90DAY_RENAME_MIGRATION.md`, `docs/reference/GOVCON_90DAY_TRIGGER_DIAGNOSTIC.md`, `SAM_90DAY_EXTRACTION_PIPELINE_SPEC.md`, `..._ADVERSARIAL_REVIEW.md`, `..._V2.md`, `SAM_90DAY_FILENAME_TAXONOMY_SIZING.md`, `SAM_ATTACHMENT_90DAY_HARVEST_AND_FORENSIC_RECORD.md`, `docs/usaspending_90day_diagnostic.md`.

---

## PER-MODULE AUDIT TABLE

Legend — **Live evidence**: `imp=N` (real `from … import` count, tests included), `main` (`__main__` CLI), git PR of last edit. **Modal?** = does the file declare `modal.App(...)` (checked; none do). Classification: **RENAME** (clear, suffix-free counterpart absent), **RESOLVE-COLLISION** (suffix-free file of different code already exists), **KEEP-FOR-NOW** (touches a live identifier the rename must coordinate).

| Module | Suffix-free counterpart? | Same/different | Live / dead + evidence | Modal app? | Classification | Recommended new name |
|---|---|---|---|---|---|---|
| `sam_attachment_extract_90day.py` | **absent** | — | **LIVE — the hub.** `imp=11` real importers incl. 6 build/serving modules + 5 tests (`govcon_gtm_schemas.py:46`, `serving/materialize_sub_targeting.py:49`, `build_*_capability_*`, `classify_sub_self_reported_tags.py:37`, `subaward_scope_append.py:41`, `govcon_p0_uniqueness_preflight.py:28`, `embed_90day:41`, `labor_demand:90`, `marking_fullbody:61`). `main`. Writes live `ops.sam_extraction_runs` (22 rows, max 2026-06-20). | No `modal.App` | **RENAME** (highest blast radius) | `sam_attachment_extract.py` |
| `sam_labor_demand_extract_90day.py` | absent | — | **LIVE.** `imp=4` (3 tests + dynamic `importlib.import_module("sam_labor_demand_extract_90day")` at `scripts/archive/bigthree_reextract_census_probe.py:29`). `main`. `FEED="sam_labor_demand_extract_90day"`. `import modal` for GPU but **no `modal.App`** here. | No | **RENAME** + FEED + dynamic-import fix | `sam_labor_demand_extract.py` |
| `sam_marking_fullbody_90day.py` | absent | — | **LIVE.** `imp=1` (`test_sam_marking_fullbody.py:20`); referenced by extract/labor as Phase-0 pre-pass. `main`. `FEED="sam_marking_fullbody_90day"`, `EXTRACTOR_TAG="sam_marking_fullbody_90day@v1"`, `SAM90_MARKING_*` env. | No | **RENAME** + FEED/TAG | `sam_marking_fullbody.py` |
| `sam_attachment_gtm_scope_90day.py` | absent | — | **LIVE.** `main`; writes `ops.sam_attachment_gtm_scope_runs` (2 rows). `FEED="sam_attachment_gtm_scope"` (**already suffix-free**). Stale docstring `:36` still says `ops.sam_attachment_gtm_scope_90day_runs`. | No | **RENAME** | `sam_attachment_gtm_scope.py` |
| `sam_attachment_embed_90day.py` | absent (twin = `sam_attachment_embed_modal.py`, the GPU Modal app `govcon-embed`) | different (modal twin is self-contained) | **LIVE.** Local/in-stack CPU embed; `main`; imports extract hub (`:41`). Modal twin `embed_modal.py` mirrors it but is a *separate* file. | No (the **twin** `embed_modal.py` has `modal.App("govcon-embed")` — name is **hardcoded, not filename-derived**) | **RENAME** | `sam_attachment_embed.py` |
| `sam_opps_attachment_manifest_90day_winners.py` | absent | — | **LIVE.** `main` (`… manifest_90day_winners.py bridge`). Referenced by `download_90day.py:5`, `scripts/archive/sam_attachment_size_probe.py:62`. The `_winners` + `90day` describe the API-fresh window; substrate URI is the de-suffixed `sam_opps_attachment_manifest_winners/`. | No | **RENAME** (keep `_winners`) | `sam_opps_attachment_manifest_winners.py` |
| `sam_attachment_download_90day.py` | **`sam_attachment_download.py` EXISTS** | **DIFFERENT** (665 vs 684 L) | **`_90day` = CURRENT.** Writes the **canonical** live ledger `sam_attachment_files/` + `sam_attachment_blobs/` + `ops.sam_attachment_download_runs` (90-day column schema that WON in prod). Last edit #542. Its docstring (`:9`) names "historical `sam_attachment_download.py`" as the thing it **supersedes**. `main`. `FEED="sam_attachment_download_90day"`. | No | **RESOLVE-COLLISION** | `sam_attachment_download.py` (after legacy file removed/archived) |
| `sam_attachment_reconcile_90day.py` | **`sam_attachment_reconcile.py` EXISTS** | **DIFFERENT** (217 vs 152 L) | **`_90day` = CURRENT.** Backstop for `download_90day`; reconciles the same canonical `sam_attachment_files/`+`blobs/`+`worklist/`. Last edit #542 (2026-06-19). The suffix-free twin last touched #275 (2026-06-06), untouched since, points at the legacy harvest. `main`. | No | **RESOLVE-COLLISION** | `sam_attachment_reconcile.py` (after legacy file removed/archived) |
| `pipelines/usaspending/govcon_teaming_edges_90day.py` | absent | — | **LIVE but ISOLATED.** `imp=0` (zero importers). `main` (`build\|verify`). Dataset URI from `govcon_gtm_schemas.TEAMING_EDGES_URI` (already de-suffixed). `FEED="govcon_teaming_edges"` (**already suffix-free**). `90day` is **misleading** — substrate is a 5-yr corpus UNIONed with a 90-day fresh feed. | No | **RENAME** (cleanest of all — no imports) | `govcon_teaming_edges.py` |

### Ops SQL files (table-level rename already done in prod; index names lag)

| SQL file | `CREATE TABLE` (de-suffixed already) | Index names (still `_90day_`) | Live prod state (verified via `HQX_DB_URL_POOLED`) |
|---|---|---|---|
| `ops_sam_attachment_download_90day_runs.sql` | `ops.sam_attachment_download_runs` | `sam_attachment_download_90day_runs_{run_id,started_at}_idx` | Table exists (3 rows, max 2026-06-15). **Both secondary indexes MISSING in prod** (pkey only) — created by the *other* DDL first. Legacy schema preserved as `ops.sam_attachment_download_runs_legacy_pre90day` (7 rows, frozen 2026-06-07). |
| `ops_sam_attachment_gtm_scope_90day_runs.sql` | `ops.sam_attachment_gtm_scope_runs` | `sam_attachment_gtm_scope_90day_runs_{run_id,started_at}_idx` | Table exists (2 rows); both `_90day`-named indexes present. |
| `ops_sam_extraction_90day_runs.sql` | `ops.sam_extraction_runs` | `sam_extraction_90day_runs_{run_id,started_at}_idx` | Table exists (22 rows, max 2026-06-20); both `_90day`-named indexes present. `cui_tagged→content_marked` rename fully applied. |

**No `_90day`-suffixed physical table exists in prod** — the table-name rename is done. The residue is (a) **index names** hard-coding `_90day_runs_` (cosmetic; index names are not resolution keys) and (b) the SQL **filenames** carrying `_90day`.

---

## BREAKAGE ANALYSIS

### A. Import graph (the exact edit list for a rename)

Real `from … import` statements that move when a module is renamed (verified `grep -E "^\s*(from|import)"`). **All of `extract`'s importers** must flip in lockstep with the `extract` rename:

`sam_attachment_extract_90day` (11 sites):
- `pipelines/sam_gov/govcon_gtm_schemas.py:46`
- `pipelines/sam_gov/govcon_p0_uniqueness_preflight.py:28`
- `pipelines/sam_gov/sam_attachment_embed_90day.py:41`
- `pipelines/sam_gov/sam_labor_demand_extract_90day.py:90`
- `pipelines/sam_gov/sam_marking_fullbody_90day.py:61`
- `pipelines/sam_gov/subaward_scope_append.py:41`
- `pipelines/sam_gov/classify_sub_self_reported_tags.py:37`
- `pipelines/sam_gov/build_award_capability_profiles.py:65`
- `pipelines/sam_gov/build_subawardee_capability_profiles.py:58`
- `pipelines/sam_gov/build_sub_capability_vectors.py:47`
- `pipelines/serving/materialize_sub_targeting.py:49` *(cross-package — pipelines/serving)*
- tests: `tests/test_sam_labor_demand_extract.py:18`, `test_sam_attachment_id_filter.py:21`, `test_sam_attachment_finalize_dedup.py:19`, `test_govcon_llm_lane.py:19`

`sam_labor_demand_extract_90day` (4 sites):
- tests: `tests/test_sam_labor_demand_extract.py:19`, `tests/test_sam_labor_h2_gates.py:20`, `tests/test_govcon_llm_lane.py:20,211`
- **dynamic:** `scripts/archive/bigthree_reextract_census_probe.py:29` `importlib.import_module("sam_labor_demand_extract_90day")` — string literal, won't be caught by an IDE rename; must be edited by hand.

`sam_marking_fullbody_90day` (1 site): `tests/test_sam_marking_fullbody.py:20`.

**Cross-imports between `_90day` modules** (rename order matters): `embed_90day`, `labor_demand`, `marking_fullbody` all `import … from sam_attachment_extract_90day`. Rename `extract` and its importers atomically in one commit, or imports break mid-rename.

`download_90day`, `gtm_scope_90day`, `manifest_90day_winners`, `teaming_edges_90day` have **zero real importers** — pure leaf renames (only docstring/script-path references to fix).

### B. FEED / env / identifier surface

| Identifier | Where | Touches a live system? | Action |
|---|---|---|---|
| `FEED="sam_attachment_extract_90day"` | `extract:84` | **YES** — written to `ops.sam_extraction_runs.feed` (22 live rows). Changing it splits the `feed` history. | Coordinate with ops (treat as a value migration, not a code edit) — or leave FEED as-is and only rename the file. |
| `FEED="sam_labor_demand_extract_90day"` | `labor:100` | **YES** — ledger `feed` value. | Same as above. |
| `FEED="sam_marking_fullbody_90day"` + `EXTRACTOR_TAG="…@v1"` | `marking:67-68` | `EXTRACTOR_TAG` is a provenance value persisted in extraction events; bumping it changes lineage strings. | Treat as data-coordinated. |
| `FEED="sam_attachment_download_90day"` | `download_90day:68` | **YES** — `ops.sam_attachment_download_runs.feed`. | Data-coordinated. |
| `FEED` already suffix-free | `gtm_scope` (`sam_attachment_gtm_scope`), `teaming_edges` (`govcon_teaming_edges`) | n/a | No action — already clean. |
| `SAM90_*` env names (≈30 vars) | `extract:67-82,230-231`, `download_90day:62-67`, `reconcile_90day:34-36`, `gtm_scope:56-60`, `embed_90day:45-46`, `marking:70-75`, `labor:2215` | **NO** — all are `os.environ.get("SAM90_X", <default>)` with defaults; **none exist in Doppler `core-x/prd`** (only `SAM_API_KEY`). | **PURE-CODE** rename (e.g. `SAM90_`→`SAM_ATTACH_`). No Doppler change. Optional/cosmetic. |
| tmp/log paths | `extract:81-82` (`/tmp/sam_90day_extract*`), `download_90day:66-67` (`/tmp/sam_90day_*`) | **NO** — ephemeral `/tmp`. | PURE-CODE, optional. |
| ops table names in code | `extract:1894/1960`, `download_90day:90/434`, `gtm_scope:257` | Tables already de-suffixed in prod & in the SQL `CREATE`; INSERTs already target the renamed names. | No change needed except stale docstring `gtm_scope_90day.py:36`. |
| ops **index** names | the 3 SQL files | Cosmetic in prod (not resolution keys). | PURE-CODE if cleaned, but renaming a live index = supervised DDL (`ALTER INDEX … RENAME`). Low value. |

### C. The ops-Postgres live dimension (verified read-only against prod)

- **Tables affected:** `ops.sam_extraction_runs`, `ops.sam_attachment_download_runs`, `ops.sam_attachment_gtm_scope_runs` — all already renamed off `_90day` in prod by #542; **no `_90day` physical table remains**.
- **Current mixed state is internal:** table names de-suffixed, **index names still `_90day_runs_*`** in both DDL and prod (`sam_extraction_90day_runs_*_idx`, `sam_attachment_gtm_scope_90day_runs_*_idx`).
- **Latent footgun (not a rename blocker, flag it):** `ops.sam_attachment_download_runs` is **missing both secondary indexes in prod** (pkey only). Two divergent DDLs claim that one table name — the 90-day schema won; the legacy schema survives as `ops.sam_attachment_download_runs_legacy_pre90day`. The legacy worker `sam_attachment_download.py` still does `CREATE … IF NOT EXISTS ops.sam_attachment_download_runs` against columns it no longer matches — a quiet break if it ever runs again.
- **Migration needed to fully de-suffix the SQL layer:** renaming the *files* is pure-code; renaming the live **indexes** (`ALTER INDEX ops.sam_extraction_90day_runs_run_id_idx RENAME TO …`) is supervised DDL with near-zero payoff. **Recommendation: rename the SQL files + the `CREATE INDEX` statements in code, leave live indexes alone** (or fold an index-rename into the same supervised window only if the operator wants zero residue).

### D. Modal redeploy needs

**NONE from file renames.** Verified: **no `_90day` module declares `modal.App`/`Stub`.** They are CLI scripts (`python -m …`, all have `__main__`). The only Modal app in the family is `sam_attachment_embed_modal.py` → `modal.App("govcon-embed")` — **name hardcoded, not derived from filename**, and that file has **no `_90day`** in its name. Renaming `sam_attachment_embed_90day.py` does not touch the deployed Modal app. No cron/yaml/toml/Procfile references any `_90day` script path (verified).

### E. gtm-mcp (deployed Render gateway)

**Not runtime-coupled to module names.** Runtime `DATASET = "govcon_scope_vectors"` is already de-suffixed (`apps/gtm_mcp/src/tools/govcon.py:40`, flipped in #542; redeploy `dep-d8qpju3bc2fs73e6bm70` live). Remaining `_90day` references are **docstrings/comments only**: `govcon.py:6,173`, `embeddings.py:6`, `requirements.txt:15` (all mention `sam_attachment_extract_90day.py` / `govcon_scope_vectors_90day` in prose). **No redeploy required** for a module rename; update the prose for accuracy.

### F. Docs

| Doc | Type | Action |
|---|---|---|
| `docs/plans/SAM_GOVCON_90DAY_RENAME_MIGRATION.md` | **historical RECORD** (the migration itself) | **KEEP** name + content. |
| `docs/reference/SAM_ATTACHMENT_90DAY_HARVEST_AND_FORENSIC_RECORD.md` | **historical RECORD** (forensic) | **KEEP**. |
| `docs/reference/SAM_90DAY_EXTRACTION_PIPELINE_SPEC_ADVERSARIAL_REVIEW.md` | historical review artifact | **KEEP**. |
| `docs/reference/SAM_90DAY_EXTRACTION_PIPELINE_SPEC.md`, `…_V2.md` | describe **current** behavior; cited by `govcon_gtm_schemas`/gtm-mcp | RENAME (drop `90DAY`) + update body, or add the WINDOW-AS-DATA note. `_V2` is the live spec. |
| `docs/reference/SAM_90DAY_FILENAME_TAXONOMY_SIZING.md` | describes current filename taxonomy | RENAME + reconcile with this audit. |
| `docs/reference/GOVCON_90DAY_TRIGGER_DIAGNOSTIC.md`, `docs/usaspending_90day_diagnostic.md` | diagnostics (point-in-time) | KEEP as records, or RENAME if treated as living runbooks (operator call). |

---

## CONCRETE SAFE-DROP PLAN (ordered; each step tagged PURE-CODE or SUPERVISED)

> The migration precedent (`SAM_GOVCON_90DAY_RENAME_MIGRATION.md:68-69`) explicitly defers module-file renames as "do it last, separately, if at all." This plan honors that: collisions first, leaf renames next, identifier/doc hygiene last, and the live ops index/FEED layer is **deliberately left out of the pure-code track**.

**Verification gates used below:**
- **G-pytest:** `cd <repo> && doppler run -p core-x -c prd -- python -m pytest pipelines/sam_gov/tests -q` (10 test files; the `extract`/`labor`/`marking` ones import the renamed modules).
- **G-import:** smoke every renamed module: `python -c "import pipelines.sam_gov.<name>"` for each, plus `python -c "import pipelines.serving.materialize_sub_targeting"` and `python pipelines/usaspending/govcon_teaming_edges_90day.py verify` (becomes the new name).
- **G-grep:** `grep -rn "_90day" pipelines/ apps/ scripts/ --include='*.py'` returns only intended residue (FEED values / docstrings deliberately kept), and no `from … import …_90day` survives.

### Step 0 — DECIDE the collisions (SUPERVISED, blocking)
Confirm the verified finding: for `download` and `reconcile`, the **`_90day` file is canonical**, the suffix-free file is **legacy/superseded**. The legacy `sam_attachment_download.py` writes the schema preserved as `ops.sam_attachment_download_runs_legacy_pre90day`; the legacy `sam_attachment_reconcile.py` (untouched since #275) reconciles the old harvest. Operator confirms whether the legacy pair is (a) **dead → archive/delete** or (b) **still a needed general/local runner**. Recommendation below assumes (a).

### Step 1 — Resolve collisions: retire the legacy suffix-free pair (SUPERVISED)
- Move `pipelines/sam_gov/sam_attachment_download.py` and `sam_attachment_reconcile.py` to `pipelines/sam_gov/_legacy/` (or delete) — they have **zero importers**, so removal is import-safe.
- Also relocate the legacy DDL `pipelines/sam_gov/ops_sam_attachment_download_runs.sql` (the divergent `worklist_tier` schema) with them, to kill the two-DDLs-one-table footgun.
- **Gate:** G-grep confirms nothing imports the removed files; G-pytest green.

### Step 2 — Rename the two now-freed canonical modules (PURE-CODE)
- `git mv sam_attachment_download_90day.py sam_attachment_download.py`
- `git mv sam_attachment_reconcile_90day.py sam_attachment_reconcile.py`
- Fix the self-references in their docstrings/`pgrep` examples and `download_90day.py:5`→manifest reference. Zero import edits (no importers).
- **Gate:** G-import on both; G-grep.

### Step 3 — Rename the extract hub + its importers ATOMICALLY (PURE-CODE, one commit)
- `git mv sam_attachment_extract_90day.py sam_attachment_extract.py`.
- Update all 11 importers from §A + the 5 test imports in the SAME commit (`from pipelines.sam_gov.sam_attachment_extract import …`).
- **Gate:** G-import (incl. `pipelines.serving.materialize_sub_targeting`), G-pytest. Do not split this across commits — partial state breaks every consumer.

### Step 4 — Rename remaining leaf modules (PURE-CODE)
- `git mv` each: `embed_90day→embed`, `gtm_scope_90day→gtm_scope`, `labor_demand_extract_90day→labor_demand_extract`, `marking_fullbody_90day→marking_fullbody`, `opps_attachment_manifest_90day_winners→opps_attachment_manifest_winners`, `usaspending/govcon_teaming_edges_90day→govcon_teaming_edges`.
- Update: the 3 `labor`/`marking` test imports; the **dynamic** `importlib.import_module("sam_labor_demand_extract_90day")` at `scripts/archive/bigthree_reextract_census_probe.py:29`; `scripts/archive/sam_attachment_size_probe.py:62`; `scripts/v2_labor_regression_guard.py:4`; cross-references inside `embed→extract`, `labor→extract`, `marking→extract` (already handled if Step 3 done first — do Step 3 before 4).
- **Gate:** G-pytest, G-import, G-grep.

### Step 5 — Identifier hygiene (PURE-CODE, optional, low-risk)
- `SAM90_*` env names → e.g. `SAM_ATTACH_*` (defaults-only, no Doppler). `/tmp/sam_90day_*` paths. Stale docstring `gtm_scope.py:36`. gtm-mcp prose (`govcon.py:6,173`, `embeddings.py:6`, `requirements.txt:15`).
- **Do NOT** touch `FEED` values or `EXTRACTOR_TAG` here — see Step 7.
- **Gate:** G-grep; gtm-mcp not redeployed (prose only).

### Step 6 — Rename the ops SQL files + `CREATE INDEX` statements (PURE-CODE for files; index DDL is supervised)
- `git mv ops_sam_*_90day_runs.sql ops_sam_*_runs.sql`; drop `_90day` from the `CREATE INDEX … _90day_runs_*` statements in the files so fresh creates are clean.
- **Live index rename is SUPERVISED and OPTIONAL** (`ALTER INDEX … RENAME`) — near-zero payoff; recommend leaving prod indexes as-is unless the operator wants zero residue. **Also** (separate, real): create the two missing secondary indexes on `ops.sam_attachment_download_runs` if that ledger is meant to have them.
- **Gate:** SQL files load; G-grep.

### Step 7 — FEED / EXTRACTOR_TAG values (SUPERVISED, data-coordinated — CANNOT be a blind code edit)
The 4 `FEED` values still carrying `_90day` (`extract`, `labor`, `marking`, `download`) and `marking`'s `EXTRACTOR_TAG` are **written into live ledger rows / provenance**. Changing them splits `feed`/lineage history. Stage exactly as #542 staged the dataset/ledger rename: decide whether to (a) leave FEED values frozen (file renamed, FEED string unchanged — fully safe, mild inconsistency) or (b) migrate the historical `feed` column values under supervision. **Recommendation: (a) — rename files, keep FEED strings.** A FEED value is a historical key, not a name; the WINDOW-AS-DATA decision was about *dataset/module names*, not ledger keys.

### Step 8 — Docs (PURE-CODE)
KEEP the records (§F: `RENAME_MIGRATION`, `HARVEST_AND_FORENSIC_RECORD`, `ADVERSARIAL_REVIEW`). RENAME the living specs (`SPEC`, `SPEC_V2`, `FILENAME_TAXONOMY_SIZING`) and update bodies + the gtm-mcp prose pointers. Diagnostics: operator's call (record vs runbook).

---

## RECOMMENDATIONS

1. **Collisions (`download`, `reconcile`):** the `_90day` files are **canonical/current**; the suffix-free files are **legacy/superseded** (verified: legacy schema lives as `ops.sam_attachment_download_runs_legacy_pre90day`; suffix-free `reconcile.py` untouched since #275). **Do NOT merge or pick a distinguishing suffix — retire (archive/delete) the legacy suffix-free pair (zero importers), then plainly rename the `_90day` canonical pair into the freed names.** A "non-90day distinguishing suffix" would be the wrong fix: there is no live general/local twin worth keeping — the legacy files are dead.

2. **Is `_90day` meaningful anywhere? No.** Every module is the cumulative/superseding generation; `90day` is a stale rolling-window claim the data no longer honors (the explicit basis for the WINDOW-AS-DATA decision and #542). `teaming_edges_90day` is the most misleading (5-yr substrate). The only places `_90day` legitimately persists are **historical record docs** and **frozen FEED/provenance values** (keep — they are keys/history, not names).

3. **Sequence that matters:** Step 0/1 (resolve collisions) → Step 3 (extract hub atomic) **before** Step 4 (leaves that import it). Steps 2-6, 8 are pure-code and self-verifiable (auto-mergeable). **Step 7 (FEED values) and any live ops index DDL are the only items that cannot be safely dropped blind** — leave FEED strings frozen unless the operator authorizes a supervised ledger-value migration.

**Cannot be safely dropped blind:** (a) `FEED`/`EXTRACTOR_TAG` values written to live `ops.*` ledgers; (b) live ops index renames (`ALTER INDEX`); (c) the collision resolution requires the operator to confirm the legacy pair is dead. Everything else (8 file renames, ~20 import edits, env/tmp/docstring/SQL-file hygiene, doc renames) is pure-code, test-gated, and reversible.
