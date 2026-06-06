# GLEIF L1 — State of Play (Independently Verifiable)

## 1. Purpose & how to use this report

This report is written for a downstream AI agent that has **no access** to the
conversation or session that produced it. It is **fact-first**: every factual claim is
paired with (a) the exact read-only reproduction command and (b) the raw output observed
when that command was run in this pass. Re-run any command to confirm. No claim below
depends on trusting a prior narrative — each was independently re-derived by re-running
read-only Lance probes and re-reading the source on disk.

- **Section 5 (VERIFIED FACTS)** contains only measurements + citations + a one-sentence
  verdict (CONFIRMED / REFUTED / REFRAMED). No judgment adjectives.
- **Section 6 (OPEN DECISIONS)** contains the only interpretation in this document, phrased
  as neutral, evidence-anchored questions for you (the assessor) to decide. This report does
  **not** recommend a course of action.
- **Section 7 (Appendix)** pastes the exact probe scripts so you can re-run them verbatim.

The data plane and all source code were treated as **strictly read-only** for this report.
The only mutation performed was writing this file.

### State anchor (instead of a wall-clock timestamp)

Wall-clock time is not observable from the probe harness, so do **not** rely on any date in
this document as "now." The authoritative state anchor for the live dataset is its Lance
version:

> **Live `s3://data-sink/active/gleif_l1_entities/` was at `ds.version == 250` when probed
> in this pass.** Every "live" fact below is as-of v250. If you re-run and observe a higher
> version, the daily cron has run again (see VERIFIED FACT #3) and the state may have moved.

---

## 2. Reproduction environment

| Component | Value |
|---|---|
| Repo worktree (cwd) | `/Users/benjamincrane/core-x/.claude/worktrees/friendly-volhard-c9fb68` |
| Git branch (worktree) | `claude/gleif-l1-hardening` |
| Read-only Lance probe venv | `/tmp/gleif_probe_venv/bin/python` (symlink → `python@3.13`) |
| Secrets injection | `doppler run --project core-x --config prd -- …` |
| `REPO_ROOT` env | set to `$PWD` so `from core.name_norm import name_norm` resolves |
| Live dataset URI | `s3://data-sink/active/gleif_l1_entities/` |
| Lance lib | `lance 7.0.0` (a.k.a. `pylance`) |
| DuckDB lib | `duckdb 1.5.3` |

**Canonical probe invocation** (used for every Lance read in this report):

```bash
REPO_ROOT="$PWD" doppler run --project core-x --config prd -- \
  /tmp/gleif_probe_venv/bin/python /tmp/<script>.py
```

**Storage options inside Python** (R2, sourced from the Doppler-injected env):

```python
import lance, os
so = {'aws_access_key_id': os.environ['R2_ACCESS_KEY_ID'],
      'aws_secret_access_key': os.environ['R2_SECRET_ACCESS_KEY'],
      'endpoint': os.environ['R2_ENDPOINT'], 'region': 'auto'}
ds = lance.dataset('s3://data-sink/active/gleif_l1_entities/', storage_options=so)
# methods used: ds.version, ds.count_rows(), len(ds.get_fragments()), ds.schema.names,
# ds.list_indices(), ds.stats.index_stats(name), ds.scanner(...).to_table()/to_reader(),
# ds.count_rows(filter=...), ds.scanner(...).explain_plan(True)
# time-travel: lance.dataset(uri, version=N, storage_options=so)
```

The canonical normalization macro is `from core.name_norm import name_norm`; it returns a
**DuckDB SQL string** (not a Python function applied row-wise). DuckDB in the same venv
applies it over a Lance scanner.

The three probe scripts used are pasted in **Section 7**:
`/tmp/gleif_sop_live.py`, `/tmp/gleif_sop_examples_tt.py`, `/tmp/gleif_sop_boundary.py`.
Two pre-existing scripts were also read and partially reused:
`/tmp/gleif_redteam_verify.py`, `/tmp/gleif_verify.py` (the latter is the prior session's
verification harness, analyzed in VERIFIED FACT #6).

---

## 3. The directive that was issued (verbatim)

> SYSTEM DIRECTIVE: GLEIF SCHEMA HARDENING, L1 WIDENING & DIRECTIVE-29 EXCEPTION. OBJECTIVE: achieve sub-50ms point lookups for global corporate entities by materializing normalized keys at write-time, expanding the index manifest, and recovering the dropped geographic data (postalCode + headquartersAddress). AUTHORIZATION: explicit override of Directive-29 isolation — materialize/store/index a normalized legal-name column directly on active GLEIF L1. EXECUTION MANDATE: (1) L1 projection widening / geo recovery in _extract_l1/_l1_schema; (2) write-time materialization of legal_name_norm via core.name_norm before commit; (3) index-spine expansion: BTREE on legal_name_norm (do not alter lei/parent_lei). VERIFICATION/DELIVERABLE: analyze_plan post-ingest proving WHERE legal_name_norm = 'CRH PUBLIC LIMITED COMPANY' emits a ScalarIndexQuery node, reads only matched rows, and executes under 50ms.

---

## 4. What was shipped (code) — git-verifiable

PR **#180** (`feat(gleif): L1 write-time legal_name_norm + geo recovery + BTREE spine
(Directive-29 override)`) is **MERGED** into `main`.

```bash
gh pr view 180 --json number,title,state,mergedAt,mergeCommit,baseRefName,headRefName,mergedBy
```
```json
{"baseRefName":"main","headRefName":"claude/gleif-l1-hardening",
 "mergeCommit":{"oid":"b1c53791ceeca297a0ac0c2bdfcc3a16a37e24de"},
 "mergedAt":"2026-06-06T03:34:48Z","mergedBy":{"login":"bencrane","name":"Ben"},
 "number":180,"state":"MERGED",
 "title":"feat(gleif): L1 write-time legal_name_norm + geo recovery + BTREE spine (Directive-29 override)"}
```

The merge commit `b1c5379` **is** an ancestor of `main`, and the merged
`pipelines/gleif/ingest.py` on `main` is **byte-identical** to the worktree `HEAD` copy:

```bash
git merge-base --is-ancestor b1c53791ceeca297a0ac0c2bdfcc3a16a37e24de main && echo "b1c5379 IS ancestor of main"
git show HEAD:pipelines/gleif/ingest.py  | md5      # 4ef4386b590c85be9aca54e7b4efeb99
git show main:pipelines/gleif/ingest.py | md5      # 4ef4386b590c85be9aca54e7b4efeb99
git diff main HEAD -- pipelines/gleif/ingest.py | wc -l   # 0
```
```
b1c5379 IS ancestor of main
4ef4386b590c85be9aca54e7b4efeb99
4ef4386b590c85be9aca54e7b4efeb99
       0
```

(Note: the worktree tip `e61396d` is itself **not** an ancestor of `main` — the squash
merge produced `b1c5379` instead — but the file contents are identical, so the hardening
source is present on `main`. This is a commit-identity artifact of squash-merge, not a
content gap.)

The four mandated code changes, with `file:line` in
`pipelines/gleif/ingest.py` (`main` == `HEAD`, identical):

| # | Change | Citation (line) | Verbatim |
|---|---|---|---|
| (a) | Import canonical macro + ship it into the Modal image | L70, L127 | `from core.name_norm import name_norm` / `.add_local_python_source("core.name_norm")` |
| (b) | Geo recovery in `_extract_l1` + `_l1_schema` | L159, L164, L196, L201 | extracts/declares `legal_address_postal_code`, `headquarters_address_postal_code` (+ HQ first_line/city/region/country) |
| (c) | Write-time `legal_name_norm` derive before commit | L237 | `"derive_sql": {"legal_name_norm": name_norm("legal_name")}` (applied per-batch in `_build_table`, L336–L364, via `SELECT * REPLACE (...)`) |
| (d) | Index-spine expansion (lei untouched) | L240, L250 | L1 `"btree": ["lei", "legal_name_norm"]`; L2 `"btree": ["lei", "parent_lei"]` |
| (e) | Fragment compaction before indexing | L443 | `def _compact_fragments(cfg, so)` → `ds.optimize.compact_files()`, called at L591 |

**A git merge does not deploy a Modal app.** No `modal deploy` was run for this code as part
of this report, and the deployed `gleif-pipelines` app's creation date predates the merge
(see VERIFIED FACT #3). The hardening source is on `main`; the running Modal app is not
proven to contain it.

---

## 5. VERIFIED FACTS

### FACT #1 — Live dataset state

**Claim (prior):** live ≈ v250, 34 fragments, 11 columns, only `lei_idx`, and NO
`legal_name_norm` / `legal_address_postal_code` / `headquarters_address_*`.

**Reproduction:** `/tmp/gleif_sop_live.py` (Section 7), block "CLAIM 1".

**Raw output:**
```
version    = 250
count_rows = 3332281
fragments  = 34
n_columns  = 11
columns    = ['lei', 'legal_name', 'legal_address_city', 'legal_address_region',
 'legal_address_country', 'registration_authority_id', 'registration_authority_entity_id',
 'entity_status', 'source_file', 'publish_date', 'ingested_at']
--- index manifest ---
index names: ['lei_idx']
   lei_idx                  type=BTree indexed=3332281 unindexed=0 (total_rows=3332281)
--- presence checks (prior-claim columns) ---
   present? legal_name_norm                    False
   present? legal_address_postal_code          False
   present? headquarters_address_postal_code   False
   present? headquarters_address_city          False
   present? headquarters_address_country       False
   present? registration_status                False
   present? entity_status                      True
```

**Verdict:** **CONFIRMED.** Live `gleif_l1_entities` is v250, 3,332,281 rows, 34 fragments,
11 columns, single index `lei_idx` (BTree, fully trained: indexed=3,332,281 unindexed=0);
`legal_name_norm` and all geo columns are absent.

---

### FACT #2 — A hardened wide-schema version existed, then was reverted

**Claim (prior):** versions ~176–215 had 18 columns incl. `legal_name_norm` + geo + a
`legal_name_norm_idx`; version ~216+ reverted to 11 columns.

**Reproduction:** `/tmp/gleif_sop_examples_tt.py` (block "CLAIM 2") for the ladder;
`/tmp/gleif_sop_boundary.py` (block "exact revert boundary") to pin the edge. Time-travel
via `lance.dataset(URI, version=v, storage_options=so)`.

**Raw output (version ladder — ncols / has legal_name_norm / has postal_code / indices):**
```
  v   | ncols | legal_name_norm | postal_code | indices
  170 | 11    | False           | False       | []
  175 | 11    | False           | False       | ['lei_idx']
  176 | 18    | True            | True        | []
  180 | 18    | True            | True        | []
  190 | 18    | True            | True        | []
  200 | 18    | True            | True        | []
  210 | 18    | True            | True        | ['lei_idx']
  215 | 18    | True            | True        | ['lei_idx', 'legal_name_norm_idx']
  216 | 11    | False           | False       | []
  220 | 11    | False           | False       | []
  230 | 11    | False           | False       | []
  240 | 11    | False           | False       | []
  249 | 11    | False           | False       | []
  250 | 11    | False           | False       | ['lei_idx']
```

**Raw output (exact boundary):**
```
  v213: ncols=18
  v214: ncols=18
  v215: ncols=18
  v216: ncols=11
  v217: ncols=11
  v218: ncols=11
```

**Raw output (v215 full hardened schema + indices):**
```
v215: ncols=18 cols=['lei', 'legal_name', 'legal_name_norm', 'legal_address_city',
 'legal_address_region', 'legal_address_country', 'legal_address_postal_code',
 'headquarters_address_first_line', 'headquarters_address_city', 'headquarters_address_region',
 'headquarters_address_country', 'headquarters_address_postal_code', 'registration_authority_id',
 'registration_authority_entity_id', 'entity_status', 'source_file', 'publish_date', 'ingested_at']
indices: ['lei_idx', 'legal_name_norm_idx']
```

**Provenance check (was the revert a NEW snapshot or a re-run of OLD code on the SAME data?):**
`/tmp/gleif_sop_boundary.py` block "publish_date / source_file provenance":
```
  v215: publish_date='2026-06-06 00:00:00' source_file='20260606-0000-gleif-goldencopy-lei2-golden-copy.xml.zip' rows=3,332,281 frags=6
  v250: publish_date='2026-06-06 00:00:00' source_file='20260606-0000-gleif-goldencopy-lei2-golden-copy.xml.zip' rows=3,332,281 frags=34
  (live v250 distinct publish_date) publish_date='2026-06-06 00:00:00' rows=3,332,281
```

**Verdict:** **CONFIRMED, with the boundary pinned precisely.** The hardened 18-column
schema (with `legal_name_norm` + geo) existed at versions **176 through 215**; the
`legal_name_norm_idx` BTREE is observable at **v215**. The revert to 11 columns happened at
**exactly v216** (v215=18 cols → v216=11 cols). v215 (hardened) and v250 (live) carry the
**identical `publish_date` and `source_file`** — so the revert did **not** consume a newer
GLEIF publish; it re-wrote the same `2026-06-06` golden-copy snapshot through the
11-column extractor (and v250's 34 fragments vs v215's 6 indicate the un-compacted append
path, i.e. code without `_compact_fragments`).

---

### FACT #3 — Clobber mechanism + recurrence

**Claim (prior):** a Trigger.dev cron `0 6 * * *` UTC dispatches the DEPLOYED
`gleif-pipelines::ingest_gleif`; the deployed app predates the hardening (≈2026-06-01); the
daily run executes OLD code and overwrites the dataset every day until `modal deploy` of new
code.

**Reproduction (cron + dispatch target):** read `src/trigger/gleif_daily.ts`.
```bash
sed -n '32,46p' src/trigger/gleif_daily.ts   # (shown via Read in this pass)
```
Cited lines (`src/trigger/gleif_daily.ts`):
- L32–L33: `const APP_NAME = "gleif-pipelines";` / `const FUNCTION_NAME = "ingest_gleif";`
- L42–L45: `schedules.task({ id: "gleif-daily", cron: { pattern: "0 6 * * *", timezone: "UTC" } …`
- L37–L40: dispatches both levels `l1` (`gleif_l1_entities`) and `l2`.
- L57–L70: POSTs to `MODAL_DISPATCHER_URL` with body `{app_name: "gleif-pipelines",
  function_name: "ingest_gleif", kwargs:{level}, …}` (the Universal Dispatcher invokes the
  **deployed** Modal app by name).

**Reproduction (deployed app + creation date):**
```bash
REPO_ROOT="$PWD" doppler run --project core-x --config prd -- modal app list --json
```
**Raw output (gleif row):**
```json
{ "App ID": "ap-AmUrMShwH6lq419a6byM1n", "Description": "gleif-pipelines",
  "State": "deployed", "Tasks": "0",
  "Created at": "2026-06-01 01:51:22-04:00", "Stopped at": null }
```

**Verdict:** **CONFIRMED, with one stated limitation.** A Trigger.dev schedule
(`cron "0 6 * * *"`, `timezone UTC`) dispatches the deployed Modal app `gleif-pipelines`
function `ingest_gleif` for `level=l1` (overwriting `gleif_l1_entities`). The deployed app's
**creation** date is `2026-06-01`, which predates PR #180's merge (`2026-06-06T03:34:48Z`).
Combined with FACT #2 (v250 has the 11-column / 34-fragment shape of the old extractor on the
same `2026-06-06` snapshot), the daily run is executing pre-hardening code.
**Limitation (NOT INDEPENDENTLY VERIFIED):** `modal app list` exposes only "Created at," not
a last-deploy timestamp; this report does not have a read-only probe that proves the *exact
code revision* currently bound to `ap-AmUrMShwH6lq419a6byM1n`. The inference that the running
code is the old extractor rests on (i) the app creation date preceding the merge and (ii) the
live-dataset shape matching the old extractor (FACT #1/#2), not on a direct read of the
deployed image.

---

### FACT #4 — Merged code is correct but undeployed

**Claim (prior):** merged code on `main` contains the hardening (write-time `legal_name_norm`
derive, geo extraction, `btree=["lei","legal_name_norm"]`, `_compact_fragments`); a git merge
does not deploy a Modal app.

**Reproduction:**
```bash
grep -nE 'from core.name_norm import name_norm|add_local_python_source\("core.name_norm"\)|legal_address_postal_code|headquarters_address_postal_code|"derive_sql":|"btree": \["lei", "legal_name_norm"\]|def _compact_fragments' pipelines/gleif/ingest.py
git show main:pipelines/gleif/ingest.py | grep -n '"btree": \["lei", "legal_name_norm"\]'
```
**Raw output (worktree == main):**
```
70:from core.name_norm import name_norm
127:).add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro into the container
159:        "legal_address_postal_code": g("{*}Entity/{*}LegalAddress/{*}PostalCode"),
164:        "headquarters_address_postal_code": g("{*}Entity/{*}HeadquartersAddress/{*}PostalCode"),
196:        ("legal_address_postal_code", pa.string()),   # ZIP tiebreaker recovered (was dropped)
201:        ("headquarters_address_postal_code", pa.string()),
237:        "derive_sql": {"legal_name_norm": name_norm("legal_name")},
240:        "btree": ["lei", "legal_name_norm"],
443:def _compact_fragments(cfg: dict, so: dict) -> None:
```
(`git show main:…` returns line 240 = `"btree": ["lei", "legal_name_norm"],` — identical.)

**Live deliverable re-check on v250** (`/tmp/gleif_sop_boundary.py`, block "deliverable"):
```
WHERE legal_name_norm = 'CRH PUBLIC LIMITED COMPANY' on LIVE v250:
  ERROR (column absent on live): ValueError: Invalid user input: Schema error:
  No field named legal_name_norm. Did you mean 'legal_name'?.
```
**Same query on hardened v215:**
```
LanceRead: ... full_filter=legal_name_norm = Utf8("CRH PUBLIC LIMITED COMPANY"), refine_filter=--
  ScalarIndexQuery: query=[legal_name_norm = CRH PUBLIC LIMITED COMPANY]@legal_name_norm_idx(BTree)
  ScalarIndexQuery present: True
  hits: 1
```

**Verdict:** **CONFIRMED.** The merged `main` code contains all four hardening changes (a–e).
The directive's deliverable query **succeeds and emits `ScalarIndexQuery` (1 hit) on the
historical hardened v215**, but **errors with "No field named legal_name_norm" on the live
v250** — i.e. the deliverable is satisfiable against the code/version that hardened the set,
but not against the live dataset as it now stands. A `git merge` did not propagate to the
running Modal app (FACT #3).

---

### FACT #5 — `core.name_norm` fitness on the GLOBAL distribution

**Claim (prior):** 135,052 dead keys (4.05%); CN 99.2%, RU 89.7%, BG 80.5%, KR 67.2%, etc.
A "dead key" = `legal_name` is non-empty but `name_norm(legal_name)` IS NULL. Computed by
applying the macro at read time over the live `legal_name` column (live has no
`legal_name_norm`).

**The macro under test** (`core/name_norm.py`, L53–L60; the strip is L57–L58):
```python
def name_norm(expr: str) -> str:
    return (
        "nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "upper(CAST(" + expr + " AS VARCHAR)),"
        " '&', ' AND ', 'g'),"
        " '[-\\x{2013}\\x{2014}]+', ' ', 'g'),"   # L57: dash / en-dash / em-dash → space
        " '[^A-Z0-9 ]+', '', 'g'),"               # L58: strip every non-[A-Z0-9 space] char
        " '\\s+', ' ', 'g')), '')"
    )
```
The `[^A-Z0-9 ]+` strip removes **all** characters outside ASCII `A–Z`, `0–9`, and space.
There is no Unicode case-fold and no NFKD/transliteration step, so any name written wholly in
a non-Latin script (Cyrillic, Han, Hangul, Greek, Arabic, Hebrew, Thai) is reduced to the
empty string and then `nullif(..., '')` → NULL.

**Reproduction:** `/tmp/gleif_sop_live.py` (block "CLAIM 5") for the counts/per-country;
`/tmp/gleif_sop_examples_tt.py` (block "CLAIM 5 examples") for verbatim raw→norm.

**Raw output (global counts):**
```
total_rows                       = 3,332,281
legal_name non-empty             = 3,332,281
DEAD KEYS (name present, norm NULL) = 135,052  (4.05% of registry, 4.05% of non-empty)
```

**Raw output (per-country dead-key rate, top 12 by dead count):**
```
   CN   dead= 106,294 /   107,129  ( 99.2%)
   JP   dead=   4,969 /    19,006  ( 26.1%)
   BG   dead=   4,790 /     5,950  ( 80.5%)
   GR   dead=   3,768 /     7,498  ( 50.3%)
   AE   dead=   3,508 /     8,914  ( 39.4%)
   HK   dead=   2,377 /    12,811  ( 18.6%)
   KR   dead=   1,819 /     2,706  ( 67.2%)
   RU   dead=   1,679 /     1,871  ( 89.7%)
   IL   dead=   1,420 /     2,403  ( 59.1%)
   TH   dead=   1,084 /     2,365  ( 45.8%)
   TW   dead=     859 /     1,738  ( 49.4%)
   SA   dead=     422 /     5,824  (  7.2%)
```

**Raw output (verbatim non-ASCII dead-key examples, raw → name_norm):**
```
   [RU] 'Общество с ограниченной ответственностью "МАТАДОР Аутомотив Рус"'  ->  None
   [BG] 'ЗАСТРАХОВАТЕЛНО ДРУЖЕСТВО СЪГЛАСИЕ'  ->  None
   [BG] 'ПЕРСИ'  ->  None
   [BG] 'БЕНЧМАРК ГРУП'  ->  None
   [BG] 'ЕНЕРГОВИА ЕООД'  ->  None
   [BG] 'ИЗИ АСЕТ МЕНИДЖМЪНТ'  ->  None
   [BG] 'ГрейнБГ'  ->  None
   [BG] 'СОФАРМА'  ->  None
```
**Control (rows where the macro DOES survive — Latin-script names from the same countries):**
```
   [CN] 'GE HANGWEI MEDICAL SYSTEMS CO., LTD.'  ->  'GE HANGWEI MEDICAL SYSTEMS CO LTD'
   [JP] 'GS JAPAN FIXED INCOME PLUS MOTHER FUND'  ->  'GS JAPAN FIXED INCOME PLUS MOTHER FUND'
   [BG] 'FINANSOVA KASHTA BROKOM'  ->  'FINANSOVA KASHTA BROKOM'
   [BG] 'Commercial Bank Victoria EAD'  ->  'COMMERCIAL BANK VICTORIA EAD'
```

**Verdict:** **CONFIRMED.** 135,052 rows (4.05% of the 3,332,281-row registry; all 3,332,281
rows have a non-empty `legal_name`, so 4.05% of non-empty too) have a non-empty `legal_name`
that `name_norm` maps to NULL. The rate concentrates in non-Latin-script jurisdictions
(CN 99.2%, RU 89.7%, BG 80.5%, KR 67.2%, IL 59.1%, GR 50.3%). The macro (`core/name_norm.py`
L58 `[^A-Z0-9 ]+` strip, no Unicode fold) is the mechanism: wholly non-Latin names empty out;
names carrying Latin tokens survive (control rows). All numbers match the prior claim exactly.

---

### FACT #6 — Verification-method circularity

**Claim (prior):** the prior "0 mismatches" integrity check recomputed `legal_name_norm` with
the SAME imported macro the column was written with, so by construction it cannot detect the
macro's own NULL-ing of non-ASCII names (both sides agree on NULL); the FACT #5 dead-key rows
would PASS that check.

**Reproduction (how the prior check was done):**
- `docs/reference/GLEIF_L1_HARDENING_EXECUTION.md` §3.1 (L80–L81):
  > `integrity : stored legal_name_norm vs canonical name_norm(legal_name) → mismatches = 0`
  > `(non-null 3,197,229 = 95.95%; nulls = entities w/ empty LegalName)`
- `/tmp/gleif_verify.py` L67–L81 (the prior harness), core line L73–L75:
  ```python
  from core.name_norm import name_norm
  ...
  bad = con.execute(
      f"SELECT count(*) FROM t WHERE legal_name_norm IS DISTINCT FROM {name_norm('legal_name')}"
  ).fetchone()[0]
  ```
  The stored `legal_name_norm` column was itself produced at write time by
  `name_norm("legal_name")` (`pipelines/gleif/ingest.py` L237). The check compares that stored
  value against `name_norm('legal_name')` recomputed by the **same imported function**.

**Reproduction (do the dead-key rows pass?):** `/tmp/gleif_sop_live.py` (block "CLAIM 6").
Because live v250 has no `legal_name_norm`, the probe emulates both sides of the prior check
over the dead-key population: the write-time stored value would have been
`name_norm(legal_name)` (NULL for these rows), and the recompute side is also
`name_norm(legal_name)` (NULL).

**Raw output:**
```
   dead_rows=135,052  recompute_is_NULL=135,052  (NULL IS DISTINCT FROM NULL) flagged=0
   => all dead rows: stored NULL vs recompute NULL → 0 mismatches → PASS the check.
```
(`NULL IS DISTINCT FROM NULL` evaluates to FALSE in SQL, so a NULL-vs-NULL pair is **not**
counted as a mismatch.)

**Verdict:** **CONFIRMED.** The prior integrity check (`legal_name_norm IS DISTINCT FROM
name_norm(legal_name)`, both sides from the same imported `core.name_norm`) is a
write-vs-rewrite identity by construction; it cannot surface the 135,052 dead keys because
both the stored side and the recompute side are NULL, and `NULL IS DISTINCT FROM NULL` is
FALSE. The "non-null 95.95%" figure in the doc is the same population as "100% − 4.05% dead."
This is a fact about what the check measures (write/read agreement), not about whether the key
is fit for non-Latin names (FACT #5).

---

### FACT #7 — Consumer access pattern (`crosswalk_hmda_gleif`)

**Claim (prior):** the GLEIF join is `ON g.lei = h.lei`; `normalized_legal_name` is a
**projected output** (not a join key); the consumer is a **bulk scan + join**, not a
single-row point lookup.

**Reproduction:** read `pipelines/resolution/crosswalk_hmda_gleif.py`.

**Citations:**
- Join condition — L185: `INNER JOIN gleif g ON g.lei = h.lei` (inside `build_crosswalk_sql()`).
- `normalized_legal_name` — L165: `{_name_norm('g.legal_name')} AS normalized_legal_name` —
  it is a **SELECT-list projection** computed at read time from `g.legal_name`, **not** part of
  the join `ON` clause. The HMDA→GLEIF resolution is entirely on `lei`.
- Materialization shape — L233–L249 (`_materialize`): both sources are opened with
  `lance.dataset(uri, …).scanner(columns=[…]).to_reader()` and read into DuckDB temp tables
  (`hmda_panel`, `gleif`), then `INNER JOIN … ON g.lei = h.lei` (L252 / L185). This is a
  **full-column bulk scan of both datasets + a hash/merge join**, not per-row point lookups.
- The output dataset's own indexes — L76: `BTREE_INDEXES = ["lei", "normalized_legal_name"]`
  (built on the *crosswalk output*, L387–L389), i.e. `normalized_legal_name` is indexed for
  **downstream** consumers of the crosswalk, not used to look up into GLEIF.
- Module docstring L20–L26 describes the grain: `gleif = gleif_l1_entities → 1 row/lei`,
  `crosswalk = hmda_panel INNER JOIN gleif ON lei`.

**Verdict:** **CONFIRMED.** In the live consumer `crosswalk_hmda_gleif`, GLEIF is joined to
HMDA on `g.lei = h.lei` (L185); `normalized_legal_name` is a read-time projection of
`g.legal_name` (L165), not a join key into GLEIF. The access pattern is a bulk scan of the
full GLEIF L1 dataset + a join on `lei`, not single-row `legal_name_norm` point lookups. This
consumer does not exercise the directive's "<50 ms point lookup on `legal_name_norm`" path.

---

### FACT #8 — `registration_status` vs `entity_status`

**Claim (prior):** `registration_status` is absent live; `entity_status` is present; the two
are semantically different (entity-alive vs LEI-registration lifecycle ISSUED/LAPSED/RETIRED).

**Reproduction (live schema + entity_status distribution):** `/tmp/gleif_sop_live.py`
(block "CLAIM 8").

**Raw output:**
```
   ACTIVE            3,081,893
   INACTIVE            241,454
   NULL                  8,934
   registration_status present in live schema? False
```

**Reproduction (is registration_status extracted by ANY version of the code?):**
```bash
grep -niE 'RegistrationStatus|registration_status' pipelines/gleif/ingest.py
grep -nE 'EntityStatus|entity_status' pipelines/gleif/ingest.py
```
**Raw output:**
```
(registration_status: no match — not extracted by the merged code)
167:        "entity_status": g("{*}Entity/{*}EntityStatus"),
204:        ("entity_status", pa.string()),
```

**Semantic distinction (sourced from a file):** `docs/reference/GLEIF_EPA_ENTITY_BRIDGE_DIAGNOSTIC.md`
L58–L59:
```
| `entity.status`                                 | ✅ `entity_status` | `ACTIVE`/`INACTIVE` |
| `registration.status` (ISSUED/LAPSED/RETIRED)   | 🛑 DROPPED         | LEI-registration validity (active keeps only entity status) |
```

**Verdict:** **CONFIRMED.** Live schema has `entity_status` (ACTIVE 3,081,893 / INACTIVE
241,454 / NULL 8,934) and **no** `registration_status`. The semantic difference is sourced
from a file (`GLEIF_EPA_ENTITY_BRIDGE_DIAGNOSTIC.md` L58–L59): `entity_status` =
ACTIVE/INACTIVE (is the legal entity alive); `registration.status` = ISSUED/LAPSED/RETIRED
(is the LEI registration itself still maintained) — and the latter is marked DROPPED.
**Additional verified detail:** `registration_status` is **not** extracted even by the merged
hardened code (`pipelines/gleif/ingest.py` `_extract_l1` extracts `EntityStatus` at L167 but
no `RegistrationStatus`), so this gap is present in both the live and the hardened code paths.

---

### FACT #9 — postalCode XPath soundness (counter-check on the hardened version)

**Claim (prior):** geo fill is high (≈98.7%) and high-null cases are jurisdictions that lack
postal codes (HK, IE). Spot-verify on the hardened historical version via time-travel.

**Reproduction:** `/tmp/gleif_sop_examples_tt.py` (block "CLAIM 9"); reads hardened **v215**.

**Raw output:**
```
  v215: ncols=18
  legal_address_postal_code fill      = 3,288,557 / 3,332,281 (98.69%)
  headquarters_address_postal_code fill = 3,289,158 / 3,332,281 (98.71%)
  postal fill by country (lowest fill, top 8 by count, fill<50%):
    CW   fill=  7.6%  (77/1,010)
    BS   fill= 12.2%  (649/5,308)
    BZ   fill= 16.4%  (250/1,527)
    AE   fill= 16.7%  (1,485/8,914)
    SC   fill= 20.7%  (423/2,039)
    KN   fill= 28.1%  (291/1,034)
    HK   fill= 49.6%  (6,348/12,811)
    PA   fill= 69.7%  (4,358/6,254)
```

**Verdict:** **CONFIRMED** (on the hardened v215; the columns do not exist on live v250).
On v215 the postal XPath produced 98.69% (legal) / 98.71% (HQ) fill. The lowest-fill
jurisdictions are tax-haven / no-mandatory-postal-code territories (CW Curaçao, BS Bahamas,
BZ Belize, AE United Arab Emirates, SC Seychelles, KN St Kitts & Nevis, HK Hong Kong,
PA Panama) — consistent with the claim that high-null cases are jurisdictions lacking postal
codes rather than an XPath fault. The prior claim named HK and IE specifically; HK is
confirmed at 49.6% fill; IE did not surface in the bottom-8-by-count list and was not
separately re-measured (NOT INDEPENDENTLY VERIFIED for IE specifically).

---

### Summary table

| Fact | Subject | Verdict |
|---|---|---|
| #1 | Live state (v250, 34 frags, 11 cols, lei_idx only) | **CONFIRMED** |
| #2 | Hardened v176–215 existed; reverted at exactly v216 | **CONFIRMED** (boundary pinned) |
| #3 | Trigger cron `0 6 * * *` → deployed `gleif-pipelines` (created 2026-06-01, pre-merge) | **CONFIRMED** (deployed-image code not directly read — see limitation) |
| #4 | Merged `main` code has all 4 changes; deliverable works on v215, errors on v250 | **CONFIRMED** |
| #5 | 135,052 dead keys (4.05%); CN 99.2%, RU 89.7%, BG 80.5%, KR 67.2% | **CONFIRMED** |
| #6 | Prior integrity check is write-vs-rewrite identity; dead keys pass it | **CONFIRMED** |
| #7 | Consumer joins `ON g.lei=h.lei`; `normalized_legal_name` projected, bulk scan+join | **CONFIRMED** |
| #8 | `entity_status` present (ACTIVE/INACTIVE/NULL); `registration_status` absent in live AND merged code | **CONFIRMED** |
| #9 | v215 postal fill 98.69%/98.71%; low-fill = no-postal-code jurisdictions | **CONFIRMED** (IE not separately re-measured) |

**No prior claim was REFUTED.** All nine were independently reproduced and CONFIRMED. Two
discrepancies/refinements vs the prior narrative are recorded above, neither of which
contradicts a claim: (i) FACT #2 pins the revert boundary to **exactly v215→v216** (prior
said "~216+") and adds the provenance fact that v215 and v250 share the same publish_date /
source_file (the revert re-wrote the same snapshot, it was not a newer publish); (ii) FACT #4
notes the squash-merge produced commit `b1c5379` (the worktree tip `e61396d` is not itself on
`main`), though the file content on `main` is byte-identical. No claim was REFRAMED as
interpretation; the items that are interpretation (the directive's premise, the key choice,
the deploy/sequence decision) were never asserted as fact and are isolated to Section 6.

---

## 6. OPEN DECISIONS (for the assessor)

These are neutral, evidence-anchored questions. This report takes **no position** on any of
them; it only routes the relevant VERIFIED FACTS to each.

**Q1 — Role of `legal_name_norm` given the LEI is already the deterministic global key.**
The LEI is the deterministic global entity key and is already indexed (`lei_idx`, trained,
FACT #1). The one live consumer examined (`crosswalk_hmda_gleif`) joins GLEIF **on `lei`**, not
on a normalized name, and uses `normalized_legal_name` only as a projected output column
(FACT #7). Given that: which downstream sources actually need a **name-based** bridge into
GLEIF (i.e. carry a corporate name but no LEI), versus already carrying the LEI? What is the
intended query that `legal_name_norm` + its BTREE is meant to serve, and does a live consumer
exercise it today?

**Q2 — Is an ASCII-only normalizer acceptable as the GLOBAL blocking key?**
The directive's objective is "global corporate entities." The canonical macro
(`core/name_norm.py` L58, no Unicode fold) maps 135,052 names (4.05% of the registry) to NULL,
concentrated in non-Latin jurisdictions (CN 99.2%, RU 89.7%, BG 80.5%, KR 67.2%; FACT #5). A
NULL `legal_name_norm` is unindexable and unjoinable on the name path. Is the ASCII-only key
acceptable for the intended bridge population (Q1), or does the "global" objective require
non-Latin names to resolve? Note the prior integrity check (FACT #6) does not measure this —
it is a write/read identity that the dead-key rows pass.

**Q3 — Recurrence: deploy now, or fix the key first?**
The daily Trigger cron (`0 6 * * *` UTC) dispatches the deployed `gleif-pipelines` app (created
2026-06-01, before the 2026-06-06 merge; FACT #3), and the live dataset (v250) has the
11-column / 34-fragment shape of the pre-hardening extractor on the same `2026-06-06` snapshot
(FACT #2). The merged hardening on `main` (FACT #4) is not running. Two ordering options sit on
the same axis — *deploy the current merged code now* (re-materializes `legal_name_norm` with
the ASCII-only key, accepting Q2's dead-key rate) versus *resolve Q2's key choice before any
deploy* (so the next ingest does not re-bake a key that will be changed again). Which ordering?

**Q4 — If the key is changed, where does the change live?**
`legal_name_norm` is byte-identical-by-construction to `sos_normalized_master.normalized_legal_name`
and the credit/SoS spines because all of them import `core.name_norm` (the module docstring,
`core/name_norm.py` L22–L29, lists **7** consumer pipelines: `sos_normalized/normalize.py`,
`fl_federal_tax_liens/ingest.py`, `osha/osha_sniper.py`, `resolution/recon_ca_ucc_sos.py`,
`resolution/crosswalk_hmda_gleif.py`, `resolution/crosswalk_sam_usaspending.py`,
`resolution/credit_spine_normalize_index.py`). The tradeoff axis, stated factually (no pick):
- **Fleet-wide edit to `core.name_norm`** (e.g. add NFKD/transliteration): moves all 7 consumers
  together and preserves cross-spine byte-identity, but changes every spine's blocking key in
  one step.
- **GLEIF-local normalizer** (a separate rule only for GLEIF): contains the blast radius to
  GLEIF, but breaks the cross-spine byte-identity that the shared import currently guarantees.
- **Carry GLEIF's own `transliteratedOtherNames[]`** (a GLEIF-provided Latin transliteration
  field, not currently extracted — the merged `_extract_l1` reads only `LegalName`): adds a
  GLEIF-native Latin name without touching the shared macro, at the cost of a new projected
  column and a second name surface. (Whether that field is populated in the golden copy is
  **NOT INDEPENDENTLY VERIFIED** in this pass.)

Which locus, given Q1's answer about who actually needs the name bridge?

**Q5 — `registration_status` precision gap.**
The live set (and the merged hardened code) carry `entity_status` = ACTIVE/INACTIVE only, and
**not** `registration_status` = ISSUED/LAPSED/RETIRED (FACT #8;
`GLEIF_EPA_ENTITY_BRIDGE_DIAGNOSTIC.md` L58–L59). For any consumer that needs to distinguish a
maintained LEI from a lapsed/retired one (LEI-registration lifecycle), `entity_status` is
insufficient. Does the intended use require `registration_status`, and if so should it be added
to `_extract_l1` (it is a one-line XPath addition, not currently present in any code version)?

---

## 7. Appendix: probe scripts (re-runnable verbatim)

All three were run with:
```bash
REPO_ROOT="$PWD" doppler run --project core-x --config prd -- \
  /tmp/gleif_probe_venv/bin/python /tmp/<script>.py
```
None of them mutate the data plane (no `write_dataset` / `create_scalar_index` /
`compact_files` / `delete` / `optimize_indices` / `add_columns`).

### 7.1 `/tmp/gleif_sop_live.py` — FACTS #1, #5, #6, #8

```python
"""GLEIF L1 STATE-OF-PLAY live probe — READ ONLY. Captures claims #1,#5,#6,#8."""
import os, sys, json
sys.path.insert(0, os.environ.get("REPO_ROOT", os.getcwd()))
from core.name_norm import name_norm
import lance, duckdb

so = {'aws_access_key_id': os.environ['R2_ACCESS_KEY_ID'],
      'aws_secret_access_key': os.environ['R2_SECRET_ACCESS_KEY'],
      'endpoint': os.environ['R2_ENDPOINT'], 'region': 'auto'}
URI = 's3://data-sink/active/gleif_l1_entities/'
ds = lance.dataset(URI, storage_options=so)

print("lance", lance.__version__, "duckdb", duckdb.__version__)
print("=" * 72); print("CLAIM 1 — LIVE DATASET STATE"); print("=" * 72)
print("version    =", ds.version)
print("count_rows =", ds.count_rows())
print("fragments  =", len(ds.get_fragments()))
print("n_columns  =", len(ds.schema.names))
print("columns    =", ds.schema.names)
print("--- full schema (name : type) ---")
for f in ds.schema:
    print(f"   {f.name:36s} {f.type}")
print("--- index manifest ---")
idxs = ds.list_indices()
print("index names:", [ix.get('name') for ix in idxs])
total = ds.count_rows()
for ix in idxs:
    name = ix.get('name')
    st = ds.stats.index_stats(name)
    recs = st if isinstance(st, list) else [st]
    for r in recs:
        print(f"   {name:24s} type={r.get('index_type')} "
              f"indexed={r.get('num_indexed_rows')} unindexed={r.get('num_unindexed_rows')} "
              f"(total_rows={total})")
print("--- presence checks (prior-claim columns) ---")
cols = ds.schema.names
for c in ("legal_name_norm", "legal_address_postal_code",
          "headquarters_address_postal_code", "headquarters_address_city",
          "headquarters_address_country", "registration_status", "entity_status"):
    print(f"   present? {c:34s} {c in cols}")

print("\n" + "=" * 72)
print("CLAIM 5 — core.name_norm fitness on the GLOBAL distribution (read-time apply)")
print("=" * 72)
con = duckdb.connect(":memory:")
con.execute("PRAGMA threads=4; SET memory_limit='8GB';")
con.register("rdr", ds.scanner(columns=["legal_name", "legal_address_country"]).to_reader())
con.execute("CREATE TABLE t AS SELECT * FROM rdr"); con.unregister("rdr")
tot, ln_nonempty, dead = con.execute(f"""
  SELECT count(*),
         count(*) FILTER (WHERE nullif(trim(legal_name),'') IS NOT NULL),
         count(*) FILTER (WHERE nullif(trim(legal_name),'') IS NOT NULL AND {name_norm('legal_name')} IS NULL)
  FROM t""").fetchone()
print(f"total_rows                       = {tot:,}")
print(f"legal_name non-empty             = {ln_nonempty:,}")
print(f"DEAD KEYS (name present, norm NULL) = {dead:,}  ({100*dead/tot:.2f}% of registry, "
      f"{100*dead/ln_nonempty:.2f}% of non-empty)")
print("\nper-country dead-key rate (top 12 by dead count):")
rows = con.execute(f"""
  SELECT legal_address_country AS c, count(*) AS n,
         count(*) FILTER (WHERE nullif(trim(legal_name),'') IS NOT NULL AND {name_norm('legal_name')} IS NULL) AS dead
  FROM t GROUP BY 1 HAVING dead>0 ORDER BY dead DESC LIMIT 12""").fetchall()
for c, n, d in rows:
    print(f"   {str(c):4s} dead={d:>8,} / {n:>9,}  ({100*d/n:5.1f}%)")
print("\nverbatim examples (raw -> name_norm) for non-ASCII names (limit 8):")
ex = con.execute(f"""SELECT legal_name, {name_norm('legal_name')} FROM t
  WHERE legal_name ~ '[^\\x00-\\x7F]' LIMIT 8""").fetchall()
for raw, nm in ex:
    print(f"   {repr(raw)[:46]:46s} -> {repr(nm)}")

print("\n" + "=" * 72)
print("CLAIM 6 — would dead-key rows PASS the prior integrity check?")
print("=" * 72)
demo = con.execute(f"""
  WITH d AS (SELECT legal_name, {name_norm('legal_name')} AS recomputed
             FROM t WHERE nullif(trim(legal_name),'') IS NOT NULL AND {name_norm('legal_name')} IS NULL)
  SELECT count(*) AS dead_rows,
         count(*) FILTER (WHERE recomputed IS NULL) AS recompute_null,
         count(*) FILTER (WHERE recomputed IS DISTINCT FROM recomputed) AS self_distinct
  FROM d""").fetchone()
print(f"   dead_rows={demo[0]:,}  recompute_is_NULL={demo[1]:,}  "
      f"(NULL IS DISTINCT FROM NULL) flagged={demo[2]:,}")
print("   => all dead rows: stored NULL vs recompute NULL → 0 mismatches → PASS the check.")
con.close()

print("\n" + "=" * 72); print("CLAIM 8 — entity_status value distribution (live)"); print("=" * 72)
if "entity_status" in cols:
    con = duckdb.connect(":memory:")
    con.register("rdr", ds.scanner(columns=["entity_status"]).to_reader())
    con.execute("CREATE TABLE e AS SELECT * FROM rdr"); con.unregister("rdr")
    dist = con.execute("SELECT coalesce(entity_status,'<null>'), count(*) FROM e GROUP BY 1 ORDER BY 2 DESC").fetchall()
    for v, n in dist:
        print(f"   {v:14s} {n:>12,}")
    con.close()
else:
    print("   entity_status NOT in live schema")
print("   registration_status present in live schema?", "registration_status" in cols)
print("\n=== LIVE PROBE COMPLETE (zero mutation) ===")
```

### 7.2 `/tmp/gleif_sop_examples_tt.py` — FACTS #5 (examples), #2 (ladder), #9

```python
"""Examples (non-ASCII raw->norm) + time-travel for hardened version (claims #2,#5-examples,#9). READ ONLY."""
import os, sys
sys.path.insert(0, os.environ.get("REPO_ROOT", os.getcwd()))
from core.name_norm import name_norm
import lance, duckdb

so = {'aws_access_key_id': os.environ['R2_ACCESS_KEY_ID'],
      'aws_secret_access_key': os.environ['R2_SECRET_ACCESS_KEY'],
      'endpoint': os.environ['R2_ENDPOINT'], 'region': 'auto'}
URI = 's3://data-sink/active/gleif_l1_entities/'
ds = lance.dataset(URI, storage_options=so)

print("=" * 72)
print("CLAIM 5 — verbatim non-ASCII raw -> name_norm examples (dead keys), live v", ds.version)
print("=" * 72)
con = duckdb.connect(":memory:")
con.execute("PRAGMA threads=4; SET memory_limit='8GB';")
con.register("rdr", ds.scanner(columns=["legal_name", "legal_address_country"],
             filter="legal_address_country IN ('CN','RU','BG','KR','GR','JP')").to_reader())
con.execute("CREATE TABLE t AS SELECT * FROM rdr"); con.unregister("rdr")
ex = con.execute(f"""
  SELECT legal_address_country, legal_name, {name_norm('legal_name')} AS norm
  FROM t
  WHERE nullif(trim(legal_name),'') IS NOT NULL AND {name_norm('legal_name')} IS NULL
  LIMIT 8""").fetchall()
for c, raw, nm in ex:
    print(f"   [{c}] {raw!r}  ->  {nm!r}")
print("\n   (control) a few rows where norm SURVIVES (mixed-script with Latin tokens):")
ex2 = con.execute(f"""
  SELECT legal_address_country, legal_name, {name_norm('legal_name')} AS norm
  FROM t WHERE {name_norm('legal_name')} IS NOT NULL LIMIT 4""").fetchall()
for c, raw, nm in ex2:
    print(f"   [{c}] {raw!r}  ->  {nm!r}")
con.close()

print("\n" + "=" * 72)
print("CLAIM 2 — time-travel: locate a hardened wide-schema version + the revert boundary")
print("=" * 72)
print("latest version =", ds.version)
def probe(v):
    try:
        d = lance.dataset(URI, version=v, storage_options=so)
        cols = d.schema.names
        idx = [ix.get('name') for ix in d.list_indices()]
        return (v, len(cols), 'legal_name_norm' in cols, 'legal_address_postal_code' in cols, idx)
    except Exception as e:
        return (v, f"ERR:{type(e).__name__}:{str(e)[:60]}", None, None, None)
ladder = [170, 175, 176, 180, 190, 200, 210, 215, 216, 220, 230, 240, 249, 250]
print("  v   | ncols | legal_name_norm | postal_code | indices")
for v in ladder:
    r = probe(v)
    if isinstance(r[1], int):
        print(f"  {r[0]:<4}| {r[1]:<6}| {str(r[2]):<16}| {str(r[3]):<12}| {r[4]}")
    else:
        print(f"  {r[0]:<4}| {r[1]}")

print("\n" + "=" * 72)
print("CLAIM 9 — postal fill on a hardened historical version (if one exists)")
print("=" * 72)
hardened_v = None
for v in [215, 214, 213, 210, 205, 200, 195, 190, 185, 180, 176, 175]:
    r = probe(v)
    if isinstance(r[1], int) and r[3]:
        hardened_v = v; break
print("first hardened version found scanning down from 215:", hardened_v)
if hardened_v:
    dh = lance.dataset(URI, version=hardened_v, storage_options=so)
    print(f"  v{hardened_v}: ncols={len(dh.schema.names)} cols={dh.schema.names}")
    print(f"  indices: {[ix.get('name') for ix in dh.list_indices()]}")
    tot = dh.count_rows()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4; SET memory_limit='8GB';")
    con.register("rdr", dh.scanner(columns=[
        "legal_address_postal_code", "headquarters_address_postal_code",
        "legal_address_country"]).to_reader())
    con.execute("CREATE TABLE g AS SELECT * FROM rdr"); con.unregister("rdr")
    lpc, hpc = con.execute(
        "SELECT count(legal_address_postal_code), count(headquarters_address_postal_code) FROM g").fetchone()
    print(f"  legal_address_postal_code fill      = {lpc:,} / {tot:,} ({100*lpc/tot:.2f}%)")
    print(f"  headquarters_address_postal_code fill = {hpc:,} / {tot:,} ({100*hpc/tot:.2f}%)")
    print("  postal fill by country (lowest fill, top 8 by count where >500):")
    rows = con.execute("""
      SELECT legal_address_country c, count(*) n, count(legal_address_postal_code) filled
      FROM g GROUP BY 1 HAVING count(*)>500
      ORDER BY (count(legal_address_postal_code)*1.0/count(*)) ASC LIMIT 8""").fetchall()
    for c, n, fld in rows:
        print(f"    {str(c):4s} fill={100*fld/n:5.1f}%  ({fld:,}/{n:,})")
    con.close()
else:
    print("  no hardened (postal-bearing) version found in the probed ladder.")
print("\n=== TIME-TRAVEL PROBE COMPLETE (zero mutation) ===")
```

### 7.3 `/tmp/gleif_sop_boundary.py` — FACTS #2 (boundary + provenance), #4 (live deliverable)

```python
"""Pin the exact revert boundary + publish_date provenance + live deliverable check. READ ONLY."""
import os, sys
sys.path.insert(0, os.environ.get("REPO_ROOT", os.getcwd()))
import lance, duckdb

so = {'aws_access_key_id': os.environ['R2_ACCESS_KEY_ID'],
      'aws_secret_access_key': os.environ['R2_SECRET_ACCESS_KEY'],
      'endpoint': os.environ['R2_ENDPOINT'], 'region': 'auto'}
URI = 's3://data-sink/active/gleif_l1_entities/'

def ncols(v):
    d = lance.dataset(URI, version=v, storage_options=so)
    return len(d.schema.names)

print("=== exact revert boundary (ncols per version 213..218) ===")
for v in [213, 214, 215, 216, 217, 218]:
    try: print(f"  v{v}: ncols={ncols(v)}")
    except Exception as e: print(f"  v{v}: ERR {e}")

print("\n=== publish_date / source_file provenance: v215 (hardened) vs v250 (live) ===")
for v in (215, 250):
    d = lance.dataset(URI, version=v, storage_options=so)
    con = duckdb.connect(":memory:")
    con.register("rdr", d.scanner(columns=["publish_date", "source_file"], limit=1).to_reader())
    row = con.execute("SELECT publish_date, source_file FROM rdr").fetchone()
    con.close()
    print(f"  v{v}: publish_date={row[0]!r}  source_file={row[1]!r}  rows={d.count_rows():,}  frags={len(d.get_fragments())}")

print("\n=== distinct publish_date present in LIVE v250 ===")
d = lance.dataset(URI, storage_options=so)
con = duckdb.connect(":memory:")
con.register("rdr", d.scanner(columns=["publish_date"]).to_reader())
con.execute("CREATE TABLE p AS SELECT * FROM rdr"); con.unregister("rdr")
for pd, n in con.execute("SELECT publish_date, count(*) FROM p GROUP BY 1 ORDER BY 2 DESC").fetchall():
    print(f"   publish_date={pd!r}  rows={n:,}")
con.close()

print("\n=== deliverable: WHERE legal_name_norm=... on LIVE v250 ===")
try:
    plan = d.scanner(columns=["lei"], filter="legal_name_norm = 'CRH PUBLIC LIMITED COMPANY'").explain_plan(True)
    print(plan)
except Exception as e:
    print(f"  ERROR (column absent on live): {type(e).__name__}: {str(e)[:200]}")

print("\n=== contrast: same query on hardened v215 ===")
d215 = lance.dataset(URI, version=215, storage_options=so)
try:
    plan = d215.scanner(columns=["lei"], filter="legal_name_norm = 'CRH PUBLIC LIMITED COMPANY'").explain_plan(True)
    print(plan)
    print("  ScalarIndexQuery present:", "ScalarIndexQuery" in plan)
    print("  hits:", d215.count_rows(filter="legal_name_norm = 'CRH PUBLIC LIMITED COMPANY'"))
except Exception as e:
    print(f"  ERR: {e}")
print("\n=== BOUNDARY PROBE COMPLETE (zero mutation) ===")
```

### 7.4 Git / Modal verification commands (read-only)

```bash
# PR #180 merge state
gh pr view 180 --json number,title,state,mergedAt,mergeCommit,baseRefName,headRefName,mergedBy

# Is the hardening on main? (byte-identical to worktree HEAD)
git merge-base --is-ancestor b1c53791ceeca297a0ac0c2bdfcc3a16a37e24de main && echo "on main"
git show HEAD:pipelines/gleif/ingest.py  | md5
git show main:pipelines/gleif/ingest.py | md5
git diff main HEAD -- pipelines/gleif/ingest.py | wc -l

# Four shipped changes (file:line)
grep -nE 'from core.name_norm import name_norm|add_local_python_source\("core.name_norm"\)|legal_address_postal_code|headquarters_address_postal_code|"derive_sql":|"btree": \["lei", "legal_name_norm"\]|def _compact_fragments' pipelines/gleif/ingest.py

# Consumer join + projection
grep -nE 'INNER JOIN gleif g ON|AS normalized_legal_name|BTREE_INDEXES =' pipelines/resolution/crosswalk_hmda_gleif.py

# registration_status not extracted by any code version; entity_status is
grep -niE 'RegistrationStatus|registration_status' pipelines/gleif/ingest.py   # (no match)
grep -nE  'EntityStatus|entity_status' pipelines/gleif/ingest.py               # L167, L204

# Deployed Modal app + creation date
REPO_ROOT="$PWD" doppler run --project core-x --config prd -- modal app list --json   # gleif-pipelines: Created 2026-06-01
```
