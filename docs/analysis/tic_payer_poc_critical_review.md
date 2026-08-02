# TiC Payer Reverse-Mapping — Adversarial Review

**Reviewed:** `docs/analysis/tic_payer_integration_poc.md` (measured 2026-06-07),
`docs/analysis/cigna_tic_toc_stream_analysis.md`, `docs/analysis/form5500_relational_diagnostic.md`,
against the implementation in `pipelines/tic_mrf/` (`reverse_map.py`, `orchestrate.py`,
`part1_filter_spine.py`, `ops_tic_reverse_map_runs.sql`, `README.md`).
**Review date:** 2026-08-01. Static analysis only; no payer endpoints touched.

## Verdict summary

| Artifact | Verdict | One-line basis |
|---|---|---|
| `tic_payer_integration_poc.md` | **Sound architecture; stale measurements; one material outcome gap** | Inversion strategies, streaming engine, and projections are real and code-backed; TIN edge and employer bridge are dropped by the code; URL/SAS/cost claims are ~8 weeks old against monthly drops. |
| `cigna_tic_toc_stream_analysis.md` | **Sound** | EIN-normalization trap, issuer-vs-employer split, and 132-file fan-in are measured, independently verified, and correctly caveated (per-entity, no-sizes). No action. |
| `form5500_relational_diagnostic.md` | **Sound** | Key topology (ACK_ID vs EIN+PN), counterparty-EIN disambiguation, and ingest order are correct and already consumed correctly by `part1_filter_spine.py`. No action. |
| `pipelines/tic_mrf/` code | **Matches the docs' described flow; 5 production defects found** | Module names, inversion strategies, and ledger shape match the POC doc. Defects: SAS-in-ledger-key, partial-append duplication, NULL-file_version dedup hole, sync client driver, dropped TIN. |

---

## Recommendations (ordered by impact)

### 1. The code discards the TIN/EIN — the single field the business outcome needs
**What.** `extract_rates` (`reverse_map.py:327-376`) resolves `provider_references` but emits
only the NPI; `tin.type`, `tin.value`, and `business_name` (present in every provider_group,
shown verbatim in the POC doc Part 3.1) are thrown away. `build_provider_spine`
(`reverse_map.py:286-324`) keeps only `{group_id → npi set}`.
**Why it matters.** The strategic use is payer-mix and book-of-business per *practice*.
The NPI→TIN(EIN) edge is what joins a rate row to the practice entity, to Form 5500 sponsors,
and to the wider entity graph — it is the only org-level identifier in the whole chain
(the SoR has no Type-2 NPI/EIN, per the POC's own premise-correction #1). Running the
403,179-NPI fan-out without capturing it means re-streaming 108 TB later to get it.
**Fix.** Carry `(tin_type, tin_value, tin_business_name)` through the spine map and onto each
emitted row; add them to the Lance schema and BTREE `tin_value`. Handle `tin.type == "npi"`
explicitly (CMS schema allows TIN=NPI for sole proprietors) — those values are NOT EINs and
must be excluded from any Form 5500/EIN join or they silently collide with the 9-digit EIN space.
**Where.** `reverse_map.py:295-324, 343-376`; `orchestrate.py:200-207` (index list).

### 2. UHC SAS tokens break both the worklist and the idempotency key
**What.** `_uhc_worklist` (`orchestrate.py:95-109`) stores the full `downloadUrl`
*including the SAS token* as `in_network_url`, and `process_file` uses that URL verbatim as
the ledger's `source_file_url` (`reverse_map.py:446-461`, unique key
`(payer, source_file_url, file_version)`).
**Why it matters.** Two failure modes: (a) SAS tokens expire — a 15–30 h fan-out over
7,170 files consuming a worklist built hours earlier will start 409ing mid-run, and nothing
refreshes the token; (b) every master-index re-fetch mints a new `sig`, so the same blob gets
a *different* `source_file_url` → the ledger never matches → full re-ingest (duplicate rows)
on every worklist rebuild. The POC doc's own SAS finding (0/15 blind GETs) proves the token
is load-bearing but never addresses its lifetime.
**Fix.** Ledger-key on the URL with the query string stripped (the blob path is stable);
store the SAS separately in the worklist row; have `process_file` re-resolve a fresh
`downloadUrl` from the master index (or a worklist refresh function) on 403/409.
**Where.** `orchestrate.py:107, 131-139`; `reverse_map.py:446-497`.

### 3. Partial-failure duplicates: mid-file appends are not rolled back
**What.** `process_file` (`orchestrate.py:149-156`) appends 50k-row Lance fragments *while
streaming*; on exception it records `status='error'` and returns. The already-appended
fragments stay in the SoR. Retry (ledger has no `success` row) re-streams the file and
re-appends everything.
**Why it matters.** At 7,170 UHC files with multi-GB parses, some fraction *will* fail
mid-stream (the doc's blast-radius design assumes it). Every such failure double-counts
rates for the matched NPIs — silent wrongness in the exact numbers (payer mix, rate
positioning) the product serves.
**Fix (cheapest sufficient).** Stamp every row with `file_version` (the ETag is already in
hand at `orchestrate.py:137-138`) and, before re-processing a file whose ledger shows a prior
non-success attempt, `lance` delete `WHERE source_file_url=? AND file_version=?` — or buffer
to a per-file staging dataset and append once on success. Row-level `file_version` also makes
monthly re-drops distinguishable in the fact table, which the current schema
(`reverse_map.py:363-376`) cannot do (`captured_at` is an env var that defaults to `""`).
**Where.** `orchestrate.py:144-157`; `reverse_map.py:363-376`.

### 4. NULL `file_version` disables idempotency entirely, silently
**What.** `already_ingested` returns `False` whenever `file_version` is None
(`reverse_map.py:451-452`), and the Postgres unique index treats NULLs as distinct, so
`ON CONFLICT` never fires either. Any origin that omits ETag *and* Last-Modified (the `head()`
fallback path at `reverse_map.py:163-180` makes this reachable) is re-ingested on every retry
with no skip and no upsert.
**Fix.** Fall back to `content_length`-based or date-slug-based version (`2026-06-01_…` is in
every UHC/Aetna filename), and make `file_version NOT NULL` with a sentinel; never run the
no-version path in production fan-out.
**Where.** `reverse_map.py:446-464`; `ops_tic_reverse_map_runs.sql:38-39`.

### 5. The driver is a sync client loop — the exact failure mode this repo already documented
**What.** `run` is a `@app.local_entrypoint()` that drives `process_file.map(...)`
synchronously (`orchestrate.py:168-189`), and the POC doc's Reproduce section instructs
`modal run …::run` (`tic_payer_integration_poc.md:337`).
**Why it matters.** A 15–30 h fan-out attached to a laptop client dies with the client.
The repo's own sidecar doctrine (CLAUDE.md) records 8 ledger failures from precisely this
pattern and mandates deploy-then-`spawn`.
**Fix.** Convert `run` to a deployed `@app.function` launched via
`modal.Function.from_name(...).spawn(...)`; update the doc's Reproduce block.
**Where.** `orchestrate.py:168-189`.

### 6. Materialize the employer→file bridge, or Module B's purpose is lost
**What.** `_uhc_worklist` keeps only the `in-network-rates` blobs and discards the 66,912
employer `_index.json` ToCs (`orchestrate.py:104-106`); nothing persists
`(sponsor EIN, plan_id, in_network_url)`. The Cigna pipeline extracts exactly this bridge
but only to a session JSONL queue, not the SoR.
**Why it matters.** Book-of-business sizing needs *which self-funded employers (with Form 5500
participant counts) route through the network file where a practice's rates appear*. Rates
alone give price position, not volume proxy. The bridge is tiny (66,912 rows UHC; 29,216
Cigna) and the ToCs are already being read.
**Fix.** Add a `tic_employer_file_bridge` Lance dataset written by `build_worklist` from the
employer ToCs (UHC: stream each `_index.json`; Cigna: promote `stream_index.py`'s queue),
EIN digits-only-normalized per the Cigna doc's §2 trap, joinable to `form5500_main` on
`SPONS_DFE_EIN`. Note the fan-in caveat in the schema docstring: file-level attribution is
~9 employers/file (UHC) to ~1,075:1 (Cigna) — the bridge gives candidate-employer sets, not
exact panel membership.
**Where.** `orchestrate.py:56-122`; `pipelines/cigna_tic/stream_index.py`.

### 7. Staleness triage before fan-out (what to re-verify vs not)
**Fast-decaying (re-verify in one cheap pass before any production run):**
- Every dated URL in the docs (`2026-06-01_…`, `2026-06-05/…`) has rotated — two monthly
  drops have passed. `latest_metadata.json` and the UHC master index must be re-fetched
  fresh anyway; confirm counts (9,410 / 7,170 / 86,514) and re-sample sizes — the cost
  band ($760–$1,525) scales linearly with them.
- SAS token scheme + TTL (feeds Rec 2): confirm `sig` is still container-scoped and measure
  its expiry.
- Anti-bot posture at *fan-out concurrency*: the doc's own caveat is honest ("5 rapid HEADs
  … not stress-tested"). 64 containers pulling 88 TB from Azure Front Door is a different
  regime than 5 HEADs; add 429/`Retry-After` handling + exponential backoff to
  `stream_gunzip` (`reverse_map.py:184-245` currently has none — any 429 is a hard
  `raise_for_status`) regardless of what a probe shows.
- `part1_filter_spine.py:79` pins `group_snapshot="2026-06"` as default — bump or
  parameterize to the current snapshot.
**Architecture-stable (do NOT re-verify):** TiC v2.0.0 schema shapes, provider_references
indirection, the ToC-has-no-NPIs finding, multi-member gzip handling, the dead Aetna
`prd2` S3 path, master-index-first inversion, bounded-RAM engine behavior.

### 8. Pin the ijson C backend or the cost model is ~10× wrong
**What.** The 75 MB/s/worker throughput underlying the 7,630 worker-hr / $760 projection was
measured locally with the C backend (the Cigna doc states `yajl2_c` explicitly). Neither
`reverse_map.py` nor the Modal image asserts a backend; if the wheel lands without the C
extension, `ijson` silently falls back to pure Python at roughly an order of magnitude slower
— the run costs ~$7,600 and takes a week, discovered only mid-run.
**Fix.** At worker start: `import ijson; assert "yajl2_c" in ijson.backend` (fail fast).
**Where.** `reverse_map.py` module init or `orchestrate.py:36-39` (image), `process_file`.

### 9. Minor doc/code drift (record, low effort)
- `ops_tic_reverse_map_runs.sql:42-43` adds `tic_reverse_map_runs_payer_idx`; the "verbatim
  mirror" `OPS_DDL` constant (`reverse_map.py:417-443`) lacks it. The SQL file declares
  itself canonical and "keep in sync" — sync it.
- `reverse_map.py:93-94` comment claims payer CDNs "403 the default python-requests / curl
  agent"; the POC doc's measurement table says UA sensitivity **None** for both payers. The
  defensive UA is fine; the comment asserts a measured falsehood — fix the comment.
- `reverse_map.py:307`: `ref.get("provider_group_id", ref.get("provider_group_id", ""))` —
  the fallback is the same key; dead expression.
- `orchestrate.py:147` starts `t0` *after* Pass A, so ledger `parse_ms`/`throughput_mb_s`
  exclude the spine pass while `compressed_bytes` includes it — ledger throughput is
  inflated vs the POC's methodology (`reverse_map.py:513` times both). Align.
- `orchestrate.py:89-91` attributes each Aetna file to `reportingPlans[0]` only; a shared
  network file serves many plans. Acceptable for payer-mix, but `plan_id` in the fact table
  is then a sample, not an attribution — document it or drop the column and rely on Rec 6's
  bridge.

### 10. Missing joins — one is warranted, one is not
- **NPPES monthly snapshot (2026-05, in-factory): yes, at serving time, not ingest time.**
  The rate fact table keys on NPI; enrich with NPPES taxonomy/practice-address at query/mart
  time to (a) validate that matched NPIs are still active (deactivation check) and (b) give
  the payer-mix output its practice geography/specialty dimensions without re-deriving from
  `practice_group_360`. No change to the streaming engine — this is a sidecar/mart join.
  Also the cheap cross-check for Rec 1: NPPES "Other Provider Identifier" and organization
  records corroborate the TiC `tin.value` for multi-EIN practices (a practice billing under
  several TINs appears under several provider_groups — the payer-mix rollup must group by
  practice entity, not raw TIN, or one practice fragments into N).
- **CMS Open Payments: no.** It carries industry-payment signal, not payer-mix or volume;
  joining it does not strengthen the sizing outcome this pipeline exists for. Leave it to the
  entity-360 layer.

---

## Verified sound — no action

- **Inversion strategies match code exactly**: master-manifest-first Aetna
  (`orchestrate.py:79-92` filters `fileSchema=IN_NETWORK_RATES` off `latest_metadata.json`,
  avoids the dead `prd2` path), master-index-first UHC (`orchestrate.py:95-109` uses
  `downloadUrl`, never reconstructs filenames). Doc premise-corrections #2/#3 are
  implemented, not just claimed.
- **Two-pass streaming engine is as described**: Pass A spine (`build_provider_spine`) /
  Pass B rates (`extract_rates`), RAM = O(cohort + matched groups), ijson item streaming,
  multi-member gzip boundary handling (`reverse_map.py:219-235`) matches the doc's
  documented bug-fix.
- **Ledger shape matches the doc** (payer, source_file_url, file_version=ETag; unique index;
  upsert on conflict) — modulo the drift items in Rec 9.
- **Filter-spine SQL matches the doc's stated predicates** (`part1_filter_spine.py:92-122`
  ≙ POC Part 1: member_count 2–9, independent=member, distinct_specialties ≤ 2, 4A +
  general-asset self-funded, large-form-only rationale is correct per the Form 5500
  diagnostic's H/I/SF grain split).
- **Cigna EIN normalization analysis** — the mixed-format-within-one-file finding, the
  issuer-EIN (59-1031071) trap, sentinel EINs, and the state-file-would-mislead point are
  all correct and correctly generalized ("normalize both sides").
- **Form 5500 relational map** — ACK_ID vs EIN+PN roles, Schedule A `FORM_ID` composite,
  counterparty-EIN warning; the POC and Cigna docs consume `SPONS_DFE_EIN` from the head
  form only, which is the correct side of that warning.
- **Reverse-map-don't-mirror storage decision** — the 50–500× reduction argument holds;
  the append-only fragment + isolated index rebuild split is correctly implemented
  (`rebuild_indexes` separate, heavy, `replace=True`).
- **Honest-zero reporting** — the POC's 0-match cohort result with a 165,729-row positive
  control is the right way to validate without overclaiming.
- **Form 5500 coverage caveat is adequately handled for sizing**: the docs already scope
  the employer seed to the self-funded/large-plan book and the Cigna doc quantifies the
  fully-insured (issuer-stamped, ~25.5%) blind spot; one residual note — participant counts
  are plan-year-2025 filings and lag ~1 year, acceptable for deal-sourcing granularity.
