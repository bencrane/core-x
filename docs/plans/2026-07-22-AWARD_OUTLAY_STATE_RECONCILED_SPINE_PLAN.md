# `award_outlay_state` — the citation-grade federal-contract receivable spine

**The capital-video outlay leg: recon, corrected build architecture, and roadmap.**

- **Status:** design-ratified, not built. Supersedes the "parked, demo uses frozen constants" note in
  [`docs/plans/2026-07-21-SIDECAR_BUILD_CYCLE_AWARD_KEY_PLAN.md`](2026-07-21-SIDECAR_BUILD_CYCLE_AWARD_KEY_PLAN.md) §1 (item 3).
- **Provenance:** a 17-agent ultracode recon (10 recon + 5 adversarial verify + architecture + critique)
  run 2026-07-22 that ground-truthed the "capital video instrument handoff" against live code and R2/sidecar
  data. Every number below is tagged **[CG]** citation-grade or **[DIR]** directional; the raw verification
  log is Appendix C.
- **One-line thesis:** federal fixed-price *unfinanced* contract receivables are a financeable asset class;
  the platform can already size the **obligation** stock at citation grade, but the **outlay/paid** balance —
  the actual receivable — has no home. `award_outlay_state` is that home.

---

## 0. What the recon changed (read this first)

The handoff was directionally right and specifically wrong in seven places. The build must be authored against
reality, not the handoff:

1. **A BULK+FRESH argmax merge already exists.** `usaspending_award_canonical` (30,697,295 rows, 393 cols,
   built 2026-07-06) already collapses `award_search` ⊕ `api_fresh` per key on `argmax(last_modified_date)`,
   tie→fresh. `award_outlay_state` is **not** a new merge — it is a small (~255k-row), active-book-scoped
   derivation that does the **outlay unification the canonical deliberately does not** (canonical leaves the two
   outlay columns side-by-side, single-source). Clone `usaspending_award_canonical.py`; do not reinvent.
2. **The "immutable" bulk was mutated once.** `award_search` Lance history shows v5 (2026-06-07, +263,371 rows,
   `last_modified_date` carried to 2026-06-05) — a `merge_insert` overlay from the now-decommissioned
   `usaspending_daily_delta.py`, with **no ops-ledger row**. Do not cite the bulk as a pristine 2026-05-06
   pg_dump. (The forward rule still holds: **never upsert the bulk again.**)
3. **The coded reconcile is dead *and* buggy.** `usaspending_award_search_reconcile.py` partitions bulk by
   `contract_award_unique_key` (absent from bulk — bulk keys on `generated_unique_award_id`), filters
   `correction_delete_ind` (absent), and its tie-break is inverted (bulk wins ties, not fresh). It has never
   run and never produced `award_search_merged`. **Supersede and quarantine it** — do not extend it.
4. **OBBBA is unbuildable.** Triple-confirmed absent: no OBBBA column in bulk or fresh, no P.L. 119-21 DEFC
   string, no OBBBA entry in the 48-row `disaster_emergency_fund_code` reference (newest = AAK/AAL). Only
   **COVID-19 and IIJA** supplemental splits exist. **Scrub "OBBBA" from every citation surface.**
5. **The coverage %s on the handoff slide are wrong.** Live: bulk covers **104,060** active keys (39.5%),
   not 78,928 (31%); union **139,000** (52.8%), not 122,538 (48%). Dollars reproduce within ~10%. Do not
   put 31%/25%/48% on camera.
6. **The demo dollars will not survive the build.** The frozen constants ($246.1B obligated / $86.6B outlaid /
   35.2% / ~$160B float) come off a capped 744k api_fresh pull with a **1.19× duplicate ratio** and ~44%
   coverage. Deduping (which the spine does) drops fixed-unfinanced obligated **$246.1B → $201.4B (−18%)**;
   the float shrinks proportionally. The spine's whole job is to move these numbers — see the "numbers that
   move" ledger (§7).
7. **Five of the seven "open ledger" items already shipped.** `gtm_entity_pricing_flow`, the 4 award-key
   companions, `gtm_person_channels`, and the lender-book bridge `ucc_lender_filings` are all built, served,
   and (mostly) catalyst-consumed as of sidecar run 45 (113 tables, 2026-07-22T03:24Z). **The outlay spine is
   the only capital-video-critical gap still open.**

---

## 1. Executive summary

**The gap.** Every existing sidecar mart measures **obligations**. There is no outlay, disbursement, File-C,
or receivable-balance column anywhere in the sidecar or on the FPDS state SoR (`usaspending_fpds_prime_award_state`
has zero outlay columns, live-confirmed). The capital video's central asset — *how much performed fixed-price
work is unpaid* — cannot be sourced today.

**The ruling design.** Build one new immutable Lance dataset, `s3://data-sink/active/usaspending_award_outlay_state/`,
at **active-award grain** (~255,523 rows, one per active `contract_award_unique_key`), on the
**transactions-spine pattern**:

> Read the two immutable sources — **BULK** `usaspending/award_search` and **FRESH**
> `usaspending_api_fresh/contract_prime_award` — collapse latest-per-key *within* each, take one
> `argmax(last_modified_date)` across both (tie→fresh) **for provenance only**, LEFT-JOIN both onto the
> authoritative active-key anchor, **coverage-coalesce** the outlay figure (never `argmax`-driven), carry a
> real File-C-linkage flag and an honest three-way `outlay_status`, compute `unspent_receivable`, parity-gate,
> then promote into the sidecar. Neither source is ever mutated.

**Lead with obligation, treat outlay as directional.** The size of the asset class is **[CG]** today:

| Headline | Value | Grade | Basis |
|---|---|---|---|
| Active federal contract book | **255,523 awards / $2,436.2B** life-to-date obligated ($2,842.3B ceiling) | **[CG]** | `usaspending_fpds_prime_award_state`, active filter, pinned vintage |
| Fixed-price **family** unfinanced, active, obligated | **~$715.9B** | **[CG]** | `gtm_entity_pricing_mix`, entity-grain |
| Firm-fixed-price (code J) unfinanced, active, obligated | **$557.5B across 30,910 firms** | **[CG]** | `gtm_entity_pricing_mix` live probe |
| Paid-% on fixed-unfinanced / "the float" | 35.2% / ~$160B | **[DIR]** | capped 744k api_fresh pull, pre-dedup — **will move** |

The float thesis is real; the **exact float dollar is not yet citation-grade** and becomes so only as the
residual-tail backfill raises File-C coverage past today's ~44%. **On camera, size the asset class with the
~$716B obligated figure; present paid-%/float as directional with a stated vintage** until coverage plateaus.

---

## 2. The strategic frame (the capital video)

The narrative, captured precisely, with grades:

- **The cash-curve thesis [CG as ordering, DIR as levels].** Rank active unfinanced contracts by paid-%:
  T&M pays down fastest (50.8%), cost next (42.4%), **firm-fixed-price slowest (35.2%)**, progress-financed
  fixed is a reporting-lag artifact (2.2%). The *ordering* is the argument ("money arrives latest exactly
  where the vendor fronts everything") and is robust; the *levels* are directional off the capped pull.
- **The float line [DIR].** "~$160B of performed-or-being-performed fixed-price work hasn't been paid out
  yet." This is `Σ obligated − Σ outlaid` on the FFP-unfinanced in-force slice ($246.1B − $86.6B). Both terms
  are pre-dedup and window-capped; the honest camera-safe framing is *"on the order of $150–200B, and here is
  exactly how we measure it,"* not a hard $160B.
- **Receivables as an asset class [CG obligation / DIR receivable].** Tens of thousands of mid-size receivables,
  not a mega-prime market. Concentration/fragmentation and the maturity ladder (`current_end_date` quarter)
  are buildable now on obligation; the receivable-balance versions wait on the spine.
- **Revealed demand — the UCC funnel [PARTIAL, see §7].** ≥70%-FFP-unfinanced firms → already UCC-pledging →
  fresh filings. Firm counts: **10,502 → 660 → ~251**. The 660 is exact and robust (two independent
  definitions); 10,502 carries an **unstated $1M materiality floor** (literal share≥0.70 alone = 28,142); 251
  is filing-window-sensitive; **660/251 are CA+CO UCC coverage only** — a two-state floor, not a national count.
- **The second market underneath [CG obligation].** Combo-#1 primes' sub-out dollars: subs sit behind
  pay-when-paid, waiting even longer (`subaward_canonical` / `award_subout_rollup`).
- **Vehicle recurrence [CG].** One IDV relationship = a stream of future task-order invoices, not a single award.

---

## 3. Ground truth — the data plane (live-probed 2026-07-22)

| Dataset (R2 `active/…`) | Grain | Rows | Award key | Outlay columns | Freshness / notes |
|---|---|---|---|---|---|
| `usaspending/award_search` (**BULK**) | award | 78,636,657 (30.4M contracts) | `generated_unique_award_id` (no `contract_award_unique_key`) | `total_outlays`, `total_covid_outlay`, `total_iija_outlay` (+ obligation splits) | pg_dump 2026-05-06 **+ one merge_insert (v5, 2026-06-07)**; max `last_modified_date` 2026-06-05; only 3 live indices (parent_uei, naics, recipient_uei) — **award key is NOT indexed** |
| `usaspending_api_fresh/contract_prime_award` (**FRESH**) | award (append-only) | 744,000 rows / **625,411 distinct keys (1.19× dup)** | `contract_award_unique_key` | `total_outlayed_amount`, `outlayed_amount_from_COVID-19_supplementals`, `outlayed_amount_from_IIJA_supplemental` | append-only accumulator over overlapping `last_modified_date` windows; fresh to ~2026-07-16; all-VARCHAR; **no `award_id` bigint** |
| `usaspending_fpds_prime_award_state` (**STATE / anchor**) | award | 82.9M base | `contract_award_unique_key` | **none** (obligation only) | active filter `current_end_date >= CURRENT_DATE AND is_terminated = FALSE` → **255,523** active keys; `life_to_date_obligated`, `is_terminated`, `current_end_date` |
| `usaspending_award_canonical` (**existing merge**) | award | 30,697,295 (393 cols) | `generated_unique_award_id` == `contract_award_unique_key` | both, **side-by-side, single-source** | built 2026-07-06; the pattern to clone; does *not* unify outlay |
| `financial_accounts_by_awards` (**File C**) | award × DEFC × period | 454,215,610 | `award_id` (bigint) | per-period obligation+outlay decomposition | the true aging/vintage source; deferred join (see §6) |

**Join basis [CG]:** `generated_unique_award_id` (bulk) == `contract_award_unique_key` (fresh/state) for all
144,361 canonical active rows, **0 nulls** — the crosswalk in `usaspending_award_canonical.py:179-180` proves the
two names are the same `CONT_AWD_/CONT_IDV_` composite. The LEFT-JOIN anchor is sound at the key level.

**Three live "active" universes (must be reconciled on camera):** state book **255,523**;
`usaspending_award_canonical` active-by-PoP **144,361**; `govcon_active_awards` (v200) **189,272**. The ruling
uses the state book; §9 records why and pre-bakes the "why is your active count different" answer.

---

## 4. The diagnosis, corrected and number-verified

The handoff's "three defects" hold in substance; the numbers are restated to what live probes actually return.

1. **Coverage is partial and the split is not what the slide said.**
   - BULK matches **104,060** of 255,523 active keys (**39.5%**) — *not* 78,928 / 31%.
   - FRESH true active intersection is **34,380 keys (13.5%) / $880.2B (36.1%)** — the handoff's "119,873 in-force
     / $1,054.6B" is **raw dup-inflated rows**, not awards; collapsed in-force ≈ **100,306 awards / ~$939.8B**.
   - Union ≈ **139,000 keys (52.8%)** — *not* 122,538 / 48%. Dollar coverage reproduces within ~10% of the
     handoff. **Neither-matched is large** (the state book's 82.9M base includes 64.8M vehicle_orders absent
     from award_search's 30.4M award-summary universe): a big share of active keys have **no outlay from either
     source**, which is why paid-% must be computed over the *linked subset only* (§6).
2. **The refresh chain is stalled [CG].** `award_search`'s API delta halted 2026-06-08 ("API returned 0 rows.
   Assuming federal warehouse lag. Halting watermark", ledger `status='stalled'`, never resumed). But this is
   **moot for the new design** — the spine draws freshness from `api_fresh` (fresh to ~07-16), not from
   reviving the bulk delta.
3. **Zeros are not proven zeros — and the bulk encoding is a citation-killer [CG probe].** In BULK CONT scope
   (30,683,126 rows): `total_outlays` is **NULL on only 263,427 (0.86%)** but **= 0.0 on 22,188,327 (72.3%)**.
   The fresh semantics (`total_outlayed_amount` NULL⇔not-linked, 0.0⇔linked-unpaid) **do not transfer to bulk** —
   a bulk 0.0 almost certainly means *not linked / not reported*, not *unpaid*. Deriving
   `has_file_c_linkage := bulk_outlay IS NOT NULL` would tag ~99% of bulk rows as linked and 72% as
   "linked_unpaid," inflating the financeable pool and understating paid-% across the entire bulk leg. **This
   is the #1 correctness hinge (see §6.1).**

**OBBBA scrub.** The handoff's repeated "OBBBA/IIJA/COVID" is wrong on OBBBA. Carry COVID-19 + IIJA supplemental
splits only; remove OBBBA from the schema, the narrative, and any slide.

---

## 5. The design ruling — what `award_outlay_state` is (and is not)

- **IS:** a new, small (~255,523-row) **active-book-scoped** award-grain outlay/receivable spine.
- **IS NOT:** a re-derivation of the 30.7M `usaspending_award_canonical` merge (that exists), and not a rewrite
  of any bulk. Because it is 255k rows, the giant boto3-uniform-part publish dance is unnecessary — a direct
  local→R2 `lance.write_dataset` with folded indices suffices.
- **Anchor:** the 255,523 active keys of `usaspending_fpds_prime_award_state`
  (`current_end_date >= CURRENT_DATE AND is_terminated = FALSE`). Every active key appears exactly once; the
  universe is the fixed active-key set, LEFT-JOINed to the outlay sources.
- **Outlay unification (the net-new work):** `outlay_to_date` resolved by **coverage** across bulk/fresh with a
  provenance flag — **not** taken from the argmax winner (a blind argmax pick would null the outlay on
  fresh-won keys whose fresh outlay is blank but bulk had a value; only ~8,107 keys carry both outlays >0, so
  coverage disjointness is the norm).
- **Honesty flags:** `has_file_c_linkage`, three-way `outlay_status {unknown|linked_unpaid|linked_paid}`,
  `outlay_source {fresh|bulk|null}`, `mod_source {fresh|bulk}`, `financing_known`, `is_unfinanced`
  (valid only where `financing_known`), `file_c_award_id` (bigint, for the future File-C decomposition).
- **Receivable:** `unspent_receivable = total_obligation − outlay_to_date`, clamped/flagged (see §6.3).

---

## 6. Correctness hardening — the citation-killers and their fixes

The naive architecture would ship four wrong numbers. These fixes are **not optional** and are sequenced into
the build (§8). Each is live-probe-grounded.

### 6.1 Bulk `0.0` ≠ linked-unpaid — resolve the encoding **before** authoring the flag (P1 blocker)
72.3% of bulk CONT rows are `total_outlays = 0.0`. `has_file_c_linkage` must key off the **actual File-C
linkage signal** (`federal_accounts_funding_this_award` / `treasury_accounts_funding_this_award` on fresh; a
bulk linkage indicator to be isolated — candidates: a non-null account-title/DEFC field), **never** off
`outlay IS NOT NULL`. Add a **gated** Tier-1 assertion that fails the build if the resolved bulk 0.0-vs-linkage
encoding is violated. Until resolved, treat bulk-only keys as `outlay_status = 'unknown'`.

### 6.2 Outlay precedence: non-zero-preferring, not blind `COALESCE(fresh, bulk)`
Fresh `0.0` is a legitimate value, and `COALESCE` treats it as present, so a fresh 0.0 overrides a bulk >0.
File-C gross outlay is cumulative/monotonic, so *fresh $0 while bulk $5M* is a not-yet-reported artifact, not
truth. **Rule:** prefer the non-zero linked value; where both are >0 prefer fresh (fresher submission); record
`outlay_source` per key and a Tier-2 monitor counting `fresh0-over-bulk-positive` overrides. The dangerous set
is `fresh_outlay IS NULL-or-0 AND bulk_outlay > 0`, which is far larger than the ~8,107 both-positive keys.

### 6.3 `unspent_receivable` can go negative — clamp and flag
576,300 CONT rows have `total_outlays > total_obligation` (mixed obligation bases: award-summary
`total_obligation` vs the state's `life_to_date_obligated`). **Pick one obligation basis, document it, clamp
`unspent_receivable` at 0, and carry a `negative_receivable` data-quality flag** — never emit negative
"financeable receivables" into the BTREE sort key that ranks the whole product.

### 6.4 Paid-% denominator = File-C-linked subset only
With a large `neither_matched` fraction, `Σoutlay / Σobligation` over the full 255k divides a numerator covering
~113k keys by a 255k denominator — a structurally wrong paid-%. **Compute paid-% over the linked subset only,
report the linked-subset totals and the unknown fraction beside it.** The ledger records
`file_c_linked_count`, `neither_matched`, and the linked-subset sums separately.

### 6.5 IDV parent/child outlay dedup
The active book mixes standalone awards, IDV parents, and orders. Outlay can be recorded on both a parent
vehicle and its orders; summing across the book without a parent/child rule risks double-counting the very
float number on camera. **Define and apply an IDV rollup rule** (outlay at order grain, parent as ceiling) before
any Σoutlay is quoted.

### 6.6 `financing_known` ~45% — carry an explicit unknown-financing bucket
The financing axis exists **only on fresh** (bulk `award_search` has no financing column across all 154 cols).
Combo-#1 "unfinanced" is reliable for <45% of active keys. `is_unfinanced` must be NULL/invalid where
`financing_known = FALSE`; the hero cut shows **classified vs unclassified transparently**, never silently
defaulting unknown→unfinanced (which inflates combo #1).

### 6.7 `file_c_award_id` is bulk-only — the fresh tail can't join File C
`award_id` (bigint) exists only on bulk; fresh has none. The residual-loop keys (fresh-only tail) can never
join `financial_accounts_by_awards`. The "carry the key for a free future join" claim is false for the tail —
document it and scope the File-C decomposition (§ open questions) to the bulk-covered subset.

---

## 7. Citation discipline — the numbers ledger

**Every on-camera number ships with a grade, a basis, and a pin** `(source dataset + Lance version,
state_build_date, query_date, sidecar_artifact)`.

### 7.1 Numbers that move (frozen demo constant → expected post-spine value)

| Frozen demo constant | Post-spine expectation | Why it moves |
|---|---|---|
| Active book "255,901" | **255,523** (drifts down daily) | 255,901 is unreproducible/uncommitted; use the live active filter |
| Coverage 31% / 25% / 48% | **39.5% / 13.5% / 52.8%** (counts) | slide undercounts bulk; fresh "in-force" was dup-inflated rows |
| api_fresh "119,873 / $1,054.6B in-force" | **~100,306 awards / ~$939.8B** | dedup collapses 1.19× duplicates |
| Fixed-unfin obligated "$246.1B" | **$201.4B (−18%)** deduped; **$715.9B** at entity-grain (family) | capped-pull artifact vs full active book |
| Float "~$160B" | shrinks ~18% with the obligation term; recomputed over linked subset | dedup + linked-denominator |
| Paid-% "35.2%" | recomputed **over File-C-linked subset**, directional until coverage plateaus | denominator fix + coverage climb |

### 7.2 Lead-with list (citation-grade today)
Active book **255,523 / $2,436.2B** obligated ($2,842.3B ceiling); **~$716B** fixed-unfinanced obligated
(the asset-class size headline); **$557.5B / 30,910 firms** FFP-strict unfinanced; **$136.2B** small-determined;
the demand funnel **10,502 → 660 → ~251** (with the definitional footnotes of §2). Pin all to
`state_build_date 2026-07-04`, the query date, and sidecar artifact `query_sidecar_20260722T032457Z`.

---

## 8. The build — phase sequence with gates

Clone `pipelines/usaspending/usaspending_award_canonical.py` → `usaspending_award_outlay_state.py`. Reuse
verbatim: `LANCE_BYPASS_SPILLING` top-set, the `s(x)=nullif(nullif(trim(x),''),'-NONE-')` sentinel macro, the
`_bulk_collapse`/`_fresh_collapse` windows, the 2-way argmax window, sentinel/amount clamps, the `--since`
scanner pushdown, and the `_record_run` `finally:` ledger pattern. Narrow the column spec to the outlay set,
add the active-key anchor, replace the enrich block with the §6-hardened unification.

| Phase | Action | Gate |
|---|---|---|
| **P0** Correct the record | Amend the parked-item note in `2026-07-21-SIDECAR_BUILD_CYCLE_AWARD_KEY_PLAN.md`; record the seven corrections (§0), the bulk-0.0 finding, and the paid-%-denominator decision | Plan reflects the buildable design; OBBBA removed everywhere |
| **P1** Anchor + encoding | Pin the active count + `state_build_date` + `query_date`; prove the 1:1 join basis on the active set; **resolve the bulk null-vs-zero File-C encoding** and confirm `award_id` non-null/unique in CONT scope | Anchor pinned; join 1:1; **bulk linkage encoding resolved** — else stop |
| **P2** Author pipeline | Clone the canonical; add `active_keys` anchor CTE; implement §6 (linkage flag off the real column, non-zero-preferring outlay, clamped receivable, linked-subset paid-%, IDV rule, financing-unknown bucket); quarantine `usaspending_award_search_reconcile.py` in the same PR | `len(COLUMN_SPEC)` locked; full SQL renders; no dependency on the stale canonical |
| **P3** `init_ops` | Author `ops_usaspending_award_outlay_state_runs.sql` (mirror the canonical ledger + coverage/receivable columns); run `init_ops` | Ledger table exists; idempotent DDL |
| **P4** Sample build | `--since`/small-slice build to a `_sample/` URI; spot-check fresh-only, bulk-only, both, neither keys | Sample passes PK-uniqueness; `outlay_status` matches hand-computed expectations |
| **P5** Full active build | `modal run --detach` the full build (bulk leg semi-joins ~30.4M CONT rows to the 255k anchor; fresh leg cheap) | **Tier-1 fail-closed:** `rows_out == active_key_count` AND `count(*) == count(distinct cauk)` AND 0 null keys AND the bulk-linkage assertion — raise before publish |
| **P6** Index + verify | Fold BTREE/BITMAP into the local dataset; independent read-back `verify()` | All indices present; `verify()` passes; addressable at the R2 URI |
| **P7** Residual-tail loop | Compute uncovered active keys; run append-only `usaspending_api_award_fresh.py` (mode=`append`, 1-day chunks, ~7.5 min/chunk); rebuild; re-measure coverage; repeat **until coverage plateaus** (not 100%) | Coverage delta recorded each rebuild; residual reported as honest "outlay-unknown"; Tier-1 holds every rebuild; **never upsert the bulk** |
| **P8** Sidecar promotion | Add one Tier-C manifest entry `{ds:'usaspending_award_outlay_state', tier:'C', sort:['contract_award_unique_key']}` (SELECT * exact-parity copy); adjacency sweep; `_preflight()`; **`modal run --detach` spawn-deployed** (NOT client-tethered — that caused the run-40/42/43 "Query interrupted" failures) | Blue-green publish only on exact-parity pass; `/healthz` new stamp + table count 114; sub-second pruned point-reads |
| **P9** Git + doc | Update `QUERY_SIDECAR_AGENT_GUIDE.md` catalog+pattern; `git mv` the capital-video gap report to `processed/`; branch → commit (add by path) → push → PR → squash-merge → **pull `~/core-x`** → `git log -1` | Merged **and** operator checkout reflects it on disk |

### 8.1 The reconcile SQL (corrected, DuckDB)

```sql
CREATE MACRO s(x) AS nullif(nullif(trim(x),''),'-NONE-');

-- (0) ACTIVE ANCHOR — authoritative universe (state SoR, READ-ONLY scanner filter):
--     current_end_date >= CURRENT_DATE AND is_terminated = FALSE   -> ~255,523 (PIN vintage)
CREATE TEMP TABLE active_keys AS
SELECT s(contract_award_unique_key) AS cauk, life_to_date_obligated, current_end_date
FROM state_r WHERE contract_award_unique_key IS NOT NULL;

-- (1) BULK collapse latest-per-key (award_search IMMUTABLE, CONT% scope, narrow outlay projection)
CREATE TEMP TABLE bulk_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (PARTITION BY generated_unique_award_id
             ORDER BY last_modified_date DESC NULLS LAST, award_id DESC NULLS LAST) rn
  FROM (SELECT s(generated_unique_award_id) AS cauk, last_modified_date AS bulk_lmd,
               award_id AS file_c_award_id,
               CASE WHEN abs(total_obligation)<=1e12 THEN total_obligation END AS bulk_obligation,
               CASE WHEN abs(total_outlays)   <=1e12 THEN total_outlays    END AS bulk_outlay,
               <bulk_file_c_linkage_signal> AS bulk_linked,   -- P1: the RESOLVED linkage column, NOT (outlay IS NOT NULL)
               generated_unique_award_id
        FROM bulk_r) WHERE generated_unique_award_id IS NOT NULL) WHERE rn = 1;

-- (2) FRESH collapse latest-per-key (contract_prime_award IMMUTABLE append-only; all-VARCHAR -> TRY_CAST)
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (PARTITION BY cauk ORDER BY fresh_lmd DESC NULLS LAST, cauk DESC) rn
  FROM (SELECT s(contract_award_unique_key) AS cauk,
               TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP) AS fresh_lmd,
               CASE WHEN abs(TRY_CAST(s(total_outlayed_amount) AS DOUBLE))<=1e12
                    THEN TRY_CAST(s(total_outlayed_amount) AS DOUBLE) END AS fresh_outlay,
               CASE WHEN abs(TRY_CAST(s(total_obligated_amount) AS DOUBLE))<=1e12
                    THEN TRY_CAST(s(total_obligated_amount) AS DOUBLE) END AS fresh_obligation,
               s(contract_financing_code) AS financing_code, s(type_of_contract_pricing_code) AS pricing_code,
               s(federal_accounts_funding_this_award) AS federal_accounts
        FROM fresh_r) WHERE cauk IS NOT NULL) WHERE rn = 1;

-- (3) TWO-SOURCE ARGMAX — provenance/freshness winner ONLY (tie -> fresh). Flat window, EXPLAIN-clean.
CREATE TEMP TABLE mod_winner AS
SELECT cauk, src AS mod_source, lmd AS last_modified_date FROM (
  SELECT *, row_number() OVER (PARTITION BY cauk ORDER BY lmd DESC NULLS LAST, source_rank ASC, cauk DESC) rn
  FROM (SELECT 'fresh' src,1 source_rank,cauk,fresh_lmd lmd FROM fresh_latest
        UNION ALL BY NAME
        SELECT 'bulk'  src,2 source_rank,cauk,bulk_lmd  lmd FROM bulk_latest)) WHERE rn = 1;

-- (4) ASSEMBLE — every active key once; outlay COVERAGE-resolved (non-zero-preferring), receivable clamped.
CREATE TEMP TABLE award_outlay_state AS
SELECT k.cauk AS contract_award_unique_key, k.cauk AS generated_unique_award_id, b.file_c_award_id,
       -- outlay: non-zero-preferring (fix 6.2), NOT blind COALESCE(fresh,bulk)
       CASE WHEN f.fresh_outlay > 0 THEN f.fresh_outlay
            WHEN b.bulk_outlay  > 0 THEN b.bulk_outlay
            ELSE COALESCE(f.fresh_outlay, b.bulk_outlay) END AS outlay_to_date,
       CASE WHEN f.fresh_outlay > 0 THEN 'fresh' WHEN b.bulk_outlay > 0 THEN 'bulk'
            WHEN f.fresh_outlay IS NOT NULL THEN 'fresh' WHEN b.bulk_outlay IS NOT NULL THEN 'bulk' END AS outlay_source,
       COALESCE(f.fresh_obligation, b.bulk_obligation) AS total_obligation, k.life_to_date_obligated,
       -- linkage off the RESOLVED signal (fix 6.1), never outlay-is-not-null
       (f.federal_accounts IS NOT NULL OR b.bulk_linked) AS has_file_c_linkage,
       CASE WHEN NOT (f.federal_accounts IS NOT NULL OR b.bulk_linked) THEN 'unknown'
            WHEN COALESCE(NULLIF(GREATEST(COALESCE(f.fresh_outlay,0), COALESCE(b.bulk_outlay,0)),0),0)=0 THEN 'linked_unpaid'
            ELSE 'linked_paid' END AS outlay_status,
       -- receivable clamped >=0 with a QC flag (fix 6.3)
       GREATEST(COALESCE(f.fresh_obligation,b.bulk_obligation,0)
                - CASE WHEN f.fresh_outlay>0 THEN f.fresh_outlay WHEN b.bulk_outlay>0 THEN b.bulk_outlay
                       ELSE COALESCE(f.fresh_outlay,b.bulk_outlay,0) END, 0) AS unspent_receivable,
       (COALESCE(f.fresh_obligation,b.bulk_obligation,0) < COALESCE(f.fresh_outlay,b.bulk_outlay,0)) AS negative_receivable,
       f.pricing_code, f.financing_code,
       (f.cauk IS NOT NULL) AS financing_known,               -- financing axis is FRESH-only (fix 6.6)
       CASE WHEN f.cauk IS NULL THEN NULL
            ELSE (f.financing_code IS NULL OR f.financing_code IN ('Z','NOT APPLICABLE')) END AS is_unfinanced,
       w.mod_source, w.last_modified_date, k.current_end_date, TIMESTAMP '<built_at_iso>' AS built_at
FROM active_keys k
LEFT JOIN bulk_latest  b ON k.cauk = b.cauk
LEFT JOIN fresh_latest f ON k.cauk = f.cauk
LEFT JOIN mod_winner   w ON k.cauk = w.cauk;
-- Tier-1 gates run over award_outlay_state; then local lance.write_dataset(mode='overwrite','2.1') -> fold indices -> publish.
-- NOTE: IDV parent/child outlay dedup (fix 6.5) applied before any Σoutlay is quoted downstream.
```

### 8.2 Index plan
BTREE: `contract_award_unique_key`, `generated_unique_award_id`, `file_c_award_id`, `last_modified_date`,
`current_end_date`, `total_obligation`, `outlay_to_date`, `unspent_receivable` (**the capital-video sort key**).
BITMAP: `has_file_c_linkage`, `outlay_status`, `outlay_source`, `mod_source`, `financing_known`, `is_unfinanced`,
`pricing_code`, `financing_code`.

### 8.3 Parity gates (real, not tautological)
Tier-1 `rows_out == |active_keys|` is a LEFT-JOIN tautology and proves nothing about outlay — keep it as a
structural guard but **add gated correctness assertions**: the bulk-linkage-encoding assertion (6.1), a non-null
`outlay_source` floor over the linked subset, and PK-uniqueness. Tier-2 (monitored, ledger-recorded, non-gating):
`file_c_coverage_pct` (~44% today), `financing_coverage_pct` (~45%), `fresh0-over-bulk-positive` override count,
`negative_receivable` count (expect ~576k universe-wide, far fewer in active scope), obligation tie-out band.

---

## 9. The 9 camera cuts — buildable now vs blocked on the spine

**All nine underlying marts serve today (sidecar run 45, 113 tables).** None is blocked on *existence*; each cut's
**obligation** version is buildable now, and only its **outlay/receivable-dollar** version waits on the spine.

| # | Cut | Live mart | Now (obligation) | After spine (receivable) |
|---|---|---|---|---|
| 1 | Growth of combo-#1 firms | `gtm_entity_fy_won` (3.81M) × `gtm_entity_pricing_mix` (766,803) | FY-won CAGR of the FFP-unfinanced cohort | — |
| 2 | Maturity ladder | `gtm_entity_pricing_mix` / state `current_end_date` | duration profile of obligated book | receivable by due-quarter |
| 3 | Small-determined slice | `gtm_entity_pricing_mix.active_obl_small_determined` ($136.2B) | addressable market by CO size determination | small-firm receivable |
| 4 | Obligor quality (DoD vs civ) | combo-portrait marts (`txn_events_combo`, 108M) | who owes, by agency | receivable by obligor |
| 5 | Concentration/fragmentation | `gtm_entity_pricing_mix` top-50 share | proves mid-market, not mega-prime | receivable concentration |
| 6 | Invoice rhythm | `gtm_txn_recipient_month_rollup` (34.1M) | monthly obligation cadence | disbursement cadence |
| 7 | Sub-side chain | `award_subout_rollup` / `subaward_canonical` | prime sub-out $ | sub receivable (pay-when-paid) |
| 8 | UCC overlay (demand proof) | `ucc_lender_filings` (8.0M) | 10,502 → 660 → ~251 funnel | pledged-receivable $ |
| 9 | Vehicle recurrence | IDV task-orders | future-invoice stream per vehicle | receivable stream |

> **Note:** `active_obl_small_determined` is a **CO business-size determination** column on
> `gtm_entity_pricing_mix` (latest_business_size='S'), **not** a DSBS registry flag (those live on
> `gtm_audience_entities`: `in_dsbs`, `dsbs_8a`, …). State which "small" a cut means.

---

## 10. Open ledger — corrected current status

The 2026-07-21 report's ledger is stale; live status (sidecar run 45):

| # | Item | Handoff status | **Actual (2026-07-22)** |
|---|---|---|---|
| 1 | `gtm_entity_pricing_flow` | "blocked on one rebuild" | **BUILT+SERVED** (162,872 rows; #1273) |
| 2 | Award-key point-read companions | "27s cold, on-camera 500 risk" | **BUILT+FIXED+CONSUMED** (#1304/#1305/#1308; pruned reads 8.3–15.4 ms) |
| 3 | **Outlay spine** | "must fix pull first" | **THE remaining gap — this plan** |
| 4 | `gtm_person_channels` | structural gap | **BUILT+SERVED** (2,252,385 rows; #1304) |
| 5 | Lender-book `ucc_lender_filings` | "scope written, never built" | **BUILT+SERVED+CONSUMED+DISPOSITIONED** (8.0M rows; JPMorgan 160,968 filings @ 10.3 ms) |
| 6 | Novation / `reason_for_modification` | proxy-only | still proxy-only; correctly parked (low video-criticality) |
| 7 | Award-key probes (origin) | superseded by #2 | resolved |

**Build-reliability note for P8:** the run-40/42/43 "Query interrupted" failures were **client-tethered
`modal run`** teardown, not preemption/OOM. Launch the sidecar build **spawn-deployed** (`modal run --detach`);
runs 44/45 both succeeded that way.

**Doc hygiene (surface, don't self-assign):** the pricing_flow/companion/person_channels gap reports +
`PRICING_FLOW_MART_HANDOFF.md` are unarchived; `QUERY_SIDECAR_AGENT_GUIDE.md` lacks catalog rows for the 3 new
marts; a stale duplicate lender-book report sits at `docs/sidecar_gaps/` top-level (authoritative copy is in
`processed/`).

---

## 11. Risk register

1. **Bulk 0.0 encoding (top citation-killer)** — 72.3% of bulk CONT rows are 0.0; a wrong linkage assumption
   mislabels ~22M rows as linked_unpaid. Mitigation: §6.1, gated in P1/P5.
2. **Directional outlay on camera** — every paid-%/float number is capped-pull-derived; a slide citing the
   capped $246.1B as asset-class size understates it ~2.9×. Mitigation: lead with ~$716B obligated; label
   outlay directional with vintage.
3. **Negative receivable** — 576,300 CONT rows have outlay>obligation; corrupts the sort key. Mitigation: clamp
   + flag (§6.3).
4. **Memory on the bulk leg** — the canonical's `include_fresh` build OOM'd once before succeeding; the bulk
   collapse scans ~30.4M CONT rows. Mitigation: keep `LANCE_BYPASS_SPILLING`, semi-join to the 255k anchor early,
   RSS reclaim before the index sort.
5. **Active-key drift** — the parity target drifts down daily; recompute per build, never hardcode 255,523.
6. **Grain mismatch / neither_matched** — state's 82.9M base (incl. 64.8M vehicle_orders) vs award_search's
   30.4M; a large neither_matched is legitimate, not a defect — the ledger must distinguish it.
7. **Dead-code trap** — quarantine `usaspending_award_search_reconcile.py` in the P2 PR so no future agent
   clones the buggy template.

---

## 12. Open questions (operator decisions)

1. **Outlay precedence where fresh 0 / bulk >0** — the §6.2 non-zero-preferring rule is the default; confirm
   a fresh $0 should never override a bulk >0 for citation.
2. **File-C decomposition** — LEFT-JOIN `financial_accounts_by_awards` (454M) via `file_c_award_id` for a true
   per-period aging curve? Deferred column-add that rides a rebuild; note it is bulk-covered-subset only (§6.7).
3. **Active-definition reconciliation** — ratify the state book (255,523) as the one true active universe over
   canonical-active-by-PoP (144,361) and `govcon_active_awards` (189,272); pre-bake the on-camera answer.
4. **Scope** — active-book-only (ruled) vs full CONT% with an `is_active` flag. Ruling favors active-book
   (trivial publish, matches the cohort); flag if a full-universe outlay surface is later demanded.
5. **Ship on directional?** — decide whether the video ships on directional paid-% (fine, if labeled) or must
   wait for the P7 residual-loop coverage plateau. This determines whether P7 is on the critical path.

---

## Appendix A — dataset catalog (live, 2026-07-22)

- BULK: `s3://data-sink/active/usaspending/award_search` — v6, 78,636,657 rows, key `generated_unique_award_id`.
- FRESH: `s3://data-sink/active/usaspending_api_fresh/contract_prime_award` — v48, 744,000 rows / 625,411 keys.
- STATE anchor: `s3://data-sink/active/usaspending_fpds_prime_award_state` — 255,523 active keys.
- Existing merge (template): `usaspending_award_canonical` — 30,697,295 rows, built 2026-07-06.
- File C: `financial_accounts_by_awards` — 454,215,610 rows.
- Sidecar: `query_sidecar_20260722T032457Z.duckdb` — 113 tables, 1,714,347,196 rows (run 45).

## Appendix B — key file references

- Clone template / pattern: `pipelines/usaspending/usaspending_award_canonical.py`
  (collapse windows `:704-758`, argmax `:857-891`, gua↔cauk crosswalk `:179-180`, verify/parity `:1188-1213`).
- FRESH puller (append-only, residual loop): `pipelines/usaspending/usaspending_api_award_fresh.py`
  (`run_daily`/`run_backfill` mode=`append` `:445,485`).
- Dead code to quarantine: `pipelines/usaspending/usaspending_award_search_reconcile.py`.
- Active-filter definition: `apps/catalyst_api/src/routers/active_awards_query_v1.py:727-728`.
- Combo-#1 mart: `pipelines/query_sidecar/build_query_sidecar.py:998` (`gtm_entity_pricing_mix`), FFP/unfinanced
  derivations `:1021-1024`, combo-#1 columns `:1040-1042`.
- Sidecar promotion: `build_query_sidecar.py` manifest (Tier-C ~`:228`), `_preflight()`, parity `:2100-2114`.

## Appendix C — verification log (adversarial)

| Claim | Verdict | Live value |
|---|---|---|
| Paid-% by combo (35.2/42.4/50.8/2.2) | **CONFIRMED (directional)** | reproduces off capped pull; 1.19× dup caveat |
| Stall 2026-06-08 + megadeals absent | **CONFIRMED** | ledger `stalled`; DEAC05840R21400 & N0001923C0003 → 0 rows; Lockheed key matches |
| Coverage table 31/25/48% | **PARTIAL/REFUTED** | bulk 104,060 (39.5%); union 139,000 (52.8%); $ within ~10% |
| api_fresh "119,873 in-force / $1,054.6B" | **PARTIAL** | true intersection 34,380 (13.5%) / $880.2B; collapsed ~100,306 / ~$939.8B |
| Demand funnel 10,502 → 660 → 251 | **PARTIAL** | 660 exact; 10,502 needs $1M floor (else 28,142); 251 window-sensitive; CA/CO only |
| "255,901 active / $2,437.4B" | **CORRECTED** | 255,523 / $2,436.2B (drop 255,901) |
| OBBBA columns | **REFUTED** | none exist; COVID-19 + IIJA only |
| "immutable 2026-05-06 bulk" | **CORRECTED** | mutated once, v5 2026-06-07, +263,371 rows |

---

*Generated from the 2026-07-22 ultracode recon (17 agents, 0 errors). Numbers pinned to sidecar artifact
`query_sidecar_20260722T032457Z` and state build 2026-07-04; re-pin on rebuild.*
