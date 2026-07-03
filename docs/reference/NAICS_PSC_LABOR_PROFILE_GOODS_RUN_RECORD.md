# naics_psc_labor_profile — Goods Lane Build Run Record

> ## ⚡ CLASSIFICATION RUNS ONLY AS IN-SESSION SUBAGENTS
> **Model = Opus 4.8, effort `xhigh`. ZERO Anthropic API spend.** Materialized via `retrieve
> --agent-results`. Concurrency capped at 4 (waves of 4 over slices of 15). Fail-closed gate over the
> goods manifest's combo count (2,361). Execution plan: [`docs/plans/GOODS_LANE_LABOR_PROFILE_PLAN.md`](../plans/GOODS_LANE_LABOR_PROFILE_PLAN.md).

**Vintage:** `source_vintage = subaward_gap_goods_2361` · **Prompt:** `goods_profile_v1` (the
make-vs-pass-through head) · **model_id:** `claude-opus-4-8:in-session`.

The goods lane is the `psc_is_service=false` half of the subaward-gap worklist — the sibling of the
service lane (`subaward_gap_service_3061`, #911). Where the service head asks *which labor delivers
this service*, the goods head decides a **prior binary first**: does winning this product combo put
dedicated labor on payroll to **make / integrate / overhaul** the good, or is it bought finished and
**passed through** (resale / lease / license → `is_labor_play=false`, one placeholder row)?

## What landed (append-only, vintage-scoped; other vintages byte-untouched)

| Dataset | Before | After | Goods added |
|---|---|---|---|
| `naics_psc_labor_profile` | 11,751 | **14,112** | +2,361 |
| `naics_psc_labor_profile_categories` | 38,822 | **45,333** | +6,511 |

- **Combos:** 2,361 distinct goods `(naics, psc)` across 339 NAICS, $30.1B subaward flow. 386 calls /
  26 slices / 7 waves @ C=4.
- **make vs pass-through:** **1,298 `is_labor_play=true` / 1,063 pass-through** (45% — the honest goods
  skew; a subaward commodity set carries far more finished-good resale than the service lane).
- **Categories:** 6,511 = 5,448 real (play combos, mean 4.2/combo) + 1,063 placeholder (one null-code
  row per non-play combo).
- **off_pattern:** 1,708 / 5,448 real category rows = **31.4%** — much higher than the off_pattern-rare
  service vintage, driven by FPDS mis-codes, sector-92/services-NAICS integration, and
  candidate-domain mismatches (R&D NAICS resolving to biotech candidates for a missile PSC).
- **role_class:** core_deliverable 2,623 · support 2,374 · overhead 451.
- **Resolution:** 4-digit 2,086 · 5-digit 185 · 3-digit 64 · sector 23 · 6-digit 3.
- **Enrichment:** `a_median` on all 5,448 real category rows (100%); EP growth where the resolved code
  is numeric.

> **These distributions are the expected goods signal, NOT drift.** Pre-declared here so a future
> reviewer does not misread the 45% pass-through / 31% off_pattern rates against the service vintage.
> `validate.py`/`verify_datasets.py` gate only on coverage, defects, vocab, and reconciliation — never
> on a play/off_pattern ratio. The one hard gate is: all 2,361 goods combos present or no write.

## The head: `SYSTEM_RULES_GOODS` (`goods_profile_v1`)

A 7-bucket make-vs-pass-through classifier: **MAKE** (mfg 31-33 → production 51-xxxx), **RESELL/COTS**
(wholesale 42 / retail 44-45 → `is_labor_play=false`, thin-true on install/assembly value-add),
**OVERHAUL/REMANUFACTURE** (labor-bearing), **SERVICES-NAICS INTEGRATION** (54 build-to-print →
engineering + production, `off_pattern` into the correct discipline), **CATCH-ALL** (telecom 51 /
facilities 56 / national-security 92 / etc. → classify by deliverable via `off_pattern`),
**LEASE/LICENSE** (`false` unless a managed/operated service is named), **FPDS MIS-CODE** (classify by
the PSC deliverable, not the orthogonal NAICS). Full text in the module and the plan §3.

## Single-knob lane resolver (code)

`NPLP_LANE=product` is the sole switch. `_prompt_head()` selects head + `prompt_version`; the manifest
URI and `source_vintage` default off the lane; a hard desync guard at the top of `retrieve()` refuses
to write if lane and vintage disagree (closing the silent-provenance-corruption path). The default
govcon service path resolves byte-identical. The manifest is written to a **separate** `_goods/` URI
because `build_manifest` writes overwrite-mode (not vintage-scoped) — isolating it prevents clobbering
the service manifest. The shared `gen_workflow.py` slice preamble was neutralized to defer framing to
`system.txt` (lane-agnostic). Code: `materialize_naics_psc_labor_profile.py` +
`labor_profile_insession/gen_workflow.py`.

## Pipeline (throttle-safe, checkpointed, fail-closed)

1. **manifest** (`NPLP_LANE=product`) → 2,361 combos / 339 NAICS / 386 calls → `_goods/` manifest URI.
   `psc_category` populated 2,361/2,361 (from `psc_reference` — no backfill needed).
2. **gold-set pre-flight** — the 6 highest-$ / hardest combos (FPDS mis-code, sector-92 integration,
   build-to-print, clean resell, clean make) classified first and reviewed; head validated with **no
   tuning**. These validated outputs were preserved (not re-run) into the final materialization.
3. **classify** — 380 remaining calls in **3 checkpointed batches** (9/9/8 slices), waves of 4,
   `opus`/`xhigh`, loop-until-complete; R2 checkpoint + validate between batches. 0 limiter contact.
4. **validate** — 386/386, 2,361/2,361 combos, **0 defects / 0 field-missing / 0 SOC-bad / 0 SCA-bad**.
5. **retrieve** — fail-closed 2,361 gate (0 failures), desync guard passed, vintage-scoped upsert →
   both datasets + all BTREE/BITMAP indexes (incl. `psc_category`) + ops ledger.

## Verification (read back from R2)

- profiles 14,112 (distinct combos 14,112, 1:1); categories 45,333.
- provenance 100% `claude-opus-4-8:in-session`; `prompt_version` = `goods_profile_v1` (2,361) +
  `labor_profile_v2` (11,751) — two versions expected.
- reconciliation: 0 combos missing either direction; non-play profiles 1,811 == null-soc placeholder
  rows 1,811 (cross-vintage); all play combos carry ≥1 real category.
- on-disk enum validity: SOC out-of-vocab 0, SCA out-of-vocab 0; play profiles with 0 real cats 0.
- indexes: profile BTREE(naics_code, psc_code) + BITMAP(is_labor_play, resolution_level,
  top_confidence, psc_category); category BTREE(naics_code, psc_code, soc_code, sca_code) +
  BITMAP(role_class, confidence, off_pattern, resolution_level, psc_category).

## Top-$ combos as materialized (the D3 mis-code review gate)

| combo | $ | is_labor_play | cats | disposition |
|---|---|---|---|---|
| `336992 × 6505` DRUGS | $7.06B | **false** | 0 (placeholder) | FPDS mis-code — a tank-mfg NAICS awarded a DRUGS PSC is not running a pharma line; ruled pass-through rather than fabricating a pharmaceutical workforce. Reversible via the vintage-scoped delete if reclassification is wanted. |
| `927110 × 1677` space C2 | $4.22B | true | 7 | sector-92 integration; `off_pattern` (engineering + production) |
| `541712 × 1410` GUIDED MISSILES | $1.40B | true | 6 | build-to-print; `off_pattern` into aerospace eng + production |
| `423430 × 7050` IT components | $1.21B | **false** | 0 | clean COTS resell (computer wholesaler) |
| `336413 × 7610` BOOKS | $0.67B | true | 3 | mis-code → classified as printing (51-51xx) |

## Rebuild / re-run

See [`RUNBOOK.md`](../../pipelines/reference/labor_profile_insession/RUNBOOK.md) → "Goods lane — the
single `NPLP_LANE=product` knob". The vintage-scoped upsert makes a re-run idempotent (delete
`source_vintage=subaward_gap_goods_2361`, then append); govcon and service vintages are never touched.
