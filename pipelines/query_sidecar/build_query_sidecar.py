"""query-sidecar builder — export the frozen Phase 0 mart manifest into one sorted .duckdb artifact.

Phase 1 of the query-sidecar plan (docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md).
Reads each manifest mart from the Lance SoR (s3://data-sink/active/), streams it
through DuckDB, materializes it as a NATIVE DuckDB table physically clustered by
its hop key (CREATE TABLE ... AS SELECT ... ORDER BY), and publishes a single
versioned .duckdb file to R2 under s3://data-sink/query-sidecar/ with a LATEST
pointer (blue-green: new file first, pointer swap second, old files retained).

Naming note: "sidecar" elsewhere in this repo means a derived LANCE dataset
(e.g. pdl_normalized_companies). THIS artifact is different — a DuckDB-native
read-only query file for the warm serving process. Hence "query-sidecar".

Doctrine (docs/reference/03_modal_compute.md):
- standalone Modal app, `modal run` invoked; NO dispatcher, NO Trigger schedule;
- NO modal.Volume — all scratch on the container's ephemeral NVMe at /tmp;
- Python is I/O only; DuckDB performs 100% of transform; Arrow is the only
  interchange (Lance scanner reader -> DuckDB register -> CTAS);
- ops ledger row written on terminal state (success AND failure), never masks
  the build; manual runs skip the Trigger callback.

Build-correctness doctrine (each rule bought with a wasted build, 2026-07-09/10):
- JOIN CONDITIONS ARE PURE EQUALITY KEYS. A probe-side predicate mixed into an
  ON clause (x = y AND a.flag = 'v') planned as a blockwise nested-loop join
  at 83M x 83M — zero-CPU "hang", three builds lost before py-spy --native
  showed PhysicalBlockwiseNLJoin. Fold probe-side gates into CASE-derived
  keys; EXPLAIN-gate every new join in the fixture (assert no NL join).
- Every special-case manifest flag MUST have a dispatch branch in _build_one;
  _preflight() asserts this and runs at build start. An unwired flag silently
  falls through to the generic copy (108M-row "aggregate").
- Fixture-test new special-case SQL through the DISPATCH path, not by calling
  the SQL constant directly — and EXPLAIN the plan at fixture time; a 4-row
  fixture executes a pathological plan instantly.
- Self-join inputs materialize locally (stream -> plain CTAS temp) before
  joining — hygiene that keeps join/sort independent of Arrow-stream pacing.
- Launch with `modal run --detach`: a non-detached app dies with its local
  client (one DNS blip killed a healthy build).

Promotion doctrine (operator-directed, 2026-07-09): the demand-evidence gate
applies to STRUCTURAL growth (new tables/grains/sort copies — recurring cost).
Column-grain adds riding a projection/join the build already performs ship
opportunistically whenever the adjacent question is foreseeable — a rebuild is
a committed fixed cost; columns are free during it and cost a full new cycle
after it. See the sidecar-gaps skill (Mode 2) for the adjacency sweep.

Parity: every mart's DuckDB count must equal ds.count_rows() at the PINNED Lance
version read at build start. Any mismatch fails the run before publish.

Entrypoints:
  modal run pipelines/query_sidecar/build_query_sidecar.py::initdb
  modal run pipelines/query_sidecar/build_query_sidecar.py::run          # full A,B,D + publish
  modal run pipelines/query_sidecar/build_query_sidecar.py::smoke       # Tier A only, smoke/ prefix, no LATEST
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time

import modal

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",   # to_arrow_reader / register surface; below the v2.0 break
        "pylance>=7",        # provides `import lance`; lancedb does NOT re-export it
        "pyarrow>=17",
        "psycopg[binary]>=3.2",
        "boto3>=1.35",
    )
)

app = modal.App("query-sidecar", image=image)

LANCE_BASE = "s3://data-sink/active/"
R2_BUCKET = "data-sink"
R2_PREFIX = "query-sidecar"
SCRATCH_ROOT = "/tmp/query_sidecar"
READ_BATCH_ROWS = 131_072

# ── The frozen manifest (docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md) ──────────
# (dataset, tier, sort_keys, columns_projection_or_None, dest_table_or_None)
# columns=None -> SELECT *. dest defaults to the dataset name.

_SUBAWARD_COLS = [
    # 35-column projection, evidence-cited from every consuming catalyst store
    # (sub_universe_pairs/full pool scans, subout_store rules, lance_store
    # subaward history) + all BTREE'd keys + filter axes. subaward_amount is a
    # source VARCHAR — kept verbatim, with a deliberate numeric cast alongside.
    "subaward_unique_key", "prime_award_unique_key", "subaward_number",
    "prime_award_piid", "prime_award_parent_piid", "usaspending_permalink",
    "subawardee_uei", "subawardee_parent_uei", "subawardee_name",
    "prime_awardee_uei", "prime_awardee_parent_uei", "prime_awardee_name",
    "subaward_amount", "subaward_action_date", "subaward_last_modified_date",
    "subaward_action_date_fiscal_year", "prime_award_naics_code",
    "prime_award_product_or_service_code", "prime_award_awarding_agency_code",
    "prime_award_awarding_agency_name", "prime_award_awarding_sub_agency_code",
    "prime_award_awarding_sub_agency_name", "subawardee_state_code",
    "subawardee_zip_code", "subawardee_country_code",
    "subaward_primary_place_of_performance_state_code",
    "subaward_primary_place_of_performance_country_code",
    "subaward_primary_place_of_performance_address_zip_code",
    "sub_place_of_perform_county_code", "sub_place_of_perform_county_name",
    "prime_awardee_state_code", "prime_awardee_country_code",
    "prime_award_primary_place_of_performance_state_code",
    "prime_award_primary_place_of_performance_country_code",
    "subaward_description",
    # gap-pass-1 E3b/E6: the prime award's base description alongside each
    # subaward (diagnostic tabs read both sides in one pass), and the FSRS
    # designation flags (designation-pulse shapes).
    "prime_award_base_transaction_description",
    "subawardee_business_types",
]

MANIFEST: list[dict] = [
    # ── Tier A — market-grain core ────────────────────────────────────────────
    {"ds": "gtm_entity_behavior_rollup", "tier": "A", "sort": ["uei"]},
    {"ds": "gtm_sam_entities", "tier": "A", "sort": ["uei"]},
    {"ds": "gtm_entity_code_lanes", "tier": "A", "sort": ["uei", "code"]},
    # gap-pass-3 E1: per-firm ranked prime code signature (see _SIGNATURE_SQL).
    # Local from_table build off code_lanes — MUST follow it in the manifest.
    {"ds": "gtm_prime_code_signature", "tier": "A",
     "sort": ["uei", "code_type", "rank_lifetime"],
     "from_table": "gtm_entity_code_lanes", "signature": True, "aggregate": True},
    # audience-spec cycle (2026-07-15, gap E1/E2/E4): the entity-grain audience
    # spine — geo (physical + primary PoP state/county), $ windows (sub/prime
    # 12/24/60mo/lifetime + bands), designation flags, people-coverage counts —
    # in ONE table so an audience count is a single-table predicate instead of
    # a 3-way join. Combined sub+prime totals ride as derived columns (E1);
    # this is also the sidecar serving home for Market-tab audience counts (E4).
    {"ds": "gtm_audience_entities", "tier": "A", "sort": ["uei"],
     "extra_select": (
         "COALESCE(sub_amt_12mo,0)+COALESCE(prime_obl_12mo,0) AS total_amt_12mo, "
         "COALESCE(sub_amt_24mo,0)+COALESCE(prime_obl_24mo,0) AS total_amt_24mo, "
         "COALESCE(sub_amt_60mo,0)+COALESCE(prime_obl_60mo,0) AS total_amt_60mo, "
         "COALESCE(sub_amt_lifetime,0)+COALESCE(prime_obl_lifetime,0) AS total_amt_lifetime")},
    {"ds": "gtm_entity_geo", "tier": "A", "sort": ["uei"]},
    {"ds": "gtm_naics_psc_pairs", "tier": "A", "sort": ["naics_code", "psc_code"]},
    {"ds": "naics_reference", "tier": "A", "sort": ["naics_code"]},
    # ── combo-grain language layers (gap-pass-5: two independent sessions hit
    # the same gap — plain-language rendering joins these onto sidecar code
    # sets). vertical_map rides the sweep (equipment_intensity dial, 279 rows);
    # naics_psc_labor_dim deliberately skipped (flattened dup of profile⋈cats).
    {"ds": "naics_psc_labor_profile", "tier": "A", "sort": ["naics_code", "psc_code"]},
    {"ds": "naics_psc_deliverable", "tier": "A", "sort": ["naics_code", "psc_code"]},
    {"ds": "naics_psc_labor_profile_categories", "tier": "A",
     "sort": ["naics_code", "psc_code", "rank"]},
    {"ds": "naics_psc_vertical_map", "tier": "A", "sort": ["naics_code", "psc_code"]},
    # equipment-needs cycle (2026-07-11): the combo->equipment-needs verdicts
    # (LLM proposed_equipment_needs + the deterministic heavy-iron slice:
    # in_scope / equipment_buckets / primary_bucket). 1 row/combo, sorted
    # (naics, psc) so a combo predicate prunes AND it co-clusters with every
    # other naics_psc_* mart + txn_events_combo for the demand join.
    {"ds": "naics_psc_equipment_needs", "tier": "A", "sort": ["naics_code", "psc_code"]},
    {"ds": "psc_reference", "tier": "A", "sort": ["psc_code"]},
    # ── Tier B — Cycle B rollups (built-but-unwired; this is their serving lane)
    {"ds": "gtm_txn_events_slim", "tier": "B", "sort": ["uei", "action_date"]},
    {"ds": "gtm_txn_recipient_month_rollup", "tier": "B", "sort": ["uei"]},
    # audience-spec cycle (2026-07-15, gap E3): laser-in sort copy — the same
    # 34M-row fact re-clustered (action_type_code, month) so "entities with N
    # actions of type X in window Y" prunes instead of full-scanning (measured
    # 2.0s unpruned on serving). Local re-sort, no R2 read; must follow base.
    {"ds": "gtm_txn_recipient_month_rollup", "tier": "B",
     "dest": "txn_recipient_month_by_type", "sort": ["action_type_code", "month"],
     "from_table": "gtm_txn_recipient_month_rollup"},
    {"ds": "gtm_award_recipient_rollup", "tier": "B", "sort": ["uei"]},
    {"ds": "gtm_award_expiry_months", "tier": "B", "sort": ["uei", "end_month"]},
    {"ds": "gtm_prime_pop_lanes", "tier": "B", "sort": ["uei"]},
    # ── Tier C — benchmark-promoted giants (Phase 2 verdicts) ────────────────
    # gap-pass-2 E2: award-grain ordering windows (see _ORDERING_WINDOWS_SQL) —
    # MUST build before usaspending_fpds_prime_award_state, which joins it.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "award_ordering_windows",
     "sort": ["contract_award_unique_key"], "ordering_windows": True, "aggregate": True,
     "cols": ["contract_award_unique_key", "ordering_period_end_date", "action_date"]},
    # award-grain rows + exact expiring: 96s live-lane -> ms-class local; also
    # removes the expiry_months month-grain approximation on two-lane phrases.
    # gap-pass-2 E2: parent_window build widens it with own + resolved-parent
    # ordering/end-window columns (see _PARENT_WINDOW_SQL).
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C", "sort": ["current_end_date"],
     "parent_window": True},
    # gap-pass-3 E1 residual: open-window position substrate (see
    # _POSITION_ORDERS_SQL) — local build off award_state, must follow it.
    {"ds": "gtm_position_orders", "tier": "C", "sort": ["contract_award_unique_key"],
     "from_table": "usaspending_fpds_prime_award_state", "position_orders": True,
     "aggregate": True},
    # equipment-needs cycle (2026-07-11): combo-grain award-lifecycle-state mart —
    # active/terminated/expired splits aggregated at (naics, psc) off the 82.8M
    # award_state table (local, no R2 read; must follow award_state). "Active"
    # ($, awards, distinct primes) is the geo product's denominator; the total +
    # terminated + expired splits and ceiling-headroom ride the SAME group-by
    # scan so "what share is active" / "how many primes" / "headroom" need no new
    # cycle. Aggregate -> non-empty parity. Sorted (naics, psc) for the demand join.
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C", "dest": "combo_award_active_state",
     "sort": ["naics_code", "psc_code"],
     "from_table": "usaspending_fpds_prime_award_state", "combo_active": True,
     "aggregate": True},
    # inferred-code semi-join legs: sorted by (code_type, code) so a code
    # predicate prunes to a handful of row groups instead of a 263M/160M scan.
    {"ds": "gtm_entity_inferred_primeable_codes", "tier": "C", "sort": ["code_type", "code"]},
    {"ds": "gtm_entity_inferred_subbable_codes", "tier": "C", "sort": ["code_type", "code"]},
    # transactions ROW serving (bundle cycle): the exact 16-column wire contract
    # (market_registry.TRANSACTION_RESULT_COLUMNS) projected from the canonical —
    # closes the last NotServable tier. Every transactions filter column is
    # among the 16, so compiled predicates run verbatim. Sorted by action_date
    # (every phrase carries a time window).
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "sort": ["action_date"],
     "dest": "txn_rows", "cols": [
         "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid",
         "recipient_uei", "recipient_name", "action_date", "action_type_code",
         "action_type_description", "subcontracting_plan", "subcontracting_plan_desc",
         "federal_action_obligation", "base_and_all_options_value", "naics_code",
         "product_or_service_code", "awarding_agency_code", "awarding_agency_name",
         # pricing-terms cycle (2026-07-15): row-serving carries the pricing
         # structure verbatim (code + source desc) — additive to the 16-col
         # wire contract, existing consumers select by name.
         "type_of_contract_pricing_code", "type_of_contract_pric_desc"]},
    # award place-of-performance centroids (bundle cycle): enables ad-hoc geo SQL
    # (bounding-box + haversine) and PoP-grain geometry; sorted state/zip5 so
    # spatial predicates prune row groups.
    {"ds": "usaspending_award_pop_centroids", "tier": "C", "sort": ["state_code", "zip5"]},
    # ── combo-portrait layer ──────────────────────────────────────────────────
    # ONE fact, every dial: combo (substr rollups), time (action_date + fy),
    # action codes, subk plan, topology (award_state join at build), geo
    # (pop state/county), agency/sub-agency. Sorted combo-first; geo-sorted
    # second copy below so county/state-anchored questions prune too.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "txn_events_combo",
     "sort": ["naics_code", "psc_code", "action_date"], "combo_fact": True},
    {"ds": "txn_events_combo", "tier": "C", "dest": "txn_events_combo_by_geo",
     "sort": ["pop_state", "pop_county_fips", "action_date"],
     "from_table": "txn_events_combo"},
    # pricing-terms cycle (2026-07-15, operator-directed): entity-event-GEO
    # month rollup — the phrase layer's disclosed refusal ("in <state> (PoP)
    # on event verbs": gtm_txn_recipient_month_rollup carries no PoP). Grain
    # uei × action_type × pop_state/county × month off the local fact; sorted
    # so "entities with action X in state S in window W" prunes. County rides
    # the same GROUP BY (the zoom-in next question). Aggregate -> non-empty
    # parity. Local build, must follow txn_events_combo.
    {"ds": "txn_events_combo", "tier": "C", "dest": "txn_recipient_month_pop",
     "sort": ["action_type_code", "pop_state", "pop_county_fips", "month"],
     "from_table": "txn_events_combo", "month_pop_rollup": True, "aggregate": True},
    # award-grain sub-out rollup: joins the fact/award_state on award key —
    # "is this combo/geo/agency getting subbed out more or less".
    {"ds": "usaspending_subaward_canonical", "tier": "C", "dest": "award_subout_rollup",
     "sort": ["prime_award_unique_key"], "subout_rollup": True, "aggregate": True,
     "cols": ["prime_award_unique_key", "subawardee_uei", "subaward_amount",
              "subaward_action_date"]},
    # sub-agency vocab (code -> majority name), same dedup rule as agency_vocab.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "agency_sub_vocab",
     "sort": [], "agency_sub_vocab": True, "aggregate": True,
     "cols": ["awarding_sub_agency_code", "awarding_sub_agency_name"]},
    # gap-pass-2 E1: PoP country vocab (code -> majority name), same dedup rule.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "country_vocab",
     "sort": [], "country_vocab": True, "aggregate": True,
     "cols": ["primary_place_of_performance_country_code", "pop_country_name"]},
    # prime-award descriptions (history-tab use case: a UEI's awards + their
    # requirement descriptions — or the visibly glaring lack thereof). Award
    # grain from award_canonical, sorted recipient_uei so the tab read prunes.
    # Per-ACTION text (canonical.transaction_description, 108M) stays gated —
    # ~2x the growth for mod-timeline detail no workload needs yet.
    {"ds": "usaspending_award_canonical", "tier": "C", "dest": "award_descriptions",
     "sort": ["recipient_uei"],
     "cols": ["generated_unique_award_id", "contract_award_unique_key",
              "recipient_uei", "award_id_piid", "description",
              # gap-pass-1 E4: solicitation join keys for the PDF-handoff workstream
              "solicitation_identifier", "solicitation_date"]},
    # gap-pass-1 E2: award-grain "carries a subcontracting plan" latest-state for
    # arbitrary/closed populations (plan lives at txn grain; this pins the
    # latest-action plan flag per award).
    # pricing-terms cycle (2026-07-15): the pricing/financing/size-determination
    # latest-state rides the SAME per-award arg_max scan — award_state (Lance)
    # carries no pricing columns, so this is the award-grain pricing home
    # (join usaspending_fpds_prime_award_state on contract_award_unique_key).
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "award_plan_state",
     "sort": ["contract_award_unique_key"], "plan_state": True, "aggregate": True,
     "cols": ["contract_award_unique_key", "subcontracting_plan", "action_date",
              "type_of_contract_pricing_code", "contract_financing",
              "contracting_officers_determination_of_business_size"]},
    # pricing-terms cycle (2026-07-15, gap E1): action-type vocabulary — the
    # empirical majority description per code (source pairs are messy: 102
    # raw tuples, truncated/null/cross-contaminated variants) FULL-joined with
    # the authored semantic layer (plain-english phrase, family, more-work /
    # funding-released flags) consumed by the phrase/query-search rebuild.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "action_type_vocab",
     "sort": [], "action_type_vocab": True, "aggregate": True,
     "cols": ["action_type_code", "action_type_description"]},
    # pricing-terms cycle (2026-07-15, adjacency): name resolution for every
    # code space the fact just gained — one scan, one (field, code, name)
    # table, same majority-dedup rule as the agency/country vocabs.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "fpds_code_vocab",
     "sort": [], "fpds_code_vocab": True, "aggregate": True,
     "cols": ["type_of_contract_pricing_code", "type_of_contract_pric_desc",
              "contract_financing", "contract_financing_descrip",
              "performance_based_service_acquisition_code", "performance_based_se_desc",
              "contracting_officers_determination_of_business_size", "contracting_officers_desc",
              "labor_standards_code", "labor_standards_descrip"]},
    # gtm_subaward_recipient_code_evidence (92M) stays OUT: no phrase.v2 shape
    # touches it (subout drill-down only) — remains gated pending a workload.
    # ── Tier D — recipe/relationship substrate ────────────────────────────────
    {"ds": "gtm_prime_sub_pairs", "tier": "D", "sort": ["prime_uei"]},
    {"ds": "gtm_prime_sub_pairs", "tier": "D", "sort": ["sub_uei"],
     "dest": "gtm_prime_sub_pairs_by_sub"},          # 2nd copy, sub-side clustering (269k rows — free)
    {"ds": "gtm_sub_universe_pairs", "tier": "D", "sort": ["target_uei"]},
    {"ds": "gtm_sub_universe_targets", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_prime_combo_lanes", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_sub_combo_lanes", "tier": "D", "sort": ["uei"]},
    # pricing-terms cycle (2026-07-15, gap E3): farm-out SHARE — the lanes gain
    # the prime-obligation denominators + precomputed shares via a row-
    # preserving LEFT JOIN to gtm_prime_combo_lanes (1 row/uei×naics×psc both
    # sides, pure-equality keys; manifest order guarantees combo_lanes is
    # already built). Exact-parity gate still applies.
    {"ds": "gtm_prime_farmout_combo_lanes", "tier": "D", "sort": ["uei"],
     "farmout_share": True},
    {"ds": "gtm_prime_vehicle_lanes", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_open_awards", "tier": "D", "sort": ["recipient_uei"]},
    {"ds": "gtm_prime_demand_events", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_primes_by_recipient_code", "tier": "D", "sort": ["recipient_code"]},
    # subout-rate cycle (2026-07-15, operator-directed): the cube gains its
    # denominators + rate + within-context share via a row-preserving LEFT
    # JOIN to gtm_entity_code_lanes (side='prime' pre-filtered to a temp so
    # join keys stay pure equalities; prime lanes are unique per
    # (uei, code_type, code) — probe-verified 2026-07-15). Exact parity kept.
    # Manifest order guarantees gtm_entity_code_lanes (Tier A) builds first.
    {"ds": "gtm_prime_subout_by_recipient_code", "tier": "D",
     "sort": ["prime_awardee_uei"], "subout_rate": True},
    # recipient-shape-anchored sort copy: every read filters ONE evidence lens
    # (the four recipient_code_source lenses overlap — summing across them
    # double-counts), then the recipient code. "Primes that route ≥N% of
    # <context> work to subs who prime in Y" prunes here instead of scanning
    # 11.8M (measured 2.6 s unpruned). Local re-sort, must follow base.
    {"ds": "gtm_prime_subout_by_recipient_code", "tier": "D",
     "dest": "gtm_prime_subout_by_code",
     "sort": ["recipient_code_source", "recipient_code_type", "recipient_code"],
     "from_table": "gtm_prime_subout_by_recipient_code"},
    {"ds": "gtm_subbed_under_to_primed_in_cooccurrence", "tier": "D", "sort": ["subbed_under_code"]},
    {"ds": "gtm_sub_profiles", "tier": "D", "sort": ["uei"]},
    {"ds": "govcon_subawardee_profiles", "tier": "D", "sort": ["sub_uei"]},
    {"ds": "usaspending_subaward_canonical", "tier": "D", "sort": ["prime_awardee_uei"],
     "cols": _SUBAWARD_COLS, "dest": "subaward_canonical_slim",
     "extra_select": "TRY_CAST(subaward_amount AS DOUBLE) AS subaward_amount_num"},
    {"ds": "usaspending_subaward_canonical", "tier": "D", "sort": ["subawardee_uei"],
     "cols": _SUBAWARD_COLS, "dest": "subaward_canonical_slim_by_sub",
     "extra_select": "TRY_CAST(subaward_amount AS DOUBLE) AS subaward_amount_num"},
    # ── UCC debt layer (operator-directed cycle 2026-07-10): "carries debt?"
    # (overlap, uei grain) + "when / from whom / against what" (filing grain,
    # dated — interleaves with the award event stream for win-then-borrow).
    {"ds": "sam_ucc_debtor_overlap", "tier": "D", "sort": ["uei"]},
    {"ds": "sam_ucc_filings", "tier": "D", "sort": ["uei", "first_filing_date"]},
    # lender-surface cycle (operator-directed 2026-07-10): the classified
    # lender grain + the FDIC/NCUA name authorities (bracket sources) + the
    # equipment-finance candidate list (read-only reconciliation target).
    {"ds": "sam_ucc_lenders", "tier": "D", "sort": ["lender_key"]},
    {"ds": "fdic_institutions", "tier": "D", "sort": ["name"],
     "cols": ["name", "cert", "active", "city", "stalp", "stname", "zip",
              "webaddr", "asset", "charter"]},
    {"ds": "ncua_credit_unions", "tier": "D", "sort": ["credit_union_name"],
     "cols": ["credit_union_name", "charter_number", "city_mailing_address",
              "state_mailing_address", "zip_code_mailing_address",
              "credit_union_type", "members", "total_assets"]},
    {"ds": "equipment_finance_candidates", "tier": "D", "sort": ["company_name"],
     "cols": ["record_id", "company_name", "company_domain", "domain_norm",
              "linkedin_url", "linkedin_url_norm", "verdict", "source",
              "landed_at"]},  # -raw_payload
    # equipment-needs cycle (2026-07-11): supply-side shop profiles. Classifier
    # (is_equipment_provider verdict, 4,700; NOT unique on domain — 201 dupes,
    # deduped in v_equipment_supply), scraped inventory (matchmaking, 1/domain),
    # and the award-overlap capability score (golden_overlap, 1/domain, firm_domain
    # == domain_norm). All key on domain -> join firmographics_blitz for name/geo,
    # and supported/qualified PSC lists join combo demand on the shared PSC taxonomy.
    {"ds": "equipment_provider", "tier": "D", "sort": ["domain_norm"],
     "cols": ["record_id", "company_domain", "domain_norm", "is_equipment_provider",
              "mode", "confidence", "reasoning", "steps_taken", "evidence_url",
              "evidence_snippet", "source", "landed_at", "materialized_at"]},  # -raw_payload
    {"ds": "equipment_matchmaking", "tier": "D", "sort": ["domain_norm"],
     "cols": ["domain_norm", "supported_pscs", "verified_inventory_matches",
              "matched_psc_count", "materialized_at"]},  # -justification_payload
    {"ds": "equipment_rental_golden_overlap", "tier": "D", "sort": ["firm_domain"]},
    {"ds": "federal_sites_lance", "tier": "D", "sort": ["state_code", "zip5"]},
    {"ds": "firmographics_blitz", "tier": "D", "sort": ["domain_norm"]},
    # ── gap-pass-4: identity/enrichment coverage layer ────────────────────────
    # Demand: SIDECAR_GAP_REPORT_2026-07-10-funding-tab-pdl-match (PDL bridge) +
    # operator-recorded next-questions (icypeas/LinkedIn coverage on the same
    # populations). "Does population X have PDL / LinkedIn / scraped-profile
    # coverage" becomes one pruned statement. pdl_companies (raw twin of
    # normalized, 35M) deliberately excluded — linkedin_slug carries the URL.
    {"ds": "bridge_sam_pdl", "tier": "D", "sort": ["uei"]},
    {"ds": "pdl_normalized_companies", "tier": "D", "sort": ["pdl_company_id"]},
    {"ds": "icypeas_company_scrapes", "tier": "D", "sort": ["uei"],
     "cols": ["uei", "company_linkedin_url", "name", "domain", "li_source",
              "source_class", "money24_usd", "in_dsbs", "status", "linkedin_url",
              "website", "domain_norm", "industry", "headcount_range",
              "employee_count", "country", "batch_label", "scraped_at"]},  # -raw_result
    {"ds": "icypeas_dsbs_company_profiles", "tier": "D", "sort": ["uei"]},
    {"ds": "icypeas_person_profiles", "tier": "D", "sort": ["uei"],
     "cols": ["file_id", "file_name", "order_idx", "sam_person_id", "uei",
              "input_url", "icypeas_item_id", "status", "found",
              "person_linkedin_url_norm", "drained_at"]},  # -raw, -li blobs
    {"ds": "icypeas_person_profile_scrapes", "tier": "D",
     "sort": ["person_linkedin_url_norm"]},
    {"ds": "bridge_dsbs_pdl_linkedin", "tier": "D", "sort": ["uei"]},
    {"ds": "dsbs_poc_linkedin", "tier": "D", "sort": ["uei"]},
    {"ds": "exa_person_linkedin_candidates", "tier": "D", "sort": ["uei"],
     "cols": ["sam_person_id", "uei", "first_name", "last_name", "company_name",
              "company_domain", "aff_used", "query", "exa_linkedin_url",
              "exa_title", "is_in_profile", "n_results", "cost_usd",
              "searched_at", "source_batch", "query_variant"]},  # -raw_results_json
    # ── labor occupation-grain layer (gap-pass-6: SIDECAR_GAP_REPORT
    # 2026-07-11-labor-occupation-grain). One connected subgraph: award
    # (naics,psc) -> combo labor layer -> sca/soc -> wage (floor:
    # wd_rates via county_coverage + fips crosswalk; market: soc_state_wage)
    # -> uei union exposure. county_coverage rides Entry 2's own fallback
    # join (the county hop is part of the demanded shape). ~600k rows total.
    {"ds": "sam_wd_rates_structured", "tier": "D",
     "sort": ["wd_id", "occupation_code"]},
    {"ds": "sam_wd_county_coverage", "tier": "D", "sort": ["wd_id"]},
    {"ds": "sam_county_fips_crosswalk", "tier": "D",
     "sort": ["state_code", "sam_county_name"]},
    {"ds": "soc_state_wage", "tier": "D", "sort": ["soc_code", "prim_state"]},
    {"ds": "sca_soc_crosswalk", "tier": "D", "sort": ["occupation_code"]},
    {"ds": "dol_sca_occupations", "tier": "D", "sort": ["occupation_code"]},
    {"ds": "olms_cba_crosswalk", "tier": "D", "sort": ["uei"]},
    # ── labor pricing + entry hop (labor-pricing cycle 2026-07-14: SIDECAR_GAP_
    # REPORT 2026-07-14-labor-pricing-entry-hop; declared platform-app live-card
    # demand). naics_labor_share: the composed award-dollar pricing scalar
    # (loaded_labor_share = SUSB payroll_share × ECEC burden, BEA cross-check,
    # provenance flags). occupation_alias_lookup: free-text role name → SOC/SCA
    # (O*NET + SCA titles, normalized; sorted on the probe key alias_norm).
    # Both SELECT * (tiny), row-preserving → exact-parity gate. The recurring
    # composite (alias → combos → priced) ships as v_role_priced_combos.
    {"ds": "naics_labor_share", "tier": "D", "sort": ["naics_code"]},
    {"ds": "occupation_alias_lookup", "tier": "D", "sort": ["alias_norm", "code"]},
    {"ds": "gtm_sam_people", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_sam_person_contactability", "tier": "D", "sort": ["sam_person_id"]},
    {"ds": "sam_pocs", "tier": "D", "sort": ["uei"]},
    {"ds": "sam_master_entities", "tier": "D", "sort": ["uei"]},
    {"ds": "people_canonical", "tier": "D", "sort": ["canonical_person_id"]},
    # ── VA veteran demand-side cluster (operator-directed cycle 2026-07-12;
    # PR #1141 ingest). County-grain (5-char FIPS) demand denominator for the
    # VA C&P exam lane (naics 621111 × psc Q403): rank where clinician-staffing
    # demand outruns local medical-labor supply. Both key on fips → join
    # txn_events_combo_by_geo.pop_county_fips (served) / SAM physical_state.
    # Adjacency sweep: disability carries scd severity bands + age + sex (the
    # next-question columns, ride free via SELECT *); vetpop_total keeps the
    # 31 projection years (FY2023-2053) so the per-county veteran TREND is one
    # statement. Parked structural: va_vetpop_county (781k age×sex×year detail)
    # — no demand yet, 8× the row weight, recurs every rebuild.
    {"ds": "va_vetpop_county_total", "tier": "D", "sort": ["fips", "snapshot_year"]},
    {"ds": "va_disability_comp_county", "tier": "D", "sort": ["fips", "fiscal_year"]},
    # ── entity-inflection cluster (operator-directed cycle 2026-07-13; bridges
    # gap-report 2026-07-12 Gaps B+C, "structural change in the last N days").
    # sam_master_profile_deltas: 1 row/(uei, field, to_label) SAM vintage-diff
    # change event (cage appears/moves, high-liability NAICS added w/ sb_flag,
    # entity_structure, legal name, purpose, extract status; set add/remove for
    # bus types / NAICS / PSC). Sorted (uei, to_date) so a "this UEI's recent
    # changes" read prunes AND a windowed "everyone who changed since D" read
    # prunes on to_date. gtm_fpds_entity_signal_events: the day-precision FPDS
    # adjacency — first/last txn day per (uei, cage) + verified structured JV/8a
    # flags; sorted uei. Both 1:1 (row-preserving) → exact-parity gate.
    {"ds": "sam_master_profile_deltas", "tier": "B", "sort": ["uei", "to_date"]},
    {"ds": "gtm_fpds_entity_signal_events", "tier": "B", "sort": ["uei"]},
]

# agency vocab: deduped (code, name) off usaspending_award_canonical — mirrors
# market_store._dedupe_agency_pairs (NULL-guarded, majority name per code,
# lexicographic tiebreak). ~136 rows off 30.7M — streamed, never materialized wide.
_AGENCY_VOCAB_SQL = """
CREATE TABLE agency_vocab AS
WITH pairs AS (
    SELECT awarding_agency_code AS code, awarding_agency_name AS name, count(*) AS n
    FROM src
    WHERE awarding_agency_code IS NOT NULL AND awarding_agency_code <> ''
      AND awarding_agency_name IS NOT NULL AND awarding_agency_name <> ''
    GROUP BY 1, 2
)
SELECT code, name
FROM (SELECT code, name,
             row_number() OVER (PARTITION BY code ORDER BY n DESC, name) AS rn
      FROM pairs)
WHERE rn = 1
ORDER BY code
"""

# combo-fact source columns (verified present on the canonical 2026-07-09)
_COMBO_SRC_COLS = [
    "recipient_uei", "contract_award_unique_key", "action_date", "action_type_code",
    "subcontracting_plan", "award_type_code", "naics_code", "product_or_service_code",
    "awarding_agency_code", "awarding_sub_agency_code",
    # gap-pass-1 E7: funding side (who PAYS vs who signs) — names resolve via the
    # existing agency_vocab / agency_sub_vocab joins (shared code space).
    "funding_agency_code", "funding_sub_agency_code",
    "primary_place_of_performance_state_code", "pop_county_fips", "pop_county_name",
    # gap-pass-2 E1: PoP country (ISO3) — overseas vs unstated split on the
    # no-US-state bucket; names resolve via country_vocab.
    "primary_place_of_performance_country_code",
    # gap-pass-2 adjacency: the set-aside dial ("how much of this market is
    # set-aside") — rides the build already paid for.
    "type_of_set_aside_code",
    # pricing-terms cycle (2026-07-15, gap E2): the cash-flow-shape dials —
    # pricing structure (FFP vs cost-reimb vs T&M), financing arrangement,
    # PBA flag, CO size determination (effective net-15 tier), SCA/DBA labor
    # standards. Codes only on the fact; names resolve via fpds_code_vocab.
    "type_of_contract_pricing_code", "contract_financing",
    "performance_based_service_acquisition_code",
    "contracting_officers_determination_of_business_size",
    "labor_standards_code",
    "federal_action_obligation",
]

# LEFT JOIN preserves the canonical's row count (award_state is 1 row/key), so
# the exact-parity gate still applies to the fact. Federal FY: Oct+ -> year+1.
_COMBO_FACT_SQL = """
CREATE TABLE txn_events_combo AS
SELECT
    t.recipient_uei                                   AS uei,
    t.contract_award_unique_key                       AS award_key,
    t.action_date,
    (year(t.action_date) + CASE WHEN month(t.action_date) >= 10 THEN 1 ELSE 0 END)::SMALLINT AS fy,
    t.action_type_code,
    t.subcontracting_plan,
    a.award_topology,
    t.award_type_code,
    t.naics_code,
    t.product_or_service_code                         AS psc_code,
    t.awarding_agency_code,
    t.awarding_sub_agency_code,
    t.funding_agency_code,
    t.funding_sub_agency_code,
    t.primary_place_of_performance_state_code         AS pop_state,
    t.pop_county_fips,
    t.pop_county_name,
    t.primary_place_of_performance_country_code       AS pop_country_code,
    t.type_of_set_aside_code,
    t.type_of_contract_pricing_code                   AS pricing_code,
    t.contract_financing                              AS financing_code,
    t.performance_based_service_acquisition_code      AS pba_code,
    t.contracting_officers_determination_of_business_size AS co_business_size,
    t.labor_standards_code,
    t.federal_action_obligation                       AS obligation
FROM src t
LEFT JOIN src_aw a ON a.contract_award_unique_key = t.contract_award_unique_key
ORDER BY t.naics_code, t.product_or_service_code, t.action_date
"""

_SUBOUT_ROLLUP_SQL = """
CREATE TABLE award_subout_rollup AS
SELECT prime_award_unique_key,
       count(*)                                       AS sub_ct,
       count(DISTINCT subawardee_uei)                 AS distinct_subs,
       sum(TRY_CAST(subaward_amount AS DOUBLE))       AS sub_amount_total,
       min(subaward_action_date)                      AS first_sub_date,
       max(subaward_action_date)                      AS last_sub_date
FROM src
WHERE prime_award_unique_key IS NOT NULL
GROUP BY 1
ORDER BY prime_award_unique_key
"""

_PLAN_STATE_SQL = """
CREATE TABLE award_plan_state AS
SELECT contract_award_unique_key,
       arg_max(subcontracting_plan, action_date)  AS latest_plan,
       arg_max(type_of_contract_pricing_code, action_date)
                                                  AS latest_pricing_code,
       arg_max(contract_financing, action_date)   AS latest_financing_code,
       arg_max(contracting_officers_determination_of_business_size, action_date)
                                                  AS latest_business_size,
       max(action_date)                           AS latest_action_date,
       count(*)                                   AS actions
FROM src
WHERE contract_award_unique_key IS NOT NULL
GROUP BY 1
ORDER BY contract_award_unique_key
"""

# pricing-terms cycle (2026-07-15, gap E1): action-type vocabulary. Empirical
# leg = majority description per code (same dedup rule as the other vocabs;
# collapses the 102 messy source tuples). Authored leg = the session-verified
# semantic layer: subject-first query phrase, family, and the two aggregation
# flags (G carries BOTH more-work and funding-released — an option exercise
# turns on new work AND obligates its money; C is the only pure-money event).
# The NULL-code row is the base/initial award (FPDS stamps action type on
# modifications only). FULL OUTER JOIN keeps any future source code visible
# even before it is authored; pure single-key equality (EXPLAIN-gated).
_ACTION_TYPE_VOCAB_SQL = """
CREATE TABLE action_type_vocab AS
WITH pairs AS (
    SELECT action_type_code AS code, action_type_description AS name, count(*) AS n
    FROM src
    WHERE action_type_code IS NOT NULL AND action_type_code <> ''
      AND action_type_description IS NOT NULL AND action_type_description <> ''
    GROUP BY 1, 2
),
empirical AS (
    SELECT code, name
    FROM (SELECT code, name,
                 row_number() OVER (PARTITION BY code ORDER BY n DESC, name) AS rn
          FROM pairs)
    WHERE rn = 1
),
authored (code, plain_english, family, is_more_work, is_funding_released) AS (
    VALUES
    (NULL, 'won a new contract',                              'new_award',   FALSE, FALSE),
    ('A',  'picked up additional work on a new agreement',    'more_work',   TRUE,  FALSE),
    ('B',  'had more work added within scope',                'more_work',   TRUE,  FALSE),
    ('C',  'received additional funding',                     'funding_only', FALSE, TRUE),
    ('D',  'got a change order',                              'more_work',   TRUE,  FALSE),
    ('E',  'was terminated for default',                      'termination', FALSE, FALSE),
    ('F',  'was terminated for convenience',                  'termination', FALSE, FALSE),
    ('G',  'had an option year exercised',                    'more_work',   TRUE,  TRUE),
    ('H',  'had a letter contract definitized',               'definitization', FALSE, FALSE),
    ('J',  'took over a contract by novation',                'admin',       FALSE, FALSE),
    ('K',  'had a contract closed out',                       'closeout',    FALSE, FALSE),
    ('L',  'had a change order definitized',                  'more_work',   TRUE,  FALSE),
    ('M',  'had an administrative action',                    'admin',       FALSE, FALSE),
    ('N',  'had a contract cancelled',                        'termination', FALSE, FALSE),
    ('P',  're-represented after a merger or acquisition',    'admin',       FALSE, FALSE),
    ('R',  're-represented its size or status',               'admin',       FALSE, FALSE),
    ('S',  'had its contract ID changed',                     'admin',       FALSE, FALSE),
    ('T',  'had a contract transferred',                      'admin',       FALSE, FALSE),
    ('V',  'changed its name or entity ID',                   'admin',       FALSE, FALSE),
    ('W',  'changed its address',                             'admin',       FALSE, FALSE),
    ('X',  'was terminated for cause',                        'termination', FALSE, FALSE),
    ('Y',  'added a subcontracting plan',                     'admin',       FALSE, FALSE)
)
SELECT COALESCE(a.code, e.code)  AS action_type_code,
       e.name                    AS source_description,
       a.plain_english, a.family, a.is_more_work, a.is_funding_released
FROM authored a
FULL OUTER JOIN empirical e ON e.code = a.code
ORDER BY action_type_code NULLS FIRST
"""

# pricing-terms cycle (2026-07-15, adjacency rider): one (field, code, name)
# vocabulary covering every code space the fact gained this cycle — majority
# name per code, same dedup rule as agency/country vocabs, five legs off the
# SAME single scan. Source noise (verbatim text appearing in the code column,
# e.g. 'SMALL BUSINESS') dedups to itself harmlessly.
_FPDS_CODE_VOCAB_SQL = """
CREATE TABLE fpds_code_vocab AS
WITH raw AS (
    -- src is a one-shot Arrow stream: FIVE separate `FROM src` legs would
    -- read an exhausted reader (silent empty legs). One scan, lateral UNNEST.
    SELECT u.field, u.code, u.name
    FROM src, UNNEST([
        {'field': 'pricing',
         'code': type_of_contract_pricing_code, 'name': type_of_contract_pric_desc},
        {'field': 'financing',
         'code': contract_financing, 'name': contract_financing_descrip},
        {'field': 'performance_based',
         'code': performance_based_service_acquisition_code, 'name': performance_based_se_desc},
        {'field': 'business_size_determination',
         'code': contracting_officers_determination_of_business_size, 'name': contracting_officers_desc},
        {'field': 'labor_standards',
         'code': labor_standards_code, 'name': labor_standards_descrip}
    ]) AS t(u)
),
pairs AS (
    SELECT field, code, name, count(*) AS n
    FROM raw
    WHERE code IS NOT NULL AND code <> '' AND name IS NOT NULL AND name <> ''
    GROUP BY 1, 2, 3
)
SELECT field, code, name
FROM (SELECT field, code, name,
             row_number() OVER (PARTITION BY field, code ORDER BY n DESC, name) AS rn
      FROM pairs)
WHERE rn = 1
ORDER BY field, code
"""

# pricing-terms cycle (2026-07-15, gap E3): the farm-out lanes gain their
# denominators + shares. farmout_base is the locally-materialized Lance
# stream (hygiene rule); gtm_prime_combo_lanes is already built (manifest
# order). Both sides are 1 row/(uei, naics, psc) -> row-preserving LEFT JOIN,
# exact-parity gate applies. Keys are pure equalities (EXPLAIN-gated).
_FARMOUT_SHARE_SQL = """
CREATE TABLE gtm_prime_farmout_combo_lanes AS
SELECT f.*,
       c.prime_obl_24mo, c.prime_obl_60mo, c.prime_obl_lifetime,
       c.n_txns_lifetime                                        AS prime_txns_lifetime,
       f.farmout_amt_24mo    / NULLIF(c.prime_obl_24mo, 0)      AS farmout_share_24mo,
       f.farmout_amt_60mo    / NULLIF(c.prime_obl_60mo, 0)      AS farmout_share_60mo,
       f.farmout_amt_lifetime / NULLIF(c.prime_obl_lifetime, 0) AS farmout_share_lifetime
FROM farmout_base f
LEFT JOIN gtm_prime_combo_lanes c
       ON c.uei = f.uei
      AND c.naics_code = f.naics_code
      AND c.psc_code = f.psc_code
ORDER BY f.uei
"""

# subout-rate cycle (2026-07-15, operator-directed): the sub-out cube gains
# the prime's in-context obligations (denominator family), the demanded rate,
# and the within-lens context share. subout_base is the locally-materialized
# Lance stream; prime_lanes is the side='prime' slice of the already-built
# code lanes (probe-side gate applied BEFORE the join so the ON clause stays
# pure equality — the NL-join trap). Join is row-preserving (prime lanes
# unique per (uei, code_type, code)) → exact-parity gate. subout_rate can
# legitimately exceed 1 (pass-through vehicles; subaward $ vs obligated $).
_SUBOUT_RATE_SQL = """
CREATE TABLE gtm_prime_subout_by_recipient_code AS
SELECT s.*,
       p.obl_24mo          AS prime_obl_24mo_in_context,
       p.obl_60mo          AS prime_obl_60mo_in_context,
       p.obl_lifetime      AS prime_obl_lifetime_in_context,
       p.action_ct         AS prime_action_ct_in_context,
       p.last_action_date  AS prime_last_action_in_context,
       s.subaward_amt_total / NULLIF(p.obl_lifetime, 0) AS subout_rate_lifetime,
       s.subaward_amt_total / NULLIF(sum(s.subaward_amt_total) OVER (
           PARTITION BY s.prime_awardee_uei, s.context_code_type, s.context_code,
                        s.recipient_code_source, s.recipient_code_type), 0)
                            AS share_of_context_subout
FROM subout_base s
LEFT JOIN prime_lanes p
       ON p.uei = s.prime_awardee_uei
      AND p.code_type = s.context_code_type
      AND p.code = s.context_code
ORDER BY s.prime_awardee_uei
"""

# pricing-terms cycle (2026-07-15, operator-directed): entity-event-geo month
# rollup — closes the phrase layer's "in <state> (PoP) on event verbs" refusal.
# Pure GROUP BY over the local fact (no join). Sorted action_type-first so the
# compiled shape (event verb × state × window -> entity set) prunes.
_MONTH_POP_SQL = """
CREATE TABLE txn_recipient_month_pop AS
SELECT uei, action_type_code, pop_state, pop_county_fips,
       date_trunc('month', action_date) AS month,
       count(*)                         AS n_actions,
       sum(obligation)                  AS obligation_sum
FROM txn_events_combo
GROUP BY 1, 2, 3, 4, 5
ORDER BY action_type_code, pop_state, pop_county_fips, month
"""

# gap-pass-2 E1: PoP country code -> majority name (same dedup rule as the
# agency vocabs; collapses source variants like "UNITED STATES OF AMERICA").
_COUNTRY_VOCAB_SQL = """
CREATE TABLE country_vocab AS
WITH pairs AS (
    SELECT primary_place_of_performance_country_code AS code,
           pop_country_name AS name, count(*) AS n
    FROM src
    WHERE primary_place_of_performance_country_code IS NOT NULL
      AND primary_place_of_performance_country_code <> ''
      AND pop_country_name IS NOT NULL AND pop_country_name <> ''
    GROUP BY 1, 2
)
SELECT code, name
FROM (SELECT code, name,
             row_number() OVER (PARTITION BY code ORDER BY n DESC, name) AS rn
      FROM pairs)
WHERE rn = 1
ORDER BY code
"""

# gap-pass-2 E2: ordering-window state lives at txn grain on the canonical only
# (~4.5M rows carry a value); this pins the latest-action value per award.
# Standalone serving table AND the build input for award_state's parent-window
# columns (_PARENT_WINDOW_SQL below) — its manifest entry must precede that one.
_ORDERING_WINDOWS_SQL = """
CREATE TABLE award_ordering_windows AS
SELECT contract_award_unique_key,
       arg_max(ordering_period_end_date, action_date) AS ordering_period_end_date,
       max(action_date)                               AS latest_action_date
FROM src
WHERE contract_award_unique_key IS NOT NULL
  AND ordering_period_end_date IS NOT NULL
GROUP BY 1
ORDER BY contract_award_unique_key
"""

# gap-pass-2 E2: the position/active-ladder primitive — every award row carries
# its own ordering-window end plus the RESOLVED parent's window state AND
# attribution (whose vehicle, what instrument), killing the per-query double
# self-join on the 83M-row table. Parent attribution columns ride the same join
# opportunistically: the rebuild is paid for, the join is already happening,
# and "agency behind the parent instrument" is the adjacent question every
# agency-lens read asks next. All joins are 1:1 (award_state is 1 row/key;
# award_ordering_windows is GROUP BY key; the parent join is gated on
# parent_match_flag='resolved' so 'self' rows stay NULL), so the exact
# row-count parity gate still applies. Two hard-won rules live in this SQL:
# (1) Join keys are PURE equalities — the probe-side gate
#     (parent_match_flag='resolved') is folded into a CASE-derived key.
#     A mixed ON clause (probe-side filter AND equality) degraded the plan to
#     a blockwise nested-loop join (83M x 83M) that presented as a zero-CPU
#     hang and ate three builds before py-spy --native exposed
#     PhysicalBlockwiseNLJoin (2026-07-09/10). The fixture's EXPLAIN gate
#     asserts every join in this plan is a hash join.
# (2) Inputs are local tables (stream -> plain CTAS temp first) — keeps the
#     join/sort pipeline independent of Arrow-stream pacing.
_PARENT_ATTRS_COLS = [
    "contract_award_unique_key", "current_end_date", "potential_end_date",
    "awarding_agency_code", "awarding_sub_agency_code",
    "idv_type_code", "award_type_code", "type_of_set_aside_code",
]

_PARENT_WINDOW_SQL = """
CREATE TABLE usaspending_fpds_prime_award_state AS
SELECT a.*,
       o.ordering_period_end_date,
       po.ordering_period_end_date   AS parent_ordering_period_end_date,
       p.current_end_date            AS parent_current_end_date,
       p.potential_end_date          AS parent_potential_end_date,
       p.awarding_agency_code        AS parent_awarding_agency_code,
       p.awarding_sub_agency_code    AS parent_awarding_sub_agency_code,
       p.idv_type_code               AS parent_idv_type_code,
       p.award_type_code             AS parent_award_type_code,
       p.type_of_set_aside_code      AS parent_type_of_set_aside_code
FROM award_state_base a
LEFT JOIN award_ordering_windows o
       ON o.contract_award_unique_key = a.contract_award_unique_key
LEFT JOIN parent_attrs p
       ON p.contract_award_unique_key
        = (CASE WHEN a.parent_match_flag = 'resolved' THEN a.parent_award_key_resolved END)
LEFT JOIN award_ordering_windows po
       ON po.contract_award_unique_key
        = (CASE WHEN a.parent_match_flag = 'resolved' THEN a.parent_award_key_resolved END)
ORDER BY a.current_end_date
"""

# gap-pass-3 E1 residual (measured post-v8: the position ladder stayed 17-22s
# because ANY join with an 83M-row side saturates the 2-thread/1.5GB serving
# box): the snapshot position substrate — every order whose own or resolved-
# parent ordering window is open AT BUILD DATE, narrow. The ladder becomes
# ring-keys (320k) ⋈ this (17M x 4 cols) — small-box-friendly. "Open" is
# as-of the artifact build date (snapshot semantics, like everything here).
_POSITION_ORDERS_SQL = """
CREATE TABLE gtm_position_orders AS
SELECT contract_award_unique_key, recipient_uei, parent_award_key_resolved,
       coalesce(parent_ordering_period_end_date, ordering_period_end_date) AS window_end
FROM usaspending_fpds_prime_award_state
WHERE coalesce(parent_ordering_period_end_date, ordering_period_end_date) >= current_date
ORDER BY contract_award_unique_key
"""

# equipment-needs cycle (2026-07-11): combo-grain award-lifecycle-state mart.
# Pure GROUP BY over the local award_state table (no join -> no NL-join risk).
# "Active" = days_to_expiry > 0 AND is_terminated = FALSE (report's definition).
# active-only was the demand; total/terminated/expired splits + distinct primes +
# current-value + ceiling-headroom ride the SAME scan (FILTER aggregates) so the
# adjacent "what share is active / how many primes / headroom" questions need no
# rebuild. Snapshot semantics (as-of the award_state build_date), like the ladder.
_COMBO_ACTIVE_SQL = """
CREATE TABLE combo_award_active_state AS
SELECT
    naics_code,
    product_or_service_code                                                                   AS psc_code,
    count(*)                                                                                   AS award_ct,
    count(DISTINCT recipient_uei)                                                              AS recipients,
    sum(total_dollars_obligated_snapshot)                                                      AS obligated_total,
    count(*)                              FILTER (WHERE days_to_expiry > 0 AND is_terminated = FALSE) AS active_award_ct,
    count(DISTINCT recipient_uei)         FILTER (WHERE days_to_expiry > 0 AND is_terminated = FALSE) AS active_recipients,
    sum(total_dollars_obligated_snapshot) FILTER (WHERE days_to_expiry > 0 AND is_terminated = FALSE) AS active_obligated,
    sum(current_total_value_of_award)     FILTER (WHERE days_to_expiry > 0 AND is_terminated = FALSE) AS active_current_value,
    sum(remaining_ceiling_headroom)       FILTER (WHERE days_to_expiry > 0 AND is_terminated = FALSE) AS active_ceiling_headroom,
    count(*)                              FILTER (WHERE is_terminated = TRUE)                         AS terminated_award_ct,
    sum(total_dollars_obligated_snapshot) FILTER (WHERE is_terminated = TRUE)                         AS terminated_obligated,
    count(*)                              FILTER (WHERE is_expired_no_followon = TRUE)                AS expired_no_followon_ct
FROM usaspending_fpds_prime_award_state
WHERE naics_code IS NOT NULL AND naics_code <> ''
  AND product_or_service_code IS NOT NULL AND product_or_service_code <> ''
GROUP BY 1, 2
ORDER BY naics_code, product_or_service_code
"""

# gap-pass-3 E1: per-firm ranked code signature off the prime record — the
# allocation workload's rank/top-N over code lanes precomputed once, for both
# windows and both code types; floors/top-N remain query-time dials so the
# (still-moving) methodology never bakes in. Built from the already-loaded
# lanes table — no R2 read. Filtered to side='prime' -> aggregate parity.
_SIGNATURE_SQL = """
CREATE TABLE gtm_prime_code_signature AS
SELECT uei, code_type, code,
       obl_12mo, obl_24mo, obl_60mo, obl_lifetime, action_ct, last_action_date,
       rank() OVER (PARTITION BY uei, code_type ORDER BY obl_24mo DESC, code)     AS rank_24mo,
       rank() OVER (PARTITION BY uei, code_type ORDER BY obl_lifetime DESC, code) AS rank_lifetime,
       obl_24mo     / NULLIF(sum(obl_24mo)     OVER (PARTITION BY uei, code_type), 0) AS share_24mo,
       obl_lifetime / NULLIF(sum(obl_lifetime) OVER (PARTITION BY uei, code_type), 0) AS share_lifetime
FROM gtm_entity_code_lanes
WHERE side = 'prime'
ORDER BY uei, code_type, rank_lifetime
"""

_AGENCY_SUB_VOCAB_SQL = """
CREATE TABLE agency_sub_vocab AS
WITH pairs AS (
    SELECT awarding_sub_agency_code AS code, awarding_sub_agency_name AS name, count(*) AS n
    FROM src
    WHERE awarding_sub_agency_code IS NOT NULL AND awarding_sub_agency_code <> ''
      AND awarding_sub_agency_name IS NOT NULL AND awarding_sub_agency_name <> ''
    GROUP BY 1, 2
)
SELECT code, name
FROM (SELECT code, name,
             row_number() OVER (PARTITION BY code ORDER BY n DESC, name) AS rn
      FROM pairs)
WHERE rn = 1
ORDER BY code
"""

_VIEWS: dict[str, str] = {
    # entity universe: identity + behavior posture + HQ geo, one row/uei
    "v_entity_universe": """
        CREATE VIEW v_entity_universe AS
        SELECT e.*, b.* EXCLUDE (uei), g.* EXCLUDE (uei)
        FROM gtm_sam_entities e
        LEFT JOIN gtm_entity_behavior_rollup b USING (uei)
        LEFT JOIN gtm_entity_geo g USING (uei)
    """,
    # teaming edges with both-side clustering available underneath
    "v_prime_sub_edges": """
        CREATE VIEW v_prime_sub_edges AS
        SELECT * FROM gtm_prime_sub_pairs
    """,
    # combo portrait: combo × FY with the standard measure set (prime $, actions,
    # recipients, plan-attached share, task-order share). Add pop_state /
    # pop_county_fips / awarding_agency_code to the GROUP BY ad hoc for geo/agency
    # cuts — the fact carries every dial.
    "v_combo_fy": """
        CREATE VIEW v_combo_fy AS
        SELECT naics_code, psc_code, fy,
               sum(obligation)                                        AS prime_obl,
               count(*)                                               AS actions,
               count(DISTINCT uei)                                    AS recipients,
               count(DISTINCT award_key)                              AS awards,
               avg(CASE WHEN subcontracting_plan IN ('C','D','E','F','G','H')
                        THEN 1.0 ELSE 0.0 END)                        AS plan_attached_share,
               avg(CASE WHEN award_topology = 'vehicle_order'
                        THEN 1.0 ELSE 0.0 END)                        AS task_order_share
        FROM txn_events_combo
        GROUP BY 1, 2, 3
    """,
    # family grain (NAICS4 × PSC first letter — the market-grammar family key)
    "v_family_fy": """
        CREATE VIEW v_family_fy AS
        SELECT substr(naics_code, 1, 4) || 'x' || substr(psc_code, 1, 1) AS family,
               fy,
               sum(obligation)           AS prime_obl,
               count(*)                  AS actions,
               count(DISTINCT uei)       AS recipients,
               count(DISTINCT award_key) AS awards
        FROM txn_events_combo
        GROUP BY 1, 2
    """,
    # gap-pass-3 E4: vintage-aware reference names — the active row when one
    # exists, else the most recent vintage (codes retired from the current
    # vintage still carry historical award dollars; a bare is_active join
    # returns NULL names for them).
    "v_psc_names": """
        CREATE VIEW v_psc_names AS
        SELECT psc_code, psc_name, is_active, source_vintage
        FROM (SELECT psc_code, psc_name, is_active, source_vintage,
                     row_number() OVER (PARTITION BY psc_code
                                        ORDER BY is_active DESC, source_vintage DESC) AS rn
              FROM psc_reference)
        WHERE rn = 1
    """,
    "v_naics_names": """
        CREATE VIEW v_naics_names AS
        SELECT naics_code, naics_title, source_vintage
        FROM (SELECT naics_code, naics_title, source_vintage,
                     row_number() OVER (PARTITION BY naics_code
                                        ORDER BY source_vintage DESC) AS rn
              FROM naics_reference)
        WHERE rn = 1
    """,
    # gap-pass-3 E3: SAM declarations, unnested from the LIST columns (the
    # *_counter/*_string near-duplicates are ingest artifacts — using them
    # produced a retracted figure; this view makes the correct read the easy
    # one).
    "v_sam_declared_codes": """
        CREATE VIEW v_sam_declared_codes AS
        SELECT uei, is_active, 'naics' AS code_type, unnest(naics_codes) AS code
        FROM sam_master_entities WHERE naics_codes IS NOT NULL
        UNION ALL
        SELECT uei, is_active, 'psc', unnest(psc_codes)
        FROM sam_master_entities WHERE psc_codes IS NOT NULL
    """,
    # gap-pass-6 adjacency rider: the county-priced statutory floor in one
    # SELECT — rates x county coverage x FIPS crosswalk (the exact recurring
    # Entry-2 shape). Predicates on wd_id / occupation_code / county_fips
    # prune the underlying sorted tables.
    "v_wd_county_rates": """
        CREATE VIEW v_wd_county_rates AS
        SELECT r.wd_id, r.revision_number, r.wd_type,
               r.occupation_code, r.classification_title,
               r.wage_rate, r.fringe, r.fringe_is_pct, r.hw_rate,
               c.state_code, c.state_name, c.county_name,
               f.county_fips, f.resolution_status
        FROM sam_wd_rates_structured r
        JOIN sam_wd_county_coverage c ON c.wd_id = r.wd_id
        LEFT JOIN sam_county_fips_crosswalk f
               ON f.state_code = c.state_code
              AND f.sam_county_name = c.county_name
    """,
    # equipment-needs cycle (2026-07-11): phrase-grain vocabulary explode —
    # proposed_equipment_needs is a comma-joined string; this makes the recurring
    # "roll up the raw equipment vocabulary / per-combo phrase profile" read the
    # easy one (mirrors v_sam_declared_codes). Trimmed, casing preserved.
    "v_equipment_needs_phrases": """
        CREATE VIEW v_equipment_needs_phrases AS
        SELECT naics_code, psc_code, primary_bucket, in_scope,
               trim(unnest(string_to_array(proposed_equipment_needs, ','))) AS phrase
        FROM naics_psc_equipment_needs
        WHERE proposed_equipment_needs IS NOT NULL AND proposed_equipment_needs <> ''
    """,
    # equipment-needs cycle: the product surface — combo active-award-$ ⋈ the
    # equipment-needs verdict on (naics, psc). "Active $ of [bucket]-needing work"
    # is a single GROUP BY over this (filter list_contains(equipment_buckets, ...)
    # or primary_bucket / in_scope). LEFT JOIN keeps every combo; combos with no
    # verdict carry NULL bucket. Both sides 1/combo, co-sorted (naics, psc).
    "v_combo_active_equipment": """
        CREATE VIEW v_combo_active_equipment AS
        SELECT c.*, e.in_scope, e.primary_bucket, e.equipment_buckets,
               e.core_phrase_count, e.other_phrase_count, e.confidence AS needs_confidence
        FROM combo_award_active_state c
        LEFT JOIN naics_psc_equipment_needs e
               ON e.naics_code = c.naics_code AND e.psc_code = c.psc_code
    """,
    # equipment-needs cycle: supply-side shop profile in one read — the classifier
    # verdict (deduped to the best row/domain: is_equipment_provider TRUE first,
    # then latest materialized_at) ⋈ scraped inventory ⋈ award-overlap capability
    # score. supported_pscs / qualified_pscs join combo demand on the shared PSC
    # taxonomy; domain_norm joins firmographics_blitz for name/geo.
    "v_equipment_supply": """
        CREATE VIEW v_equipment_supply AS
        WITH prov AS (
            SELECT * EXCLUDE (_rn) FROM (
                SELECT p.*, row_number() OVER (
                    PARTITION BY domain_norm
                    ORDER BY is_equipment_provider DESC NULLS LAST, materialized_at DESC) AS _rn
                FROM equipment_provider p)
            WHERE _rn = 1)
        SELECT prov.*,
               m.supported_pscs, m.verified_inventory_matches, m.matched_psc_count,
               g.qualified_pscs, g.qualified_psc_count, g.qualified_value_exposure,
               g.capability_capture_ratio, g.qualified_nearby_award_count,
               g.all_nearby_award_count
        FROM prov
        LEFT JOIN equipment_matchmaking m ON m.domain_norm = prov.domain_norm
        LEFT JOIN equipment_rental_golden_overlap g ON g.firm_domain = prov.domain_norm
    """,
    # labor-pricing cycle (2026-07-14): the pre-call composite in one SELECT —
    # role alias → (soc leg ∪ sca leg) → ranked combo categories ⋈ labor share.
    # category_award_share PRECOMPUTES loaded_labor_share × pct_of_industry/100
    # (pct_of_industry is PERCENT — baking the /100 here prevents the 100×
    # inflation a raw consumer would produce). a_median is a source VARCHAR →
    # TRY_CAST. Two UNION ALL legs keep the joins pure single-key equalities.
    "v_role_priced_combos": """
        CREATE VIEW v_role_priced_combos AS
        WITH hits AS (
            SELECT a.alias, a.alias_norm, a.code_type, a.code, a.occupation_title,
                   a.title_source, a.in_combo_layer,
                   c.naics_code, c.psc_code, c.rank, c.soc_code, c.sca_code,
                   c.soc_title, c.sca_title, c.role_class, c.pct_of_industry,
                   TRY_CAST(c.a_median AS DOUBLE) AS a_median,
                   c.ep_growth_2024_2034_pct
            FROM occupation_alias_lookup a
            JOIN naics_psc_labor_profile_categories c ON c.soc_code = a.code
            WHERE a.code_type = 'soc'
            UNION ALL
            SELECT a.alias, a.alias_norm, a.code_type, a.code, a.occupation_title,
                   a.title_source, a.in_combo_layer,
                   c.naics_code, c.psc_code, c.rank, c.soc_code, c.sca_code,
                   c.soc_title, c.sca_title, c.role_class, c.pct_of_industry,
                   TRY_CAST(c.a_median AS DOUBLE) AS a_median,
                   c.ep_growth_2024_2034_pct
            FROM occupation_alias_lookup a
            JOIN naics_psc_labor_profile_categories c ON c.sca_code = a.code
            WHERE a.code_type = 'sca'
        )
        SELECT h.*,
               l.payroll_share, l.payroll_share_level,
               l.burden_multiplier, l.burden_match_level,
               l.loaded_labor_share,
               l.loaded_labor_share * h.pct_of_industry / 100.0 AS category_award_share
        FROM hits h
        LEFT JOIN naics_labor_share l ON l.naics_code = h.naics_code
    """,
    # pricing-terms cycle (2026-07-15, gap E4 directional): the staffing-
    # absorption composite — implied labor demand per prime combo lane
    # (prime $ × loaded_labor_share), a visible v1 FTE estimate (60mo
    # annualized / combo avg SOC wage — methodology stays a query-time dial,
    # deliberately NOT baked into a mart), the entity's observable headcount
    # (PDL band + LinkedIn count via the SAM↔PDL bridge), and the reported
    # farm-out. The staffed-out signal is the residual: big implied labor,
    # small headcount, little reported farm-out. Filter on uei/naics/psc —
    # the underlying sorted tables prune; full-universe scans are for builds.
    "v_staffing_absorption": """
        CREATE VIEW v_staffing_absorption AS
        WITH combo_wage AS (
            SELECT naics_code, psc_code,
                   avg(TRY_CAST(a_median AS DOUBLE)) AS avg_soc_wage
            FROM naics_psc_labor_profile_categories
            GROUP BY 1, 2
        ),
        hc AS (
            SELECT b.uei,
                   max(TRY_CAST(fb.employees_on_linkedin AS BIGINT)) AS employees_on_linkedin,
                   max(p.employee_size_range)                        AS pdl_employee_size_range
            FROM bridge_sam_pdl b
            LEFT JOIN pdl_normalized_companies p ON p.pdl_company_id = b.pdl_company_id
            LEFT JOIN firmographics_blitz fb     ON fb.domain_norm = b.normalized_domain
            GROUP BY 1
        )
        SELECT c.uei, c.naics_code, c.psc_code,
               c.prime_obl_24mo, c.prime_obl_60mo, c.prime_obl_lifetime,
               l.loaded_labor_share,
               c.prime_obl_60mo * l.loaded_labor_share             AS implied_labor_dollars_60mo,
               w.avg_soc_wage,
               c.prime_obl_60mo * l.loaded_labor_share
                   / NULLIF(w.avg_soc_wage, 0) / 5.0               AS implied_fte_per_year_60mo,
               f.farmout_amt_60mo, f.farmout_share_60mo,
               h.employees_on_linkedin, h.pdl_employee_size_range
        FROM gtm_prime_combo_lanes c
        LEFT JOIN naics_labor_share l ON l.naics_code = c.naics_code
        LEFT JOIN combo_wage w
               ON w.naics_code = c.naics_code AND w.psc_code = c.psc_code
        LEFT JOIN gtm_prime_farmout_combo_lanes f
               ON f.uei = c.uei AND f.naics_code = c.naics_code
              AND f.psc_code = c.psc_code
        LEFT JOIN hc h ON h.uei = c.uei
    """,
    # sub-out portrait at award grain: award_state × sub-out rollup — "is this
    # combo/geo/agency getting subbed out more or less" is a GROUP BY over this.
    "v_award_subout": """
        CREATE VIEW v_award_subout AS
        SELECT a.contract_award_unique_key, a.recipient_uei, a.naics_code,
               a.product_or_service_code AS psc_code, a.awarding_agency_code,
               a.award_topology, a.first_action_date, a.last_action_date,
               a.current_end_date, a.life_to_date_obligated,
               s.sub_ct, s.distinct_subs, s.sub_amount_total,
               s.first_sub_date, s.last_sub_date,
               (s.sub_ct IS NOT NULL) AS is_subbed_out
        FROM usaspending_fpds_prime_award_state a
        LEFT JOIN award_subout_rollup s
               ON s.prime_award_unique_key = a.contract_award_unique_key
    """,
}

_CREATE_LEDGER_SQL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.query_sidecar_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text NOT NULL DEFAULT 'query_sidecar',
    tiers          text,
    marts          integer,
    rows_total     bigint,
    file_bytes     bigint,
    r2_key         text,
    latest_updated boolean,
    status         text NOT NULL,
    error_message  text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS query_sidecar_runs_status_idx ON ops.query_sidecar_runs (status);
CREATE INDEX IF NOT EXISTS query_sidecar_runs_recorded_idx ON ops.query_sidecar_runs (recorded_at DESC);
"""


_SPEC_STRUCTURAL_KEYS = {"ds", "tier", "sort", "cols", "dest", "extra_select",
                         "aggregate", "from_table"}


def _preflight() -> None:
    """Assert every special-case manifest flag has a dispatch branch in
    _build_one. An unwired flag falls through to the generic CTAS silently —
    caught live 2026-07-09 (award_ordering_windows built as a 108M-row copy)."""
    import inspect

    src = inspect.getsource(_build_one)
    unwired = sorted({
        f'{spec.get("dest", spec["ds"])}: {flag}'
        for spec in MANIFEST
        for flag in set(spec) - _SPEC_STRUCTURAL_KEYS
        if f'spec.get("{flag}")' not in src
    })
    if unwired:
        raise RuntimeError(f"manifest flags without a _build_one branch: {unwired}")


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    return boto3.client(
        "s3",
        endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _record_run(**fields) -> None:
    """Terminal-state ledger row. WARN-and-return on any failure — audit must not mask the build."""
    try:
        import psycopg

        dsn = os.environ.get("HQX_DB_URL_POOLED")
        if not dsn:
            print("[warn] HQX_DB_URL_POOLED unset; skipping ops ledger row")
            return
        cols = ", ".join(fields)
        ph = ", ".join(["%s"] * len(fields))
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO ops.query_sidecar_runs ({cols}) VALUES ({ph})",
                tuple(fields.values()),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] ops ledger write failed (non-fatal): {exc}")


def _build_one(con, so: dict[str, str], spec: dict) -> dict:
    """Stream one Lance mart into a sorted native DuckDB table. Returns the parity row."""
    import lance

    name, dest = spec["ds"], spec.get("dest", spec["ds"])

    if spec.get("from_table"):
        # Local build off an already-built table (second sort copy or derived
        # rollup) — no R2 read.
        src_table = spec["from_table"]
        pinned_version = -1
        lance_rows = con.execute(f'SELECT count(*) FROM "{src_table}"').fetchone()[0]
        t0 = time.monotonic()
        if spec.get("signature"):
            con.execute(_SIGNATURE_SQL)
        elif spec.get("position_orders"):
            con.execute(_POSITION_ORDERS_SQL)
        elif spec.get("combo_active"):
            con.execute(_COMBO_ACTIVE_SQL)
        elif spec.get("month_pop_rollup"):
            con.execute(_MONTH_POP_SQL)
        else:
            order = ", ".join(spec["sort"])
            con.execute(f'CREATE TABLE "{dest}" AS SELECT * FROM "{src_table}" ORDER BY {order}')
    else:
        ds = lance.dataset(f"{LANCE_BASE}{name}/", storage_options=so)
        pinned_version = ds.version
        lance_rows = ds.count_rows()

        t0 = time.monotonic()
        if spec.get("combo_fact"):
            # canonical txn ⋈ award_state(topology) at build; LEFT JOIN preserves
            # the txn row count (award_state is 1 row/contract_award_unique_key),
            # so exact parity still gates this fact.
            aw = lance.dataset(f"{LANCE_BASE}usaspending_fpds_prime_award_state/",
                               storage_options=so)
            con.register("src_aw", aw.scanner(
                columns=["contract_award_unique_key", "award_topology"],
                batch_size=READ_BATCH_ROWS).to_reader())
            con.register("src", ds.scanner(
                columns=_COMBO_SRC_COLS, batch_size=READ_BATCH_ROWS).to_reader())
            con.execute(_COMBO_FACT_SQL)
            con.unregister("src")
            con.unregister("src_aw")
        elif spec.get("parent_window"):
            # award_state ⋈ itself (resolved parent) ⋈ award_ordering_windows
            # (already built — manifest order guarantees it). Streams feed
            # plain CTAS materializations; the join+sort runs over local
            # tables with pure-equality keys (see _PARENT_WINDOW_SQL note).
            con.register("src_parent", ds.scanner(
                columns=_PARENT_ATTRS_COLS,
                batch_size=READ_BATCH_ROWS).to_reader())
            con.execute("CREATE TEMP TABLE parent_attrs AS SELECT * FROM src_parent")
            con.unregister("src_parent")
            con.register("src", ds.scanner(batch_size=READ_BATCH_ROWS).to_reader())
            con.execute("CREATE TEMP TABLE award_state_base AS SELECT * FROM src")
            con.unregister("src")
            con.execute(_PARENT_WINDOW_SQL)
            con.execute("DROP TABLE parent_attrs")
            con.execute("DROP TABLE award_state_base")
        else:
            reader = ds.scanner(columns=spec.get("cols"),
                                batch_size=READ_BATCH_ROWS).to_reader()  # single-pass
            con.register("src", reader)
            if spec.get("agency_vocab"):
                con.execute(_AGENCY_VOCAB_SQL)
            elif spec.get("agency_sub_vocab"):
                con.execute(_AGENCY_SUB_VOCAB_SQL)
            elif spec.get("country_vocab"):
                con.execute(_COUNTRY_VOCAB_SQL)
            elif spec.get("action_type_vocab"):
                con.execute(_ACTION_TYPE_VOCAB_SQL)
            elif spec.get("fpds_code_vocab"):
                con.execute(_FPDS_CODE_VOCAB_SQL)
            elif spec.get("ordering_windows"):
                con.execute(_ORDERING_WINDOWS_SQL)
            elif spec.get("subout_rollup"):
                con.execute(_SUBOUT_ROLLUP_SQL)
            elif spec.get("plan_state"):
                con.execute(_PLAN_STATE_SQL)
            elif spec.get("subout_rate"):
                # stream -> local temp; probe-side gate (side='prime') applied
                # while materializing prime_lanes so the join ON stays pure
                # equality; then the row-preserving denominator join.
                con.execute("CREATE TEMP TABLE subout_base AS SELECT * FROM src")
                con.execute("""CREATE TEMP TABLE prime_lanes AS
                    SELECT uei, code_type, code, obl_24mo, obl_60mo,
                           obl_lifetime, action_ct, last_action_date
                    FROM gtm_entity_code_lanes WHERE side = 'prime'""")
                con.execute(_SUBOUT_RATE_SQL)
                con.execute("DROP TABLE subout_base")
                con.execute("DROP TABLE prime_lanes")
            elif spec.get("farmout_share"):
                # stream -> local temp first (hygiene rule), then the
                # row-preserving denominator join against the already-built
                # gtm_prime_combo_lanes table.
                con.execute("CREATE TEMP TABLE farmout_base AS SELECT * FROM src")
                con.execute(_FARMOUT_SHARE_SQL)
                con.execute("DROP TABLE farmout_base")
            else:
                extra = spec.get("extra_select")
                select = "SELECT *" + (f", {extra}" if extra else "")
                order = ", ".join(spec["sort"])
                con.execute(f'CREATE TABLE "{dest}" AS {select} FROM src ORDER BY {order}')
            con.unregister("src")

    duck_rows = con.execute(f'SELECT count(*) FROM "{dest}"').fetchone()[0]
    elapsed = round(time.monotonic() - t0, 1)
    # Aggregate tables REDUCE the source — their row count can never equal the
    # source count; parity there is non-emptiness.
    aggregate = bool(spec.get("agency_vocab") or spec.get("aggregate"))
    row = {
        "table": dest, "dataset": name, "tier": spec["tier"],
        "sort": ",".join(spec.get("sort", [])) or None,
        "lance_version": pinned_version, "lance_rows": lance_rows,
        "duck_rows": duck_rows,
        "parity_ok": (duck_rows > 0) if aggregate else (duck_rows == lance_rows),
        "seconds": elapsed,
    }
    print(f"[mart] {dest}: {duck_rows:,} rows in {elapsed}s "
          f"(lance v{pinned_version}={lance_rows:,}) parity={'OK' if row['parity_ok'] else 'MISMATCH'}")
    return row


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"),
             modal.Secret.from_name("query-sidecar")],  # refresh-hook bearer (Phase 5)
    memory=131_072,          # 128 GiB — the >100M-row sort precedent (cms_medicare giant)
    cpu=8.0,
    ephemeral_disk=524_288,  # 512 GiB local NVMe: DuckDB spill + the output file
    timeout=60 * 60 * 12,
)
def build(tiers: str = "A,B,C,D", publish: bool = True, smoke: bool = False,
          trigger_callback_url: str | None = None) -> dict:
    """Build the query-sidecar .duckdb for the requested tiers; publish blue-green to R2."""
    import duckdb

    _preflight()
    started_at = dt.datetime.now(dt.timezone.utc)
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    wanted = {t.strip().upper() for t in tiers.split(",") if t.strip()}
    specs = [s for s in MANIFEST if s["tier"] in wanted]
    # agency vocab rides with Tier D (its consumers are the market/phrase lanes)
    if "D" in wanted:
        specs.append({"ds": "usaspending_award_canonical", "tier": "D",
                      "cols": ["awarding_agency_code", "awarding_agency_name"],
                      "dest": "agency_vocab", "agency_vocab": True, "sort": []})

    os.makedirs(f"{SCRATCH_ROOT}/spill", exist_ok=True)
    db_path = f"{SCRATCH_ROOT}/query_sidecar_{stamp}.duckdb"
    so = _r2_storage_options()

    status, error_message, r2_key, latest_updated = "success", None, None, False
    parity: list[dict] = []
    file_bytes = 0
    try:
        con = duckdb.connect(db_path)
        try:
            # Out-of-core sort config — memory_limit BELOW the container cap
            # (cgroup auto-detect misreads), spill on local NVMe, and
            # preserve_insertion_order=true so the CTAS ORDER BY survives the
            # parallel insert (this is the one place true is required).
            con.execute(f"""
                SET memory_limit='96GB';
                SET threads=8;
                SET temp_directory='{SCRATCH_ROOT}/spill';
                SET max_temp_directory_size='400GB';
                SET preserve_insertion_order=true;
            """)
            for spec in specs:
                parity.append(_build_one(con, so, spec))

            mismatches = [p["table"] for p in parity if not p["parity_ok"]]
            if mismatches:
                raise RuntimeError(f"row-count parity failed for: {mismatches}")

            # bake build metadata + the parity manifest into the file itself
            con.execute("CREATE TABLE _sidecar_meta (built_at VARCHAR, tiers VARCHAR, source VARCHAR)")
            con.execute("INSERT INTO _sidecar_meta VALUES (?, ?, ?)",
                        [started_at.isoformat(), ",".join(sorted(wanted)),
                         "docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md"])
            con.execute("""CREATE TABLE _sidecar_manifest (
                table_name VARCHAR, dataset VARCHAR, tier VARCHAR, sort_key VARCHAR,
                lance_version BIGINT, lance_rows BIGINT, duck_rows BIGINT, seconds DOUBLE)""")
            con.executemany(
                "INSERT INTO _sidecar_manifest VALUES (?,?,?,?,?,?,?,?)",
                [(p["table"], p["dataset"], p["tier"], p["sort"], p["lance_version"],
                  p["lance_rows"], p["duck_rows"], p["seconds"]) for p in parity])
            if {"A", "C", "D"} <= wanted:  # views span all three tiers' tables
                for _vname, vsql in _VIEWS.items():
                    con.execute(vsql)
            con.execute("CHECKPOINT")
        finally:
            con.close()

        file_bytes = os.path.getsize(db_path)
        print(f"[build] {db_path}: {file_bytes/2**30:.2f} GiB, {len(parity)} tables")

        if publish:
            s3 = _s3_client()
            prefix = f"{R2_PREFIX}/smoke" if smoke else R2_PREFIX
            r2_key = f"{prefix}/query_sidecar_{stamp}.duckdb"
            s3.upload_file(db_path, R2_BUCKET, r2_key)   # boto3 multipart handles the size
            print(f"[publish] s3://{R2_BUCKET}/{r2_key}")
            if not smoke:
                pointer = {"key": r2_key, "built_at": started_at.isoformat(),
                           "file_bytes": file_bytes, "tiers": sorted(wanted),
                           "tables": [p["table"] for p in parity]}
                s3.put_object(Bucket=R2_BUCKET, Key=f"{R2_PREFIX}/LATEST.json",
                              Body=json.dumps(pointer, indent=1).encode(),
                              ContentType="application/json")
                latest_updated = True
                print(f"[publish] LATEST.json -> {r2_key}")
                _notify_refresh()
    except Exception as exc:  # noqa: BLE001
        status, error_message = "error", str(exc)[:2000]
        raise
    finally:
        _record_run(
            tiers=",".join(sorted(wanted)), marts=len(parity),
            rows_total=sum(p["duck_rows"] for p in parity),
            file_bytes=file_bytes, r2_key=r2_key, latest_updated=latest_updated,
            status=status, error_message=error_message,
            started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc),
        )
        if trigger_callback_url:
            _post_callback(trigger_callback_url, status, parity)

    return {"status": status, "r2_key": r2_key, "file_bytes": file_bytes,
            "tables": len(parity), "parity": parity}


def _notify_refresh() -> None:
    """Best-effort: tell query-sidecar-api to hot-swap to the new artifact
    (Phase 5 refresh loop). Never fails the build — the service also picks up
    LATEST on its next boot; this just closes the loop immediately."""
    import json as _json
    import urllib.request

    url = os.environ.get("QUERY_SIDECAR_URL")
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not (url and token):
        print("[refresh] QUERY_SIDECAR_URL/TOKEN unset; skipping serving refresh")
        return
    try:
        req = urllib.request.Request(
            url.rstrip("/") + "/api/v1/refresh", data=b"{}", method="POST",
            headers={"authorization": f"Bearer {token}", "content-type": "application/json"})
        with urllib.request.urlopen(req, timeout=600) as resp:  # noqa: S310 — config-controlled host
            print(f"[refresh] serving endpoint: {_json.loads(resp.read())}")
    except Exception as exc:  # noqa: BLE001
        print(f"[refresh] non-fatal: {exc}")


def _post_callback(url: str, status: str, parity: list[dict]) -> None:
    import requests

    payload = {"status": status, "feed": "query_sidecar",
               "rows": sum(p["duck_rows"] for p in parity)}
    for attempt in range(3):
        try:
            requests.post(url, json=payload, timeout=30).raise_for_status()
            return
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] callback attempt {attempt + 1} failed: {exc}")
            time.sleep(2 ** attempt)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_schema() -> None:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_CREATE_LEDGER_SQL)
        conn.commit()
    print("[initdb] ops.query_sidecar_runs ready")


@app.local_entrypoint()
def initdb():
    init_schema.remote()


@app.local_entrypoint()
def run(tiers: str = "A,B,C,D"):
    result = build.remote(tiers=tiers, publish=True, smoke=False, trigger_callback_url=None)
    print(json.dumps({k: v for k, v in result.items() if k != "parity"}, indent=1))


@app.local_entrypoint()
def smoke():
    result = build.remote(tiers="A", publish=True, smoke=True, trigger_callback_url=None)
    print(json.dumps(result, indent=1, default=str))
