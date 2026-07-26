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

Doctrine (docs/reference/03_modal_compute.md, §6.1 launch durability):
- standalone Modal app, `modal deploy`-ed; builds are fired by SPAWNING `build` on
  the DEPLOYED app (`modal.Function.from_name("query-sidecar","build").spawn(...)`)
  — an ASYNC input with no client tether. NO Trigger schedule (parked);
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
  fixture executes a pathological plan instantly. THE fixture is
  pipelines/query_sidecar/test_fixture_explain.py (python3 -m pytest, local,
  no network) — it drives every manifest spec through _build_one and asserts
  no NESTED_LOOP/CROSS_PRODUCT node in any CREATE TABLE plan. Run it green
  before every builder merge.
- Self-join inputs materialize locally (stream -> plain CTAS temp) before
  joining — hygiene that keeps join/sort independent of Arrow-stream pacing.
- Launch ONLY by spawning on the deployed app:
  `modal.Function.from_name("query-sidecar","build").spawn(...)` — record the
  fc-id. A `modal run …::run` launch (with or without --detach) issued a SYNC
  input through the local_entrypoint; the server cancels a SYNC input ~90 s
  after the client stops heartbeating (session end, sleep, DNS blip) — this
  killed 8 builds as `Query interrupted`. --detach detaches the APP, not the
  INPUT. Receipts: ~/Desktop/hq/sessions/2026-07-23-sidecar-rebuild-recon.md.

Promotion doctrine (operator-directed, 2026-07-09): the demand-evidence gate
applies to STRUCTURAL growth (new tables/grains/sort copies — recurring cost).
Column-grain adds riding a projection/join the build already performs ship
opportunistically whenever the adjacent question is foreseeable — a rebuild is
a committed fixed cost; columns are free during it and cost a full new cycle
after it. See the sidecar-gaps skill (Mode 2) for the adjacency sweep.

Parity: every mart's DuckDB count must equal ds.count_rows() at the PINNED Lance
version read at build start. Any mismatch fails the run before publish.

Entrypoints:
  modal deploy pipelines/query_sidecar/build_query_sidecar.py            # REQUIRED first — spawn runs the deployed snapshot
  modal run pipelines/query_sidecar/build_query_sidecar.py::initdb
  modal run pipelines/query_sidecar/build_query_sidecar.py::run          # spawn-fires build on the DEPLOYED app, prints fc-id, returns
  modal run pipelines/query_sidecar/build_query_sidecar.py::smoke       # Tier A only, smoke/ prefix, no LATEST (client-tethered; short)
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
        "requests>=2.32",   # _post_callback + _prev_artifact_counts (was an unguarded
                            # import inside build()'s finally — recon §2.2)
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
     "from_table": "gtm_entity_code_lanes", "after": ["gtm_entity_code_lanes"],
     "signature": True, "aggregate": True},
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
    # BEA cost-structure cycle (2026-07-24, sidecar-gaps Mode 2 — the code-space
    # bridge for bea-io-purchased-services). 499 rows: the sole warm path to
    # bea_detail_code (unlocks the intake's 389-industry detail grain that
    # naics_labor_share structurally cannot reach) and the only complete 73-code BEA
    # summary vocabulary. naics_code_clean is NOT unique (471 distinct of 483 non-null;
    # '23' appears 12x, all flagged naics_multi_io=True) — collapses at summary level,
    # fans out at detail; a naive equality join must dedupe. Sorted naics_code_clean
    # so the FPDS-side prefix walk prunes.
    {"ds": "bea_naics_concordance", "tier": "A", "sort": ["naics_code_clean"]},
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
     "from_table": "gtm_txn_recipient_month_rollup",
     "after": ["gtm_txn_recipient_month_rollup"]},
    # growth-lane cycle (2026-07-19, operator-directed): firm × construction
    # work-lane × month — the surety Growth card's substrate. The month rollups
    # above carry no naics/psc, so lane-scoped growth windows were scanning the
    # 108M txn mart at 1.5–7s per cut (gap report 2026-07-19). Lane membership
    # = the 5 work-typed pair-sets (544 pairs; authority
    # hq/data-cache/surety/construction_pairsets_v1.json, inlined as
    # _CONSTRUCTION_LANE_PAIRS). Adjacency riders on the same GROUP BY:
    # action/award counts, new-award split (base-award rows, action_type NULL —
    # "is the growth new work or mods", the bonding-event signal), distinct
    # agencies ("one buyer or many"). Windows stay query-time dials — this
    # bakes GRAIN, never windows. Must follow gtm_txn_events_slim (local).
    {"ds": "gtm_txn_events_slim", "tier": "B", "dest": "gtm_construction_lane_months",
     "sort": ["lane", "uei", "month"],
     "from_table": "gtm_txn_events_slim", "after": ["gtm_txn_events_slim"],
     "construction_lane_months": True, "aggregate": True},
    {"ds": "gtm_award_recipient_rollup", "tier": "B", "sort": ["uei"]},
    {"ds": "gtm_award_expiry_months", "tier": "B", "sort": ["uei", "end_month"]},
    {"ds": "gtm_prime_pop_lanes", "tier": "B", "sort": ["uei"]},
    # ── Tier C — benchmark-promoted giants (Phase 2 verdicts) ────────────────
    # gap-pass-2 E2: award-grain ordering windows (see _ORDERING_WINDOWS_SQL) —
    # MUST build before usaspending_fpds_prime_award_state, which joins it.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "award_ordering_windows",
     "sort": ["contract_award_unique_key"], "ordering_windows": True, "aggregate": True,
     "cols": ["contract_award_unique_key", "ordering_period_end_date", "action_date"]},
    # gap-pass-1 E2 + pricing-terms cycle: award-grain latest-action state
    # (subcontracting plan + pricing/financing/size). billing-latency cycle
    # (2026-07-16): MOVED before award_state — the parent_window build now
    # denormalizes these columns onto award_state itself, because ANY
    # query-time join with the 83M side saturates the 2-thread serving box
    # (measured 32-49 s on the billing shapes; the position-orders precedent).
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "award_plan_state",
     "sort": ["contract_award_unique_key"], "plan_state": True, "aggregate": True,
     "cols": ["contract_award_unique_key", "subcontracting_plan", "action_date",
              "type_of_contract_pricing_code", "contract_financing",
              "contracting_officers_determination_of_business_size"]},
    # award-grain rows + exact expiring: 96s live-lane -> ms-class local; also
    # removes the expiry_months month-grain approximation on two-lane phrases.
    # gap-pass-2 E2: parent_window build widens it with own + resolved-parent
    # ordering/end-window columns (see _PARENT_WINDOW_SQL).
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C", "sort": ["current_end_date"],
     "parent_window": True, "after": ["award_ordering_windows", "award_plan_state"]},
    # gap-pass-3 E1 residual: open-window position substrate (see
    # _POSITION_ORDERS_SQL) — local build off award_state, must follow it.
    {"ds": "gtm_position_orders", "tier": "C", "sort": ["contract_award_unique_key"],
     "from_table": "usaspending_fpds_prime_award_state",
     "after": ["usaspending_fpds_prime_award_state"], "position_orders": True,
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
     "from_table": "usaspending_fpds_prime_award_state",
     "after": ["usaspending_fpds_prime_award_state"], "combo_active": True,
     "aggregate": True},
    # award-key companion: the profile's anchor row keyed by the award (was a
    # 4.8s zone-map crawl on the current_end_date-sorted spine). Full-width —
    # every column any by-key consumer will ask for rides free. Prefix-led
    # sort — see txn_rows_by_award note.
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C",
     "dest": "prime_award_state_by_key",
     "extra_select": "substr(contract_award_unique_key, 10, 12) AS award_key_pfx",
     "sort": ["award_key_pfx", "contract_award_unique_key"],
     "from_table": "usaspending_fpds_prime_award_state",
     "after": ["usaspending_fpds_prime_award_state"]},
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
    # award-key point-read companion (gap 2026-07-21-award-key-probes): the
    # award drawer's "recent actions" leg keyed by the award. award_key_pfx
    # leads the sort: full keys share an 8-byte prefix (CONT_AWD_/CONT_IDV_)
    # and DuckDB string zone-maps truncate at 8 bytes, so sorting by the full
    # key alone cannot prune — probes carry pfx + full-key equality.
    {"ds": "txn_rows", "tier": "C", "dest": "txn_rows_by_award",
     "extra_select": "substr(contract_award_unique_key, 10, 12) AS award_key_pfx",
     "sort": ["award_key_pfx", "contract_award_unique_key", "action_date"],
     "from_table": "txn_rows", "after": ["txn_rows"]},
    # novation cycle (2026-07-24, sidecar-gaps Mode 2 — SIDECAR_GAP_REPORT
    # 2026-07-20-novation-mod-reason Gap 1, REDUCED to a rider). The report's premise
    # was wrong: the mod-reason was never missing — FPDS action_type_code J/S/T =
    # NOVATION AGREEMENT / CHANGE PIID / TRANSFER ACTION (already on 11 serving tables;
    # action_type_vocab already glosses them), so the capability serves TODAY at ~0.94s
    # (that is a routing-guide fix, not a build). The ONE genuinely unserved leg is
    # predecessor→successor identity — FPDS carries it in no column; it must be read off
    # the award's transaction ladder (measured 10.0s at query time). lag() over
    # txn_rows_by_award, ALREADY sorted (award_key_pfx, contract_award_unique_key,
    # action_date), so the window streams with no re-sort — NOT a self-join, so the
    # nested-loop trap cannot fire. ~88K rows all-time. is_uei_change splits true
    # change-of-hands from same-UEI re-papering (the class the SAM-delta proxy missed).
    # Award-$ context denormalized via a two-pure-equality LEFT JOIN to
    # prime_award_state_by_key. Reducing (108M→88K) -> aggregate floor parity.
    {"ds": "txn_rows_by_award", "tier": "C", "dest": "gtm_award_novation_events",
     "sort": ["action_date", "to_uei"],
     "from_table": "txn_rows_by_award",
     "after": ["txn_rows_by_award", "prime_award_state_by_key"],
     "novation_events": True, "aggregate": True},
    # award place-of-performance centroids (bundle cycle): enables ad-hoc geo SQL
    # (bounding-box + haversine) and PoP-grain geometry; sorted state/zip5 so
    # spatial predicates prune row groups.
    {"ds": "usaspending_award_pop_centroids", "tier": "C", "sort": ["state_code", "zip5"]},
    # award-key companion: centroid point-read keyed by the award (was 0.9s on
    # the state/zip5-sorted copy). Prefix-led sort — see txn_rows_by_award note.
    {"ds": "usaspending_award_pop_centroids", "tier": "C",
     "dest": "award_pop_centroids_by_key",
     "extra_select": "substr(generated_unique_award_id, 10, 12) AS award_key_pfx",
     "sort": ["award_key_pfx", "generated_unique_award_id"],
     "from_table": "usaspending_award_pop_centroids",
     "after": ["usaspending_award_pop_centroids"]},
    # ── combo-portrait layer ──────────────────────────────────────────────────
    # ONE fact, every dial: combo (substr rollups), time (action_date + fy),
    # action codes, subk plan, topology (award_state join at build), geo
    # (pop state/county), agency/sub-agency. Sorted combo-first; geo-sorted
    # second copy below so county/state-anchored questions prune too.
    {"ds": "usaspending_fpds_canonical_txn", "tier": "C", "dest": "txn_events_combo",
     "sort": ["naics_code", "psc_code", "action_date"], "combo_fact": True},
    {"ds": "txn_events_combo", "tier": "C", "dest": "txn_events_combo_by_geo",
     "sort": ["pop_state", "pop_county_fips", "action_date"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"]},
    # award-key companion: the drawer's FY-ledger leg keyed by the award —
    # turns the per-award combo probe (0.65s uei-pruned, 11.6s raw) ms-class.
    # Prefix-led sort — see txn_rows_by_award note.
    {"ds": "txn_events_combo", "tier": "C", "dest": "txn_events_combo_by_award",
     "extra_select": "substr(award_key, 10, 12) AS award_key_pfx",
     "sort": ["award_key_pfx", "award_key", "action_date"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"]},
    # geo-spine cycle (2026-07-24, sidecar-gaps Mode 2 — SIDECAR_GAP_REPORT
    # 2026-07-22 Gap 1 (award-grain county) + 2026-07-23 Entry 2 (non-local share,
    # award ⋈ PoP ⋈ HQ)): award-grain geography state. The promote is CORRECTNESS,
    # not speed — the query-time centroid route silently sampled 40.6% of the active
    # universe (topology-biased: vehicles 11%), so the published import ratios were
    # computed on a non-random 40% sample. This mart derives PoP from txn_events_combo
    # instead: 100% award-key coverage, 62.4% county fill on the active set (1.5x the
    # centroid route). PER-FIELD arg_max is deliberate — each geo field independently
    # pins the LATEST TXN THAT CARRIES IT (coverage-maximizing), so it is NOT strict
    # latest-txn-per-award; a future reader must not "fix" this to a single-struct
    # arg_max, which would repropagate the latest row's NULLs. Local (from_table +
    # reads the already-built txn_events_combo + gtm_sam_entities) — ZERO R2 read.
    # Row-preserving LEFT-JOIN chain (all three legs 1:1) -> EXACT parity 82,868,654.
    # Sort mirrors txn_events_combo_by_geo so award- and txn-grain sector pages cluster
    # on the same membership key; current_end_date trails for "expiring in sector".
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C", "dest": "award_geo_state",
     "sort": ["pop_state", "pop_county_fips", "current_end_date"],
     "from_table": "usaspending_fpds_prime_award_state",
     "after": ["usaspending_fpds_prime_award_state", "txn_events_combo", "gtm_sam_entities"],
     "award_geo_state": True},
    # geo-spine cycle (2026-07-24, Entry 1): place × FY rollup — the distinct-
    # places-of-work count over a FY window that twice OOMed the serving box and
    # shipped only as a bound ("20,000+"). count(DISTINCT pop_zip5) over this rollup
    # is ms-class and exact. Pure GROUP BY over the local fact (no join, no
    # count(DISTINCT) at build). Aggregate -> floor parity. Must follow txn_events_combo.
    {"ds": "txn_events_combo", "tier": "C", "dest": "pop_place_fy",
     "sort": ["fy", "pop_state", "pop_county_fips", "pop_zip5"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"],
     "pop_place_fy": True, "aggregate": True},
    # demo-region-grain cycle (2026-07-26) — the three place-grain atoms every
    # region rollup composes from. See the _POP_COMBO_FY_SQL block for why these are
    # atoms and not the 21 baked region rows the gap report's demand implies.
    # All three are local off txn_events_combo (zero R2 read); reducing -> aggregate
    # floor parity. Sorted place-first so a region's state/county predicate prunes.
    {"ds": "txn_events_combo", "tier": "C", "dest": "pop_combo_fy",
     "sort": ["pop_state", "pop_county_fips", "naics_code", "psc_code", "fy"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"],
     "pop_combo_fy": True, "aggregate": True},
    {"ds": "txn_events_combo", "tier": "C", "dest": "pop_entity_fy",
     "sort": ["pop_state", "pop_county_fips", "uei", "fy"],
     "from_table": "txn_events_combo",
     "after": ["txn_events_combo", "gtm_sam_entities"],
     "pop_entity_fy": True, "aggregate": True},
    {"ds": "txn_events_combo", "tier": "C", "dest": "pop_award_fy",
     "sort": ["pop_state", "pop_county_fips", "naics_code", "psc_code", "award_key"],
     "from_table": "txn_events_combo",
     "after": ["txn_events_combo", "award_geo_state"],
     "pop_award_fy": True, "aggregate": True},
    # the live book, award-grain, place-sorted (entry 2) — 263k rows off the 82.87M spine.
    {"ds": "award_geo_state", "tier": "C", "dest": "award_geo_active",
     "sort": ["pop_state", "pop_county_fips", "naics_code", "psc_code"],
     "from_table": "award_geo_state", "after": ["award_geo_state"],
     "award_geo_active": True, "aggregate": True},
    # pricing-terms cycle (2026-07-15, operator-directed): entity-event-GEO
    # month rollup — the phrase layer's disclosed refusal ("in <state> (PoP)
    # on event verbs": gtm_txn_recipient_month_rollup carries no PoP). Grain
    # uei × action_type × pop_state/county × month off the local fact; sorted
    # so "entities with action X in state S in window W" prunes. County rides
    # the same GROUP BY (the zoom-in next question). Aggregate -> non-empty
    # parity. Local build, must follow txn_events_combo.
    {"ds": "txn_events_combo", "tier": "C", "dest": "txn_recipient_month_pop",
     "sort": ["action_type_code", "pop_state", "pop_county_fips", "month"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"],
     "month_pop_rollup": True, "aggregate": True},
    # market-composition cycle (2026-07-17, operator-directed — gc-hq platform
    # users compose markets in arbitrary slicings; SIDECAR_MARKET_COMPOSITION_
    # SUBSTRATE.md §3): entity × federal-FY won mart — the collections
    # doctrine's headline measure ("won FY23–25" = Σ prime obligations in the
    # FY window) as a ms-class uei-pruned leg instead of a 0.8s group-by over
    # the 108M fact per composed market. Set-aside riders (any + the four big
    # program families, codes probe-verified: NONE/None = no set-aside) ride
    # the SAME scan — "actually WINS set-aside work" vs merely-certified is
    # the foreseeable next predicate. Local off txn_events_combo (has fy +
    # set_aside + award_key). Aggregate parity.
    {"ds": "txn_events_combo", "tier": "C", "dest": "gtm_entity_fy_won",
     "sort": ["uei", "fy"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"],
     "entity_fy_won": True, "aggregate": True},
    # capitalization-triggers cycle (2026-07-21, sidecar-gaps Mode 2 —
    # SIDECAR_GAP_REPORT_2026-07-21-capitalization-triggers.md entries 2+3):
    # entity-grain trailing-window FLOW — the velocity/transition complement to
    # gtm_entity_pricing_mix's active STOCK. recent-24mo vs prior-24mo obligation
    # split by pricing class (the FFP->cost/T&M transition, a 2.8s combo scan
    # made ms-class) + SCA/DBA labor-covered exposure (entry 3, was 0.8-3.1s) +
    # financing / new-award / buyer-geo-breadth adjacency riders on the SAME
    # scan. Windows anchored to max(action_date) (FPDS-lag watermark). Local off
    # txn_events_combo (pure GROUP BY, no join). Aggregate parity.
    {"ds": "txn_events_combo", "tier": "C", "dest": "gtm_entity_pricing_flow",
     "sort": ["uei"],
     "from_table": "txn_events_combo", "after": ["txn_events_combo"],
     "pricing_flow": True, "aggregate": True},
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
    # (award_plan_state moved up — billing-latency cycle 2026-07-16: it must
    # build BEFORE award_state so parent_window denormalizes its columns.)
    # billing-latency cycle (2026-07-16): entity-grain pricing mix — the demo
    # lens ("primes whose active book is predominantly FFP / unfinanced /
    # small-determined") as ONE uei-sorted read. 767k rows off the local
    # award_state (which now carries the pricing columns). Aggregate parity.
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C",
     "dest": "gtm_entity_pricing_mix", "sort": ["uei"],
     "from_table": "usaspending_fpds_prime_award_state",
     "after": ["usaspending_fpds_prime_award_state"],
     "entity_pricing_mix": True, "aggregate": True},
    # market-composition cycle (2026-07-17): entity-grain award book — the
    # ontology's active_committed_book / vehicle_capacity / headroom as named
    # uei-sorted columns (doctrine: committed vs vehicle NEVER blended; zero
    # floors per award). Median/avg committed award value ride the same scan
    # (the award-size-texture predicate: "wins $1–10M contracts" ≠ "$10M
    # book"); next expiry, active agency breadth, and set-aside committed
    # value ride too (foreseeable composer legs). Local off award_state.
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C",
     "dest": "gtm_entity_award_book", "sort": ["uei"],
     "from_table": "usaspending_fpds_prime_award_state",
     "after": ["usaspending_fpds_prime_award_state"],
     "entity_award_book": True, "aggregate": True},
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
     "farmout_share": True, "after": ["gtm_prime_combo_lanes"]},
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
     "sort": ["prime_awardee_uei"], "subout_rate": True,
     "after": ["gtm_entity_code_lanes"]},
    # recipient-shape-anchored sort copy: every read filters ONE evidence lens
    # (the four recipient_code_source lenses overlap — summing across them
    # double-counts), then the recipient code. "Primes that route ≥N% of
    # <context> work to subs who prime in Y" prunes here instead of scanning
    # 11.8M (measured 2.6 s unpruned). Local re-sort, must follow base.
    {"ds": "gtm_prime_subout_by_recipient_code", "tier": "D",
     "dest": "gtm_prime_subout_by_code",
     "sort": ["recipient_code_source", "recipient_code_type", "recipient_code"],
     "from_table": "gtm_prime_subout_by_recipient_code",
     "after": ["gtm_prime_subout_by_recipient_code"]},
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
    # full-corpus UCC layer (operator-directed 2026-07-16): the sam_ucc_* pair
    # is the SAM intersection by construction; these lift the constraint —
    # every CA/CO debtor (org + individual), uei/in_sam as nullable enrichment.
    # Lender grain gains total_firms (all debtors) alongside sam_firms.
    {"ds": "ucc_filings_all", "tier": "D", "sort": ["ucc_state", "debtor_name_norm"]},
    {"ds": "ucc_lenders_all", "tier": "D", "sort": ["lender_key"]},
    # lender-book cycle (operator-directed 2026-07-17): lender_key-grain
    # filing bridge — one lender's full debtor book as a pruned probe instead
    # of a 4s normalize-scan of secured_parties over the 7.7M corpus (which
    # also row-capped mega-lender books). Local off ucc_filings_all; blobs
    # (secured_parties, collateral_text) stay one equality join behind.
    {"ds": "ucc_filings_all", "tier": "D", "dest": "ucc_lender_filings",
     "sort": ["lender_key", "uei"],
     "from_table": "ucc_filings_all", "after": ["ucc_filings_all"],
     "ucc_lender_filings": True, "aggregate": True},
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
    # installations cycle (2026-07-17, operator-directed): DoD MIRTA site points
    # (831 rows — name, component, state, status, lat/lon) for territory-vs-
    # installation overlay questions across ALL market collections. Generic copy,
    # exact parity. Parked at source (not landed to Lance): isFirrmaSite/isCui
    # compliance flags — no foreseeable GTM question; re-land + rebuild if asked.
    {"ds": "military_installations_lance", "tier": "D",
     "dest": "military_installations", "sort": ["state_code"]},
    {"ds": "firmographics_blitz", "tier": "D", "sort": ["domain_norm"]},
    # compliance-friction cycle (2026-07-20): the US software/SaaS commercial
    # universe (173,119 domains, natural PK; landed s3://.../us_software_companies).
    # The warm "is this a commercial software vendor" membership set. Demand:
    # three consecutive analyses this session hand-joined it out of Lance
    # (SIDECAR_GAP_REPORT_2026-07-20 sbir-phase3-crossover, gwac-vehicle-crosswalk,
    # compliance-friction-securitypal). Domain-keyed → joins
    # gtm_sam_entities.normalized_domain (the uei↔domain bridge, already warm), so
    # "commercial-software ∩ federal-behavior" is a native join instead of a
    # three-system hand-join. Generic copy, exact parity.
    {"ds": "us_software_companies", "tier": "D", "sort": ["domain"]},
    # ── gap-pass-4: identity/enrichment coverage layer ────────────────────────
    # Demand: SIDECAR_GAP_REPORT_2026-07-10-funding-tab-pdl-match (PDL bridge) +
    # operator-recorded next-questions (icypeas/LinkedIn coverage on the same
    # populations). "Does population X have PDL / LinkedIn / scraped-profile
    # coverage" becomes one pruned statement. pdl_companies (raw twin of
    # normalized, 35M) deliberately excluded — linkedin_slug carries the URL.
    {"ds": "bridge_sam_pdl", "tier": "D", "sort": ["uei"]},
    {"ds": "pdl_normalized_companies", "tier": "D", "sort": ["pdl_company_id"]},
    # linkedin-resolve cycle (2026-07-17, operator-directed): slug-sorted slim
    # lookup — the LinkedIn-URL resolution hop for gc-hq list uploads. The
    # unsorted linkedin_slug probe on the 35.4M base measured 18.4s (saturation
    # class); this narrow copy prunes to ms. Grain 1/(slug, pdl_company_id);
    # reducing projection -> aggregate parity. Local off the base table.
    # aggregate mis-flag fixed (recon §2.2 / directive §5.2): today the lookup is
    # measured 1:1 with the base (every pdl row carries a non-empty slug), so it
    # gets the EXACT parity gate. If upstream ever lands null/empty slugs the
    # build fails loudly here — flip the flag back deliberately, with eyes on it.
    {"ds": "pdl_normalized_companies", "tier": "D", "dest": "pdl_slug_lookup",
     "sort": ["linkedin_slug"],
     "from_table": "pdl_normalized_companies", "after": ["pdl_normalized_companies"],
     "slug_lookup": True},
    # market-composition cycle rider (parked 2026-07-17, ships this build):
    # SAM ultimate-parent hierarchy — the family disambiguator for shared-domain
    # /shared-slug resolution candidates (akima.com -> NANA family) and the
    # rollup dimension for family-based analysis. 148,766 rows, generic copy.
    {"ds": "entity_hierarchy", "tier": "D", "sort": ["uei"]},
    # market-composition cycle (2026-07-17): uei-grain firmographics — the
    # employee-size / industry / founded predicate as a ms-class leg. Measured
    # 10.0s through the query-time bridge⋈PDL join (saturation class, not a
    # workaround). Bridge is NOT 1/uei (802k rows / 464k ueis) — deterministic
    # best-row rule: prefer a row with employee_size_range, then a non-generic
    # domain, then lowest pdl_company_id. Local join of two already-built
    # tables, single pure-equality key. Aggregate parity (reduces to 1/uei).
    {"ds": "bridge_sam_pdl", "tier": "D", "dest": "gtm_entity_firmographics",
     "sort": ["uei"],
     "from_table": "bridge_sam_pdl",
     "after": ["bridge_sam_pdl", "pdl_normalized_companies"],
     "entity_firmographics": True, "aggregate": True},
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
    # ── ECEC compensation-component decomposition (demo-narrative cycle 2026-07-24,
    # sidecar-gaps Mode 2 — SIDECAR_GAP_REPORT_2026-07-23 Entry 3; flips the 2026-07-14
    # structural park on its second dated demand point). bls_ecec_costs is the full CM
    # series universe with the series key ALREADY decoded into 7 code/label pairs, so the
    # CMU2__0000000000P flagship is pure column equality — no consumer parses a series id.
    # CONSUMER MUST pin area (31 levels), datatype (8, mixes percent+dollars), and
    # (year, period) explicitly, or the numbers are silently wrong — documented in the
    # guide. Health-insurance detail (estimate 15) exists only at supersector level (59.2%
    # of NAICS); degrade to component 'Insurance' (estimate 13) below it. bls_ecec_burden
    # is the 321-row national provenance grid behind naics_labor_share.burden_multiplier.
    # Both SELECT *, row-preserving → EXACT parity (pin v15=627,050 / v8=321).
    {"ds": "bls_ecec_costs", "tier": "D",
     "sort": ["ownership", "industry_group", "occupation_group", "subcell",
              "area", "datatype", "year", "period", "estimate_code"]},
    {"ds": "bls_ecec_burden", "tier": "D",
     "sort": ["ownership", "industry_group", "occupation_group", "subcell", "quarter"]},
    # ── BEA industry cost-structure family (2026-07-24, sidecar-gaps Mode 2 —
    # bea-io-purchased-services: 2026-07-15 Entry 5 + demo draw; upstream landed 07-23
    # #1324/#1325/#1326). All plain SELECT * copies, EXACT parity, no join at build.
    # bea_bls_klems (the literal ask): Service/Materials/Energy Compensation ÷ Gross
    # Output, 59 BEA summary industries × 1997-2024 (value_num 100% non-null; the 6,552
    # NULL production_code rows are BEA aggregate labels, sort last, parity-neutral).
    # bea_contingent_labor_intake: the staffing slice, both grains, both denominators —
    # NOTE summary-grain shares are a FLAGGED PROXY (BEA dissolves NAICS 5613 into all of
    # 561), an upper bound; the detail grain is the unproxied number. bea_io_use_summary_
    # annual answers "purchased services — of what?" and is the source the intake was
    # derived from (40.8% suppressed on value_musd; denominators T005/T018/V001/… clean).
    {"ds": "bea_bls_klems", "tier": "D", "sort": ["production_code", "sheet", "year"]},
    {"ds": "bea_contingent_labor_intake", "tier": "D",
     "sort": ["industry_code", "grain", "year"]},
    {"ds": "bea_io_use_summary_annual", "tier": "D",
     "sort": ["industry_code", "commodity_code", "year"]},
    # ── county reference authorities (geo-spine cycle 2026-07-24) — all tiny, plain
    # copies, EXACT parity. national_county2020 + census_county_gazetteer_2023: county
    # name/state authority (canonical join for pop_county_name source variants) + county
    # centroid geometry — ship BOTH; neither alone covers CT's dual FIPS vintage.
    # census_county_cbsa_2023: county→metro (CBSA/CSA) rollup, the "zoom out" leg.
    # census_county_adjacency: the honest county-neighbors sector set that replaces the
    # 94%-under-counting 45-mile haversine envelope.
    {"ds": "national_county2020", "tier": "D", "sort": ["county_fips"]},
    {"ds": "census_county_gazetteer_2023", "tier": "D", "sort": ["county_fips"]},
    {"ds": "census_county_cbsa_2023", "tier": "D", "sort": ["county_fips"]},
    {"ds": "census_county_adjacency", "tier": "D", "sort": ["county_fips", "neighbor_fips"]},
    # demo-region-grain cycle (2026-07-26): the region-MEMBERSHIP authorities, so a
    # region rollup is one in-sidecar join instead of a Lance round-trip plus a
    # client-assembled 300-county IN-list. demo_region_catalog (3,222 rows) = the drill
    # regions the bakes key on; state_region_county_map (1,398) = the intra-state
    # directional regions ("western TX") the company-region derivation resolves against;
    # equipment_flowdown_factors (60) = the per-industry equipment share the bakes weight
    # every flow-down number with. Slashed Lance paths -> explicit dest.
    {"ds": "reference/demo_region_catalog", "tier": "D", "dest": "demo_region_catalog",
     "sort": ["demo_region", "county_fips"]},
    {"ds": "reference/state_region_county_map", "tier": "D", "dest": "state_region_county_map",
     "sort": ["state_region", "county_fips"]},
    {"ds": "reference/equipment_flowdown_factors", "tier": "D",
     "dest": "equipment_flowdown_factors", "sort": ["production_code"]},
    {"ds": "gtm_sam_people", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_sam_person_contactability", "tier": "D", "sort": ["sam_person_id"]},
    # person contact channels (gap 2026-07-21-firm-contact-channels, operator-
    # directed): people ⋈ contactability pre-joined at uei grain so the firm
    # drawer's people section is one ms-class point-read with email/phone/li.
    {"ds": "gtm_sam_people", "tier": "D", "dest": "gtm_person_channels",
     "sort": ["uei"], "from_table": "gtm_sam_people",
     "after": ["gtm_sam_people", "gtm_sam_person_contactability"],
     "person_channels": True},
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
    # geo-spine cycle (2026-07-24, sidecar-gaps Mode 2 — SIDECAR_GAP_REPORT
    # 2026-07-22 Gap 1 + 2026-07-23 Entry 1/2): finer PoP geo rides the canonical
    # scan already in flight. zip5 (probe-verified 84.00% fill, 100% first-5-numeric
    # among non-null — the try_cast guard below is belt-and-suspenders), the as-acted
    # congressional district (87.47%, best-filled PoP geo), the city (82.28% — the
    # sub-county "name it" next-question), and the award-contemporaneous holder state
    # (97.02%). Row count unchanged -> txn_events_combo keeps EXACT parity; columns
    # propagate free into txn_events_combo_by_geo / _by_award.
    "primary_place_of_performance_zip_4", "pop_congressional_code",
    "pop_city_name", "recipient_state_code",
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
    -- geo-spine cycle (2026-07-24): zip5 with a numeric guard (fails closed to
    -- NULL, never corrupts), the as-acted district, city, and the holder state as
    -- of the action. NO regex on the 108M row set — a bounded substr + try_cast.
    CASE WHEN try_cast(substr(t.primary_place_of_performance_zip_4, 1, 5) AS INTEGER)
                  IS NOT NULL
         THEN substr(t.primary_place_of_performance_zip_4, 1, 5) END AS pop_zip5,
    t.pop_congressional_code,
    t.pop_city_name,
    t.recipient_state_code                            AS recipient_state,
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

# geo-spine cycle (2026-07-24): award-grain geography state. Every ON clause is a
# PURE EQUALITY key; the only probe-side gate (award_key IS NOT NULL) lives inside
# the CTE, never in an ON — no nested-loop exposure. PER-FIELD arg_max: DuckDB's
# arg_max skips rows where either argument is NULL, so each geo field independently
# pins the latest transaction that ACTUALLY CARRIES it (coverage-maximizing; this is
# why this route beats the 40.6%-coverage centroid route). Row-preserving off the
# 82.87M award_state -> EXACT parity.
_AWARD_GEO_STATE_SQL = """
CREATE TABLE award_geo_state AS
WITH geo AS (
    SELECT award_key,
           arg_max(pop_state,              action_date) AS pop_state,
           arg_max(pop_county_fips,        action_date) AS pop_county_fips,
           arg_max(pop_county_name,        action_date) AS pop_county_name,
           arg_max(pop_zip5,               action_date) AS pop_zip5,
           arg_max(pop_city_name,          action_date) AS pop_city_name,
           arg_max(pop_congressional_code, action_date) AS pop_congressional_code,
           arg_max(pop_country_code,       action_date) AS pop_country_code,
           arg_max(recipient_state,        action_date) AS recipient_state_latest,
           max(action_date)                             AS geo_as_of
    FROM txn_events_combo
    WHERE award_key IS NOT NULL
    GROUP BY 1
),
hq AS (
    SELECT uei, physical_state AS hq_state FROM gtm_sam_entities
)
SELECT s.contract_award_unique_key                       AS award_key,
       s.recipient_uei                                   AS uei,
       g.pop_state, g.pop_county_fips, g.pop_county_name,
       g.pop_zip5, g.pop_city_name, g.pop_congressional_code, g.pop_country_code,
       g.recipient_state_latest, g.geo_as_of,
       h.hq_state,
       CASE WHEN g.pop_state IS NOT NULL AND h.hq_state IS NOT NULL
            THEN (g.pop_state <> h.hq_state) END         AS is_nonlocal,
       s.life_to_date_obligated                          AS obligated,
       s.total_dollars_obligated_snapshot                AS obligated_snapshot,
       s.current_total_value_of_award                    AS current_value,
       s.remaining_ceiling_headroom,
       s.award_topology, s.naics_code,
       s.product_or_service_code                         AS psc_code,
       s.awarding_agency_code, s.awarding_sub_agency_code,
       s.type_of_set_aside_code, s.award_type_code, s.idv_type_code,
       s.first_action_date, s.last_action_date,
       s.current_end_date, s.days_to_expiry, s.is_terminated
FROM usaspending_fpds_prime_award_state s
LEFT JOIN geo g ON g.award_key = s.contract_award_unique_key
LEFT JOIN hq  h ON h.uei       = s.recipient_uei
ORDER BY g.pop_state, g.pop_county_fips, s.current_end_date
"""

# geo-spine cycle (2026-07-24, Entry 1): place × FY rollup. Pure GROUP BY, no join,
# NO count(DISTINCT) at build (the distinct-places answer is count(*) over this
# rollup's distinct rows, exact without a per-group hash). Reducing -> aggregate floor.
_POP_PLACE_FY_SQL = """
CREATE TABLE pop_place_fy AS
SELECT fy, pop_state, pop_county_fips, pop_zip5,
       count(*)         AS n_actions,
       sum(obligation)  AS obligation_sum,
       min(action_date) AS first_action_date,
       max(action_date) AS last_action_date
FROM txn_events_combo
GROUP BY 1, 2, 3, 4
ORDER BY fy, pop_state, pop_county_fips, pop_zip5
"""

# ── demo-region-grain cycle (2026-07-26, sidecar-gaps Mode 2 — SIDECAR_GAP_REPORT
# 2026-07-26-demo-region-grain, 5 entries + operator directive "too much computing /
# lag on the actual demo"). The demo bakes recompute region aggregates by scanning the
# full 108M fact once per region (21 regions/bake, 1-3 min each; the firms shape 408'd
# outright on large macros and killed a bake run after ~25 min).
#
# The promote is deliberately NOT the 21 baked region rows the demand implies. Regions
# here are UNIONS OF PLACES — macro regions are state unions (14, hardcoded in the bake
# scripts), drill regions are county-FIPS sets from reference/demo_region_catalog — and
# a new deal adds a new region. Baking region rows would mean a rebuild per region; the
# ATOM below composes ANY region (macro, drill, state, county, CBSA, a region invented
# on a call) by summing the places it contains, with no rebuild. Grain carries
# pop_state AND pop_county_fips because ~17% of actions have no county: a state-scoped
# region must keep those rows, so county alone would silently drop them.
#
# Three atoms, split by what is composable at which grain:
#   pop_combo_fy  — region $ by industry×work×FY (entries 3+4). PSC in the grain is an
#                   operator-directed rider (2026-07-26): work categories/archetypes are
#                   NAICS×PSC-defined, so category-scoped cuts must answer post-build.
#   pop_entity_fy — the firm metrics (entry 1). Firm counts/medians/growth are NOT
#                   additive across places, so the atom is per-firm: "firms winning
#                   >=$500K in-region" = GROUP BY uei over the region's places.
#                   first_action_date carries the first-time-winner metric (min over the
#                   region's places); hq_state carries local-vs-non-local.
#   pop_award_fy  — award-grain, floored (entries 1-median + 5-archetypes). Unfloored
#                   this grain is 100.2M rows (probe-measured) — no compression at all.
#                   The >=$100K award floor (probe: 16.9M of 108M actions survive ->
#                   10.5M cells) sits an order below the metrics that read it (median
#                   over awards >=$250K; archetype picks >=$5M), so both are EXACT.
_POP_COMBO_FY_SQL = """
CREATE TABLE pop_combo_fy AS
SELECT pop_state, pop_county_fips, naics_code, psc_code, fy,
       sum(obligation)                                                    AS obligation_sum,
       count(*)                                                           AS n_actions,
       count(DISTINCT award_key)                                          AS award_ct,
       sum(obligation) FILTER (WHERE type_of_set_aside_code IS NOT NULL
                                 AND type_of_set_aside_code NOT IN ('NONE','None',''))
                                                                          AS obl_set_aside,
       min(action_date)                                                   AS first_action_date,
       max(action_date)                                                   AS last_action_date
FROM txn_events_combo
GROUP BY 1, 2, 3, 4, 5
ORDER BY pop_state, pop_county_fips, naics_code, psc_code, fy
"""

# hq join is the same 1:1 uei leg _AWARD_GEO_STATE_SQL uses (gtm_sam_entities is
# 1 row/uei) — pure column equality, applied AFTER the aggregation so the join
# runs 7.4M x 2.0M instead of over the 108M fact.
_POP_ENTITY_FY_SQL = """
CREATE TABLE pop_entity_fy AS
WITH agg AS (
    SELECT pop_state, pop_county_fips, uei, fy,
           sum(obligation)                                                AS obligation_sum,
           count(*)                                                       AS n_actions,
           count(DISTINCT award_key)                                      AS award_ct,
           sum(obligation) FILTER (WHERE type_of_set_aside_code IS NOT NULL
                                     AND type_of_set_aside_code NOT IN ('NONE','None',''))
                                                                          AS obl_set_aside,
           min(action_date)                                               AS first_action_date,
           max(action_date)                                               AS last_action_date
    FROM txn_events_combo
    WHERE uei IS NOT NULL AND uei <> ''
    GROUP BY 1, 2, 3, 4
),
hq AS (
    SELECT uei, physical_state AS hq_state FROM gtm_sam_entities
)
SELECT a.pop_state, a.pop_county_fips, a.uei, a.fy,
       a.obligation_sum, a.n_actions, a.award_ct, a.obl_set_aside,
       a.first_action_date, a.last_action_date,
       h.hq_state,
       CASE WHEN a.pop_state IS NOT NULL AND h.hq_state IS NOT NULL
            THEN (a.pop_state <> h.hq_state) END                          AS is_nonlocal
FROM agg a
LEFT JOIN hq h ON h.uei = a.uei
ORDER BY a.pop_state, a.pop_county_fips, a.uei, a.fy
"""

# The $ floor is a probe-side gate on the BUILD side of the semi-join, never in the ON
# clause (that shape planned as PhysicalBlockwiseNLJoin and cost three builds).
# award_pop_state/_county ride along so ONE mart answers under BOTH place-of-performance
# semantics the bakes use: transaction-level PoP (pop_state/pop_county_fips — each action
# where it happened) and award-level PoP (award_pop_* — award_geo_state's per-field
# arg_max, one place per award, which is what the firms/econ bakes key on today).
_POP_AWARD_FY_SQL = """
CREATE TABLE pop_award_fy AS
WITH keys AS (
    SELECT award_key,
           pop_state       AS award_pop_state,
           pop_county_fips AS award_pop_county_fips
    FROM award_geo_state
    WHERE obligated >= 100000 OR current_value >= 100000
)
SELECT c.pop_state, c.pop_county_fips, c.naics_code, c.psc_code, c.award_key, c.fy,
       k.award_pop_state, k.award_pop_county_fips,
       sum(c.obligation)                    AS obligation_sum,
       count(*)                             AS n_actions,
       min(c.action_date)                   AS first_action_date,
       max(c.action_date)                   AS last_action_date,
       arg_max(c.uei,             c.action_date) AS uei,
       arg_max(c.pop_city_name,   c.action_date) AS pop_city_name,
       arg_max(c.recipient_state, c.action_date) AS recipient_state,
       arg_max(c.award_topology,  c.action_date) AS award_topology,
       arg_max(c.awarding_agency_code,     c.action_date) AS awarding_agency_code,
       arg_max(c.type_of_set_aside_code,   c.action_date) AS type_of_set_aside_code
FROM txn_events_combo c
JOIN keys k ON c.award_key = k.award_key
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY c.pop_state, c.pop_county_fips, c.naics_code, c.psc_code, c.award_key
"""

# Entry 2's active-book rollup. award_geo_state already carries every column, but it is
# 82.87M rows sorted (pop_state, pop_county_fips, current_end_date) — a region cut is
# tens of seconds, x21 regions x2 passes per bake. The ACTIVE set is only 263k awards
# (probe-measured), so the whole live book fits in a mart that reads ms-class and stays
# award-grain, i.e. count(DISTINCT uei) / per-NAICS-PSC sums / expiry / set-aside /
# agency cuts are all exact for any region. Snapshot semantics as of the award_state
# build date, same as combo_award_active_state; current_end_date rides so a consumer can
# re-apply its own as-of date. Reducing -> aggregate floor parity.
_AWARD_GEO_ACTIVE_SQL = """
CREATE TABLE award_geo_active AS
SELECT *
FROM award_geo_state
WHERE is_terminated = FALSE AND days_to_expiry > 0
ORDER BY pop_state, pop_county_fips, naics_code, psc_code
"""

# novation cycle (2026-07-24): predecessor→successor identity off the award's txn
# ladder. lag() OVER a window is NOT a self-join (no inequality ON clause), so the
# nested-loop trap cannot fire; txn_rows_by_award is already physically ordered by
# (award_key_pfx, contract_award_unique_key, action_date) so the window streams with
# no re-sort. The one join carries award_key_pfx through the CTE so both legs are bare
# column equalities. Reducing 108M -> ~88K -> aggregate floor.
_NOVATION_EVENTS_SQL = """
CREATE TABLE gtm_award_novation_events AS
WITH ladder AS (
    SELECT contract_award_unique_key                AS award_key,
           award_key_pfx,
           action_date,
           award_id_piid,
           action_type_code,
           action_type_description,
           recipient_uei                            AS to_uei,
           recipient_name                           AS to_name,
           federal_action_obligation                AS obligation,
           base_and_all_options_value,
           naics_code,
           product_or_service_code                  AS psc_code,
           awarding_agency_code,
           awarding_agency_name,
           lag(recipient_uei)  OVER w               AS from_uei,
           lag(recipient_name) OVER w               AS from_name,
           lag(action_date)    OVER w               AS prev_action_date
    FROM txn_rows_by_award
    WINDOW w AS (PARTITION BY contract_award_unique_key
                 ORDER BY action_date, contract_transaction_unique_key)
),
ev AS (
    SELECT *,
           (from_uei IS NOT NULL AND from_uei <> to_uei) AS is_uei_change
    FROM ladder
    WHERE action_type_code IN ('J', 'S', 'T')   -- NOVATION / CHANGE PIID / TRANSFER
)
SELECT e.award_key, e.award_id_piid, e.action_date, e.action_type_code,
       e.action_type_description, e.from_uei, e.from_name, e.to_uei, e.to_name,
       e.is_uei_change, e.prev_action_date, e.obligation,
       e.base_and_all_options_value, e.naics_code, e.psc_code,
       e.awarding_agency_code, e.awarding_agency_name,
       s.award_topology, s.life_to_date_obligated,
       s.current_total_value_of_award, s.current_end_date,
       s.days_to_expiry, s.is_terminated
FROM ev e
LEFT JOIN prime_award_state_by_key s
       ON s.award_key_pfx              = e.award_key_pfx
      AND s.contract_award_unique_key  = e.award_key
ORDER BY e.action_date, e.to_uei
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
       naics_code, psc_code,
       date_trunc('month', action_date) AS month,
       count(*)                         AS n_actions,
       sum(obligation)                  AS obligation_sum
FROM txn_events_combo
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY action_type_code, pop_state, pop_county_fips, month
"""
# billing-latency cycle (2026-07-16): naics/psc added to the grain (27.5M ->
# 37.0M rows, probe-measured) so event-verb × PoP-state composes with the
# job/need combo layer. Aggregations to the coarser grain stay correct
# (SUM over the finer rows); the sort is unchanged so state-anchored reads
# prune exactly as before.

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
       p.type_of_set_aside_code      AS parent_type_of_set_aside_code,
       -- billing-latency cycle (2026-07-16): pricing/plan latest-state
       -- DENORMALIZED onto the award row. Query-time joins with this 83M
       -- table saturate the 2-thread serving box (measured 32-49 s on the
       -- billing shapes); award_plan_state is GROUP BY key -> 1:1, the
       -- exact-parity gate still applies.
       ps.latest_plan,
       ps.latest_pricing_code,
       ps.latest_financing_code,
       ps.latest_business_size
FROM award_state_base a
LEFT JOIN award_ordering_windows o
       ON o.contract_award_unique_key = a.contract_award_unique_key
LEFT JOIN award_plan_state ps
       ON ps.contract_award_unique_key = a.contract_award_unique_key
LEFT JOIN parent_attrs p
       ON p.contract_award_unique_key
        = (CASE WHEN a.parent_match_flag = 'resolved' THEN a.parent_award_key_resolved END)
LEFT JOIN award_ordering_windows po
       ON po.contract_award_unique_key
        = (CASE WHEN a.parent_match_flag = 'resolved' THEN a.parent_award_key_resolved END)
ORDER BY a.current_end_date
"""

# billing-latency cycle (2026-07-16): entity-grain pricing mix — one row per
# recipient over the (now pricing-carrying) local award_state. Class map from
# fpds_code_vocab (probe-verified): fixed = A,B,J,K,L,M; cost = R,S,T,U,V;
# tm_lh = Y,Z; everything else (0,1,2,3,NO,O,P,X,null) = other/unknown.
# "Unfinanced" = financing NULL / Z / literal 'NOT APPLICABLE' (source noise
# keeps text in the code column on old rows). Active = the combo_active rule.
_ENTITY_PRICING_MIX_SQL = """
CREATE TABLE gtm_entity_pricing_mix AS
WITH cls AS (
    SELECT recipient_uei,
           total_dollars_obligated_snapshot AS obl,
           (days_to_expiry > 0 AND is_terminated = FALSE) AS is_active,
           CASE WHEN latest_pricing_code IN ('A','B','J','K','L','M') THEN 'fixed'
                WHEN latest_pricing_code IN ('R','S','T','U','V')     THEN 'cost'
                WHEN latest_pricing_code IN ('Y','Z')                 THEN 'tm_lh'
                ELSE 'other' END AS pricing_class,
           -- pricing×financing cycle (2026-07-17, operator directive): financing
           -- class from latest_financing_code. Legacy rows keep description TEXT
           -- in the code column (same noise pattern as 'NOT APPLICABLE') — each
           -- class lists its text twins. F/N/X are undocumented in the probed
           -- inventory -> othfin (disposition records the parked decode).
           CASE WHEN latest_financing_code IS NULL
                  OR latest_financing_code IN ('Z','NOT APPLICABLE')            THEN 'unfin'
                WHEN latest_financing_code IN ('A','B','E',
                     'FAR 52.232-16 PROGRESS PAYMENTS',
                     'UNUSUAL PROGRESS PAYMENTS OR ADVANCE PAYMENTS',
                     'PERCENTAGE OF COMPLETION PROGRESS PAYMENTS')              THEN 'prog'
                WHEN latest_financing_code IN ('C','PERFORMANCE-BASED FINANCING') THEN 'perf'
                WHEN latest_financing_code IN ('D','COMMERCIAL FINANCING')      THEN 'comm'
                ELSE 'othfin' END AS financing_class,
           (latest_pricing_code = 'J') AS is_ffp,
           (latest_financing_code IS NULL
            OR latest_financing_code IN ('Z', 'NOT APPLICABLE'))      AS is_unfinanced,
           (latest_business_size = 'S')                               AS is_small_det,
           -- instrument riders (test-log entry 9): standalone split D vs B
           (award_type_code = 'D') AS is_definitive,
           (award_type_code = 'B') AS is_purchase_order
    FROM usaspending_fpds_prime_award_state
    WHERE recipient_uei IS NOT NULL AND recipient_uei <> ''
)
SELECT recipient_uei                                                   AS uei,
       count(*)                                                        AS award_ct,
       sum(obl)                                                        AS obl_total,
       count(*)  FILTER (WHERE is_active)                              AS active_award_ct,
       sum(obl)  FILTER (WHERE is_active)                              AS active_obl,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed')  AS active_obl_fixed,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'cost')   AS active_obl_cost,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'tm_lh')  AS active_obl_tm_lh,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'other')  AS active_obl_other,
       count(*)  FILTER (WHERE is_active AND is_ffp AND is_unfinanced) AS active_ffp_unfinanced_ct,
       sum(obl)  FILTER (WHERE is_active AND is_ffp AND is_unfinanced) AS active_obl_ffp_unfinanced,
       sum(obl)  FILTER (WHERE is_active AND is_small_det)             AS active_obl_small_determined,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed')
           / NULLIF(sum(obl) FILTER (WHERE is_active), 0)              AS active_fixed_share,
       sum(obl)  FILTER (WHERE is_active AND is_ffp AND is_unfinanced)
           / NULLIF(sum(obl) FILTER (WHERE is_active), 0)              AS active_ffp_unfinanced_share,
       -- financing-class rollups (any pricing)
       sum(obl)  FILTER (WHERE is_active AND financing_class = 'unfin')  AS active_obl_fin_unfin,
       count(*)  FILTER (WHERE is_active AND financing_class = 'unfin')  AS active_fin_unfin_ct,
       sum(obl)  FILTER (WHERE is_active AND financing_class = 'prog')   AS active_obl_fin_prog,
       count(*)  FILTER (WHERE is_active AND financing_class = 'prog')   AS active_fin_prog_ct,
       sum(obl)  FILTER (WHERE is_active AND financing_class = 'perf')   AS active_obl_fin_perf,
       count(*)  FILTER (WHERE is_active AND financing_class = 'perf')   AS active_fin_perf_ct,
       sum(obl)  FILTER (WHERE is_active AND financing_class = 'comm')   AS active_obl_fin_comm,
       count(*)  FILTER (WHERE is_active AND financing_class = 'comm')   AS active_fin_comm_ct,
       sum(obl)  FILTER (WHERE is_active AND financing_class = 'othfin') AS active_obl_fin_othfin,
       count(*)  FILTER (WHERE is_active AND financing_class = 'othfin') AS active_fin_othfin_ct,
       sum(obl)  FILTER (WHERE is_active AND financing_class <> 'unfin')
           / NULLIF(sum(obl) FILTER (WHERE is_active), 0)              AS active_financed_share,
       -- the full pricing×financing matrix (first-class combo predicates)
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'unfin') AS active_obl_fixed_unfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'unfin') AS active_fixed_unfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'prog') AS active_obl_fixed_prog,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'prog') AS active_fixed_prog_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'perf') AS active_obl_fixed_perf,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'perf') AS active_fixed_perf_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'comm') AS active_obl_fixed_comm,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'comm') AS active_fixed_comm_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'othfin') AS active_obl_fixed_othfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'fixed' AND financing_class = 'othfin') AS active_fixed_othfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'unfin') AS active_obl_cost_unfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'unfin') AS active_cost_unfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'prog') AS active_obl_cost_prog,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'prog') AS active_cost_prog_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'perf') AS active_obl_cost_perf,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'perf') AS active_cost_perf_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'comm') AS active_obl_cost_comm,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'comm') AS active_cost_comm_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'othfin') AS active_obl_cost_othfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'cost' AND financing_class = 'othfin') AS active_cost_othfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'unfin') AS active_obl_tm_lh_unfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'unfin') AS active_tm_lh_unfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'prog') AS active_obl_tm_lh_prog,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'prog') AS active_tm_lh_prog_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'perf') AS active_obl_tm_lh_perf,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'perf') AS active_tm_lh_perf_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'comm') AS active_obl_tm_lh_comm,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'comm') AS active_tm_lh_comm_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'othfin') AS active_obl_tm_lh_othfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'tm_lh' AND financing_class = 'othfin') AS active_tm_lh_othfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'unfin') AS active_obl_other_unfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'unfin') AS active_other_unfin_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'prog') AS active_obl_other_prog,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'prog') AS active_other_prog_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'perf') AS active_obl_other_perf,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'perf') AS active_other_perf_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'comm') AS active_obl_other_comm,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'comm') AS active_other_comm_ct,
       sum(obl)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'othfin') AS active_obl_other_othfin,
       count(*)  FILTER (WHERE is_active AND pricing_class = 'other' AND financing_class = 'othfin') AS active_other_othfin_ct,
       -- instrument riders: standalone split, lifetime AND active (free on this scan)
       count(*)  FILTER (WHERE is_definitive)                          AS lifetime_definitive_ct,
       count(*)  FILTER (WHERE is_purchase_order)                      AS lifetime_purchase_order_ct,
       count(*)  FILTER (WHERE is_active AND is_definitive)            AS active_definitive_ct,
       count(*)  FILTER (WHERE is_active AND is_purchase_order)        AS active_purchase_order_ct,
       sum(obl)  FILTER (WHERE is_active AND is_definitive)            AS active_obl_definitive,
       sum(obl)  FILTER (WHERE is_active AND is_purchase_order)        AS active_obl_purchase_order
FROM cls
GROUP BY 1
ORDER BY uei
"""

# market-composition cycle (2026-07-17): entity × federal-FY won. fy is
# precomputed on the combo fact; set-aside families per the probe-verified
# code inventory ('NONE'/'None' = none; 8A/8AN, SDVOSB*, *WOSB*, HZ* are the
# program families worth their own columns — everything else aggregates into
# won_obl_set_aside). Pure GROUP BY over a local table — no join.
# construction work-lane pair-sets v1 (hq/data-cache/surety/construction_pairsets_v1.json,
# composed 2026-07-19 from the 36mo prime pair sweep; 544 pairs, five work-typed lanes;
# the durable authority is the hq JSON — regenerate this constant from it, never edit inline).
_CONSTRUCTION_LANE_PAIRS: dict[str, list[tuple[str, str]]] = {
    "construction-vertical-building": [
        ("236115","Y1FA"), ("236115","Y1JZ"), ("236116","Y1DA"), ("236116","Y1FA"), ("236116","Y1FC"),
        ("236118","Y1FA"), ("236210","Y1AA"), ("236210","Y1AZ"), ("236210","Y1CZ"), ("236210","Y1DB"),
        ("236210","Y1FB"), ("236210","Y1JZ"), ("236220","Y142"), ("236220","Y1AA"), ("236220","Y1AZ"),
        ("236220","Y1BE"), ("236220","Y1CA"), ("236220","Y1CZ"), ("236220","Y1DA"), ("236220","Y1DB"),
        ("236220","Y1DZ"), ("236220","Y1FA"), ("236220","Y1FB"), ("236220","Y1FC"), ("236220","Y1FD"),
        ("236220","Y1FE"), ("236220","Y1FF"), ("236220","Y1GC"), ("236220","Y1GZ"), ("236220","Y1HZ"),
        ("236220","Y1JA"), ("236220","Y1JZ"), ("236220","Y1PA"), ("237110","Y1AZ"), ("237110","Y1DA"),
        ("237110","Y1JZ"), ("237110","Y1PA"), ("237120","Y1DZ"), ("237120","Y1GC"), ("237130","Y1AZ"),
        ("237130","Y1JZ"), ("237310","Y1PA"), ("237990","Y1AZ"), ("237990","Y1DZ"), ("237990","Y1FE"),
        ("237990","Y1JZ"), ("237990","Y1PA"), ("238160","Y1AA"), ("238160","Y1AZ"), ("238160","Y1DA"),
        ("238160","Y1DZ"), ("238160","Y1JZ"), ("238210","Y1AZ"), ("238210","Y1DA"), ("238210","Y1JZ"),
        ("238220","Y1AA"), ("238220","Y1AZ"), ("238220","Y1DA"), ("238220","Y1DZ"), ("238220","Y1JZ"),
        ("238290","Y1AA"), ("238290","Y1DA"), ("238290","Y1JZ"), ("238390","Y1DZ"), ("238910","Y1AZ"),
        ("238910","Y1JZ"), ("238990","Y141"), ("238990","Y1AA"), ("238990","Y1AZ"), ("238990","Y1CA"),
        ("238990","Y1DA"), ("238990","Y1DZ"), ("238990","Y1FA"), ("238990","Y1FB"), ("238990","Y1GB"),
        ("238990","Y1GZ"), ("238990","Y1JA"), ("238990","Y1JZ"), ("238990","Y1PA"),
    ],
    "construction-building-repair-alteration": [
        ("236116","Z2FA"), ("236118","Z2FA"), ("236210","Z2AA"), ("236210","Z2AZ"), ("236210","Z2GZ"),
        ("236210","Z2JZ"), ("236220","Z2AA"), ("236220","Z2AZ"), ("236220","Z2BE"), ("236220","Z2CA"),
        ("236220","Z2CZ"), ("236220","Z2DA"), ("236220","Z2DB"), ("236220","Z2DZ"), ("236220","Z2FA"),
        ("236220","Z2FB"), ("236220","Z2FC"), ("236220","Z2FD"), ("236220","Z2FE"), ("236220","Z2FF"),
        ("236220","Z2GC"), ("236220","Z2GZ"), ("236220","Z2JA"), ("236220","Z2JZ"), ("236220","Z2PA"),
        ("236220","Z2PB"), ("237110","Z2AZ"), ("237110","Z2FF"), ("237110","Z2JZ"), ("237120","Z2GC"),
        ("237130","Z2AA"), ("237130","Z2AZ"), ("237130","Z2DZ"), ("237130","Z2GZ"), ("237130","Z2JZ"),
        ("237310","Z2AA"), ("237310","Z2JZ"), ("237990","Z2AA"), ("237990","Z2AZ"), ("237990","Z2FC"),
        ("237990","Z2JZ"), ("237990","Z2PA"), ("238110","Z2JZ"), ("238140","Z2AA"), ("238140","Z2FC"),
        ("238150","Z2AA"), ("238160","Z2AA"), ("238160","Z2AZ"), ("238160","Z2CZ"), ("238160","Z2DA"),
        ("238160","Z2FA"), ("238160","Z2FF"), ("238160","Z2GZ"), ("238160","Z2JZ"), ("238190","Z2DB"),
        ("238190","Z2FF"), ("238190","Z2JZ"), ("238210","Z2AA"), ("238210","Z2AZ"), ("238210","Z2DA"),
        ("238210","Z2FC"), ("238210","Z2FF"), ("238210","Z2GZ"), ("238210","Z2JZ"), ("238220","Z2AA"),
        ("238220","Z2AZ"), ("238220","Z2CA"), ("238220","Z2CZ"), ("238220","Z2DA"), ("238220","Z2DB"),
        ("238220","Z2FA"), ("238220","Z2FB"), ("238220","Z2FC"), ("238220","Z2FD"), ("238220","Z2FF"),
        ("238220","Z2JZ"), ("238290","Z2AA"), ("238290","Z2AZ"), ("238290","Z2DZ"), ("238290","Z2FF"),
        ("238290","Z2JZ"), ("238320","Z2AA"), ("238320","Z2AZ"), ("238320","Z2JZ"), ("238330","Z2AA"),
        ("238390","Z2AA"), ("238390","Z2JZ"), ("238910","Z2AZ"), ("238910","Z2FC"), ("238910","Z2JZ"),
        ("238990","Z2AA"), ("238990","Z2AZ"), ("238990","Z2FA"), ("238990","Z2FC"), ("238990","Z2FD"),
        ("238990","Z2FF"), ("238990","Z2JZ"), ("238990","Z2PA"),
    ],
    "construction-building-maintenance": [
        ("236118","Z1AA"), ("236118","Z1FA"), ("236118","Z1FC"), ("236210","Z1AA"), ("236210","Z1AZ"),
        ("236210","Z1JZ"), ("236220","Z1AA"), ("236220","Z1AZ"), ("236220","Z1CA"), ("236220","Z1CZ"),
        ("236220","Z1DA"), ("236220","Z1DB"), ("236220","Z1DZ"), ("236220","Z1FA"), ("236220","Z1FB"),
        ("236220","Z1FC"), ("236220","Z1GC"), ("236220","Z1GZ"), ("236220","Z1JA"), ("236220","Z1JZ"),
        ("237110","Z1DA"), ("237110","Z1JZ"), ("237130","Z1AA"), ("237130","Z1DA"), ("237310","Z1AZ"),
        ("237990","Z1JZ"), ("237990","Z1PA"), ("238140","Z1AZ"), ("238140","Z1DZ"), ("238140","Z1JZ"),
        ("238160","Z1AA"), ("238160","Z1AZ"), ("238160","Z1DA"), ("238160","Z1FC"), ("238160","Z1JZ"),
        ("238190","Z1DA"), ("238210","Z1AA"), ("238210","Z1AZ"), ("238210","Z1DA"), ("238210","Z1JZ"),
        ("238220","Z1AA"), ("238220","Z1AZ"), ("238220","Z1DA"), ("238220","Z1DB"), ("238220","Z1DZ"),
        ("238220","Z1JZ"), ("238290","Z1AA"), ("238290","Z1AZ"), ("238290","Z1DA"), ("238290","Z1DZ"),
        ("238290","Z1JZ"), ("238320","Z1AA"), ("238320","Z1AZ"), ("238320","Z1FA"), ("238320","Z1JZ"),
        ("238330","Z1AA"), ("238990","Z1AA"), ("238990","Z1AZ"), ("238990","Z1CA"), ("238990","Z1DA"),
        ("238990","Z1FA"), ("238990","Z1FB"), ("238990","Z1JZ"), ("238990","Z1PA"),
    ],
    "construction-civil-infrastructure": [
        ("236210","Y1BZ"), ("236210","Y1NZ"), ("236210","Y1QA"), ("236210","Z1BG"), ("236210","Z2BG"),
        ("236210","Z2KA"), ("236210","Z2NA"), ("236210","Z2NZ"), ("236220","Y1BA"), ("236220","Y1BC"),
        ("236220","Y1BD"), ("236220","Y1BF"), ("236220","Y1BG"), ("236220","Y1BZ"), ("236220","Y1KZ"),
        ("236220","Y1LB"), ("236220","Y1LC"), ("236220","Y1LZ"), ("236220","Y1NA"), ("236220","Y1NC"),
        ("236220","Y1ND"), ("236220","Y1NE"), ("236220","Y1NZ"), ("236220","Y1PD"), ("236220","Y1PZ"),
        ("236220","Y1QA"), ("236220","Z1BA"), ("236220","Z1BC"), ("236220","Z1BD"), ("236220","Z1BG"),
        ("236220","Z1BZ"), ("236220","Z1KB"), ("236220","Z1LB"), ("236220","Z1LZ"), ("236220","Z1NA"),
        ("236220","Z1ND"), ("236220","Z1NE"), ("236220","Z1NZ"), ("236220","Z1PD"), ("236220","Z1PZ"),
        ("236220","Z1QA"), ("236220","Z2BA"), ("236220","Z2BC"), ("236220","Z2BD"), ("236220","Z2BF"),
        ("236220","Z2BG"), ("236220","Z2BZ"), ("236220","Z2KA"), ("236220","Z2KF"), ("236220","Z2LB"),
        ("236220","Z2LC"), ("236220","Z2LZ"), ("236220","Z2NA"), ("236220","Z2ND"), ("236220","Z2NE"),
        ("236220","Z2NZ"), ("236220","Z2PD"), ("236220","Z2PZ"), ("236220","Z2QA"), ("237110","Y1BC"),
        ("237110","Y1KA"), ("237110","Y1KB"), ("237110","Y1KZ"), ("237110","Y1NC"), ("237110","Y1ND"),
        ("237110","Y1NE"), ("237110","Y1NZ"), ("237110","Y1PD"), ("237110","Y1PZ"), ("237110","Z1ND"),
        ("237110","Z1NE"), ("237110","Z2BC"), ("237110","Z2BZ"), ("237110","Z2ND"), ("237110","Z2NE"),
        ("237110","Z2NZ"), ("237110","Z2PD"), ("237110","Z2PZ"), ("237110","Z2QA"), ("237120","Y1NA"),
        ("237120","Y1QA"), ("237120","Z1NA"), ("237120","Z2NA"), ("237120","Z2NZ"), ("237130","Y1BC"),
        ("237130","Y1BF"), ("237130","Y1BG"), ("237130","Y1BZ"), ("237130","Y1KA"), ("237130","Y1NZ"),
        ("237130","Y1PZ"), ("237130","Z1BG"), ("237130","Z1KA"), ("237130","Z2BA"), ("237130","Z2BC"),
        ("237130","Z2BG"), ("237130","Z2BZ"), ("237130","Z2KA"), ("237130","Z2NZ"), ("237130","Z2PZ"),
        ("237130","Z2QA"), ("237310","Y1BC"), ("237310","Y1BD"), ("237310","Y1BZ"), ("237310","Y1KA"),
        ("237310","Y1LA"), ("237310","Y1LB"), ("237310","Y1LZ"), ("237310","Y1QA"), ("237310","Z1BC"),
        ("237310","Z1BD"), ("237310","Z1KA"), ("237310","Z1LB"), ("237310","Z1LZ"), ("237310","Z1PZ"),
        ("237310","Z2BD"), ("237310","Z2BZ"), ("237310","Z2KA"), ("237310","Z2LB"), ("237310","Z2LZ"),
        ("237310","Z2NE"), ("237310","Z2PZ"), ("237310","Z2QA"), ("237990","Y1BC"), ("237990","Y1BD"),
        ("237990","Y1BF"), ("237990","Y1BZ"), ("237990","Y1KA"), ("237990","Y1KB"), ("237990","Y1KF"),
        ("237990","Y1KZ"), ("237990","Y1LA"), ("237990","Y1LB"), ("237990","Y1LC"), ("237990","Y1LZ"),
        ("237990","Y1NA"), ("237990","Y1ND"), ("237990","Y1NE"), ("237990","Y1NZ"), ("237990","Y1PD"),
        ("237990","Y1PZ"), ("237990","Y1QA"), ("237990","Z1KA"), ("237990","Z1KB"), ("237990","Z1KE"),
        ("237990","Z1KF"), ("237990","Z1KZ"), ("237990","Z1LB"), ("237990","Z1LC"), ("237990","Z1PZ"),
        ("237990","Z1QA"), ("237990","Z2BC"), ("237990","Z2BZ"), ("237990","Z2KA"), ("237990","Z2KB"),
        ("237990","Z2KF"), ("237990","Z2KZ"), ("237990","Z2LB"), ("237990","Z2NA"), ("237990","Z2NE"),
        ("237990","Z2NZ"), ("237990","Z2PZ"), ("237990","Z2QA"), ("238110","Y1PZ"), ("238110","Z1KA"),
        ("238120","Y1BZ"), ("238120","Y1QA"), ("238160","Y1BZ"), ("238160","Y1QA"), ("238160","Z2BZ"),
        ("238160","Z2QA"), ("238210","Y1NZ"), ("238210","Y1PZ"), ("238210","Z1BG"), ("238210","Z1KA"),
        ("238210","Z1NZ"), ("238210","Z2BD"), ("238210","Z2BF"), ("238210","Z2BG"), ("238210","Z2BZ"),
        ("238210","Z2KA"), ("238210","Z2NZ"), ("238210","Z2PZ"), ("238210","Z2QA"), ("238220","Y1BA"),
        ("238220","Y1BC"), ("238220","Y1BZ"), ("238220","Y1LC"), ("238220","Y1NZ"), ("238220","Y1QA"),
        ("238220","Z1BA"), ("238220","Z1BZ"), ("238220","Z1LZ"), ("238220","Z2NZ"), ("238220","Z2PZ"),
        ("238220","Z2QA"), ("238290","Y1PZ"), ("238290","Z1BA"), ("238290","Z1KA"), ("238290","Z1PZ"),
        ("238290","Z2KA"), ("238290","Z2PZ"), ("238290","Z2QA"), ("238320","Y1QA"), ("238320","Z1PZ"),
        ("238320","Z2KA"), ("238390","Z2NZ"), ("238910","Y1LZ"), ("238910","Y1PZ"), ("238910","Z1PZ"),
        ("238910","Z2BF"), ("238910","Z2PZ"), ("238990","Y1BZ"), ("238990","Y1LB"), ("238990","Y1LZ"),
        ("238990","Y1QA"), ("238990","Z1BA"), ("238990","Z1LB"), ("238990","Z1NA"), ("238990","Z1PZ"),
        ("238990","Z1QA"), ("238990","Z2BG"), ("238990","Z2BZ"), ("238990","Z2LB"), ("238990","Z2LZ"),
        ("238990","Z2NA"), ("238990","Z2ND"), ("238990","Z2NZ"), ("238990","Z2PZ"), ("238990","Z2QA"),
    ],
    "construction-industrial-defense-facilities": [
        ("234930","Y181"), ("236210","Y1EA"), ("236210","Y1EB"), ("236210","Y1EZ"), ("236210","Z2EB"),
        ("236210","Z2EC"), ("236210","Z2ED"), ("236210","Z2EZ"), ("236210","Z2NB"), ("236220","Y1EA"),
        ("236220","Y1EB"), ("236220","Y1EC"), ("236220","Y1ED"), ("236220","Y1EZ"), ("236220","Y1GA"),
        ("236220","Y1NB"), ("236220","Z1EB"), ("236220","Z1EC"), ("236220","Z1ED"), ("236220","Z1EZ"),
        ("236220","Z1GA"), ("236220","Z1NB"), ("236220","Z2EA"), ("236220","Z2EB"), ("236220","Z2EC"),
        ("236220","Z2ED"), ("236220","Z2EZ"), ("236220","Z2GA"), ("236220","Z2MF"), ("236220","Z2NB"),
        ("237110","Z1ED"), ("237110","Z1MD"), ("237110","Z2NB"), ("237120","Y1MB"), ("237130","Y1EZ"),
        ("237130","Y1MF"), ("237130","Y1MG"), ("237130","Y1MZ"), ("237130","Y1NB"), ("237130","Z2EZ"),
        ("237130","Z2MD"), ("237130","Z2MZ"), ("237990","Y1EA"), ("237990","Y1EB"), ("237990","Y1EC"),
        ("237990","Y1ED"), ("237990","Y1GA"), ("237990","Z1MD"), ("237990","Z2EA"), ("237990","Z2EB"),
        ("237990","Z2ED"), ("237990","Z2GA"), ("237990","Z2MD"), ("238110","Z2EC"), ("238160","Z2EB"),
        ("238160","Z2EC"), ("238160","Z2EZ"), ("238210","Y1MG"), ("238210","Z2EC"), ("238210","Z2EZ"),
        ("238220","Y1EZ"), ("238220","Y1NB"), ("238220","Z1NB"), ("238220","Z2EB"), ("238220","Z2EC"),
        ("238220","Z2EZ"), ("238220","Z2NB"), ("238290","Z2EB"), ("238320","Y1MD"), ("238990","Z1EB"),
        ("238990","Z2EA"), ("238990","Z2EB"), ("238990","Z2EZ"),
    ],
}

# growth-lane cycle (2026-07-19): firm × lane × month over the local slim txn
# fact. Pure-equality VALUES join (naics, psc) — hash-joinable; EXPLAIN-gated
# at fixture time. Reducing GROUP BY → aggregate parity. new-award split keys
# on FPDS's NULL action type (the base award row — market_query_v1 precedent).
_CONSTRUCTION_LANE_MONTHS_SQL = (
    "CREATE TABLE gtm_construction_lane_months AS\n"
    "WITH pairs(lane, naics_code, psc_code) AS (VALUES "
    + ",".join(
        f"('{lane}','{n}','{p}')"
        for lane, prs in _CONSTRUCTION_LANE_PAIRS.items()
        for n, p in prs
    )
    + ")\n"
    """
SELECT pr.lane,
       t.uei,
       date_trunc('month', t.action_date)::DATE                          AS month,
       sum(t.obligation)                                                 AS obligation_sum,
       count(*)                                                          AS n_actions,
       count(DISTINCT t.award_key)                                       AS n_awards,
       count(*) FILTER (WHERE t.action_type_code IS NULL)                AS n_new_awards,
       coalesce(sum(t.obligation) FILTER (WHERE t.action_type_code IS NULL), 0)
                                                                         AS new_award_obligation_sum,
       count(DISTINCT t.awarding_agency_code)                            AS n_agencies
FROM gtm_txn_events_slim t
JOIN pairs pr
  ON t.naics_code = pr.naics_code AND t.psc_code = pr.psc_code
WHERE t.uei IS NOT NULL AND t.uei <> '' AND t.action_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY lane, uei, month
"""
)

_ENTITY_FY_WON_SQL = """
CREATE TABLE gtm_entity_fy_won AS
SELECT uei,
       fy,
       sum(obligation)                                                    AS won_obl,
       count(*)                                                           AS action_ct,
       count(DISTINCT award_key)                                          AS award_ct,
       sum(obligation) FILTER (WHERE type_of_set_aside_code IS NOT NULL
                                 AND type_of_set_aside_code NOT IN ('NONE','None',''))
                                                                          AS won_obl_set_aside,
       sum(obligation) FILTER (WHERE type_of_set_aside_code LIKE '8A%')   AS won_obl_8a,
       sum(obligation) FILTER (WHERE type_of_set_aside_code LIKE 'SDVOSB%') AS won_obl_sdvosb,
       sum(obligation) FILTER (WHERE type_of_set_aside_code LIKE '%WOSB%') AS won_obl_wosb,
       sum(obligation) FILTER (WHERE type_of_set_aside_code LIKE 'HZ%')   AS won_obl_hubzone
FROM txn_events_combo
WHERE uei IS NOT NULL AND uei <> '' AND fy IS NOT NULL
GROUP BY 1, 2
ORDER BY uei, fy
"""


def _entity_pricing_flow_sql(e12: str, e24: str, e48: str) -> str:
    """capitalization-triggers cycle (2026-07-21, sidecar-gaps Mode 2 —
    SIDECAR_GAP_REPORT_2026-07-21-capitalization-triggers.md entries 2+3): the
    entity-grain trailing-window FLOW — recent-24mo vs prior-24mo obligations
    split by pricing class — the velocity/transition complement to
    gtm_entity_pricing_mix's active STOCK. Detects the FFP -> cost/T&M contract-
    type shift (entry 2, a 2.8s combo scan made ms-class) and carries SCA/DBA
    labor-covered exposure (entry 3, was 0.8-3.1s) + financing / new-award /
    buyer-geo-breadth riders on the SAME scan (adjacency sweep). Class map
    identical to _ENTITY_PRICING_MIX_SQL (fixed A,B,J,K,L,M; cost R,S,T,U,V;
    tm_lh Y,Z; else other); unfinanced = financing NULL/Z/NOT APPLICABLE;
    labor-covered = labor_standards_code='Y'; new-award = action_type_code IS
    NULL (FPDS base row). Windows anchored to max(action_date) — the FPDS-lag
    watermark, NOT current_date — computed by the dispatch branch and inlined as
    DATE literals so the plan is a single pure GROUP BY (no join / cross-product,
    EXPLAIN-clean by construction). Share numerators coalesce to 0 (0.0 = window
    had activity, none of that class; NULL = no activity in the window). Reducing
    GROUP BY -> aggregate parity. 48-month prune: firms dark >48mo carry no
    recent-flow signal and are legitimately absent."""
    return f"""
CREATE TABLE gtm_entity_pricing_flow AS
WITH ev AS (
    SELECT uei,
           action_date,
           obligation AS obl,
           CASE WHEN pricing_code IN ('A','B','J','K','L','M') THEN 'fixed'
                WHEN pricing_code IN ('R','S','T','U','V')     THEN 'cost'
                WHEN pricing_code IN ('Y','Z')                 THEN 'tm_lh'
                ELSE 'other' END                               AS pricing_class,
           (financing_code IS NULL
            OR financing_code IN ('Z','NOT APPLICABLE'))       AS is_unfinanced,
           (labor_standards_code = 'Y')                        AS is_labor_covered,
           (action_type_code IS NULL)                          AS is_new_award,
           (co_business_size = 'S')                            AS is_small_co,
           (action_date >  DATE '{e24}')                                   AS r24,
           (action_date <= DATE '{e24}' AND action_date > DATE '{e48}')    AS p24,
           (action_date >  DATE '{e12}')                                   AS r12,
           awarding_agency_code,
           pop_state
    FROM txn_events_combo
    WHERE uei IS NOT NULL AND uei <> ''
      AND action_date IS NOT NULL AND action_date > DATE '{e48}'
)
SELECT uei,
       coalesce(sum(obl) FILTER (WHERE r24), 0)                            AS obl_total_recent24,
       coalesce(sum(obl) FILTER (WHERE p24), 0)                            AS obl_total_prior24,
       coalesce(sum(obl) FILTER (WHERE r12), 0)                            AS obl_total_recent12,
       count(*)          FILTER (WHERE r24)                                AS action_ct_recent24,
       count(*)          FILTER (WHERE p24)                                AS action_ct_prior24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class='fixed'), 0)  AS obl_fixed_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class='cost'),  0)  AS obl_cost_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class='tm_lh'), 0)  AS obl_tm_lh_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class='other'), 0)  AS obl_other_recent24,
       coalesce(sum(obl) FILTER (WHERE p24 AND pricing_class='fixed'), 0)  AS obl_fixed_prior24,
       coalesce(sum(obl) FILTER (WHERE p24 AND pricing_class='cost'),  0)  AS obl_cost_prior24,
       coalesce(sum(obl) FILTER (WHERE p24 AND pricing_class='tm_lh'), 0)  AS obl_tm_lh_prior24,
       coalesce(sum(obl) FILTER (WHERE p24 AND pricing_class='other'), 0)  AS obl_other_prior24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class='cost'), 0)
           / NULLIF(sum(obl) FILTER (WHERE r24), 0)                        AS cost_share_recent24,
       coalesce(sum(obl) FILTER (WHERE p24 AND pricing_class='cost'), 0)
           / NULLIF(sum(obl) FILTER (WHERE p24), 0)                        AS cost_share_prior24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class IN ('cost','tm_lh')), 0)
           / NULLIF(sum(obl) FILTER (WHERE r24), 0)                        AS cost_tm_share_recent24,
       coalesce(sum(obl) FILTER (WHERE p24 AND pricing_class IN ('cost','tm_lh')), 0)
           / NULLIF(sum(obl) FILTER (WHERE p24), 0)                        AS cost_tm_share_prior24,
       coalesce(sum(obl) FILTER (WHERE r24 AND pricing_class='fixed'), 0)
           / NULLIF(sum(obl) FILTER (WHERE r24), 0)                        AS fixed_share_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_labor_covered), 0)       AS obl_labor_covered_recent24,
       coalesce(sum(obl) FILTER (WHERE p24 AND is_labor_covered), 0)       AS obl_labor_covered_prior24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_labor_covered), 0)
           / NULLIF(sum(obl) FILTER (WHERE r24), 0)                        AS labor_covered_share_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_unfinanced), 0)          AS obl_unfinanced_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_unfinanced), 0)
           / NULLIF(sum(obl) FILTER (WHERE r24), 0)                        AS unfinanced_share_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_new_award), 0)           AS obl_new_award_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_new_award), 0)
           / NULLIF(sum(obl) FILTER (WHERE r24), 0)                        AS new_award_share_recent24,
       count(DISTINCT awarding_agency_code) FILTER (WHERE r24)             AS n_agencies_recent24,
       count(DISTINCT pop_state)            FILTER (WHERE r24)             AS n_states_recent24,
       coalesce(sum(obl) FILTER (WHERE r24 AND is_small_co), 0)            AS obl_small_co_recent24
FROM ev
GROUP BY uei
ORDER BY uei
"""


# market-composition cycle (2026-07-17): entity-grain award book. Doctrine
# encoded once: active = days_to_expiry > 0 AND NOT terminated; committed =
# topology <> 'vehicle' (standalone + orders); every $ floored at 0 per award;
# committed value/runway at current_total_value_of_award; vehicles at
# potential_ceiling — reported as separate labeled columns, never summed.
# Pure GROUP BY over the local award_state — no join.
_ENTITY_AWARD_BOOK_SQL = """
CREATE TABLE gtm_entity_award_book AS
WITH a AS (
    SELECT recipient_uei,
           (days_to_expiry > 0 AND is_terminated = FALSE)            AS is_active,
           (award_topology <> 'vehicle')                             AS is_committed,
           greatest(coalesce(current_total_value_of_award, 0), 0)    AS cvalue,
           greatest(coalesce(total_dollars_obligated_snapshot, 0), 0) AS obl,
           greatest(coalesce(current_total_value_of_award, 0)
                    - coalesce(total_dollars_obligated_snapshot, 0), 0) AS runway,
           greatest(coalesce(potential_ceiling, 0), 0)               AS ceiling,
           greatest(coalesce(remaining_ceiling_headroom, 0), 0)      AS headroom,
           current_end_date,
           awarding_agency_code,
           (type_of_set_aside_code IS NOT NULL
            AND type_of_set_aside_code NOT IN ('NONE','None',''))    AS is_set_aside
    FROM usaspending_fpds_prime_award_state
    WHERE recipient_uei IS NOT NULL AND recipient_uei <> ''
)
SELECT recipient_uei                                                      AS uei,
       count(*)        FILTER (WHERE is_active AND is_committed)          AS committed_award_ct,
       sum(cvalue)     FILTER (WHERE is_active AND is_committed)          AS committed_value,
       sum(obl)        FILTER (WHERE is_active AND is_committed)          AS committed_obligated,
       sum(runway)     FILTER (WHERE is_active AND is_committed)          AS committed_runway,
       median(cvalue)  FILTER (WHERE is_active AND is_committed)          AS committed_award_median,
       avg(cvalue)     FILTER (WHERE is_active AND is_committed)          AS committed_award_avg,
       sum(cvalue)     FILTER (WHERE is_active AND is_committed AND is_set_aside)
                                                                          AS committed_value_set_aside,
       count(*)        FILTER (WHERE is_active AND NOT is_committed)      AS vehicle_ct,
       sum(ceiling)    FILTER (WHERE is_active AND NOT is_committed)      AS vehicle_ceiling,
       sum(headroom)   FILTER (WHERE is_active AND NOT is_committed)      AS vehicle_headroom,
       min(current_end_date) FILTER (WHERE is_active AND is_committed)    AS next_committed_end_date,
       count(DISTINCT awarding_agency_code) FILTER (WHERE is_active)      AS active_agency_ct
FROM a
GROUP BY 1
ORDER BY uei
"""

# market-composition cycle (2026-07-17): uei-grain firmographics off the two
# local tables. Single pure-equality join key (pdl_company_id); best-row-per-
# uei rule is deterministic and documented in the manifest comment.
_ENTITY_FIRMOGRAPHICS_SQL = """
CREATE TABLE gtm_entity_firmographics AS
WITH ranked AS (
    SELECT b.uei, b.duns, p.pdl_company_id, p.company_name, p.normalized_domain,
           p.is_generic_domain, p.linkedin_slug, p.locality, p.region, p.country,
           p.industry, p.employee_size_range, p.year_founded,
           row_number() OVER (
               PARTITION BY b.uei
               ORDER BY (p.employee_size_range IS NOT NULL) DESC,
                        (coalesce(p.is_generic_domain, TRUE) = FALSE) DESC,
                        p.pdl_company_id
           ) AS rn
    FROM bridge_sam_pdl b
    JOIN pdl_normalized_companies p ON p.pdl_company_id = b.pdl_company_id
)
SELECT uei, duns, pdl_company_id, company_name, normalized_domain, is_generic_domain,
       linkedin_slug, locality, region, country, industry, employee_size_range, year_founded
FROM ranked
WHERE rn = 1
ORDER BY uei
"""

# linkedin-resolve cycle (2026-07-17): slug-sorted resolution hop. Narrow
# projection; slug normalized to lowercase at build so probes are exact.
_SLUG_LOOKUP_SQL = """
CREATE TABLE pdl_slug_lookup AS
SELECT lower(linkedin_slug)  AS linkedin_slug,
       pdl_company_id,
       company_name,
       normalized_domain,
       is_generic_domain
FROM pdl_normalized_companies
WHERE linkedin_slug IS NOT NULL AND linkedin_slug <> ''
ORDER BY lower(linkedin_slug)
"""

# lender-book cycle (operator-directed 2026-07-17): lender_key-grain filing
# bridge. "A lender's full debtor book" previously required normalizing +
# LIKE-scanning secured_parties across all 7.7M filings (~4s, and any book
# over 50k filings exceeded the API row cap). This explodes secured_parties
# once at build, normalizes each party with the SAME expression that mints
# lender_key in sam_ucc_debtor_overlap.py (_LK — keep in lockstep), and
# clusters on lender_key so one lender's book is a pruned probe. Grain:
# 1/(lender_key, ucc_state, filing_id, debtor_key) — GROUP BY collapses
# spelling variants of the same lender on one filing; any_value() picks the
# raw name (all other columns are functionally dependent on the filing key).
# Blob columns (secured_parties, collateral_text) deliberately stay behind:
# both remain one pure-equality join away on (ucc_state, filing_id,
# debtor_key) against ucc_filings_all, and duplicating them here would grow
# the artifact by ~1GB for detail no first-read needs. Explode changes the
# row count -> aggregate parity.
_UCC_LENDER_FILINGS_SQL = """
CREATE TABLE ucc_lender_filings AS
WITH exploded AS (
    SELECT trim(p.party) AS lender_name,
           f.ucc_state, f.filing_id, f.debtor_key, f.uei, f.sos_entity_key,
           f.in_sam, f.is_org, f.debtor_name, f.debtor_name_norm,
           f.debtor_city, f.debtor_state, f.debtor_zip,
           f.first_filing_date, f.last_filing_date, f.lapse_date,
           f.filing_class, f.terminated, f.is_active_financing, f.is_lease,
           f.n_secured_parties
    FROM ucc_filings_all f,
         unnest(string_split(f.secured_parties, '; ')) AS p(party)
    WHERE f.secured_parties IS NOT NULL
)
SELECT trim(regexp_replace(regexp_replace(upper(lender_name), '[^A-Z0-9 ]', '', 'g'),
        ' (INC|LLC|LP|LLP|CORP|CORPORATION|CO|COMPANY|NA|NATIONAL ASSOCIATION|ASSOCIATION|LTD|THE)$',
        '', 'g'))                        AS lender_key,
       any_value(lender_name)            AS lender_name,
       ucc_state, filing_id, debtor_key,
       any_value(uei)                    AS uei,
       any_value(sos_entity_key)         AS sos_entity_key,
       any_value(in_sam)                 AS in_sam,
       any_value(is_org)                 AS is_org,
       any_value(debtor_name)            AS debtor_name,
       any_value(debtor_name_norm)       AS debtor_name_norm,
       any_value(debtor_city)            AS debtor_city,
       any_value(debtor_state)           AS debtor_state,
       any_value(debtor_zip)             AS debtor_zip,
       any_value(first_filing_date)      AS first_filing_date,
       any_value(last_filing_date)       AS last_filing_date,
       any_value(lapse_date)             AS lapse_date,
       any_value(filing_class)           AS filing_class,
       any_value(terminated)             AS terminated,
       any_value(is_active_financing)    AS is_active_financing,
       any_value(is_lease)               AS is_lease,
       any_value(n_secured_parties)      AS n_secured_parties
FROM exploded
WHERE length(lender_name) > 3
GROUP BY 1, ucc_state, filing_id, debtor_key
ORDER BY 1, uei
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
ALTER TABLE ops.query_sidecar_runs ADD COLUMN IF NOT EXISTS launch_mode text;
ALTER TABLE ops.query_sidecar_runs ADD COLUMN IF NOT EXISTS function_call_id text;
"""


_SPEC_STRUCTURAL_KEYS = {"ds", "tier", "sort", "cols", "dest", "extra_select",
                         "aggregate", "from_table", "after"}


def _preflight(wanted: set[str] | None = None) -> None:
    """Build-start gate, three assertions (directive 2026-07-23 §5.1 added 2+3):

    1. Every special-case manifest flag has a dispatch branch in _build_one.
       An unwired flag falls through to the generic CTAS silently — caught
       live 2026-07-09 (award_ordering_windows built as a 108M-row copy).
    2. Declared ordering: every `after` target is built EARLIER in MANIFEST,
       and every `from_table` source is declared in its spec's `after` list
       (the two must stay in lockstep — `after` is the machine-checked truth).
    3. Tier closure (only when `wanted` is passed): a partial-tier selection
       that drops a dependency fails HERE with a clear message instead of
       building garbage (e.g. Tier-D subout_rate reads Tier-A code_lanes).
    """
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

    errs: list[str] = []
    seen: set[str] = set()
    for spec in MANIFEST:
        dest = spec.get("dest", spec["ds"])
        ft = spec.get("from_table")
        if ft and ft not in spec.get("after", []):
            errs.append(f"{dest}: from_table {ft!r} missing from its after list")
        for dep in spec.get("after", []):
            if dep not in seen:
                errs.append(f"{dest}: after-target {dep!r} is not built earlier in MANIFEST")
        seen.add(dest)
    if errs:
        raise RuntimeError(f"manifest ordering violations: {errs}")

    if wanted is not None:
        sel = [s for s in MANIFEST if s["tier"] in wanted]
        dests = {s.get("dest", s["ds"]) for s in sel}
        closure = [
            f'{s.get("dest", s["ds"])} (tier {s["tier"]}) requires {dep!r}, '
            f'which the tier selection {sorted(wanted)} does not build'
            for s in sel for dep in s.get("after", []) if dep not in dests
        ]
        if closure:
            raise RuntimeError(
                "partial-tier selection drops dependencies — widen the tier set "
                f"or build full-manifest: {closure}")


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


# ── transient-retry layer (directive 2026-07-23 §5.4): one network blip must
# not cost a 30-minute build. Retries ONLY transient transport classes around
# the per-mart Lance open/scan and the artifact upload; SQL errors (binder,
# parser, constraint, parity) surface immediately.
_TRANSIENT_MARKERS = (
    "timeout", "timed out", "connection", "reset", "broken pipe", "eof",
    "temporarily", "unavailable", "throttl", "slow down", "429", "500", "502",
    "503", "504",
)
_SQL_ERROR_TYPES = {
    "ParserException", "BinderException", "CatalogException",
    "ConstraintException", "ConversionException", "InvalidInputException",
    "OutOfRangeException", "SyntaxException", "NotImplementedException",
}


def _is_transient(exc: Exception) -> bool:
    name = type(exc).__name__
    if name in _SQL_ERROR_TYPES or isinstance(exc, RuntimeError):
        return False  # parity/preflight/SQL failures are never retried
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    return any(m in str(exc).lower() for m in _TRANSIENT_MARKERS)


def _with_retry(label: str, fn, attempts: int = 3, base_sleep: float = 2.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — classified below
            if i == attempts - 1 or not _is_transient(exc):
                raise
            print(f"[retry] {label}: transient {type(exc).__name__}: "
                  f"{str(exc)[:200]} — retrying ({i + 2}/{attempts})")
            time.sleep(base_sleep * (2 ** i))


# Cleanup targets between per-mart retry attempts: the dest table plus every
# temp a special-case branch may have left behind mid-flight.
_RETRY_TEMP_TABLES = ("parent_attrs", "award_state_base", "subout_base",
                      "prime_lanes", "farmout_base")


def _build_one_with_retry(con, so: dict[str, str], spec: dict,
                          prev_counts: dict[str, int] | None) -> dict:
    dest = spec.get("dest", spec["ds"])

    def attempt():
        return _build_one(con, so, spec, prev_counts)

    def attempt_clean():
        for t in (dest, *_RETRY_TEMP_TABLES):
            con.execute(f'DROP TABLE IF EXISTS "{t}"')
        try:
            con.unregister("src")
        except Exception:  # noqa: BLE001 — nothing registered
            pass
        return attempt()

    try:
        return attempt()
    except Exception as exc:  # noqa: BLE001 — classified by _is_transient
        if not _is_transient(exc):
            raise
        print(f"[retry] {dest}: transient {type(exc).__name__}: "
              f"{str(exc)[:200]} — rebuilding mart (2/3)")
        time.sleep(2.0)
        try:
            return attempt_clean()
        except Exception as exc2:  # noqa: BLE001
            if not _is_transient(exc2):
                raise
            print(f"[retry] {dest}: transient {type(exc2).__name__}: "
                  f"{str(exc2)[:200]} — rebuilding mart (3/3)")
            time.sleep(4.0)
            return attempt_clean()


def _prev_artifact_counts() -> dict[str, int]:
    """Per-table duck_rows of the SERVING artifact, for the aggregate-parity
    floor (directive §5.2). Best-effort: unreachable serving -> {} -> the
    aggregate gate falls back to the legacy >0 check with a warning."""
    url = os.environ.get("QUERY_SIDECAR_URL")
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not (url and token):
        print("[parity] QUERY_SIDECAR_URL/TOKEN unset; aggregate floor falls back to >0")
        return {}
    try:
        import requests

        resp = requests.post(
            url.rstrip("/") + "/api/v1/sql",
            json={"sql": "SELECT table_name, duck_rows FROM _sidecar_manifest"},
            headers={"authorization": f"Bearer {token}"}, timeout=30)
        resp.raise_for_status()
        rows = resp.json()["rows"]
        print(f"[parity] previous-artifact counts loaded for {len(rows)} tables")
        return {r[0]: int(r[1]) for r in rows}
    except Exception as exc:  # noqa: BLE001 — floor is best-effort by design
        print(f"[parity] WARNING: previous-artifact counts unavailable "
              f"({type(exc).__name__}: {str(exc)[:200]}); aggregate floor falls back to >0")
        return {}


def _reap_artifacts(s3, keep_recent: int = 3) -> None:
    """Post-publish R2 reap (directive §4). Retain: the artifact LATEST.json
    points at, its immediate predecessor, and the keep_recent most recent
    beyond those; the smoke/ prefix's most recent is never touched. Deletes
    print a manifest. Best-effort — called inside try/except, never fatal."""
    latest_key = json.loads(
        s3.get_object(Bucket=R2_BUCKET, Key=f"{R2_PREFIX}/LATEST.json")["Body"].read()
    )["key"]
    objs, tok = [], None
    while True:
        kw = dict(Bucket=R2_BUCKET, Prefix=f"{R2_PREFIX}/")
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        objs.extend(r.get("Contents", []))
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    smoke_prefix = f"{R2_PREFIX}/smoke/"
    arts = sorted(o["Key"] for o in objs
                  if o["Key"].endswith(".duckdb") and not o["Key"].startswith(smoke_prefix))
    smoke = sorted(o["Key"] for o in objs if o["Key"].startswith(smoke_prefix))
    if latest_key not in arts:
        print(f"[reap] LATEST target {latest_key} not in listing; skipping reap")
        return
    li = arts.index(latest_key)
    retain = {latest_key, f"{R2_PREFIX}/LATEST.json"}
    if li > 0:
        retain.add(arts[li - 1])
    for k in reversed(arts):
        if len(retain) >= keep_recent + 3:  # latest + predecessor + keep_recent (+pointer)
            break
        retain.add(k)
    if smoke:
        retain.add(smoke[-1])
    # Reap ONLY .duckdb artifacts — the prefix also carries LATEST.json and
    # non-artifact records (e.g. bench/ run records) that are never candidates.
    doomed = [o for o in objs
              if o["Key"].endswith(".duckdb") and o["Key"] not in retain]
    if not doomed:
        print("[reap] nothing to reap")
        return
    total = sum(o["Size"] for o in doomed)
    for o in doomed:
        print(f"[reap] deleting {o['Key']} ({o['Size'] / 2**30:.2f} GiB)")
    for i in range(0, len(doomed), 1000):
        s3.delete_objects(Bucket=R2_BUCKET, Delete={
            "Objects": [{"Key": o["Key"]} for o in doomed[i:i + 1000]], "Quiet": True})
    print(f"[reap] reclaimed {total / 2**30:.2f} GiB ({len(doomed)} objects); "
          f"retained {sorted(retain)}")


_PERSON_CHANNELS_SQL = """
CREATE TABLE gtm_person_channels AS
SELECT p.uei, p.sam_person_id, p.display_name, p.first_name, p.last_name,
       p.best_title, p.is_govt_poc, p.is_ebiz_poc, p.n_sources,
       c.email, c.email_verification_status, c.phone, c.phone_status, c.phone_type,
       c.person_linkedin_url_norm, c.linkedin_match_score
FROM gtm_sam_people p
LEFT JOIN gtm_sam_person_contactability c ON c.sam_person_id = p.sam_person_id
ORDER BY p.uei, p.sam_person_id
"""


def _current_call_id() -> str | None:
    """The Modal function-call id of THIS build, for the ledger (autopsy aid)."""
    try:
        import modal
        return modal.current_function_call_id()
    except Exception:  # noqa: BLE001 — instrumentation never fails the build
        return None


def _record_run(run_id: int | None = None, **fields) -> int | None:
    """Ledger row, WARN-and-return on any failure — audit must not mask the build.

    Two-phase (watchdog rider, directive §2.2/§5): build() INSERTs a
    status='running' row at start (returning its id) and UPDATEs it to the
    terminal state in the finally. A build that dies without reaching the
    finally (OOM SIGKILL, preemption before restart, pre-try failure) leaves
    the 'running' row behind — exactly what the ops-watchdog's hung-build
    check (started_at set, completed_at NULL, >90 min) alerts on.
    """
    try:
        import psycopg

        dsn = os.environ.get("HQX_DB_URL_POOLED")
        if not dsn:
            print("[warn] HQX_DB_URL_POOLED unset; skipping ops ledger row")
            return None
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            if run_id is not None:
                sets = ", ".join(f"{c} = %s" for c in fields)
                cur.execute(
                    f"UPDATE ops.query_sidecar_runs SET {sets} WHERE id = %s",
                    (*fields.values(), run_id),
                )
            else:
                cols = ", ".join(fields)
                ph = ", ".join(["%s"] * len(fields))
                cur.execute(
                    f"INSERT INTO ops.query_sidecar_runs ({cols}) VALUES ({ph}) RETURNING id",
                    tuple(fields.values()),
                )
                run_id = cur.fetchone()[0]
            conn.commit()
            return run_id
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] ops ledger write failed (non-fatal): {exc}")
        return None


def _build_one(con, so: dict[str, str], spec: dict,
               prev_counts: dict[str, int] | None = None) -> dict:
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
        elif spec.get("entity_pricing_mix"):
            con.execute(_ENTITY_PRICING_MIX_SQL)
        elif spec.get("entity_fy_won"):
            con.execute(_ENTITY_FY_WON_SQL)
        elif spec.get("pricing_flow"):
            # windows anchored to the data's max(action_date) watermark (FPDS
            # publication lag — NOT current_date), computed here and inlined as
            # DATE literals so the mart SQL is a single pure GROUP BY.
            _b = con.execute(
                "SELECT (max(action_date) - INTERVAL '12 months')::DATE, "
                "(max(action_date) - INTERVAL '24 months')::DATE, "
                "(max(action_date) - INTERVAL '48 months')::DATE FROM txn_events_combo"
            ).fetchone()
            con.execute(_entity_pricing_flow_sql(
                _b[0].isoformat(), _b[1].isoformat(), _b[2].isoformat()))
        elif spec.get("construction_lane_months"):
            con.execute(_CONSTRUCTION_LANE_MONTHS_SQL)
        elif spec.get("entity_award_book"):
            con.execute(_ENTITY_AWARD_BOOK_SQL)
        elif spec.get("entity_firmographics"):
            con.execute(_ENTITY_FIRMOGRAPHICS_SQL)
        elif spec.get("ucc_lender_filings"):
            con.execute(_UCC_LENDER_FILINGS_SQL)
        elif spec.get("slug_lookup"):
            con.execute(_SLUG_LOOKUP_SQL)
        elif spec.get("person_channels"):
            con.execute(_PERSON_CHANNELS_SQL)
        elif spec.get("award_geo_state"):
            con.execute(_AWARD_GEO_STATE_SQL)
        elif spec.get("pop_place_fy"):
            con.execute(_POP_PLACE_FY_SQL)
        elif spec.get("pop_combo_fy"):
            con.execute(_POP_COMBO_FY_SQL)
        elif spec.get("pop_entity_fy"):
            con.execute(_POP_ENTITY_FY_SQL)
        elif spec.get("pop_award_fy"):
            con.execute(_POP_AWARD_FY_SQL)
        elif spec.get("award_geo_active"):
            con.execute(_AWARD_GEO_ACTIVE_SQL)
        elif spec.get("novation_events"):
            con.execute(_NOVATION_EVENTS_SQL)
        else:
            order = ", ".join(spec["sort"])
            extra = spec.get("extra_select")
            select = "SELECT *" + (f", {extra}" if extra else "")
            con.execute(f'CREATE TABLE "{dest}" AS {select} FROM "{src_table}" ORDER BY {order}')
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
    # Aggregate tables REDUCE (or explode) the source — their row count cannot
    # equal the source count. Gate (directive §5.2): count must be >= 50% of the
    # SAME table's count in the previous (serving) artifact; when that count is
    # unavailable (first build of a mart, serving unreachable) fall back to >0.
    aggregate = bool(spec.get("agency_vocab") or spec.get("aggregate"))
    if aggregate:
        prev = (prev_counts or {}).get(dest)
        if prev:
            parity_ok = duck_rows >= 0.5 * prev
            parity_note = f"floor 50% of prev {prev:,}"
        else:
            parity_ok = duck_rows > 0
            parity_note = "floor >0 (no prev count)"
    else:
        parity_ok = duck_rows == lance_rows
        parity_note = "exact"
    row = {
        "table": dest, "dataset": name, "tier": spec["tier"],
        "sort": ",".join(spec.get("sort", [])) or None,
        "lance_version": pinned_version, "lance_rows": lance_rows,
        "duck_rows": duck_rows,
        "parity_ok": parity_ok,
        "seconds": elapsed,
    }
    print(f"[mart] {dest}: {duck_rows:,} rows in {elapsed}s "
          f"(lance v{pinned_version}={lance_rows:,}) "
          f"parity={'OK' if parity_ok else 'MISMATCH'} [{parity_note}]")
    return row


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"),
             modal.Secret.from_name("query-sidecar")],  # refresh-hook bearer (Phase 5)
    memory=131_072,          # 128 GiB — the >100M-row sort precedent (cms_medicare giant)
    cpu=8.0,
    ephemeral_disk=524_288,  # 512 GiB local NVMe: DuckDB spill + the output file
    max_containers=1,        # "fire exactly ONE build" is structural, not aspirational
    timeout=60 * 60 * 2,     # ~3x the trend-projected 60-min worst case; re-raise as duration grows
)
def build(tiers: str = "A,B,C,D", publish: bool = True, smoke: bool = False,
          trigger_callback_url: str | None = None,
          launch_mode: str | None = None) -> dict:
    """Build the query-sidecar .duckdb for the requested tiers; publish blue-green to R2."""
    import duckdb

    started_at = dt.datetime.now(dt.timezone.utc)
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    wanted = {t.strip().upper() for t in tiers.split(",") if t.strip()}
    _preflight(wanted)  # flag dispatch + declared ordering + tier closure
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
    # Start-row: makes a silent death (OOM SIGKILL, preemption, pre-try crash)
    # visible as a stuck 'running' row — the ops-watchdog hung-build check.
    run_id = _record_run(
        tiers=",".join(sorted(wanted)), status="running", started_at=started_at,
        launch_mode=launch_mode, function_call_id=_current_call_id(),
    )
    prev_counts = _prev_artifact_counts()  # aggregate-parity floor (best-effort)
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
                parity.append(_build_one_with_retry(con, so, spec, prev_counts))

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
            _with_retry("upload", lambda: s3.upload_file(db_path, R2_BUCKET, r2_key))
            print(f"[publish] s3://{R2_BUCKET}/{r2_key}")
            if not smoke:
                pointer = {"key": r2_key, "built_at": started_at.isoformat(),
                           "file_bytes": file_bytes, "tiers": sorted(wanted),
                           "tables": [p["table"] for p in parity]}
                _with_retry("pointer-swap", lambda: s3.put_object(
                    Bucket=R2_BUCKET, Key=f"{R2_PREFIX}/LATEST.json",
                    Body=json.dumps(pointer, indent=1).encode(),
                    ContentType="application/json"))
                latest_updated = True
                print(f"[publish] LATEST.json -> {r2_key}")
                _notify_refresh()
                try:
                    _reap_artifacts(s3)  # §4: best-effort, like _notify_refresh
                except Exception as exc:  # noqa: BLE001
                    print(f"[reap] non-fatal: {exc}")
    except Exception as exc:  # noqa: BLE001
        status, error_message = "error", str(exc)[:2000]
        raise
    finally:
        _record_run(
            run_id=run_id,
            tiers=",".join(sorted(wanted)), marts=len(parity),
            rows_total=sum(p["duck_rows"] for p in parity),
            file_bytes=file_bytes, r2_key=r2_key, latest_updated=latest_updated,
            status=status, error_message=error_message,
            started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc),
            launch_mode=launch_mode, function_call_id=_current_call_id(),
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
    # Spawns on the DEPLOYED app (ASYNC input, no client tether) and returns.
    # build.remote() here would issue a SYNC input the server cancels ~90 s after
    # client loss — the launch mode behind 8 "Query interrupted" ledger failures.
    fn = modal.Function.from_name("query-sidecar", "build")
    fc = fn.spawn(tiers=tiers, publish=True, smoke=False,
                  trigger_callback_url=None, launch_mode="spawn-deployed")
    print(f"FUNCTION_CALL_ID: {fc.object_id}")
    print("Follow: modal app logs query-sidecar   |   result: "
          f"modal.FunctionCall.from_id('{fc.object_id}').get(timeout=0)")


@app.local_entrypoint()
def smoke():
    result = build.remote(tiers="A", publish=True, smoke=True, trigger_callback_url=None)
    print(json.dumps(result, indent=1, default=str))
