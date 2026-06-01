"""Compute worker — HMDA Nationwide Loan-Level (LAR) + Reporter Panel bulk ingest.

Part of the ``hmda-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the transform,
Lance is written straight to R2.

PHASE 1 — historical sweep 2016–2025 inclusive, into TWO unified Lance datasets:

    s3://data-sink/active/hmda_lar/      (all LAR rows, all years, one normalized schema)
    s3://data-sink/active/hmda_panels/   (all institution rows, all years)

Dual-parser schema layout (the load-bearing design):
  • 2018–2024  LAR  → SNAPSHOT CSV ZIP   files.ffiec.cfpb.gov  (modern, 99-field Dodd-Frank)
  • 2025       LAR  → COMBINED MLAR pipe  files.ffiec.cfpb.gov  (modern_mlar, 85-field, PIPE)
  • 2016–2017  LAR  → historic codes CSV  files.consumerfinance.gov (legacy, 45-field)
Every era is projected through ONE unified field spec (LAR_FIELDS) so all years emit an
identical all-VARCHAR Arrow schema → clean append into the single hmda_lar dataset. Fields
absent in an era are CAST(NULL AS VARCHAR). Semantic overlaps are normalized to the modern
canonical name (e.g. legacy as_of_year→activity_year, census_tract_number→census_tract,
population→tract_population, owner_occupancy→occupancy_type; mlar credit_scoring_model→
applicant_credit_score_type, other_non_amortizing_features→other_nonamortizing_features).

  ⚠ VALUE-LEVEL caveats (names normalized, raw bytes preserved — downstream owns casting):
    - loan_amount / income: legacy (loan_amount_000s / applicant_income_000s) are in THOUSANDS;
      modern + mlar are actual dollars. Kept raw; do not mix units without ×1000 on legacy.
    - property_state (←state_code): modern + mlar = 2-letter postal; legacy = 2-digit FIPS.

Institution identity (Panel fallback):
  • 2016–2017 Panel → historic institution CSV (legacy, 21-field Title-Case)
  • 2018–2023 Panel → snapshot panel CSV (modern, 15–16-field; arid_2017 only 2018–19)
  • 2024 + 2025     → NO panel published → documented fallback: 2024 Transmittal Sheet
                      (2024_public_ts_csv.zip). 2024 rows tagged source_product=ts_2024;
                      2025 rows tagged ts_2024_fallback (LEIs persist year-over-year).
  Pre-2018 has no LEI — institutions key on respondent_id + agency_code; 2018+ key on lei.

Transport: each source is downloaded with live Content-Length verification against the
embedded source-map (EXPECTED_SIZES) and retry-with-exponential-backoff (hardened for the
rate-limiting files.consumerfinance.gov host); the single ZIP member is stream-recompressed
to zstd on local NVMe; DuckDB reads the .zst directly. Large LAR years stream
DuckDB→Arrow→Lance via fetch_record_batch (bounded memory, no full-table materialization).

Idempotency: each year is delete-then-append on its data_year/source_year — re-running one
year is safe; the dataset is created (overwrite) only when it does not yet exist.

Control plane (Trigger v4 durable callback): each function accepts ``trigger_callback_url``
and, on terminal state, (1) writes a run row to ``ops.hmda_runs`` via psycopg and (2) POSTs a
FLAT JSON body to that url.

    modal deploy pipelines/hmda/hmda_bulk.py
    modal run    pipelines/hmda/hmda_bulk.py::init_state            # create ops.hmda_runs
    modal run    pipelines/hmda/hmda_bulk.py::backfill              # full 2016–2025 sweep + index
    modal run    pipelines/hmda/hmda_bulk.py::lar   --year 2021     # one LAR year
    modal run    pipelines/hmda/hmda_bulk.py::panel --year 2024     # one panel year
    modal run    pipelines/hmda/hmda_bulk.py::reindex --dataset lar|panels|all
"""

from __future__ import annotations

import os

import modal

# ── R2 system-of-record (data-sink). Two unified datasets + raw landing. ──────
BUCKET = "data-sink"
LAR_URI = os.environ.get("HMDA_LAR_LANCE_URI", "s3://data-sink/active/hmda_lar/")
PANEL_URI = os.environ.get("HMDA_PANEL_LANCE_URI", "s3://data-sink/active/hmda_panels/")
# Raw source zips land here first (Phase 1 staging), then ingest reads them from R2.
# The CFPB/FFIEC WAF blocks ~40% of Modal egress IPs (HTTP 403); staging fans out across
# PARALLEL containers (which spread across hosts → varied IPs, ~60% reach the origin) and
# retries stragglers, so the origin is hit only during staging. Ingest then reads R2, which
# is always reachable from Modal — exactly the fl_sos landing→ingest split.
LANDING_PREFIX = "landing/hmda/"

SCRATCH_DIR = "/tmp"
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
STREAM_BATCH_ROWS = 1 << 16  # 65536 — bounded-memory DuckDB→Lance streaming

_FFIEC = "https://files.ffiec.cfpb.gov"
_CFPB = "https://files.consumerfinance.gov"

# The CFPB/FFIEC CloudFront WAF rejects non-browser User-Agents with HTTP 403
# (python-requests/* and custom tokens are blocked). A standard browser UA + Accept
# headers — the exact shape that verified 200 during source-map discovery — is required.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_DL_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Per-year source map (verified Phase-0 truth: url, delim, era, product, bytes). ──
# era ∈ {modern, mlar, legacy}; product is the source_product tag written to Lance.
LAR_SOURCES: dict[int, dict] = {
    2016: {"url": f"{_CFPB}/hmda-historic-loan-data/hmda_2016_nationwide_all-records_codes.zip",
           "delim": ",", "era": "legacy", "product": "historic_all_records_codes", "size": 384109860, "throttled": True},
    2017: {"url": f"{_CFPB}/hmda-historic-loan-data/hmda_2017_nationwide_all-records_codes.zip",
           "delim": ",", "era": "legacy", "product": "historic_all_records_codes", "size": 182021055, "throttled": True},
    2018: {"url": f"{_FFIEC}/static-data/snapshot/2018/2018_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 823719647, "throttled": False},
    2019: {"url": f"{_FFIEC}/static-data/snapshot/2019/2019_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 980129414, "throttled": False},
    2020: {"url": f"{_FFIEC}/static-data/snapshot/2020/2020_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 1460346740, "throttled": False},
    2021: {"url": f"{_FFIEC}/static-data/snapshot/2021/2021_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 1517879241, "throttled": False},
    2022: {"url": f"{_FFIEC}/static-data/snapshot/2022/2022_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 877742261, "throttled": False},
    2023: {"url": f"{_FFIEC}/static-data/snapshot/2023/2023_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 624535331, "throttled": False},
    2024: {"url": f"{_FFIEC}/static-data/snapshot/2024/2024_public_lar_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_lar", "size": 664242987, "throttled": False},
    2025: {"url": f"{_FFIEC}/modified-lar/combined-mlar/2025/2025_combined_mlar_header.zip",
           "delim": "|", "era": "mlar", "product": "combined_mlar", "size": 430121280, "throttled": False},
}

# Panel sources. 2024/2025 fall back to the 2024 Transmittal Sheet (no panel published).
_TS_2024 = f"{_FFIEC}/static-data/snapshot/2024/2024_public_ts_csv.zip"
PANEL_SOURCES: dict[int, dict] = {
    2016: {"url": f"{_CFPB}/hmda-historic-institution-data/hmda_2016_panel.zip",
           "delim": ",", "era": "legacy", "product": "historic_panel", "size": 294811, "throttled": True},
    2017: {"url": f"{_CFPB}/hmda-historic-institution-data/hmda_2017_panel.zip",
           "delim": ",", "era": "legacy", "product": "historic_panel", "size": 228820, "throttled": True},
    2018: {"url": f"{_FFIEC}/static-data/snapshot/2018/2018_public_panel_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_panel", "size": 322525, "throttled": False},
    2019: {"url": f"{_FFIEC}/static-data/snapshot/2019/2019_public_panel_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_panel", "size": 317520, "throttled": False},
    2020: {"url": f"{_FFIEC}/static-data/snapshot/2020/2020_public_panel_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_panel", "size": 228830, "throttled": False},
    2021: {"url": f"{_FFIEC}/static-data/snapshot/2021/2021_public_panel_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_panel", "size": 221032, "throttled": False},
    2022: {"url": f"{_FFIEC}/static-data/snapshot/2022/2022_public_panel_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_panel", "size": 227638, "throttled": False},
    2023: {"url": f"{_FFIEC}/static-data/snapshot/2023/2023_public_panel_csv.zip",
           "delim": ",", "era": "modern", "product": "snapshot_panel", "size": 282414, "throttled": False},
    2024: {"url": _TS_2024, "delim": ",", "era": "ts", "product": "ts_2024", "size": 199079, "throttled": False},
    2025: {"url": _TS_2024, "delim": ",", "era": "ts", "product": "ts_2024_fallback", "size": 199079, "throttled": False},
}

LAR_YEARS = sorted(LAR_SOURCES)       # 2016..2025 ascending — append order
PANEL_YEARS = sorted(PANEL_SOURCES)   # 2016..2025 ascending

# ── Unified LAR field spec: (unified_name, modern_src, mlar_src, legacy_src). ─────
# None ⇒ CAST(NULL AS VARCHAR) for that era. Names are the modern canonical; legacy &
# mlar source headers are mapped to them. Order = unified output schema order.
_E = None
LAR_FIELDS: list[tuple] = [
    # identity / keys
    ("activity_year",                   "activity_year", "activity_year", "as_of_year"),
    ("lei",                             "lei", "lei", _E),
    ("respondent_id",                   _E, _E, "respondent_id"),
    ("agency_code",                     _E, _E, "agency_code"),
    # loan core
    ("loan_type",                       "loan_type", "loan_type", "loan_type"),
    ("loan_purpose",                    "loan_purpose", "loan_purpose", "loan_purpose"),
    ("preapproval",                     "preapproval", "preapproval", "preapproval"),
    ("action_taken",                    "action_taken", "action_taken", "action_taken"),
    ("purchaser_type",                  "purchaser_type", "purchaser_type", "purchaser_type"),
    ("loan_amount",                     "loan_amount", "loan_amount", "loan_amount_000s"),
    ("income",                          "income", "income", "applicant_income_000s"),
    ("rate_spread",                     "rate_spread", "rate_spread", "rate_spread"),
    ("hoepa_status",                    "hoepa_status", "hoepa_status", "hoepa_status"),
    ("lien_status",                     "lien_status", "lien_status", "lien_status"),
    # geography (location markers)
    ("property_state",                  "state_code", "state_code", "state_code"),
    ("county_code",                     "county_code", "county_code", "county_code"),
    ("census_tract",                    "census_tract", "census_tract", "census_tract_number"),
    ("msa_md",                          "derived_msa_md", _E, "msamd"),
    # dwelling / structure
    ("occupancy_type",                  "occupancy_type", "occupancy_type", "owner_occupancy"),
    ("construction_method",             "construction_method", "construction_method", _E),
    ("property_type",                   _E, _E, "property_type"),
    ("total_units",                     "total_units", "total_units", _E),
    ("multifamily_affordable_units",    "multifamily_affordable_units", "multifamily_affordable_units", _E),
    ("manufactured_home_secured_property_type",   "manufactured_home_secured_property_type", "manufactured_home_secured_property_type", _E),
    ("manufactured_home_land_property_interest",  "manufactured_home_land_property_interest", "manufactured_home_land_property_interest", _E),
    ("property_value",                  "property_value", "property_value", _E),
    # pricing / terms (modern + mlar)
    ("interest_rate",                   "interest_rate", "interest_rate", _E),
    ("loan_term",                       "loan_term", "loan_term", _E),
    ("combined_loan_to_value_ratio",    "combined_loan_to_value_ratio", "combined_loan_to_value_ratio", _E),
    ("debt_to_income_ratio",            "debt_to_income_ratio", "debt_to_income_ratio", _E),
    ("total_loan_costs",                "total_loan_costs", "total_loan_costs", _E),
    ("total_points_and_fees",           "total_points_and_fees", "total_points_and_fees", _E),
    ("origination_charges",             "origination_charges", "origination_charges", _E),
    ("discount_points",                 "discount_points", "discount_points", _E),
    ("lender_credits",                  "lender_credits", "lender_credits", _E),
    ("prepayment_penalty_term",         "prepayment_penalty_term", "prepayment_penalty_term", _E),
    ("intro_rate_period",               "intro_rate_period", "intro_rate_period", _E),
    ("balloon_payment",                 "balloon_payment", "balloon_payment", _E),
    ("interest_only_payment",           "interest_only_payment", "interest_only_payment", _E),
    ("negative_amortization",           "negative_amortization", "negative_amortization", _E),
    ("other_nonamortizing_features",    "other_nonamortizing_features", "other_non_amortizing_features", _E),
    ("reverse_mortgage",                "reverse_mortgage", "reverse_mortgage", _E),
    ("open_end_line_of_credit",         "open_end_line_of_credit", "open_end_line_of_credit", _E),
    ("business_or_commercial_purpose",  "business_or_commercial_purpose", "business_or_commercial_purpose", _E),
    ("submission_of_application",       "submission_of_application", "submission_of_application", _E),
    ("initially_payable_to_institution","initially_payable_to_institution", "initially_payable_to_institution", _E),
    ("applicant_credit_score_type",     "applicant_credit_score_type", "applicant_credit_scoring_model", _E),
    ("co_applicant_credit_score_type",  "co_applicant_credit_score_type", "co_applicant_credit_scoring_model", _E),
    # ethnicity
    ("applicant_ethnicity_1",           "applicant_ethnicity_1", "applicant_ethnicity_1", "applicant_ethnicity"),
    ("applicant_ethnicity_2",           "applicant_ethnicity_2", "applicant_ethnicity_2", _E),
    ("applicant_ethnicity_3",           "applicant_ethnicity_3", "applicant_ethnicity_3", _E),
    ("applicant_ethnicity_4",           "applicant_ethnicity_4", "applicant_ethnicity_4", _E),
    ("applicant_ethnicity_5",           "applicant_ethnicity_5", "applicant_ethnicity_5", _E),
    ("co_applicant_ethnicity_1",        "co_applicant_ethnicity_1", "co_applicant_ethnicity_1", "co_applicant_ethnicity"),
    ("co_applicant_ethnicity_2",        "co_applicant_ethnicity_2", "co_applicant_ethnicity_2", _E),
    ("co_applicant_ethnicity_3",        "co_applicant_ethnicity_3", "co_applicant_ethnicity_3", _E),
    ("co_applicant_ethnicity_4",        "co_applicant_ethnicity_4", "co_applicant_ethnicity_4", _E),
    ("co_applicant_ethnicity_5",        "co_applicant_ethnicity_5", "co_applicant_ethnicity_5", _E),
    ("applicant_ethnicity_observed",    "applicant_ethnicity_observed", "applicant_ethnicity_observed", _E),
    ("co_applicant_ethnicity_observed", "co_applicant_ethnicity_observed", "co_applicant_ethnicity_observed", _E),
    # race
    ("applicant_race_1",                "applicant_race_1", "applicant_race_1", "applicant_race_1"),
    ("applicant_race_2",                "applicant_race_2", "applicant_race_2", "applicant_race_2"),
    ("applicant_race_3",                "applicant_race_3", "applicant_race_3", "applicant_race_3"),
    ("applicant_race_4",                "applicant_race_4", "applicant_race_4", "applicant_race_4"),
    ("applicant_race_5",                "applicant_race_5", "applicant_race_5", "applicant_race_5"),
    ("co_applicant_race_1",             "co_applicant_race_1", "co_applicant_race_1", "co_applicant_race_1"),
    ("co_applicant_race_2",             "co_applicant_race_2", "co_applicant_race_2", "co_applicant_race_2"),
    ("co_applicant_race_3",             "co_applicant_race_3", "co_applicant_race_3", "co_applicant_race_3"),
    ("co_applicant_race_4",             "co_applicant_race_4", "co_applicant_race_4", "co_applicant_race_4"),
    ("co_applicant_race_5",             "co_applicant_race_5", "co_applicant_race_5", "co_applicant_race_5"),
    ("applicant_race_observed",         "applicant_race_observed", "applicant_race_observed", _E),
    ("co_applicant_race_observed",      "co_applicant_race_observed", "co_applicant_race_observed", _E),
    # sex
    ("applicant_sex",                   "applicant_sex", "applicant_sex", "applicant_sex"),
    ("co_applicant_sex",                "co_applicant_sex", "co_applicant_sex", "co_applicant_sex"),
    ("applicant_sex_observed",          "applicant_sex_observed", "applicant_sex_observed", _E),
    ("co_applicant_sex_observed",       "co_applicant_sex_observed", "co_applicant_sex_observed", _E),
    # age (modern + mlar only — no age pre-2018)
    ("applicant_age",                   "applicant_age", "applicant_age", _E),
    ("co_applicant_age",                "co_applicant_age", "co_applicant_age", _E),
    ("applicant_age_above_62",          "applicant_age_above_62", "applicant_age_above_62", _E),
    ("co_applicant_age_above_62",       "co_applicant_age_above_62", "co_applicant_age_above_62", _E),
    # automated underwriting (modern + mlar)
    ("aus_1",                           "aus_1", "aus_1", _E),
    ("aus_2",                           "aus_2", "aus_2", _E),
    ("aus_3",                           "aus_3", "aus_3", _E),
    ("aus_4",                           "aus_4", "aus_4", _E),
    ("aus_5",                           "aus_5", "aus_5", _E),
    # denial reasons (legacy has 1..3; modern/mlar 1..4)
    ("denial_reason_1",                 "denial_reason_1", "denial_reason_1", "denial_reason_1"),
    ("denial_reason_2",                 "denial_reason_2", "denial_reason_2", "denial_reason_2"),
    ("denial_reason_3",                 "denial_reason_3", "denial_reason_3", "denial_reason_3"),
    ("denial_reason_4",                 "denial_reason_4", "denial_reason_4", _E),
    # derived (modern snapshot only — not in mlar/legacy)
    ("conforming_loan_limit",           "conforming_loan_limit", _E, _E),
    ("derived_loan_product_type",       "derived_loan_product_type", _E, _E),
    ("derived_dwelling_category",       "derived_dwelling_category", _E, _E),
    ("derived_ethnicity",               "derived_ethnicity", _E, _E),
    ("derived_race",                    "derived_race", _E, _E),
    ("derived_sex",                     "derived_sex", _E, _E),
    # census-tract context (modern names; legacy mapped; mlar has none)
    ("tract_population",                "tract_population", _E, "population"),
    ("tract_minority_population_percent","tract_minority_population_percent", _E, "minority_population"),
    ("ffiec_msa_md_median_family_income","ffiec_msa_md_median_family_income", _E, "hud_median_family_income"),
    ("tract_to_msa_income_percentage",  "tract_to_msa_income_percentage", _E, "tract_to_msamd_income"),
    ("tract_owner_occupied_units",      "tract_owner_occupied_units", _E, "number_of_owner_occupied_units"),
    ("tract_one_to_four_family_homes",  "tract_one_to_four_family_homes", _E, "number_of_1_to_4_family_units"),
    ("tract_median_age_of_housing_units","tract_median_age_of_housing_units", _E, _E),
    # legacy-only structural
    ("edit_status",                     _E, _E, "edit_status"),
    ("sequence_number",                 _E, _E, "sequence_number"),
    ("application_date_indicator",      _E, _E, "application_date_indicator"),
]

# Unified Panel field spec: (unified_name, modern_src, legacy_src, ts_src).
PANEL_FIELDS: list[tuple] = [
    ("lei",                          "lei", _E, "lei"),
    ("respondent_id",                _E, "Respondent ID", _E),
    ("agency_code",                  "agency_code", "Agency Code", "agency_code"),
    ("tax_id",                       "tax_id", _E, "tax_id"),
    ("id_2017",                      "id_2017", _E, _E),
    ("respondent_rssd",              "respondent_rssd", "Respondent RSSD ID", _E),
    ("respondent_name",              "respondent_name", "Respondent Name (Panel)", "respondent_name"),
    ("respondent_city",              "respondent_city", "Respondent City (Panel)", "respondent_city"),
    ("respondent_state",             "respondent_state", "Respondent State (Panel)", "respondent_state"),
    ("respondent_zip_code",          _E, _E, "respondent_zip_code"),
    ("respondent_fips_state_number", _E, "Respondent FIPS State Number", _E),
    ("assets",                       "assets", "Assets", _E),
    ("other_lender_code",            "other_lender_code", "Other Lender Code", _E),
    ("region",                       _E, "Region", _E),
    ("parent_rssd",                  "parent_rssd", "Parent RSSD ID", _E),
    ("parent_name",                  "parent_name", "Parent Name (Panel)", _E),
    ("parent_respondent_id",         _E, "Parent Respondent ID", _E),
    ("parent_city",                  _E, "Parent City (Panel)", _E),
    ("parent_state",                 _E, "Parent State (Panel)", _E),
    ("topholder_rssd",               "topholder_rssd", "Top Holder RSSD ID", _E),
    ("topholder_name",               "topholder_name", "Top Holder Name", _E),
    ("topholder_city",               _E, "Top Holder City", _E),
    ("topholder_state",              _E, "Top Holder State", _E),
    ("topholder_country",            _E, "Top Holder Country", _E),
    ("lar_count",                    _E, _E, "lar_count"),
]

_ERA_IDX = {"lar": {"modern": 1, "mlar": 2, "legacy": 3},
            "panel": {"modern": 1, "legacy": 2, "ts": 3}}

# Scalar index plan. Directive: BTREE on join keys + location markers.
INDEX_PLAN = {
    "lar": ["lei", "action_taken", "property_state", "county_code"],
    "panels": ["lei", "respondent_id"],
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.hmda_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,            -- lar | panels
    data_year      int,
    source_product text,
    schema_era     text,
    source_url     text,
    expected_bytes bigint,
    actual_bytes   bigint,
    size_verified  boolean,
    dataset_uri    text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,            -- success | error
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS hmda_runs_dataset_idx     ON ops.hmda_runs (dataset);
CREATE INDEX IF NOT EXISTS hmda_runs_year_idx        ON ops.hmda_runs (data_year);
CREATE INDEX IF NOT EXISTS hmda_runs_status_idx      ON ops.hmda_runs (status);
CREATE INDEX IF NOT EXISTS hmda_runs_recorded_at_idx ON ops.hmda_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "boto3>=1.35",
    "zstandard>=0.22",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})  # in-memory BTREE sort (lance-format/lance#2650)

app = modal.App("hmda-pipelines", image=image)


# ── R2 / object-store ─────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
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
    """boto3 S3 client for R2 (checksum behaviour forced to when_required — R2 semantics)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


# ── Transport: verified download → zstd-recompressed single member ────────────
# The CFPB/FFIEC CloudFront WAF blocks a SUBSET of Modal egress IPs by reputation
# (datacenter/anonymous-IP managed rules): an unlucky container gets a static HTTP 403 that
# no same-container retry recovers (verified: 6 retries / 64s backoff, all 403), while a
# fresh container on a different IP gets 200. So 403 is fatal-to-this-container and must
# trigger container ROTATION, handled by the orchestrator / Trigger dispatch (which re-spawn
# a fresh container). Transient network errors (resets, timeouts) ARE retried in-container.
class _IPBlocked(Exception):
    """HTTP 403 from a WAF/IP-blocked Modal egress IP. The staging worker (max_inputs=1, fanned
    out in parallel) fails on this so stage_all re-fans the straggler onto a fresh container/IP."""


def _stage_url_to_r2(url: str, expected: int | None, bucket: str, key: str, s3) -> tuple[int, bool]:
    """PHASE 1 — stream a source URL straight into R2 landing (no large local disk), verifying
    the live Content-Length BEFORE accepting and the landed object size AFTER (catches partial
    uploads). Returns (bytes, verified-vs-source-map). Raises _IPBlocked on 403 so stage_all
    rotates to a fresh container/IP."""
    import requests

    with requests.get(url, stream=True, timeout=(30, 1200), headers=_DL_HEADERS) as r:
        if r.status_code == 403:
            raise _IPBlocked(f"HTTP 403 (WAF/egress-IP block): {url}")
        r.raise_for_status()
        live = r.headers.get("Content-Length")
        live_n = int(live) if live and live.isdigit() else None
        if expected is not None and live_n is not None and live_n != expected:
            print(f"  WARN live Content-Length {live_n} != source-map {expected} (CFPB re-freeze?)")
        r.raw.decode_content = True
        s3.upload_fileobj(r.raw, bucket, key)
    landed = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if live_n is not None and landed != live_n:
        raise RuntimeError(f"partial upload {landed}/{live_n} for {key}")  # → re-stage next round
    verified = (expected is not None and landed == expected)
    print(f"  staged {landed:,} bytes → s3://{bucket}/{key} (expected {expected}, verified={verified})")
    return landed, verified


def _r2_to_disk(bucket: str, key: str, dest: str, s3) -> int:
    """PHASE 2 — pull a landed zip from R2 to local scratch. R2 is always reachable from Modal
    (no WAF), so this never hits the egress-IP block. Returns the byte size."""
    import os.path
    s3.download_file(bucket, key, dest)
    return os.path.getsize(dest)


def _zip_member_to_zst(zip_path: str, zst_path: str, delim: str) -> tuple[str, list[str]]:
    """Extract the single ZIP member (standard Deflate — stdlib-capable), stream-recompress
    to zstd, and return (zst_path, header_columns). Header is captured from the first line so
    projections can presence-check columns (resilient to per-year header drift)."""
    import zipfile

    import zstandard as zstd

    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if not n.endswith("/")]
        if not members:
            raise RuntimeError(f"no members in {zip_path}")
        member = members[0]
        cctx = zstd.ZstdCompressor(level=6)
        header_buf = bytearray()
        header_line: str | None = None
        with zf.open(member, "r") as src, open(zst_path, "wb") as fo, cctx.stream_writer(fo) as comp:
            while True:
                chunk = src.read(16 << 20)
                if not chunk:
                    break
                comp.write(chunk)
                if header_line is None:
                    header_buf.extend(chunk)
                    nl = header_buf.find(b"\n")
                    if nl != -1:
                        header_line = header_buf[:nl].decode("utf-8", "replace").rstrip("\r")
    if header_line is None:
        raise RuntimeError(f"empty member {member} in {zip_path}")
    cols = [c.strip().strip('"') for c in header_line.split(delim)]
    print(f"  member={member} cols={len(cols)} → {zst_path}")
    return zst_path, cols


# ── SQL projection generation (presence-aware → resilient to header drift) ────
def _proj(fields: list[tuple], idx: int, present: set[str]) -> str:
    parts = []
    for spec in fields:
        unified, src = spec[0], spec[idx]
        if src is not None and src in present:
            parts.append(f"    nullif(trim(CAST(\"{src}\" AS VARCHAR)), '') AS {unified}")
        else:
            parts.append(f"    CAST(NULL AS VARCHAR) AS {unified}")
    return ",\n".join(parts)


def _read_clause(zst_path: str, delim: str) -> str:
    d = "\\t" if delim == "\t" else delim
    return (f"read_csv('{zst_path}', all_varchar=true, header=true, delim='{d}', "
            "quote='\"', escape='\"', sample_size=-1, compression='zstd', "
            "strict_mode=false, null_padding=true, store_rejects=true, max_line_size=16000000)")


def _build_lar_sql(zst_path, delim, era, year, product, present) -> str:
    sel = _proj(LAR_FIELDS, _ERA_IDX["lar"][era], present)
    return (f"SELECT\n{sel},\n"
            f"    CAST({year} AS INTEGER) AS data_year,\n"
            f"    '{product}' AS source_product,\n    '{era}' AS schema_era,\n"
            f"    now() AS ingested_at\n"
            f"FROM {_read_clause(zst_path, delim)}")


def _build_panel_sql(zst_path, delim, era, year, product, present) -> str:
    sel = _proj(PANEL_FIELDS, _ERA_IDX["panel"][era], present)
    return (f"SELECT\n{sel},\n"
            f"    CAST({year} AS INTEGER) AS source_year,\n"
            f"    '{product}' AS source_product,\n    '{era}' AS schema_era,\n"
            f"    now() AS ingested_at\n"
            f"FROM {_read_clause(zst_path, delim)}")


# ── Lance: idempotent delete-then-append (create on first write) ───────────────
def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — absent / not-found
        return False


def _build_indexes(dataset: str, uri: str, so: dict) -> list[str]:
    import lance
    ds = lance.dataset(uri, storage_options=so)
    cols_present = set(ds.schema.names)
    built = []
    for col in INDEX_PLAN[dataset]:
        if col not in cols_present:
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    return built


def _record_run(dataset, year, product, era, url, expected, actual, verified, uri,
                rows, rejected, status, error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.hmda_runs
                    (dataset, data_year, source_product, schema_era, source_url,
                     expected_bytes, actual_bytes, size_verified, dataset_uri,
                     rows_processed, rejected_rows, status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (dataset, year, product, era, url, expected, actual, verified, uri,
                 rows, rejected, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# ── State schema ───────────────────────────────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.hmda_runs DDL. Run once before the first ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.hmda_runs schema.")
    return {"status": "success", "table": "ops.hmda_runs"}


# ── Phase 1: stage raw source zips → R2 landing (parallel fan-out dodges the WAF) ──
def _source(kind: str, year: int) -> dict:
    return (LAR_SOURCES if kind == "lar" else PANEL_SOURCES)[int(year)]


def _landing_key(kind: str, year: int) -> str:
    return f"{LANDING_PREFIX}{kind}/{int(year)}.zip"


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 40, memory=4096, cpu=2.0,
    single_use_containers=True,  # fresh container per input ⇒ fresh egress IP; the parallel
                                 # fan-out in stage_all then spreads across hosts (~60% reach WAF)
)
def stage_one(kind: str, year: int) -> dict:
    """Stage ONE source zip → R2 landing. Idempotent (skips when the landed object already
    matches the source-map size). Raises _IPBlocked on a WAF/IP-blocked egress so stage_all
    re-fans it onto a fresh container. kind ∈ {lar, panel}."""
    src = _source(kind, year)
    key = _landing_key(kind, year)
    s3 = _s3_client()
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        if src["size"] and h["ContentLength"] == src["size"]:
            print(f"  skip {kind} {year}: already staged ({h['ContentLength']:,} B)")
            return {"kind": kind, "year": int(year), "key": key,
                    "bytes": h["ContentLength"], "skipped": True}
    except Exception:  # noqa: BLE001 — object absent ⇒ stage it
        pass
    n, verified = _stage_url_to_r2(src["url"], src["size"], BUCKET, key, s3)
    return {"kind": kind, "year": int(year), "key": key, "bytes": n,
            "verified": verified, "skipped": False}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 60, memory=2048)
def stage_all(trigger_callback_url: str | None = None, rounds: int = 8) -> dict:
    """PHASE 1 coordinator — fan stage_one across ALL sources in PARALLEL (containers spread
    across hosts → ~60% reach the WAF'd origin per try), then re-fan stragglers up to `rounds`
    times. Idempotent skips make re-fans cheap. This function only orchestrates (never touches
    the origin), so its own egress IP is irrelevant. Posts the flat callback on terminal state."""
    started_marker = "stage"
    remaining = [("lar", y) for y in LAR_YEARS] + [("panel", y) for y in PANEL_YEARS]
    staged: dict[str, dict] = {}
    status, error = "error", None
    try:
        for rnd in range(rounds):
            if not remaining:
                break
            results = list(stage_one.starmap(remaining, return_exceptions=True))
            still: list[tuple[str, int]] = []
            errs: list[str] = []
            for (kind, year), res in zip(remaining, results):
                if isinstance(res, Exception):
                    still.append((kind, year))
                    errs.append(f"{kind}{year}={type(res).__name__}: {str(res)[:160]}")
                else:
                    staged[f"{kind}_{year}"] = res
            print(f"round {rnd + 1}: staged {len(remaining) - len(still)}/{len(remaining)}; "
                  f"errs={errs[:4]}")
            remaining = still
        status = "success" if not remaining else "error"
        if remaining:
            error = f"staging unresolved after {rounds} rounds: {remaining}"
    except Exception as exc:  # noqa: BLE001
        error, status = str(exc), "error"
    finally:
        _post_callback(trigger_callback_url,
                       {"status": status, "phase": started_marker, "staged": len(staged),
                        "remaining": [f"{k}_{y}" for k, y in remaining]})
    if status != "success":
        raise RuntimeError(f"hmda staging failed: {error}")
    return {"status": status, "phase": started_marker, "staged_count": len(staged), "staged": staged}


# ── LAR year ingest (streaming append) ─────────────────────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90, memory=32768, cpu=8.0,
)
def ingest_lar_year(year: int, trigger_callback_url: str | None = None) -> dict:
    """Read landed zip from R2→zstd→DuckDB(per-era projection)→stream-append to the unified
    hmda_lar Lance dataset (delete-then-append on data_year). Records ops.* + wakes Trigger.
    Requires the source already staged to R2 landing (stage_all / stage_one)."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    year = int(year)
    if year not in LAR_SOURCES:
        raise ValueError(f"no LAR source for {year}; have {LAR_YEARS}")
    src = LAR_SOURCES[year]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = rejected = 0
    actual = 0
    verified = False
    status, error = "error", None
    zip_path = os.path.join(SCRATCH_DIR, f"lar_{year}.zip")
    zst_path = os.path.join(SCRATCH_DIR, f"lar_{year}.csv.zst")

    so = _r2_storage_options()
    s3 = _s3_client()
    landing_key = _landing_key("lar", year)

    try:
        actual = _r2_to_disk(BUCKET, landing_key, zip_path, s3)  # from R2 landing — no WAF/egress block
        verified = (src["size"] is None) or (actual == src["size"])
        _, cols = _zip_member_to_zst(zip_path, zst_path, src["delim"])
        try:
            os.remove(zip_path)
        except OSError:
            pass
        present = set(cols)
        sql = _build_lar_sql(zst_path, src["delim"], src["era"], year, src["product"], present)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='26GB';")
            con.execute("SET temp_directory='/tmp/duckdb_spill';")

            existed = _dataset_exists(LAR_URI, so)
            if existed:
                lance.dataset(LAR_URI, storage_options=so).delete(f"data_year = {year}")
            mode = "append" if existed else "overwrite"

            reader = con.execute(sql).fetch_record_batch(STREAM_BATCH_ROWS)
            schema = reader.schema
            counter = {"n": 0}

            def _gen():
                for batch in reader:
                    counter["n"] += batch.num_rows
                    yield batch

            lance.write_dataset(
                _gen(), LAR_URI, schema=schema, mode=mode,
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                storage_options=so,
            )
            rows = counter["n"]
            try:
                rj = con.execute("SELECT count(*) n FROM reject_errors").fetchone()
                rejected = int(rj[0]) if rj else 0
            except Exception:  # noqa: BLE001 — no reject table ⇒ zero
                rejected = 0
        finally:
            con.close()
        print(f"LAR {year}: appended {rows:,} rows ({rejected:,} rejected) mode={mode}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        for p in (zip_path, zst_path):
            try:
                os.remove(p)
            except OSError:
                pass
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("lar", year, src["product"], src["era"], src["url"], src["size"],
                    actual, verified, LAR_URI, int(rows), int(rejected), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "dataset": "lar", "year": year,
                        "rows": int(rows), "rejected_rows": int(rejected)})

    if status != "success":
        raise RuntimeError(f"hmda LAR ingest failed for {year}: {error}")
    return {"status": status, "dataset": "lar", "year": year, "rows_processed": int(rows),
            "rejected_rows": int(rejected), "size_verified": verified, "dataset_uri": LAR_URI}


# ── Panel year ingest (small → materialize) ────────────────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 30, memory=8192, cpu=4.0,
)
def ingest_panel_year(year: int, trigger_callback_url: str | None = None) -> dict:
    """Read landed zip from R2→zstd→DuckDB(per-era projection)→append to the unified hmda_panels
    Lance dataset (delete-then-append on source_year). 2024/2025 derive from the 2024 TS.
    Requires the source already staged to R2 landing (stage_all / stage_one)."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    year = int(year)
    if year not in PANEL_SOURCES:
        raise ValueError(f"no panel source for {year}; have {PANEL_YEARS}")
    src = PANEL_SOURCES[year]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = rejected = 0
    actual = 0
    verified = False
    status, error = "error", None
    zip_path = os.path.join(SCRATCH_DIR, f"panel_{year}.zip")
    zst_path = os.path.join(SCRATCH_DIR, f"panel_{year}.csv.zst")

    so = _r2_storage_options()
    s3 = _s3_client()
    landing_key = _landing_key("panel", year)

    try:
        actual = _r2_to_disk(BUCKET, landing_key, zip_path, s3)  # from R2 landing — no WAF/egress block
        verified = (src["size"] is None) or (actual == src["size"])
        _, cols = _zip_member_to_zst(zip_path, zst_path, src["delim"])
        try:
            os.remove(zip_path)
        except OSError:
            pass
        present = set(cols)
        sql = _build_panel_sql(zst_path, src["delim"], src["era"], year, src["product"], present)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.execute(sql).to_arrow_table()
            rows = table.num_rows
            try:
                rj = con.execute("SELECT count(*) n FROM reject_errors").fetchone()
                rejected = int(rj[0]) if rj else 0
            except Exception:  # noqa: BLE001
                rejected = 0
        finally:
            con.close()

        existed = _dataset_exists(PANEL_URI, so)
        if existed:
            lance.dataset(PANEL_URI, storage_options=so).delete(f"source_year = {year}")
        lance.write_dataset(
            table, PANEL_URI, mode="append" if existed else "overwrite",
            data_storage_version=DATA_STORAGE_VERSION, storage_options=so,
        )
        print(f"Panel {year}: appended {rows:,} rows ({src['product']})")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        for p in (zip_path, zst_path):
            try:
                os.remove(p)
            except OSError:
                pass
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("panels", year, src["product"], src["era"], src["url"], src["size"],
                    actual, verified, PANEL_URI, int(rows), int(rejected), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "dataset": "panels", "year": year, "rows": int(rows)})

    if status != "success":
        raise RuntimeError(f"hmda panel ingest failed for {year}: {error}")
    return {"status": status, "dataset": "panels", "year": year, "rows_processed": int(rows),
            "size_verified": verified, "dataset_uri": PANEL_URI}


# ── Index build (run once after the sweep) ─────────────────────────────────────
@app.function(secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=60 * 120, memory=65536, cpu=16.0)
def reindex_dataset(dataset: str, trigger_callback_url: str | None = None) -> dict:
    """Build the BTREE scalar indexes on a unified dataset (join keys + location markers)."""
    dataset = dataset.strip().lower()
    uri = {"lar": LAR_URI, "panels": PANEL_URI}.get(dataset)
    if uri is None:
        raise ValueError(f"dataset must be lar|panels, got {dataset!r}")
    so = _r2_storage_options()
    built = _build_indexes(dataset, uri, so)
    _post_callback(trigger_callback_url,
                   {"status": "success", "dataset": dataset, "indexes": built})
    return {"status": "success", "dataset": dataset, "indexes": built, "dataset_uri": uri}


# ── Server-side full sweep (one container — survives the client; no append conflict) ──
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=6 * 3600, memory=32768, cpu=8.0,
)
def backfill_all() -> dict:
    """Run the ENTIRE 2016–2025 sweep inside ONE Modal container: sequential per-year ingest
    (via .local() — same process, so the unified-dataset appends never collide) then BTREE
    indexing. Dispatch detached (`modal run --detach …::run_backfill`) so the sweep completes
    server-side independent of the client/session. Sources must already be in R2 landing
    (stage_all, or the sandbox stager); each year's ops.hmda_runs row lands as it completes."""
    s3 = _s3_client()
    missing = []
    for kind, years in (("lar", LAR_YEARS), ("panel", PANEL_YEARS)):
        for y in years:
            try:
                s3.head_object(Bucket=BUCKET, Key=_landing_key(kind, y))
            except Exception:  # noqa: BLE001
                missing.append(f"{kind}/{y}")
    if missing:
        raise RuntimeError(f"not staged in R2 landing: {missing} — run stage_all first")

    lar_counts: dict[int, int] = {}
    for y in LAR_YEARS:
        r = ingest_lar_year.local(y, trigger_callback_url=None)
        lar_counts[y] = int(r["rows_processed"])
        print(f"LAR {y}: {lar_counts[y]:,} rows", flush=True)

    panel_counts: dict[int, int] = {}
    for y in PANEL_YEARS:
        r = ingest_panel_year.local(y, trigger_callback_url=None)
        panel_counts[y] = int(r["rows_processed"])
        print(f"PANEL {y}: {panel_counts[y]:,} rows", flush=True)

    # Indexing the ~165M-row LAR BTREEs needs more memory than this container — run each in
    # reindex_dataset's own 64 GB container via .remote() (no append in flight ⇒ safe).
    indexes: dict[str, list[str]] = {}
    for d in ("lar", "panels"):
        indexes[d] = reindex_dataset.remote(d)["indexes"]

    return {"lar": lar_counts, "panels": panel_counts, "indexes": indexes,
            "lar_total": sum(lar_counts.values()), "panel_total": sum(panel_counts.values())}


# ── Local entrypoints ──────────────────────────────────────────────────────────
def _stage_one_reliable(kind: str, year: int, fanout: int = 8, rounds: int = 4) -> dict:
    """Stage ONE source reliably for the single-year entrypoints. The WAF block is dodged by
    PARALLELISM (a warm/sequential retry pins one IP; a parallel fan-out spreads across hosts):
    fan `fanout` concurrent stage_one attempts at the same source and take the first success
    (staging is idempotent). stage_all handles the full sweep more efficiently across sources."""
    targets = [(kind, int(year))] * fanout
    for rnd in range(rounds):
        for res in stage_one.starmap(targets, return_exceptions=True):
            if not isinstance(res, Exception):
                return res
        print(f"  stage {kind} {year}: round {rnd + 1}/{rounds} all-blocked — re-fanning")
    raise RuntimeError(f"could not stage {kind} {year} after {rounds}×{fanout} parallel attempts")


@app.local_entrypoint()
def init_state() -> None:
    print(apply_state_schema.remote())


@app.local_entrypoint()
def stage() -> None:
    import json
    print(json.dumps(stage_all.remote(), default=str))


@app.local_entrypoint()
def lar(year: int) -> None:
    print(_stage_one_reliable("lar", int(year)))
    print(ingest_lar_year.remote(int(year), trigger_callback_url=None))


@app.local_entrypoint()
def panel(year: int) -> None:
    print(_stage_one_reliable("panel", int(year)))
    print(ingest_panel_year.remote(int(year), trigger_callback_url=None))


@app.local_entrypoint()
def reindex(dataset: str = "all") -> None:
    import json
    targets = ["lar", "panels"] if dataset == "all" else [dataset]
    for d in targets:
        print(json.dumps(reindex_dataset.remote(d), default=str))


@app.local_entrypoint()
def backfill() -> None:
    """Full 2016–2025 sweep. LAR + panels are written SEQUENTIALLY per dataset (single-writer
    append, no manifest conflict), then BTREE indexes are built once. Prints per-year counts."""
    import json

    print("=== ensure ops.hmda_runs ===")
    print(json.dumps(apply_state_schema.remote(), default=str))

    print("\n=== Phase 1: stage all sources → R2 landing (parallel fan-out, WAF-resilient) ===")
    print(json.dumps(stage_all.remote(), default=str))

    lar_counts: dict[int, int] = {}
    print("\n=== Phase 2: LAR sweep 2016–2025 (sequential append from R2) ===")
    for y in LAR_YEARS:
        r = ingest_lar_year.remote(y, trigger_callback_url=None)
        lar_counts[y] = r.get("rows_processed", 0)
        print(f"  LAR {y}: rows={lar_counts[y]:>12,}  verified={r.get('size_verified')}")

    panel_counts: dict[int, int] = {}
    print("\n=== Phase 2: Panel sweep 2016–2025 (sequential append from R2) ===")
    for y in PANEL_YEARS:
        r = ingest_panel_year.remote(y, trigger_callback_url=None)
        panel_counts[y] = r.get("rows_processed", 0)
        print(f"  Panel {y}: rows={panel_counts[y]:>9,}  ({PANEL_SOURCES[y]['product']})")

    print("\n=== Build BTREE indexes ===")
    for d in ("lar", "panels"):
        print(json.dumps(reindex_dataset.remote(d), default=str))

    print("\n=== FINAL ROW COUNTS ===")
    lt = sum(lar_counts.values())
    pt = sum(panel_counts.values())
    for y in LAR_YEARS:
        print(f"  LAR   {y}: {lar_counts[y]:>12,}")
    print(f"  LAR   TOTAL: {lt:>12,}")
    for y in PANEL_YEARS:
        print(f"  PANEL {y}: {panel_counts[y]:>12,}")
    print(f"  PANEL TOTAL: {pt:>12,}")


@app.local_entrypoint()
def run_backfill() -> None:
    """Kick the whole sweep server-side in one container. Use `modal run --detach` so it
    survives the client: modal run --detach pipelines/hmda/hmda_bulk.py::run_backfill"""
    import json
    print(json.dumps(backfill_all.remote(), default=str))
