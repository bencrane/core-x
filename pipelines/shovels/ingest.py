"""Compute worker — Shovels.ai building-permit / contractor intelligence (REST v2).

Part of the ``shovels-pipelines`` Modal app. Endpoint-less functions, spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — Python does the HTTP fetch + JSON
projection (a typed REST API, not a CSV to hand-parse), DuckDB does the relational
dedup, Lance is written straight to R2 and BTREE-indexed in place.

The runtime contract is governed by ``docs/SHOVELS_API_CANONICAL_REFERENCE.md``
(authored 2026-05-29 from independent live verification). Section (§) cites below
point at that document.

SIX canonical tables, ONE domain. Every record the API returns lands BOTH as the
typed/queryable projection AND verbatim in ``raw_json`` (lossless 1:1 mirror), plus
the 7-column provenance tail (§"Source ingest invariant"):

    permits      (PermitsRead,    §6.1)  PK id            -> s3://data-sink/active/shovels_permits/
    contractors  (ContractorsRead,§6.3)  PK id            -> s3://data-sink/active/shovels_contractors/
    employees    (Employees,      §6.4)  PK id    (PII)   -> s3://data-sink/active/shovels_employees/
    residents    (ResidentsRead,  §6.5)  PK resident_key (PII) -> s3://data-sink/active/shovels_residents/
    geo          (GeoEntitiesRead,§6.6)  PK geo_id        -> s3://data-sink/active/shovels_geo/
    tags         (list/tags,      §8)    PK id            -> s3://data-sink/active/shovels_tags/

Deployed parameterized workers in this revision: ``ingest_permits`` and
``ingest_contractors`` (the billable geo+window search pulls) plus ``ingest_tags``
(the FREE 22-row catalog — a zero-credit end-to-end proof of the rail). The other
three schemas are migrated and the generic driver supports them; wiring their workers
(employee = contractor-id list, resident = address-geo_id list, geo = state seeds) is
a thin future addition.

Auth / transport (§2): single header ``X-API-Key``; HTTPS GET only; dates are
``YYYY-MM-DD``. Pagination (§4.1/§11): the ``{items, size, next_cursor, total_count}``
cursor envelope. Errors (§9): ``detail`` may be an ARRAY (framework), an OBJECT
(domain), or a STRING (auth/402) — parsed defensively, the key never surfaced.
Billing (§3): every returned record costs 1 credit; ``size`` (page size) == spend
per page. The pull is therefore PARAMETERIZED (geo_id + date window + filters) and
BOUNDED (``max_pages`` credit guardrail) — never a blind national pull.

Idempotency / dedup (§12 id stability): each run upserts latest-per-PK via Lance
``merge_insert`` (re-running the same geo/window updates rows in place; the row count
stays stable). Lance MVCC retains prior versions for time-travel.

Control plane (Trigger v4 durable callback): each worker accepts
``trigger_callback_url`` and, on terminal state (success OR failure), (1) writes a run
row to ``ops.shovels_ingest_runs`` via psycopg and (2) POSTs a FLAT JSON body to that
url. No ``{"data": ...}`` envelope.

    modal deploy pipelines/shovels/ingest.py
    modal run    pipelines/shovels/ingest.py::migrate              # create ops.shovels_ingest_runs
    modal run    pipelines/shovels/ingest.py::tags                 # FREE end-to-end proof (0 credits)
    modal run    pipelines/shovels/ingest.py::permits --geo-id CA --permit-from 2025-01-01 --permit-to 2025-01-08 --size 5 --max-pages 1
    modal run    pipelines/shovels/ingest.py::contractors --geo-id 94110 --permit-from 2024-01-01 --permit-to 2024-12-31 --size 25 --max-pages 4
    modal run    pipelines/shovels/ingest.py::reindex_one --entity permits
    modal run    pipelines/shovels/ingest.py::show_ledger
"""

from __future__ import annotations

import os

import modal

# ── Static surface ───────────────────────────────────────────────────────────
SHOVELS_BASE_URL = os.environ.get("SHOVELS_BASE_URL", "https://api.shovels.ai/v2")
SOURCE_PROVIDER = "shovels"
FEED = "shovels"
BUCKET = "data-sink"

# Lance system-of-record tier — one dataset per entity (env-overridable). The
# directive's s3://data-sink/active/shovels_<entity>/ layout.
PERMITS_URI = os.environ.get("SHOVELS_PERMITS_LANCE_URI", "s3://data-sink/active/shovels_permits/")
CONTRACTORS_URI = os.environ.get("SHOVELS_CONTRACTORS_LANCE_URI", "s3://data-sink/active/shovels_contractors/")
EMPLOYEES_URI = os.environ.get("SHOVELS_EMPLOYEES_LANCE_URI", "s3://data-sink/active/shovels_employees/")
RESIDENTS_URI = os.environ.get("SHOVELS_RESIDENTS_LANCE_URI", "s3://data-sink/active/shovels_residents/")
GEO_URI = os.environ.get("SHOVELS_GEO_LANCE_URI", "s3://data-sink/active/shovels_geo/")
TAGS_URI = os.environ.get("SHOVELS_TAGS_LANCE_URI", "s3://data-sink/active/shovels_tags/")

# Lance fragment sizing — Lance defaults (02_lancedb_storage.md §2.2). 90 GiB.
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Credit-safety defaults (§3: 1 credit / returned record; size == spend per page).
# The worker is BOUNDED by default: ceiling ≈ DEFAULT_SIZE * DEFAULT_MAX_PAGES
# records/run. max_pages <= 0 means "exhaust the cursor" (explicit operator opt-in
# to an uncapped full-market backfill).
DEFAULT_SIZE = 50
DEFAULT_MAX_PAGES = 20

# Idempotent ops.* DDL — mirror of pipelines/shovels/ops_shovels_ingest_runs.sql
# (that .sql is the canonical source of truth; keep the two in sync).
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.shovels_ingest_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id           text        NOT NULL,
    feed             text        NOT NULL,
    entity           text        NOT NULL,
    source_endpoint  text,
    dataset_uri      text,
    geo_id           text,
    permit_from      text,
    permit_to        text,
    page_size        integer,
    max_pages        integer,
    query_spec       jsonb,
    write_mode       text,
    api_calls        integer,
    rows_fetched     bigint,
    rows_written     bigint,
    credits_spent    integer,
    indexes_built    jsonb,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    duration_seconds double precision,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_feed_idx        ON ops.shovels_ingest_runs (feed);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_entity_idx      ON ops.shovels_ingest_runs (entity);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_status_idx      ON ops.shovels_ingest_runs (status);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_geo_idx         ON ops.shovels_ingest_runs (geo_id);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_run_id_idx      ON ops.shovels_ingest_runs (run_id);
CREATE INDEX IF NOT EXISTS shovels_ingest_runs_recorded_at_idx ON ops.shovels_ingest_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",        # Shovels API + Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort (lance-format/lance#2650); cheap at this scale
)

app = modal.App("shovels-pipelines", image=image)


# ══════════════════════════════════════════════════════════════════════════════
# 1. Schema definitions — typed-column coercion + the 6 EntityIngestSpec blueprints
#    (migrated from the canonical reference §6 field dictionaries; raw_json + the
#    7-column provenance tail make every row a lossless 1:1 mirror of the API record)
# ══════════════════════════════════════════════════════════════════════════════
import json as _json
from dataclasses import dataclass
from datetime import date as _date, datetime as _dt, timezone as _tz
from typing import Any, Callable


def to_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return v or None
    return str(value)


def to_int(value: Any) -> int | None:
    """Integer coercion. Money fields are already integer dollars in the Shovels
    payload (§2/§6) — no scaling; mirror the upstream int verbatim."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(float(s))
        except ValueError:
            return None
    return None


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def to_json_str(value: Any) -> str | None:
    """Serialize a nested struct/list (tags, geo_ids, address, classification_derived,
    …) to a compact JSON string so it stays queryable in DuckDB via ``json_extract``
    without a LIST<…> Lance definition-buffer concern."""
    if value is None:
        return None
    return _json.dumps(value, sort_keys=True, separators=(",", ":"))


def _g(*keys: str) -> Callable[[dict], Any]:
    """Extractor for a top-level field via the first present key."""
    def _extract(raw: dict[str, Any]) -> Any:
        for k in keys:
            if k in raw:
                return raw.get(k)
        return None
    return _extract


def _nested(outer: str, inner: str) -> Callable[[dict], Any]:
    def _extract(raw: dict[str, Any]) -> Any:
        sub = raw.get(outer)
        return sub.get(inner) if isinstance(sub, dict) else None
    return _extract


def _str_of(*keys: str):
    inner = _g(*keys)
    return lambda raw: to_str(inner(raw))


def _int_of(*keys: str):
    inner = _g(*keys)
    return lambda raw: to_int(inner(raw))


def _float_of(*keys: str):
    inner = _g(*keys)
    return lambda raw: to_float(inner(raw))


def _json_of(*keys: str):
    inner = _g(*keys)
    return lambda raw: to_json_str(inner(raw))


def _nested_str(outer: str, inner: str):
    fn = _nested(outer, inner)
    return lambda raw: to_str(fn(raw))


# The 7-column provenance tail appended to EVERY entity's typed projection.
# ``raw_json`` is the verbatim-mirror column (lossless fallback); the rest is run
# lineage. Built lazily inside the spec so importing this module needs no pyarrow
# (the Modal image has it; local unit tests import pyarrow explicitly).
def _provenance_fields():
    import pyarrow as pa

    return [
        pa.field("raw_json", pa.string(), nullable=False),
        pa.field("source_provider", pa.string(), nullable=False),
        pa.field("source_endpoint", pa.string(), nullable=False),
        pa.field("source_query_spec", pa.string(), nullable=False),
        pa.field("snapshot_date", pa.string(), nullable=False),   # YYYY-MM-DD
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]


_PROVENANCE_COLUMN_NAMES = {
    "raw_json", "source_provider", "source_endpoint", "source_query_spec",
    "snapshot_date", "source_run_id", "ingested_at",
}


def resident_key(*, address_geo_id: str, raw: dict[str, Any]) -> str:
    """Deterministic composite PK for a resident row (§6.5 has NO natural id).

    Keyed on the address geo_id + a stable hash of the identity tuple (name +
    personal_emails + phone). Same person at same address on a re-fetch ⇒ identical
    key ⇒ merge_insert collapses to one row. The raw PII still lands verbatim in
    raw_json; the key itself is a non-reversible digest (no PII recoverable from it
    alone). Used by a future ``ingest_residents`` worker.
    """
    import hashlib

    name = (to_str(raw.get("name")) or "").strip().lower()
    email = (to_str(raw.get("personal_emails")) or "").strip().lower()
    phone = (to_str(raw.get("phone")) or "").strip().lower()
    basis = f"{address_geo_id}|{name}|{email}|{phone}"
    digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]
    return f"{address_geo_id}:{digest}"


@dataclass
class EntityIngestSpec:
    """Everything entity-specific about a Shovels ingest.

    ``typed_columns`` is an ordered list of (column_name, arrow_type, extractor);
    ``extractor`` maps a raw record dict → the typed value. The driver appends the
    7-column provenance tail automatically. ``pk_column`` is the BTREE / dedup key.
    """

    entity: str
    pk_column: str
    typed_columns: list[tuple[Any, Any, Callable[[dict[str, Any]], Any]]]

    def arrow_schema(self):
        import pyarrow as pa

        fields = [pa.field(name, dtype, nullable=True) for name, dtype, _ in self.typed_columns]
        # PK kept nullable in Arrow to tolerate a rare upstream null; the dedup
        # SELECT applies WHERE pk IS NOT NULL at emit time.
        return pa.schema(fields + _provenance_fields())

    def project(
        self,
        *,
        raw: dict[str, Any],
        source_endpoint: str,
        query_spec_json: str,
        snapshot_date: str,
        run_id: str,
        ingested_at,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for name, _dtype, extractor in self.typed_columns:
            if name in _PROVENANCE_COLUMN_NAMES:
                raise ValueError(f"typed column {name!r} collides with a provenance column")
            row[name] = extractor(raw)
        row["raw_json"] = _json.dumps(raw, sort_keys=True, separators=(",", ":"))
        row["source_provider"] = SOURCE_PROVIDER
        row["source_endpoint"] = source_endpoint
        row["source_query_spec"] = query_spec_json
        row["snapshot_date"] = snapshot_date
        row["source_run_id"] = run_id
        row["ingested_at"] = ingested_at
        return row


def _permit_spec() -> EntityIngestSpec:
    import pyarrow as pa

    return EntityIngestSpec(
        entity="permits",
        pk_column="id",
        typed_columns=[
            ("id", pa.string(), _str_of("id")),
            ("number", pa.string(), _str_of("number")),
            ("description", pa.string(), _str_of("description")),
            ("jurisdiction", pa.string(), _str_of("jurisdiction")),
            ("job_value", pa.int64(), _int_of("job_value")),
            ("fees", pa.int64(), _int_of("fees")),
            ("type", pa.string(), _str_of("type")),
            ("subtype", pa.string(), _str_of("subtype")),
            ("status", pa.string(), _str_of("status")),
            ("file_date", pa.string(), _str_of("file_date")),
            ("issue_date", pa.string(), _str_of("issue_date")),
            ("final_date", pa.string(), _str_of("final_date")),
            ("start_date", pa.string(), _str_of("start_date")),
            ("end_date", pa.string(), _str_of("end_date")),
            ("total_duration", pa.int64(), _int_of("total_duration")),
            ("construction_duration", pa.int64(), _int_of("construction_duration")),
            ("approval_duration", pa.int64(), _int_of("approval_duration")),
            ("inspection_pass_rate", pa.float64(), _float_of("inspection_pass_rate")),
            ("contractor_id", pa.string(), _str_of("contractor_id")),
            ("tags", pa.string(), _json_of("tags")),
            ("property_type", pa.string(), _str_of("property_type")),
            ("property_type_detail", pa.string(), _str_of("property_type_detail")),
            ("property_year_built", pa.int64(), _int_of("property_year_built")),
            ("property_building_area", pa.int64(), _int_of("property_building_area")),
            ("property_lot_size", pa.int64(), _int_of("property_lot_size")),
            ("property_unit_count", pa.int64(), _int_of("property_unit_count")),
            ("property_assess_market_value", pa.int64(), _int_of("property_assess_market_value")),
            ("geo_ids", pa.string(), _json_of("geo_ids")),
            ("address_id", pa.string(), _nested_str("geo_ids", "address_id")),
            ("city_id", pa.string(), _nested_str("geo_ids", "city_id")),
            ("county_id", pa.string(), _nested_str("geo_ids", "county_id")),
            ("jurisdiction_id", pa.string(), _nested_str("geo_ids", "jurisdiction_id")),
            ("address_city", pa.string(), _nested_str("address", "city")),
            ("address_state", pa.string(), _nested_str("address", "state")),
            ("address_zip_code", pa.string(), _nested_str("address", "zip_code")),
            ("address", pa.string(), _json_of("address")),
        ],
    )


def _contractor_spec() -> EntityIngestSpec:
    import pyarrow as pa

    return EntityIngestSpec(
        entity="contractors",
        pk_column="id",
        typed_columns=[
            ("id", pa.string(), _str_of("id")),
            ("license", pa.string(), _str_of("license")),
            ("name", pa.string(), _str_of("name")),
            ("business_name", pa.string(), _str_of("business_name")),
            ("business_type", pa.string(), _str_of("business_type")),
            ("classification", pa.string(), _json_of("classification")),
            ("classification_derived", pa.string(), _json_of("classification_derived")),
            ("license_issue_date", pa.string(), _str_of("license_issue_date")),
            ("license_exp_date", pa.string(), _str_of("license_exp_date")),
            ("primary_email", pa.string(), _str_of("primary_email")),
            ("primary_phone", pa.string(), _str_of("primary_phone")),
            ("website", pa.string(), _str_of("website")),
            ("dba", pa.string(), _str_of("dba")),
            ("sic", pa.string(), _str_of("sic")),
            ("naics", pa.string(), _str_of("naics")),
            ("linkedin_url", pa.string(), _str_of("linkedin_url")),
            ("revenue", pa.string(), _str_of("revenue")),
            ("employee_count", pa.string(), _str_of("employee_count")),  # RANGE STRING (§13.7)
            ("primary_industry", pa.string(), _str_of("primary_industry")),
            ("review_count", pa.int64(), _int_of("review_count")),
            ("rating", pa.float64(), _float_of("rating")),
            ("permit_count", pa.int64(), _int_of("permit_count")),
            ("avg_job_value", pa.int64(), _int_of("avg_job_value")),
            ("total_job_value", pa.int64(), _int_of("total_job_value")),
            ("avg_construction_duration", pa.int64(), _int_of("avg_construction_duration")),
            ("avg_inspection_pass_rate", pa.float64(), _float_of("avg_inspection_pass_rate")),
            ("first_seen_date", pa.string(), _str_of("first_seen_date")),
            ("status_tally", pa.string(), _json_of("status_tally")),
            ("tag_tally", pa.string(), _json_of("tag_tally")),
            ("address_state", pa.string(), _nested_str("address", "state")),
            ("address", pa.string(), _json_of("address")),
        ],
    )


def _employee_spec() -> EntityIngestSpec:
    import pyarrow as pa

    return EntityIngestSpec(
        entity="employees",
        pk_column="id",
        typed_columns=[
            ("id", pa.string(), _str_of("id")),
            ("contractor_id", pa.string(), _str_of("contractor_id")),
            ("name", pa.string(), _str_of("name")),                 # PII
            ("phone", pa.string(), _str_of("phone")),               # PII
            ("email", pa.string(), _str_of("email")),               # PII
            ("business_email", pa.string(), _str_of("business_email")),  # PII
            ("linkedin_url", pa.string(), _str_of("linkedin_url")),
            ("street_no", pa.string(), _str_of("street_no")),
            ("street", pa.string(), _str_of("street")),
            ("city", pa.string(), _str_of("city")),
            ("state", pa.string(), _str_of("state")),
            ("zip_code", pa.string(), _str_of("zip_code")),
            ("gender", pa.string(), _str_of("gender")),
            ("age_range", pa.string(), _str_of("age_range")),
            ("income_range", pa.string(), _str_of("income_range")),
            ("net_worth", pa.string(), _str_of("net_worth")),
            ("homeowner", pa.string(), _str_of("homeowner")),
            ("job_title", pa.string(), _str_of("job_title")),
            ("seniority_level", pa.string(), _str_of("seniority_level")),
            ("department", pa.string(), _str_of("department")),
        ],
    )


def _resident_spec() -> EntityIngestSpec:
    import pyarrow as pa

    # The future ``ingest_residents`` worker augments each raw resident with
    # '_resident_key' and '_address_geo_id' (it knows the source address geo_id)
    # before projection — see ``resident_key``.
    return EntityIngestSpec(
        entity="residents",
        pk_column="resident_key",
        typed_columns=[
            ("resident_key", pa.string(), _str_of("_resident_key")),
            ("address_geo_id", pa.string(), _str_of("_address_geo_id")),
            ("name", pa.string(), _str_of("name")),                 # PII
            ("personal_emails", pa.string(), _str_of("personal_emails")),  # PII (single string, §6.5)
            ("phone", pa.string(), _str_of("phone")),               # PII
            ("linkedin_url", pa.string(), _str_of("linkedin_url")),
            ("net_worth", pa.string(), _str_of("net_worth")),
            ("income_range", pa.string(), _str_of("income_range")),
            ("is_homeowner", pa.string(), _str_of("is_homeowner")),
            ("street_no", pa.string(), _str_of("street_no")),
            ("street", pa.string(), _str_of("street")),
            ("city", pa.string(), _str_of("city")),
            ("state", pa.string(), _str_of("state")),
            ("zip_code", pa.string(), _str_of("zip_code")),
        ],
    )


def _geo_spec() -> EntityIngestSpec:
    import pyarrow as pa

    # The future ``ingest_geo`` worker normalizes city/county/jurisdiction/state/
    # zipcode rows into a common shape augmented with '_geo_type' and '_seed_state'
    # before projection.
    return EntityIngestSpec(
        entity="geo",
        pk_column="geo_id",
        typed_columns=[
            ("geo_id", pa.string(), _str_of("geo_id")),
            ("geo_type", pa.string(), _str_of("_geo_type")),  # city|county|jurisdiction|state|zipcode
            ("name", pa.string(), _str_of("name")),
            ("state", pa.string(), _str_of("state")),
            ("seed_state", pa.string(), _str_of("_seed_state")),
            ("counties", pa.string(), _json_of("counties")),
            ("jurisdictions", pa.string(), _json_of("jurisdictions")),
            ("zipcodes", pa.string(), _json_of("zipcodes")),
        ],
    )


def _tag_spec() -> EntityIngestSpec:
    import pyarrow as pa

    return EntityIngestSpec(
        entity="tags",
        pk_column="id",
        typed_columns=[
            ("id", pa.string(), _str_of("id")),
            ("description", pa.string(), _str_of("description")),
        ],
    )


# entity slug → (spec factory, dataset uri, source_endpoint). source_endpoint is the
# canonical pull path; permits/contractors are the deployed search workers, tags the
# FREE catalog. The 3 unwired entities carry their endpoint for the future worker.
ENTITIES: dict[str, dict] = {
    "permits":     {"spec": _permit_spec,     "uri": PERMITS_URI,     "endpoint": "permits/search"},
    "contractors": {"spec": _contractor_spec, "uri": CONTRACTORS_URI, "endpoint": "contractors/search"},
    "employees":   {"spec": _employee_spec,   "uri": EMPLOYEES_URI,   "endpoint": "contractors/{id}/employees"},
    "residents":   {"spec": _resident_spec,   "uri": RESIDENTS_URI,   "endpoint": "addresses/{geo_id}/residents"},
    "geo":         {"spec": _geo_spec,        "uri": GEO_URI,         "endpoint": "geo/search"},
    "tags":        {"spec": _tag_spec,        "uri": TAGS_URI,        "endpoint": "list/tags"},
}

# The §7.1 permit/contractor search filter surface forwarded verbatim. Shovels does
# NOT enum-validate most of these (§8 gotcha) — we pass through and let the API be the
# source of truth. geo_id/permit_from/permit_to are required and handled separately.
SEARCH_FILTER_KEYS = {
    "permit_q", "permit_tags", "permit_status", "permit_has_contractor",
    "permit_min_approval_duration", "permit_min_construction_duration",
    "permit_min_inspection_pr", "permit_min_job_value", "permit_min_fees",
    "property_type", "property_min_market_value", "property_min_building_area",
    "property_min_lot_size", "property_min_story_count", "property_min_unit_count",
    "contractor_classification_derived", "contractor_name", "contractor_website",
    "contractor_min_total_job_value", "contractor_min_total_permits_count",
    "contractor_min_inspection_pr", "contractor_license", "include_tallies",
}


# ══════════════════════════════════════════════════════════════════════════════
# 2. API client — X-API-Key auth, cursor-envelope pagination, defensive error
#    parsing (§9), live credit accounting from X-Credits-Request (§3)
# ══════════════════════════════════════════════════════════════════════════════
class ShovelsAPIError(RuntimeError):
    def __init__(self, status: int, detail: Any, *, endpoint: str):
        self.status = status
        self.detail = detail
        self.endpoint = endpoint
        super().__init__(f"Shovels {endpoint} -> HTTP {status}: {_summarize_detail(detail)}")


def _summarize_detail(detail: Any) -> str:
    """§9: detail may be an array (framework), an object (domain), or a string
    (auth/402). Summarize defensively for logs; never surface the API key."""
    try:
        return _json.dumps(detail)[:300]
    except (TypeError, ValueError):
        return str(detail)[:300]


class ShovelsClient:
    """Sync Shovels client. One instance per ingest run.

    Auth (§2): single ``X-API-Key`` header (no OAuth/bearer/query-key). Credit
    accounting (§3): every billable response carries ``X-Credits-Request`` == the
    number of records returned; we sum it across all pages into ``credits_spent``.
    Free endpoints emit no header → contribute 0.
    """

    def __init__(self, api_key: str, *, timeout: float = 60.0):
        import requests

        if not api_key:
            raise EnvironmentError("SHOVELS_API_KEY is required")
        self._session = requests.Session()
        self._session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
        self._base = SHOVELS_BASE_URL.rstrip("/")
        self._timeout = timeout
        self.credits_spent = 0
        self.api_calls = 0

    def __enter__(self) -> "ShovelsClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:  # noqa: BLE001
            pass

    def _get(self, path: str, params: list[tuple[str, Any]] | None = None):
        """Low-level GET with one-shot 429 backoff (§10.3) + credit tallying (§3)."""
        import time

        url = f"{self._base}/{path.lstrip('/')}"
        attempt = 0
        while True:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            self.api_calls += 1
            if resp.status_code == 429 and attempt == 0:
                try:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                except ValueError:
                    retry_after = 5
                print(f"429 on {path} — sleeping {retry_after}s (one-shot retry)")
                time.sleep(retry_after)
                attempt += 1
                continue
            credit_header = resp.headers.get("X-Credits-Request")
            if credit_header is not None:
                try:
                    self.credits_spent += int(credit_header)
                except ValueError:
                    pass
            return resp

    def get_json(
        self, path: str, params: list[tuple[str, Any]] | None = None,
        *, treat_404_as_empty: bool = False,
    ) -> dict[str, Any]:
        """GET → parsed JSON envelope. §13: metrics endpoints return a real 404 for
        "no rows" (caller sets ``treat_404_as_empty``); search/by-id/list return 200
        with ``items: []`` for "no data" (handled by the empty-items path upstream)."""
        resp = self._get(path, params)
        try:
            body = resp.json()
        except ValueError:
            body = {"raw_text": resp.text[:500]}
        if resp.status_code == 404 and treat_404_as_empty:
            return {"items": []}
        if resp.status_code >= 400:
            detail = body.get("detail") if isinstance(body, dict) else body
            raise ShovelsAPIError(resp.status_code, detail, endpoint=path)
        return body if isinstance(body, dict) else {"items": body}

    def paginate(
        self,
        path: str,
        *,
        base_params: list[tuple[str, Any]],
        size: int,
        max_pages: int | None,
        treat_404_as_empty: bool = False,
    ):
        """Yield raw record dicts across the §4.1/§11 cursor envelope.

        First page omits ``cursor``; subsequent pages pass the prior ``next_cursor``
        until it is null or ``max_pages`` is reached (the credit guardrail). Works for
        every paginated list endpoint; endpoints that ignore pagination return one page
        with ``next_cursor: null`` and are handled transparently. An empty ``items``
        page (§13: "no data") simply yields nothing.
        """
        cursor: str | None = None
        page = 0
        while True:
            if max_pages is not None and max_pages > 0 and page >= max_pages:
                print(f"max_pages={max_pages} reached for {path} (credit guardrail)")
                return
            params = list(base_params) + [("size", size)]
            if cursor:
                params.append(("cursor", cursor))
            body = self.get_json(path, params, treat_404_as_empty=treat_404_as_empty)
            for item in body.get("items") or []:
                if isinstance(item, dict):
                    yield item
            page += 1
            cursor = body.get("next_cursor")
            if not cursor:
                return


def _build_search_params(
    *, geo_id: str, permit_from: str, permit_to: str, filters: dict[str, Any],
) -> list[tuple[str, Any]]:
    """Flatten the required geo+window axis (§7.1) plus the verbatim §7 filter surface
    into ``(key, value)`` query params. List-valued filters expand to repeated params
    (Shovels AND-combines repeated ``permit_tags`` etc., ``-`` excludes — §7.4)."""
    params: list[tuple[str, Any]] = [
        ("geo_id", geo_id),
        ("permit_from", permit_from),
        ("permit_to", permit_to),
    ]
    for key, value in (filters or {}).items():
        if key not in SEARCH_FILTER_KEYS:
            raise ValueError(
                f"unknown search filter {key!r}; allowed: {sorted(SEARCH_FILTER_KEYS)}"
            )
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                if item is not None and item != "":
                    params.append((key, item))
        elif isinstance(value, bool):
            params.append((key, "true" if value else "false"))
        else:
            params.append((key, value))
    return params


# ══════════════════════════════════════════════════════════════════════════════
# 3. Data plane — R2 storage options, Lance write / dedup / BTREE index
# ══════════════════════════════════════════════════════════════════════════════
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


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — not yet created
        return False


def _dedupe_table(table, pk: str):
    """Collapse the freshly-fetched batch to latest-per-PK and drop null PKs, so the
    merge_insert source carries unique keys. DuckDB does the relational op (§12: ids are
    stable-with-churn — within a single run any duplicate is the same record). Returns an
    Arrow table with the spec schema preserved."""
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.register("batch", table)
        out = con.execute(
            f'SELECT * EXCLUDE (_rn) FROM ('
            f'  SELECT *, ROW_NUMBER() OVER (PARTITION BY "{pk}" ORDER BY ingested_at DESC) AS _rn'
            f'  FROM batch WHERE "{pk}" IS NOT NULL'
            f') WHERE _rn = 1'
        ).fetch_arrow_table()
    finally:
        con.close()
    # Re-cast to the exact spec schema (DuckDB round-trip can widen types).
    return out.cast(table.schema)


def _write_or_merge(table, uri: str, pk: str, so: dict) -> str:
    """Idempotent materialization. First run → create (overwrite); thereafter →
    merge_insert(pk) upserting latest-per-PK in place (the §12 id-churn-safe incremental
    model). Returns the write mode used."""
    import lance

    if not _dataset_exists(uri, so):
        lance.write_dataset(
            table, uri, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        return "create"
    (lance.dataset(uri, storage_options=so)
        .merge_insert(pk)
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(table))
    return "merge_insert"


def _build_index(uri: str, pk: str, so: dict) -> list[str]:
    """Build the BTREE scalar index on the PK directly in R2 (replace=True → idempotent;
    LANCE_BYPASS_SPILLING does the sort in-RAM). The directive's load-bearing resolution
    key. An index miss is logged, never fatal — the Lance data write is the critical
    artifact."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    built: list[str] = []
    try:
        ds.create_scalar_index(pk, index_type="BTREE", replace=True)
        built.append(f"BTREE:{pk}")
        print(f"  BTREE  ✓ {pk}")
    except Exception as exc:  # noqa: BLE001
        print(f"  WARN BTREE {pk} failed: {exc}")
    return built


def _optimize_indices(uri: str, so: dict) -> None:
    """Fold the merge_insert's new fragments into the existing BTREE."""
    import lance

    try:
        lance.dataset(uri, storage_options=so).optimize.optimize_indices()
        print("  index optimize ✓")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: optimize_indices failed ({exc}); index still covers pre-merge rows.")


# ══════════════════════════════════════════════════════════════════════════════
# 4. State + callback (ARCHITECTURE.md §5: the worker owns terminal state)
# ══════════════════════════════════════════════════════════════════════════════
def _record_run(
    *, run_id, entity, source_endpoint, dataset_uri, geo_id, permit_from, permit_to,
    page_size, max_pages, query_spec, write_mode, api_calls, rows_fetched, rows_written,
    credits_spent, indexes_built, status, error, started_at, completed_at,
) -> None:
    """Terminal run row → ops.shovels_ingest_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest. Logs the directive's telemetry —
    parameters used (geo/window/size/max_pages/query_spec), rows fetched, credits spent."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    duration = None
    if started_at and completed_at:
        duration = (completed_at - started_at).total_seconds()
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.shovels_ingest_runs
                    (run_id, feed, entity, source_endpoint, dataset_uri, geo_id,
                     permit_from, permit_to, page_size, max_pages, query_spec, write_mode,
                     api_calls, rows_fetched, rows_written, credits_spent, indexes_built,
                     status, error, started_at, completed_at, duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s)
                """,
                (run_id, FEED, entity, source_endpoint, dataset_uri, geo_id,
                 permit_from, permit_to, page_size, max_pages, Jsonb(query_spec), write_mode,
                 api_calls, rows_fetched, rows_written, credits_spent, Jsonb(indexes_built),
                 status, error, started_at, completed_at, duration),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. FLAT JSON body — NO
    {"data": ...} envelope, NO API key (the callbackHash in the url is the auth)."""
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
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def _require_api_key() -> str:
    key = os.environ.get("SHOVELS_API_KEY")
    if not key:
        raise RuntimeError("SHOVELS_API_KEY not set (Modal secret 'shovels-api').")
    return key


def _new_run_id() -> str:
    import uuid

    return str(uuid.uuid4())


# ══════════════════════════════════════════════════════════════════════════════
# 5. The generic ingest driver — fetch → project → dedup → Lance → BTREE → ops
# ══════════════════════════════════════════════════════════════════════════════
def _run_ingest(
    *, entity: str, record_iter, client: "ShovelsClient", query_spec: dict,
    geo_id: str | None, permit_from: str | None, permit_to: str | None,
    page_size: int | None, max_pages: int | None, trigger_callback_url: str | None,
) -> dict:
    """Consume ``record_iter`` (raw dicts already being fetched via ``client``), project
    each to typed + raw_json + 7-col provenance, dedup latest-per-PK, write/merge to the
    entity's R2 Lance dataset, build the BTREE on the PK, then record state + wake Trigger.
    Re-raises on failure so the Modal call is marked failed (after recording terminal state).
    """
    import pyarrow as pa
    import lance

    cfg = ENTITIES[entity]
    spec: EntityIngestSpec = cfg["spec"]()
    uri, source_endpoint, pk = cfg["uri"], cfg["endpoint"], spec.pk_column

    started_at = _dt.now(_tz.utc)
    snapshot_date = started_at.date().isoformat()
    run_id = _new_run_id()
    query_spec_json = _json.dumps(query_spec, sort_keys=True, separators=(",", ":"))
    ingested_at = started_at

    rows_fetched = 0
    rows_written = 0
    write_mode = "noop"
    indexes_built: list[str] = []
    status, error = "error", None

    try:
        so = _r2_storage_options()
        rows: list[dict] = []
        for raw in record_iter:
            rows.append(spec.project(
                raw=raw, source_endpoint=source_endpoint, query_spec_json=query_spec_json,
                snapshot_date=snapshot_date, run_id=run_id, ingested_at=ingested_at,
            ))
        rows_fetched = len(rows)
        print(f"[{entity}] fetched rows={rows_fetched} api_calls={client.api_calls} "
              f"credits_spent={client.credits_spent}")

        if rows_fetched == 0:
            # §13: items:[] is "no data" — not an error. Record a clean no_data run and
            # leave the dataset untouched (no empty-dataset creation).
            status = "no_data"
            rows_written = (lance.dataset(uri, storage_options=so).count_rows()
                            if _dataset_exists(uri, so) else 0)
        else:
            table = pa.Table.from_pylist(rows, schema=spec.arrow_schema())
            table = _dedupe_table(table, pk)
            write_mode = _write_or_merge(table, uri, pk, so)
            del table
            if write_mode == "create":
                indexes_built = _build_index(uri, pk, so)
            else:
                _optimize_indices(uri, so)
                indexes_built = [f"BTREE:{pk}(optimized)"]
            rows_written = lance.dataset(uri, storage_options=so).count_rows()
            print(f"[{entity}] {write_mode}: dataset now {rows_written:,} rows "
                  f"(BTREE on {pk})")
            status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = f"{type(exc).__name__}: {exc}"
        status = "error"
    finally:
        completed_at = _dt.now(_tz.utc)
        _record_run(
            run_id=run_id, entity=entity, source_endpoint=source_endpoint, dataset_uri=uri,
            geo_id=geo_id, permit_from=permit_from, permit_to=permit_to,
            page_size=page_size, max_pages=max_pages, query_spec=query_spec,
            write_mode=write_mode, api_calls=client.api_calls, rows_fetched=rows_fetched,
            rows_written=rows_written, credits_spent=client.credits_spent,
            indexes_built=indexes_built, status=status, error=error,
            started_at=started_at, completed_at=completed_at,
        )
        client.close()
        _post_callback(trigger_callback_url, {
            "status": status, "feed": FEED, "entity": entity, "dataset_uri": uri,
            "rows_fetched": rows_fetched, "rows": rows_written,
            "credits_spent": client.credits_spent, "run_id": run_id,
        })

    if status == "error":
        raise RuntimeError(f"shovels {entity} ingest failed: {error}")
    return {
        "status": status, "feed": FEED, "entity": entity, "dataset_uri": uri,
        "rows_fetched": rows_fetched, "rows": rows_written, "write_mode": write_mode,
        "credits_spent": client.credits_spent, "api_calls": client.api_calls,
        "indexes_built": indexes_built, "run_id": run_id,
    }


def _search_ingest(
    *, entity: str, geo_id: str, permit_from: str, permit_to: str,
    size: int, max_pages: int, filters_json: str | None, trigger_callback_url: str | None,
) -> dict:
    """Shared driver for the two billable search workers (permits, contractors). Validates
    the required geo+window axis (§7.1), assembles the param surface, paginates the cursor
    envelope under the ``max_pages`` credit guardrail, and hands the record stream to
    ``_run_ingest``."""
    missing = [k for k, v in (("geo_id", geo_id), ("permit_from", permit_from),
                              ("permit_to", permit_to)) if not v]
    if missing:
        raise RuntimeError(
            f"shovels {entity} requires {missing} — refusing to run an unbounded pull. "
            "geo_id accepts a 2-letter state code (CA), a ZIP (90210), or a Shovels "
            "geo_id (§18); dates are YYYY-MM-DD (§2)."
        )
    filters = _json.loads(filters_json) if filters_json else {}
    if not isinstance(filters, dict):
        raise RuntimeError("filters_json must decode to a JSON object")

    base_params = _build_search_params(
        geo_id=geo_id, permit_from=permit_from, permit_to=permit_to, filters=filters,
    )
    query_spec = {
        "geo_id": geo_id, "permit_from": permit_from, "permit_to": permit_to,
        "size": size, "max_pages": max_pages, "filters": filters,
    }
    client = ShovelsClient(_require_api_key())
    record_iter = client.paginate(
        ENTITIES[entity]["endpoint"], base_params=base_params, size=size, max_pages=max_pages,
    )
    return _run_ingest(
        entity=entity, record_iter=record_iter, client=client, query_spec=query_spec,
        geo_id=geo_id, permit_from=permit_from, permit_to=permit_to,
        page_size=size, max_pages=max_pages, trigger_callback_url=trigger_callback_url,
    )


# ══════════════════════════════════════════════════════════════════════════════
# 6. Modal functions
# ══════════════════════════════════════════════════════════════════════════════
_SECRETS = [
    modal.Secret.from_name("r2-credentials"),
    modal.Secret.from_name("hqx-postgres"),
    modal.Secret.from_name("shovels-api"),
]


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_migration() -> dict:
    """Create ops.shovels_ingest_runs (idempotent). Mirrors ops_shovels_ingest_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.shovels_ingest_runs')")
        present = cur.fetchone()[0]
    print(f"ops.shovels_ingest_runs present = {present}")
    return {"table": "ops.shovels_ingest_runs", "present": str(present)}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=4.0)
def ingest_permits(
    geo_id: str | None = None,
    permit_from: str | None = None,
    permit_to: str | None = None,
    size: int = DEFAULT_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    filters_json: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Permits search (PermitsRead, §6.1) → s3://data-sink/active/shovels_permits/.

    Parameterized + BOUNDED: required geo_id + permit_from + permit_to (§7.1); ``size`` is
    credits/page and ``max_pages`` caps spend (≈ size*max_pages records/run; max_pages<=0
    exhausts the cursor — uncapped, operator opt-in). ``filters_json`` is an optional JSON
    object of the §7 filter surface (e.g. {"permit_tags":["solar"],"property_type":"residential"}).
    """
    return _search_ingest(
        entity="permits", geo_id=geo_id, permit_from=permit_from, permit_to=permit_to,
        size=size, max_pages=max_pages, filters_json=filters_json,
        trigger_callback_url=trigger_callback_url,
    )


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=4.0)
def ingest_contractors(
    geo_id: str | None = None,
    permit_from: str | None = None,
    permit_to: str | None = None,
    size: int = DEFAULT_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    filters_json: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Contractors search (ContractorsRead, §6.3) → s3://data-sink/active/shovels_contractors/.

    Same parameterization/credit-safety contract as ``ingest_permits``. ``filters_json``
    may also carry ``include_tallies`` (§7.3) to populate status_tally/tag_tally.
    """
    return _search_ingest(
        entity="contractors", geo_id=geo_id, permit_from=permit_from, permit_to=permit_to,
        size=size, max_pages=max_pages, filters_json=filters_json,
        trigger_callback_url=trigger_callback_url,
    )


@app.function(secrets=_SECRETS, timeout=60 * 10, memory=4096, cpu=2.0)
def ingest_tags(trigger_callback_url: str | None = None) -> dict:
    """Tag vocabulary (list/tags, §8) → s3://data-sink/active/shovels_tags/. FREE (0 credits)
    — the 22-row static catalog. Doubles as the zero-credit end-to-end proof of the rail:
    client auth → envelope parse → projection → Lance write → BTREE → ops ledger."""
    client = ShovelsClient(_require_api_key())
    record_iter = client.paginate("list/tags", base_params=[], size=100, max_pages=5)
    return _run_ingest(
        entity="tags", record_iter=record_iter, client=client, query_spec={},
        geo_id=None, permit_from=None, permit_to=None, page_size=100, max_pages=5,
        trigger_callback_url=trigger_callback_url,
    )


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 30,
              memory=8192, cpu=4.0)
def reindex(entity: str) -> dict:
    """Rebuild the BTREE scalar index on an existing dataset (no re-ingest)."""
    import lance

    key = entity.strip().lower()
    if key not in ENTITIES:
        raise ValueError(f"entity must be one of {sorted(ENTITIES)}, got {entity!r}")
    uri = ENTITIES[key]["uri"]
    pk = ENTITIES[key]["spec"]().pk_column
    so = _r2_storage_options()
    if not _dataset_exists(uri, so):
        raise RuntimeError(f"{key}: no dataset at {uri} to reindex")
    rows = lance.dataset(uri, storage_options=so).count_rows()
    print(f"Reindexing {uri} — {rows:,} rows")
    built = _build_index(uri, pk, so)
    return {"entity": key, "dataset_uri": uri, "rows": rows, "indexes": built}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def ledger(limit: int = 20) -> list:
    """Read the most recent ops.shovels_ingest_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, entity, source_endpoint, geo_id, permit_from, permit_to, "
            "page_size, max_pages, write_mode, rows_fetched, rows_written, credits_spent, "
            "status, error, started_at, completed_at "
            "FROM ops.shovels_ingest_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ══════════════════════════════════════════════════════════════════════════════
# 7. Manual ops entrypoints (local — no callback). ops.* write still fires.
# ══════════════════════════════════════════════════════════════════════════════
@app.local_entrypoint()
def migrate() -> None:
    import json
    print(json.dumps(apply_migration.remote(), indent=2, default=str))


@app.local_entrypoint()
def tags() -> None:
    import json
    print(json.dumps(ingest_tags.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def permits(
    geo_id: str, permit_from: str, permit_to: str,
    size: int = DEFAULT_SIZE, max_pages: int = DEFAULT_MAX_PAGES, filters_json: str = "",
) -> None:
    import json
    print(json.dumps(ingest_permits.remote(
        geo_id=geo_id, permit_from=permit_from, permit_to=permit_to,
        size=size, max_pages=max_pages, filters_json=filters_json or None,
        trigger_callback_url=None,
    ), indent=2, default=str))


@app.local_entrypoint()
def contractors(
    geo_id: str, permit_from: str, permit_to: str,
    size: int = DEFAULT_SIZE, max_pages: int = DEFAULT_MAX_PAGES, filters_json: str = "",
) -> None:
    import json
    print(json.dumps(ingest_contractors.remote(
        geo_id=geo_id, permit_from=permit_from, permit_to=permit_to,
        size=size, max_pages=max_pages, filters_json=filters_json or None,
        trigger_callback_url=None,
    ), indent=2, default=str))


@app.local_entrypoint()
def reindex_one(entity: str = "permits") -> None:
    import json
    print(json.dumps(reindex.remote(entity), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 20) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
