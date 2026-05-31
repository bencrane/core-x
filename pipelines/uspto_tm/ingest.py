"""Compute worker — USPTO Trademark bulk ingest (Applications / Assignments / TTAB).

Part of the ``uspto-trademarks`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints. Clean-room
data plane: no Iceberg, no Polaris — Python does XML→NDJSON I/O only, DuckDB does 100%
of the transform/cast, Lance is written straight to R2.

THE XML→NDJSON BRIDGE (approved). DuckDB has no core read_xml; the USPTO bulk format is
deeply-nested XML (depth 10). Python's role is a *lossless, mechanical* XML→newline-
delimited-JSON transcode (the same class of I/O concern as the SAM/PPP cp1252→utf-8
transcode-on-write): stream one record element at a time with lxml.iterparse (the
fast_iter pattern — clear the element + drop consumed siblings so RSS stays flat over
the ~30-40 GB Applications corpus), serialize its subtree faithfully (repeating groups →
JSON arrays, nested groups → JSON objects), write one line, move on. NO projection, NO
casting, NO business logic in Python.

DuckDB then ingests the NDJSON with an EXPLICIT per-dataset schema (read_json columns=...)
into native nested STRUCT / LIST<STRUCT> columns, TRY_CASTs the scalar spine, maps the
'T'/'F' flags to BOOLEAN, and exports zero-copy Arrow. NO bare VARIANT, NO raw-JSON-text
column anywhere. Every identifier (serial_number, registration_number, reel_no, frame_no,
proceeding_number) is held VARCHAR to preserve leading zeros / fixed width.

Mutation model (approved). A daily file re-emits the FULL current snapshot of every
touched record (proven: a 1999 mark reappears whole in a 2026-05-30 delta). So:
  • backfile  → mode="overwrite" on the first part, mode="append" for the rest, run as ONE
                sequential loop in a single container (no concurrent Lance manifest writers).
  • delta     → Lance merge_insert(on=<pk>).when_matched_update_all().when_not_matched_insert_all(),
                the SQL-MERGE UPSERT that folds pending→registered→dead state changes in place.
Lance immutable-manifest MVCC retains each daily version for free point-in-time time-travel.

Topology (approved sequence): Applications (apc) is the master entity spine, keyed on
serial_number with a hard BTREE. Assignments (asb) and TTAB (tt) are child datasets whose
serial numbers resolve back to that spine (each row also carries a flattened
property_serial_numbers LIST<VARCHAR> for FK lookup; explicit bridge datasets are a
downstream derivation).

Control plane (Trigger v4 durable callback): on terminal state (success OR failure) the
worker (1) writes a run row to ops.uspto_tm_runs via psycopg and (2) POSTs a FLAT JSON body
{status, rows, feed, dataset, run_mode, dataset_uri, as_of} to trigger_callback_url. No
{"data": ...} envelope.

    modal deploy pipelines/uspto_tm/ingest.py
    modal run    pipelines/uspto_tm/ingest.py::migrate                       # create ops.uspto_tm_runs
    modal run    pipelines/uspto_tm/ingest.py::backfill --dataset applications
    modal run    pipelines/uspto_tm/ingest.py::delta    --dataset applications
    modal run    pipelines/uspto_tm/ingest.py::reindex  --dataset applications
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/uspto/trademarks/"
SCRATCH_DIR = "/tmp"

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact.
#   max_bytes_per_file: the directive wrote `90 * 10243`, annotated "(90 GiB)". The only
#   reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160), Lance's documented
#   default; confirmed across the SBA / CO-SoS feeds. A literal `90 * 10243` (~900 KB) would
#   shatter the dataset into hundreds of thousands of fragments.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (per 02_lancedb_storage.md §2.3, directive).
DATA_STORAGE_VERSION = "2.1"

# Idempotent ops.* DDL — mirror of pipelines/uspto_tm/ops_uspto_tm_runs.sql (source of truth).
_OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.uspto_tm_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset         text        NOT NULL,
    feed            text        NOT NULL,
    run_mode        text        NOT NULL,
    write_mode      text,
    dataset_uri     text,
    as_of           text,
    source_files    jsonb,
    parts_processed integer,
    rows_processed  bigint,
    rows_upserted   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_dataset_idx     ON ops.uspto_tm_runs (dataset);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_feed_idx        ON ops.uspto_tm_runs (feed);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_status_idx      ON ops.uspto_tm_runs (status);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_as_of_idx       ON ops.uspto_tm_runs (as_of DESC);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_recorded_at_idx ON ops.uspto_tm_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table/reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "lxml>=5.2",             # streaming iterparse XML→NDJSON transcode
    "boto3>=1.35",           # R2 landing read
    "requests>=2.32",        # Trigger callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE scalar-index builds sort the column. Lance's spill-to-disk sorter uses a small
    # bounded DataFusion memory pool that OOMs on the high-cardinality serial_number /
    # mark_identification columns at scale. Force the in-memory sort path (container RAM).
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("uspto-trademarks", image=image)


# ──────────────────────────────────────────────────────────────────────────────
# Per-dataset configuration. Each carries: XML record/list shape (for the transcode),
# the EXPLICIT DuckDB read_json column schema (nested STRUCT / LIST<STRUCT>, all-VARCHAR
# leaves — no VARIANT), the projection SQL (typed spine + faithful nested detail), the
# merge key, and the BTREE/BITMAP index plan.
# ──────────────────────────────────────────────────────────────────────────────

# ===== Applications (apc) — the master entity spine. =====
_APC_HEADER = (
    "STRUCT("
    "filing_date VARCHAR, registration_date VARCHAR, status_code VARCHAR, status_date VARCHAR, "
    "mark_identification VARCHAR, mark_drawing_code VARCHAR, published_for_opposition_date VARCHAR, "
    "amend_to_register_date VARCHAR, abandonment_date VARCHAR, cancellation_code VARCHAR, "
    "cancellation_date VARCHAR, renewal_date VARCHAR, renewal_filed_in VARCHAR, "
    "attorney_name VARCHAR, attorney_docket_number VARCHAR, domestic_representative_name VARCHAR, "
    "current_location VARCHAR, location_date VARCHAR, law_office_assigned_location_code VARCHAR, "
    "employee_name VARCHAR, "
    "principal_register_amended_in VARCHAR, supplemental_register_amended_in VARCHAR, "
    "trademark_in VARCHAR, collective_trademark_in VARCHAR, service_mark_in VARCHAR, "
    "collective_service_mark_in VARCHAR, collective_membership_mark_in VARCHAR, certification_mark_in VARCHAR, "
    "cancellation_pending_in VARCHAR, published_concurrent_in VARCHAR, concurrent_use_in VARCHAR, "
    "concurrent_use_proceeding_in VARCHAR, interference_pending_in VARCHAR, opposition_pending_in VARCHAR, "
    "section_12c_in VARCHAR, section_2f_in VARCHAR, section_2f_in_part_in VARCHAR, "
    "section_8_filed_in VARCHAR, section_8_partial_accept_in VARCHAR, section_8_accepted_in VARCHAR, "
    "section_15_acknowledged_in VARCHAR, section_15_filed_in VARCHAR, supplemental_register_in VARCHAR, "
    "foreign_priority_in VARCHAR, change_registration_in VARCHAR, intent_to_use_in VARCHAR, "
    "intent_to_use_current_in VARCHAR, filed_as_use_application_in VARCHAR, amended_to_use_application_in VARCHAR, "
    "use_application_currently_in VARCHAR, amended_to_itu_application_in VARCHAR, "
    "filing_basis_filed_as_44d_in VARCHAR, amended_to_44d_application_in VARCHAR, filing_basis_current_44d_in VARCHAR, "
    "filing_basis_filed_as_44e_in VARCHAR, filing_basis_current_44e_in VARCHAR, amended_to_44e_application_in VARCHAR, "
    "without_basis_currently_in VARCHAR, filing_current_no_basis_in VARCHAR, "
    "color_drawing_filed_in VARCHAR, color_drawing_current_in VARCHAR, drawing_3d_filed_in VARCHAR, "
    "drawing_3d_current_in VARCHAR, standard_characters_claimed_in VARCHAR, "
    "filing_basis_filed_as_66a_in VARCHAR, filing_basis_current_66a_in VARCHAR"
    ")"
)
_APC_COLUMNS = {
    "serial_number": "VARCHAR",
    "registration_number": "VARCHAR",
    "transaction_date": "VARCHAR",
    "action_key": "VARCHAR",
    "file_creation_datetime": "VARCHAR",
    "case_file_header": _APC_HEADER,
    "case_file_statements": 'STRUCT(case_file_statement STRUCT(type_code VARCHAR, "text" VARCHAR)[])',
    "case_file_event_statements": (
        'STRUCT(case_file_event_statement STRUCT(code VARCHAR, "type" VARCHAR, '
        'description_text VARCHAR, "date" VARCHAR, "number" VARCHAR)[])'
    ),
    "classifications": (
        "STRUCT(classification STRUCT(international_code_total_no VARCHAR, us_code_total_no VARCHAR, "
        "international_code VARCHAR, us_code VARCHAR[], status_code VARCHAR, status_date VARCHAR, "
        "first_use_anywhere_date VARCHAR, first_use_in_commerce_date VARCHAR, primary_code VARCHAR)[])"
    ),
    "correspondent": "STRUCT(address_1 VARCHAR, address_2 VARCHAR, address_3 VARCHAR, address_4 VARCHAR, address_5 VARCHAR)",
    "case_file_owners": (
        "STRUCT(case_file_owner STRUCT(entry_number VARCHAR, party_type VARCHAR, "
        "nationality STRUCT(country VARCHAR, state VARCHAR), legal_entity_type_code VARCHAR, party_name VARCHAR, "
        "address_1 VARCHAR, address_2 VARCHAR, city VARCHAR, state VARCHAR, country VARCHAR, postcode VARCHAR, "
        "name_change_explanation VARCHAR)[])"
    ),
    "foreign_applications": (
        "STRUCT(foreign_application STRUCT(entry_number VARCHAR, country VARCHAR, "
        "foreign_priority_claim_in VARCHAR, foreign_registration_number VARCHAR, "
        "registration_date VARCHAR, filing_date VARCHAR)[])"
    ),
    "prior_registration_applications": (
        'STRUCT(other_related_in VARCHAR, prior_registration_application STRUCT(relationship_type VARCHAR, "number" VARCHAR)[])'
    ),
    "design_searches": "STRUCT(design_search STRUCT(code VARCHAR)[])",
    "madrid_international_filing_requests": (
        "STRUCT(madrid_international_filing_record STRUCT(entry_number VARCHAR, reference_number VARCHAR, "
        "original_filing_date_uspto VARCHAR, international_registration_number VARCHAR, "
        "international_registration_date VARCHAR, international_status_code VARCHAR, international_status_date VARCHAR, "
        'international_renewal_date VARCHAR, madrid_history_events STRUCT(madrid_history_event '
        'STRUCT(code VARCHAR, "date" VARCHAR, description_text VARCHAR, entry_number VARCHAR)[]))[])'
    ),
}
_APC_PROJECTION = """
SELECT
    nullif(trim(serial_number), '')                                                       AS serial_number,
    nullif(trim(registration_number), '')                                                 AS registration_number,
    TRY_CAST(TRY_STRPTIME(nullif(trim(transaction_date), ''), '%Y%m%d') AS DATE)           AS transaction_date,
    nullif(trim(action_key), '')                                                          AS action_key,
    TRY_CAST(TRY_STRPTIME(nullif(trim(file_creation_datetime), ''), '%Y%m%d%H%M') AS TIMESTAMP) AS file_creation_datetime,
    nullif(trim(case_file_header.mark_identification), '')                                 AS mark_identification,
    nullif(trim(case_file_header.status_code), '')                                         AS status_code,
    TRY_CAST(TRY_STRPTIME(nullif(trim(case_file_header.status_date), ''), '%Y%m%d') AS DATE) AS status_date,
    nullif(trim(case_file_header.mark_drawing_code), '')                                   AS mark_drawing_code,
    TRY_CAST(TRY_STRPTIME(nullif(trim(case_file_header.filing_date), ''), '%Y%m%d') AS DATE)       AS filing_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(case_file_header.registration_date), ''), '%Y%m%d') AS DATE) AS registration_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(case_file_header.abandonment_date), ''), '%Y%m%d') AS DATE)  AS abandonment_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(case_file_header.cancellation_date), ''), '%Y%m%d') AS DATE) AS cancellation_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(case_file_header.renewal_date), ''), '%Y%m%d') AS DATE)      AS renewal_date,
    (case_file_header.section_8_accepted_in = 'T')                                         AS section_8_accepted,
    (case_file_header.section_15_acknowledged_in = 'T')                                    AS section_15_acknowledged,
    (case_file_header.standard_characters_claimed_in = 'T')                                AS standard_characters_claimed,
    (case_file_header.intent_to_use_in = 'T')                                              AS intent_to_use,
    nullif(trim(case_file_header.attorney_name), '')                                       AS attorney_name,
    nullif(trim(case_file_header.current_location), '')                                    AS current_location,
    case_file_header                                                                       AS case_file_header,
    classifications.classification                                                         AS classifications,
    case_file_statements.case_file_statement                                              AS statements,
    case_file_event_statements.case_file_event_statement                                  AS event_statements,
    case_file_owners.case_file_owner                                                       AS owners,
    correspondent                                                                          AS correspondent,
    foreign_applications.foreign_application                                               AS foreign_applications,
    prior_registration_applications.prior_registration_application                         AS prior_registrations,
    design_searches.design_search                                                          AS design_searches,
    madrid_international_filing_requests.madrid_international_filing_record                 AS madrid_records,
    '__SOURCE_FILE__'                                                                      AS source_file,
    now()                                                                                  AS ingested_at
FROM raw_records
WHERE nullif(trim(serial_number), '') IS NOT NULL
"""

# ===== Assignments (asb) — chain of title. =====
_ASB_COLUMNS = {
    "action_key_code": "VARCHAR",
    "transaction_date": "VARCHAR",
    "file_creation_datetime": "VARCHAR",
    "assignment": (
        "STRUCT(reel_no VARCHAR, frame_no VARCHAR, last_update_date VARCHAR, purge_indicator VARCHAR, "
        "date_recorded VARCHAR, page_count VARCHAR, conveyance_text VARCHAR, "
        "correspondent STRUCT(person_or_organization_name VARCHAR, address_1 VARCHAR, address_2 VARCHAR, "
        "address_3 VARCHAR, address_4 VARCHAR))"
    ),
    # NB: assignor/assignee `nationality` is a SCALAR string here (e.g. "OHIO"), NOT a
    # struct — unlike apc's case-file owner. Probed shape: assignors.assignor[].nationality = str.
    "assignors": (
        "STRUCT(assignor STRUCT(person_or_organization_name VARCHAR, execution_date VARCHAR, "
        "date_acknowledged VARCHAR, legal_entity_text VARCHAR, nationality VARCHAR, "
        "formerly_statement VARCHAR, composed_of_statement VARCHAR, dba_aka_ta_statement VARCHAR, "
        "address_1 VARCHAR, address_2 VARCHAR, city VARCHAR, state VARCHAR, country_name VARCHAR, postcode VARCHAR)[])"
    ),
    "assignees": (
        "STRUCT(assignee STRUCT(person_or_organization_name VARCHAR, legal_entity_text VARCHAR, "
        "nationality VARCHAR, composed_of_statement VARCHAR, "
        "address_1 VARCHAR, address_2 VARCHAR, city VARCHAR, state VARCHAR, country_name VARCHAR, postcode VARCHAR)[])"
    ),
    "properties": (
        "STRUCT(property STRUCT(serial_no VARCHAR, registration_no VARCHAR, "
        "trademark_law_treaty_property STRUCT(tlt_mark_name VARCHAR))[])"
    ),
}
_ASB_PROJECTION = """
SELECT
    nullif(trim(assignment.reel_no), '')                                                  AS reel_no,
    nullif(trim(assignment.frame_no), '')                                                 AS frame_no,
    nullif(trim(assignment.reel_no), '') || '-' || coalesce(nullif(trim(assignment.frame_no), ''), '') AS assignment_id,
    TRY_CAST(TRY_STRPTIME(nullif(trim(assignment.last_update_date), ''), '%Y%m%d') AS DATE) AS last_update_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(assignment.date_recorded), ''), '%Y%m%d') AS DATE)   AS date_recorded,
    nullif(trim(assignment.conveyance_text), '')                                          AS conveyance_text,
    nullif(trim(assignment.purge_indicator), '')                                          AS purge_indicator,
    TRY_CAST(nullif(trim(assignment.page_count), '') AS INTEGER)                           AS page_count,
    nullif(trim(action_key_code), '')                                                     AS action_key_code,
    TRY_CAST(TRY_STRPTIME(nullif(trim(transaction_date), ''), '%Y%m%d') AS DATE)           AS transaction_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(file_creation_datetime), ''), '%Y%m%d%H%M') AS TIMESTAMP) AS file_creation_datetime,
    assignment                                                                            AS assignment,
    assignors.assignor                                                                    AS assignors,
    assignees.assignee                                                                    AS assignees,
    properties.property                                                                   AS properties,
    list_transform(coalesce(properties.property, []), x -> nullif(trim(x.serial_no), ''))  AS property_serial_numbers,
    '__SOURCE_FILE__'                                                                      AS source_file,
    now()                                                                                  AS ingested_at
FROM raw_records
WHERE nullif(trim(assignment.reel_no), '') IS NOT NULL
"""

# ===== TTAB (tt) — proceedings (oppositions / cancellations / extensions / expungement). =====
_TT_COLUMNS = {
    "number": "VARCHAR",
    "type_code": "VARCHAR",
    "status_code": "VARCHAR",
    "filing_date": "VARCHAR",
    "status_update_date": "VARCHAR",
    "interlocutory_attorney_name": "VARCHAR",
    "location_code": "VARCHAR",
    "employee_number": "VARCHAR",
    "day_in_location": "VARCHAR",
    "action_key_code": "VARCHAR",
    "transaction_date": "VARCHAR",
    "file_creation_datetime": "VARCHAR",
    "party_information": (
        "STRUCT(party STRUCT(identifier VARCHAR, role_code VARCHAR, name VARCHAR, orgname VARCHAR, "
        "address_information STRUCT(proceeding_address STRUCT(identifier VARCHAR, name VARCHAR, orgname VARCHAR, "
        "address_1 VARCHAR, address_2 VARCHAR, city VARCHAR, state VARCHAR, country VARCHAR, postcode VARCHAR, "
        "type_code VARCHAR)[]), "
        "property_information STRUCT(property STRUCT(identifier VARCHAR, serial_number VARCHAR, "
        "registration_number VARCHAR, mark_text VARCHAR, "
        "tma_proceeding STRUCT(proceeding_number VARCHAR, proceeding_type_code VARCHAR))[]))[])"
    ),
    "prosecution_history": (
        'STRUCT(prosecution_entry STRUCT(identifier VARCHAR, code VARCHAR, type_code VARCHAR, '
        '"date" VARCHAR, due_date VARCHAR, history_text VARCHAR)[])'
    ),
}
_TT_PROJECTION = """
SELECT
    nullif(trim("number"), '')                                                            AS proceeding_number,
    nullif(trim("number"), '') || ':' || coalesce(nullif(trim(type_code), ''), '')        AS proceeding_key,
    nullif(trim(type_code), '')                                                           AS type_code,
    nullif(trim(status_code), '')                                                         AS status_code,
    TRY_CAST(TRY_STRPTIME(nullif(trim(filing_date), ''), '%Y%m%d') AS DATE)                AS filing_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(status_update_date), ''), '%Y%m%d') AS DATE)         AS status_update_date,
    nullif(trim(interlocutory_attorney_name), '')                                         AS interlocutory_attorney_name,
    nullif(trim(location_code), '')                                                       AS location_code,
    nullif(trim(action_key_code), '')                                                     AS action_key_code,
    TRY_CAST(TRY_STRPTIME(nullif(trim(transaction_date), ''), '%Y%m%d') AS DATE)           AS transaction_date,
    TRY_CAST(TRY_STRPTIME(nullif(trim(file_creation_datetime), ''), '%Y%m%d%H%M') AS TIMESTAMP) AS file_creation_datetime,
    party_information.party                                                                AS parties,
    prosecution_history.prosecution_entry                                                 AS prosecution_history,
    flatten(list_transform(coalesce(party_information.party, []),
        p -> list_transform(coalesce(p.property_information.property, []),
            x -> nullif(trim(x.serial_number), '')))) AS property_serial_numbers,
    '__SOURCE_FILE__'                                                                      AS source_file,
    now()                                                                                  AS ingested_at
FROM raw_records
WHERE nullif(trim("number"), '') IS NOT NULL
"""

DATASETS: dict[str, dict] = {
    "applications": {
        "feed": "uspto_tm_applications",
        "lance_uri": os.environ.get("USPTO_TM_APPLICATIONS_LANCE_URI", "s3://data-sink/active/uspto_tm_applications/"),
        "root_tag": "trademark-applications-daily",
        "record_tag": "case-file",
        "action_tag": "action-key",        # apc: governing action-key precedes the case-file siblings → track + inject
        "list_tags": {
            "case_file_statement", "case_file_event_statement", "classification", "us_code",
            "case_file_owner", "foreign_application", "prior_registration_application",
            "design_search", "madrid_international_filing_record", "madrid_history_event",
        },
        "columns": _APC_COLUMNS,
        "projection": _APC_PROJECTION,
        "merge_key": "serial_number",
        "dedup_order": "transaction_date DESC NULLS LAST",
        "btree": ["serial_number", "registration_number", "mark_identification"],
        "bitmap": ["status_code", "mark_drawing_code"],
        "backfile_re": r"^apc\d{8}-\d{8}-\d+\.zip$",
        "delta_re": r"^apc\d{6}\.zip$",
    },
    "assignments": {
        "feed": "uspto_tm_assignments",
        "lance_uri": os.environ.get("USPTO_TM_ASSIGNMENTS_LANCE_URI", "s3://data-sink/active/uspto_tm_assignments/"),
        "root_tag": "trademark-assignments",
        "record_tag": "assignment-entry",
        "action_tag": None,                 # asb: action-key-code is an in-record child
        "list_tags": {"assignor", "assignee", "property"},
        "columns": _ASB_COLUMNS,
        "projection": _ASB_PROJECTION,
        "merge_key": "assignment_id",
        "dedup_order": "transaction_date DESC NULLS LAST, last_update_date DESC NULLS LAST",
        "btree": ["assignment_id", "reel_no", "frame_no"],
        "bitmap": ["action_key_code", "purge_indicator"],
        "backfile_re": r"^asb\d{8}-\d{8}-\d+\.zip$",
        "delta_re": r"^asb\d{6}\.zip$",
    },
    "ttab": {
        "feed": "uspto_tm_ttab",
        "lance_uri": os.environ.get("USPTO_TM_TTAB_LANCE_URI", "s3://data-sink/active/uspto_tm_ttab/"),
        "root_tag": "ttab-proceedings",
        "record_tag": "proceeding-entry",
        "action_tag": None,                 # tt: action-key-code is an in-record child
        "list_tags": {"party", "proceeding_address", "property", "prosecution_entry"},
        "columns": _TT_COLUMNS,
        "projection": _TT_PROJECTION,
        # <number> is NOT unique (an application serial carries EXA/EXT/MIS sub-proceedings,
        # and the source re-emits exact-duplicate rows). The natural key is number:type_code.
        "merge_key": "proceeding_key",
        "dedup_order": "transaction_date DESC NULLS LAST, status_update_date DESC NULLS LAST",
        "btree": ["proceeding_key", "proceeding_number"],
        "bitmap": ["type_code", "status_code"],
        "backfile_re": r"^tt\d{8}-\d{8}-\d+\.zip$",
        "delta_re": r"^tt\d{6}\.zip$",
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# R2 / object-store
# ──────────────────────────────────────────────────────────────────────────────
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
    """boto3 S3 client for R2 — checksum behaviour forced to ``when_required`` (R2 rejects
    botocore's default flexible-checksum validation)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _list_landing(s3, pattern: str) -> list[str]:
    """Landing keys whose basename matches `pattern`, sorted by trailing part number then name."""
    import re

    rx = re.compile(pattern)
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for obj in page.get("Contents", []):
            base = obj["Key"].rsplit("/", 1)[-1]
            if rx.match(base):
                keys.append(obj["Key"])

    def _part(key: str) -> tuple:
        base = key.rsplit("/", 1)[-1]
        m = re.search(r"-(\d+)\.zip$", base)
        return (int(m.group(1)) if m else 0, base)

    return sorted(keys, key=_part)


def _as_of_from_key(key: str) -> str | None:
    """The YYMMDD / YYYYMMDD date stamp in a delta basename, e.g. apc260530.zip → 260530."""
    import re

    m = re.search(r"(\d{6,8})\.zip$", key.rsplit("/", 1)[-1])
    return m.group(1) if m else None


# ──────────────────────────────────────────────────────────────────────────────
# XML → NDJSON transcode (Python = I/O only; lossless, mechanical)
# ──────────────────────────────────────────────────────────────────────────────
def _localname(tag) -> str:
    """Namespace-stripped, snake_cased local tag name."""
    return str(tag).split("}", 1)[-1].replace("-", "_")


def _elem_to_obj(elem, list_tags: set[str]):
    """Recursively convert an lxml element subtree to JSON-native Python. A leaf → its
    stripped text (or None). A branch → dict; a child tag is a LIST when it is in
    `list_tags` or occurs >1 under this element, else a scalar/object. Faithful: no
    projection, no casting — only XML→JSON structure + key snake_casing."""
    kids = [c for c in elem if isinstance(c.tag, str)]  # skip comments / PIs
    if not kids:
        text = (elem.text or "").strip()
        return text or None
    order: list[str] = []
    groups: dict[str, list] = {}
    for c in kids:
        t = _localname(c.tag)
        if t not in groups:
            groups[t] = []
            order.append(t)
        groups[t].append(c)
    obj: dict = {}
    for t in order:
        vals = [_elem_to_obj(c, list_tags) for c in groups[t]]
        obj[t] = vals if (t in list_tags or len(vals) > 1) else vals[0]
    return obj


def _zip_to_ndjson(local_zip: str, cfg: dict, ndjson_path: str) -> int:
    """Stream the single XML member of `local_zip` and write one NDJSON line per record
    element (fast_iter: clear + drop consumed siblings → flat RSS over multi-hundred-MB
    members). Injects file-level `file_creation_datetime` and (apc) the governing
    `action_key` as faithful provenance. Returns the record count."""
    import json
    import zipfile

    from lxml import etree

    record_tag = cfg["record_tag"]
    action_tag = cfg.get("action_tag")
    list_tags = cfg["list_tags"]
    watch = [record_tag, "creation-datetime"] + ([action_tag] if action_tag else [])

    count = 0
    cur_action: str | None = None
    cur_creation: str | None = None

    with zipfile.ZipFile(local_zip) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not members:
            raise RuntimeError(f"no .xml member in {local_zip} (members={zf.namelist()[:5]})")
        member = max(members, key=lambda n: zf.getinfo(n).file_size)
        with zf.open(member) as xf, open(ndjson_path, "w", encoding="utf-8") as out:
            context = etree.iterparse(
                xf, events=("end",), tag=watch,
                resolve_entities=False, load_dtd=False, no_network=True,
                huge_tree=True, recover=True,
            )
            for _, elem in context:
                lname = elem.tag.split("}", 1)[-1] if isinstance(elem.tag, str) else ""
                if lname == "creation-datetime":
                    cur_creation = (elem.text or "").strip() or None
                    elem.clear()
                    continue
                if action_tag and lname == action_tag:
                    cur_action = (elem.text or "").strip() or None
                    elem.clear()
                    continue
                if lname == record_tag:
                    obj = _elem_to_obj(elem, list_tags)
                    if not isinstance(obj, dict):
                        obj = {}
                    obj["file_creation_datetime"] = cur_creation
                    if action_tag:
                        obj["action_key"] = cur_action
                    out.write(json.dumps(obj, ensure_ascii=False))
                    out.write("\n")
                    count += 1
                    # fast_iter prune: free this record and every consumed sibling before it.
                    elem.clear()
                    parent = elem.getparent()
                    if parent is not None:
                        while elem.getprevious() is not None:
                            del parent[0]
            del context
    return count


# ──────────────────────────────────────────────────────────────────────────────
# DuckDB transform (100%): NDJSON → typed nested Arrow
# ──────────────────────────────────────────────────────────────────────────────
def _projected_relation(con, ndjson_path: str, cfg: dict, source_file: str):
    """Read the NDJSON under the EXPLICIT nested schema, expose as `raw_records`, return the
    projected DuckDBPyRelation (typed spine + nested LIST<STRUCT> detail; no VARIANT)."""
    rel = con.read_json(
        ndjson_path,
        columns=cfg["columns"],
        format="newline_delimited",
        maximum_object_size=64 * 1024 * 1024,
    )
    rel.create_view("raw_records", replace=True)
    projection = cfg["projection"].replace("__SOURCE_FILE__", source_file.replace("'", "''"))
    return con.sql(projection)


def _project_dedup(con, ndjson_path: str, cfg: dict, source_file: str):
    """Project, then collapse to one row per merge key keeping the latest snapshot. A
    current-state master requires a UNIQUE key per write/merge batch — the source re-emits a
    record on every change within a daily file, and the TTAB backfile carries exact-duplicate
    rows. Returns the deduped DuckDBPyRelation."""
    proj = _projected_relation(con, ndjson_path, cfg, source_file)
    proj.create_view("projected", replace=True)
    mk = cfg["merge_key"]
    order = cfg.get("dedup_order", "transaction_date DESC NULLS LAST")
    return con.sql(
        f'SELECT * EXCLUDE (__rn) FROM ('
        f'  SELECT *, row_number() OVER (PARTITION BY "{mk}" ORDER BY {order}) AS __rn FROM projected'
        f") WHERE __rn = 1"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Lance write / index
# ──────────────────────────────────────────────────────────────────────────────
def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001 — not yet created
        return False


def _write_dataset(data, uri: str, mode: str, so: dict, schema=None) -> None:
    import lance

    kwargs = dict(
        mode=mode,
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    if schema is not None:
        kwargs["schema"] = schema
    lance.write_dataset(data, uri, **kwargs)


def _create_indexes(cfg: dict, so: dict) -> None:
    """Full BTREE + BITMAP build on the dataset (replace=True → idempotent). An index miss is
    logged, never fatal — the Lance data write is the critical artifact."""
    import lance

    ds = lance.dataset(cfg["lance_uri"], storage_options=so)
    for col in cfg["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: BTREE index on {col} failed: {exc}")
    for col in cfg["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: BITMAP index on {col} failed: {exc}")


def _optimize_indices(uri: str, so: dict) -> None:
    """Fold new fragments into existing scalar indexes after a delta merge (incremental —
    avoids a full daily rebuild). Best-effort across pylance optimize-API shapes."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    try:
        ds.optimize.optimize_indices()
        print("  index optimize ✓")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: optimize_indices failed ({exc}); indexes still cover pre-merge rows.")


# ──────────────────────────────────────────────────────────────────────────────
# State + callback
# ──────────────────────────────────────────────────────────────────────────────
def _record_run(dataset, feed, run_mode, write_mode, dataset_uri, as_of, source_files,
                parts, rows, rows_upserted, status, error, started_at, completed_at) -> None:
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.uspto_tm_runs
                    (dataset, feed, run_mode, write_mode, dataset_uri, as_of, source_files,
                     parts_processed, rows_processed, rows_upserted, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, feed, run_mode, write_mode, dataset_uri, as_of, Jsonb(source_files),
                 parts, rows, rows_upserted, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


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


def _resolve_dataset(dataset: str) -> dict:
    key = dataset.strip().lower()
    if key not in DATASETS:
        raise ValueError(f"dataset must be one of {sorted(DATASETS)}, got {dataset!r}")
    return DATASETS[key]


# ──────────────────────────────────────────────────────────────────────────────
# Workers
# ──────────────────────────────────────────────────────────────────────────────
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 6,   # ~30-40 GB Applications corpus streamed part-by-part in one container
    memory=16384,
    cpu=4.0,
)
def ingest_backfile(dataset: str, trigger_callback_url: str | None = None) -> dict:
    """Sequential backfile load: stream every landing part in order through XML→NDJSON →
    DuckDB typed projection → Lance (first part overwrite, rest append) in ONE container —
    no concurrent manifest writers — then build BTREE/BITMAP indexes once."""
    import datetime as dt
    import os.path

    import duckdb

    cfg = _resolve_dataset(dataset)
    dskey = dataset.strip().lower()
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = 0
    rows_written = 0
    status = "error"
    error: str | None = None
    write_mode = "overwrite+append"
    processed: list[str] = []

    try:
        so = _r2_storage_options()
        s3 = _s3_client()
        parts = _list_landing(s3, cfg["backfile_re"])
        if not parts:
            raise RuntimeError(f"no backfile parts match {cfg['backfile_re']} under {LANDING_PREFIX}")
        print(f"[{dskey}] {len(parts)} backfile part(s): {[p.rsplit('/',1)[-1] for p in parts]}")

        for idx, key in enumerate(parts):
            base = key.rsplit("/", 1)[-1]
            zip_path = os.path.join(SCRATCH_DIR, base)
            ndjson_path = os.path.join(SCRATCH_DIR, base + ".ndjson")
            s3.download_file(BUCKET, key, zip_path)          # Python: I/O only
            n = _zip_to_ndjson(zip_path, cfg, ndjson_path)   # Python: lossless XML→NDJSON
            rows += n

            con = duckdb.connect(":memory:")
            try:
                con.execute("PRAGMA threads=4;")
                projected = _project_dedup(con, ndjson_path, cfg, base)  # unique key per part
                reader = projected.to_arrow_reader(batch_size=131072)   # stream → flat RSS
                _write_dataset(reader, cfg["lance_uri"],
                               mode=("overwrite" if idx == 0 else "append"),
                               so=so, schema=reader.schema)
            finally:
                con.close()
            print(f"[{dskey}] part {idx + 1}/{len(parts)} {base}: {n:,} records → "
                  f"{'overwrite' if idx == 0 else 'append'}")
            processed.append(base)
            for p in (zip_path, ndjson_path):
                try:
                    os.remove(p)
                except OSError:
                    pass

        import lance
        rows_written = lance.dataset(cfg["lance_uri"], storage_options=so).count_rows()
        print(f"[{dskey}] backfile committed — {rows_written:,} rows. Building indexes…")
        _create_indexes(cfg, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(dskey, cfg["feed"], "backfile", write_mode, cfg["lance_uri"], "backfile",
                    processed, len(processed), int(rows), int(rows_written), status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows_written), "feed": cfg["feed"], "dataset": dskey,
            "run_mode": "backfile", "dataset_uri": cfg["lance_uri"], "as_of": "backfile",
        })

    if status != "success":
        raise RuntimeError(f"uspto_tm backfile failed for {dskey}: {error}")
    return {"feed": cfg["feed"], "dataset": dskey, "run_mode": "backfile",
            "parts": len(processed), "rows_processed": int(rows), "rows": int(rows_written),
            "dataset_uri": cfg["lance_uri"], "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_delta(dataset: str, key: str | None = None,
                 trigger_callback_url: str | None = None) -> dict:
    """Daily delta UPSERT: stream one daily file → DuckDB typed projection → dedup to the
    latest snapshot per key → Lance merge_insert (when_matched_update_all /
    when_not_matched_insert_all). Folds pending→registered→dead state changes in place. If
    the dataset does not yet exist, falls back to an initial overwrite + index build."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    cfg = _resolve_dataset(dataset)
    dskey = dataset.strip().lower()
    mk = cfg["merge_key"]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = 0
    rows_upserted = 0
    status = "error"
    error: str | None = None
    write_mode = "merge_insert"
    base = None
    as_of = None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        if not key:
            deltas = _list_landing(s3, cfg["delta_re"])
            if not deltas:
                raise RuntimeError(f"no delta file matches {cfg['delta_re']} under {LANDING_PREFIX}")
            key = deltas[-1]   # most-recent date stamp
        if "/" not in key:
            key = LANDING_PREFIX + key
        base = key.rsplit("/", 1)[-1]
        as_of = _as_of_from_key(key)
        print(f"[{dskey}] delta {base} (as_of={as_of}) → merge_insert on {mk}")

        zip_path = os.path.join(SCRATCH_DIR, base)
        ndjson_path = os.path.join(SCRATCH_DIR, base + ".ndjson")
        s3.download_file(BUCKET, key, zip_path)
        rows = _zip_to_ndjson(zip_path, cfg, ndjson_path)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            # Project + collapse to one row per merge key (latest snapshot wins).
            deduped = _project_dedup(con, ndjson_path, cfg, base).to_arrow_table()
        finally:
            con.close()

        if deduped.num_rows == 0:
            # Empty daily file (common for assignments/TTAB on quiet days) — nothing to upsert.
            print(f"[{dskey}] {base} carries 0 records; no-op merge.")
            rows_written = (lance.dataset(cfg["lance_uri"], storage_options=so).count_rows()
                            if _dataset_exists(cfg["lance_uri"], so) else 0)
            write_mode = "noop"
            status = "success"
            return {"feed": cfg["feed"], "dataset": dskey, "run_mode": "delta", "as_of": as_of,
                    "source_file": base, "rows_processed": int(rows), "rows_upserted": 0,
                    "dataset_uri": cfg["lance_uri"], "status": status}

        if not _dataset_exists(cfg["lance_uri"], so):
            print(f"[{dskey}] dataset absent — initial create from delta ({deduped.num_rows:,} rows)")
            _write_dataset(deduped, cfg["lance_uri"], mode="overwrite", so=so)
            write_mode = "create"
            _create_indexes(cfg, so)
        else:
            ds = lance.dataset(cfg["lance_uri"], storage_options=so)
            (ds.merge_insert(mk)
               .when_matched_update_all()
               .when_not_matched_insert_all()
               .execute(deduped))
            _optimize_indices(cfg["lance_uri"], so)
        rows_upserted = deduped.num_rows
        rows_written = lance.dataset(cfg["lance_uri"], storage_options=so).count_rows()
        print(f"[{dskey}] {write_mode}: {rows_upserted:,} rows upserted; dataset now {rows_written:,} rows")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
        rows_written = 0
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(dskey, cfg["feed"], "delta", write_mode, cfg["lance_uri"], as_of,
                    [base] if base else [], 1 if base else 0, int(rows), int(rows_upserted),
                    status, error, started_at, completed_at)
        _post_callback(trigger_callback_url, {
            "status": status, "rows": int(rows_upserted), "feed": cfg["feed"], "dataset": dskey,
            "run_mode": "delta", "dataset_uri": cfg["lance_uri"], "as_of": as_of,
        })

    if status != "success":
        raise RuntimeError(f"uspto_tm delta failed for {dskey} ({base}): {error}")
    return {"feed": cfg["feed"], "dataset": dskey, "run_mode": "delta", "as_of": as_of,
            "source_file": base, "rows_processed": int(rows), "rows_upserted": int(rows_upserted),
            "dataset_uri": cfg["lance_uri"], "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def reindex(dataset: str) -> dict:
    """Rebuild the BTREE/BITMAP scalar indexes on an existing dataset (no re-ingest)."""
    import lance

    cfg = _resolve_dataset(dataset)
    so = _r2_storage_options()
    rows = lance.dataset(cfg["lance_uri"], storage_options=so).count_rows()
    print(f"Reindexing {cfg['lance_uri']} — {rows:,} rows")
    _create_indexes(cfg, so)
    return {"dataset": dataset.strip().lower(), "dataset_uri": cfg["lance_uri"], "rows": rows,
            "btree": cfg["btree"], "bitmap": cfg["bitmap"]}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_migration() -> dict:
    """Create ops.uspto_tm_runs (idempotent). Mirrors ops_uspto_tm_runs.sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.uspto_tm_runs')")
        present = cur.fetchone()[0]
    print(f"ops.uspto_tm_runs present = {present}")
    return {"table": "ops.uspto_tm_runs", "present": present}


# ──────────────────────────────────────────────────────────────────────────────
# Manual ops entrypoints (local — no callback). ops.* write still fires.
# ──────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def migrate() -> None:
    import json
    print(json.dumps(apply_migration.remote(), indent=2, default=str))


@app.local_entrypoint()
def backfill(dataset: str = "applications") -> None:
    import json
    print(json.dumps(ingest_backfile.remote(dataset, trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def delta(dataset: str = "applications", key: str = "") -> None:
    import json
    print(json.dumps(ingest_delta.remote(dataset, key=(key or None), trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def reindex_one(dataset: str = "applications") -> None:
    import json
    print(json.dumps(reindex.remote(dataset), indent=2, default=str))
