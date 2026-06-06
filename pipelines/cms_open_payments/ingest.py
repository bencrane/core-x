"""Compute worker — CMS Open Payments bulk ingest (2018 → present, all program years).

Part of the ``cms-open-payments-pipelines`` Modal app. Endpoint-less functions, spawned
by the Universal Dispatcher (core/modal_dispatcher.py) or driven by the local entrypoints.
Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the transform, the
Lance dataset is staged on local disk and published to R2 via boto3 (uniform-part
multipart — Cloudflare R2 rejects the variable-size parts a direct Lance→R2 write emits).

The catalog is the CMS CKAN metastore (a DKAN dataset/items list). Three DETAIL dataset
families, each accumulated across every program year the catalog advertises, into three
DISTINCT Lance datasets keyed on the NPI resolution key:

    {YEAR} General Payment Data    (OP_DTL_GNRL_*)    -> s3://data-sink/active/cms_general_payments/
    {YEAR} Research Payment Data    (OP_DTL_RSRCH_*)   -> s3://data-sink/active/cms_research_payments/
    {YEAR} Ownership Payment Data   (OP_DTL_OWNRSHP_*)  -> s3://data-sink/active/cms_ownership/

Reconnaissance (verified live against the metastore + the 2018 / 2023 / 2024 payloads):
  1. CATALOG — GET …/metastore/schemas/dataset/items returns a flat list. The DETAIL feeds
     are exactly the items whose title matches ``^{YEAR} (General|Research|Ownership)
     Payment Data$`` (this title gate is what excludes the dozens of "grouped by …" /
     "state payment totals …" AGGREGATE items that share the same theme). Each carries
     ``theme[0].data`` ∈ {General,Research,Ownership} Payments and ``keyword[0].data`` =
     the 4-digit year; the file URL is ``distribution[0].data.downloadURL``. The catalog
     currently advertises program years 2018-2024 (21 detail datasets = 7 yr × 3 fam) — the
     worker reads whatever the catalog returns, so new years are picked up automatically.
  2. PAYLOAD — each downloadURL is a DIRECT, uncompressed UTF-8 CSV (HTTP 200, no redirect,
     no ZIP). General 2023 alone is ~8.2 GB; Research ~1 GB; Ownership tens of MB. A ZIP
     fallback is retained (magic-byte sniff) for robustness against a future packaging
     change, but the live path is plain CSV.
  3. DIALECT — comma-delimited, RFC-4180 quoting (embedded commas + ""-quoted empties,
     e.g. ``"Minerva Surgical, Inc"``). Read all_varchar; every projected field trimmed.
  4. SCHEMA — STABLE across years within a family (CMS re-publishes every year under the
     current data dictionary, stamped one publication date → zero cross-year drift), but
     WIDE and DIVERGENT across families: General 91 cols, Research 252 (5 repeating
     Principal_Investigator_* groups), Ownership 30. The projection is therefore built
     DYNAMICALLY from each file's actual header (full-fidelity: every column preserved as
     trimmed VARCHAR, snake_cased; a small typed-column allow-list gets DATE / DECIMAL
     casts) rather than hand-enumerated — robust to any family and any future column.
  5. KEYS — the NPI is the central join key and stays VARCHAR (10-digit identifier; a
     numeric cast risks leading-zero / precision loss). General + Research expose
     ``covered_recipient_npi``; Ownership exposes ``physician_npi`` (no covered-recipient
     concept). Ownership has NO ``date_of_payment`` (an ownership interest is not a dated
     payment) — its index plan substitutes accordingly.
  6. TYPES — ``date_of_payment`` / ``payment_publication_date`` are MM/DD/YYYY → DATE;
     money (``total_amount_of_payment_usdollars``, ``total_amount_invested_usdollars``,
     ``value_of_interest``) is plain decimal → DECIMAL(14,2); ``number_of_payments_…`` →
     INTEGER. Everything else (NPIs, profile/record/payment IDs, codes) stays VARCHAR.

Topology — SINGLE sequential orchestrator (``refresh_all``), the directive's model:
  one container. Per family, for each year in catalog order:
      download CSV → /tmp (UTF-8-sanitised stream)  [Python: I/O only, no transform]
        → DuckDB read_csv(all_varchar, quote-aware, parallel=false) → dynamic project/cast
        → to_arrow_reader (streaming; never materialise the 8 GB General file)
        → Lance append to the LOCAL family dataset (v2.1)
        → rm the local CSV   [bounded disk: one CSV at a time, never concurrent]
  GIANTS (general) stage on a Modal Volume (ARCHITECTURE.md "Giants — Volume-staged,
  append-only"): off ephemeral_disk (so the job is NOT forced onto preemptible spot capacity)
  and CHECKPOINTED per landed year (vol.commit), so an interrupted backfill resumes
  (resume=True) and re-ingests only the missing years instead of restarting from 2018. Small
  families stage on ephemeral scratch. After a family's years land, build its BTREE + BITMAP
  scalar indexes ONCE, then PUBLISH to R2 via boto3 (uniform-part multipart). Direct Lance→R2
  writes are NOT used: object_store emits variable-size multipart parts, which R2 rejects ("all
  non-trailing parts must have the same length"); boto3 does not. The publish is HARDENED: a
  from-scratch rebuild stages to a sibling __staging prefix, verifies object set + per-file
  sizes == local, then swaps over the live prefix (manifest uploaded LAST) — the live copy is
  never wiped before the replacement is staged + verified; staged-from-R2 ops (reindex /
  single-year) publish append-only (only new files, never re-pushing the multi-GB data tree).
  Every publish path then passes a READ-BACK VERIFY GATE (_verify_published): the dataset is
  reopened FRESH from R2 and its row + index counts asserted before success is recorded — the
  check whose absence let run #58 record a 'success' over a corpse. See _publish_full_swap /
  _publish_incremental / _verify_published. A full refresh rebuilds each family cleanly.

Control plane (Trigger v4 durable callback): ``refresh_all`` accepts ``trigger_callback_url``
and, on terminal state, (1) writes per-unit run rows to ``ops.cms_open_payments_runs`` via
psycopg and (2) POSTs a FLAT JSON summary to that url. Scheduled QUARTERLY by
src/trigger/cms_open_payments.ts to catch CMS's annual publish + rolling late submissions.

    modal deploy pipelines/cms_open_payments/ingest.py
    modal run    pipelines/cms_open_payments/ingest.py::init_state            # create ops.* ledger
    modal run    pipelines/cms_open_payments/ingest.py::discover              # verify catalog URL parsing
    modal run    pipelines/cms_open_payments/ingest.py::discover --only-year 2023
    modal run    pipelines/cms_open_payments/ingest.py::backfill              # full historical backfill
    modal run    pipelines/cms_open_payments/ingest.py::backfill --only-family general --only-year 2023
    modal run    pipelines/cms_open_payments/ingest.py::ingest_one --family research --year 2024
    modal run    pipelines/cms_open_payments/ingest.py::reindex --family general
    modal run    pipelines/cms_open_payments/ingest.py::show_ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"

# CKAN/DKAN metastore catalog. ``show-reference-ids=false`` makes DKAN expand the
# distribution references inline (distribution[].data.downloadURL); the bare endpoint
# returns a flatter shape (distribution[].downloadURL). _download_urls tolerates BOTH, so
# this param is for determinism, not correctness. Overridable so the resolver is not
# pinned to one host.
METASTORE_URL = os.environ.get(
    "CMS_OP_METASTORE_URL",
    "https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items"
    "?show-reference-ids=false",
)

SCRATCH_DIR = "/tmp/cms_open_payments"

# Giant staging (ARCHITECTURE.md "Giants — Volume-staged, append-only"). General's ~15 GiB
# dataset stages on a Modal Volume, NOT a large ephemeral_disk (a large ephemeral_disk request
# is what forces a worker onto preemptible spot capacity — the documented cause of giant-job
# preemption). The Volume also PERSISTS landed years across a container restart, so an
# interrupted backfill resumes (refresh_all resume=True) instead of restarting from 2018 —
# the recovery the partial-publish corpse and the run #59-64 mid-giant death both needed.
# Small families (research/ownership) stay on ephemeral scratch (fast, nothing to resume).
cms_vol = modal.Volume.from_name("cms-op-staging", create_if_missing=True)
VOL_DIR = "/vol"
_RESUMABLE_FAMILIES = {"general"}  # giants → Volume-staged + per-year resume

# Lance fragment sizing (fleet constants). max_rows_per_file = the Lance default;
# max_bytes_per_file = 90 GiB (the documented Lance default; every other worker uses it).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new datasets → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

FEED = "cms_open_payments"

# ── Family registry. The single source of truth for catalog classification, the Lance
#    dataset URI (env-overridable), and the per-family scalar-index plan. ──────────────
#  • title_word / theme — the two corroborating catalog signals used to classify an item.
#  • url_token — the OP_DTL_* filename token, asserted on the resolved downloadURL as a
#    defensive cross-check that the catalog link matches the family it was filed under.
#  • npi_col — the family's NPI resolution key (General/Research: covered_recipient_npi;
#    Ownership: physician_npi). The directive's central join key, per family.
#  • btree — high-cardinality resolution / join keys. The directive mandates BTREE on
#    covered_recipient_npi, applicable_manufacturer_or_applicable_gpo_making_payment_id,
#    and date_of_payment; record_id (the row PK) and principal_investigator_1_npi are
#    load-bearing resolution keys added per the ARCHITECTURE BTREE mandate. Ownership has
#    no date_of_payment, so it indexes physician_npi + payment id + record_id.
#  • bitmap — genuinely low-cardinality categoricals (house style: every fleet worker
#    pairs BTREE resolution keys with BITMAP categoricals). Index build is best-effort per
#    column, so a column absent in some year is logged and skipped, never fatal.
FAMILIES: dict[str, dict] = {
    "general": {
        "label": "General",
        "theme": "General Payments",
        "title_word": "General",
        "url_token": "OP_DTL_GNRL_",
        "uri": os.environ.get(
            "CMS_GENERAL_LANCE_URI", "s3://data-sink/active/cms_general_payments/"
        ),
        "npi_col": "covered_recipient_npi",
        "btree": [
            "covered_recipient_npi",
            "applicable_manufacturer_or_applicable_gpo_making_payment_id",
            "date_of_payment",
            "record_id",
        ],
        "bitmap": [
            "payment_year",
            "covered_recipient_type",
            "nature_of_payment_or_transfer_of_value",
            "form_of_payment_or_transfer_of_value",
            "recipient_state",
            "dispute_status_for_publication",
        ],
    },
    "research": {
        "label": "Research",
        "theme": "Research Payments",
        "title_word": "Research",
        "url_token": "OP_DTL_RSRCH_",
        "uri": os.environ.get(
            "CMS_RESEARCH_LANCE_URI", "s3://data-sink/active/cms_research_payments/"
        ),
        "npi_col": "covered_recipient_npi",
        "btree": [
            "covered_recipient_npi",
            "applicable_manufacturer_or_applicable_gpo_making_payment_id",
            "date_of_payment",
            "principal_investigator_1_npi",
            "record_id",
        ],
        "bitmap": [
            "payment_year",
            "covered_recipient_type",
            "related_product_indicator",
            "recipient_state",
            "dispute_status_for_publication",
        ],
    },
    "ownership": {
        "label": "Ownership",
        "theme": "Ownership Payments",
        "title_word": "Ownership",
        "url_token": "OP_DTL_OWNRSHP_",
        "uri": os.environ.get("CMS_OWNERSHIP_LANCE_URI", "s3://data-sink/active/cms_ownership/"),
        "npi_col": "physician_npi",
        "btree": [
            "physician_npi",
            "applicable_manufacturer_or_applicable_gpo_making_payment_id",
            "record_id",
        ],
        "bitmap": [
            "payment_year",
            "physician_primary_type",
            "recipient_state",
            "dispute_status_for_publication",
            "interest_held_by_physician_or_an_immediate_family_member",
        ],
    },
}

# Typed-column allow-list (matched on the snake_cased alias). Everything not listed is
# preserved as a trimmed VARCHAR. CMS dates are MM/DD/YYYY; money is plain decimal.
_DATE_COLS = {"date_of_payment", "payment_publication_date"}
_MONEY_COLS = {
    "total_amount_of_payment_usdollars",
    "total_amount_invested_usdollars",
    "value_of_interest",
}
_INT_COLS = {"number_of_payments_included_in_total_amount"}

# read_csv options — quote-aware (RFC-4180), all_varchar (zero type-inference surprises on
# a 91-252 col government CSV), malformed rows quarantined to the rejects table rather than
# aborting an 8 GB load. null_padding tolerates short trailing rows.
#   parallel = false: MANDATORY. CMS detail files contain embedded newlines inside quoted
#   fields (e.g. General 2022 at line 12.26M), and DuckDB's parallel CSV scanner rejects
#   null_padding in conjunction with quoted newlines ("does not support null_padding in
#   conjunction with quoted new lines"). Single-threaded scan handles both; slower on the
#   8 GB General files but correct (these are quarterly batch jobs — correctness > speed).
READ_OPTS = (
    "all_varchar = true, header = true, delim = ',', quote = '\"', escape = '\"', "
    "sample_size = -1, ignore_errors = true, null_padding = true, store_rejects = true, "
    "parallel = false"
)
# DESCRIBE needs only the schema; drop store_rejects (it would allocate a rejects table).
DESCRIBE_OPTS = (
    "all_varchar = true, header = true, delim = ',', quote = '\"', escape = '\"', "
    "sample_size = -1, ignore_errors = true, null_padding = true, parallel = false"
)

# Mirrored verbatim by pipelines/cms_open_payments/ops_cms_open_payments_runs.sql.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.cms_open_payments_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    phase          text        NOT NULL,
    family         text,
    dataset_uri    text,
    payment_year   smallint,
    source_file    text,
    source_url     text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cms_op_runs_family_idx      ON ops.cms_open_payments_runs (family);
CREATE INDEX IF NOT EXISTS cms_op_runs_year_idx        ON ops.cms_open_payments_runs (payment_year);
CREATE INDEX IF NOT EXISTS cms_op_runs_phase_idx       ON ops.cms_open_payments_runs (phase);
CREATE INDEX IF NOT EXISTS cms_op_runs_status_idx      ON ops.cms_open_payments_runs (status);
CREATE INDEX IF NOT EXISTS cms_op_runs_recorded_at_idx ON ops.cms_open_payments_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_reader; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",           # R2 dataset publish (boto3 = uniform-part multipart, R2-compliant)
    "requests>=2.32",        # CMS download + metastore + Trigger waitpoint callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    {"LANCE_BYPASS_SPILLING": "true"}  # in-memory BTREE sort (lance-format/lance#2650)
)

app = modal.App("cms-open-payments-pipelines", image=image)


# ─────────────────────────── catalog parsing (pure, testable) ───────────────────────────
import re as _re

_TITLE_RE = _re.compile(r"^\s*(\d{4})\s+(General|Research|Ownership)\s+Payment\s+Data\s*$")
_WORD_TO_FAMILY = {cfg["title_word"]: key for key, cfg in FAMILIES.items()}


def _first_ref_data(values) -> list:
    """DKAN reference fields (theme/keyword) arrive either as ``[{identifier, data}, …]``
    (reference-expanded) or as bare scalars. Return the inner values either way."""
    out = []
    for v in values or []:
        if isinstance(v, dict) and "data" in v:
            out.append(v["data"])
        elif isinstance(v, str):
            out.append(v)
    return out


def _download_urls(item: dict) -> list[str]:
    """Pull every distribution downloadURL from a metastore item, tolerant of BOTH DKAN
    response shapes: reference-expanded (``distribution[].data.downloadURL``, returned with
    show-reference-ids=false) and flat (``distribution[].downloadURL``, the bare endpoint)."""
    urls = []
    for dist in item.get("distribution", []) or []:
        if not isinstance(dist, dict):
            continue
        data = dist.get("data")
        url = data.get("downloadURL") if isinstance(data, dict) else None
        if not url:
            url = dist.get("downloadURL")  # flat shape
        if url:
            urls.append(url)
    return urls


def _parse_metastore_items(
    items: list, only_family: str | None = None, only_year: int | None = None
) -> list[dict]:
    """Pure catalog → ingest-unit resolver (NO network; unit-testable against a saved
    items list). Isolates the DETAIL feeds via the title gate, classifies each by family,
    extracts the year and the CSV downloadURL, and cross-checks the family against the
    item's ``theme`` and the resolved URL's OP_DTL_* token.

    Returns a deterministic, sorted list of ``{family, label, year, url, source_file,
    dataset_uri, title}`` — one per (family, year). Raises on an item that passes the
    title gate but yields no/ambiguous downloadURL (a catalog regression we must see, not
    silently skip)."""
    units: dict[tuple[str, int], dict] = {}
    for item in items:
        title = (item.get("title") or "").strip()
        m = _TITLE_RE.match(title)
        if not m:
            continue  # aggregate / profile / summary item — not a detail dataset
        year = int(m.group(1))
        family = _WORD_TO_FAMILY[m.group(2)]
        cfg = FAMILIES[family]

        # Corroborate the title classification with the catalog theme (advisory: warn,
        # don't fail — the title gate is authoritative, theme is a sanity check).
        themes = _first_ref_data(item.get("theme"))
        if themes and cfg["theme"] not in themes:
            print(f"WARN: {title!r} themes={themes} disagree with family {family!r}; trusting title")

        urls = _download_urls(item)
        csv_urls = [u for u in urls if u.lower().endswith((".csv", ".zip"))] or urls
        if len(csv_urls) != 1:
            raise RuntimeError(
                f"{title!r}: expected exactly 1 downloadURL, got {len(csv_urls)}: {csv_urls}"
            )
        url = csv_urls[0]
        if cfg["url_token"] not in url:
            print(f"WARN: {title!r} url {url} missing expected token {cfg['url_token']!r}")

        if only_family and family != only_family:
            continue
        if only_year and year != only_year:
            continue

        key = (family, year)
        if key in units:
            raise RuntimeError(f"duplicate catalog entry for {family} {year}: {title!r}")
        units[key] = {
            "family": family,
            "label": cfg["label"],
            "year": year,
            "url": url,
            "source_file": url.rsplit("/", 1)[-1],
            "dataset_uri": cfg["uri"],
            "title": title,
        }
    # Deterministic order: family (registry order) then year ascending.
    fam_order = {k: i for i, k in enumerate(FAMILIES)}
    return sorted(units.values(), key=lambda u: (fam_order[u["family"]], u["year"]))


def _fetch_metastore() -> list:
    """GET the metastore items list. Network I/O only."""
    import requests

    resp = requests.get(METASTORE_URL, timeout=(30, 180))
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"metastore returned {type(data).__name__}, expected a list")
    return data


def _resolve_units(only_family: str | None, only_year: int | None) -> list[dict]:
    if only_family and only_family not in FAMILIES:
        raise ValueError(f"family must be one of {sorted(FAMILIES)}, got {only_family!r}")
    return _parse_metastore_items(_fetch_metastore(), only_family, only_year)


# ───────────────────────────── R2 / object-store config ─────────────────────────────
# Lance reads/writes are LOCAL (no storage_options) — the dataset is staged on the
# container's ephemeral disk and published to R2 via boto3. boto3/s3transfer uses uniform
# multipart part sizes, which Cloudflare R2 requires ("all non-trailing parts must have the
# same length"); Lance's object_store writer does NOT, so a DIRECT multi-GB Lance→R2 write
# fails with InvalidPart. This is the proven fleet pattern (PDL, FMCSA, SAM entity-reg).
def _r2_storage_options() -> dict[str, str]:
    """R2 endpoint + AWS-style creds from the Modal secret, consumed by the boto3 client.
    Endpoint supplied directly (R2_ENDPOINT) or derived from R2_ACCOUNT_ID."""
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
    """boto3 S3 client for R2. checksum behaviour forced to ``when_required`` (R2 semantics);
    path-style addressing — the directive's ``virtual_hosted_style_request=false`` translated
    to the boto3 world (the default for custom endpoints, set explicitly)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required",
                 retries={"max_attempts": 5, "mode": "standard"},  # underlying transient-error retry
                 s3={"addressing_style": "path"})
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _family_prefix(family: str) -> str:
    """R2 key prefix for a family dataset, derived from its s3:// URI (bucket stripped)."""
    return FAMILIES[family]["uri"].split(f"s3://{BUCKET}/", 1)[1]


def _local_ds(family: str, *, on_volume: bool = False) -> str:
    """Local Lance staging directory for a family (accumulated across years, then published).
    Giants stage on the Modal Volume (durable across restarts → resumable); small families
    stage on ephemeral scratch. on_volume defaults False so every existing caller is unchanged."""
    base = VOL_DIR if on_volume else SCRATCH_DIR
    return os.path.join(base, f"{family}_lance")


def _dataset_exists(local_ds: str) -> bool:
    """True if a readable Lance dataset (a committed manifest) exists at local_ds."""
    import lance

    try:
        lance.dataset(local_ds)
        return True
    except Exception:  # noqa: BLE001 — absent / no manifest ⇒ does not exist yet
        return False


def _landed_years(local_ds: str, years: list[int]) -> set[int]:
    """Subset of `years` already present (row count > 0) in the local dataset — the resume
    skip-set. Empty when no dataset exists. A per-year count (not a bare existence check) so a
    half-written year is NOT treated as complete: only years with committed rows are 'landed',
    and anything else is re-ingested (the per-year delete+append is idempotent)."""
    import lance

    if not _dataset_exists(local_ds):
        return set()
    ds = lance.dataset(local_ds)
    out: set[int] = set()
    for y in years:
        try:
            if ds.count_rows(filter=f"payment_year = {int(y)}") > 0:
                out.add(int(y))
        except Exception:  # noqa: BLE001 — unreadable year ⇒ re-ingest it
            pass
    return out


# ── Hardened R2 publish (root-cause fix for the general partial-publish corpse) ──────────
# The retired ``_replace_r2_prefix`` wiped the live prefix, then re-uploaded every data file
# with NO retry and NO atomic swap. On general's 16 GiB / ~83-file giant a single transient
# R2 error mid-upload (run #58, file ~72) left the prefix as orphaned ``data/*.lance``
# fragments with zero ``_versions/`` manifests — unreadable AND unrecoverable, because the
# wipe had already destroyed the prior good copy. Two publish modes replace it, per
# ARCHITECTURE.md "Giants — Volume-staged, append-only":
#   • _publish_incremental — for a local dataset STAGED FROM R2 (reindex, single-year).
#     Uploads ONLY files absent/size-changed vs R2, manifest LAST, never wipes. This is the
#     giant rule verbatim ("upload only the new files … never wipe or re-upload data files").
#   • _publish_full_swap — for a FROM-SCRATCH rebuild (refresh_all). Stages the whole new
#     dataset to a sibling ``__staging`` prefix (resumable + retried), VERIFIES object set +
#     per-file sizes == local, then swaps over the live prefix (delete-old → server-side copy
#     in dependency order, manifest LAST → verify). Live is never wiped before the
#     replacement is fully staged and verified; worst case on failure is a briefly-unreadable
#     (never corrupt) prefix a re-run reconciles — the inverse of the retired corpse.
_R2_PUBLISH_ATTEMPTS = 4           # outer per-file retry (botocore also retries internally)
_R2_RETRY_BACKOFF_S = 8            # linear backoff base → 8s, 16s, 24s between attempts
_R2_SINGLE_COPY_MAX = 5 * 1024**3  # R2/S3 single-request CopyObject ceiling (5 GiB)


def _staging_prefix(live_prefix: str) -> str:
    """Sibling staging prefix for the swap publish: ``active/cms_general_payments/`` →
    ``active/cms_general_payments__staging/``. The ``__`` guarantees staging keys do NOT
    fall under the live prefix, so a list/read of the live dataset never sees a half-staged
    copy, and listing live never enumerates staging."""
    return live_prefix.rstrip("/") + "__staging/"


def _upload_rank(rel: str) -> int:
    """Dependency-order key for upload/copy. A Lance manifest references its data, deletion,
    index, and transaction files, so those MUST be written before the manifest — else a
    reader can observe a manifest pointing at absent data (the exact corruption we eliminate).
    Lower rank is written first; ``_versions/`` (the manifest) is written LAST."""
    if rel.startswith("_versions/"):
        return 4
    if rel.startswith("_transactions/"):
        return 3
    if rel.startswith("_indices/"):
        return 2
    return 1  # data/, _deletions/, and anything else the manifest depends on


def _ordered_rels(rels) -> list[str]:
    """Deterministic dependency order: by upload rank, then path."""
    return sorted(rels, key=lambda r: (_upload_rank(r), r))


def _list_prefix_sizes(s3, prefix: str) -> dict[str, int]:
    """``{relative_path: size_bytes}`` for every object under an R2 prefix. R2 is strongly
    read-after-write consistent, so this is a reliable post-upload verification source."""
    out: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if rel:
                out[rel] = o["Size"]
    return out


def _local_file_sizes(local_dir: str) -> dict[str, int]:
    """``{relative_path: size_bytes}`` for every file in the local Lance dataset."""
    out: dict[str, int] = {}
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            out[rel] = os.path.getsize(lp)
    return out


def _upload_file_retry(s3, local_path: str, key: str) -> None:
    """Upload one file to R2 with bounded linear backoff. The transient R2 error that aborted
    run #58 mid-upload (and corrupted general) is precisely what this recovers."""
    import time

    last: Exception | None = None
    for attempt in range(1, _R2_PUBLISH_ATTEMPTS + 1):
        try:
            s3.upload_file(local_path, BUCKET, key)
            return
        except Exception as exc:  # noqa: BLE001 — transient R2/network; retried below
            last = exc
            print(f"    upload {key} attempt {attempt}/{_R2_PUBLISH_ATTEMPTS} failed: {str(exc)[:200]}")
            if attempt < _R2_PUBLISH_ATTEMPTS:
                time.sleep(_R2_RETRY_BACKOFF_S * attempt)
    raise RuntimeError(f"upload failed after {_R2_PUBLISH_ATTEMPTS} attempts: {key}: {last}")


def _copy_key_retry(s3, src_key: str, dst_key: str) -> None:
    """Server-side copy one object within R2 (no worker egress) with bounded backoff. Used
    by the swap step so the verified staging bytes become live without re-transferring GiBs
    from the worker."""
    import time

    last: Exception | None = None
    for attempt in range(1, _R2_PUBLISH_ATTEMPTS + 1):
        try:
            s3.copy_object(Bucket=BUCKET, Key=dst_key,
                           CopySource={"Bucket": BUCKET, "Key": src_key})
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            print(f"    copy {dst_key} attempt {attempt}/{_R2_PUBLISH_ATTEMPTS} failed: {str(exc)[:200]}")
            if attempt < _R2_PUBLISH_ATTEMPTS:
                time.sleep(_R2_RETRY_BACKOFF_S * attempt)
    raise RuntimeError(f"copy failed after {_R2_PUBLISH_ATTEMPTS} attempts: {src_key} → {dst_key}: {last}")


def _delete_keys(s3, keys: list[str]) -> int:
    """Batched delete of explicit R2 keys (≤1000 per request). Returns objects deleted."""
    n = 0
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i:i + 1000]]
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
        n += len(batch)
    return n


def _delete_prefix(s3, prefix: str) -> int:
    """Delete every object under an R2 prefix (batched). Returns objects deleted."""
    keys: list[str] = []
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
            if len(keys) == 1000:
                n += _delete_keys(s3, keys)
                keys = []
    if keys:
        n += _delete_keys(s3, keys)
    return n


def _publish_incremental(s3, live_prefix: str, local_dir: str) -> int:
    """Append-only publish for a local dataset STAGED FROM R2 (reindex / single-year update).
    Uploads only files absent-from or size-changed-vs R2 — in dependency order, manifest LAST
    — and NEVER deletes. Lance files are immutable per version, so a size match means
    identical content (skip); the only new files are the freshly written index dirs, deletion
    vectors, transactions, and the new manifest. This is the ARCHITECTURE.md giant rule —
    'upload only the new files … never wipe or re-upload data files' — and avoids re-pushing
    the multi-GB data tree every cycle. Superseded index/manifest objects remain as harmless
    orphans (pruned out-of-band), exactly as the federal-spine staged index campaign does.
    Returns files uploaded."""
    local_sizes = _local_file_sizes(local_dir)
    if not local_sizes:
        raise RuntimeError(f"refusing to publish empty dataset to {live_prefix}")
    r2_before = _list_prefix_sizes(s3, live_prefix)
    uploaded = 0
    for rel in _ordered_rels(local_sizes):
        if r2_before.get(rel) == local_sizes[rel]:
            continue  # already in R2, byte-identical
        _upload_file_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), live_prefix + rel)
        uploaded += 1
    print(f"  published {uploaded} new/changed files (append-only) → s3://{BUCKET}/{live_prefix} "
          f"({len(local_sizes) - uploaded} reused)")
    return uploaded


def _publish_full_swap(s3, live_prefix: str, local_dir: str) -> int:
    """Non-destructive full replace for a FROM-SCRATCH rebuild (refresh_all). The local
    dataset is a brand-new Lance lineage (fresh fragment UUIDs, version chain restarted at 1),
    so it cannot be diffed against the live copy — it must wholesale replace it. Instead of
    wiping-then-uploading (which left the corpse on any mid-upload error), it stages then
    swaps:

      1. Resumable upload local → ``__staging`` sibling: skip files already staged
         byte-identically (a re-run after a transient failure resumes rather than restarts),
         retry each with backoff, manifest LAST.
      2. Prune stale ``__staging`` debris from a prior aborted lineage.
      3. VERIFY: staging object set + per-file sizes == local, exactly. Live is STILL
         UNTOUCHED — a failure here aborts with the live copy fully intact.
      4. SWAP: delete the old live objects, then server-side copy staging → live in
         dependency order (manifest LAST, so live never exposes a manifest before its data),
         then verify live == local.
      5. Best-effort delete of staging (correctness does not depend on it).

    Worst case on failure is a briefly-unreadable (never corrupt) live prefix a re-run makes
    consistent — the inverse of the retired primitive's permanent corpse. Returns files
    published."""
    local_sizes = _local_file_sizes(local_dir)
    if not local_sizes:
        raise RuntimeError(f"refusing to publish empty dataset to {live_prefix}")
    staging = _staging_prefix(live_prefix)
    ordered = _ordered_rels(local_sizes)

    # 1. Resumable staged upload (live untouched throughout this step).
    staged_before = _list_prefix_sizes(s3, staging)
    up = reused = 0
    for rel in ordered:
        if staged_before.get(rel) == local_sizes[rel]:
            reused += 1
            continue
        _upload_file_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), staging + rel)
        up += 1
    # 2. Prune stale staging debris (older lineage / partial prior run) so verify is exact.
    stale = [staging + rel for rel in staged_before if rel not in local_sizes]
    if stale:
        _delete_keys(s3, stale)

    # 3. Verify staging == local BEFORE touching live. Abort intact on any mismatch.
    staged_after = _list_prefix_sizes(s3, staging)
    if staged_after != local_sizes:
        missing = {r: local_sizes[r] for r in local_sizes if staged_after.get(r) != local_sizes[r]}
        extra = {r: staged_after[r] for r in staged_after if r not in local_sizes}
        raise RuntimeError(
            f"staging verify failed for {staging}: LIVE LEFT INTACT. "
            f"{len(missing)} missing/size-mismatch, {len(extra)} extra. "
            f"sample_missing={dict(list(missing.items())[:5])}")
    print(f"  staged {up} uploaded + {reused} reused → {staging}; "
          f"{len(local_sizes)} files verified == local")

    # 4. Swap: delete old live, copy staging → live (ordered, manifest LAST), verify live.
    deleted = _delete_prefix(s3, live_prefix)
    print(f"  swap: deleted {deleted} old object(s) under {live_prefix}; copying {len(ordered)} from staging")
    for rel in ordered:
        if local_sizes[rel] > _R2_SINGLE_COPY_MAX:
            # Fragment exceeds R2's single-request copy ceiling — re-upload from local instead.
            _upload_file_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), live_prefix + rel)
        else:
            _copy_key_retry(s3, staging + rel, live_prefix + rel)
    live_after = _list_prefix_sizes(s3, live_prefix)
    if live_after != local_sizes:
        bad = {r: (local_sizes[r], live_after.get(r)) for r in local_sizes
               if live_after.get(r) != local_sizes[r]}
        raise RuntimeError(
            f"live verify failed after swap for {live_prefix}: re-run to reconcile. "
            f"{len(bad)} mismatched; sample={dict(list(bad.items())[:5])}")

    # 5. Best-effort staging cleanup.
    try:
        _delete_prefix(s3, staging)
    except Exception as exc:  # noqa: BLE001 — staging debris is harmless
        print(f"  WARN: staging cleanup failed (harmless): {str(exc)[:160]}")
    print(f"  published {len(local_sizes)} files (staged-swap) → s3://{BUCKET}/{live_prefix}")
    return len(local_sizes)


# ── Read-back verify gate (O1/O2) ────────────────────────────────────────────────────────
# The check whose ABSENCE let run #58 record a 'successful' publish over a corpse: after the
# swap commits, OPEN THE DATASET FRESH FROM R2 and assert it is whole (rows + full index plan
# + a BTREE point-probe) before "success" is recorded. The size-census inside _publish_full_swap
# proves the bytes match local; this proves the committed manifest actually opens and resolves.
EXPECTED_INDEX_COUNT = {"general": 10, "research": 10, "ownership": 8}
# Defensive: the explicit gate must track the registry (BTREE + BITMAP per family). A future
# index-plan edit that forgets to update this constant trips here at import, not silently in prod.
assert all(
    EXPECTED_INDEX_COUNT[f] == len(c["btree"]) + len(c["bitmap"]) for f, c in FAMILIES.items()
), "EXPECTED_INDEX_COUNT out of sync with the FAMILIES index plan"


def _verify_published(uri: str, expected_rows: int, expected_indices: int | None,
                      npi_col: str, storage_options: dict) -> dict:
    """Open the just-published dataset FRESH from R2 and assert wholeness. Raises on any
    disagreement (the caller records verify=error and leaves the published bytes for
    inspection — they are valid, this is an alarm, not corruption). R2 is strongly
    read-after-write consistent, so a successful open reflects the manifest the swap just
    committed. expected_indices=None skips the index assertion (an index-less ingest path)."""
    import lance

    ds = lance.dataset(uri, storage_options=storage_options)
    rows = ds.count_rows()
    if rows != expected_rows:
        raise RuntimeError(f"verify {uri}: R2 rows={rows:,} != expected {expected_rows:,}")
    healthy = [i for i in _list_committed_indices(ds) if "error" not in i]
    nidx = len(healthy)
    if expected_indices is not None and nidx != expected_indices:
        raise RuntimeError(
            f"verify {uri}: R2 indices={nidx} != expected {expected_indices} "
            f"(committed: {[i.get('name') for i in healthy]})")
    probe = ds.scanner(filter=f"{npi_col} IS NOT NULL", columns=[npi_col],
                       limit=1, prefilter=True).to_table().num_rows
    if probe < 1:
        raise RuntimeError(f"verify {uri}: BTREE probe on {npi_col} returned 0 rows")
    return {"rows": rows, "indices": nidx}


def _download_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Stage the committed R2 dataset back to local disk (for a single-year update or an
    in-place reindex — avoids re-downloading every year's CSV). Returns files downloaded;
    0 means no dataset exists yet at that prefix."""
    import shutil

    shutil.rmtree(local_dir, ignore_errors=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel:
                continue
            lp = os.path.join(local_dir, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(lp), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], lp)
            n += 1
    return n


# ───────────────────────────── download (Python: I/O only) ─────────────────────────────
def _sanitise_stream_to_utf8(read_chunk, out_path: str) -> int:
    """Stream bytes through an INCREMENTAL UTF-8 decoder (errors='replace') and write UTF-8
    to disk. Returns bytes written. CMS publishes UTF-8, so valid input is byte-identical
    out (lossless — accented recipient names are preserved); a stray invalid byte is
    quarantined to U+FFFD instead of aborting a multi-GB DuckDB scan. The incremental
    decoder buffers partial multibyte sequences across chunk boundaries (a naive
    per-chunk decode would corrupt characters split across reads). I/O only — the SQL
    still does 100% of the transform.

    ``read_chunk(n)`` is any callable returning up to n bytes (b'' at EOF) — an HTTP
    iter_content wrapper or a file object's .read."""
    import codecs

    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    written = 0
    with open(out_path, "w", encoding="utf-8", newline="") as out:
        while True:
            chunk = read_chunk(1 << 20)
            if not chunk:
                break
            text = decoder.decode(chunk, final=False)
            if text:
                out.write(text)
                written += len(text)
        tail = decoder.decode(b"", final=True)
        if tail:
            out.write(tail)
    return written


def _download_csv(url: str, dest_dir: str) -> str:
    """Download a CMS detail file to ``dest_dir`` and return the path to a ready-to-read
    UTF-8 CSV. Common path: a direct .csv streamed + UTF-8-sanitised in one pass. Fallback:
    if the payload is a ZIP (magic bytes ``PK\\x03\\x04`` or a .zip URL), save it, extract
    the single OP_DTL_* CSV member, sanitise that, and drop the archive. Raises on an empty
    download or a ZIP without exactly one CSV member."""
    import os.path
    import zipfile

    import requests

    os.makedirs(dest_dir, exist_ok=True)
    base = url.rsplit("/", 1)[-1]
    looks_zip = url.lower().endswith(".zip")
    csv_path = os.path.join(dest_dir, base if base.lower().endswith(".csv") else base + ".csv")

    with requests.get(url, stream=True, timeout=(30, 1800)) as resp:
        resp.raise_for_status()
        it = resp.iter_content(chunk_size=1 << 20)
        first = next(it, b"")
        if not first:
            raise RuntimeError(f"empty download: {url}")
        is_zip = looks_zip or first[:4] == b"PK\x03\x04"

        if not is_zip:
            # Direct CSV — sanitise the stream straight to disk (one pass, one file).
            buf = {"head": first, "done": False}

            def _read(n: int) -> bytes:
                if buf["head"] is not None:
                    head, buf["head"] = buf["head"], None
                    return head
                return next(it, b"")

            _sanitise_stream_to_utf8(_read, csv_path)
            return csv_path

        # ZIP fallback: persist the archive, then extract + sanitise its CSV member.
        zip_path = os.path.join(dest_dir, base if looks_zip else base + ".zip")
        with open(zip_path, "wb") as zf:
            zf.write(first)
            for chunk in it:
                if chunk:
                    zf.write(chunk)

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(f"{url}: expected 1 CSV in ZIP, found {members}")
        with zf.open(members[0], "r") as src:
            _sanitise_stream_to_utf8(src.read, csv_path)
    try:
        os.remove(zip_path)
    except OSError:
        pass
    return csv_path


# ───────────────────────────── DuckDB transform (100% in SQL) ─────────────────────────────
def _snake(name: str) -> str:
    """CMS headers are underscore-delimited (``Covered_Recipient_NPI``) — lowercasing
    yields a clean snake_case identifier. Non-alphanumerics fold to '_' defensively."""
    return _re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def _projection(con, csv_path: str) -> tuple[str, list[str]]:
    """Read the file header via DuckDB DESCRIBE and build the full-fidelity dynamic
    projection. Every source column is preserved (trimmed VARCHAR → snake_case alias);
    the typed allow-list applies DATE / DECIMAL / INTEGER casts. Returns (projection_sql,
    alias_list). Identifiers are double-quoted; the /tmp path is a repo-controlled literal."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{csv_path.replace(chr(39), chr(39) * 2)}', {DESCRIBE_OPTS})"
    ).fetchall()
    src_cols = [r[0] for r in rows]

    exprs: list[str] = []
    aliases: list[str] = []
    seen: dict[str, int] = {}
    for src in src_cols:
        alias = _snake(src)
        if not alias:
            continue
        if alias in seen:  # defensive: disambiguate any case-folding collision
            seen[alias] += 1
            alias = f"{alias}_{seen[alias]}"
        else:
            seen[alias] = 1
        q = '"' + src.replace('"', '""') + '"'
        if alias in _DATE_COLS:
            expr = f"TRY_CAST(TRY_STRPTIME(nullif(trim({q}), ''), '%m/%d/%Y') AS DATE)"
        elif alias in _MONEY_COLS:
            expr = f"TRY_CAST(nullif(trim({q}), '') AS DECIMAL(14,2))"
        elif alias in _INT_COLS:
            expr = f"TRY_CAST(nullif(trim({q}), '') AS INTEGER)"
        else:
            expr = f"nullif(trim({q}), '')"
        exprs.append(f"{expr} AS {alias}")
        aliases.append(alias)
    return ",\n    ".join(exprs), aliases


def _build_sql(con, csv_path: str, year: int, source_file: str, source_url: str) -> str:
    """Assemble the full transform SELECT. Adds the authoritative ``payment_year`` partition
    key (from the catalog, NOT the file's Program_Year — guarantees the idempotent delete
    predicate matches every row) plus provenance columns. ``year`` is an int and the string
    literals are repo-controlled (catalog-derived), single-quote escaped regardless."""
    projection, _ = _projection(con, csv_path)

    def lit(s: str) -> str:
        return s.replace("'", "''")

    return (
        f"WITH raw AS (SELECT * FROM read_csv('{lit(csv_path)}', {READ_OPTS}))\n"
        f"SELECT\n    {projection},\n"
        f"    CAST({int(year)} AS SMALLINT) AS payment_year,\n"
        f"    '{lit(source_file)}' AS source_file,\n"
        f"    '{lit(source_url)}' AS source_url,\n"
        f"    now() AS ingested_at\n"
        f"FROM raw"
    )


def _append_local(reader, local_ds: str, create: bool) -> None:
    """Write this year's batches to the LOCAL family Lance dataset (no storage_options → no
    R2 multipart during compute). ``create`` for the first landed year of the family, append
    thereafter. SEQUENTIAL by design (single writer per local dataset)."""
    import lance

    lance.write_dataset(
        reader,
        local_ds,
        schema=reader.schema,  # REQUIRED when the source is a RecordBatchReader
        mode="create" if create else "append",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
    )


def _build_indexes_local(local_ds: str, family: str) -> list[str]:
    """Build BTREE + BITMAP scalar indexes on the LOCAL family dataset (no storage_options —
    local index files also sidestep R2's multipart part-size rule). create_scalar_index
    defaults to replace=True → idempotent. Best-effort per column: a column absent in the
    written schema, or a single heavy BTREE that fails, must not abort the others."""
    import lance

    cfg = FAMILIES[family]
    ds = lance.dataset(local_ds)
    built: list[str] = []
    for col in cfg["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in cfg["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices. Tolerant of pylance return-shape
    drift (dict vs object, list_indices vs list_indexes)."""
    for attr in ("list_indices", "list_indexes"):
        fn = getattr(ds, attr, None)
        if fn is None:
            continue
        try:
            out = []
            for ix in fn():
                if isinstance(ix, dict):
                    out.append({k: ix.get(k) for k in ("name", "type", "fields")})
                else:
                    out.append({
                        "name": getattr(ix, "name", None),
                        "type": str(getattr(ix, "type", None)),
                        "fields": getattr(ix, "fields", None),
                    })
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method on dataset"}]


# ───────────────────────────── terminal state + callback ─────────────────────────────
def _record_run(phase, family, dataset_uri, payment_year, source_file, source_url,
                rows, rejected, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.cms_open_payments_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.cms_open_payments_runs
                    (feed, phase, family, dataset_uri, payment_year, source_file, source_url,
                     rows_processed, rejected_rows, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, phase, family, dataset_uri, payment_year, source_file, source_url,
                 rows, rejected, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint URL. FLAT JSON body — no
    ``{"data": …}`` envelope; the whole body becomes result.output. A few retries for
    delivery reliability (this is the only thing that wakes Trigger) — not a polling loop."""
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


# ───────────────────────────── unit of work (one family-year) ─────────────────────────────
def _ingest_year_local(unit: dict, local_ds: str, create: bool) -> dict:
    """Download ONE (family, year) CSV → DuckDB project/cast → streaming Arrow → append to
    the LOCAL family Lance dataset, then rm the CSV. Records its own ops 'ingest' row. Raises
    on failure (caller records + continues). No R2 I/O here — the family is published to R2
    ONCE, after all its years land locally (avoids the direct-Lance→R2 multipart rule)."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    family, year = unit["family"], unit["year"]
    uri, source_file, source_url = unit["dataset_uri"], unit["source_file"], unit["url"]
    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rejected, status, error = 0, 0, "error", None
    csv_path = None

    try:
        print(f"[{family} {year}] downloading {source_url}")
        csv_path = _download_csv(source_url, SCRATCH_DIR)
        print(f"[{family} {year}] downloaded {os.path.getsize(csv_path):,} bytes")

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='24GB';")
            con.execute("SET preserve_insertion_order=false;")  # streaming-friendly
            con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
            sql = _build_sql(con, csv_path, year, source_file, source_url)
            # Streaming reader: never materialise the ~8 GB General file in memory; Lance
            # consumes it fragment-by-fragment (the proven FEC pattern).
            reader = con.sql(sql).to_arrow_reader(MAX_ROWS_PER_FILE)
            _append_local(reader, local_ds, create)
            try:
                rj = con.execute("SELECT count(*) FROM reject_errors").fetchone()
                rejected = int(rj[0]) if rj else 0
            except Exception:  # noqa: BLE001 — table absent ⇒ zero rejects
                rejected = 0
        finally:
            con.close()

        # Exact committed count for this year on the LOCAL dataset (no CSV rescan).
        rows = lance.dataset(local_ds).count_rows(filter=f"payment_year = {int(year)}")
        status = "success"
        print(f"[{family} {year}] appended {rows:,} rows locally ({rejected:,} rejected)")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
        print(f"[{family} {year}] FAILED: {error}")
        raise
    finally:
        if csv_path:
            try:
                os.remove(csv_path)  # bounded ephemeral disk: drop before the next year
            except OSError:
                pass
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", family, uri, year, source_file, source_url,
                    int(rows), int(rejected), status, error, started_at, completed_at)

    return {"family": family, "year": year, "rows_processed": int(rows),
            "rejected_rows": int(rejected), "dataset_uri": uri,
            "source_file": source_file, "status": status}


# ───────────────────────────── Modal functions ─────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    """Apply the idempotent ops.cms_open_payments_runs DDL. Run once before the first ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.cms_open_payments_runs schema.")
    return {"status": "success", "table": "ops.cms_open_payments_runs"}


@app.function(timeout=60 * 5)
def discover_units(only_family: str | None = None, only_year: int | None = None) -> list[dict]:
    """Resolve the catalog → ingest units (NO download). The directive's URL-parsing
    verification step: confirms the metastore filter extracts the correct downloadURLs."""
    units = _resolve_units(only_family, only_year)
    for u in units:
        print(f"  {u['year']} {u['label']:10s} -> {u['url']}")
    print(f"resolved {len(units)} ingest unit(s)")
    return units


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,  # Modal floor (512 GiB) — far exceeds one ~8 GB year + DuckDB spill
)
def ingest_family_year(
    family: str, year: int, url: str | None = None,
    build_index: bool = True, trigger_callback_url: str | None = None,
) -> dict:
    """Surgical SINGLE-YEAR update (targeted re-ingest / ops / testing). Stages the existing
    R2 family dataset to local disk (if any), replaces this payment_year (delete + append),
    reindexes locally, and republishes to R2 via boto3 — preserving the family's other years
    and re-downloading only this one CSV. Records state + wakes Trigger. Re-raises on failure."""
    import datetime as dt
    import os.path
    import shutil

    import duckdb
    import lance

    family = family.strip().lower()
    if family not in FAMILIES:
        raise ValueError(f"family must be one of {sorted(FAMILIES)}, got {family!r}")
    year = int(year)
    cfg = FAMILIES[family]
    local_ds = _local_ds(family)
    prefix = _family_prefix(family)

    if url:
        unit = {"year": year, "url": url, "source_file": url.rsplit("/", 1)[-1]}
    else:
        matches = _resolve_units(family, year)
        if not matches:
            raise RuntimeError(f"catalog has no {family} dataset for year {year}")
        unit = matches[0]
    source_url, source_file = unit["url"], unit["source_file"]

    started_at = dt.datetime.now(dt.timezone.utc)
    rows, rejected, status, error = 0, 0, "error", None
    built: list[str] = []
    published = 0
    csv_path = None

    try:
        s3 = _s3_client()
        staged = _download_r2_prefix(s3, prefix, local_ds)  # 0 → dataset does not exist yet
        print(f"[{family} {year}] staged {staged} existing files from {cfg['uri']}")

        csv_path = _download_csv(source_url, SCRATCH_DIR)
        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='24GB';")
            con.execute("SET preserve_insertion_order=false;")
            con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
            sql = _build_sql(con, csv_path, year, source_file, source_url)
            reader = con.sql(sql).to_arrow_reader(MAX_ROWS_PER_FILE)
            if staged:
                ds = lance.dataset(local_ds)
                try:
                    ds.delete(f"payment_year = {int(year)}")  # idempotent replace of this year
                except Exception as exc:  # noqa: BLE001
                    print(f"WARN: delete payment_year={year} failed (continuing): {exc}")
                lance.write_dataset(reader, local_ds, schema=reader.schema, mode="append",
                                    data_storage_version=DATA_STORAGE_VERSION,
                                    max_rows_per_file=MAX_ROWS_PER_FILE,
                                    max_bytes_per_file=MAX_BYTES_PER_FILE)
            else:
                _append_local(reader, local_ds, create=True)
            try:
                rj = con.execute("SELECT count(*) FROM reject_errors").fetchone()
                rejected = int(rj[0]) if rj else 0
            except Exception:  # noqa: BLE001
                rejected = 0
        finally:
            con.close()

        rows = lance.dataset(local_ds).count_rows(filter=f"payment_year = {int(year)}")
        if build_index:
            print(f"[{family}] building indexes locally")
            built = _build_indexes_local(local_ds, family)
        # Local dataset was staged FROM R2 → append-only publish (only new files, never wipe).
        published = _publish_incremental(s3, prefix, local_ds)
        # Read-back gate (O1): open fresh from R2 and assert wholeness before success. Assert
        # the full index plan only when this run (re)built it; otherwise verify rows only.
        total_rows = lance.dataset(local_ds).count_rows()
        _verify_published(cfg["uri"], total_rows,
                          EXPECTED_INDEX_COUNT[family] if build_index else None,
                          cfg["npi_col"], _r2_storage_options())
        status = "success"
        print(f"[{family} {year}] published {published} files, verified {total_rows:,} rows on R2 "
              f"→ {cfg['uri']} ({rows:,} rows this year)")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
        raise
    finally:
        if csv_path:
            try:
                os.remove(csv_path)
            except OSError:
                pass
        shutil.rmtree(local_ds, ignore_errors=True)
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("ingest", family, cfg["uri"], year, source_file, source_url,
                    int(rows), int(rejected), status, error, started_at, completed_at)
        _post_callback(trigger_callback_url, {"status": status, "phase": "ingest_family_year",
                                              "family": family, "year": year, "rows": int(rows),
                                              "files_published": published, "indices": built})

    return {"status": status, "family": family, "year": year, "rows_processed": int(rows),
            "rejected_rows": int(rejected), "dataset_uri": cfg["uri"],
            "files_published": published, "indices": built}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    volumes={VOL_DIR: cms_vol},  # giants (general) stage here, off ephemeral_disk → off spot
    timeout=60 * 60 * 10,  # full historical backfill across all families/years, sequential
    memory=49152,          # 48 GiB — sized for general's in-RAM BTREE build over 82M rows
                           # (LANCE_BYPASS_SPILLING; diagnostic §4.3). Matches reindex_family;
                           # memory does NOT force spot capacity (only ephemeral_disk does).
    cpu=8.0,
    ephemeral_disk=524288,  # 512 GiB — Modal's HARD FLOOR (requests <512 GiB are rejected), so
                            # the plan's "lower to 128 GiB to dodge spot" is infeasible here. The
                            # Volume's value is therefore RESUME, not a smaller disk: a preempted
                            # run re-runs with resume=True and reuses checkpointed years. Only the
                            # ~8 GB CSV + DuckDB spill use this disk; general's dataset is on /vol.
    retries=0,             # a 10 h job must not silently restart from scratch; re-run resumes
)
def refresh_all(
    only_family: str | None = None, only_year: int | None = None,
    skip_index: bool = False, resume: bool = False,
    trigger_callback_url: str | None = None,
) -> dict:
    """THE dispatched orchestrator (Trigger → Universal Dispatcher → here). One container.
    For each family, SEQUENTIALLY (download → transform → append-LOCAL → rm CSV) accumulate
    every year into a local Lance dataset (never two years' CSVs on disk at once), then build
    scalar indexes ONCE, publish the whole family to R2 via boto3 (uniform-part multipart —
    the R2-compliant write the direct Lance→R2 path cannot do), and READ-BACK VERIFY it before
    success. Per-unit failures are recorded and skipped (one bad year must not sink the family).

    Giants (general) stage on a Modal Volume and CHECKPOINT each landed year (vol.commit), so a
    preempted/killed run re-runs with resume=True and re-ingests only the MISSING years instead
    of restarting from 2018 (the recovery run #59-64 needed). resume defaults False — every
    quarterly refresh is a clean from-scratch rebuild. Posts ONE flat-JSON summary callback."""
    import datetime as dt
    import shutil

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    units = _resolve_units(only_family, only_year)

    # Group by family, preserving registry order (general → research → ownership).
    by_family: dict[str, list[dict]] = {}
    for u in units:
        by_family.setdefault(u["family"], []).append(u)

    per_unit: list[dict] = []
    by_family_summary: dict[str, dict] = {}
    failures: list[dict] = []
    s3 = _s3_client()

    for family, fam_units in by_family.items():
        on_volume = family in _RESUMABLE_FAMILIES  # giant → Volume-staged + per-year resume
        local_ds = _local_ds(family, on_volume=on_volume)
        fam_years = [u["year"] for u in fam_units]

        # Resume (giant recovery): keep years already checkpointed on the Volume from a prior
        # interrupted run and re-ingest only the rest. A fresh run (default; every quarterly
        # refresh) wipes and rebuilds from scratch — CMS revises prior years, so fresh is correct.
        if on_volume and resume:
            already = _landed_years(local_ds, fam_years)
            if already:
                print(f"[{family}] resume: years {sorted(already)} already on Volume — skipping re-ingest")
        else:
            shutil.rmtree(local_ds, ignore_errors=True)
            if on_volume:
                cms_vol.commit()  # persist the wipe so resume never sees a stale partial
            already = set()

        landed: list[int] = sorted(already)
        for unit in fam_units:
            if unit["year"] in already:
                continue  # resume: already landed + checkpointed
            try:
                # create only when no local dataset exists yet (first landed year, or a fresh
                # family); append once it does — including a resume onto already-landed years.
                per_unit.append(
                    _ingest_year_local(unit, local_ds, create=not _dataset_exists(local_ds)))
                landed.append(unit["year"])
                if on_volume:
                    cms_vol.commit()  # CHECKPOINT: a preemption after this keeps the year
            except Exception as exc:  # noqa: BLE001 — record + continue; re-run is idempotent
                failures.append({"family": family, "year": unit["year"], "error": str(exc)})

        built: list[str] = []
        published = 0
        publish_status = "skipped"
        fam_rows = 0
        if landed:
            if not skip_index:
                try:
                    print(f"[{family}] building indexes locally over {len(landed)} year(s)")
                    built = _build_indexes_local(local_ds, family)
                except Exception as exc:  # noqa: BLE001
                    built = [f"ERROR: {exc}"]
            # Authoritative family row count from the dataset itself — robust to resume, where
            # per_unit holds only THIS run's years but the dataset holds every landed year.
            fam_rows = lance.dataset(local_ds).count_rows() if _dataset_exists(local_ds) else 0
            pub_started = dt.datetime.now(dt.timezone.utc)
            try:
                # From-scratch rebuild → stage to __staging, verify == local, then swap over the
                # live prefix. Live is never wiped before the new copy is verified; for general's
                # restore the swap also reclaims the orphaned partial-publish corpse cleanly.
                published = _publish_full_swap(s3, _family_prefix(family), local_ds)
                publish_status = "success"
                print(f"[{family}] published {published} files → {FAMILIES[family]['uri']}")
            except Exception as exc:  # noqa: BLE001
                publish_status = "error"
                failures.append({"family": family, "year": "publish", "error": str(exc)})
                print(f"[{family}] PUBLISH FAILED: {exc}")
            _record_run("publish", family, FAMILIES[family]["uri"], None, None, None,
                        fam_rows, 0, publish_status,
                        None if publish_status == "success" else "boto3 publish failed",
                        pub_started, dt.datetime.now(dt.timezone.utc))

            # Read-back verify gate (O1/O2): only NOW is the family truly done. Records a
            # distinct 'verify' ledger row; a failure downgrades the family (bytes stay for
            # inspection — this is the alarm whose absence let run #58 'succeed' over a corpse).
            if publish_status == "success":
                ver_started = dt.datetime.now(dt.timezone.utc)
                try:
                    vinfo = _verify_published(
                        FAMILIES[family]["uri"], fam_rows,
                        None if skip_index else EXPECTED_INDEX_COUNT[family],
                        FAMILIES[family]["npi_col"], _r2_storage_options())
                    print(f"[{family}] verify OK: rows={vinfo['rows']:,} indices={vinfo['indices']}")
                    _record_run("verify", family, FAMILIES[family]["uri"], None, None, None,
                                vinfo["rows"], 0, "success", None, ver_started,
                                dt.datetime.now(dt.timezone.utc))
                except Exception as exc:  # noqa: BLE001
                    publish_status = "verify_failed"
                    failures.append({"family": family, "year": "verify", "error": str(exc)})
                    print(f"[{family}] VERIFY FAILED: {exc}")
                    _record_run("verify", family, FAMILIES[family]["uri"], None, None, None,
                                fam_rows, 0, "error", str(exc)[:2000], ver_started,
                                dt.datetime.now(dt.timezone.utc))

        # Free disk before the next family. A giant's Volume copy is RETAINED across a failed
        # run (publish/verify/ingest) so a resume reuses its landed years; it is cleared only
        # after a clean publish+verify. Small families always clear (nothing to resume).
        if on_volume:
            if publish_status == "success":
                shutil.rmtree(local_ds, ignore_errors=True)
                cms_vol.commit()
        else:
            shutil.rmtree(local_ds, ignore_errors=True)

        by_family_summary[family] = {
            "rows": fam_rows,
            "years": sorted(landed),
            "indices": built,
            "files_published": published,
            "publish": publish_status,
        }

    completed_at = dt.datetime.now(dt.timezone.utc)
    ok = [r for r in per_unit if r["status"] == "success"]
    published_ok = all(v["publish"] == "success" for v in by_family_summary.values()
                       if v["years"])
    summary = {
        "status": "success" if (not failures and published_ok)
                  else ("partial" if ok else "error"),
        "feed": FEED,
        "phase": "refresh_all",
        "units_total": len(units),
        "units_succeeded": len(ok),
        "units_failed": len([f for f in failures if f["year"] not in ("publish", "verify")]),
        "rows_processed": sum(r["rows_processed"] for r in ok),
        "by_family": by_family_summary,
        "failures": failures,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
    }
    _record_run("refresh_all", None, None, None, None, None,
                summary["rows_processed"], 0, summary["status"],
                None if not failures else str(failures)[:2000], started_at, completed_at)
    _post_callback(trigger_callback_url, summary)

    if summary["status"] == "error":
        raise RuntimeError(f"cms_open_payments refresh_all: no units landed of {len(units)}")
    return summary


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 60 * 3,
    memory=49152,  # BTREE sort over the full ~40-50M-row General dataset (bypass-spilling)
    cpu=8.0,
    ephemeral_disk=524288,  # stage the full family dataset locally before reindexing
)
def reindex_family(family: str) -> dict:
    """(Re)build the scalar indexes without re-ingesting: stage the committed R2 dataset to
    local disk, index locally (no R2 multipart), republish via boto3. Idempotent."""
    import shutil

    import lance

    family = family.strip().lower()
    if family not in FAMILIES:
        raise ValueError(f"family must be one of {sorted(FAMILIES)}, got {family!r}")
    cfg = FAMILIES[family]
    local_ds = _local_ds(family)
    prefix = _family_prefix(family)

    s3 = _s3_client()
    staged = _download_r2_prefix(s3, prefix, local_ds)
    if staged == 0:
        raise RuntimeError(f"{family}: no dataset at {cfg['uri']} to reindex")
    print(f"Staged {staged} files from {cfg['uri']} → {local_ds}")
    try:
        built = _build_indexes_local(local_ds, family)
        # Local dataset was staged FROM R2 → append-only publish (only new index/manifest
        # files; the multi-GB data tree is reused in place, never re-pushed).
        published = _publish_incremental(s3, prefix, local_ds)
        ds = lance.dataset(local_ds)
        local_rows = ds.count_rows()
        # Read-back gate (O1/O2): the reindex always (re)builds the full plan → assert it.
        verified = _verify_published(cfg["uri"], local_rows, EXPECTED_INDEX_COUNT[family],
                                     cfg["npi_col"], _r2_storage_options())
        out = {"family": family, "dataset_uri": cfg["uri"], "rows": local_rows,
               "built": built, "files_published": published, "verified": verified,
               "committed_indices": _list_committed_indices(ds)}
    finally:
        shutil.rmtree(local_ds, ignore_errors=True)
    return out


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15, memory=8192)
def verify_family(family: str) -> dict:
    """Read-only health probe of a published family on R2 (no staging, no mutation): open the
    committed dataset straight from its R2 manifest and return row count + committed scalar
    indices (count, names, fields). The operational check after a restore/publish — confirms
    the manifest opens (a partial-publish corpse raises 'Not found: …/_versions'), the row
    count matches the catalog ingest, and the full BTREE+BITMAP plan committed."""
    import lance

    family = family.strip().lower()
    if family not in FAMILIES:
        raise ValueError(f"family must be one of {sorted(FAMILIES)}, got {family!r}")
    cfg = FAMILIES[family]
    so = _r2_storage_options()
    ds = lance.dataset(cfg["uri"], storage_options=so)
    indices = _list_committed_indices(ds)
    healthy_idx = [i for i in indices if "error" not in i]
    expected = len(cfg["btree"]) + len(cfg["bitmap"])
    rows = ds.count_rows()
    out = {
        "family": family,
        "dataset_uri": cfg["uri"],
        "rows": rows,
        "version": getattr(ds, "version", None),
        "num_indices": len(healthy_idx),
        "expected_indices": expected,
        "indices_ok": len(healthy_idx) == expected,
        "committed_indices": indices,
    }
    print(f"[{family}] rows={rows:,} indices={len(healthy_idx)}/{expected} "
          f"v{out['version']} → {cfg['uri']}")
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 30) -> list:
    """Read the most recent ops.cms_open_payments_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, phase, family, payment_year, source_file, rows_processed, "
            "rejected_rows, status, error, started_at, completed_at "
            "FROM ops.cms_open_payments_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ───────────────────────────── local entrypoints (manual ops) ─────────────────────────────
@app.local_entrypoint()
def init_state() -> None:
    """Create ops.cms_open_payments_runs (idempotent)."""
    import json

    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def discover(only_family: str = "", only_year: int = 0) -> None:
    """Verify catalog URL parsing: print the resolved (family, year, url) ingest units."""
    import json

    units = discover_units.remote(only_family or None, only_year or None)
    print(json.dumps(units, indent=2, default=str))


@app.local_entrypoint()
def ingest_one(family: str, year: int, url: str = "", no_index: bool = False) -> None:
    """Ingest a single (family, year). Resolves the URL from the catalog unless --url given."""
    import json

    print(json.dumps(
        ingest_family_year.remote(family, year, url or None, not no_index, None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def backfill(only_family: str = "", only_year: int = 0, skip_index: bool = False,
             resume: bool = False) -> None:
    """Full historical backfill (or a filtered slice). Sequential, one container.
    --resume re-uses years already checkpointed on the Volume from a prior interrupted run
    (giant recovery); default is a clean from-scratch rebuild (the quarterly-refresh posture)."""
    import json

    print(json.dumps(
        refresh_all.remote(only_family or None, only_year or None, skip_index, resume, None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def reindex(family: str = "") -> None:
    """Rebuild scalar indexes on existing family dataset(s) (no re-ingest). Default: all."""
    import json

    for fam in ([family] if family else list(FAMILIES)):
        print(json.dumps(reindex_family.remote(fam), indent=2, default=str))


@app.local_entrypoint()
def verify(family: str = "") -> None:
    """Read-only health probe: row count + committed indices for a family (default: all)."""
    import json

    for fam in ([family] if family else list(FAMILIES)):
        print(json.dumps(verify_family.remote(fam), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 30) -> None:
    """Print the most recent ops ledger rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
