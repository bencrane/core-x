"""Single source of truth for company identity URI + source-platform provenance.

`source_platform` no longer lives on the company row. It moves wholesale into the
`active/company_source_platforms` sidecar, and `active/companies_canonical` carries
EVERY companies column EXCEPT `source_platform`, keyed 1:1 on `company_id`.

NO DEDUP (the load-bearing constraint). `company_id` stays the 1:1 primary key of the
canonical companies dataset — every one of the 117,037 rows is preserved. There is NO
`canonical_company_id`, NO row collapse, NO merge on `linkedin`/`domain`. A LinkedIn- or
domain-level dedup is a SEPARATE, FUTURE workstream the operator decides later (and it must
reckon with tribal-holding-company / placeholder cruft before collapsing anything). This
module deliberately does not touch that question: it splits the `source_platform` column off,
nothing more.

WHY THE SPLIT (vs the people sidecar, which IS an entity-resolution collapse). For people,
`person_id` is manufactured per-source, so the same human lands under many ids and the sidecar
is keyed on a derived `canonical_person_id`. Companies are different: `company_id` IS already
the stable, cross-source-unique 1:1 key of every companies writer (uei / sfnet-directory UUID /
dex UUID). So the company sidecar is keyed directly on `(company_id, source_platform)` — no
derivation, no id manufacturing. The split is purely about getting `source_platform` off the
wide company row so a company is one identity row and its origins are a joinable set.

CUTOVER MODEL = REPOINT (non-destructive). Everything points at the NEW URIs. The old
`active/companies` (v131) is FROZEN for rollback and is NEVER written by this module. Consumers
repoint via the `GTM_COMPANIES_URI` default (unset in Doppler → the code default wins). See
docs/plans/COMPANIES_SOURCE_PLATFORM_SIDECAR.md.

SIDECAR CONTRACT (must match the built dataset — do NOT alter without a rebuild):
    company_source_platforms
        company_id      string          NOT NULL   BTREE   — the companies PK (1:1 join key)
        source_platform string          NOT NULL   BITMAP  — the origin tag (23 values today)
        first_seen_at   timestamp(us,UTC) NOT NULL          — set now() on first insert; never updated
        source_ref      string          nullable            — free-form cohort / run note
    idempotency key = (company_id, source_platform); one row per (company_id, source_platform).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pyarrow as pa

# ── Canonical dataset URIs (REPOINT target — the NEW datasets the coordinator built) ─────────
# Overridable per-dataset via env so a staging sink can be redirected without a code change.
# GTM_COMPANIES_URI is UNSET in Doppler, so this default is what wins at cutover.
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", "s3://data-sink/active/companies_canonical/")
COMPANY_SOURCE_PLATFORMS_URI = os.environ.get(
    "COMPANY_SOURCE_PLATFORMS_URI", "s3://data-sink/active/company_source_platforms/"
)

# Sidecar idempotency key (grain = one row per company × source).
SIDECAR_KEY = ["company_id", "source_platform"]

# Sidecar Arrow schema — MUST match the committed dataset exactly. The coordinator built
# company_source_platforms with first_seen_at in America/New_York; a UTC field type here is
# rejected on append (Lance schema check is tz-strict, verified on the person sidecar twin). tz
# is display metadata only — the stored instant is identical — so the schema is aligned to the
# built dataset, not rebuilt.
SIDECAR_SCHEMA = pa.schema(
    [
        pa.field("company_id", pa.string()),
        pa.field("source_platform", pa.string()),
        pa.field("first_seen_at", pa.timestamp("us", tz="America/New_York")),
        pa.field("source_ref", pa.string()),
    ]
)


def r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2 (r2-credentials secret). Endpoint from
    ``R2_ENDPOINT`` or derived from ``R2_ACCOUNT_ID`` — byte-identical to every worker in
    ``pipelines/*``."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _sidecar_rows(
    company_ids: list[str],
    source_platform: str,
    source_ref: str | None,
    first_seen_at: datetime,
) -> "pa.Table":
    """Build a sidecar Arrow table (SIDECAR_SCHEMA) from a de-duplicated company_id list.

    One row per company_id (the caller passes distinct ids); ``source_platform`` and
    ``source_ref`` are literals, ``first_seen_at`` is the batch stamp."""
    n = len(company_ids)
    return pa.table(
        {
            "company_id": pa.array([str(c) for c in company_ids], type=pa.string()),
            "source_platform": pa.array([source_platform] * n, type=pa.string()),
            "first_seen_at": pa.array([first_seen_at] * n, type=pa.timestamp("us", tz="UTC")),
            "source_ref": pa.array([source_ref] * n, type=pa.string()),
        },
        schema=SIDECAR_SCHEMA,
    )


def record_company_sources(
    company_ids,
    source_platform: str,
    source_ref: str | None,
    storage_options: dict[str, str],
    *,
    sidecar_uri: str | None = None,
    first_seen_at: datetime | None = None,
) -> dict[str, int]:
    """Record ``(company_id → source_platform)`` provenance into the sidecar — idempotent.

    ``company_ids`` is any iterable of company ids (the caller's freshly-landed companies for
    this ``source_platform``). Null/empty ids are dropped; the rest are de-duplicated so the
    merge source never carries an intra-batch duplicate key.

    Write mode: ``merge_insert(["company_id","source_platform"]).when_not_matched_insert_all()``
    with ``first_seen_at = now()`` (never updated on re-run) and ``source_ref``. One sidecar row
    per ``(company_id, source_platform)``; re-running any writer never duplicates a pair and
    never disturbs ``first_seen_at``. The sidecar MUST already exist (the coordinator built it) —
    this is an append/merge path, never a create.

    Returns ``{"sidecar_candidates": <n>}`` — the pre-merge distinct-id count (merge_insert
    applies them idempotently)."""
    import lance

    sidecar_uri = sidecar_uri or COMPANY_SOURCE_PLATFORMS_URI
    first_seen_at = first_seen_at or datetime.now(timezone.utc)

    seen: set[str] = set()
    distinct: list[str] = []
    for cid in company_ids:
        if cid is None:
            continue
        s = str(cid).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        distinct.append(s)

    tbl = _sidecar_rows(distinct, source_platform, source_ref, first_seen_at)
    if tbl.num_rows == 0:
        return {"sidecar_candidates": 0}

    target = lance.dataset(sidecar_uri, storage_options=storage_options)
    target.merge_insert(SIDECAR_KEY).when_not_matched_insert_all().execute(tbl)
    return {"sidecar_candidates": tbl.num_rows}
