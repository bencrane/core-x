"""Single source of truth for canonical person identity + source provenance (Phase D).

This module is the ONE place that (a) normalizes a LinkedIn URL to the canonical form, (b)
derives ``canonical_person_id`` from it, and (c) lands identity into the canonical people
dataset while routing source-platform provenance into the ``person_source_platforms`` sidecar.
Every writer imports these functions so ids are byte-for-byte identical to the datasets the
Phase A–C coordinator already built and verified in R2.

WHY THE SPLIT.
    * ``person_id`` is NOT a human-canonical key — every writer manufactures it under a
      different scheme (source-native UUID / upstream Clay id / sha256 / md5 / uuid5), so the
      same human lands under different ``person_id``s from different sources. The only
      cross-source-stable human key is the normalized LinkedIn URL.
    * ``canonical_person_id`` = ``sha256(canonical_linkedin_url)`` collapses those to one id
      per human. Null-URL people fall back to ``sha256('pid:' || legacy_person_id)`` — stable
      but degenerate (one canonical id per legacy id, no cross-source merge without a URL).
    * ``active/people_canonical`` carries ONE row per canonical person, ``source_platform``
      DROPPED. ``active/person_source_platforms`` (the sidecar) carries EVERY current
      ``(person_id → source_platform)`` mapping under its canonical human, append-only.

CUTOVER MODEL = REPOINT (non-destructive). Everything points at the NEW URIs. The old
``active/people`` (v67) is FROZEN for rollback and is NEVER written by this module. Consumers
repoint via env default. See docs/plans/PEOPLE_SOURCE_PLATFORM_SIDECAR.md.

EXACT CANONICAL SPEC (must match the built datasets — do NOT alter without a rebuild):
    normalize_linkedin(url):
        lower(trim(url))
        → strip leading  ^https?://
        → strip leading  ^[a-z0-9.-]*linkedin\\.com/   (host + any cc-subdomain + www.)
        → strip          [?#].*$                        (query + fragment)
        → strip trailing /+$
        → None if empty
    canonical_person_id(url, legacy_person_id):
        norm = normalize_linkedin(url)
        sha256(norm).hexdigest()                        when norm is not None
        sha256('pid:' + legacy_person_id).hexdigest()   otherwise
"""
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone

import pyarrow as pa

# ── Canonical dataset URIs (REPOINT target — the NEW datasets the coordinator built) ─────────
# Overridable per-dataset via env so a staging sink can be redirected without a code change.
PEOPLE_URI = os.environ.get("PEOPLE_LANCE_URI", "s3://data-sink/active/people_canonical/")
SIDECAR_URI = os.environ.get(
    "PERSON_SOURCE_PLATFORMS_URI", "s3://data-sink/active/person_source_platforms/"
)

# Lance write params — mirror companies_people_bulk (fleet defaults; net-new → pin v2.1).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3  # 90 GiB — Lance's documented default

# Sidecar idempotency key (grain = one row per legacy id × source under a canonical human).
SIDECAR_KEY = ["canonical_person_id", "source_platform", "legacy_person_id"]

# ── Canonical LinkedIn normalization (compiled once; the merge boundary lives here) ──────────
_SCHEME = re.compile(r"^https?://")
_HOST = re.compile(r"^[a-z0-9.-]*linkedin\.com/")
_QF = re.compile(r"[?#].*$")
_TRAIL = re.compile(r"/+$")

# Sidecar Arrow schema — MUST match the committed dataset exactly. The coordinator built
# person_source_platforms with first_seen_at in America/New_York; a UTC field type here is
# rejected on append (Lance schema check is tz-strict). tz is display metadata only — the
# stored instant is identical — so the schema is aligned to the built dataset, not rebuilt.
SIDECAR_SCHEMA = pa.schema(
    [
        pa.field("canonical_person_id", pa.string()),
        pa.field("source_platform", pa.string()),
        pa.field("legacy_person_id", pa.string()),
        pa.field("person_linkedin_url_norm", pa.string()),
        pa.field("first_seen_at", pa.timestamp("us", tz="America/New_York")),
        pa.field("source_ref", pa.string()),
    ]
)


def normalize_linkedin(url: str | None) -> str | None:
    """Canonical LinkedIn normalization (EXACT spec). Returns the normalized path form
    (e.g. ``in/alexkigel``) or ``None`` when the input is empty/normalizes away."""
    if url is None:
        return None
    s = url.lower().strip()
    s = _SCHEME.sub("", s)
    s = _HOST.sub("", s)
    s = _QF.sub("", s)
    s = _TRAIL.sub("", s)
    return s or None


def canonical_person_id(url: str | None, legacy_person_id: str | None) -> str:
    """``sha256(normalize_linkedin(url))`` when the URL normalizes non-null, else
    ``sha256('pid:' || legacy_person_id)``. Lowercase hex. This is the ONE id function —
    every writer produces ids identical to the sidecar/people datasets through it."""
    norm = normalize_linkedin(url)
    if norm is not None:
        return hashlib.sha256(norm.encode()).hexdigest()
    return hashlib.sha256(("pid:" + str(legacy_person_id)).encode()).hexdigest()


def r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2 (r2-credentials secret). Endpoint from
    ``R2_ENDPOINT`` or derived from ``R2_ACCOUNT_ID``."""
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


def add_canonical_person_id(
    rows: "pa.Table", *, url_col: str = "person_linkedin_url", legacy_id_col: str = "person_id"
) -> "pa.Table":
    """Return ``rows`` with two derived columns appended:
      * ``canonical_person_id`` — via :func:`canonical_person_id` per row.
      * ``person_linkedin_url_norm`` — via :func:`normalize_linkedin` per row.
    The source URL / legacy-id columns are read by name (defaults match the people contract).
    Pure/vectorized in Python over the two source columns — no engine dependency."""
    urls = rows.column(url_col).to_pylist()
    legacy = rows.column(legacy_id_col).to_pylist()
    norm = [normalize_linkedin(u) for u in urls]
    cpid = [canonical_person_id(u, lid) for u, lid in zip(urls, legacy)]
    out = rows.append_column("canonical_person_id", pa.array(cpid, type=pa.string()))
    out = out.append_column("person_linkedin_url_norm", pa.array(norm, type=pa.string()))
    return out


def _sidecar_rows(
    canonical_ids: list[str],
    legacy_ids: list[str | None],
    norms: list[str | None],
    source_platform: str,
    source_ref: str | None,
    first_seen_at: datetime,
) -> "pa.Table":
    """Build a sidecar Arrow table (SIDECAR_SCHEMA) from parallel per-row columns."""
    n = len(canonical_ids)
    return pa.table(
        {
            "canonical_person_id": pa.array(canonical_ids, type=pa.string()),
            "source_platform": pa.array([source_platform] * n, type=pa.string()),
            "legacy_person_id": pa.array(
                [None if v is None else str(v) for v in legacy_ids], type=pa.string()
            ),
            "person_linkedin_url_norm": pa.array(norms, type=pa.string()),
            "first_seen_at": pa.array([first_seen_at] * n, type=pa.timestamp("us", tz="UTC")),
            "source_ref": pa.array([source_ref] * n, type=pa.string()),
        },
        schema=SIDECAR_SCHEMA,
    )


def land_people(
    rows_arrow: "pa.Table",
    source_platform: str,
    source_ref: str | None,
    storage_options: dict[str, str],
    *,
    url_col: str = "person_linkedin_url",
    legacy_id_col: str = "person_id",
    people_uri: str | None = None,
    sidecar_uri: str | None = None,
    first_seen_at: datetime | None = None,
) -> dict[str, int]:
    """Land identity + provenance for a batch of people (idempotent, out-of-core-safe).

    ``rows_arrow`` is a people-contract Arrow table (canonical people schema columns; it MUST
    carry ``url_col`` and ``legacy_id_col``, and MUST NOT carry a ``source_platform`` column).

    Two writes, both idempotent:
      1. PEOPLE — compute ``canonical_person_id`` per row, dedup to one row per canonical id
         (first occurrence wins), then ``merge_insert(canonical_person_id)
         .when_not_matched_insert_all()`` into ``people_uri``. Identity-only; NO source_platform
         column ever reaches ``people``. Existing canonical people are never disturbed.
      2. SIDECAR — ``merge_insert(SIDECAR_KEY).when_not_matched_insert_all()`` into
         ``sidecar_uri`` with ``first_seen_at = now()`` (never updated on re-run) and
         ``source_ref``. One sidecar row per ``(canonical_person_id, source_platform,
         legacy_person_id)``.

    Returns ``{"people_candidates", "sidecar_candidates"}`` (the pre-merge candidate row counts;
    merge_insert applies them idempotently). The datasets MUST already exist (Phase A–C built
    them) — this is an append/merge path, never a create.
    """
    import lance

    people_uri = people_uri or PEOPLE_URI
    sidecar_uri = sidecar_uri or SIDECAR_URI
    first_seen_at = first_seen_at or datetime.now(timezone.utc)

    if "source_platform" in rows_arrow.schema.names:
        raise ValueError(
            "land_people: rows_arrow carries a 'source_platform' column — provenance must route "
            "to the sidecar, never the canonical people schema."
        )

    enriched = add_canonical_person_id(rows_arrow, url_col=url_col, legacy_id_col=legacy_id_col)

    # ── SIDECAR: one row per (canonical_person_id, source_platform, legacy_person_id) ──
    cids = enriched.column("canonical_person_id").to_pylist()
    legacy = enriched.column(legacy_id_col).to_pylist()
    norms = enriched.column("person_linkedin_url_norm").to_pylist()
    seen_key: set[tuple[str, str | None]] = set()
    s_cid: list[str] = []
    s_legacy: list[str | None] = []
    s_norm: list[str | None] = []
    for cid, lid, nrm in zip(cids, legacy, norms):
        k = (cid, None if lid is None else str(lid))
        if k in seen_key:
            continue
        seen_key.add(k)
        s_cid.append(cid)
        s_legacy.append(lid)
        s_norm.append(nrm)
    sidecar_tbl = _sidecar_rows(
        s_cid, s_legacy, s_norm, source_platform, source_ref, first_seen_at
    )

    # ── PEOPLE: identity-only, one row per canonical id (drop the derived norm helper col) ──
    people_target = lance.dataset(people_uri, storage_options=storage_options)
    people_schema = people_target.schema
    # Keep only the columns the canonical people schema declares (drops person_linkedin_url_norm
    # and any other helper column). canonical_person_id must be present in the people schema.
    keep = [f.name for f in people_schema if f.name in enriched.schema.names]
    missing = [f.name for f in people_schema if f.name not in enriched.schema.names]
    people_cand = enriched.select(keep)
    # NULL-fill any people-schema column the source batch didn't populate, in exact schema order.
    if missing:
        cols = {name: people_cand.column(name) for name in people_cand.schema.names}
        arrays = [
            cols[f.name].cast(f.type)
            if f.name in cols
            else pa.nulls(people_cand.num_rows, f.type)
            for f in people_schema
        ]
        people_cand = pa.Table.from_arrays(arrays, schema=people_schema)
    else:
        people_cand = pa.Table.from_arrays(
            [people_cand.column(f.name).cast(f.type) for f in people_schema], schema=people_schema
        )

    # Dedup people candidates to one row per canonical_person_id (first occurrence wins) so the
    # merge_insert source never carries an intra-batch duplicate key.
    seen_cid: set[str] = set()
    mask = []
    for cid in people_cand.column("canonical_person_id").to_pylist():
        keep_row = cid not in seen_cid
        seen_cid.add(cid)
        mask.append(keep_row)
    people_cand = people_cand.filter(pa.array(mask, type=pa.bool_()))

    people_target.merge_insert(
        "canonical_person_id"
    ).when_not_matched_insert_all().execute(people_cand)

    sidecar_target = lance.dataset(sidecar_uri, storage_options=storage_options)
    sidecar_target.merge_insert(SIDECAR_KEY).when_not_matched_insert_all().execute(sidecar_tbl)

    return {
        "people_candidates": people_cand.num_rows,
        "sidecar_candidates": sidecar_tbl.num_rows,
    }
