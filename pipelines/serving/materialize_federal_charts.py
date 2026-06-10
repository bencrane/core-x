"""Serving precompute — materialize the FEDERAL chart + map-entity snapshot (WARM).

WHY THIS EXISTS (the warm invariant). The rare-structure-hq map/chart cockpit serves
a PUBLIC surface that must answer in milliseconds. The chart aggregations
(``federal_spend_by_industry`` / ``_by_state`` / ``_by_agency``) and the map/list
projection (``federal_entities_by_filter``) scan the full 1.54M-row
``entity_profile_gold`` over Lance/DuckDB — a cold open of that dataset costs ~60s and
several seconds of grouped scan. That is unacceptable on the request path. So this
worker PRECOMPUTES every serving artifact ONCE, writes it to a durable R2 location, and
the BFF loads those tiny artifacts into memory at boot — the request path never opens
Lance, never runs DuckDB. The data is slow-changing (gold refreshes on its own cadence);
a refresh here is a MANUAL re-run, no cron.

SINGLE SOURCE OF TRUTH. This module does NOT re-implement any SQL. It IMPORTS the merged
``apps.gtm_mcp.src.tools.federal`` query functions and calls them — the same code the MCP
gateway exposes — so the materialized artifact is byte-for-byte the function output. If
the aggregation logic changes, this snapshot changes with it; there is no drift surface.

ARTIFACTS (written under ``s3://data-sink/active/federal_serving/``):

  charts/spend_by_industry.json   — top-N NAICS by lifetime spend (tens of rows)
  charts/spend_by_state.json      — every state, full set (~50 rows)
  charts/spend_by_agency.json     — top-N awarding agencies (tens of rows)
  entities/map_entities.parquet   — the map/list slice (columnar, P1 list/filter path)
  manifest.json                   — materialized_at + profile_as_of_date + row counts
                                    + artifact URIs + the entity-slice provenance/bound

ENTITY-SLICE BOUND (P1 — stated, never silent). The full ``has_federal_awards=TRUE``
universe is ~354k entities; projected to the 9 list columns that is too large to hold
in BFF memory as a static array and load on every boot. The serving slice is therefore
bounded to ``total_lifetime_obligations >= ENTITY_MIN_LIFETIME`` (default $1,000,000 →
~117k entities, a few MB of Parquet) — the firms that actually matter for an
origination/market-map surface; sub-threshold federal activity is noise for this
audience. The bound + the full unbounded count are recorded in the manifest so the
choice is auditable and the UI can surface it. Tune with ``--entity-min-lifetime``.

PROVENANCE. ``profile_as_of_date`` is read straight from ``entity_profile_gold`` (its own
gold as-of stamp); ``materialized_at`` is this run's UTC instant. The BFF shows
"data as of {profile_as_of_date}" and "snapshot built {materialized_at}".

RUN (from the core-x repo root, R2 creds via Doppler):

    doppler run -p core-x -c prd -- ./.venv/bin/python3 \
        -m pipelines.serving.materialize_federal_charts --refresh

    # add --verify to read the artifacts back and print the spend-by-industry top-10
    # add --dry-run to compute + print metrics, write NOTHING
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import sys
from typing import Any

# The merged federal query functions are the SINGLE source of truth — imported, not
# re-implemented. ``database`` gives the same warm R2/Lance handle the gateway uses
# (for reading the gold as-of stamp and the bounded entity slice).
from apps.gtm_mcp.src import database
from apps.gtm_mcp.src.tools import federal

# ── Durable serving location (active sink, SoR-consistent) ───────────────────
# The Gen-3 system of record is LanceDB under s3://data-sink/active/. These serving
# artifacts are derived projections of that SoR, co-located under the same active
# prefix in a dedicated ``federal_serving/`` namespace. They are NOT Lance datasets
# (no _versions/ marker) so dataset discovery skips them — they are plain JSON/Parquet
# objects addressed by R2 key, exactly the warm-serve contract the BFF wants.
BUCKET = "data-sink"
SERVING_PREFIX = "active/federal_serving"
SERVING_URI = f"s3://{BUCKET}/{SERVING_PREFIX}"

GOLD = "entity_profile_gold"

# Chart top-N. Industry + agency are top-N leaderboards — generous so the chart never
# feels truncated.
INDUSTRY_LIMIT = 50
AGENCY_LIMIT = 50

# Canonical US postal jurisdictions for the state chart. ``physical_address_state`` in
# gold is dirty free-text — it carries foreign provinces ("LIÈGE", "JALAPA", "GT"),
# fragments, and >450 distinct junk values, so the raw ``federal_spend_by_state`` group
# set hits its internal 500-cap with pollution in the tail. The map cockpit's state
# chart is a US-states vantage, so the serving artifact is filtered to the 50 states +
# DC + the five inhabited US territories. This is a SERVING-boundary cleanup (stated,
# not silent): the underlying ``federal.py`` function stays the raw deterministic
# aggregation; only the materialized chart is constrained to plottable US jurisdictions.
US_STATES = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",  # District of Columbia
    "PR", "GU", "VI", "AS", "MP",  # Puerto Rico, Guam, US Virgin Islands, American Samoa, N. Mariana Is.
})

# P1 entity-slice bound (stated, non-silent — see module docstring). Env/flag tunable.
ENTITY_MIN_LIFETIME = float(os.environ.get("FEDERAL_ENTITY_MIN_LIFETIME", "1000000"))

# The 9 list columns the map/filter path serves (the federal module's _LIST_COLUMNS).
LIST_COLUMNS = list(federal._LIST_COLUMNS)


# ── R2 client (same three-credential convention as the gateway / every worker) ──
def _s3_client():
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT (or R2_ACCOUNT_ID) — cannot reach the R2 sink.")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _put_json(s3, key: str, payload: Any) -> int:
    body = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    return len(body)


def _put_bytes(s3, key: str, body: bytes, content_type: str) -> int:
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType=content_type)
    return len(body)


def _get_json(s3, key: str) -> Any:
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(body)


# ── Gold provenance ──────────────────────────────────────────────────────────
def _profile_as_of_date() -> str | None:
    """Read the gold as-of stamp from ``entity_profile_gold`` (one scalar). Returns the
    ISO date string, or ``None`` if the column is empty. This is the "data as of X" the
    UI shows — sourced from the SoR, not synthesized here."""
    tbl = (
        database.open_dataset(GOLD)
        .scanner(columns=["profile_as_of_date"], limit=1)
        .to_table()
    )
    rows = tbl.to_pylist()
    if not rows:
        return None
    val = rows[0].get("profile_as_of_date")
    return None if val is None else str(val)


# ── Chart artifacts (call the merged functions verbatim) ─────────────────────
def _build_charts() -> dict[str, Any]:
    """Call the three chart aggregations — the imported functions ARE the SQL. The map
    cockpit's unfiltered headline charts use no predicate (the whole federal universe),
    matching the demo's "by industry" / "by state" commands."""
    industry = federal.federal_spend_by_industry(limit=INDUSTRY_LIMIT)
    state = federal.federal_spend_by_state()
    agency = federal.federal_spend_by_agency(limit=AGENCY_LIMIT)

    # Serving-boundary state cleanup (see US_STATES note): keep only plottable US
    # jurisdictions, drop the dirty foreign/free-text tail. Rows stay lifetime-desc
    # ordered (the function already sorted them); group_count is recomputed to the
    # filtered set so the chart header reports the truthful number.
    raw_state_rows = state.get("rows", [])
    clean_state_rows = [r for r in raw_state_rows if (r.get("state") or "").upper() in US_STATES]
    state = {
        **state,
        "rows": clean_state_rows,
        "group_count": len(clean_state_rows),
        "raw_group_count": state.get("group_count"),
        "note": "filtered to US states + DC + inhabited territories (dirty foreign/free-text values dropped)",
    }
    return {"industry": industry, "state": state, "agency": agency}


# ── Entity slice artifact (the P1 list/filter path) ──────────────────────────
def _build_entity_slice(min_lifetime: float) -> tuple["Any", int, int]:
    """Project the bounded map-entity slice to a pyarrow Table.

    Returns ``(table, slice_rows, full_universe_rows)``. The slice is
    ``total_lifetime_obligations >= min_lifetime`` over the 9 list columns, ordered by
    lifetime desc (so the BFF can page/slice without re-sorting). ``full_universe_rows``
    is the unbounded ``has_federal_awards = TRUE`` count — recorded so the bound is
    auditable. The Lance scanner does the predicate + projection (BTREE pushdown); no
    DuckDB needed for the slice."""
    import pyarrow.compute as pc

    ds = database.open_dataset(GOLD)
    full_universe_rows = ds.scanner(
        filter="has_federal_awards = TRUE", columns=["uei"]
    ).to_table().num_rows

    table = ds.scanner(
        filter=f"total_lifetime_obligations >= {float(min_lifetime)}",
        columns=LIST_COLUMNS,
    ).to_table()
    # Stable order: lifetime obligations desc, nulls last — the list's natural ranking.
    order = pc.sort_indices(
        table, sort_keys=[("total_lifetime_obligations", "descending")],
        null_placement="at_end",
    )
    table = table.take(order)
    return table, table.num_rows, full_universe_rows


def _table_to_parquet_bytes(table) -> bytes:
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


# ── Orchestration ────────────────────────────────────────────────────────────
def materialize(*, min_lifetime: float, dry_run: bool) -> dict[str, Any]:
    """Build every artifact and (unless dry-run) write them + the manifest to R2.
    Idempotent: each object is overwritten in place at a stable key, so re-running with
    ``--refresh`` republishes a fresh snapshot with no residue."""
    materialized_at = dt.datetime.now(dt.timezone.utc).isoformat()
    profile_as_of = _profile_as_of_date()

    charts = _build_charts()
    entity_table, slice_rows, full_universe_rows = _build_entity_slice(min_lifetime)

    metrics = {
        "materialized_at": materialized_at,
        "profile_as_of_date": profile_as_of,
        "charts": {
            "spend_by_industry_rows": charts["industry"]["returned"],
            "spend_by_industry_groups": charts["industry"]["group_count"],
            "spend_by_state_rows": charts["state"]["group_count"],
            "spend_by_agency_rows": charts["agency"]["returned"],
            "spend_by_agency_groups": charts["agency"]["group_count"],
        },
        "entities": {
            "min_lifetime_obligation": min_lifetime,
            "slice_rows": slice_rows,
            "full_has_awards_universe": full_universe_rows,
            "columns": LIST_COLUMNS,
        },
    }

    if dry_run:
        metrics["dry_run"] = True
        return metrics

    s3 = _s3_client()
    keys = {
        "industry": f"{SERVING_PREFIX}/charts/spend_by_industry.json",
        "state": f"{SERVING_PREFIX}/charts/spend_by_state.json",
        "agency": f"{SERVING_PREFIX}/charts/spend_by_agency.json",
        "entities": f"{SERVING_PREFIX}/entities/map_entities.parquet",
        "manifest": f"{SERVING_PREFIX}/manifest.json",
    }

    # Each chart artifact carries its own provenance stamp so a consumer that reads only
    # one chart still knows the data vintage.
    stamp = {"materialized_at": materialized_at, "profile_as_of_date": profile_as_of}
    bytes_written: dict[str, int] = {}
    bytes_written["industry"] = _put_json(
        s3, keys["industry"], {**charts["industry"], "_provenance": stamp}
    )
    bytes_written["state"] = _put_json(
        s3, keys["state"], {**charts["state"], "_provenance": stamp}
    )
    bytes_written["agency"] = _put_json(
        s3, keys["agency"], {**charts["agency"], "_provenance": stamp}
    )
    bytes_written["entities"] = _put_bytes(
        s3, keys["entities"], _table_to_parquet_bytes(entity_table),
        "application/vnd.apache.parquet",
    )

    manifest = {
        **metrics,
        "artifacts": {
            "spend_by_industry": {"uri": f"{SERVING_URI}/charts/spend_by_industry.json",
                                  "bytes": bytes_written["industry"]},
            "spend_by_state": {"uri": f"{SERVING_URI}/charts/spend_by_state.json",
                               "bytes": bytes_written["state"]},
            "spend_by_agency": {"uri": f"{SERVING_URI}/charts/spend_by_agency.json",
                                "bytes": bytes_written["agency"]},
            "map_entities": {"uri": f"{SERVING_URI}/entities/map_entities.parquet",
                             "bytes": bytes_written["entities"]},
        },
    }
    bytes_written["manifest"] = _put_json(s3, keys["manifest"], manifest)
    metrics["artifacts"] = manifest["artifacts"]
    metrics["manifest_uri"] = f"{SERVING_URI}/manifest.json"
    return metrics


# ── Read-back verification (independent of the write path) ───────────────────
def verify() -> dict[str, Any]:
    """Read the published artifacts back from R2 and return the manifest + the real
    spend-by-industry top-10 (label + lifetime spend). Proves the warm artifact is
    self-contained and correct, independent of the build path."""
    s3 = _s3_client()
    manifest = _get_json(s3, f"{SERVING_PREFIX}/manifest.json")
    industry = _get_json(s3, f"{SERVING_PREFIX}/charts/spend_by_industry.json")
    top10 = industry["rows"][:10]
    # Independent Parquet read-back of the entity slice (row count + schema).
    import pyarrow.parquet as pq

    body = s3.get_object(Bucket=BUCKET, Key=f"{SERVING_PREFIX}/entities/map_entities.parquet")[
        "Body"
    ].read()
    ent = pq.read_table(io.BytesIO(body))
    return {
        "manifest": manifest,
        "spend_by_industry_top10": top10,
        "entity_slice_readback_rows": ent.num_rows,
        "entity_slice_schema": [f"{f.name}:{f.type}" for f in ent.schema],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--refresh", action="store_true",
                   help="(re)materialize and publish the artifacts to R2 (default action).")
    p.add_argument("--dry-run", action="store_true",
                   help="compute + print metrics, write NOTHING.")
    p.add_argument("--verify", action="store_true",
                   help="read the published artifacts back and print the spend-by-industry top-10.")
    p.add_argument("--entity-min-lifetime", type=float, default=ENTITY_MIN_LIFETIME,
                   help=f"lifetime-obligation floor for the entity slice (default ${ENTITY_MIN_LIFETIME:,.0f}).")
    args = p.parse_args(argv)

    if args.verify and not (args.refresh or args.dry_run):
        print(json.dumps(verify(), indent=2, default=str))
        return 0

    metrics = materialize(min_lifetime=args.entity_min_lifetime, dry_run=args.dry_run)
    print(json.dumps(metrics, indent=2, default=str))

    if args.verify:
        print("\n── read-back verification ──")
        print(json.dumps(verify(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
