#!/usr/bin/env python3
"""Catalog of what's built in the data factory (core-x, Gen-3 clean room).

Probes the LIVE R2 system of record + the HQX control-plane Postgres and emits a
markdown inventory of what exists to query. This is NOT a failure dashboard.

Sections:
  - Lance datasets        — R2 ``data-sink/active/<dataset>/`` (flat; the system of record)
  - Bridges & crosswalks  — the ``active/bridge_*`` and ``active/crosswalk_*`` datasets
  - SAM.gov Opportunities — R2 ``data-sink/sam-gov-opps/`` (live reference feed)
  - Active ingest sources — derived from the ``ops.*_runs`` ledgers in HQX Postgres;
                            a source whose latest run is within ACTIVE_WINDOW_DAYS is active

Retired and intentionally NOT touched (Gen-2):
  - Polaris is dead — does not scan ``dex-raw-landing-zone/polaris-warehouse/``.
  - DEX Postgres is dead — does not read ``ops.bridges`` / ``ops.data_sources``.
  - HQX Postgres remains the permanent control plane (``ops.*_runs`` ledgers).

Usage (core-x — no local venv required; deps resolved ephemerally by uv):

    doppler run -p core-x -c prd -- \\
        uv run --no-project --with boto3 --with 'psycopg[binary]' \\
        python3 /Users/benjamincrane/core-x/scripts/data-factory-catalog.py

Required env (from Doppler core-x/prd):
  - R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY   (R2 data plane)
  - HQX_DB_URL_POOLED                                       (ops.*_runs ledgers)
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import psycopg

VAULT = Path.home() / "Desktop" / "hq"
STATE_DIR = VAULT / "state"
OUTPUT_PATH = STATE_DIR / "factory-catalog.md"

R2_BUCKET = "data-sink"
ACTIVE_PREFIX = "active/"
SAM_OPPS_PREFIX = "sam-gov-opps/"
LANDING_PREFIX = "landing/"

# Datasets whose name carries one of these prefixes are cross-source identity products.
BRIDGE_PREFIXES = ("bridge_", "crosswalk_")
# A source whose latest ops.*_runs row is within this window counts as "active".
ACTIVE_WINDOW_DAYS = 90
# Priority order for the per-row "when did this run happen" timestamp.
RUN_TS_COLS = ("recorded_at", "completed_at", "started_at", "created_at")

# Derived spine artifacts + integrity harnesses per feed. These are CODE artifacts (Modal apps,
# plan docs) — not discoverable from R2/Postgres — so they are registered here, letting
# /factory-state surface what anchors and verifies each feed's deterministic ID spine. Extend
# as spines are built for other feeds.
SPINE_REGISTRY: dict[str, dict[str, str]] = {
    "msha": {
        "anchors": [
            ("msha_site_master", "1 row per MINE_ID — current controller/operator (Option D: "
             "latest-start-wins, NULL on genuine ties) + pre-computed GTM signal rollups"),
            ("msha_contractor_master", "1 row per CONTRACTOR_ID — the contractor third spine's full "
             "cross-spine footprint (registry production + violations + accidents + exposure samples)"),
        ],
        "harness": "modal run pipelines/ingest_msha/verify_spine.py::check_local",
        "harness_desc": "asserts every spine resolution edge, present-key index coverage, ID "
                        "hygiene (whitespace/lowercase), and the SCD tie-collapse; fails on drift",
        "contract": "docs/plans/MSHA_ID_SPINE_EXECUTION_PLAN.md §9 (join contract)",
    },
}


def _count(n: int, noun: str) -> str:
    return f"{n} {noun}" if n == 1 else f"{n} {noun}s"


def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )


def list_immediate_children(client, prefix: str) -> list[str]:
    """Sorted immediate child 'directory' names one level under data-sink/<prefix>."""
    paginator = client.get_paginator("list_objects_v2")
    out: list[str] = []
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix, Delimiter="/"):
        for p in (page.get("CommonPrefixes") or []):
            name = p["Prefix"].removeprefix(prefix).rstrip("/")
            if name and not name.startswith("_"):
                out.append(name)
    return sorted(out)


def describe_prefix(client, prefix: str) -> tuple[list[str], bool]:
    """(children, is_leaf_dataset). ``children`` = immediate sub-dataset dirs; if there are
    none but objects exist directly under the prefix, the prefix is itself one Lance dataset."""
    children = list_immediate_children(client, prefix)
    if children:
        return children, False
    resp = client.list_objects_v2(Bucket=R2_BUCKET, Prefix=prefix, MaxKeys=1)
    return [], resp.get("KeyCount", 0) > 0


def list_landing_zones(client) -> dict[str, list[str]]:
    """Each ``landing/<feed>/`` raw drop zone → its member files. This is the operator's
    materialize-EVERYTHING handoff area; surfacing it here means never hand-listing R2 to see
    what was dropped. Exact landing→active parity per feed is checked by that feed's mirror
    dry-run (e.g. ``materialize_msha_mirror.py::show``) — this is the inventory half."""
    out: dict[str, list[str]] = {}
    paginator = client.get_paginator("list_objects_v2")
    for feed in list_immediate_children(client, LANDING_PREFIX):
        prefix = f"{LANDING_PREFIX}{feed}/"
        members: list[str] = []
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
            for o in (page.get("Contents") or []):
                rel = o["Key"][len(prefix):]
                if rel and not rel.endswith("/") and not rel.endswith(".keep"):
                    members.append(rel)
        out[feed] = sorted(members)
    return out


def list_ingest_sources() -> list[dict] | None:
    """Active ingest sources derived from the ops.*_runs ledgers in the HQX control plane.

    For each ``ops.<name>_runs`` table, take the most-recent run timestamp (the GREATEST of
    whichever run-timestamp columns the table has) and the total run count. Returns a list of
    {source, last_run, runs}. Degrades to None (section omitted) if HQX is unreachable."""
    url = os.environ.get("HQX_DB_URL_POOLED")
    if not url:
        print("[warn] HQX_DB_URL_POOLED unset; omitting ingest-sources section", file=sys.stderr)
        return None
    try:
        with psycopg.connect(url, connect_timeout=10) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='ops' ORDER BY table_name"
            )
            run_tables = [r[0] for r in cur.fetchall() if r[0].endswith("_runs")]

            rows: list[dict] = []
            for t in run_tables:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='ops' AND table_name=%s",
                    (t,),
                )
                cols = {r[0] for r in cur.fetchall()}
                present = [c for c in RUN_TS_COLS if c in cols]
                # table/column names come from information_schema (trusted) and a fixed
                # allowlist — safe to inline as identifiers.
                if not present:
                    ts_expr = "NULL::timestamptz"
                elif len(present) == 1:
                    ts_expr = present[0]
                else:
                    ts_expr = "GREATEST(" + ", ".join(present) + ")"
                cur.execute(f'SELECT max({ts_expr}), count(*) FROM ops."{t}"')
                last_run, n = cur.fetchone()
                rows.append({"source": t[:-len("_runs")], "last_run": last_run, "runs": n})
            return rows
    except Exception as e:
        print(
            f"[warn] ingest sources unavailable ({type(e).__name__}: {str(e).splitlines()[0]})",
            file=sys.stderr,
        )
        return None


def format_catalog(
    active_datasets: list[str],
    sam_opps: tuple[list[str], bool],
    ingest_sources: list[dict] | None,
    landing_zones: dict[str, list[str]] | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    bridges = [d for d in active_datasets if d.startswith(BRIDGE_PREFIXES)]
    sources = [d for d in active_datasets if not d.startswith(BRIDGE_PREFIXES)]

    out: list[str] = [
        "# Data factory — what's built",
        "",
        f"_Refreshed: {now.isoformat(timespec='seconds')} · system of record: s3://data-sink/active/_",
        "",
        "## Lance datasets (system of record — data-sink/active/)",
        "",
        f"_({_count(len(sources), 'dataset')})_",
        "",
    ]
    out += [f"- `{d}`" for d in sources]
    out += [
        "",
        "## Bridges & crosswalks (cross-source identity)",
        "",
        f"_({_count(len(bridges), 'dataset')})_",
        "",
    ]
    out += [f"- `{d}`" for d in bridges]
    out.append("")

    sam_children, sam_is_leaf = sam_opps
    out += ["## SAM.gov Opportunities reference feed (data-sink/sam-gov-opps/)", ""]
    if sam_children:
        out += [f"_({_count(len(sam_children), 'dataset')})_", ""]
        out += [f"- `sam-gov-opps/{d}`" for d in sam_children]
    elif sam_is_leaf:
        out += ["_(1 dataset — the feed root is itself a Lance dataset)_", "", "- `sam-gov-opps/`"]
    else:
        out += ["_(empty)_"]
    out.append("")

    out += ["## Landing zones (raw drop area — data-sink/landing/<feed>/)", ""]
    if landing_zones:
        total_members = sum(len(v) for v in landing_zones.values())
        out += [f"_({_count(len(landing_zones), 'feed')} · {total_members} members · "
                "materialize-everything handoff)_", "",
                "| feed | members | sample |", "|---|---:|---|"]
        for feed in sorted(landing_zones):
            members = landing_zones[feed]
            sample = ", ".join(members[:6])
            if len(members) > 6:
                sample += f", … (+{len(members) - 6} more)"
            out.append(f"| `{feed}` | {len(members)} | {sample or '—'} |")
        out += ["", "_Exact landing→active parity per feed is checked by that feed's mirror "
                "dry-run (e.g. `modal run pipelines/ingest_msha/materialize_msha_mirror.py::show`)._"]
    else:
        out += ["_(none)_"]
    out.append("")

    out += ["## ID spines — derived anchors & integrity harnesses", ""]
    if SPINE_REGISTRY:
        out += [f"_({_count(len(SPINE_REGISTRY), 'feed')} with a hardened deterministic ID spine)_", ""]
        for feed in sorted(SPINE_REGISTRY):
            s = SPINE_REGISTRY[feed]
            out += [
                f"**`{feed}`** — {_count(len(s['anchors']), 'anchor')}:",
                *[f"  - `{nm}` — {desc}" for nm, desc in s["anchors"]],
                f"  - harness: `{s['harness']}`",
                f"    - {s['harness_desc']}",
                f"  - contract: `{s['contract']}`",
                "",
            ]
    else:
        out += ["_(none)_", ""]

    if ingest_sources is not None:
        cutoff = now - timedelta(days=ACTIVE_WINDOW_DAYS)
        floor = datetime.min.replace(tzinfo=timezone.utc)
        ranked = sorted(ingest_sources, key=lambda r: r["last_run"] or floor, reverse=True)
        n_active = sum(1 for r in ranked if r["last_run"] and r["last_run"] >= cutoff)
        out += [
            f"## Active ingest sources (ops.*_runs in HQX · active = run ≤ {ACTIVE_WINDOW_DAYS}d)",
            "",
            f"_({n_active} active / {len(ranked)} total run-ledgers)_",
            "",
            "| source | last run (UTC) | runs | status |",
            "|---|---|---:|---|",
        ]
        for r in ranked:
            lr = r["last_run"].isoformat(timespec="seconds") if r["last_run"] else "—"
            active = bool(r["last_run"]) and r["last_run"] >= cutoff
            out.append(f"| `{r['source']}` | {lr} | {r['runs']} | {'● active' if active else '○ stale'} |")
        out.append("")

    return "\n".join(out)


def main() -> int:
    client = r2_client()
    active = list_immediate_children(client, ACTIVE_PREFIX)
    sam_opps = describe_prefix(client, SAM_OPPS_PREFIX)
    landing_zones = list_landing_zones(client)
    ingest_sources = list_ingest_sources()

    catalog = format_catalog(active, sam_opps, ingest_sources, landing_zones)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(catalog)
    print(catalog)
    return 0


if __name__ == "__main__":
    sys.exit(main())
