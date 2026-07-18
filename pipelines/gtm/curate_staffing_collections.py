"""Curate the STAFFING-NATIVE market-collection family → HQX pg + Lance.

A separate collection family organized around how staffing firms think about
what they staff ("bodies supplied", PSC-family-anchored), per the 2026-07-18
collections-fit analysis. The original 22 ``gtm.market_collections`` are LEFT
UNTOUCHED — they serve a different organizing purpose; pair overlap ACROSS the
two families is expected and fine. Disjointness holds WITHIN this family only
(every pair belongs to exactly one staffing collection, first-claim by the
priority order below).

The 8 collections:
  Copied verbatim from the existing staffing-shaped five:
    medical-clinical-staffing · federal-it-staffing ·
    engineering-technical-staffing · administrative-office-support-staffing
  Extracted + widened (new):
    finance-accounting-staffing        PSC R703/R704/R705/R710/R707, NAICS open
    logistics-supply-chain-staffing    PSC R706 ∪ AD25/AD26 (NAICS open) ∪
                                       logistics NAICS × S215/V112/V119/V999
    light-industrial-trades-labor      PSC J0xx (equipment maint) ∪ Z2xx
                                       (facility maint) ∪ temp-help 5613xx ×
                                       production/warehouse PSC families
  Reshaped (new remainder):
    program-management-support-staffing-core = the original program-mgmt
    catch-all MINUS the pairs claimed by finance + logistics above.

New pair sets are grounded in observed awards: a pair enters only with
FY23–25 prime obligations >= $100k (measured against the serving sidecar
artifact, recorded in params). Storage: ``gtm.staffing_market_collections``
(HQX pg, same row shape as market_collections) + Lance
``s3://data-sink/active/staffing_market_collections/`` (1 row/(slug,naics,psc),
BTREE all three).

Run:
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/curate_staffing_collections.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request

import lance  # noqa: F401  (via publisher)
import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

DATASET_URI = "s3://data-sink/active/staffing_market_collections/"
FLOOR = 100_000
WINDOW = "FY23-25 (2022-10-01 -> 2025-09-30)"

COPIED = [
    "medical-clinical-staffing",
    "federal-it-staffing",
    "engineering-technical-staffing",
    "administrative-office-support-staffing",
]
FIN_PSCS = ("R703", "R704", "R705", "R710", "R707")
LOG_PSCS_OPEN = ("R706", "AD25", "AD26")
LOG_NAICS_PREFIX = ("4931", "488510", "541614")
LOG_PSCS_NAICS_GATED = ("S215", "V112", "V119", "V999")
LI_TEMP_NAICS_PREFIX = "5613"
LI_TEMP_PSC_PREFIXES = ("S2", "39", "35", "37")  # ops-support + mat-handling equip families


def _sidecar(sql: str) -> list:
    tok = subprocess.check_output(
        ["doppler", "secrets", "get", "QUERY_SIDECAR_TOKEN", "-p", "core-x", "-c", "prd", "--plain"],
        text=True).strip()
    req = urllib.request.Request(
        "https://query-sidecar-api.onrender.com/api/v1/sql",
        data=json.dumps({"sql": sql, "limit": 50000}).encode(),
        headers={"Authorization": f"Bearer {tok}", "content-type": "application/json"})
    body = json.load(urllib.request.urlopen(req, timeout=300))
    assert not body.get("truncated"), "sidecar result truncated — tighten the recipe"
    return body["rows"], body["artifact"]


def _pairs(where: str) -> tuple[set[tuple[str, str]], str]:
    rows, artifact = _sidecar(
        "SELECT naics_code, psc_code FROM txn_events_combo "
        f"WHERE fy BETWEEN 2023 AND 2025 AND naics_code IS NOT NULL AND psc_code IS NOT NULL "
        f"AND ({where}) GROUP BY 1,2 HAVING sum(obligation) >= {FLOOR}")
    return {(r[0], r[1]) for r in rows}, artifact


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _psql(sql: str) -> str:
    return subprocess.check_output(
        ["psql", os.environ["HQX_DB_URL_POOLED"], "-Atc", sql], text=True).strip()


def main() -> None:
    # 1. source pair sets from the original family (read-only)
    src = {}
    for slug in COPIED + ["program-management-support-staffing"]:
        raw = _psql(
            "SELECT params->'combo_pairs' FROM gtm.market_collections "
            f"WHERE slug = '{slug}' AND status = 'active'")
        src[slug] = {(n, p) for n, p in json.loads(raw)}
        print(f"source {slug}: {len(src[slug])} pairs")

    # 2. new sets, observed-award-grounded
    in_list = ",".join(f"'{p}'" for p in FIN_PSCS)
    fin, artifact = _pairs(f"psc_code IN ({in_list})")
    log_open, _ = _pairs("psc_code IN (" + ",".join(f"'{p}'" for p in LOG_PSCS_OPEN) + ")")
    log_gated, _ = _pairs(
        "psc_code IN (" + ",".join(f"'{p}'" for p in LOG_PSCS_NAICS_GATED) + ") AND ("
        + " OR ".join(f"naics_code LIKE '{n}%'" for n in LOG_NAICS_PREFIX) + ")")
    logistics = log_open | log_gated
    li_maint, _ = _pairs("psc_code LIKE 'J0%' OR psc_code LIKE 'Z2%'")
    li_temp, _ = _pairs(
        f"naics_code LIKE '{LI_TEMP_NAICS_PREFIX}%' AND ("
        + " OR ".join(f"psc_code LIKE '{p}%'" for p in LI_TEMP_PSC_PREFIXES) + ")")
    light_industrial = li_maint | li_temp

    # 3. family assembly — disjoint within the family, first-claim priority
    order: list[tuple[str, str, set]] = [
        ("medical-clinical-staffing", "Medical & Clinical Staffing", src["medical-clinical-staffing"]),
        ("federal-it-staffing", "Federal IT Staffing", src["federal-it-staffing"]),
        ("engineering-technical-staffing", "Engineering & Technical Staffing", src["engineering-technical-staffing"]),
        ("administrative-office-support-staffing", "Administrative & Office Support Staffing", src["administrative-office-support-staffing"]),
        ("finance-accounting-staffing", "Finance & Accounting Staffing", fin),
        ("logistics-supply-chain-staffing", "Logistics & Supply Chain Staffing", logistics),
        ("light-industrial-trades-labor", "Light Industrial & Trades Labor", light_industrial),
        ("program-management-support-staffing-core", "Program & Management Support Staffing (core)", src["program-management-support-staffing"]),
    ]
    DESC = {
        "medical-clinical-staffing": "Clinicians into government settings (copied from the original family).",
        "federal-it-staffing": "IT firms selling people under R-codes (copied from the original family).",
        "engineering-technical-staffing": "SETA/engineering-technical embeds (copied from the original family).",
        "administrative-office-support-staffing": "Admin/clerical/office-support placements, R6xx-anchored (copied).",
        "finance-accounting-staffing": "Accountants/auditors/financial analysts supplied: PSC R703/R704/R705/R710/R707, NAICS open, extracted from the program-mgmt catch-all and widened to all observed winners.",
        "logistics-supply-chain-staffing": "Logistics/supply-chain professionals supplied: R706 + AD25/AD26 open; S215/V-codes gated to logistics NAICS.",
        "light-industrial-trades-labor": "Placed production/trades labor: equipment-maintenance J0xx, facility-maintenance Z2xx, temp-help 5613xx into ops-support families.",
        "program-management-support-staffing-core": "The program-mgmt catch-all MINUS the pairs claimed by finance and logistics above (the true PM/comms/acquisition core).",
    }
    claimed: set = set()
    rows_pg = []
    lance_rows = []
    for slug, title, pairs in order:
        mine = sorted(pairs - claimed)
        claimed |= set(mine)
        params = {
            "definition_type": "explicit_pairs",
            "combo_pairs": [list(p) for p in mine],
            "window": WINDOW,
            "artifact": artifact,
            "floor": f"pair enters with >= ${FLOOR:,} FY23-25 prime obligations (new sets); copied sets inherit their original curation",
            "family": "staffing_native",
            "disjointness": "within the staffing family only; overlap with gtm.market_collections is expected",
            "source": "session 2026-07-18 collections-fit analysis; original 22 untouched",
        }
        rows_pg.append((slug, title, DESC[slug], json.dumps(params)))
        lance_rows.extend({"slug": slug, "naics_code": n, "psc_code": p} for n, p in mine)
        print(f"{slug}: {len(mine)} pairs")

    # 4. write HQX pg (idempotent replace of this family's rows)
    _psql(
        "CREATE TABLE IF NOT EXISTS gtm.staffing_market_collections ("
        "slug text PRIMARY KEY, title text NOT NULL, description text NOT NULL, "
        "params jsonb NOT NULL, status text NOT NULL DEFAULT 'active', "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "updated_at timestamptz NOT NULL DEFAULT now())")
    for slug, title, desc, params in rows_pg:
        _psql(
            "INSERT INTO gtm.staffing_market_collections (slug, title, description, params) "
            f"VALUES ('{slug}', '{title.replace(chr(39), chr(39)*2)}', "
            f"'{desc.replace(chr(39), chr(39)*2)}', '{params.replace(chr(39), chr(39)*2)}'::jsonb) "
            "ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title, "
            "description = EXCLUDED.description, params = EXCLUDED.params, updated_at = now()")
    print(f"pg upserted {len(rows_pg)} collections")

    # 5. materialize Lance
    tbl = pa.Table.from_pylist(lance_rows, schema=pa.schema(
        [("slug", pa.string()), ("naics_code", pa.string()), ("psc_code", pa.string())]))
    ds = write_indexed_dataset(
        tbl, DATASET_URI,
        [("slug", "BTREE"), ("naics_code", "BTREE"), ("psc_code", "BTREE")],
        _r2_storage_options())
    print(f"published {DATASET_URI} rows={ds.count_rows():,}")


if __name__ == "__main__":
    main()
