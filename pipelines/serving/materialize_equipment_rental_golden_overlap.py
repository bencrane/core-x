"""Materializer — equipment_rental_golden_overlap Lance SoR.

The GOLDEN TRIPLE OVERLAP: equipment-rental candidate firms that are (1) geo-placed within 50mi
of (2) active federal construction demand in a PSC that (3) their scraped inventory can actually
supply. The capability-qualified target list for the rental GTM motion.

Join executed in scripts/golden_overlap_probe.py:
    govcon_firm_construction_proximity  (firm_domain x psc_code -> nearby_award_count / value)
    JOIN equipment_matchmaking          (domain_norm -> supported_pscs)
    ON firm_domain == domain_norm AND psc_code IN supported_pscs
The probe emits reports/golden_overlap_firm_level.jsonl; this materializer lands it to Lance.
Deterministic I/O only — no LLM/API calls.

SoR: s3://data-sink/active/equipment_rental_golden_overlap/  (native Lance v2.1)

Schema:
    firm_domain                  VARCHAR  NOT NULL  (PK — BTREE)
    qualified_pscs               LIST<VARCHAR> NOT NULL  (PSCs near AND serviceable)
    qualified_psc_count          INT32    NOT NULL  (BITMAP)
    qualified_nearby_award_count INT32    NOT NULL  (nearby active projects the firm can supply)
    qualified_value_exposure     DOUBLE   NOT NULL  (Σ award value over qualified pairs; double-counts shared awards)
    mapped_nearby_award_count    INT32    NOT NULL  (nearby demand in the 15 mapped PSCs, regardless of capability)
    all_nearby_award_count       INT32    NOT NULL  (nearby demand across all 333 PSCs)
    capability_capture_ratio     DOUBLE   NOT NULL  (qualified / mapped)
    qualified_psc_demand         VARCHAR  NOT NULL  (JSON {psc: nearby_count})
    materialized_at              TIMESTAMP(us, UTC) NOT NULL

Run:
    doppler run -p core-x -c prd -- python3 scripts/golden_overlap_probe.py            # produce JSONL
    doppler run -p core-x -c prd -- python3 pipelines/serving/materialize_equipment_rental_golden_overlap.py
"""

from __future__ import annotations

import datetime as dt
import json
import os

import lance
import pyarrow as pa


_BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                     "reports")
SRC_JSONL = os.environ.get("GOLDEN_OVERLAP_JSONL", os.path.join(_BASE, "golden_overlap_firm_level.jsonl"))
DATASET_URI = os.environ.get("GOLDEN_OVERLAP_URI", "s3://data-sink/active/equipment_rental_golden_overlap/")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

INDEXES: dict[str, list[str]] = {
    "BTREE": ["firm_domain"],
    "BITMAP": ["qualified_psc_count"],
}


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (and R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _schema() -> pa.Schema:
    ts = pa.timestamp("us", tz="UTC")
    return pa.schema([
        pa.field("firm_domain",                  pa.string(),           nullable=False),
        pa.field("qualified_pscs",               pa.list_(pa.string()), nullable=False),
        pa.field("qualified_psc_count",          pa.int32(),            nullable=False),
        pa.field("qualified_nearby_award_count", pa.int32(),            nullable=False),
        pa.field("qualified_value_exposure",     pa.float64(),          nullable=False),
        pa.field("mapped_nearby_award_count",    pa.int32(),            nullable=False),
        pa.field("all_nearby_award_count",       pa.int32(),            nullable=False),
        pa.field("capability_capture_ratio",     pa.float64(),          nullable=False),
        pa.field("qualified_psc_demand",         pa.string(),           nullable=False),
        pa.field("materialized_at",              ts,                    nullable=False),
    ])


def _load() -> list[dict]:
    if not os.path.exists(SRC_JSONL):
        raise RuntimeError(f"{SRC_JSONL} missing — run scripts/golden_overlap_probe.py first")
    now = dt.datetime.now(dt.timezone.utc)
    rows, seen = [], set()
    with open(SRC_JSONL, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            dn = r["firm_domain"]
            if dn in seen:
                continue
            seen.add(dn)
            pscs = list(r.get("qualified_pscs") or [])
            rows.append({
                "firm_domain": dn,
                "qualified_pscs": pscs,
                "qualified_psc_count": len(pscs),
                "qualified_nearby_award_count": int(r.get("qualified_nearby_award_count") or 0),
                "qualified_value_exposure": float(r.get("qualified_value_exposure") or 0.0),
                "mapped_nearby_award_count": int(r.get("mapped_nearby_award_count") or 0),
                "all_nearby_award_count": int(r.get("all_nearby_award_count") or 0),
                "capability_capture_ratio": float(r.get("capability_capture_ratio") or 0.0),
                "qualified_psc_demand": json.dumps(r.get("qualified_psc_demand") or {}, ensure_ascii=False),
                "materialized_at": now,
            })
    return rows


def _create_indexes(so: dict) -> None:
    ds = lance.dataset(DATASET_URI, storage_options=so)
    for index_type, cols in INDEXES.items():
        for col in cols:
            ds.create_scalar_index(col, index_type=index_type)
            print(f"  {index_type:<6} ✓ equipment_rental_golden_overlap.{col}")


def _verify(so: dict) -> dict:
    import pyarrow.compute as pc

    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["firm_domain"])
    distinct = pc.count_distinct(keys.column("firm_domain")).as_py()
    if n != distinct:
        raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(firm_domain)={distinct}")
    sample = next((v for v in keys.column("firm_domain").to_pylist() if v), None)
    probe = ds.scanner(columns=["firm_domain"],
                       filter=f"firm_domain = '{sample}'").to_table().num_rows if sample else -1
    indexes = sorted(
        ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix))
        for ix in ds.list_indices()
    )
    return {"uri": DATASET_URI, "rows": n, "distinct_firm_domain": distinct,
            "schema": [f.name for f in ds.schema], "indexes": indexes,
            f"probe_firm={sample!r}": probe}


def main() -> None:
    so = _r2_storage_options()
    rows = _load()
    schema = _schema()
    cols = {f.name: [r[f.name] for r in rows] for f in schema}
    table = pa.Table.from_pydict(cols, schema=schema)
    print(f"building Lance table — {table.num_rows} golden firms, {len(table.schema)} cols")

    lance.write_dataset(
        table, DATASET_URI, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    print(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")
    _create_indexes(so)
    print(json.dumps(_verify(so), indent=2, default=str))


if __name__ == "__main__":
    main()
