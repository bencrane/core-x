"""Reference materializer — PSC (Federal Product/Service Code) → Equipment mapping.

Curated, hand-authored deterministic mapping from federal PSC codes to the plain-English
description of the physical work AND the heavy equipment necessitated by that work. Used
downstream by the Equipment Rental GTM motion to (a) translate USAspending awards into
equipment demand signal and (b) seed contextual LLM email copy.

SoR contract:
    s3://data-sink/active/reference/psc_equipment_mapping/  (native Lance v2.1)

Schema:
    psc_code                       VARCHAR  NOT NULL   (PK — 4-char federal code, BTREE-indexed)
    psc_name                       VARCHAR  NOT NULL   (official federal classification name)
    work_description_plain_english VARCHAR  NOT NULL   (LLM-priming description of the work)
    required_equipment             LIST<VARCHAR> NOT NULL  (machinery needed to execute the work)

Re-running is a full overwrite of the entire dataset — seed data is the authoritative source.
psc_code uniqueness is asserted on read-back.

Run:
    doppler run -p core-x -c prd -- python3 pipelines/reference/materialize_psc_equipment.py
"""

from __future__ import annotations

import os

import lance
import pyarrow as pa


DATASET_URI = os.environ.get(
    "PSC_EQUIPMENT_URI",
    "s3://data-sink/active/reference/psc_equipment_mapping/",
)

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

INDEXES: dict[str, list[str]] = {
    "BTREE": ["psc_code"],
}


SEED_DATA: list[dict] = [
    {
        "psc_code": "Z2AA",
        "psc_name": "Repair or Alteration of Office Buildings",
        "work_description_plain_english": "renovating existing federal office spaces and interior infrastructure",
        "required_equipment": ["Scissor Lifts", "Boom Lifts", "Telehandlers", "Portable Generators", "Light Towers", "Skid Steers"],
    },
    {
        "psc_code": "Y1DA",
        "psc_name": "Construction of Hospitals and Infirmaries",
        "work_description_plain_english": "building new medical facilities from the ground up, including deep foundations and structural steel",
        "required_equipment": ["Excavators", "Bulldozers", "Rough-Terrain Cranes", "Crawler Cranes", "High-Reach Telehandlers"],
    },
    {
        "psc_code": "Z1DA",
        "psc_name": "Maintenance of Hospitals and Infirmaries",
        "work_description_plain_english": "major preventative upkeep of hospital infrastructure and critical power/HVAC systems",
        "required_equipment": ["Towable Generators", "Temporary Chiller Units", "Boom Lifts", "Electric Scissor Lifts"],
    },
    {
        "psc_code": "Z2DA",
        "psc_name": "Repair or Alteration of Hospitals and Infirmaries",
        "work_description_plain_english": "invasive structural renovations and heavy interior demolition of medical wings",
        "required_equipment": ["Skid Steers", "Grapple Buckets", "Telehandlers", "Electric Slab Scissor Lifts"],
    },
    {
        "psc_code": "Y1LB",
        "psc_name": "Construction of Highways Roads Streets and Bridges",
        "work_description_plain_english": "cutting new roads, heavy grading, and laying massive amounts of asphalt or concrete",
        "required_equipment": ["Motor Graders", "Smooth Drum Compactors", "Pneumatic Rollers", "Wheel Loaders", "Articulated Dump Trucks", "Water Trucks", "Asphalt Pavers", "Milling Machines"],
    },
    {
        "psc_code": "Z1LB",
        "psc_name": "Maintenance or Repair of Highways Roads Streets and Bridges",
        "work_description_plain_english": "resurfacing existing highways, bridge deck rehabilitation, and pothole mitigation",
        "required_equipment": ["Milling Machines", "Asphalt Pavers", "Material Transfer Vehicles", "Smooth Drum Compactors", "Pneumatic Rollers", "Sweepers", "Water Trucks", "Variable Message Boards"],
    },
    {
        "psc_code": "Y1PC",
        "psc_name": "Construction of Unimproved Real Property (Land)",
        "work_description_plain_english": "massive land reshaping, site preparation, and building levees or retention ponds",
        "required_equipment": ["Bulldozers", "Pull-Type Scrapers", "Excavators", "Articulated Off-Road Dump Trucks", "Sheepsfoot Compactors"],
    },
    {
        "psc_code": "Y1NE",
        "psc_name": "Construction of Water Supply Facilities",
        "work_description_plain_english": "deep trenching and laying miles of massive underground municipal water mains",
        "required_equipment": ["Crawler Excavators", "Pipe Layers", "Sideboom Dozers", "Wheel Loaders", "Trench Boxes", "Heavy Shoring Equipment"],
    },
    {
        "psc_code": "Y1KD",
        "psc_name": "Construction of Mine Subsidence Control Facilities",
        "work_description_plain_english": "drilling into collapsing abandoned underground mines and pumping them full of grout or concrete",
        "required_equipment": ["Rotary Drilling Rigs", "Grout Pumps", "Concrete Pumps", "Dry-Bulk Trailers", "Skid Steers", "Wheel Loaders"],
    },
    {
        "psc_code": "Y1PZ",
        "psc_name": "Construction of Other Non-Building Facilities",
        "work_description_plain_english": "complex heavy civil engineering, border wall construction, and large perimeter security installs",
        "required_equipment": ["Excavators", "Bulldozers", "Articulated Dump Trucks", "Rough-Terrain Cranes"],
    },
    {
        "psc_code": "Z2KA",
        "psc_name": "Repair or Alteration of Dams / Dredging Facilities",
        "work_description_plain_english": "complex wet civil engineering, stabilizing massive embankments, and replacing heavy steel sluice gates",
        "required_equipment": ["Long-Reach Excavators", "Amphibious Excavators", "Swamp Buggies", "Industrial Dewatering Pumps", "Rough-Terrain Cranes", "Articulated Off-Road Dump Trucks"],
    },
    {
        "psc_code": "Z1KF",
        "psc_name": "Maintenance or Repair of Dredging Facilities",
        "work_description_plain_english": "clearing silt from locks and deepening shipping channels from the shoreline",
        "required_equipment": ["Long-Reach Excavators", "Amphibious Excavators", "Swamp Buggies", "Bulldozers", "Track Loaders", "Industrial Pumps"],
    },
    {
        "psc_code": "P400",
        "psc_name": "Demolition of Buildings",
        "work_description_plain_english": "tearing down federal buildings and clearing massive amounts of structural debris",
        "required_equipment": ["Excavators", "Crusher Attachments", "Shear Attachments", "Heavy Wheel Loaders", "Skid Steers"],
    },
    {
        "psc_code": "F108",
        "psc_name": "Environmental Remediation",
        "work_description_plain_english": "cleaning up Superfund sites and moving millions of tons of contaminated soil",
        "required_equipment": ["Bulldozers", "Articulated Off-Road Dump Trucks", "Excavators"],
    },
    {
        "psc_code": "F014",
        "psc_name": "Tree Thinning",
        "work_description_plain_english": "clearing paths for power lines, border perimeters, or large-scale wildfire mitigation",
        "required_equipment": ["Forestry Mulchers", "Heavy Bulldozers", "Track Loaders"],
    },
]


def _schema() -> pa.Schema:
    return pa.schema([
        pa.field("psc_code",                       pa.string(),                 nullable=False),
        pa.field("psc_name",                       pa.string(),                 nullable=False),
        pa.field("work_description_plain_english", pa.string(),                 nullable=False),
        pa.field("required_equipment",             pa.list_(pa.string()),       nullable=False),
    ])


def _build_table() -> pa.Table:
    schema = _schema()
    cols = {f.name: [row[f.name] for row in SEED_DATA] for f in schema}
    return pa.Table.from_pydict(cols, schema=schema)


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


def _create_indexes(so: dict) -> None:
    ds = lance.dataset(DATASET_URI, storage_options=so)
    for index_type, cols in INDEXES.items():
        for col in cols:
            ds.create_scalar_index(col, index_type=index_type)
            print(f"  {index_type:<6} ✓ psc_equipment_mapping.{col}")


def _verify(so: dict) -> dict:
    import pyarrow.compute as pc

    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["psc_code"])
    distinct = pc.count_distinct(keys.column("psc_code")).as_py()
    unique_ok = (n == distinct)

    sample = next((v for v in keys.column("psc_code").to_pylist() if v), None)
    probe = ds.scanner(columns=["psc_code"], filter=f"psc_code = '{sample}'").to_table().num_rows if sample else -1
    indexes = sorted(
        ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix))
        for ix in ds.list_indices()
    )
    out = {
        "uri": DATASET_URI,
        "rows": n,
        "distinct_psc_code": distinct,
        "unique_invariant_ok": unique_ok,
        "schema": [f.name for f in ds.schema],
        "indexes": indexes,
        f"probe_psc_code={sample!r}": probe,
    }
    if not unique_ok:
        raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(psc_code)={distinct}")
    return out


def main() -> None:
    import json

    so = _r2_storage_options()
    table = _build_table()
    print(f"building Lance table — {table.num_rows} rows, {len(table.schema)} cols")

    lance.write_dataset(
        table, DATASET_URI, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    print(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

    _create_indexes(so)
    print(json.dumps(_verify(so), indent=2, default=str))


if __name__ == "__main__":
    main()
