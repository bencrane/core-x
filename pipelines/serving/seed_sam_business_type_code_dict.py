"""Reference seed — `sam_business_type_code_dict` (the SAM/FPDS business-type CODE→designation legend).

SoR  s3://data-sink/active/sam_business_type_code_dict/  (Lance v2.1; reference, snapshot-overwrite).

WHY THIS EXISTS
SAM stores socioeconomic reps & certs as 2-char CODES in two SEPARATE namespaces and ships
NO value-level decode dictionary in the public extract. The same token (e.g. `A6`) means
different things across namespaces, so a flat decode invites silent conflation. This table
is the authoritative, namespace-scoped legend — the join target that
`materialize_subawardee_designations.py` (and any future decode) keys on.

  namespace `business_types`            ← SAM master field pos 32  "BUS TYPE STRING"  (self-cert, well populated)
  namespace `sba_business_types_string` ← SAM master field pos 118 "SBA BUSINESS TYPES STRING" (SBA-administered cert, ~13% populated)

PROVENANCE OF EACH ROW
- `gsa_confirmed`     : label verbatim from the GSA SAM Functional Data Dictionary (business_types)
                        / SAM Entity Management Public V2 Extract Layout field 118 (sba string).
- `empirical_validated`: code→designation established by joining sam_master_entities.uei →
                        govcon_active_awards.recipient_uei and measuring precision P(flag|code) /
                        recall P(code|flag) against the FPDS prime self-cert booleans
                        (the operator's exact-named flags). A9/A0/JT are absent from the legacy
                        public-extract layout PDF (post-2020 SBA WOSB/EDWOSB/8a-JV certs) but are
                        unambiguous in-data; they are kept on empirical grounds and tagged as such.

Grain: 1 row per (namespace, code). Idempotent snapshot-overwrite. BTREE on the join keys.

    doppler run --project core-x --config prd -- python pipelines/serving/seed_sam_business_type_code_dict.py
    doppler run --project core-x --config prd -- python pipelines/serving/seed_sam_business_type_code_dict.py --verify
"""
from __future__ import annotations

import datetime as dt
import os
import sys

SERVING_URI = os.environ.get("SAM_BUSINESS_TYPE_CODE_DICT_URI",
                             "s3://data-sink/active/sam_business_type_code_dict/")
DATA_STORAGE_VERSION = "2.1"
BTREE_COLS = ["code", "namespace", "designation_key"]

GSA_SRC_BT = "GSA SAM Functional Data Dictionary v2 (2023-06-23), Business Types field"
GSA_SRC_SBA = "GSA SAM Entity Management Public V2 Extract Layout, field 118 (SBA Business Types String)"
EMP_SRC = "empirical: sam_master_entities.uei ⋈ govcon_active_awards.recipient_uei vs FPDS prime self-cert booleans"

# (namespace, code, label, designation_key, sba_administered, confidence, prec, recall, source)
ROWS = [
    # ── business_types namespace (self-cert, GSA-confirmed) ──
    ("business_types", "QF", "Service-Disabled Veteran-Owned Business",
     "service_disabled_veteran_owned_business", False, "gsa_confirmed", 0.97, 0.93, GSA_SRC_BT),
    ("business_types", "A5", "Veteran-Owned Business",
     "veteran_owned_business", False, "gsa_confirmed", 0.98, 0.92, GSA_SRC_BT),
    ("business_types", "A2", "Woman-Owned Business",
     "woman_owned_business", False, "gsa_confirmed", 0.95, 0.91, GSA_SRC_BT),
    ("business_types", "8W", "Women-Owned Small Business (WOSB)",
     "women_owned_small_business", False, "gsa_confirmed", 0.92, 0.91, GSA_SRC_BT),
    ("business_types", "27", "Self-Certified Small Disadvantaged Business",
     "self_certified_small_disadvantaged_business", False, "gsa_confirmed", 0.95, 0.90, GSA_SRC_BT),
    ("business_types", "23", "Minority-Owned Business",
     "minority_owned_business", False, "gsa_confirmed", 0.94, 0.92, GSA_SRC_BT),
    ("business_types", "8C", "Women-Owned Small Business (WOSB) Joint Venture",
     "joint_venture_women_owned_small_business", False, "gsa_confirmed", 0.87, 0.87, GSA_SRC_BT),
    # ── sba_business_types_string namespace (SBA-administered cert; 2-char prefix of <code><YYYYMMDD?>) ──
    ("sba_business_types_string", "A6", "SBA Certified 8(a) Program Participant",
     "c8a_program_participant", True, "gsa_confirmed", 0.93, 0.68, GSA_SRC_SBA),
    ("sba_business_types_string", "XX", "SBA Certified HUBZone Firm",
     "historically_underutilized_business_zone_hubzone_firm", True, "gsa_confirmed", 0.90, 0.68, GSA_SRC_SBA),
    ("sba_business_types_string", "JT", "SBA Certified 8(a) Joint Venture",
     "joint_venture_8a", True, "empirical_validated", 0.87, 0.58, EMP_SRC),
    ("sba_business_types_string", "A9", "SBA Certified Women-Owned Small Business (WOSB)",
     "women_owned_small_business", True, "empirical_validated", 0.95, 0.37, EMP_SRC),
    ("sba_business_types_string", "A0", "SBA Certified Economically Disadvantaged Women-Owned Small Business (EDWOSB)",
     "economically_disadvantaged_women_owned_small_business", True, "empirical_validated", 0.69, 0.32, EMP_SRC),
]


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _table():
    import pyarrow as pa

    now = dt.datetime.now(dt.timezone.utc)
    cols = list(zip(*ROWS))
    return pa.table({
        "namespace":         pa.array(cols[0], pa.string()),
        "code":              pa.array(cols[1], pa.string()),
        "label":             pa.array(cols[2], pa.string()),
        "designation_key":   pa.array(cols[3], pa.string()),
        "sba_administered":  pa.array(cols[4], pa.bool_()),
        "confidence":        pa.array(cols[5], pa.string()),
        "emp_precision":     pa.array(cols[6], pa.float64()),
        "emp_recall":        pa.array(cols[7], pa.float64()),
        "source":            pa.array(cols[8], pa.string()),
        "source_version":    pa.array(["2026-06-19"] * len(ROWS), pa.string()),
        "built_at":          pa.array([now] * len(ROWS), pa.timestamp("us", tz="UTC")),
    })


def build() -> dict:
    import lance

    so = _r2_storage_options()
    tbl = _table()
    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    for c in BTREE_COLS:
        try:
            ds.create_scalar_index(c, index_type="BTREE")
            print(f"  BTREE ✓ {c}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {c}: {exc}")
    n = ds.count_rows()
    assert n == len(ROWS), f"row-count gate: {n} != {len(ROWS)}"
    print(f"WROTE {SERVING_URI} rows={n} cols={len(ds.schema)}")
    return {"uri": SERVING_URI, "rows": n}


def verify() -> None:
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    print(f"{SERVING_URI}  rows={ds.count_rows()}  cols={len(ds.schema)}")
    print("indices:", sorted(
        (i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ds.list_indices()))
    import duckdb
    con = duckdb.connect(":memory:"); con.register("d", ds)
    print(con.execute("SELECT namespace, code, label, designation_key, sba_administered, confidence "
                      "FROM d ORDER BY namespace, code").df().to_string(index=False))


if __name__ == "__main__":
    (verify if "--verify" in sys.argv else build)()
