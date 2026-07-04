"""Reference loader — fpds_action_type_ref: FPDS action-type ("Reason for Modification") code ->
AUTHORITATIVE description, sourced VERBATIM from the DEC (usaspending_data_dictionary, element
`ActionType`, `domain_values` — the `Contracts:` section). Government strings only; NO editorial
gloss. The spine `usaspending_fpds_canonical_txn` carries only `action_type_code` (col 42); this dim
supplies the description so downstream stops hardcoding letter codes (e.g. 'G' = EXERCISE AN OPTION).

GRAIN  1 row per action_type_code (the FPDS/procurement domain). action_type_code unique (1:1 key).
SoR    s3://data-sink/active/fpds_action_type_ref/   (Lance v2.1; derived, mode=overwrite)
SCOPE  FPDS/procurement only — parsed from the DEC domain's `Contracts:` block. The `Assistance:`
       block (FABS: A=New, B=Continuation, …) collides on A/B/C/D and is deliberately excluded; our
       spine is FPDS. A NULL/blank `action_type_code` on the spine = a NEW procurement award (no
       modification) — no code row; consumers COALESCE the null.
COLS   action_type_code, action_type_description, source, source_vintage, ingested_at.
KEYS   action_type_code (BTREE).

SOURCE Derived in-plane from the DEC (active/usaspending_data_dictionary), fetched live from the
       USAspending DATA Element Crosswalk — the authoritative field dictionary of record. The DEC is
       the single source of truth; this dim is a verbatim projection of its ActionType domain.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_fpds_action_type_ref.py <build|verify>
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys

ACTIVE = "s3://data-sink/active"
REF_URI = os.environ.get("FPDS_ACTION_TYPE_REF_URI", f"{ACTIVE}/fpds_action_type_ref/")
DEC_URI = os.environ.get("USA_DATA_DICTIONARY_URI", f"{ACTIVE}/usaspending_data_dictionary/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VINTAGE = "dec_actiontype_domain"

BTREE_INDEXES = ["action_type_code"]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _parse_contracts_domain(domain_values: str) -> list[tuple[str, str]]:
    """Extract the `Contracts:` block from the DEC ActionType.domain_values and return verbatim
    (code, description) pairs. Fail-closed if the block/codes are missing or the shape changed."""
    if not domain_values or "Contracts:" not in domain_values:
        raise RuntimeError("DEC ActionType.domain_values missing a 'Contracts:' block")
    block = domain_values.split("Contracts:", 1)[1]
    block = re.split(r"\n[A-Z][A-Za-z ]+:\n", block, 1)[0]   # stop at any later section header
    out, seen = [], set()
    for line in block.splitlines():
        m = re.match(r"\s*([A-Z0-9]{1,3})\s*=\s*(.+?)\s*$", line)
        if not m:
            continue
        code, desc = m.group(1), m.group(2)
        if code in seen:
            raise RuntimeError(f"duplicate contract code {code!r} in DEC domain")
        seen.add(code)
        out.append((code, desc))
    if len(out) < 15:
        raise RuntimeError(f"parsed only {len(out)} contract codes — DEC domain shape changed")
    return out


def build():
    import lance
    import pyarrow as pa
    so = _r2_so()
    dd = lance.dataset(DEC_URI, storage_options=so)
    rows = dd.scanner(columns=["element", "domain_values"], filter="element = 'ActionType'").to_table().to_pylist()
    if not rows:
        raise RuntimeError("DEC has no ActionType element")
    pairs = _parse_contracts_domain(rows[0]["domain_values"])
    log(f"parsed {len(pairs)} verbatim FPDS contract action-type codes from the DEC")

    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    schema = pa.schema([("action_type_code", pa.string()), ("action_type_description", pa.string()),
                        ("source", pa.string()), ("source_vintage", pa.string()), ("ingested_at", pa.string())])
    tbl = pa.table({
        "action_type_code": [c for c, _ in pairs],
        "action_type_description": [d for _, d in pairs],
        "source": [f"{DEC_URI} (element=ActionType, domain_values Contracts block)"] * len(pairs),
        "source_vintage": [SOURCE_VINTAGE] * len(pairs),
        "ingested_at": [ingested] * len(pairs),
    }, schema=schema)
    lance.write_dataset(tbl, REF_URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(REF_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    log(f"DONE → {REF_URI} rows={tbl.num_rows}")
    return {"rows": tbl.num_rows, "uri": REF_URI}


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(REF_URI, storage_options=so)
    t = ds.scanner(columns=["action_type_code", "action_type_description"]).to_table().to_pylist()
    t.sort(key=lambda r: r["action_type_code"])
    codes = [r["action_type_code"] for r in t]
    try:
        idx = [getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    print(json.dumps({
        "uri": REF_URI, "rows": ds.count_rows(),
        "distinct_action_type_code": len(set(codes)),
        "grain_unique": len(set(codes)) == len(codes) == ds.count_rows(),
        "indices": idx,
        "all_codes": {r["action_type_code"]: r["action_type_description"] for r in t},
    }, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
