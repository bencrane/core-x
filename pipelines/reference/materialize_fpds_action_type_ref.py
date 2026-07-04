"""Reference loader — fpds_action_type_ref: the FPDS action-type (a.k.a. "Reason for
Modification") code -> description domain, extracted out of the FPDS spine as its own
lookup table so downstream code stops hardcoding letter codes (e.g. 'G' = Exercise an Option).

The spine `usaspending_fpds_canonical_txn` carries only `action_type_code` (col 42); the matching
`action_type_description` is documented in BULK but NOT projected onto the spine (see
docs/reference/FPDS_CANONICAL_FIELD_DICTIONARY.md §6). This dim supplies the missing description.

GRAIN  1 row per action_type_code. 21 rows — the 20-code A–X standard set + the one empirically-
       present non-standard code 'Y'. action_type_code is unique (the 1:1 resolution key).
SoR    s3://data-sink/active/fpds_action_type_ref/   (Lance v2.1; derived, mode=overwrite)
SCOPE  FPDS/procurement only. FABS (financial-assistance) `action_type` is a DIFFERENT domain
       (A=New, B=Continuation, C=Revision, D=Adjustment) whose A/B/C/D collide with these; our
       spine is FPDS, so the assistance domain is deliberately excluded to keep the code the
       clean 1:1 key. A NULL/blank `action_type_code` on the spine = a NEW procurement award
       (no modification) — there is no code row for it; consumers COALESCE the null.
COLS   action_type_code, action_type_description, notes, source_vintage, ingested_at.
KEYS   action_type_code (BTREE) — the join/resolution key.

SOURCE Authoritative published domain (no scan): USAspending data-type whitepaper
       https://fedspendingtransparency.github.io/whitepapers/types/  (contract "Reason for
       Modification"), cross-checked to the FPDS-NG Data Dictionary
       https://www.fpds.gov/downloads/Version_1.5_specs/FPDS_DataDictionary_V15_OT.pdf .
       Description strings are the USAspending canonical forms (the FPDS Atom `action_type_desc`
       tag as it lands in our BULK feed is the same text, upper-cased).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' \
      python3 pipelines/reference/materialize_fpds_action_type_ref.py <build|verify>

    # Optional data-drift check against the live 108M spine (opt-in; scans one low-card column):
    #   ... materialize_fpds_action_type_ref.py verify --against-spine
"""
from __future__ import annotations

import collections
import datetime as dt
import json
import os
import sys

ACTIVE = "s3://data-sink/active"
REF_URI = os.environ.get("FPDS_ACTION_TYPE_REF_URI", f"{ACTIVE}/fpds_action_type_ref/")
SPINE_URI = os.environ.get("FPDS_CANONICAL_TXN_URI", f"{ACTIVE}/usaspending_fpds_canonical_txn/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VINTAGE = "fpds_reason_for_modification_2026_07"

BTREE_INDEXES = ["action_type_code"]

# (code, description, notes) — the FPDS procurement "Reason for Modification" domain.
# Verbatim USAspending canonical descriptions for A–X (the 20-code standard set); 'Y' appended as the one
# empirically-present non-standard code (live spine: 8,736 mod txns — small-$ admin + minor re-reps,
# 0 terminations). I/O/Q/U/Z remain unused.
ACTION_TYPES: list[tuple[str, str, str | None]] = [
    ("A", "Additional Work (new agreement, FAR part 6 applies)", "Out-of-scope additional work; new competition applies."),
    ("B", "Supplemental Agreement for work within scope", None),
    ("C", "Funding Only Action", "Obligates/deobligates funds only; no scope change."),
    ("D", "Change Order", None),
    ("E", "Terminate for Default (complete or partial)", "Contractor-fault termination."),
    ("F", "Terminate for Convenience (complete or partial)", "Government-elected termination."),
    ("G", "Exercise an Option", "Government commits the next priced work tranche — the contractor mobilization event."),
    ("H", "Definitize Letter Contract", "Converts an undefinitized letter contract to a definitive contract."),
    ("J", "Novation Agreement", "Recognizes a successor-in-interest (asset sale/transfer of the contract)."),
    ("K", "Close Out", "Administrative closeout of a completed award."),
    ("L", "Definitize Change Order", None),
    ("M", "Other Administrative Action", None),
    ("N", "Legal Contract Cancellation", None),
    ("P", "Re-representation of Non-Novated Merger/Acquisition", "Size/ownership re-representation after a non-novated M&A."),
    ("R", "Re-representation", "Socioeconomic size/status re-representation (e.g. at option exercise)."),
    ("S", "Change PIID", "Reassigns the contract's PIID."),
    ("T", "Transfer Action", None),
    ("V", "Vendor DUNS Change", "Legacy DUNS identifier change (DUNS retired in favor of UEI)."),
    ("W", "Vendor Address Change", None),
    ("X", "Terminate for Cause", "Commercial-item termination for cause (FAR 12)."),
    ("Y", "Non-Standard / Undocumented Reason Code", "Outside the FPDS 20-code Reason-for-Modification set; empirically small-dollar administrative actions plus minor re-representations (0 terminations on the live spine). Classified 'nonstandard' downstream; deltas are computed regardless."),
]


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


def _assemble():
    import pyarrow as pa
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    seen, out = set(), []
    for code, desc, notes in ACTION_TYPES:
        code = code.strip()
        if not code or code in seen:  # action_type_code is the 1:1 key — never fan out
            raise RuntimeError(f"bad/duplicate action_type_code: {code!r}")
        seen.add(code)
        out.append({
            "action_type_code": code,
            "action_type_description": desc,
            "notes": notes,
            "source_vintage": SOURCE_VINTAGE,
            "ingested_at": ingested,
        })
    schema = pa.schema([
        ("action_type_code", pa.string()),
        ("action_type_description", pa.string()),
        ("notes", pa.string()),
        ("source_vintage", pa.string()),
        ("ingested_at", pa.string()),
    ])
    return pa.table({f.name: [r[f.name] for r in out] for f in schema}, schema=schema)


def build():
    import lance
    so = _r2_so()
    tbl = _assemble()
    if tbl.num_rows == 0:
        raise RuntimeError("zero action-type rows assembled")
    log(f"assembled {tbl.num_rows} action_type_code -> description rows")
    lance.write_dataset(tbl, REF_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(REF_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        log(f"  BTREE ✓ {col}")
    log(f"DONE → {REF_URI} rows={tbl.num_rows}")
    return {"rows": tbl.num_rows, "uri": REF_URI}


def _spine_coverage(so) -> dict:
    """Opt-in: scan the spine's action_type_code (one low-card column) and reconcile against the
    reference domain — flags any live code not covered here (data drift), with observed counts."""
    import lance
    log(f"scanning spine action_type_code for drift check: {SPINE_URI}")
    sds = lance.dataset(SPINE_URI, storage_options=so)
    col = sds.scanner(columns=["action_type_code"]).to_table().column("action_type_code").to_pylist()
    counts = collections.Counter((c.strip() if isinstance(c, str) else c) for c in col)
    ref_codes = {c for c, _, _ in ACTION_TYPES}
    live = {(k if k else "<null/base>"): v for k, v in counts.items()}
    uncovered = {k: v for k, v in counts.items() if k and k not in ref_codes}
    return {
        "spine_rows": sds.count_rows(),
        "distinct_live_codes": sorted(k for k in counts if k),
        "counts_by_code": dict(sorted(live.items(), key=lambda kv: -kv[1])),
        "uncovered_by_ref": uncovered,  # MUST be empty
    }


def verify(against_spine: bool = False):
    import lance
    so = _r2_so()
    ds = lance.dataset(REF_URI, storage_options=so)
    t = ds.scanner(columns=["action_type_code", "action_type_description"]).to_table()
    codes = t.column("action_type_code").to_pylist()
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {
        "uri": REF_URI,
        "rows": ds.count_rows(),
        "distinct_action_type_code": len(set(codes)),
        "grain_unique": len(set(codes)) == len(codes) == ds.count_rows(),
        "indices": idx,
        "spot_check": ds.scanner(
            columns=["action_type_code", "action_type_description", "notes"],
            filter="action_type_code IN ('G','H','J','F')").to_table().to_pylist(),
    }
    if against_spine:
        out["spine_coverage"] = _spine_coverage(so)
    print(json.dumps(out, indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        print(json.dumps(build(), indent=2, default=str))
    elif cmd == "verify":
        verify(against_spine="--against-spine" in sys.argv[2:])
    else:
        print(f"unknown command: {cmd} (build|verify [--against-spine])")
        sys.exit(2)


if __name__ == "__main__":
    main()
