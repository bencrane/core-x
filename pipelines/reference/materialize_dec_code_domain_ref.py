"""Reference loader — dec_code_domain_ref: the CANONICAL, normalized code dimension for the entire
USAspending/FPDS substrate. One row per (db_element, sub_domain, code) -> verbatim government
description, parsed fail-closed from the DEC (usaspending_data_dictionary, the DAIMS Data Element
Crosswalk). This is Layer 0 — the bedrock every code column resolves through and every derived
taxonomy validates against.

WHY  Government codes are namespace-scoped: a bare `A` means "BPA Call" (award_type), "GWAC"
     (idv_type), "Fixed Price Redetermination" (type_of_contract_pricing), "Additional Work"
     (action_type/Contracts), or "New" (action_type/Assistance) depending on the domain. The DEC
     holds all of it, but as unstructured `domain_values` BLOBS (one per element). This dim refines
     those blobs into a queryable (db_element, sub_domain, code) -> description reference so no
     derivation ever has to guess (the `Y='nonstandard'` bug was a guess a one-row lookup prevents).

GRAIN  1 row per (db_element, sub_domain, code). `sub_domain` disambiguates the 2 elements that pack
       two code namespaces into one blob (ActionType / its DescriptionTag twin -> Assistance +
       Contracts); '' for all single-domain elements. `...DescriptionTag` twins are redundant
       serializations of their base element's domain and are excluded (they would collide on the key).
SoR    s3://data-sink/active/dec_code_domain_ref/   (Lance v2.1; derived, mode=overwrite)
SUPERSEDES  fpds_action_type_ref (retired) == this dim WHERE db_element='action_type' AND
       sub_domain='Contracts'. A `smoke`/`build` reconciliation gate asserts the two agree.

COLS   element, db_element, fpds_element, grouping, sub_domain, code, description, code_description,
       is_boolean, is_placeholder, dec_row_ord, source, source_vintage, dec_version, ingested_at.
KEYS   BTREE (db_element, code, element); BITMAP (sub_domain, grouping, is_boolean).

    doppler run -p core-x -c prd -- python3 pipelines/reference/materialize_dec_code_domain_ref.py \
        <smoke|build|verify>
      smoke  — parse live DEC, run all fail-closed gates, write a _sample copy, print the report.
               NO active/ write. Exit 1 on any gate failure.
      build  — same gates, then overwrite active/dec_code_domain_ref/ + build indices.
      verify — read active/ back; grain + index sanity.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
from collections import Counter

ACTIVE = "s3://data-sink/active"
DEC_URI = os.environ.get("USA_DATA_DICTIONARY_URI", f"{ACTIVE}/usaspending_data_dictionary/")
REF_URI = os.environ.get("DEC_CODE_DOMAIN_REF_URI", f"{ACTIVE}/dec_code_domain_ref/")
SAMPLE_URI = os.environ.get("DEC_CODE_DOMAIN_REF_SAMPLE_URI", f"{ACTIVE}/_sample/dec_code_domain_ref/")
LEGACY_ACTION_TYPE_REF = f"{ACTIVE}/fpds_action_type_ref/"
DATA_STORAGE_VERSION = "2.1"
SOURCE_VINTAGE = "dec_full_domain_normalize"

BTREE_INDEXES = ["db_element", "code", "element"]
BITMAP_INDEXES = ["sub_domain", "grouping", "is_boolean"]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

# Fail-closed sentinels — exact counts on the load-bearing FPDS code domains. A shape drift in the
# DEC that changes any of these aborts the build rather than silently mislabeling downstream.
EXPECTED = {
    ("contract_award_type", ""): 4,          # A BPA Call, B Purchase Order, C Delivery Order, D Definitive
    ("idv_type", ""): 5,                     # A GWAC, B IDC, C FSS, D BOA, E BPA
    ("action_type", "Contracts"): 21,        # FPDS Reason-for-Modification A..Y (== fpds_action_type_ref)
    ("action_type", "Assistance"): 5,        # FABS type-of-action A..E
}
EXPECTED_MIN = {
    ("type_of_contract_pricing", ""): 15,    # FAR Part 16 pricing families
}
TOTAL_FLOOR = 200

HEADER_RE = re.compile(r"^[A-Za-z][A-Za-z /&()\-]{0,45}:$")
CODE_RE = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")
PLACEHOLDER = {"(empty)", "blank", "[future code(s)]", "n/a"}


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


def _is_code(tok: str) -> bool:
    tok = tok.strip()
    return 1 <= len(tok) <= 18 and tok.count(" ") <= 2


def _segments(dv: str) -> list[tuple[str, list[str]]]:
    """Split a domain_values blob into (sub_domain, [lines]) on `Header:` lines. Single-domain
    elements yield one segment with sub_domain ''."""
    segs: list[tuple[str, list[str]]] = []
    cur_sub, cur = "", []
    for raw in dv.split("\n"):
        s = raw.strip()
        if not s:
            continue
        if "=" not in s and HEADER_RE.match(s):
            if cur:
                segs.append((cur_sub, cur))
                cur = []
            cur_sub = s[:-1].strip()
        else:
            cur.append(s)
    if cur:
        segs.append((cur_sub, cur))
    return segs


def _parse_blob(dv: str | None) -> list[list[str]]:
    """-> list of [sub_domain, code, description]. A line without a plausible `code =` is a
    description continuation appended to the prior code (89 DEC elements spill descriptions)."""
    if not dv or dv.strip().upper() == "N/A":
        return []
    out: list[list[str]] = []
    for sub, lines in _segments(dv):
        last = None
        for ln in lines:
            m = CODE_RE.match(ln)
            if m and _is_code(m.group(1)):
                out.append([sub, m.group(1).strip(), m.group(2).strip()])
                last = out[-1]
            elif last is not None:
                last[2] = (last[2] + " " + ln).strip()
            # leading non-code line before any code in a segment -> ignored
    return out


def _is_placeholder(code: str) -> bool:
    c = code.strip().lower()
    return c in PLACEHOLDER or code.startswith("(") or code.startswith("[")


def _rows_from_dec(dd_rows: list[dict], dec_version: int) -> tuple[list[dict], int, int, list[tuple]]:
    ingested = dt.datetime.now(dt.timezone.utc).isoformat()
    rows: list[dict] = []
    skipped_tags = 0
    dropped_ph = 0
    collisions: list[tuple] = []
    seen: set[tuple] = set()
    for r in dd_rows:
        el = (r.get("element") or "").strip()
        if el.endswith("DescriptionTag") or el.endswith("Description Tag"):
            skipped_tags += 1
            continue
        parsed = _parse_blob(r.get("domain_values"))
        if not parsed:
            continue
        cd_map = {(s, c): d for s, c, d in _parse_blob(r.get("domain_values_code_description"))}
        db_el = (r.get("db_element") or "").strip() or re.sub(r"[^a-z0-9]+", "_", el.lower()).strip("_")
        real = [(s, c, d) for s, c, d in parsed if not _is_placeholder(c)]  # drop absence markers (N/A, (empty), …)
        dropped_ph += len(parsed) - len(real)
        codes_this = {c for _, c, _ in real}
        is_bool = bool(codes_this) and codes_this <= {"Y", "N", "T", "F"}
        for sub, code, desc in real:
            key = (db_el, sub, code)
            if key in seen:
                collisions.append(key)
                continue
            seen.add(key)
            rows.append({
                "element": el, "db_element": db_el,
                "fpds_element": r.get("fpds_data_dictionary_element"),
                "grouping": r.get("grouping"), "sub_domain": sub, "code": code,
                "description": desc, "code_description": cd_map.get((sub, code)),
                "is_boolean": is_bool,
                "dec_row_ord": r.get("row_ord"),
                "source": f"{DEC_URI} (element={el}" + (f", sub={sub}" if sub else "") + ")",
                "source_vintage": SOURCE_VINTAGE, "dec_version": dec_version, "ingested_at": ingested,
            })
    return rows, skipped_tags, dropped_ph, collisions


def _gate(rows: list[dict], ref_codes: set | None) -> list[str]:
    by = Counter((r["db_element"], r["sub_domain"]) for r in rows)
    problems: list[str] = []
    for k, exp in EXPECTED.items():
        got = by.get(k, 0)
        if got != exp:
            problems.append(f"count {k}: expected {exp}, got {got}")
    for k, mn in EXPECTED_MIN.items():
        got = by.get(k, 0)
        if got < mn:
            problems.append(f"count {k}: expected >= {mn}, got {got}")
    keys = [(r["db_element"], r["sub_domain"], r["code"]) for r in rows]
    if len(keys) != len(set(keys)):
        problems.append("non-unique (db_element, sub_domain, code)")
    y = [r for r in rows if r["db_element"] == "action_type" and r["sub_domain"] == "Contracts" and r["code"] == "Y"]
    if not y or y[0]["description"].upper() != "ADD SUBCONTRACT PLAN":
        problems.append(f"action_type/Contracts Y != 'ADD SUBCONTRACT PLAN' (got {y[0]['description'] if y else 'MISSING'})")
    if ref_codes is not None:
        new_at = {r["code"] for r in rows if r["db_element"] == "action_type" and r["sub_domain"] == "Contracts"}
        if new_at != ref_codes:
            problems.append(f"action_type/Contracts != fpds_action_type_ref: missing={ref_codes - new_at}, extra={new_at - ref_codes}")
    if len(rows) < TOTAL_FLOOR:
        problems.append(f"total rows {len(rows)} < {TOTAL_FLOOR} floor")
    return problems


def _load():
    import lance
    so = _r2_so()
    dd = lance.dataset(DEC_URI, storage_options=so)
    dec_version = dd.version
    dd_rows = dd.to_table().to_pylist()
    try:
        ref_codes = set(lance.dataset(LEGACY_ACTION_TYPE_REF, storage_options=so)
                        .scanner(columns=["action_type_code"]).to_table().column("action_type_code").to_pylist())
    except Exception as e:  # noqa: BLE001
        log(f"legacy fpds_action_type_ref not readable ({e}); reconciliation gate skipped")
        ref_codes = None
    rows, skipped_tags, dropped_ph, collisions = _rows_from_dec(dd_rows, dec_version)
    log(f"parsed {len(rows)} code rows from {len(dd_rows)} DEC elements "
        f"(skipped {skipped_tags} DescriptionTag twins, dropped {dropped_ph} placeholder codes, "
        f"{len(collisions)} residual key collisions)")
    problems = _gate(rows, ref_codes)
    if collisions:
        problems.append(f"{len(collisions)} (db_element,sub_domain,code) collisions e.g. {collisions[:5]}")
    return rows, problems, so, dec_version


def _to_table(rows: list[dict]):
    import pyarrow as pa
    schema = pa.schema([
        ("element", pa.string()), ("db_element", pa.string()), ("fpds_element", pa.string()),
        ("grouping", pa.string()), ("sub_domain", pa.string()), ("code", pa.string()),
        ("description", pa.string()), ("code_description", pa.string()),
        ("is_boolean", pa.bool_()),
        ("dec_row_ord", pa.int64()), ("source", pa.string()), ("source_vintage", pa.string()),
        ("dec_version", pa.int64()), ("ingested_at", pa.string()),
    ])
    cols = [f.name for f in schema]
    return pa.table({c: [r.get(c) for r in rows] for c in cols}, schema=schema)


def _write(uri: str, tbl, so: dict, index: bool):
    import lance
    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(uri, storage_options=so)
    if index:
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            log(f"  BTREE  ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            log(f"  BITMAP ✓ {col}")
    return ds


def _report(rows: list[dict]) -> dict:
    by = Counter((r["db_element"], r["sub_domain"]) for r in rows)
    return {
        "total_rows": len(rows),
        "distinct_db_elements": len({r["db_element"] for r in rows}),
        "boolean_domains": len({r["db_element"] for r in rows if r["is_boolean"]}),
        "load_bearing": {f"{k[0]}/{k[1] or 'default'}": v for k, v in sorted(by.items())
                         if k in {**EXPECTED, **EXPECTED_MIN}},
        "action_type_Y": next((r["description"] for r in rows if r["db_element"] == "action_type"
                               and r["sub_domain"] == "Contracts" and r["code"] == "Y"), None),
        "sample_action_type_contracts": sorted(
            [f"{r['code']}={r['description']}" for r in rows
             if r["db_element"] == "action_type" and r["sub_domain"] == "Contracts"])[:6],
    }


def smoke():
    rows, problems, so, ver = _load()
    rep = _report(rows)
    if problems:
        print(json.dumps({"status": "GATE_FAIL", "dec_version": ver, "problems": problems, "report": rep},
                         indent=2, default=str))
        sys.exit(1)
    _write(SAMPLE_URI, _to_table(rows), so, index=False)
    print(json.dumps({"status": "GATES_PASS", "dec_version": ver, "sample_uri": SAMPLE_URI, "report": rep},
                     indent=2, default=str))


def build():
    rows, problems, so, ver = _load()
    if problems:
        raise RuntimeError("GATE_FAIL: " + "; ".join(problems))
    tbl = _to_table(rows)
    _write(REF_URI, tbl, so, index=True)
    log(f"DONE → {REF_URI} rows={tbl.num_rows}")
    print(json.dumps({"status": "BUILT", "uri": REF_URI, "rows": tbl.num_rows,
                      "dec_version": ver, "report": _report(rows)}, indent=2, default=str))


def verify():
    import lance
    so = _r2_so()
    ds = lance.dataset(REF_URI, storage_options=so)
    t = ds.scanner(columns=["db_element", "sub_domain", "code"]).to_table().to_pylist()
    keys = [(r["db_element"], r["sub_domain"], r["code"]) for r in t]
    try:
        idx = [getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    print(json.dumps({"uri": REF_URI, "rows": ds.count_rows(),
                      "grain_unique": len(keys) == len(set(keys)) == ds.count_rows(),
                      "distinct_db_elements": len({k[0] for k in keys}), "indices": idx},
                     indent=2, default=str))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    fn = {"smoke": smoke, "build": build, "verify": verify}.get(cmd)
    if not fn:
        print(f"unknown command: {cmd} (smoke|build|verify)")
        sys.exit(2)
    fn()


if __name__ == "__main__":
    main()
