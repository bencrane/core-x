#!/usr/bin/env python3
"""Tier-3 brochure extraction — segment helper CLI (coordination substrate).

Multi-session batch pattern: the 912 tier1_ok brochures are pre-sharded into
4 priority segments in ``s3://data-sink/active/_tier3_extract_worklist/``
(segment 1 = declared tier, descending GAV). Each executor session owns ONE
segment and processes it in bounded waves of subagents. Idempotency is
key-level: every completed extraction lands one JSON object at
``s3://data-sink/active/_tier3_extract_staging/{crd}_{version_id}.json``;
``fetch`` excludes already-staged keys, so any session can crash/resume/re-claim
without duplication. A final merge session compacts staging into the
``sec_adv_tier3_extractions`` Lance dataset.

Subcommands:
  fetch  --segment N --out FILE   pending rows for segment N (JSONL, with
                                  item_4/5/8 text inlined; staged keys excluded)
  submit --in FILE                validate one extraction JSON, put to staging
  status                          per-segment staged/total counts
  prompt                          print the frozen extraction prompt template

Run under: doppler run -p core-x -c prd -- uv run --no-project \
  --with pylance --with duckdb --with boto3 python3 pipelines/sec_adv/tier3_extract_helper.py ...
"""
from __future__ import annotations

import argparse
import json
import os
import sys

BUCKET = "data-sink"
WORKLIST = f"s3://{BUCKET}/active/_tier3_extract_worklist/"
BROCHURES = f"s3://{BUCKET}/active/sec_adv_part_2_brochures_lance/"
STAGING_PREFIX = "active/_tier3_extract_staging/"

# ── Frozen output schema — all sessions must produce exactly this shape ───────
SCHEMA_FIELDS: dict[str, type | tuple] = {
    "crd_number": str,
    "brochure_version_id": str,
    "pc_verdict": str,            # yes | no | unclear — runs private-credit strategies?
    "sub_strategies": list,       # subset of SUB_STRATEGY_ENUM
    "seniority": str,             # senior | subordinated | mixed | unclear | n/a
    "sponsored": str,             # sponsored | non_sponsored | mixed | unclear | n/a
    "borrower_segment": str,      # lower_middle_market | middle_market | upper_middle_market
                                  # | large_cap | consumer | real_estate | mixed | unclear | n/a
    "sectors": list,              # short free-text sector focuses, [] if none stated
    "mgmt_fee_pct_min": (float, int, type(None)),
    "mgmt_fee_pct_max": (float, int, type(None)),
    "carry_pct": (float, int, type(None)),
    "hurdle_pct": (float, int, type(None)),
    "fee_notes": str,             # <=300 chars
    "client_types": list,
    "evidence_quote": str,        # <=250 chars, verbatim from the brochure text
    "extraction_confidence": str, # high | medium | low
}
SUB_STRATEGY_ENUM = [
    "direct_lending", "mezzanine", "distressed", "specialty_finance",
    "structured_credit_clo", "venture_debt", "real_estate_debt",
    "infrastructure_debt", "asset_based_lending", "credit_hedge",
    "bdc", "other_credit", "none",
]

PROMPT_TEMPLATE = """You are extracting structured private-credit fields from a SEC Form ADV Part 2A \
brochure. You are given the adviser's Item 4 (Advisory Business), Item 5 (Fees and \
Compensation), and Item 8 (Methods of Analysis / Investment Strategies) text.

Return ONLY a JSON object with exactly these fields:
- crd_number: "{crd_number}" (echo verbatim)
- brochure_version_id: "{brochure_version_id}" (echo verbatim)
- pc_verdict: "yes"|"no"|"unclear" — does this adviser actually run private-credit \
strategies (privately originated / non-bank lending: direct lending, mezzanine, \
distressed debt, specialty finance, CLOs/structured credit, venture debt, RE/infra debt)? \
"no" if credit words appear only as risk boilerplate.
- sub_strategies: array from {enum} (["none"] iff pc_verdict is "no")
- seniority: "senior"|"subordinated"|"mixed"|"unclear"|"n/a"
- sponsored: "sponsored"|"non_sponsored"|"mixed"|"unclear"|"n/a" (PE-sponsor-backed borrowers?)
- borrower_segment: "lower_middle_market"|"middle_market"|"upper_middle_market"|"large_cap"|"consumer"|"real_estate"|"mixed"|"unclear"|"n/a"
- sectors: array of short sector-focus strings stated in the text ([] if none)
- mgmt_fee_pct_min, mgmt_fee_pct_max: numbers (e.g. 1.5) or null — management fee range \
for the PRIVATE FUNDS / credit vehicles (not SMA/wrap fees)
- carry_pct: number or null (performance allocation / carried interest)
- hurdle_pct: number or null (preferred return / hurdle)
- fee_notes: <=300 chars of nuance (fee waivers, tiers, which vehicle the numbers apply to)
- client_types: array of short strings (who they advise: private funds, pensions, HNW...)
- evidence_quote: <=250 chars quoted VERBATIM from the provided text supporting pc_verdict
- extraction_confidence: "high"|"medium"|"low"

Rules: extract only what the text states — never infer numbers. If Item 5 gives fees \
only for non-credit products, fee fields are null with a fee_note. Output raw JSON, no markdown.

=== ITEM 4 ===
{item_4}
=== ITEM 5 ===
{item_5}
=== ITEM 8 ===
{item_8}"""


def _so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "aws_endpoint": ep, "aws_region": "auto"}


def _s3():
    import boto3
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return boto3.client("s3", endpoint_url=ep,
                        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])


def _staged_keys() -> set[str]:
    s3, out, tok = _s3(), set(), None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": STAGING_PREFIX, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            out.add(o["Key"].rsplit("/", 1)[-1].removesuffix(".json"))
        if not r.get("IsTruncated"):
            return out
        tok = r.get("NextContinuationToken")


def cmd_fetch(segment: int, out_path: str) -> None:
    import duckdb
    import lance
    so = _so()
    wl = lance.dataset(WORKLIST, storage_options=so).to_table()
    br = lance.dataset(BROCHURES, storage_options=so).to_table(
        columns=["crd_number", "brochure_version_id", "item_4", "item_5", "item_8"])
    con = duckdb.connect()
    con.register("wl", wl)
    con.register("br", br)
    rows = con.execute(
        """SELECT w.crd_number, w.brochure_version_id, w.adviser_legal_name, w.tier,
                  b.item_4, b.item_5, b.item_8
           FROM wl w JOIN br b USING (crd_number, brochure_version_id)
           WHERE w.segment = ? ORDER BY w.pc_gav DESC NULLS LAST""",
        [segment]).fetchall()
    staged = _staged_keys()
    n_all, n_out = len(rows), 0
    with open(out_path, "w") as f:
        for crd, vid, name, tier, i4, i5, i8 in rows:
            if f"{crd}_{vid}" in staged:
                continue
            f.write(json.dumps({"crd_number": crd, "brochure_version_id": vid,
                                "adviser_legal_name": name, "tier": tier,
                                "item_4": i4 or "", "item_5": i5 or "", "item_8": i8 or ""}) + "\n")
            n_out += 1
    print(f"segment {segment}: {n_all} total, {n_all - n_out} already staged, {n_out} pending -> {out_path}")


def validate(rec: dict) -> list[str]:
    errs = []
    for k, t in SCHEMA_FIELDS.items():
        if k not in rec:
            errs.append(f"missing: {k}")
            continue
        if not isinstance(rec[k], t):
            errs.append(f"bad type: {k}")
    for s in rec.get("sub_strategies", []):
        if s not in SUB_STRATEGY_ENUM:
            errs.append(f"bad sub_strategy: {s}")
    if rec.get("pc_verdict") not in ("yes", "no", "unclear"):
        errs.append("bad pc_verdict")
    if len(rec.get("evidence_quote", "")) > 250:
        errs.append("evidence_quote >250 chars")
    if len(rec.get("fee_notes", "")) > 300:
        errs.append("fee_notes >300 chars")
    return errs


def cmd_submit(in_path: str) -> None:
    rec = json.load(open(in_path))
    errs = validate(rec)
    if errs:
        print("REJECTED:", "; ".join(errs))
        sys.exit(1)
    key = f"{STAGING_PREFIX}{rec['crd_number']}_{rec['brochure_version_id']}.json"
    _s3().put_object(Bucket=BUCKET, Key=key, Body=json.dumps(rec).encode(),
                     ContentType="application/json")
    print(f"staged -> s3://{BUCKET}/{key}")


def cmd_status() -> None:
    import duckdb
    import lance
    wl = lance.dataset(WORKLIST, storage_options=_so()).to_table(
        columns=["crd_number", "brochure_version_id", "segment"])
    staged = _staged_keys()
    con = duckdb.connect()
    con.register("wl", wl)
    rows = con.execute("SELECT segment, crd_number, brochure_version_id FROM wl").fetchall()
    per: dict[int, list[int]] = {}
    for seg, crd, vid in rows:
        per.setdefault(seg, [0, 0])
        per[seg][1] += 1
        if f"{crd}_{vid}" in staged:
            per[seg][0] += 1
    for seg in sorted(per):
        d, t = per[seg]
        print(f"segment {seg}: {d}/{t} staged")
    print(f"TOTAL: {sum(v[0] for v in per.values())}/{sum(v[1] for v in per.values())}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--segment", type=int, required=True)
    f.add_argument("--out", required=True)
    s = sub.add_parser("submit")
    s.add_argument("--in", dest="in_path", required=True)
    sub.add_parser("status")
    sub.add_parser("prompt")
    a = p.parse_args()
    if a.cmd == "fetch":
        cmd_fetch(a.segment, a.out)
    elif a.cmd == "submit":
        cmd_submit(a.in_path)
    elif a.cmd == "status":
        cmd_status()
    elif a.cmd == "prompt":
        print(PROMPT_TEMPLATE)


if __name__ == "__main__":
    main()
