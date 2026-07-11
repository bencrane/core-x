#!/usr/bin/env python3
"""Build ``sec_adv_private_credit`` — a derived private-credit classification layer
over the surviving SEC ADV private-funds Lance dataset (Gen-3 SoR on R2).

Design (agreed architecture):
  * FUND grain — one row per private fund (69,307), every fund scored.
  * Deterministic RECALL BASE (no LLM, no re-scrape): ``pc_tier`` from the SEC's
    own ``fund_type_other`` self-labels + fund/adviser name credit tells.
  * POSITIVE-ONLY UCC overlay: ``ucc_name_candidate`` is *additive* evidence — it
    can only raise confidence, and is NULL (never False-as-signal) where a fund's
    adviser did not surface in the warm SAM×UCC lender surface. Absence from UCC
    carries ZERO negative weight: UCC-1 perfects security interests in hard
    collateral (equipment/inventory/receivables) and is structurally blind to
    cash-flow / unsecured / mezzanine / out-of-state (CA+CO only) direct lending.
    The overlay is CANDIDATE-grade (name-fuzzy); promotion to confirmed requires
    the raw ``ca_ucc/secured_parties`` domicile/LEI cross-check (follow-on).

Sources:
  * READ  : s3://data-sink/active/sec_adv_private_funds/            (Lance, SoR)
  * READ  : query-sidecar  sam_ucc_lenders (non_bank)              (warm, HTTP)
  * WRITE : s3://data-sink/active/sec_adv_private_credit/           (Lance, derived)

Run:
  doppler run -p core-x -c prd -- \
    uv run --no-project --with pylance --with duckdb --with rapidfuzz \
    python3 scripts/build_sec_adv_private_credit.py
"""
from __future__ import annotations

import json
import os
import re
import urllib.request

import duckdb
import lance
import pyarrow as pa
from rapidfuzz import fuzz, process

BUCKET = "data-sink"
SRC = f"s3://{BUCKET}/active/sec_adv_private_funds/"
DST = f"s3://{BUCKET}/active/sec_adv_private_credit/"
SIDECAR = "https://query-sidecar-api.onrender.com/api/v1/sql"

# ── Credit lexicon ────────────────────────────────────────────────────────────
# Explicit self-identification (fund_type_other free text) — highest-signal.
DECLARED = (
    r"(private credit|private debt|direct lend|credit fund|debt fund|mezzanine|"
    r"senior loan|senior secured|collateral(is|iz)ed loan|\bclo\b|specialty finance|"
    r"distressed debt|asset[- ]based lend|middle market (debt|credit|lend)|\bbdc\b|"
    r"opportunistic credit|structured credit|venture debt|credit opportunit)"
)
# Broader name-level tell (fund_name / adviser_legal_name).
NAME = (
    r"(credit|direct lend|private debt|mezzanine|\bclo\b|\bcdo\b|senior loan|"
    r"senior secured|specialty finance|asset[- ]based|middle market|\bbdc\b|"
    r"loan fund|debt fund|distressed|opportunistic credit|structured credit|"
    r"lending|venture debt)"
)

SUFF = re.compile(
    r"\b(LLC|LP|LLP|INC|INCORPORATED|LTD|LIMITED|CO|CORP|CORPORATION|COMPANY|GROUP|"
    r"HOLDINGS?|FUND[S]?|MANAGEMENT|MGMT|CAPITAL|PARTNERS|ADVISORS?|ADVISERS?|ASSET|"
    r"INVESTMENTS?|FINANCE|FINANCIAL|GLOBAL|USA|AMERICA)\b"
)


def norm(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9 ]", " ", str(s).upper())
    s = SUFF.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "aws_endpoint": ep,
        "aws_region": "auto",
    }


def fetch_nonbank_lenders() -> list[dict]:
    token = os.environ["QUERY_SIDECAR_TOKEN"]
    body = json.dumps(
        {
            "sql": "SELECT lender_name, filings, active_filings, ca_firms, co_firms, sam_firms "
            "FROM sam_ucc_lenders WHERE lender_class = 'non_bank'",
            "limit": 50000,
        }
    ).encode()
    req = urllib.request.Request(
        SIDECAR,
        data=body,
        headers={"Authorization": f"Bearer {token}", "content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        d = json.load(r)
    cols = d["columns"]
    return [dict(zip(cols, row)) for row in d["rows"]]


def main() -> None:
    so = storage_options()
    con = duckdb.connect()

    # ── 1. Load surviving private-funds SoR ────────────────────────────────────
    ds = lance.dataset(SRC, storage_options=so)
    t = ds.to_table(
        columns=[
            "crd_number", "adviser_legal_name", "adviser_regulatory_aum", "adviser_lei",
            "fund_id", "fund_name", "state", "country", "fund_type", "fund_type_other",
            "gross_asset_value",
        ]
    )
    con.register("pf", t)

    # ── 2. Deterministic per-fund classification (recall base) ─────────────────
    classified = con.execute(
        f"""
        SELECT
          fund_id, fund_name, crd_number, adviser_legal_name, adviser_lei,
          adviser_regulatory_aum, state, country, fund_type, fund_type_other,
          gross_asset_value,
          regexp_matches(lower(coalesce(fund_type_other,'')), '{DECLARED}')                AS m_declared,
          (regexp_matches(lower(coalesce(fund_name,'')), '{NAME}')
             OR regexp_matches(lower(coalesce(adviser_legal_name,'')), '{NAME}'))          AS m_name,
          lower(coalesce(fund_type,'')) = 'securitized asset fund'                          AS is_securitized_asset,
          coalesce(regexp_extract(lower(coalesce(fund_type_other,'')), '{DECLARED}'),
                   regexp_extract(lower(coalesce(fund_name,'')), '{NAME}'),
                   regexp_extract(lower(coalesce(adviser_legal_name,'')), '{NAME}'))        AS matched_term
        FROM pf
        """
    ).fetchdf()

    classified["pc_tier"] = "unscored"
    classified.loc[classified["m_name"], "pc_tier"] = "name_signal"
    classified.loc[classified["m_declared"], "pc_tier"] = "declared"
    classified["pc_flag"] = classified["pc_tier"].isin(["declared", "name_signal"])
    classified.drop(columns=["m_declared", "m_name"], inplace=True)

    # ── 3. Positive-only UCC name-evidence overlay (adviser grain → join to funds)
    lenders = fetch_nonbank_lenders()
    lidx: dict[str, dict] = {}
    for r in lenders:
        n = norm(r["lender_name"])
        if n:
            lidx.setdefault(n, r)
    lnorms = list(lidx.keys())

    cand = classified[classified["pc_flag"] & classified["crd_number"].notna()][
        ["crd_number", "adviser_legal_name"]
    ].drop_duplicates("crd_number")

    ucc_map: dict[str, dict] = {}
    for _, a in cand.iterrows():
        an = norm(a["adviser_legal_name"])
        if len(an) < 3:
            continue
        m = process.extractOne(an, lnorms, scorer=fuzz.token_sort_ratio)
        if not m:
            continue
        name, score, _ = m
        if score < 85:  # candidate floor; positive-only, never demotes
            continue
        lr = lidx[name]
        ucc_map[a["crd_number"]] = {
            "ucc_name_candidate": True,
            "ucc_match_score": int(score),
            "ucc_lender_name": lr["lender_name"],
            "ucc_filings": int(lr["filings"]) if lr["filings"] is not None else None,
            "ucc_ca_borrowers": int(lr["ca_firms"]) if lr["ca_firms"] is not None else None,
            "ucc_co_borrowers": int(lr["co_firms"]) if lr["co_firms"] is not None else None,
        }

    # attach (NULL everywhere there is no positive match — absence carries no signal)
    def get(crd, k):
        return ucc_map.get(crd, {}).get(k)

    classified["ucc_name_candidate"] = classified["crd_number"].map(lambda c: bool(c in ucc_map))
    classified["ucc_match_score"] = classified["crd_number"].map(lambda c: get(c, "ucc_match_score"))
    classified["ucc_lender_name"] = classified["crd_number"].map(lambda c: get(c, "ucc_lender_name"))
    classified["ucc_filings"] = classified["crd_number"].map(lambda c: get(c, "ucc_filings"))
    classified["ucc_ca_borrowers"] = classified["crd_number"].map(lambda c: get(c, "ucc_ca_borrowers"))
    classified["ucc_co_borrowers"] = classified["crd_number"].map(lambda c: get(c, "ucc_co_borrowers"))

    # ── 4. Persist to Lance (derived, under active/) ───────────────────────────
    tbl = pa.Table.from_pandas(classified, preserve_index=False)
    # normalize large_string -> string so Lance scalar indices accept the columns
    fields = []
    for f in tbl.schema:
        if pa.types.is_large_string(f.type):
            fields.append(pa.field(f.name, pa.string()))
        else:
            fields.append(f)
    tbl = tbl.cast(pa.schema(fields))
    lance.write_dataset(tbl, DST, storage_options=so, mode="overwrite")

    out = lance.dataset(DST, storage_options=so)
    for col, kind in [("fund_id", "BTREE"), ("crd_number", "BTREE"), ("pc_tier", "BITMAP")]:
        try:
            out.create_scalar_index(col, index_type=kind)
        except Exception as e:  # noqa: BLE001
            print(f"  index {col}: {e}")

    # ── 5. Verify ──────────────────────────────────────────────────────────────
    v = duckdb.connect()
    v.register("c", tbl)
    print(f"rows: {out.count_rows():,}   ->  {DST}")
    print(v.execute(
        "SELECT pc_tier, count(*) funds, count(distinct crd_number) advisers, "
        "sum(CASE WHEN ucc_name_candidate THEN 1 ELSE 0 END) ucc_hits "
        "FROM c GROUP BY 1 ORDER BY 2 DESC"
    ).fetchdf().to_string(index=False))
    print("\nsecuritized-asset context (not auto-flagged):",
          int(v.execute("SELECT count(*) FROM c WHERE is_securitized_asset").fetchone()[0]))
    print("distinct advisers with UCC name-evidence:",
          int(v.execute("SELECT count(distinct crd_number) FROM c WHERE ucc_name_candidate").fetchone()[0]))


if __name__ == "__main__":
    main()
