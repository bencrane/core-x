#!/usr/bin/env python3
"""gtm_naics_psc_pairs — the NAICS×PSC pairing substrate (stamping practice +
normative overlay). Prime awards ONLY — subawards inherit the prime stamp and
therefore contribute zero pairing information by construction (verified to the
broker SQL, 2026-07-08).

SoR  s3://data-sink/active/gtm_naics_psc_pairs/
     (grain: naics_code × psc_code — one row per pair observed on any prime
      award at either grain OR suggested by the psctool crosswalk; Lance;
      snapshot-overwrite; BTREE naics_code / psc_code / family_key)

WHY (inferred-combo-profile cycle, step 0)
SAM-declared entity codes are UNPAIRED lists (no combo assertions exist at
entity grain), and most small entities declare NAICS but no PSC. This substrate
is the generation layer: declared NAICS → the eligible PSC set, ranked by the
government's own stamping practice — P(psc | naics) — with the psctool curated
pairing as a normative flag, never a substitute for observation.

TWO GRAINS, ONE ROW
  award grain (usaspending_fpds_prime_award_state, 82.9M): n_awards_lifetime,
    obligated_lifetime (Σ life_to_date_obligated VERBATIM — NET, negatives ride),
    n_recipients_lifetime, n_agencies_lifetime.
  action grain (gtm_txn_events_slim, 107.9M, full history): n_actions_lifetime,
    obligation_txn_lifetime, 24mo/60mo cuts, n_recipients_60mo,
    first_action_date / last_action_date.
  psctool overlay (psctool/naics_psc_map, 1,695 curated pairs):
    is_psctool_suggested. Suggested-but-never-stamped pairs ride with ZERO
    counts — these are complete-universe aggregations, so absence IS zero here
    (not an unknown; the null≠zero doctrine applies to partial knowledge, and
    both spines were read in full).

CONDITIONAL SHARES (the ranking substrate, both directions)
  share_of_naics_awards / share_of_naics_dollars  — P(psc | naics)
  share_of_psc_awards   / share_of_psc_dollars    — P(naics | psc)
  share_of_naics_dollars_60mo                     — recency-weighted P(psc | naics)

PAIR DEFINITION: both codes non-null. A stamped award missing either half has
no pair to contribute — exclusion is definitional, disclosed here, not a null
suppression. family_key baked per freeze §0.1.3 (PSC[0] alpha | PSC[:2] FSC
group), NULL when naics shorter than 4.

    doppler run --project core-x --config prd -- \
      /Users/benjamincrane/core-x/.venv/bin/python \
      scripts/build_gtm_naics_psc_pairs.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
SRC_AWARD = f"{A}/usaspending_fpds_prime_award_state/"
SRC_TXN = f"{A}/gtm_txn_events_slim/"
SRC_PSCTOOL = f"{A}/psctool/naics_psc_map/"
OUT = f"{A}/gtm_naics_psc_pairs/"
PARAM_SET_ID = "v1"
BTREE = ["naics_code", "psc_code", "family_key"]

# family_key per freeze §0.1.3 — MUST match apps/catalyst_api/src/psc_families.py
FAMILY_SQL = """
    CASE WHEN length(naics_code) >= 4 AND length(psc_code) >= 1 THEN
        substr(naics_code, 1, 4) || 'x' ||
        CASE WHEN regexp_matches(substr(psc_code, 1, 1), '[A-Z]')
             THEN substr(psc_code, 1, 1) ELSE substr(psc_code, 1, 2) END
    END"""


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _cx():
    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")
    return con


def build() -> int:
    opt = so()
    as_of = date.today()
    w60 = (as_of - timedelta(days=1826)).isoformat()
    w24 = (as_of - timedelta(days=730)).isoformat()
    con = _cx()

    aw = lance.dataset(SRC_AWARD, storage_options=opt)
    tx = lance.dataset(SRC_TXN, storage_options=opt)
    pt = lance.dataset(SRC_PSCTOOL, storage_options=opt)
    print(f"award  {SRC_AWARD} v{aw.version} rows={aw.count_rows():,}", flush=True)
    print(f"txn    {SRC_TXN} v{tx.version} rows={tx.count_rows():,}", flush=True)
    print(f"psctool {SRC_PSCTOOL} v{pt.version} rows={pt.count_rows():,}", flush=True)

    con.register("_aw", aw.scanner(
        columns=["naics_code", "product_or_service_code", "recipient_uei",
                 "awarding_agency_code", "life_to_date_obligated"],
        filter="naics_code IS NOT NULL AND product_or_service_code IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE award_pairs AS
        SELECT UPPER(TRIM(naics_code)) AS naics_code,
               UPPER(TRIM(product_or_service_code)) AS psc_code,
               COUNT(*) AS n_awards_lifetime,
               SUM(life_to_date_obligated) AS obligated_lifetime,
               COUNT(DISTINCT recipient_uei) AS n_recipients_lifetime,
               COUNT(DISTINCT awarding_agency_code) AS n_agencies_lifetime
        FROM _aw
        WHERE TRIM(naics_code) <> '' AND TRIM(product_or_service_code) <> ''
        GROUP BY 1, 2""")
    print(f"award pairs: {con.execute('SELECT count(*) FROM award_pairs').fetchone()[0]:,}",
          flush=True)

    con.register("_tx", tx.scanner(
        columns=["naics_code", "psc_code", "action_date", "obligation", "uei"],
        filter="naics_code IS NOT NULL AND psc_code IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE txn_pairs AS
        SELECT UPPER(TRIM(naics_code)) AS naics_code,
               UPPER(TRIM(psc_code)) AS psc_code,
               COUNT(*) AS n_actions_lifetime,
               SUM(obligation) AS obligation_txn_lifetime,
               COUNT(*) FILTER (action_date >= DATE '{w60}') AS n_actions_60mo,
               SUM(obligation) FILTER (action_date >= DATE '{w60}') AS obligation_60mo,
               COUNT(*) FILTER (action_date >= DATE '{w24}') AS n_actions_24mo,
               SUM(obligation) FILTER (action_date >= DATE '{w24}') AS obligation_24mo,
               COUNT(DISTINCT uei) FILTER (action_date >= DATE '{w60}') AS n_recipients_60mo,
               MIN(action_date) AS first_action_date,
               MAX(action_date) AS last_action_date
        FROM _tx
        WHERE TRIM(naics_code) <> '' AND TRIM(psc_code) <> ''
        GROUP BY 1, 2""")
    print(f"txn pairs: {con.execute('SELECT count(*) FROM txn_pairs').fetchone()[0]:,}",
          flush=True)

    con.register("_pt", pt.scanner(columns=["naics_code", "psc_code"]).to_reader())
    con.execute("""CREATE TABLE psct AS
        SELECT DISTINCT UPPER(TRIM(naics_code)) AS naics_code,
               UPPER(TRIM(psc_code)) AS psc_code
        FROM _pt WHERE naics_code IS NOT NULL AND psc_code IS NOT NULL""")

    # FULL OUTER union of the three pair sources; observed counts are
    # complete-universe → COALESCE 0; dates stay NULL when unobserved.
    con.execute(f"""CREATE TABLE merged AS
        WITH keys AS (
            SELECT naics_code, psc_code FROM award_pairs
            UNION
            SELECT naics_code, psc_code FROM txn_pairs
            UNION
            SELECT naics_code, psc_code FROM psct)
        SELECT k.naics_code, k.psc_code,
               COALESCE(a.n_awards_lifetime, 0) AS n_awards_lifetime,
               COALESCE(a.obligated_lifetime, 0) AS obligated_lifetime,
               COALESCE(a.n_recipients_lifetime, 0) AS n_recipients_lifetime,
               COALESCE(a.n_agencies_lifetime, 0) AS n_agencies_lifetime,
               COALESCE(t.n_actions_lifetime, 0) AS n_actions_lifetime,
               COALESCE(t.obligation_txn_lifetime, 0) AS obligation_txn_lifetime,
               COALESCE(t.n_actions_60mo, 0) AS n_actions_60mo,
               COALESCE(t.obligation_60mo, 0) AS obligation_60mo,
               COALESCE(t.n_actions_24mo, 0) AS n_actions_24mo,
               COALESCE(t.obligation_24mo, 0) AS obligation_24mo,
               COALESCE(t.n_recipients_60mo, 0) AS n_recipients_60mo,
               t.first_action_date, t.last_action_date,
               (p.naics_code IS NOT NULL) AS is_psctool_suggested,
               {FAMILY_SQL} AS family_key
        FROM keys k
        LEFT JOIN award_pairs a USING (naics_code, psc_code)
        LEFT JOIN txn_pairs t USING (naics_code, psc_code)
        LEFT JOIN psct p USING (naics_code, psc_code)""")

    con.execute("""CREATE TABLE final AS
        SELECT *,
            n_awards_lifetime::DOUBLE
                / NULLIF(SUM(n_awards_lifetime) OVER (PARTITION BY naics_code), 0)
                AS share_of_naics_awards,
            obligated_lifetime
                / NULLIF(SUM(obligated_lifetime) OVER (PARTITION BY naics_code), 0)
                AS share_of_naics_dollars,
            n_awards_lifetime::DOUBLE
                / NULLIF(SUM(n_awards_lifetime) OVER (PARTITION BY psc_code), 0)
                AS share_of_psc_awards,
            obligated_lifetime
                / NULLIF(SUM(obligated_lifetime) OVER (PARTITION BY psc_code), 0)
                AS share_of_psc_dollars,
            obligation_60mo
                / NULLIF(SUM(obligation_60mo) OVER (PARTITION BY naics_code), 0)
                AS share_of_naics_dollars_60mo
        FROM merged""")
    n = con.execute("SELECT count(*) FROM final").fetchone()[0]
    ns = con.execute("SELECT count(*) FROM final WHERE is_psctool_suggested").fetchone()[0]
    nz = con.execute(
        "SELECT count(*) FROM final WHERE n_awards_lifetime = 0 AND n_actions_lifetime = 0"
    ).fetchone()[0]
    print(f"final pairs: {n:,}  (psctool-suggested: {ns:,}; suggested-never-stamped: {nz:,})",
          flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of.isoformat()}' AS as_of,
        'usaspending_fpds_prime_award_state:v{aw.version}+gtm_txn_events_slim:v{tx.version}+psctool_naics_psc_map:v{pt.version}'
            AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM final""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """541330 cell check: award-grain counts/$ per psc reconcile against a direct
    award-state filter; the (541330,R425) 60mo txn cut reconciles against the slim
    mart; psctool flag matches the crosswalk; family_key matches psc_families.py."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from apps.catalyst_api.src.psc_families import family_key as fk_py

    opt = so()
    mart = lance.dataset(OUT, storage_options=opt)
    aw = lance.dataset(SRC_AWARD, storage_options=opt)
    tx = lance.dataset(SRC_TXN, storage_options=opt)
    pt = lance.dataset(SRC_PSCTOOL, storage_options=opt)
    meta = mart.scanner(columns=["as_of"], limit=1).to_table().to_pylist()[0]
    as_of = date.fromisoformat(str(meta["as_of"])[:10])
    w60 = (as_of - timedelta(days=1826)).isoformat()

    def close(a, b):
        return abs(a - b) <= max(0.01, 1e-9 * max(abs(a), abs(b)))

    mm = mart.scanner(filter="naics_code = '541330'").to_table().to_pylist()
    mside = {r["psc_code"]: r for r in mm}

    raw = aw.scanner(columns=["product_or_service_code", "life_to_date_obligated"],
                     filter="naics_code = '541330' AND product_or_service_code IS NOT NULL"
                     ).to_table().to_pylist()
    rside: dict = {}
    for r in raw:
        p = (r["product_or_service_code"] or "").strip().upper()
        if not p:
            continue
        c, s = rside.get(p, (0, 0.0))
        rside[p] = (c + 1, s + float(r["life_to_date_obligated"] or 0))
    bad = [p for p, (c, s) in rside.items()
           if p not in mside or mside[p]["n_awards_lifetime"] != c
           or not close(float(mside[p]["obligated_lifetime"]), s)]
    if bad:
        print(f"FAIL award-grain reconcile for 541330: {len(bad)} psc cells differ "
              f"(e.g. {bad[:3]})")
        return 1
    print(f"award-grain OK: 541330 × {len(rside)} PSCs reconcile (counts exact, $ within tol)")

    t = tx.scanner(columns=["action_date", "obligation"],
                   filter=f"naics_code = '541330' AND psc_code = 'R425' "
                          f"AND action_date >= DATE '{w60}'").to_table().to_pylist()
    r_n, r_o = len(t), sum(float(r["obligation"] or 0) for r in t)
    m = mside.get("R425")
    if m is None or m["n_actions_60mo"] != r_n or not close(float(m["obligation_60mo"]), r_o):
        print(f"FAIL txn 60mo reconcile (541330,R425): mart "
              f"{(m or {}).get('n_actions_60mo')}/{(m or {}).get('obligation_60mo')} "
              f"vs spine {r_n}/{r_o:,.2f}")
        return 1
    print(f"txn-grain OK: (541330,R425) 60mo = {r_n:,} actions ${r_o:,.2f} exact")

    sugg = {(r["naics_code"].strip().upper(), r["psc_code"].strip().upper())
            for r in pt.to_table(columns=["naics_code", "psc_code"]).to_pylist()
            if r["naics_code"] and r["psc_code"]}
    flagged = {(r["naics_code"], r["psc_code"])
               for r in mart.scanner(columns=["naics_code", "psc_code", "is_psctool_suggested"],
                                     filter="is_psctool_suggested = true").to_table().to_pylist()}
    if flagged != sugg:
        print(f"FAIL psctool flag: {len(flagged)} flagged vs {len(sugg)} in crosswalk "
              f"(sym-diff {len(flagged ^ sugg)})")
        return 1
    print(f"psctool flag OK: {len(flagged):,} pairs flagged, exact set match")

    fam_bad = [r for r in mm if r["family_key"] != fk_py(r["naics_code"], r["psc_code"])]
    if fam_bad:
        print(f"FAIL family_key mismatch vs psc_families.py: {len(fam_bad)} rows")
        return 1
    print(f"family_key OK: {len(mm)} rows match psc_families.family_key exactly")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
