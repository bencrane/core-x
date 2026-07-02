#!/usr/bin/env python3
"""subaward_naics_psc_wide — widened (2021+) FFATA subaward-line combo table.

The wide counterpart of ``subaward_naics_psc`` (the narrow 25K feed the capability recommender
reads today). One row per FFATA subaward line for the FULL subawardee universe since a fixed
floor (default 2021-01-01), seeded from BOTH sub sources and deduped on the subaward identity:

  • ``usaspending/subaward_search``            — bulk mirror, the historical breadth
  • ``usaspending_api_fresh/contract_subaward`` — API-fresh feed, the recent edge (incl. the
                                                  latest refresh); mirror preferred where both hold a line

PROCUREMENT ONLY (``prime_award_unique_key LIKE 'CONT_%'``), matching the narrow table's scope.
Assistance/grant subawards (``ASST_*`` keys) are excluded: they carry no PSC (PSC is a contract
concept; grants use CFDA) and cannot form a NAICS×PSC lane. This is why contract subawards reach
~100% PSC coverage while a raw whole-mirror read would look like ~30% (grant dilution).

Neither sub source carries the prime award's PSC (FSRS does not collect subaward-level codes),
so ``prime_psc_code`` / ``prime_psc_description`` are attached via an INDEXED FPDS join:
``subaward.prime_award_unique_key -> transaction_search_fpds.generated_unique_award_id`` (one PSC
per award, latest by action_date), scanned by the ``recipient_uei`` BTREE pushdown over the
subaward population's prime contractors.

Schema-identical to the narrow ``subaward_naics_psc`` (13 cols) so ``capability_lanes`` can consume
it by pointing its source URI here. Written to a DISTINCT table; the narrow table is NOT touched.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      python3 scripts/build_subaward_naics_psc_wide.py [floor]
Read-only sources; one Lance write (overwrite). Doppler core-x/prd.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

A = "s3://data-sink/active"
SUBS = f"{A}/usaspending/subaward_search/"
FRESH = f"{A}/usaspending_api_fresh/contract_subaward/"
FPDS = f"{A}/usaspending/transaction_search_fpds/"
OUT = os.environ.get("SUBAWARD_NAICS_PSC_WIDE_URI", f"{A}/subaward_naics_psc_wide").rstrip("/") + "/"
FLOOR = os.environ.get("SUBAWARD_NAICS_PSC_WIDE_FLOOR", "2021-01-01")
# FSRS subaward_amount is notoriously dirty — a single mis-keyed line ($39.16T, verified
# 2026-07-01) dominated every dollar sum. Only GROSS mis-keys are NULLed: the threshold sits in
# the empty gap between the largest REAL line (~$5.5B, a Lockheed megasub) and the $39.16T
# impossible one — so all plausible $1–5.5B megasubs are KEPT (exactly one line exceeds it today).
# A mis-key is an UNKNOWN, not a capped value → NULL, not clamp; counts/medians are unaffected.
AMT_MAX = float(os.environ.get("SUBAWARD_AMOUNT_MAX", "2e10"))  # $20B
FPDS_PSC_FLOOR = os.environ.get("SNPW_FPDS_FLOOR", "2018-01-01")  # bound the FPDS scan; active 2021+ awards land after this
BATCH = 4000
INDEX_COLS = ["subawardee_uei", "prime_awardee_uei", "prime_award_unique_key",
              "prime_naics_code", "prime_psc_code"]
SCRATCH = os.environ.get("SNPW_SCRATCH", "/tmp/subaward_naics_psc_wide")


def log(m): print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _inlist(vals): return ",".join("'" + v.replace("'", "''") + "'" for v in vals)
def _batched(seq, n): return [seq[i:i + n] for i in range(0, len(seq), n)]


def main() -> int:
    import duckdb
    import lance
    import pyarrow as pa

    floor = sys.argv[1] if len(sys.argv) > 1 else FLOOR
    opt = so()
    os.makedirs(SCRATCH, exist_ok=True); os.makedirs(f"{SCRATCH}/spill", exist_ok=True)
    con = duckdb.connect(f"{SCRATCH}/build.duckdb")
    con.execute("PRAGMA threads=4;"); con.execute(f"PRAGMA temp_directory='{SCRATCH}/spill';")
    for t in ("ss", "fr", "lines", "fp", "psc", "final"):
        con.execute(f"DROP TABLE IF EXISTS {t};")

    # 1. bulk mirror — subaward_search >= floor
    ss = lance.dataset(SUBS, storage_options=opt)
    con.register("ss_src", ss.scanner(columns=[
        "sub_awardee_or_recipient_uei", "sub_awardee_or_recipient_legal", "awardee_or_recipient_uei",
        "awardee_or_recipient_legal", "unique_award_key", "subaward_number", "subaward_amount",
        "sub_action_date", "naics", "naics_description", "subaward_description"],
        filter=f"sub_action_date >= DATE '{floor}' AND sub_action_date <= DATE '2026-12-31'").to_reader())
    con.execute(f"""CREATE TABLE ss AS SELECT
        nullif(trim(sub_awardee_or_recipient_uei),'')  AS subawardee_uei,
        nullif(trim(sub_awardee_or_recipient_legal),'') AS subawardee_name,
        nullif(trim(awardee_or_recipient_uei),'')      AS prime_awardee_uei,
        nullif(trim(awardee_or_recipient_legal),'')    AS prime_awardee_name,
        trim(unique_award_key)                         AS prime_award_unique_key,
        nullif(trim(subaward_number),'')               AS subaward_number,
        CASE WHEN abs(try_cast(subaward_amount AS DOUBLE)) > {AMT_MAX:.0f}
             THEN NULL ELSE try_cast(subaward_amount AS DOUBLE) END AS subaward_amount,
        cast(try_cast(sub_action_date AS DATE) AS VARCHAR) AS subaward_action_date,
        nullif(trim(naics),'')                         AS prime_naics_code,
        nullif(trim(naics_description),'')             AS prime_naics_description,
        nullif(trim(subaward_description),'')          AS subaward_description,
        0 AS pr
      FROM ss_src WHERE nullif(trim(sub_awardee_or_recipient_uei),'') IS NOT NULL
        AND starts_with(trim(unique_award_key), 'CONT_')""")
    con.unregister("ss_src")

    # 2. API-fresh feed >= floor
    fr = lance.dataset(FRESH, storage_options=opt)
    con.register("fr_src", fr.scanner(columns=[
        "subawardee_uei", "subawardee_name", "prime_awardee_uei", "prime_awardee_name",
        "prime_award_unique_key", "subaward_number", "subaward_amount", "subaward_action_date",
        "prime_award_naics_code", "prime_award_naics_description", "subaward_description"]).to_reader())
    con.execute(f"""CREATE TABLE fr AS SELECT
        nullif(trim(subawardee_uei),'')                AS subawardee_uei,
        nullif(trim(subawardee_name),'')               AS subawardee_name,
        nullif(trim(prime_awardee_uei),'')             AS prime_awardee_uei,
        nullif(trim(prime_awardee_name),'')            AS prime_awardee_name,
        trim(prime_award_unique_key)                   AS prime_award_unique_key,
        nullif(trim(subaward_number),'')               AS subaward_number,
        CASE WHEN abs(try_cast(subaward_amount AS DOUBLE)) > {AMT_MAX:.0f}
             THEN NULL ELSE try_cast(subaward_amount AS DOUBLE) END AS subaward_amount,
        cast(try_cast(subaward_action_date AS DATE) AS VARCHAR) AS subaward_action_date,
        nullif(trim(prime_award_naics_code),'')        AS prime_naics_code,
        nullif(trim(prime_award_naics_description),'') AS prime_naics_description,
        nullif(trim(subaward_description),'')          AS subaward_description,
        1 AS pr
      FROM fr_src
      WHERE nullif(trim(subawardee_uei),'') IS NOT NULL
        AND starts_with(trim(prime_award_unique_key), 'CONT_')
        AND try_cast(subaward_action_date AS DATE) >= DATE '{floor}'
        AND try_cast(subaward_action_date AS DATE) <= DATE '2026-12-31'""")
    con.unregister("fr_src")

    # 3. union + dedup on subaward identity; prefer the mirror (pr=0), then latest action
    con.execute("""CREATE TABLE lines AS
      WITH u AS (SELECT * FROM ss UNION ALL SELECT * FROM fr),
      r AS (SELECT *, row_number() OVER (
              PARTITION BY prime_award_unique_key, coalesce(subaward_number,''), subawardee_uei
              ORDER BY pr ASC, subaward_action_date DESC NULLS LAST) rn FROM u)
      SELECT * EXCLUDE(pr, rn) FROM r WHERE rn = 1""")
    nlines = con.execute("SELECT count(*) FROM lines").fetchone()[0]
    primes = [r[0] for r in con.execute(
        "SELECT DISTINCT prime_awardee_uei FROM lines WHERE prime_awardee_uei IS NOT NULL").fetchall()]
    log(f"union lines={nlines:,}  distinct_primes={len(primes):,}  floor={floor}")

    # 4. FPDS PSC map — recipient_uei pushdown, one PSC per award (latest action)
    con.execute("CREATE TABLE fp(gid VARCHAR, psc VARCHAR, psc_desc VARCHAR, adt DATE);")
    fpds = lance.dataset(FPDS, storage_options=opt)
    for bi, b in enumerate(_batched(primes, BATCH)):
        flt = f"recipient_uei IN ({_inlist(b)}) AND action_date >= DATE '{FPDS_PSC_FLOOR}'"
        tbl = fpds.scanner(columns=["generated_unique_award_id", "product_or_service_code",
                                    "product_or_service_description", "action_date"],
                           filter=flt).to_table()
        con.register("t", tbl)
        con.execute("""INSERT INTO fp SELECT trim(generated_unique_award_id),
            nullif(trim(product_or_service_code),''), nullif(trim(product_or_service_description),''),
            CAST(action_date AS DATE) FROM t WHERE nullif(trim(product_or_service_code),'') IS NOT NULL""")
        con.unregister("t")
        log(f"  FPDS PSC batch {bi+1}/{len(_batched(primes, BATCH))} rows+={tbl.num_rows}")
    con.execute("""CREATE TABLE psc AS
      WITH r AS (SELECT gid, psc, psc_desc,
                   row_number() OVER (PARTITION BY gid ORDER BY adt DESC NULLS LAST) rn FROM fp)
      SELECT gid, psc, psc_desc FROM r WHERE rn = 1""")

    # 5. assemble (narrow-table column order) + attach PSC by award key
    con.execute("""CREATE TABLE final AS SELECT
        l.subaward_number, l.prime_award_unique_key, l.prime_awardee_uei, l.prime_awardee_name,
        l.subawardee_uei, l.subawardee_name, l.subaward_amount, l.subaward_action_date,
        l.prime_naics_code, l.prime_naics_description,
        p.psc AS prime_psc_code, p.psc_desc AS prime_psc_description,
        l.subaward_description
      FROM lines l LEFT JOIN psc p ON p.gid = l.prime_award_unique_key""")

    tbl = con.execute("SELECT * FROM final").arrow()
    if isinstance(tbl, pa.RecordBatchReader):
        tbl = tbl.read_all()
    rows = tbl.num_rows
    if rows == 0:
        raise RuntimeError("0 rows assembled")
    lance.write_dataset(tbl, OUT, mode="overwrite", data_storage_version="2.1",
                        max_rows_per_file=500_000, storage_options=opt)
    ds = lance.dataset(OUT, storage_options=opt)
    for c in INDEX_COLS:
        ds.create_scalar_index(c, index_type="BTREE"); log(f"  BTREE {c}")

    st = con.execute("""SELECT count(*), count(DISTINCT subawardee_uei),
        count(*) FILTER (WHERE prime_psc_code IS NOT NULL),
        count(*) FILTER (WHERE prime_naics_code IS NOT NULL AND prime_psc_code IS NOT NULL)
        FROM final""").fetchone()
    con.close()
    log(f"DONE rows={st[0]:,} subawardees={st[1]:,} with_psc={st[2]:,} "
        f"({st[2]/st[0]:.0%}) with_full_combo={st[3]:,} ({st[3]/st[0]:.0%}) -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
