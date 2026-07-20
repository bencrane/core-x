"""Bake the Subawardee Market viewer fixture (rare-structure-hq /viewer tab).

Reconstructs, durably, the generator that shipped rshq #325/#326/#327/#328 —
it previously lived in a session scratchpad and died with a worktree recycle.

Cohort: every subawardee whose FY23–25 total (FSRS subaward $ received +
FPDS prime obligations won) >= $1M. Per firm:
  - sub $ / prime $ / totals, subaward count, avg + median subaward size
  - employee band / state / primary NAICS (+ title) from the audience spine
  - normalized_domain + domain_source (SAM entity_url ∪ DSBS)
  - top-3 OWN-prime combos (txn_events_combo, share of own positive prime $)
  - top-3 SUBBED-UNDER combos (the PRIME award's NAICS×PSC on their
    subawards — describes the prime's work, NEVER the sub's)
plus a combo→language map: gpt-5.4 JTBD phrase (Lance combo_job_to_be_done,
operator trust ruling: gpt-5.4 only) + work_summary (naics_psc_labor_profile).

All sidecar statements pin ``require_artifact`` to the first response's
artifact so the fixture is internally consistent.

Run (writes the rshq fixture in place; the viewer is fixture-driven):
    doppler run -p core-x -c prd -- \
        python3 pipelines/serving/bake_subawardee_market_fixture.py \
        [--out /path/to/subawardee-market.json]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.request

DEFAULT_OUT = (
    "/Users/benjamincrane/rare-structure-hq/apps/platform-app/src/internal/subawardee-market.json"
)
SIDECAR = "https://query-sidecar-api.onrender.com/api/v1/sql"
JTBD_URI = "s3://data-sink/active/combo_job_to_be_done/"

COHORT_CTE = """
WITH sub AS (
  SELECT subawardee_uei AS uei,
         sum(subaward_amount_num) AS sub_amt,
         count(*) AS n_subs,
         avg(subaward_amount_num) AS avg_sub,
         median(subaward_amount_num) AS med_sub,
         arg_max(subawardee_name, subaward_action_date) AS sub_name
  FROM subaward_canonical_slim_by_sub
  WHERE subaward_action_date_fiscal_year BETWEEN 2023 AND 2025
    AND subawardee_uei IS NOT NULL
  GROUP BY 1
),
pr AS (
  SELECT uei, sum(won_obl) AS prime_amt, sum(action_ct) AS prime_action_ct
  FROM gtm_entity_fy_won WHERE fy BETWEEN 2023 AND 2025 GROUP BY 1
),
j AS (
  SELECT s.uei, s.sub_amt, s.n_subs, s.avg_sub, s.med_sub, s.sub_name,
         coalesce(p.prime_amt, 0) AS prime_amt,
         coalesce(p.prime_action_ct, 0) AS prime_action_ct
  FROM sub s LEFT JOIN pr p USING (uei)
  WHERE s.sub_amt > 0 AND s.sub_amt + coalesce(p.prime_amt, 0) >= 1000000
)
"""

MAIN_SQL = COHORT_CTE + """
SELECT j.uei,
       coalesce(ae.legal_business_name, j.sub_name) AS name,
       ae.physical_state AS state,
       ae.employee_size_band AS band,
       ae.primary_naics AS naics,
       nn.naics_title AS naics_name,
       round(j.sub_amt) AS sub_amt,
       round(j.prime_amt) AS prime_amt,
       j.n_subs,
       round(j.avg_sub) AS avg_sub,
       round(j.med_sub) AS med_sub,
       j.prime_action_ct,
       se.normalized_domain AS domain,
       se.domain_source AS domain_source
FROM j
LEFT JOIN gtm_audience_entities ae ON ae.uei = j.uei
LEFT JOIN v_naics_names nn ON nn.naics_code = ae.primary_naics
LEFT JOIN gtm_sam_entities se ON se.uei = j.uei
ORDER BY j.sub_amt + j.prime_amt DESC
"""

# NOTE: '~' separates packed combos ("NxP|share~...") — the sidecar API's
# statement splitter treats ';' inside literals as multi-statement.
PRIME_TOP3_SQL = COHORT_CTE + """
, coh AS (SELECT uei FROM j)
, pc AS (
  SELECT t.uei, t.naics_code, t.psc_code, sum(t.obligation) AS amt
  FROM txn_events_combo t SEMI JOIN coh ON t.uei = coh.uei
  WHERE t.fy BETWEEN 2023 AND 2025 GROUP BY 1,2,3
), ranked AS (
  SELECT uei, naics_code, psc_code, amt,
         row_number() OVER (PARTITION BY uei ORDER BY amt DESC) AS rk,
         sum(amt) FILTER (WHERE amt > 0) OVER (PARTITION BY uei) AS tot
  FROM pc WHERE amt > 0
)
SELECT uei, string_agg(
         coalesce(naics_code,'?') || 'x' || coalesce(psc_code,'?') || '|' ||
         round(100.0 * amt / tot)::INT, '~' ORDER BY rk) AS top3
FROM ranked WHERE rk <= 3 GROUP BY uei
"""

SUB_TOP3_SQL = COHORT_CTE + """
, coh AS (SELECT uei FROM j)
, sc AS (
  SELECT b.subawardee_uei AS uei, b.prime_award_naics_code AS naics_code,
         b.prime_award_product_or_service_code AS psc_code,
         sum(b.subaward_amount_num) AS amt
  FROM subaward_canonical_slim_by_sub b SEMI JOIN coh ON b.subawardee_uei = coh.uei
  WHERE b.subaward_action_date_fiscal_year BETWEEN 2023 AND 2025
  GROUP BY 1,2,3
), ranked AS (
  SELECT uei, naics_code, psc_code, amt,
         row_number() OVER (PARTITION BY uei ORDER BY amt DESC) AS rk,
         sum(amt) FILTER (WHERE amt > 0) OVER (PARTITION BY uei) AS tot
  FROM sc WHERE amt > 0
)
SELECT uei, string_agg(
         coalesce(naics_code,'?') || 'x' || coalesce(psc_code,'?') || '|' ||
         round(100.0 * amt / tot)::INT, '~' ORDER BY rk) AS top3
FROM ranked WHERE rk <= 3 GROUP BY uei
"""


def _token() -> str:
    return subprocess.check_output(
        ["doppler", "secrets", "get", "QUERY_SIDECAR_TOKEN", "-p", "core-x", "-c", "prd", "--plain"],
        text=True).strip()


def _run_sql(token: str, sql: str, artifact: str | None = None) -> dict:
    body: dict = {"sql": sql, "limit": 50000}
    if artifact:
        body["require_artifact"] = artifact
    req = urllib.request.Request(
        SIDECAR, data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}", "content-type": "application/json"})
    try:
        resp = json.load(urllib.request.urlopen(req, timeout=300))
    except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
        sys.exit(f"HTTP {e.code}: {e.read().decode()[:500]}")
    assert not resp.get("truncated"), "sidecar result truncated"
    return resp


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    token = _token()
    main_resp = _run_sql(token, MAIN_SQL)
    artifact = main_resp["artifact"]
    rows = main_resp["rows"]

    prime_map = {r[0]: r[1] for r in _run_sql(token, PRIME_TOP3_SQL, artifact)["rows"]}
    sub_map = {r[0]: r[1] for r in _run_sql(token, SUB_TOP3_SQL, artifact)["rows"]}
    for row in rows:
        row.append(prime_map.get(row[0]))
        row.append(sub_map.get(row[0]))

    # combo language: work_summary (sidecar) + gpt-5.4 JTBD (Lance)
    needed: set[str] = set()
    for row in rows:
        for packed in (row[14], row[15]):
            if packed:
                for part in packed.split("~"):
                    needed.add(part.split("|")[0])

    ws_map = {r[0]: r[1] for r in _run_sql(
        token, "SELECT naics_code || 'x' || psc_code, work_summary FROM naics_psc_labor_profile",
        artifact)["rows"]}

    import lance
    jt = lance.dataset(JTBD_URI, storage_options=_r2_storage_options())
    jt_map = {
        f"{r['naics_code']}x{r['psc_code']}": r["output_sentence"]
        for r in jt.to_table(filter="model_id = 'gpt-5.4'",
                             columns=["naics_code", "psc_code", "output_sentence"]).to_pylist()
    }
    combo_lang = {}
    for key in needed:
        jtbd, ws = jt_map.get(key), ws_map.get(key)
        if jtbd or ws:
            combo_lang[key] = [jtbd, ws]

    out = {
        "window": "FY2023–FY2025 (2022-10-01 → 2025-09-30)",
        "artifact": artifact,
        "cohort": "subawardee with sub+prime total >= $1M in window",
        "columns": ["uei", "name", "state", "band", "naics", "naics_name",
                    "sub_amt", "prime_amt", "n_subs", "avg_sub", "med_sub",
                    "prime_action_ct", "domain", "domain_source",
                    "top3_prime", "top3_sub"],
        "rows": rows,
        "combo_lang": combo_lang,
    }
    with open(args.out, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"{len(rows):,} rows | combo_lang {len(combo_lang):,}/{len(needed):,} covered "
          f"-> {args.out} ({os.path.getsize(args.out)/1e6:.1f} MB) artifact={artifact}")


if __name__ == "__main__":
    main()
