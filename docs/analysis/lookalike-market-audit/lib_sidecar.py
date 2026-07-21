"""Shared harness for the lookalike-market audit (2026-07-21).

Every script in this directory imports this. It pins the sidecar artifact so
re-runs read the SAME snapshot, hard-codes the sample UEIs so there is no
run-time re-sampling, and centralises the raw-SQL executor.

Run any script from anywhere:
    doppler run -p core-x -c prd -- python3 docs/analysis/lookalike-market-audit/0X_*.py

If the pinned artifact has rolled the sidecar returns HTTP 409 and q() raises
ArtifactRolled — that is the signal to re-pin PIN_ARTIFACT to the current
GET /healthz stamp and note in FINDINGS that numbers legitimately move.
"""
import json, os, subprocess, sys

# --- PINNED SNAPSHOT (GET https://query-sidecar-api.onrender.com/healthz) ------
PIN_ARTIFACT = "query-sidecar/query_sidecar_20260721T020734Z.duckdb"
# built_at 2026-07-21T02:07:34Z, 107 tables. All FINDINGS numbers are against this.

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

# --- PINNED SAMPLE (no random/hash selection anywhere; edit here to change) ----
# Chosen from subaward_canonical_slim_by_sub: FY23-25 sub $ >= $1M,
# subawardee_uei <> prime_awardee_uei, spanning reseller / electronics /
# staffing / big-integrator business shapes (see sql/00_sample_selection.sql).
SAMPLE = [
    ("DT8KJHZXVJH5", "CARAHSOFT TECHNOLOGY CORP"),      # software reseller
    ("C8VFSNKTMQB6", "WORLD WIDE TECHNOLOGY LLC"),       # IT integrator/reseller
    ("UER4AJLUB8D5", "THUNDERCAT TECHNOLOGY, LLC"),      # IT reseller
    ("YVBHN7GKLQ44", "GLENAIR, INC."),                   # aerospace connectors (mfg)
    ("LW88Z8MMJHT3", "INSIGHT GLOBAL, LLC"),             # staffing/services
    ("JCBMLGPE6Z71", "BOOZ ALLEN HAMILTON INC"),         # big prime + sub
    ("Q2M4FYALZJ89", "IRON BOW TECHNOLOGIES, LLC"),      # IT integrator
    ("MMLKPW9JLX64", "SCIENCE APPLICATIONS INTL CORP"),  # big integrator
]
SAMPLE_UEIS = [u for u, _ in SAMPLE]

# Default market dials copied verbatim from sub_dossier_v1.DEFAULT_DIALS (the
# knobs that shape the gate-OFF base count). Kept here so the raw-SQL
# reconstructions never silently drift from the engine's defaults.
D_SIG_RANK = 5
D_SIG_SHARE = 0.05
D_MIN_LANE_HITS = 2
D_MARKET_PRIME_FLOOR = 1e7
MARKET_FETCH_CEILING = 25_000


class ArtifactRolled(RuntimeError):
    pass


def q(sql, limit=1000, pin=True):
    """One sidecar statement. Pins PIN_ARTIFACT unless pin=False. Returns list
    of dicts. Raises ArtifactRolled on 409 (snapshot moved)."""
    body = {"sql": sql, "limit": limit}
    if pin:
        body["require_artifact"] = PIN_ARTIFACT
    proc = subprocess.run(
        ["curl", "-s", "-w", "\n%{http_code}", "-X", "POST",
         f"{SIDECAR_URL}/api/v1/sql",
         "-H", f"Authorization: Bearer {os.environ['QUERY_SIDECAR_TOKEN']}",
         "-H", "content-type: application/json", "-d", json.dumps(body)],
        capture_output=True, text=True)
    txt = proc.stdout
    nl = txt.rfind("\n")
    payload, code = txt[:nl], txt[nl + 1:].strip()
    if code == "409":
        raise ArtifactRolled(f"pinned artifact {PIN_ARTIFACT} has rolled; re-pin to current /healthz")
    d = json.loads(payload)
    if "rows" not in d:
        print("SIDECAR ERROR:", str(d)[:400]); sys.exit(1)
    return [dict(zip(d["columns"], r)) for r in d["rows"]]


def scalar(sql):
    return q(sql, limit=2)[0]


def inlist(xs):
    return ", ".join(f"'{x}'" for x in xs)


# --- the gate-OFF base candidate query (verbatim mirror of sub_dossier_v1
#     sql_market with require_subout=False, reduced to the candidate identity +
#     wt; the fo/sub_out LEFT JOINs are omitted because under gate OFF they are
#     unreferenced and cannot change the row set — proven in 03). --------------
def cand_sql(uei, sr=D_SIG_RANK, ss=D_SIG_SHARE, mh=D_MIN_LANE_HITS,
             floor=D_MARKET_PRIME_FLOOR, select="c.uei, c.wt, c.lane_hits, b.prime_obl_60mo, b.top_naics"):
    return f"""
WITH seeds AS (
  SELECT DISTINCT prime_uei AS uei FROM gtm_prime_sub_pairs_by_sub
  WHERE sub_uei='{uei}' AND edge_dollars_5y>0 AND prime_uei<>'{uei}'),
seed_sig AS (
  SELECT s.code_type, s.code, COUNT(DISTINCT s.uei) AS seed_ct
  FROM gtm_prime_code_signature s JOIN seeds ON seeds.uei=s.uei
  WHERE s.rank_lifetime<={sr} AND s.share_lifetime>={ss} GROUP BY 1,2),
lane_freq AS (
  SELECT s.code_type, s.code, COUNT(DISTINCT s.uei) AS n_with_lane
  FROM gtm_prime_code_signature s JOIN seed_sig g ON g.code_type=s.code_type AND g.code=s.code
  WHERE s.rank_lifetime<={sr} AND s.share_lifetime>={ss} GROUP BY 1,2),
cand AS (
  SELECT s.uei, COUNT(*) AS lane_hits,
         SUM(g.seed_ct * s.share_lifetime / LN(1 + f.n_with_lane)) AS wt
  FROM gtm_prime_code_signature s
  JOIN seed_sig g ON g.code_type=s.code_type AND g.code=s.code
  JOIN lane_freq f ON f.code_type=s.code_type AND f.code=s.code
  WHERE s.rank_lifetime<={sr} AND s.share_lifetime>={ss}
    AND s.uei<>'{uei}' AND s.uei NOT IN (SELECT uei FROM seeds)
  GROUP BY 1 HAVING COUNT(*)>={mh})
SELECT {select}
FROM cand c JOIN gtm_entity_behavior_rollup b USING(uei)
WHERE b.prime_obl_60mo >= {floor}
ORDER BY c.wt DESC, c.uei"""


def shape_naics(uei):
    """The target's demonstrated work-shape NAICS (sub-side lanes, obl>=250k,
    top 12 by obl) — the engine's top_naics for the 'sub' archetype."""
    return [r["code"] for r in q(
        f"SELECT code FROM gtm_entity_code_lanes WHERE uei='{uei}' AND side='sub' "
        f"AND code_type='naics' AND obl_lifetime>=250000 ORDER BY obl_lifetime DESC LIMIT 12")]
