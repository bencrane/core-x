"""Compute worker — SEC ADV × UCC private-credit-book crosswalk (GLEIF-anchored bridge).

The ``resolution-sec-adv-ucc-pipelines`` Modal app. A standalone bridge dataset that links
every SEC-registered private-fund MANAGER (Form ADV Schedule D adviser) to the UCC-1
financing statements on which it — or a lending vehicle it controls — is the secured party.
Those debtors are the closest public proxy for a private-credit fund's holdings: the credit
book reconstructed from CA + CO UCC filings. Spawned by the Universal Dispatcher; no web
endpoint.

WHY GLEIF IS THE SPINE. A naive fuzzy join of ADV manager names to UCC secured-party names
poisons itself: private-credit funds lend through SPVs and administrative agents, so the
manager's registered name is rarely the verbatim secured party, AND generic tokens collide
(HIGHLAND CAPITAL CORP [equipment finance] ≠ Highland Capital Management LP; APOGEE ≠ APOGEM;
CARDINAL HEALTH ≠ Varde's "Cardinal LT"). The fix: resolve BOTH sides to the clean, canonical
GLEIF Legal-Entity registry — never to each other — then join on the LEI graph. The manager
anchors to a GLEIF LEI (via its ADV adviser_lei, else an exact canonical-name match); its
lending family is the anchor plus GLEIF L2 ACTIVE *descendant* edges (parent→child only — a
UCC lender is the manager or a vehicle it controls, never its upstream parent bank); each UCC
secured party resolves to a GLEIF LEI after its role suffix (", AS ADMINISTRATIVE AGENT") is
stripped. A filing links to a manager iff a resolved secured-party LEI is in that manager's
family. Everything here is deterministic and reproducible from the plane.

Source-of-truth inputs (read-only; this worker never mutates them):
  - sec_adv_private_funds     s3://…/active/sec_adv_private_funds/     (adviser↔fund grain)
  - sec_adv_private_credit    s3…/active/sec_adv_private_credit/       (pc_flag per fund)
  - gleif_l1_entities         s3…/active/gleif_l1_entities/            (LEI → canonical name)
  - gleif_l2_relationships    s3…/active/gleif_l2_relationships/       (LEI parent→child graph)
  - ucc_filings_all           s3…/active/ucc_filings_all/              (CA+CO, secured_parties)

LLM overlay (optional, additive). The deterministic spine covers the exact/agent-suffix
cases. A curated overlay of fuzzy near-misses adjudicated by in-session Claude subagents
(NOT the Anthropic API) — each verified against an adversarial refutation pass — is checked
in at ``seeds/sec_adv_ucc_llm_links.json``. When present it is expanded against the same UCC
party set and unioned in with ``match_method='llm_adjudicated'``. Absent the seed, the
pipeline still produces the full deterministic dataset.

Grain & output (100% DuckDB transform, Arrow interchange):
  1 row per (manager crd × resolved lender entity × UCC filing/debtor). ``match_method``
  separates the deterministic spine from the LLM overlay. Written to R2 active (overwrite),
  BTREE on crd_number / lender_lei / filing_id / debtor_uei.

Coverage ceiling (documented, not silent): UCC is CA + CO only; a private-credit lender's
SPV that carries no LEI and is not a fuzzy variant of a known manager stays invisible. This
is a lower bound on each manager's book, never the complete portfolio.

    modal run    pipelines/resolution/crosswalk_sec_adv_ucc.py::init_ops   # create ops table
    modal deploy pipelines/resolution/crosswalk_sec_adv_ucc.py             # dispatcher-resolvable
    modal run    pipelines/resolution/crosswalk_sec_adv_ucc.py             # spawn build on the DEPLOYED app → prints fc-… and returns
    modal run    pipelines/resolution/crosswalk_sec_adv_ucc.py --dry-run   # coverage plan, local, no write, no ops row

Follow a spawned build: ``modal app logs resolution-sec-adv-ucc-pipelines``, or poll
``modal.FunctionCall.from_id("fc-…").get(timeout=0)``. The worker writes its own terminal
row to ``ops.crosswalk_sec_adv_ucc_runs``; the build metrics dict lives in that row and in
the app logs (the entrypoint no longer prints it). Phase 2 — ``verify_crosswalk`` — is NOT
auto-chained: only after the build fc reaches terminal success (ledger status='success' or
``fc.get()`` returns) spawn it on the deployed app —
``modal.Function.from_name("resolution-sec-adv-ucc-pipelines", "verify_crosswalk").spawn()``
— and collect the handle. Never spawn verify concurrently with a build; it would read the
pre-build dataset.
"""

from __future__ import annotations

import os

import modal

from core.name_norm import legal_name_base as _base
from core.name_norm import name_norm as _name_norm

BUCKET = "data-sink"
GLEIF_L1_URI = os.environ.get("GLEIF_L1_LANCE_URI", "s3://data-sink/active/gleif_l1_entities/")
GLEIF_L2_URI = os.environ.get("GLEIF_L2_LANCE_URI", "s3://data-sink/active/gleif_l2_relationships/")
ADV_FUNDS_URI = os.environ.get("SEC_ADV_PRIVATE_FUNDS_URI", "s3://data-sink/active/sec_adv_private_funds/")
ADV_PC_URI = os.environ.get("SEC_ADV_PRIVATE_CREDIT_URI", "s3://data-sink/active/sec_adv_private_credit/")
UCC_URI = os.environ.get("UCC_FILINGS_ALL_URI", "s3://data-sink/active/ucc_filings_all/")
DATASET_URI = os.environ.get("SEC_ADV_UCC_LINK_URI", "s3://data-sink/active/sec_adv_ucc_link/")
FEED = "crosswalk_sec_adv_ucc"

SEED_PATH = os.path.join(os.path.dirname(__file__), "seeds", "sec_adv_ucc_llm_links.json")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Resolution keys → BTREE (equality point-lookup).
BTREE_INDEXES = ["crd_number", "lender_lei", "filing_id", "debtor_uei"]

DUCKDB_MEMORY_LIMIT = "24GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# Trailing role designators appended to a lender name on a UCC filing. Stripped off the
# NORMALIZED name so it exact-resolves to the GLEIF legal name; the role is captured
# separately. Precision-safe: the family-membership gate still filters any third-party
# agent that isn't in a manager's descendant set.
ROLE_RE = (
    "( AS)?( THE)? (ADMINISTRATIVE|COLLATERAL|SECURITY|SUB|SUCCESSOR|SYNDICATION|FACILITY|"
    "PAYING)? ?(AGENT|REPRESENTATIVE|TRUSTEE|SECURED PARTY|LENDER).*$"
)

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_sec_adv_ucc_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text        NOT NULL,
    dataset_uri              text        NOT NULL,
    gleif_publish_date       text,
    advisers_total           bigint,
    advisers_pc              bigint,
    advisers_anchored        bigint,
    ucc_party_rows           bigint,
    ucc_party_resolved       bigint,
    link_rows                bigint,
    link_rows_llm            bigint,
    distinct_managers        bigint,
    distinct_pc_managers     bigint,
    distinct_debtor_filings  bigint,
    rows_written             bigint,
    status                   text        NOT NULL,
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_sec_adv_ucc_runs_feed_idx
    ON ops.crosswalk_sec_adv_ucc_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sec_adv_ucc_runs_status_idx
    ON ops.crosswalk_sec_adv_ucc_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sec_adv_ucc_runs_recorded_at_idx
    ON ops.crosswalk_sec_adv_ucc_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env(
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm").add_local_file(
    SEED_PATH, "/root/seeds/sec_adv_ucc_llm_links.json", copy=True
)

app = modal.App("resolution-sec-adv-ucc-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / S3
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


# --------------------------------------------------------------------------- #
# DuckDB transform — pure SQL builders (importable without modal/auth).
# name_norm / legal_name_base are the canonical blocking-key macros, so every normalized
# name here is byte-compatible with the rest of the resolution layer.
# --------------------------------------------------------------------------- #
def _gleif_temp_sql() -> str:
    """GLEIF L1 → 1 row/lei with canonical name key; name→best-lei resolver (exact when a
    normalized name is unique, else a same-country dup-registration best-pick); directed
    ACTIVE parent→child edges."""
    return f"""
CREATE TEMP TABLE g AS
SELECT lei, legal_name, {_name_norm('legal_name')} AS nln,
       legal_address_region AS region, legal_address_country AS country, entity_status,
       publish_date
FROM g1_src WHERE nullif(trim(lei),'') IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY lei ORDER BY publish_date DESC NULLS LAST)=1;

CREATE TEMP TABLE gname AS
WITH per AS (
  SELECT nln, lei, entity_status, country,
         count(DISTINCT lei)     OVER (PARTITION BY nln) AS n_lei,
         count(DISTINCT country) OVER (PARTITION BY nln) AS n_country
  FROM g WHERE nln IS NOT NULL
)
SELECT nln, n_lei, n_country,
       arg_min(lei, (CASE WHEN entity_status='ISSUED' THEN 0 ELSE 1 END,
                     CASE WHEN country='US' THEN 0 ELSE 1 END, lei)) AS lei,
       CASE WHEN n_lei=1 THEN 'exact' WHEN n_country=1 THEN 'multi' END AS res_conf
FROM per GROUP BY nln, n_lei, n_country;

CREATE TEMP TABLE child_edge AS
SELECT DISTINCT parent_lei AS parent, lei AS child FROM g2_src
WHERE relationship_status='ACTIVE'
  AND nullif(trim(lei),'') IS NOT NULL AND nullif(trim(parent_lei),'') IS NOT NULL;
"""


def _adviser_temp_sql() -> str:
    """ADV adviser grain (1 row/crd), PC flag, and the GLEIF anchor (adviser_lei exact →
    else canonical-name resolve); the manager family = anchor ∪ directed descendants (2 hops)."""
    return f"""
CREATE TEMP TABLE adviser AS
SELECT crd_number,
       any_value(adviser_legal_name) AS adviser_legal_name,
       max(adviser_lei) FILTER (WHERE nullif(trim(adviser_lei),'') IS NOT NULL) AS adviser_lei,
       count(DISTINCT fund_id) AS n_funds
FROM advf_src WHERE nullif(trim(crd_number),'') IS NOT NULL GROUP BY 1;

CREATE TEMP TABLE pc_crd AS SELECT DISTINCT crd_number FROM pc_src WHERE pc_flag=TRUE;

CREATE TEMP TABLE adv_anchor AS
SELECT a.crd_number, a.adviser_legal_name, a.adviser_lei, a.n_funds,
       {_name_norm('a.adviser_legal_name')} AS adviser_nln,
       (a.crd_number IN (SELECT crd_number FROM pc_crd)) AS pc_flag,
       CASE WHEN gl.lei IS NOT NULL THEN a.adviser_lei
            WHEN gn.lei IS NOT NULL THEN gn.lei END AS anchor_lei,
       CASE WHEN gl.lei IS NOT NULL THEN 'lei'
            WHEN gn.lei IS NOT NULL THEN 'name:'||gn.res_conf END AS anchor_path
FROM adviser a
LEFT JOIN g gl ON gl.lei = a.adviser_lei
LEFT JOIN gname gn ON gn.nln = {_name_norm('a.adviser_legal_name')} AND gn.res_conf IS NOT NULL;

CREATE TEMP TABLE manager_family AS
SELECT DISTINCT crd_number, anchor_lei AS fam_lei, 'anchor' AS via FROM adv_anchor WHERE anchor_lei IS NOT NULL
UNION
SELECT DISTINCT aa.crd_number, ce.child AS fam_lei, 'child'
FROM adv_anchor aa JOIN child_edge ce ON ce.parent = aa.anchor_lei WHERE aa.anchor_lei IS NOT NULL
UNION
SELECT DISTINCT aa.crd_number, ce2.child AS fam_lei, 'grandchild'
FROM adv_anchor aa
JOIN child_edge ce1 ON ce1.parent = aa.anchor_lei
JOIN child_edge ce2 ON ce2.parent = ce1.child
WHERE aa.anchor_lei IS NOT NULL;
"""


def _ucc_party_temp_sql() -> str:
    """UCC secured_parties exploded on '; ', normalized, role-suffix stripped, and resolved
    to a GLEIF LEI (exact-unique or same-country dup best-pick)."""
    return f"""
CREATE TEMP TABLE ucc_party AS
SELECT filing_id, ucc_state, uei, is_org, debtor_name, debtor_state,
       filing_class, is_active_financing, is_lease,
       trim(party) AS party_raw,
       {_name_norm('trim(party)')} AS party_nln,
       nullif(trim(regexp_replace({_name_norm('trim(party)')}, '{ROLE_RE}', '')), '') AS party_nln_clean,
       (regexp_matches({_name_norm('trim(party)')}, '{ROLE_RE}')) AS is_agent_role
FROM (SELECT *, unnest(string_split(secured_parties,'; ')) AS party
      FROM ucc_src WHERE secured_parties IS NOT NULL)
WHERE {_name_norm('trim(party)')} IS NOT NULL;

CREATE TEMP TABLE ucc_party_r AS
SELECT up.*,
  CASE WHEN up.is_agent_role THEN 'agent' ELSE 'party' END AS party_role,
  gn.lei AS party_lei, gn.res_conf AS party_res_conf
FROM ucc_party up
LEFT JOIN gname gn ON gn.nln = coalesce(up.party_nln_clean, up.party_nln) AND gn.res_conf IS NOT NULL;
"""


def _link_sql() -> str:
    """Deterministic link: a resolved UCC secured party whose LEI is in a manager's GLEIF
    family. Grain = one row per (crd, lender lei, filing); when the same lender is reachable
    by several family paths or appears in multiple roles on one filing, keep the strongest
    (family_via anchor>child>grandchild, then role party>agent)."""
    return """
CREATE TEMP TABLE link_det AS
SELECT crd_number, adviser_legal_name, anchor_lei, anchor_path, pc_flag,
       lender_lei, lender_name, lender_nln, party_role, family_via,
       filing_id, ucc_state, debtor_uei, debtor_name, debtor_state,
       filing_class, is_active_financing, is_lease
FROM (
  SELECT
    mf.crd_number, aa.adviser_legal_name, aa.anchor_lei, aa.anchor_path, aa.pc_flag,
    upr.party_lei AS lender_lei, upr.party_raw AS lender_name,
    coalesce(upr.party_nln_clean, upr.party_nln) AS lender_nln,
    upr.party_role, mf.via AS family_via,
    upr.filing_id, upr.ucc_state, upr.uei AS debtor_uei, upr.debtor_name, upr.debtor_state,
    upr.filing_class, upr.is_active_financing, upr.is_lease,
    row_number() OVER (
      PARTITION BY mf.crd_number, upr.filing_id, upr.party_lei
      ORDER BY CASE mf.via WHEN 'anchor' THEN 0 WHEN 'child' THEN 1 ELSE 2 END,
               CASE upr.party_role WHEN 'party' THEN 0 ELSE 1 END) AS rn
  FROM ucc_party_r upr
  JOIN manager_family mf ON mf.fam_lei = upr.party_lei
  JOIN adv_anchor aa ON aa.crd_number = mf.crd_number
  WHERE upr.party_lei IS NOT NULL
) WHERE rn = 1;
"""


def _llm_overlay_sql() -> str:
    """LLM overlay: expand each curated (lender_nln → manager crd) accepted match against the
    UCC party set to recover every filing it names. `llm_seed` is a registered table with
    columns (lender_nln, crd_number, adviser_legal_name, llm_relationship, match_confidence,
    llm_rationale). Excludes filings already carried by the deterministic spine."""
    return """
CREATE TEMP TABLE link_llm AS
SELECT crd_number, adviser_legal_name, anchor_lei, anchor_path, pc_flag,
       lender_lei, lender_name, lender_nln, party_role, family_via,
       filing_id, ucc_state, debtor_uei, debtor_name, debtor_state,
       filing_class, is_active_financing, is_lease,
       llm_relationship, match_confidence, llm_rationale
FROM (
  SELECT
    s.crd_number, s.adviser_legal_name,
    aa.anchor_lei, aa.anchor_path, aa.pc_flag,
    CAST(NULL AS VARCHAR) AS lender_lei, upr.party_raw AS lender_name,
    coalesce(upr.party_nln_clean, upr.party_nln) AS lender_nln,
    upr.party_role, 'llm' AS family_via,
    upr.filing_id, upr.ucc_state, upr.uei AS debtor_uei, upr.debtor_name, upr.debtor_state,
    upr.filing_class, upr.is_active_financing, upr.is_lease,
    s.llm_relationship, s.match_confidence, s.llm_rationale,
    row_number() OVER (PARTITION BY s.crd_number, upr.filing_id, coalesce(upr.party_nln_clean, upr.party_nln)
      ORDER BY CASE upr.party_role WHEN 'party' THEN 0 ELSE 1 END) AS rn
  FROM llm_seed s
  JOIN ucc_party_r upr ON coalesce(upr.party_nln_clean, upr.party_nln) = s.lender_nln
  LEFT JOIN adv_anchor aa ON aa.crd_number = s.crd_number
  WHERE NOT EXISTS (
    SELECT 1 FROM link_det d WHERE d.filing_id = upr.filing_id AND d.crd_number = s.crd_number
  )
) WHERE rn = 1;
"""


def _final_sql() -> str:
    """Union deterministic spine + LLM overlay into the published grain with match_method /
    confidence / built_at. Deterministic confidence: exact-anchor+exact-party=1.0 else 0.9."""
    return """
SELECT crd_number, adviser_legal_name, anchor_lei, anchor_path, pc_flag,
       lender_lei, lender_name, lender_nln, party_role, family_via,
       filing_id, ucc_state, debtor_uei, debtor_name, debtor_state,
       filing_class, is_active_financing, is_lease,
       'deterministic' AS match_method,
       CASE WHEN anchor_path='lei' AND family_via='anchor' THEN 1.0 ELSE 0.9 END AS match_confidence,
       CAST(NULL AS VARCHAR) AS llm_relationship,
       CAST(NULL AS VARCHAR) AS llm_rationale,
       CAST(now() AS TIMESTAMP) AS built_at
FROM link_det
UNION ALL BY NAME
SELECT crd_number, adviser_legal_name, anchor_lei, anchor_path, pc_flag,
       lender_lei, lender_name, lender_nln, party_role, family_via,
       filing_id, ucc_state, debtor_uei, debtor_name, debtor_state,
       filing_class, is_active_financing, is_lease,
       'llm_adjudicated' AS match_method,
       match_confidence,
       llm_relationship, llm_rationale,
       CAST(now() AS TIMESTAMP) AS built_at
FROM link_llm
"""


def _load_seed():
    """Read the checked-in LLM overlay seed → Arrow table (or None if absent/empty)."""
    import json

    path = "/root/seeds/sec_adv_ucc_llm_links.json"
    if not os.path.exists(path):
        path = SEED_PATH
    if not os.path.exists(path):
        return None
    rows = json.load(open(path))
    if not rows:
        return None
    import pyarrow as pa

    keep = ["lender_nln", "crd_number", "adviser_legal_name", "llm_relationship",
            "match_confidence", "llm_rationale"]
    cols = {k: [r.get(k) for r in rows] for k in keep}
    return pa.table(cols)


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _materialize(con):
    """Register the five Lance sources + the LLM seed, build all temps, and return
    (arrow_table, metrics, gleif_publish_date)."""
    import lance

    so = _r2_storage_options()

    def rdr(uri, columns):
        return lance.dataset(uri, storage_options=so).scanner(columns=columns).to_reader()

    con.register("g1_src", rdr(GLEIF_L1_URI,
        ["lei", "legal_name", "legal_address_region", "legal_address_country",
         "entity_status", "publish_date"]))
    con.register("g2_src", rdr(GLEIF_L2_URI,
        ["lei", "parent_lei", "relationship_type", "relationship_status"]))
    con.execute(_gleif_temp_sql())
    gleif_pub = con.execute("SELECT max(publish_date) FROM g").fetchone()[0]
    con.unregister("g1_src")
    con.unregister("g2_src")

    con.register("advf_src", rdr(ADV_FUNDS_URI,
        ["crd_number", "adviser_legal_name", "adviser_lei", "fund_id"]))
    con.register("pc_src", rdr(ADV_PC_URI, ["crd_number", "fund_id", "pc_flag"]))
    con.execute(_adviser_temp_sql())
    con.unregister("advf_src")
    con.unregister("pc_src")

    con.register("ucc_src", rdr(UCC_URI,
        ["ucc_state", "filing_id", "uei", "is_org", "debtor_name", "debtor_state",
         "filing_class", "is_active_financing", "is_lease", "secured_parties"]))
    con.execute(_ucc_party_temp_sql())
    con.unregister("ucc_src")

    con.execute(_link_sql())

    seed = _load_seed()
    if seed is not None:
        con.register("llm_seed", seed)
        con.execute(_llm_overlay_sql())
    else:
        con.execute("""CREATE TEMP TABLE link_llm AS SELECT *,
            CAST(NULL AS VARCHAR) llm_relationship, CAST(NULL AS DOUBLE) match_confidence,
            CAST(NULL AS VARCHAR) llm_rationale FROM link_det WHERE FALSE""")

    con.execute(f"CREATE TEMP TABLE final AS {_final_sql()}")

    adv_tot, adv_pc, adv_anc = con.execute(
        "SELECT count(*), count(*) FILTER(WHERE pc_flag), count(*) FILTER(WHERE anchor_lei IS NOT NULL) "
        "FROM adv_anchor").fetchone()
    up_rows, up_res = con.execute(
        "SELECT count(*), count(*) FILTER(WHERE party_lei IS NOT NULL) FROM ucc_party_r").fetchone()
    link_rows, link_llm, mgrs, pcmgrs, debs = con.execute("""
        SELECT count(*), count(*) FILTER(WHERE match_method='llm_adjudicated'),
               count(DISTINCT crd_number), count(DISTINCT crd_number) FILTER(WHERE pc_flag),
               count(DISTINCT filing_id) FROM final""").fetchone()

    table = con.sql("SELECT * FROM final").to_arrow_table()
    metrics = {
        "advisers_total": int(adv_tot), "advisers_pc": int(adv_pc),
        "advisers_anchored": int(adv_anc), "ucc_party_rows": int(up_rows),
        "ucc_party_resolved": int(up_res), "link_rows": int(link_rows),
        "link_rows_llm": int(link_llm), "distinct_managers": int(mgrs),
        "distinct_pc_managers": int(pcmgrs), "distinct_debtor_filings": int(debs),
        "rows_written": int(link_rows),
    }
    return table, metrics, gleif_pub


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, gleif_publish_date, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.crosswalk_sec_adv_ucc_runs
                    (feed, dataset_uri, gleif_publish_date, advisers_total, advisers_pc,
                     advisers_anchored, ucc_party_rows, ucc_party_resolved, link_rows,
                     link_rows_llm, distinct_managers, distinct_pc_managers,
                     distinct_debtor_filings, rows_written, status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, DATASET_URI, gleif_publish_date, metrics.get("advisers_total"),
                 metrics.get("advisers_pc"), metrics.get("advisers_anchored"),
                 metrics.get("ucc_party_rows"), metrics.get("ucc_party_resolved"),
                 metrics.get("link_rows"), metrics.get("link_rows_llm"),
                 metrics.get("distinct_managers"), metrics.get("distinct_pc_managers"),
                 metrics.get("distinct_debtor_filings"), metrics.get("rows_written"),
                 status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Core build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 40,
    memory=32768,
    cpu=8.0,
)
def build_crosswalk(trigger_callback_url: str | None = None) -> dict:
    """Rebuild the SEC-ADV × UCC crosswalk and publish it to R2 active. Idempotent full
    overwrite; BTREE indexes rebuilt on the R2 dataset each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, gleif_pub = "error", None, None
    metrics: dict = {}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics, gleif_pub = _materialize(con)
        finally:
            con.close()
        print(f"Built crosswalk: {metrics}")

        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE ✓ {col}")
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}")
        if committed != metrics.get("rows_written"):
            raise RuntimeError(
                f"parity gate: committed {committed} != computed {metrics.get('rows_written')}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(gleif_publish_date=gleif_pub, metrics=metrics, status=status,
                    error=error, started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "dataset_uri": DATASET_URI,
                        "rows": metrics.get("rows_written"), **{k: metrics.get(k) for k in
                        ("distinct_managers", "distinct_pc_managers", "distinct_debtor_filings",
                         "link_rows_llm")}})

    if status != "success":
        raise RuntimeError(f"crosswalk build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_crosswalk() -> dict:
    """Read-back proof: open the published crosswalk from R2 and report row count, grain,
    method mix, and top managers — independent of the write path."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner(
        columns=["crd_number", "filing_id", "match_method", "pc_flag", "adviser_legal_name"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, mgrs, debs = con.execute(
        "SELECT count(*), count(DISTINCT crd_number), count(DISTINCT filing_id) FROM d2").fetchone()
    method_mix = dict(con.execute(
        "SELECT match_method, count(*) FROM d2 GROUP BY 1 ORDER BY 2 DESC").fetchall())
    top = con.execute(
        "SELECT adviser_legal_name, count(DISTINCT filing_id) FROM d2 WHERE pc_flag "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 15").fetchall()
    con.close()
    return {"rows": n, "distinct_managers": mgrs, "distinct_debtor_filings": debs,
            "method_mix": method_mix, "top_pc_managers": top, "indices": idx,
            "uri": DATASET_URI, "schema": [f"{f.name}:{f.type}" for f in ds.schema]}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.crosswalk_sec_adv_ucc_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.crosswalk_sec_adv_ucc_runs"}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    if dry_run:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            _table, metrics, gleif_pub = _materialize(con)
        finally:
            con.close()
        print(f"[dry-run] {metrics}")
        return
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("resolution-sec-adv-ucc-pipelines", "build_crosswalk",
                   deploy_file=__file__, trigger_callback_url=None)


@app.local_entrypoint()
def init() -> None:
    import json
    print(json.dumps(init_ops.remote(), indent=2, default=str))
