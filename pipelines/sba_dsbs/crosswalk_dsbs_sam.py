"""Compute worker — canonical DSBS ↔ SAM.gov UEI crosswalk (1 row / uei).

Part of the ``crosswalk-dsbs-sam`` Modal app. Endpoint-less; dispatcher-resolvable or run manually.
Clean-room data plane: DuckDB does 100% of the read/cast/join, Lance is written straight to R2.
No catalog round-trip. Net-new create — NO existing SoR is mutated.

Plan of record: docs/plans/DSBS_SAM_UEI_BRIDGE_PLAN.md.

WHY THIS EXISTS
    DSBS (sba_dsbs_certified_firms) and SAM (sam_master_entities) are both UEI-native and both
    1-row-per-uei. Every consumer that wants "the SAM entity_url / domain / POC / PDL id for a DSBS
    cohort" was hand-assembling a 5-dataset join every time (and silently fanning out on the
    bridge_sam_pdl multi-row grain). This lands that join ONCE, behind a single BTREE(uei) lookup.

GRAIN. Exactly 1 row per DSBS uei (67,234), left-anchored on DSBS so the dataset IS the DSBS
    certified universe; SAM columns are nullable enrichment. POCs stay in sam_pocs (multi/uei) and
    are surfaced here only as a bounded presence rollup (has_sam_poc / count / primary name).
    bridge_sam_pdl is DISTINCT-collapsed to 1/uei before it touches the spine.

best_domain (identity-first) vs matched_domain (linkedin-max) — TWO INDEPENDENT facts per firm:
    Four source-domains per firm, each core.web_norm-normalized and dropped if generic under the
    canonical (extended) blocklist: entity_url (via sam_master_domains), website, email-suffix,
    additional_website.
      best_domain    = first PRESENT in fixed order [entity_url, website, email, additional_website];
                       the firm's #1 identity domain.  (+ best_domain_source, _source_count, _in_pdl)
      matched_domain = first in that same order that RESOLVES in PDL (has a linkedin_slug); drives
                       company_linkedin_url.  (+ matched_domain_source, _pdl_company_id, _name_consistent)
    matched can be a lower-ranked source than best when best_domain isn't in PDL — that is by design;
    both are recorded, neither gates. Filter downstream on *_source / *_name_consistent as needed.

TARGET (Gen-3 system of record — native Lance v2.1):
    s3://data-sink/active/crosswalk_dsbs_sam/

REFRESH. ``run`` full overwrite (deterministic over stable Lance snapshots). Rebuild on the
    downstream of any upstream refresh; a single weekly control-plane task is sufficient.

    modal run    pipelines/sba_dsbs/crosswalk_dsbs_sam.py::init_ops
    modal run    pipelines/sba_dsbs/crosswalk_dsbs_sam.py::verify_only
    modal deploy pipelines/sba_dsbs/crosswalk_dsbs_sam.py
    # durable launch — ::run is spawn-only: deploys if stale, spawns `ingest` on the
    # DEPLOYED app, prints the fc-id, and returns immediately (it no longer blocks on
    # or prints the result; the worker's ledger row and app logs are the record):
    modal run    pipelines/sba_dsbs/crosswalk_dsbs_sam.py::run
    # follow:  modal app logs crosswalk-dsbs-sam
    # result:  python3 -c "import modal; print(modal.FunctionCall.from_id('fc-...').get(timeout=0))"
    # terminal truth: latest row in ops.crosswalk_dsbs_sam_runs
    # local materialize (no Modal): doppler run --project core-x --config prd -- \
    #   uv run --with pylance --with duckdb --with pyarrow --with 'psycopg[binary]' --with modal \
    #   python pipelines/sba_dsbs/crosswalk_dsbs_sam.py
"""
from __future__ import annotations

import os
import sys

import modal

_ACTIVE = "s3://data-sink/active"
DATASET = "crosswalk_dsbs_sam"
DATASET_URI = os.environ.get("CROSSWALK_DSBS_SAM_URI", f"{_ACTIVE}/{DATASET}/")

# Read-only sources (NEVER written by this worker).
DSBS_URI = f"{_ACTIVE}/sba_dsbs_certified_firms/"
SAM_ENT_URI = f"{_ACTIVE}/sam_master_entities/"
SAM_DOM_URI = f"{_ACTIVE}/sam_master_domains/"
SAM_POC_URI = f"{_ACTIVE}/sam_pocs/"
BRIDGE_PDL_URI = f"{_ACTIVE}/bridge_sam_pdl/"
PDL_NORM_URI = f"{_ACTIVE}/pdl_normalized_companies/"

FEED = "crosswalk_dsbs_sam"
SOURCE_DB = "r2:sba_dsbs_certified_firms+sam_master_*+bridge_sam_pdl->pdl_normalized_companies"
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
PDL_CHUNK = 8000  # domains per indexed IN-list probe against pdl_normalized_companies

INDEXES: dict[str, list[str]] = {
    "BTREE": ["uei", "cage_code", "primary_naics", "best_domain", "matched_domain",
              "company_linkedin_url", "matched_pdl_company_id", "pdl_company_id"],
    "BITMAP": ["in_sam", "is_active", "exclusion_status_flag", "cert_programs",
               "best_domain_source", "matched_domain_source", "best_domain_in_pdl",
               "matched_name_consistent", "has_sam_poc",
               "active_8a", "active_hz", "active_wosb", "active_edwosb", "active_sdvosb", "active_vosb"],
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.crosswalk_dsbs_sam_runs (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed         text        NOT NULL,
    source_db    text        NOT NULL,
    datasets     jsonb       NOT NULL,
    mode         text        NOT NULL DEFAULT 'overwrite',
    rows_total   bigint      NOT NULL DEFAULT 0,
    rows_matched bigint      NOT NULL DEFAULT 0,
    status       text        NOT NULL,
    error        text,
    started_at   timestamptz,
    completed_at timestamptz,
    recorded_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS crosswalk_dsbs_sam_runs_feed_idx        ON ops.crosswalk_dsbs_sam_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_dsbs_sam_runs_recorded_at_idx ON ops.crosswalk_dsbs_sam_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "psycopg[binary]>=3.2", "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core")

app = modal.App("crosswalk-dsbs-sam", image=image)


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials).")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


# ── the whole transform (pure; heavy imports inside so the module stays import-light) ──
def build_crosswalk() -> dict:
    import datetime as dt
    import re
    import time

    import duckdb
    import lance
    import pyarrow as pa

    # repo root on path so `core.web_norm` resolves both locally and in the Modal image.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from core.web_norm import _bare_host as BH, is_generic_domain as GD, normalized_domain as ND

    so = _r2_storage_options()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4; SET memory_limit='12GB';")

    def reg(name: str, uri: str, cols: list[str]) -> None:
        con.register(name, lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader())

    reg("dsbs", DSBS_URI, ["uei", "legal_business_name", "website", "additional_website", "email",
                           "phone", "contact_person", "cage_code", "naics_primary", "cert_programs",
                           "active_8a_boolean", "active_hz_boolean", "active_wosb_boolean",
                           "active_edwosb_boolean", "active_sdvosb_boolean", "active_vosb_boolean"])
    reg("sme", SAM_ENT_URI, ["uei", "legal_business_name", "entity_url", "cage_code", "primary_naics",
                             "is_active", "exclusion_status_flag", "registration_expiration_date",
                             "entity_structure", "sba_business_types_string"])
    reg("smd", SAM_DOM_URI, ["uei", "normalized_domain"])
    reg("pocs", SAM_POC_URI, ["uei", "poc_type", "full_name"])
    reg("bridge", BRIDGE_PDL_URI, ["uei", "pdl_company_id", "registration_status"])

    def dom(expr: str) -> str:
        return f"CASE WHEN ({ND(BH(expr))} IS NOT NULL AND ({GD(ND(BH(expr)))})=false) THEN {ND(BH(expr))} END"

    con.execute("CREATE TEMP TABLE _sam AS SELECT uei, min(legal_business_name) sam_name, "
                "min(entity_url) entity_url, min(cage_code) sam_cage, min(primary_naics) sam_naics, "
                "bool_or(is_active) is_active, min(exclusion_status_flag) exclusion_status_flag, "
                "min(registration_expiration_date) registration_expiration_date, "
                "min(entity_structure) entity_structure, min(sba_business_types_string) sba_business_types_string "
                "FROM sme GROUP BY uei")
    con.execute("CREATE TEMP TABLE _dom AS SELECT uei, min(lower(normalized_domain)) sd "
                "FROM smd WHERE normalized_domain IS NOT NULL GROUP BY uei")
    con.execute("CREATE TEMP TABLE _poc AS SELECT uei, count(*) poc_count, "
                "coalesce(max(full_name) FILTER (WHERE lower(poc_type) LIKE '%government%'), max(full_name)) primary_poc "
                "FROM pocs GROUP BY uei")
    con.execute("CREATE TEMP TABLE _bridge AS SELECT uei, min(pdl_company_id) pdl_company_id "
                "FROM bridge WHERE pdl_company_id IS NOT NULL GROUP BY uei")

    con.execute(f"""
        CREATE TEMP TABLE X AS
        SELECT
          d.uei,
          d.legal_business_name                                   AS dsbs_legal_business_name,
          s.sam_name                                              AS sam_legal_business_name,
          (s.uei IS NOT NULL)                                     AS in_sam,
          coalesce(d.cage_code, s.sam_cage)                       AS cage_code,
          coalesce(s.sam_naics, d.naics_primary)                  AS primary_naics,
          s.is_active, s.exclusion_status_flag, s.registration_expiration_date,
          s.entity_structure, s.sba_business_types_string,
          s.entity_url,
          d.website AS dsbs_website, d.additional_website AS dsbs_additional_website,
          d.email AS dsbs_email, d.phone AS dsbs_phone, d.contact_person AS dsbs_contact_person,
          CASE WHEN dd.sd IS NOT NULL AND ({GD('dd.sd')})=false THEN dd.sd END AS domain_entity_url,
          {dom('d.website')}            AS domain_website,
          {dom("split_part(d.email,'@',2)")} AS domain_email,
          {dom('d.additional_website')} AS domain_additional,
          b.pdl_company_id,
          d.cert_programs,
          coalesce(d.active_8a_boolean,false)  AS active_8a,  coalesce(d.active_hz_boolean,false)     AS active_hz,
          coalesce(d.active_wosb_boolean,false) AS active_wosb, coalesce(d.active_edwosb_boolean,false) AS active_edwosb,
          coalesce(d.active_sdvosb_boolean,false) AS active_sdvosb, coalesce(d.active_vosb_boolean,false) AS active_vosb,
          (p.uei IS NOT NULL)                                     AS has_sam_poc,
          coalesce(p.poc_count, 0)                                AS sam_poc_count,
          p.primary_poc                                           AS sam_primary_poc_name
        FROM dsbs d
        LEFT JOIN _sam s   ON s.uei=d.uei
        LEFT JOIN _dom dd  ON dd.uei=d.uei
        LEFT JOIN _poc p   ON p.uei=d.uei
        LEFT JOIN _bridge b ON b.uei=d.uei
    """)

    # per-firm source-domains for identity/linkedin resolution
    firms = con.execute(
        "SELECT uei, dsbs_legal_business_name, domain_entity_url, domain_website, domain_email, domain_additional FROM X"
    ).fetchall()

    cand = set()
    for _, _, de, dw, dm, da in firms:
        for x in (de, dw, dm, da):
            if x:
                cand.add(x)
    cand = list(cand)

    pdl: dict[str, tuple] = {}
    pnc = lance.dataset(PDL_NORM_URI, storage_options=so)
    esc = lambda x: x.replace("'", "''")  # noqa: E731
    t0 = time.time()
    for i in range(0, len(cand), PDL_CHUNK):
        ch = cand[i:i + PDL_CHUNK]
        flt = "normalized_domain IN (" + ",".join("'%s'" % esc(x) for x in ch) + ")"
        t = pnc.scanner(columns=["normalized_domain", "linkedin_slug", "company_name", "pdl_company_id"],
                        filter=flt).to_table()
        for nd, sl, cn, pid in zip(t.column("normalized_domain").to_pylist(),
                                   t.column("linkedin_slug").to_pylist(),
                                   t.column("company_name").to_pylist(),
                                   t.column("pdl_company_id").to_pylist()):
            k = (nd or "").lower()
            if k and sl and sl.strip() and k not in pdl:
                pdl[k] = (sl, cn, pid)
    print(f"  pdl probe: {len(cand):,} domains, {len(pdl):,} resolved, {time.time()-t0:.0f}s", flush=True)

    SRC = ["entity_url", "website", "email", "additional_website"]
    STOP = {"llc", "inc", "incorporated", "corp", "corporation", "co", "company", "ltd", "limited",
            "the", "and", "of", "group", "services", "service", "solutions", "enterprises", "pllc",
            "lp", "llp", "holdings", "associates", "a", "for"}

    def toks(s):
        return {t for t in re.split(r"[^a-z0-9]+", (s or "").lower()) if len(t) >= 4 and t not in STOP}

    R = {k: [] for k in ["uei", "best_domain", "best_domain_source", "best_domain_source_count",
                         "best_domain_in_pdl", "matched_domain", "matched_domain_source",
                         "company_linkedin_url", "matched_pdl_company_id", "matched_name_consistent"]}
    for uei, nm, de, dw, dm, da in firms:
        doms = [de, dw, dm, da]
        if not any(doms):
            continue
        bi = next(i for i, x in enumerate(doms) if x)
        best = doms[bi]
        mi = next((i for i, x in enumerate(doms) if x and x in pdl), None)
        md = doms[mi] if mi is not None else None
        p = pdl.get(md) if md else None
        sl, cn, pid = (p if p else (None, None, None))
        R["uei"].append(uei)
        R["best_domain"].append(best)
        R["best_domain_source"].append(SRC[bi])
        R["best_domain_source_count"].append(sum(1 for x in doms if x == best))
        R["best_domain_in_pdl"].append(best in pdl)
        R["matched_domain"].append(md)
        R["matched_domain_source"].append(SRC[mi] if mi is not None else None)
        R["company_linkedin_url"].append(("https://www.linkedin.com/company/" + sl) if sl else None)
        R["matched_pdl_company_id"].append(pid)
        R["matched_name_consistent"].append((len(toks(nm) & toks(cn)) >= 1) if cn else False)

    con.register("resolved", pa.table(R))
    mat = dt.datetime.now(dt.UTC)
    out = con.execute("""
        SELECT X.uei, X.dsbs_legal_business_name, X.sam_legal_business_name, X.in_sam, X.cage_code,
               X.primary_naics, X.is_active, X.exclusion_status_flag, X.registration_expiration_date,
               X.entity_structure, X.sba_business_types_string, X.entity_url,
               X.dsbs_website, X.dsbs_additional_website, X.dsbs_email, X.dsbs_phone, X.dsbs_contact_person,
               X.domain_entity_url, X.domain_website, X.domain_email, X.domain_additional,
               r.best_domain, r.best_domain_source, r.best_domain_source_count, r.best_domain_in_pdl,
               r.matched_domain, r.matched_domain_source, r.company_linkedin_url,
               r.matched_pdl_company_id, r.matched_name_consistent,
               X.pdl_company_id, X.cert_programs,
               X.active_8a, X.active_hz, X.active_wosb, X.active_edwosb, X.active_sdvosb, X.active_vosb,
               X.has_sam_poc, X.sam_poc_count, X.sam_primary_poc_name
        FROM X LEFT JOIN resolved r ON r.uei = X.uei
    """).to_arrow_table()
    out = out.append_column("materialized_at",
                            pa.array([mat] * out.num_rows, pa.timestamp("us", tz="UTC")))

    lance.write_dataset(out, DATASET_URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    assert n == out.num_rows, f"write-integrity: {n} != {out.num_rows}"
    names = set(ds.schema.names)
    for typ, cols in INDEXES.items():
        for col in cols:
            if col not in names:
                continue
            try:
                ds.create_scalar_index(col, index_type=typ)
            except Exception as exc:  # noqa: BLE001
                print(f"  idx skip {typ} {col}: {exc}", flush=True)
    distinct = con.execute("SELECT count(DISTINCT uei) FROM X").fetchone()[0]
    assert distinct == n, f"grain gate: distinct uei {distinct} != rows {n}"
    matched = con.execute("SELECT count(*) FROM resolved WHERE company_linkedin_url IS NOT NULL").fetchone()[0]
    print(f"WROTE {DATASET_URI}  rows={n:,}  distinct_uei={distinct:,}  best_domain_firms={len(R['uei']):,}  "
          f"matched_linkedin={matched:,}  cols={len(ds.schema)}", flush=True)
    return {"uri": DATASET_URI, "rows": n, "best_domain_firms": len(R["uei"]), "matched": matched}


def _record_run(rows_total: int, rows_matched: int, status: str, error, started, completed) -> None:
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED unset; skipping ops.* ledger.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "INSERT INTO ops.crosswalk_dsbs_sam_runs (feed,source_db,datasets,mode,rows_total,"
                "rows_matched,status,error,started_at,completed_at) VALUES (%s,%s,%s,'overwrite',%s,%s,%s,%s,%s,%s)",
                (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), rows_total, rows_matched, status,
                 error, started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* ledger write failed: {exc}")


_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


@app.function(secrets=_SECRETS, timeout=60 * 45, memory=32768, cpu=4.0)
def ingest() -> dict:
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_crosswalk()
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(res.get("rows", 0), res.get("matched", 0), status, error, started, completed)
    return res


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192)
def verify() -> dict:
    import lance
    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                 for i in ds.list_indices())
    out = {"uri": DATASET_URI, "rows": n, "cols": len(ds.schema), "indices": idx,
           "schema": [f.name for f in ds.schema]}
    print(out)
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    import psycopg
    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    return {"table": "ops.crosswalk_dsbs_sam_runs"}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run() -> None:
    from pipelines._shared.launch import spawn_deployed
    spawn_deployed("crosswalk-dsbs-sam", "ingest", deploy_file=__file__)


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))


if __name__ == "__main__":
    # Local materialization path (no Modal remote): runs the same transform against R2 using the
    # env-resolved credentials (doppler). Records the ops ledger if HQX_DB_URL_POOLED is present.
    import datetime as dt

    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_crosswalk()
        status = "success"
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(res.get("rows", 0), res.get("matched", 0), status, error, started, completed)
    print(res)
