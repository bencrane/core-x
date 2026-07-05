"""jsearch_capture_roles_enriched — company-level resolution of the JSearch capture-roles feed.

SoR   s3://data-sink/active/jsearch_capture_roles_enriched/  (Lance v2.1; DERIVED, OVERWRITE snapshot)
Grain 1 row per company_key = coalesce(employer_domain, lower(employer_name))

Derived, recomputable projection that resolves each harvested capture-role employer against the
federal award/subaward canonical spines + PDL + internal firmographic LinkedIn sources. Answers, per
company: is it a federal prime and/or subawardee, how recently, and is it reachable on LinkedIn.

BLAST RADIUS: read-only over ingested internal Lance SoRs + a single OVERWRITE write to the derived
prefix. It NEVER mutates the append-only source feed `jsearch_capture_roles`. ALL inputs are internal
`s3://data-sink/active/` datasets — NO external/billed API calls (no Serper/blitz-live/Clay-live/PDL-API).

Sources (all s3://data-sink/active/, free R2 reads):
  jsearch_capture_roles           the feed (employer_name/domain, staffing/confidential flags)
  sam_master_domains              domain -> uei crosswalk (SAM-registered)
  usaspending_subaward_canonical  subawardee_uei/name + subaward_action_date        (1.3M, loaded)
  usaspending_award_canonical     recipient_uei/name + action_date                  (30.7M, streamed)
  pdl_normalized_companies        normalized_domain/company_name -> linkedin_slug   (35.4M, streamed)
  clay_find_companies / firmographics_blitz / company_addresses / companies   internal LinkedIn fallback

Resolution (§4.7 recall upgrades baked in):
  * de-artifacted name key (strip glued leading digits, drop len<2, filter staffing/jobboard/gov noise)
  * root-domain eTLD+1 (tldextract) with ATS-vendor stripping, matched ALONGSIDE the raw domain
  * company_linkedin_url = PDL linkedin_slug, else coalesced from the internal firmographic union

Data plane: Lance(sources) -> DuckDB (out-of-core; award/PDL streamed single-pass-filtered) -> Arrow ->
  lance.write_dataset(R2 active, v2.1, OVERWRITE) -> BTREE + BITMAP scalar indexes on the R2 dataset.
Ops ledger: one terminal-state row -> ops.jsearch_capture_roles_enriched_runs.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.1' \
      --with 'psycopg[binary]>=3.2' --with 'tldextract' \
      python3 pipelines/jsearch/build_capture_roles_enriched.py <build|verify>
"""
from __future__ import annotations

import datetime as dt
import os
import sys

FEED = "jsearch_capture_roles_enriched"
_ACTIVE = "s3://data-sink/active"
DATASET_URI = os.environ.get("JSEARCH_ENRICHED_URI", f"{_ACTIVE}/{FEED}/").rstrip("/") + "/"
DATA_STORAGE_VERSION = "2.1"
CUT24 = "2024-07-04"          # 24-month recency cutoff (today = 2026-07-04)

BTREE_INDEXES = ["company_key", "employer_domain", "root_domain", "pdl_company_id", "company_linkedin_url"]
BITMAP_INDEXES = ["is_prime", "is_subawardee", "federal_footprint", "active_24mo",
                  "match_path_prime", "match_path_sub", "prime_active_24mo", "sub_active_24mo",
                  "has_pdl", "pdl_match_path", "pdl_employee_size_range", "linkedin_source",
                  "has_linkedin", "is_staffing", "is_confidential"]

# ATS/job-board eTLD+1s whose registrable domain is the vendor, not the employer — unusable as a key.
ATS = {"icims.com", "applytojob.com", "smartrecruiters.com", "myworkdayjobs.com", "greenhouse.io",
       "lever.co", "workable.com", "bamboohr.com", "jazzhr.com", "ultipro.com", "adp.com",
       "paylocity.com", "ashbyhq.com", "recruitee.com", "teamtailor.com", "applicantpro.com",
       "paycomonline.net", "taleo.net", "brassring.com", "indeed.com", "ziprecruiter.com",
       "glassdoor.com", "linkedin.com", "paycom.com", "dayforcehcm.com"}

# ── normalizers (SQL macro strings; {c} = column expr) ──────────────────────────────────────────
def dn(c: str) -> str:
    return f"regexp_replace(lower(trim({c})), '^www\\.', '')"

def _lead(c: str) -> str:                         # strip glued leading digits: "100 salesforce inc" -> "salesforce inc"
    return f"regexp_replace(trim({c}), '^[0-9]+[ .,-]*', '')"

def nnd(c: str) -> str:                            # de-artifacted name key
    inner = _lead(c)
    return ("regexp_replace(regexp_replace(regexp_replace(lower(trim(" + inner + ")),'[^a-z0-9]+',' ','g'),"
            "'\\b(inc|incorporated|llc|l l c|llp|lp|plc|corp|corporation|co|company|ltd|limited|the|"
            "group|holdings|hldgs|intl|international|dba|career site|careers)\\b','','g'),' ','','g')")

# name-match noise guard (staffing / jobboard / gov / generic — never a real target account)
NOISE = ("(company_key ~ '(staffing|recruit|talent|adecco|randstad|manpower|kelly serv|aerotek|"
         "insight global|teksystems|robert half|kforce|apex systems|judge group|beacon hill|"
         "job ?list|jobsin|jobs\\.|\\bhires\\b|joblist|\\.gov|promoteproject|a medium corporation)')")


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    acct = os.environ.get("R2_ACCOUNT_ID")
    if not ep and acct:
        ep = f"https://{acct}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials).")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def _hqx_dsn() -> str | None:
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
        if not pooled:
            return None
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.jsearch_capture_roles_enriched_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    rows              integer     NOT NULL DEFAULT 0,
    federal_footprint integer     NOT NULL DEFAULT 0,
    is_prime          integer     NOT NULL DEFAULT 0,
    is_subawardee     integer     NOT NULL DEFAULT 0,
    active_24mo       integer     NOT NULL DEFAULT 0,
    has_pdl           integer     NOT NULL DEFAULT 0,
    has_linkedin      integer     NOT NULL DEFAULT 0,
    status            text        NOT NULL,
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jsearch_enriched_runs_status_chk CHECK (status IN ('success','error'))
);
CREATE INDEX IF NOT EXISTS jsearch_enriched_runs_recorded_idx
    ON ops.jsearch_capture_roles_enriched_runs (recorded_at DESC);
"""


def _record_run(counts: dict, status: str, error: str | None, started, completed) -> None:
    dsn = _hqx_dsn()
    if not dsn:
        log("WARN: no HQX DSN — skipping run-ledger write")
        return
    try:
        import psycopg
        with psycopg.connect(dsn, autocommit=True, prepare_threshold=None) as conn:
            cur = conn.cursor()
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.jsearch_capture_roles_enriched_runs
                     (feed, rows, federal_footprint, is_prime, is_subawardee, active_24mo,
                      has_pdl, has_linkedin, status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, counts.get("rows", 0), counts.get("federal_footprint", 0), counts.get("is_prime", 0),
                 counts.get("is_subawardee", 0), counts.get("active_24mo", 0), counts.get("has_pdl", 0),
                 counts.get("has_linkedin", 0), status, error, started, completed))
        log("run-ledger row written")
    except Exception as exc:  # noqa: BLE001 — ledger must never sink the build
        log(f"WARN: run-ledger write failed: {exc}")


def _reader(uri, cols, so, batch=131072):
    import lance
    return lance.dataset(uri, storage_options=so).scanner(columns=cols, batch_size=batch).to_reader()


def build() -> dict:
    import duckdb
    import lance
    import tldextract

    started = dt.datetime.now(dt.timezone.utc)
    built_at = started.strftime("%Y-%m-%d %H:%M:%S+00")
    so = _r2_so()
    ext = tldextract.TLDExtract(suffix_list_urls=())

    def root(d):
        if not d:
            return None
        rd = ext(d.strip().lower()).top_domain_under_public_suffix
        return None if (not rd or rd in ATS) else rd

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='12GB'; PRAGMA threads=4;")
    con.execute(f"PRAGMA temp_directory='{os.environ.get('SCRATCH', '/tmp/jx_enriched')}';")

    def load(name, cols, alias):
        con.register(alias, lance.dataset(f"{_ACTIVE}/{name}/", storage_options=so).to_table(columns=cols))

    # ── jsearch grain ────────────────────────────────────────────────────────────────────────
    load("jsearch_capture_roles",
         ["employer_name", "employer_domain", "employer_is_staffing", "employer_is_confidential"], "js_raw")
    con.execute(f"""CREATE TABLE js AS
    WITH b AS (SELECT nullif({dn('employer_domain')},'') dom, nullif(lower(trim(employer_name)),'') nml,
                      employer_name raw, nullif({nnd('employer_name')},'') nmn,
                      employer_is_staffing st, employer_is_confidential cf FROM js_raw)
    SELECT coalesce(dom,nml) company_key, arg_max(raw, length(raw)) employer_name, max(dom) employer_domain,
           max(nmn) nmn, bool_or(st) is_staffing, bool_or(cf) is_confidential, count(*) n_postings
    FROM b WHERE coalesce(dom,nml) IS NOT NULL GROUP BY 1""")
    n = con.execute("SELECT count(*) FROM js").fetchone()[0]
    log(f"jsearch grain: {n:,} companies")

    # root domains (python; small)
    con.execute("CREATE TABLE js_root(company_key VARCHAR, root_domain VARCHAR)")
    rows = con.execute("SELECT company_key, employer_domain FROM js WHERE employer_domain IS NOT NULL").fetchall()
    con.executemany("INSERT INTO js_root VALUES (?,?)", [(ck, root(d)) for ck, d in rows])

    # domain / name / uei match keys
    con.execute("""CREATE TABLE js_dom AS
        SELECT DISTINCT company_key, employer_domain d FROM js WHERE employer_domain IS NOT NULL
        UNION SELECT DISTINCT company_key, root_domain d FROM js_root WHERE root_domain IS NOT NULL""")
    con.execute(f"""CREATE TABLE js_nm AS SELECT DISTINCT company_key, nmn FROM js
        WHERE nmn IS NOT NULL AND length(nmn) >= 2 AND NOT is_staffing AND NOT is_confidential AND NOT {NOISE}""")
    load("sam_master_domains", ["normalized_domain", "uei"], "sam_raw")
    con.execute(f"CREATE TABLE sam AS SELECT DISTINCT {dn('normalized_domain')} d, uei FROM sam_raw WHERE normalized_domain IS NOT NULL AND uei IS NOT NULL")
    con.execute("CREATE TABLE js_uei AS SELECT DISTINCT j.company_key, s.uei FROM js_dom j JOIN sam s ON j.d=s.d")
    con.execute("CREATE TABLE resolved AS SELECT company_key, list(DISTINCT uei) resolved_uei, count(DISTINCT uei) n_resolved_uei FROM js_uei GROUP BY 1")

    # ── subaward (loaded) ────────────────────────────────────────────────────────────────────
    load("usaspending_subaward_canonical", ["subawardee_uei", "subawardee_name", "subaward_action_date"], "sub_raw")
    con.execute(f"""CREATE TABLE sub_matched AS
        SELECT subawardee_uei uei, {nnd('subawardee_name')} nmn, try_cast(subaward_action_date AS DATE) d FROM sub_raw""")
    con.execute("CREATE TABLE sub_u AS SELECT u.company_key, max(m.d) d FROM sub_matched m JOIN js_uei u ON m.uei=u.uei GROUP BY 1")
    con.execute("CREATE TABLE sub_n AS SELECT x.company_key, max(m.d) d FROM sub_matched m JOIN js_nm x ON m.nmn=x.nmn GROUP BY 1")

    # ── prime (streamed, single-pass filtered) ───────────────────────────────────────────────
    con.register("aw", _reader(f"{_ACTIVE}/usaspending_award_canonical/", ["recipient_uei", "recipient_name", "action_date"], so))
    con.execute(f"""CREATE TABLE prime_matched AS
        SELECT recipient_uei uei, {nnd('recipient_name')} nmn, try_cast(action_date AS DATE) d FROM aw
        WHERE recipient_uei IN (SELECT uei FROM js_uei) OR {nnd('recipient_name')} IN (SELECT nmn FROM js_nm)""")
    con.unregister("aw")
    con.execute("CREATE TABLE prime_u AS SELECT u.company_key, max(m.d) d FROM prime_matched m JOIN js_uei u ON m.uei=u.uei GROUP BY 1")
    con.execute("CREATE TABLE prime_n AS SELECT x.company_key, max(m.d) d FROM prime_matched m JOIN js_nm x ON m.nmn=x.nmn GROUP BY 1")
    log("prime + subaward resolved")

    # ── PDL (streamed, single-pass filtered) ─────────────────────────────────────────────────
    con.register("pdl", _reader(f"{_ACTIVE}/pdl_normalized_companies/",
                 ["normalized_domain", "company_name", "linkedin_slug", "pdl_company_id", "employee_size_range", "is_generic_domain"], so))
    con.execute(f"""CREATE TABLE pdl_matched AS
        SELECT {dn('normalized_domain')} d, {nnd('company_name')} nmn, pdl_company_id, linkedin_slug slug,
               employee_size_range esize, coalesce(is_generic_domain,false) gen FROM pdl
        WHERE ({dn('normalized_domain')} IN (SELECT d FROM js_dom) AND coalesce(is_generic_domain,false)=false)
           OR ({nnd('company_name')} IN (SELECT nmn FROM js_nm))""")
    con.unregister("pdl")
    con.execute("""CREATE TABLE pdl_d AS
        SELECT j.company_key, arg_min(m.pdl_company_id, m.pdl_company_id) pdl_company_id,
               arg_min(m.slug, m.pdl_company_id) slug, arg_min(m.esize, m.pdl_company_id) esize
        FROM js_dom j JOIN pdl_matched m ON j.d=m.d WHERE m.gen=false GROUP BY 1""")
    con.execute("""CREATE TABLE pdl_n AS
        SELECT x.company_key, arg_min(m.pdl_company_id, m.pdl_company_id) pdl_company_id,
               arg_min(m.slug, m.pdl_company_id) slug, arg_min(m.esize, m.pdl_company_id) esize
        FROM js_nm x JOIN pdl_matched m ON x.nmn=m.nmn GROUP BY 1""")
    log("PDL resolved")

    # ── internal LinkedIn fallback union (priority: clay < blitz < company_addresses < companies) ──
    li_src = [("clay_find_companies", "domain_norm", "linkedin_url", 1, "clay"),
              ("firmographics_blitz", "domain_norm", "linkedin_url", 2, "blitz"),
              ("company_addresses", "domain_norm", "company_linkedin_url", 3, "company_addresses"),
              ("companies", "normalized_domain", "company_linkedin_url", 4, "companies")]
    for name, dcol, lcol, pr, tag in li_src:
        load(name, [dcol, lcol], f"lisrc_{tag}")
        con.execute(f"CREATE TABLE li_{tag} AS SELECT DISTINCT {dn(dcol)} d, nullif(trim({lcol}),'') li, {pr} pr, '{tag}' src FROM lisrc_{tag} WHERE {dcol} IS NOT NULL AND nullif(trim({lcol}),'') IS NOT NULL")
    con.execute("CREATE TABLE li_union AS " + " UNION ALL ".join(f"SELECT d,li,pr,src FROM li_{t[4]}" for t in li_src))
    con.execute("""CREATE TABLE li_fallback AS
        SELECT company_key, arg_min(li, pr) li, arg_min(src, pr) src FROM (
          SELECT j.company_key, u.li, u.pr, u.src FROM js_dom j JOIN li_union u ON j.d=u.d) GROUP BY 1""")

    # ── assemble ─────────────────────────────────────────────────────────────────────────────
    con.execute(f"""CREATE TABLE m AS
    WITH base AS (
      SELECT j.company_key, j.employer_name, j.employer_domain, jr.root_domain, j.n_postings,
             j.is_staffing, j.is_confidential,
             coalesce(r.resolved_uei, []::VARCHAR[]) resolved_uei, coalesce(r.n_resolved_uei,0) n_resolved_uei,
             (pu.company_key IS NOT NULL OR pn.company_key IS NOT NULL) is_prime,
             CASE WHEN pu.company_key IS NOT NULL AND pn.company_key IS NOT NULL THEN 'both'
                  WHEN pu.company_key IS NOT NULL THEN 'domain' WHEN pn.company_key IS NOT NULL THEN 'name'
                  ELSE 'none' END match_path_prime,
             greatest(pu.d, pn.d) last_prime_action_date,
             (su.company_key IS NOT NULL OR sn.company_key IS NOT NULL) is_subawardee,
             CASE WHEN su.company_key IS NOT NULL AND sn.company_key IS NOT NULL THEN 'both'
                  WHEN su.company_key IS NOT NULL THEN 'domain' WHEN sn.company_key IS NOT NULL THEN 'name'
                  ELSE 'none' END match_path_sub,
             greatest(su.d, sn.d) last_subaward_action_date,
             (pd.company_key IS NOT NULL OR pnm.company_key IS NOT NULL) has_pdl,
             CASE WHEN pd.company_key IS NOT NULL AND pnm.company_key IS NOT NULL THEN 'both'
                  WHEN pd.company_key IS NOT NULL THEN 'domain' WHEN pnm.company_key IS NOT NULL THEN 'name'
                  ELSE 'none' END pdl_match_path,
             coalesce(pd.pdl_company_id, pnm.pdl_company_id) pdl_company_id,
             coalesce(pd.slug, pnm.slug) pdl_slug,
             coalesce(pd.esize, pnm.esize) pdl_employee_size_range,
             lf.li li_internal, lf.src li_internal_src
      FROM js j
      LEFT JOIN js_root jr USING(company_key)
      LEFT JOIN resolved r USING(company_key)
      LEFT JOIN prime_u pu USING(company_key)  LEFT JOIN prime_n pn USING(company_key)
      LEFT JOIN sub_u su USING(company_key)    LEFT JOIN sub_n sn USING(company_key)
      LEFT JOIN pdl_d pd USING(company_key)    LEFT JOIN pdl_n pnm USING(company_key)
      LEFT JOIN li_fallback lf USING(company_key)
    )
    SELECT company_key, employer_name, employer_domain, root_domain, n_postings,
           is_staffing, is_confidential, resolved_uei, n_resolved_uei,
           is_prime, match_path_prime, last_prime_action_date,
           coalesce(last_prime_action_date >= DATE '{CUT24}', false) prime_active_24mo,
           is_subawardee, match_path_sub, last_subaward_action_date,
           coalesce(last_subaward_action_date >= DATE '{CUT24}', false) sub_active_24mo,
           (is_prime OR is_subawardee) federal_footprint,
           (coalesce(last_prime_action_date >= DATE '{CUT24}',false) OR coalesce(last_subaward_action_date >= DATE '{CUT24}',false)) active_24mo,
           has_pdl, pdl_match_path, pdl_company_id, pdl_employee_size_range,
           coalesce(CASE WHEN pdl_slug IS NOT NULL THEN 'https://www.linkedin.com/company/'||pdl_slug END, li_internal) company_linkedin_url,
           CASE WHEN pdl_slug IS NOT NULL THEN 'pdl' WHEN li_internal IS NOT NULL THEN li_internal_src ELSE NULL END linkedin_source,
           (coalesce(CASE WHEN pdl_slug IS NOT NULL THEN 'x' END, li_internal) IS NOT NULL) has_linkedin,
           TIMESTAMPTZ '{built_at}' built_at
    FROM base""")

    def cnt(pred=None):
        q = "SELECT count(*) FROM m" if not pred else f"SELECT count(*) FILTER (WHERE {pred}) FROM m"
        return con.execute(q).fetchone()[0]
    counts = {"rows": cnt(), "federal_footprint": cnt("federal_footprint"), "is_prime": cnt("is_prime"),
              "is_subawardee": cnt("is_subawardee"), "active_24mo": cnt("active_24mo"),
              "has_pdl": cnt("has_pdl"), "has_linkedin": cnt("has_linkedin")}
    log(f"assembled: rows={counts['rows']:,} federal={counts['federal_footprint']:,} prime={counts['is_prime']:,} "
        f"sub={counts['is_subawardee']:,} active24={counts['active_24mo']:,} pdl={counts['has_pdl']:,} "
        f"linkedin={counts['has_linkedin']:,} ({100*counts['has_linkedin']/counts['rows']:.1f}%)")

    # ── write Lance + index ──────────────────────────────────────────────────────────────────
    tbl = con.execute("SELECT * FROM m ORDER BY company_key").arrow()
    lance.write_dataset(tbl, DATASET_URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        storage_options=so)
    ds = lance.dataset(DATASET_URI, storage_options=so)
    assert ds.count_rows() == counts["rows"], f"write-integrity {ds.count_rows()} != {counts['rows']}"
    names = set(ds.schema.names)
    for col in BTREE_INDEXES:
        if col in names:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
    for col in BITMAP_INDEXES:
        if col in names:
            try:
                ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            except Exception as exc:  # noqa: BLE001
                log(f"WARN: BITMAP index {col} skipped: {exc}")
    log(f"WROTE {DATASET_URI} rows={ds.count_rows():,} + {len(BTREE_INDEXES)} BTREE / {len(BITMAP_INDEXES)} BITMAP")
    _record_run(counts, "success", None, started, dt.datetime.now(dt.timezone.utc))
    return counts


def verify() -> dict:
    import json
    import lance
    so = _r2_so()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices())
    out = {"uri": DATASET_URI, "rows": ds.count_rows(), "cols": len(ds.schema),
           "schema": [f.name for f in ds.schema], "indices": idx}
    print(json.dumps(out, indent=2, default=str))
    return out


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        import json
        started = dt.datetime.now(dt.timezone.utc)
        try:
            print(json.dumps(build(), indent=2, default=str))
        except Exception as exc:  # noqa: BLE001
            _record_run({}, "error", f"{type(exc).__name__}: {exc}", started, dt.datetime.now(dt.timezone.utc))
            raise
    elif cmd == "verify":
        verify()
    else:
        print("usage: build_capture_roles_enriched.py <build|verify>"); sys.exit(2)
