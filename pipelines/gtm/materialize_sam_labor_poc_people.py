"""Compute worker — SAM labor-universe POCs resolved to real people (enrichment-ready) + the residual worklist.

Part of the ``sam-labor-poc-people`` Modal app. Endpoint-less; dispatcher-resolvable or run manually.
Clean-room: DuckDB + Python read/match, Lance written straight to R2. No catalog.

WHY THIS EXISTS
    The SAM labor universe (active/sam_labor_universe ∪ active/sam_facilities_labor_universe) has POCs in
    two places: sam_pocs (government/electronic-business + past-performance slots — the entity's OWN
    people, incl. President/CEO/Owner) and dsbs_pocs (DSBS owners/officers/contacts). This resolves each
    distinct (uei, person) POC to a REAL person-record in clay_find_people / active/people that works at
    that same firm (company domain → uei, name-matched), attaching the enrichment key
    (person_linkedin_url) + current work-email / mobile coverage. What does NOT resolve becomes the
    find-people worklist.

MATCH. A POC (uei, name_key) resolves iff a clay/people person works at that firm — their company domain
    ∈ the firm's domain universe (sam_master_domains ∪ crosswalk_dsbs_sam best_domain/email-suffix/etc.
    ∪ the labor-universe self domains) → uei — AND their name normalizes to the same name_key
    (order-independent: lowercased alpha tokens >=2, suffixes dropped, sorted). name_key is RECOMPUTED
    from full_name on BOTH sides; the stored sam_pocs.name_key is the uppercased full name (a different
    normalization) and is NOT trusted.

SOURCES (Lance, R2): sam_labor_universe + sam_facilities_labor_universe (the firms), sam_master_domains +
    crosswalk_dsbs_sam (uei → domain universe), sam_pocs + dsbs_pocs (the POCs), clay_find_people +
    people (the people), work_emails + phone_resolutions (current coverage).
TARGETS:
    s3://data-sink/active/sam_labor_poc_people/      resolved matches — 1 row per (uei × person_id)
    s3://data-sink/active/sam_labor_poc_unresolved/  residual worklist — 1 row per (uei × name_key)
    (both native Lance v2.1; overwrite — deterministic rebuild of a derived product)

INDEXES: people      — BTREE uei/person_id/person_linkedin_url ; BITMAP poc_source/poc_type/match_source/universe/is_principal/has_work_email/has_mobile
         unresolved  — BTREE uei ; BITMAP poc_source/poc_type/universe/is_principal

    modal run    pipelines/gtm/materialize_sam_labor_poc_people.py::run
    modal deploy pipelines/gtm/materialize_sam_labor_poc_people.py
    # local: doppler run -p core-x -c prd -- uv run --no-project --with pylance --with duckdb --with pyarrow \
    #        --with 'psycopg[binary]' --with modal python pipelines/gtm/materialize_sam_labor_poc_people.py
"""
from __future__ import annotations

import os
import re
import unicodedata

import modal

_ACTIVE = "s3://data-sink/active"
DS_PEOPLE = "sam_labor_poc_people"
DS_UNRES = "sam_labor_poc_unresolved"
PEOPLE_URI = os.environ.get("SAM_LABOR_POC_PEOPLE_URI", f"{_ACTIVE}/{DS_PEOPLE}/")
UNRES_URI = os.environ.get("SAM_LABOR_POC_UNRESOLVED_URI", f"{_ACTIVE}/{DS_UNRES}/")

LABOR_URI = f"{_ACTIVE}/sam_labor_universe/"
FACIL_URI = f"{_ACTIVE}/sam_facilities_labor_universe/"
SMD_URI = f"{_ACTIVE}/sam_master_domains/"
XWALK_URI = f"{_ACTIVE}/crosswalk_dsbs_sam/"
SAM_POCS_URI = f"{_ACTIVE}/sam_pocs/"
DSBS_POCS_URI = f"{_ACTIVE}/dsbs_pocs/"
CLAY_URI = f"{_ACTIVE}/clay_find_people/"
SPINE_URI = f"{_ACTIVE}/people/"
WORK_EMAILS_URI = f"{_ACTIVE}/work_emails/"
PHONE_URI = f"{_ACTIVE}/phone_resolutions/"

FEED = "sam_labor_poc_people"
SOURCE_DB = "r2:(sam_pocs+dsbs_pocs)⋈(clay_find_people+people)@[sam_master_domains+crosswalk_dsbs_sam]+work_emails+phone_resolutions"

INDEXES_PEOPLE = {"BTREE": ["uei", "person_id", "person_linkedin_url"],
                  "BITMAP": ["poc_source", "poc_type", "match_source", "universe",
                             "is_principal", "has_work_email", "has_mobile"]}
INDEXES_UNRES = {"BTREE": ["uei"],
                 "BITMAP": ["poc_source", "poc_type", "universe", "is_principal"]}

# Free / generic email providers — a firm listing one of these as its "domain" is not a real corporate
# domain; joining clay people on it would match strangers. Excluded from the domain universe.
GENERIC_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "rocketmail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com", "mac.com",
    "comcast.net", "verizon.net", "att.net", "sbcglobal.net", "bellsouth.net", "cox.net",
    "charter.net", "earthlink.net", "roadrunner.com", "frontier.com", "windstream.net",
    "protonmail.com", "proton.me", "mail.com", "gmx.com", "gmx.us", "yandex.com", "zoho.com",
    "juno.com", "netzero.net", "ptd.net", "embarqmail.com", "peoplepc.com", "aim.com",
}

# name_key match relies on this being IDENTICAL across POC side and person side.
_SUF = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "cpa", "esq", "pe", "mba", "jd", "dds"}

# Owner / decision-maker POC slots. past_performance is the entity's OWN designated contact (skews
# President/CEO/Owner); government_business is the primary contract decision POC; current_principal is
# the DSBS-declared owner/officer.
_PRINCIPAL_SLOTS = {"current_principal", "government_business", "past_performance"}
# Priority for the single rolled-up poc_type label per (uei, person).
_POC_PRIORITY = ["current_principal", "government_business", "past_performance", "electronic_business",
                 "government_business_alt", "electronic_business_alt", "past_performance_alt",
                 "contact_person"]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_labor_poc_people_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, feed text NOT NULL, source_db text NOT NULL,
    datasets jsonb NOT NULL, mode text NOT NULL DEFAULT 'overwrite', rows_total bigint NOT NULL DEFAULT 0,
    status text NOT NULL, error text, started_at timestamptz, completed_at timestamptz,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sam_labor_poc_people_runs_feed_idx ON ops.sam_labor_poc_people_runs (feed);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("sam-labor-poc-people", image=image)


def _name_key(n):
    if not n:
        return None
    s = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode().lower()
    t = sorted(x for x in re.split(r"[^a-z]+", s) if len(x) >= 2 and x not in _SUF)
    return " ".join(t) or None


def _r2_storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def _roll_poc(types: set) -> str:
    for t in _POC_PRIORITY:
        if t in types:
            return t
    return sorted(types)[0] if types else None


def build_sam_labor_poc_people() -> dict:
    import datetime as dt

    import duckdb
    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    tmp = os.environ.get("DUCKDB_TEMP_DIR", "/tmp/duck_slpp")
    os.makedirs(tmp, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads=4; SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT', '8GB')}'; "
                f"SET temp_directory='{tmp}';")
    # order-independent name_key parity is enforced in Python; DuckDB only normalizes domains.
    con.execute(r"CREATE MACRO norm(x) AS "
                r"nullif(regexp_replace(split_part(regexp_replace(lower(trim(x)), '^https?://', ''), '/', 1), "
                r"'^www\.', ''), '');")
    con.execute("CREATE TABLE generic_domains(dom VARCHAR);")
    con.executemany("INSERT INTO generic_domains VALUES (?)", [(d,) for d in sorted(GENERIC_DOMAINS)])

    def reg(name, uri, cols):
        con.register(name, lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader())

    UNIV_COLS = ["uei", "legal_business_name", "normalized_domain", "entity_url", "primary_naics",
                 "naics_family", "naics_label", "in_our_staffing", "has_prime", "has_subaward",
                 "award_active", "pdl_company_linkedin_url"]
    reg("slu", LABOR_URI, UNIV_COLS)
    reg("sflu", FACIL_URI, UNIV_COLS)
    con.execute("CREATE TABLE univ AS "
                "SELECT *, 'labor' AS universe FROM slu "
                "UNION ALL BY NAME SELECT *, 'facilities' AS universe FROM sflu")
    con.execute("CREATE TABLE univ_uei AS SELECT DISTINCT uei FROM univ WHERE uei IS NOT NULL AND uei<>''")

    # --- domain universe: sam_master_domains ∪ crosswalk_dsbs_sam(best+email-suffix+5 more) ∪ self ---
    reg("smd", SMD_URI, ["uei", "normalized_domain"])
    con.execute("CREATE TABLE smd_t AS SELECT uei, normalized_domain FROM smd WHERE uei IN (SELECT uei FROM univ_uei)")
    reg("xw", XWALK_URI, ["uei", "best_domain", "matched_domain", "domain_entity_url", "domain_website",
                          "domain_email", "domain_additional", "company_linkedin_url"])
    con.execute("CREATE TABLE xw_t AS SELECT * FROM xw WHERE uei IN (SELECT uei FROM univ_uei)")
    con.execute("""CREATE TABLE dom_uei AS SELECT DISTINCT uei, dom FROM (
        SELECT uei, norm(normalized_domain) dom FROM univ
        UNION ALL SELECT uei, norm(entity_url)          FROM univ
        UNION ALL SELECT uei, norm(normalized_domain)   FROM smd_t
        UNION ALL SELECT uei, norm(best_domain)         FROM xw_t
        UNION ALL SELECT uei, norm(matched_domain)      FROM xw_t
        UNION ALL SELECT uei, norm(domain_entity_url)   FROM xw_t
        UNION ALL SELECT uei, norm(domain_website)      FROM xw_t
        UNION ALL SELECT uei, norm(domain_email)        FROM xw_t
        UNION ALL SELECT uei, norm(domain_additional)   FROM xw_t
    ) WHERE dom IS NOT NULL AND dom LIKE '%.%' AND dom NOT IN (SELECT dom FROM generic_domains)""")

    # --- firm dimension: 1 row per uei ---
    con.execute("""CREATE TABLE firm AS
        SELECT u.uei, any_value(u.legal_business_name) legal_business_name, any_value(u.primary_naics) primary_naics,
               any_value(u.naics_family) naics_family, any_value(u.naics_label) naics_label, any_value(u.universe) universe,
               any_value(u.in_our_staffing) in_our_staffing, any_value(u.has_prime) has_prime,
               any_value(u.has_subaward) has_subaward, any_value(u.award_active) award_active,
               coalesce(any_value(norm(u.normalized_domain)), any_value(norm(u.entity_url)),
                        any_value(x.best_domain_norm)) firm_domain,
               coalesce(any_value(u.pdl_company_linkedin_url), any_value(x.company_linkedin_url)) company_linkedin_url
        FROM univ u LEFT JOIN (SELECT uei, norm(best_domain) best_domain_norm, any_value(company_linkedin_url) company_linkedin_url
                               FROM xw_t GROUP BY uei, norm(best_domain)) x USING(uei)
        GROUP BY u.uei""")

    # --- POCs: sam_pocs (all slots) + dsbs_pocs, in universe, name present ---
    reg("sp", SAM_POCS_URI, ["uei", "poc_type", "full_name", "title"])
    con.execute("CREATE TABLE poc_sam AS SELECT uei, poc_type, full_name, title FROM sp "
                "WHERE uei IN (SELECT uei FROM univ_uei) AND full_name IS NOT NULL AND trim(full_name)<>''")
    reg("dp", DSBS_POCS_URI, ["uei", "poc_type", "full_name", "title"])
    con.execute("CREATE TABLE poc_dsbs AS SELECT uei, poc_type, full_name, title FROM dp "
                "WHERE uei IN (SELECT uei FROM univ_uei) AND full_name IS NOT NULL AND trim(full_name)<>''")

    # aggregate to (uei, name_key) in Python — name_key must match the person side exactly
    poc_agg: dict = {}

    def _fold(rows, src):
        for uei, poc_type, full_name, title in rows:
            nk = _name_key(full_name)
            if not nk:
                continue
            r = poc_agg.setdefault((uei, nk), {"uei": uei, "name_key": nk, "full_name": full_name,
                                               "title": title, "types": set(), "src": set()})
            if poc_type:
                r["types"].add(poc_type)
            r["src"].add(src)
            if full_name and len(full_name) > len(r["full_name"] or ""):
                r["full_name"] = full_name
            if title and not r["title"]:
                r["title"] = title

    _fold(con.execute("SELECT uei, poc_type, full_name, title FROM poc_sam").fetchall(), "sam")
    _fold(con.execute("SELECT uei, poc_type, full_name, title FROM poc_dsbs").fetchall(), "dsbs")

    PA = {k: [] for k in ["uei", "name_key", "poc_full_name", "poc_title", "poc_type", "poc_types_all",
                          "poc_source", "is_principal"]}
    for r in poc_agg.values():
        PA["uei"].append(r["uei"]); PA["name_key"].append(r["name_key"])
        PA["poc_full_name"].append(r["full_name"]); PA["poc_title"].append(r["title"])
        PA["poc_type"].append(_roll_poc(r["types"]))
        PA["poc_types_all"].append(",".join(sorted(r["types"])) or None)
        s = r["src"]
        PA["poc_source"].append("both" if len(s) == 2 else next(iter(s)))
        PA["is_principal"].append(bool(r["types"] & _PRINCIPAL_SLOTS))
    con.register("poc_agg_arw", pa.table(PA))
    con.execute("CREATE TABLE poc_agg AS SELECT * FROM poc_agg_arw")
    poc_keys = set(zip(PA["uei"], PA["name_key"]))

    # --- people at the firm: clay_find_people + active/people, joined on domain, name-matched ---
    reg("cp", CLAY_URI, ["person_id", "domain_norm", "full_name", "matched_job_title",
                         "linkedin_url_norm", "linkedin_url_raw"])
    con.execute("CREATE TABLE clay_cand AS "
                "SELECT DISTINCT d.uei, c.person_id, c.full_name, c.matched_job_title AS title, "
                "coalesce(c.linkedin_url_norm, c.linkedin_url_raw) AS linkedin "
                "FROM cp c JOIN dom_uei d ON d.dom = norm(c.domain_norm) "
                "WHERE c.full_name IS NOT NULL AND c.person_id IS NOT NULL")
    reg("pe", SPINE_URI, ["person_id", "normalized_domain", "full_name", "title", "person_linkedin_url"])
    con.execute("CREATE TABLE peo_cand AS "
                "SELECT DISTINCT d.uei, p.person_id, p.full_name, p.title, p.person_linkedin_url AS linkedin "
                "FROM pe p JOIN dom_uei d ON d.dom = norm(p.normalized_domain) "
                "WHERE p.full_name IS NOT NULL AND p.person_id IS NOT NULL")

    rows: dict = {}
    for u, pid, fn, ti, li in con.execute("SELECT uei, person_id, full_name, title, linkedin FROM clay_cand").fetchall():
        k = _name_key(fn)
        if (u, k) not in poc_keys:
            continue
        r = rows.setdefault((u, pid), {"uei": u, "person_id": pid, "name_key": k, "full_name": fn,
                                       "title": ti, "linkedin": li, "src": set()})
        r["src"].add("clay")
    for u, pid, fn, ti, li in con.execute("SELECT uei, person_id, full_name, title, linkedin FROM peo_cand").fetchall():
        k = _name_key(fn)
        if (u, k) not in poc_keys:
            continue
        r = rows.setdefault((u, pid), {"uei": u, "person_id": pid, "name_key": k, "full_name": fn,
                                       "title": None, "linkedin": None, "src": set()})
        r["full_name"] = fn or r["full_name"]
        r["title"] = ti or r.get("title")
        r["linkedin"] = li or r.get("linkedin")  # prefer active/people identity linkedin
        r["src"].add("people")

    M = {k: [] for k in ["uei", "person_id", "name_key", "person_full_name", "person_title",
                         "person_linkedin_url", "match_source"]}
    for r in rows.values():
        M["uei"].append(r["uei"]); M["person_id"].append(r["person_id"]); M["name_key"].append(r["name_key"])
        M["person_full_name"].append(r["full_name"]); M["person_title"].append(r["title"])
        M["person_linkedin_url"].append(r["linkedin"])
        s = r["src"]
        M["match_source"].append("both" if len(s) == 2 else next(iter(s)))
    con.register("matched", pa.table(M))

    # --- coverage aggregates ---
    reg("we", WORK_EMAILS_URI, ["person_id", "email", "verification_status"])
    con.execute("CREATE TABLE we_agg AS SELECT person_id, max(email) work_email, "
                "max(verification_status) work_email_status FROM we WHERE email IS NOT NULL GROUP BY person_id")
    reg("ph", PHONE_URI, ["person_id", "phone", "phone_type"])
    con.execute("CREATE TABLE ph_agg AS SELECT person_id, max(phone) mobile_phone FROM ph "
                "WHERE phone_type='mobile' AND phone IS NOT NULL GROUP BY person_id")

    mat = dt.datetime.now(dt.timezone.utc)

    # ---------- OUTPUT 1: resolved people ----------
    people = con.execute("""
        SELECT m.uei, f.legal_business_name, f.primary_naics, f.naics_family, f.naics_label, f.universe,
               f.firm_domain, f.company_linkedin_url, f.in_our_staffing, f.has_prime, f.has_subaward, f.award_active,
               m.name_key, p.poc_source, p.poc_type, p.poc_types_all, p.is_principal, p.poc_full_name, p.poc_title,
               m.person_id, m.person_linkedin_url, m.person_full_name, m.person_title, m.match_source,
               (we.person_id IS NOT NULL) AS has_work_email, we.work_email, we.work_email_status,
               (ph.person_id IS NOT NULL) AS has_mobile, ph.mobile_phone
        FROM matched m
        LEFT JOIN firm f    ON f.uei = m.uei
        LEFT JOIN poc_agg p ON p.uei = m.uei AND p.name_key = m.name_key
        LEFT JOIN we_agg we ON we.person_id = m.person_id
        LEFT JOIN ph_agg ph ON ph.person_id = m.person_id
    """).to_arrow_table()
    people = people.append_column("materialized_at", pa.array([mat] * people.num_rows, pa.timestamp("us", tz="UTC")))

    # ---------- OUTPUT 2: unresolved worklist ----------
    con.execute("CREATE TABLE matched_keys AS SELECT DISTINCT uei, name_key FROM matched")
    unres = con.execute("""
        SELECT p.uei, f.legal_business_name, f.primary_naics, f.naics_family, f.naics_label, f.universe,
               f.firm_domain, f.company_linkedin_url, f.in_our_staffing, f.has_prime, f.has_subaward, f.award_active,
               p.name_key, p.poc_source, p.poc_type, p.poc_types_all, p.is_principal, p.poc_full_name, p.poc_title
        FROM poc_agg p
        LEFT JOIN firm f ON f.uei = p.uei
        WHERE NOT EXISTS (SELECT 1 FROM matched_keys k WHERE k.uei=p.uei AND k.name_key=p.name_key)
    """).to_arrow_table()
    unres = unres.append_column("materialized_at", pa.array([mat] * unres.num_rows, pa.timestamp("us", tz="UTC")))

    def _write(tbl, uri, indexes):
        lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version="2.1", storage_options=so)
        ds = lance.dataset(uri, storage_options=so)
        assert ds.count_rows() == tbl.num_rows
        names = set(ds.schema.names)
        for typ, cols in indexes.items():
            for col in cols:
                if col in names:
                    try:
                        ds.create_scalar_index(col, index_type=typ)
                    except Exception as exc:  # noqa: BLE001
                        print(f"  idx skip {typ} {col}: {exc}", flush=True)
        return ds

    dsp = _write(people, PEOPLE_URI, INDEXES_PEOPLE)
    _write(unres, UNRES_URI, INDEXES_UNRES)

    con.register("o", dsp.to_table(columns=["uei", "person_id", "name_key", "match_source",
                                            "is_principal", "has_work_email", "has_mobile"]))
    firms = con.execute("SELECT count(DISTINCT uei) FROM o").fetchone()[0]
    ppl = con.execute("SELECT count(DISTINCT person_id) FROM o").fetchone()[0]
    resolved_keys = con.execute("SELECT count(DISTINCT (uei, name_key)) FROM o").fetchone()[0]
    em = con.execute("SELECT sum(has_work_email::int) FROM o").fetchone()[0]
    mo = con.execute("SELECT sum(has_mobile::int) FROM o").fetchone()[0]
    prin = con.execute("SELECT count(*) FROM o WHERE is_principal").fetchone()[0]
    poc_total = len(poc_keys)
    print(f"POC universe (uei,name_key) : {poc_total:,}", flush=True)
    print(f"WROTE {PEOPLE_URI}  rows={people.num_rows:,}  firms={firms:,}  distinct_people={ppl:,}  "
          f"resolved_pocs={resolved_keys:,} ({100*resolved_keys/max(poc_total,1):.1f}%)  "
          f"principals={prin:,}  has_work_email={em:,}  has_mobile={mo:,}", flush=True)
    print(f"WROTE {UNRES_URI}  rows={unres.num_rows:,}  (worklist = POC universe − resolved)", flush=True)
    return {"people_uri": PEOPLE_URI, "people_rows": people.num_rows, "unresolved_uri": UNRES_URI,
            "unresolved_rows": unres.num_rows, "poc_universe": poc_total, "resolved_pocs": resolved_keys,
            "firms": firms, "distinct_people": ppl}


def _record_run(res, status, error, started, completed):
    import psycopg
    from psycopg.types.json import Jsonb
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED unset; skipping ops ledger.")
        return
    datasets = {DS_PEOPLE: res.get("people_rows", 0), DS_UNRES: res.get("unresolved_rows", 0)}
    rows_total = res.get("people_rows", 0) + res.get("unresolved_rows", 0)
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("INSERT INTO ops.sam_labor_poc_people_runs (feed,source_db,datasets,mode,rows_total,"
                        "status,error,started_at,completed_at) VALUES (%s,%s,%s,'overwrite',%s,%s,%s,%s,%s)",
                        (FEED, SOURCE_DB, Jsonb(datasets), rows_total, status, error, started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops ledger write failed: {exc}")


_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


@app.function(secrets=_SECRETS, timeout=60 * 40, memory=32768, cpu=4.0)
def ingest() -> dict:
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_sam_labor_poc_people()
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record_run(res, status, error, started, dt.datetime.now(dt.timezone.utc))
    return res


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest.remote(), indent=2, default=str))


if __name__ == "__main__":
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_sam_labor_poc_people()
        status = "success"
    finally:
        _record_run(res, status, error, started, dt.datetime.now(dt.timezone.utc))
    print(res)
