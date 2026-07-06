#!/usr/bin/env python3
"""gtm_audience_marts — the four-mart audience-building layer (demonstrated behavior only).

SoR  s3://data-sink/active/gtm_sub_combo_lanes/      (uei × naics × psc, windowed sub $)
     s3://data-sink/active/gtm_prime_combo_lanes/    (uei × naics × psc, windowed prime obl)
     s3://data-sink/active/gtm_audience_entities/    (entity-grain filter spine)
     s3://data-sink/active/gtm_audience_people/      (person-grain, full universe, enrichment_state)
     (Lance; derived, snapshot-overwrite; BTREE on join keys)

WHY
Audience cuts of the form  code-combo × $ window × firmographics × contactability  were
each a fresh multi-minute scan over usaspending_subaward_canonical (1.3M events) and
usaspending_fpds_canonical_txn (108M txns). These marts pre-aggregate the raw DIMENSIONS
(never the cuts), so any slightly-different audience question is a WHERE clause over
narrow indexed tables — no rebuild, no rescan.

DEMONSTRATED ONLY. Lanes aggregate actual subaward events / actual FPDS prime
transactions. The inference layer (gtm_entity_inferred_primeable_codes /
gtm_entity_inferred_subbable_codes) is separate, single-code, and NOT reproduced here.

COMBO KEY. naics_code × psc_code taken together from the same event — for sub lanes the
PRIME award's codes the sub worked under; for prime lanes the entity's own award codes.
NULL codes are retained (their $ still count toward entity totals); combo filters simply
don't match them.

WINDOWS are rolling, anchored at build-time as_of (12/24/60mo + lifetime) — same
staleness model as gtm_entity_code_lanes; rerun to refresh, move anchors deliberately.

PROVIDER VALUES ARE VERBATIM in gtm_audience_people (phone/email/status fields pass
through from gtm_sam_person_contactability untouched). enrichment_state is derived,
priority-ordered:
  dialable             mobile in hand (phone_status='found')
  emailable            work email in hand, no mobile → phone-enrichment candidate
  bridged_no_contacts  LinkedIn known, no assets → contact-resolution candidate
  unbridged            no LinkedIn → identity-bridge candidate (icypeas/exa) first

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_audience_marts.py [--verify]
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
OUT_SUB_LANES = f"{A}/gtm_sub_combo_lanes/"
OUT_PRIME_LANES = f"{A}/gtm_prime_combo_lanes/"
OUT_ENTITIES = f"{A}/gtm_audience_entities/"
OUT_PEOPLE = f"{A}/gtm_audience_people/"
PARAM_SET_ID = "v3"  # v2: lane code titles; entity 12/60mo windows + $ text bands,
                     # primary PoP state/county, FFATA flag, DSBS + FSRS designation
                     # flags, NAICS 2-6 rollup strings; people exec-officer fields
                     # v3: people dm_class_v2 (deterministic DM-title upgrade)

LANE_BTREE = ["uei", "naics_code", "psc_code"]
ENTITY_BTREE = ["uei", "normalized_domain"]
PEOPLE_BTREE = ["sam_person_id", "uei"]

# dm_class_v2 (operator-directed 2026-07-06): provider-verbatim dm_class has a
# ~25% false-negative rate on decision makers at subawardees (titles literally
# 'Chief Executive Officer', 'President & CEO', 'Founder' marked not_dm, plus
# VPs excluded by policy). dm_class_v2 upgrades to 'dm' on a deterministic
# title match — never downgrades; dm_class passes through verbatim.
DM_TITLE_REGEXES = [
    r"\bc\.?e\.?o\.?\b|chief\s+exec",
    r"\bowner\b",
    r"\bfounder\b",
    r"managing\s+(partner|member|principal|director)",
    r"\bprincipal\b",
    r"\bpartner\b",
    (r"\bc\.?o\.?o\.?\b|\bc\.?f\.?o\.?\b|\bc\.?t\.?o\.?\b|\bc\.?i\.?o\.?\b"
     r"|\bc\.?a\.?o\.?\b|\bc\.?r\.?o\.?\b|chief\s+\w+\s+officer"),
    r"executive\s+director",
    r"\bv\.?p\.?\b|vice\s+president|\bs\.?v\.?p\.?\b|\ba\.?v\.?p\.?\b|\be\.?v\.?p\.?\b",
    r"general\s+manager|\bg\.?m\.?\b",
]


def dm_title_hit_sql(col: str) -> str:
    """DuckDB predicate: does `col` carry a decision-maker title?

    President is matched by stripping vice/assistant/deputy-president phrases
    first and requiring a surviving standalone 'president' — VP-only titles
    can't land here (they are matched deliberately by the vp regex above).
    """
    ors = " OR ".join(
        f"regexp_matches(lower({col}), '{p}')" for p in DM_TITLE_REGEXES)
    pres = (f"regexp_matches(regexp_replace(lower({col}), "
            f"'(vice|assistant|asst\\.?|deputy)[\\s-]*president', '', 'g'), "
            f"'\\bpresident\\b')")
    return f"({col} IS NOT NULL AND ({ors} OR {pres}))"


def _band(col: str) -> str:
    """At-a-glance text range for a $ column (Close-friendly; sorts by magnitude prefix)."""
    return f"""CASE
        WHEN {col} IS NULL THEN NULL
        WHEN {col} <= 0 THEN '<=$0'
        WHEN {col} < 250e3 THEN '<$250K'
        WHEN {col} < 1e6   THEN '$250K-$1M'
        WHEN {col} < 5e6   THEN '$1M-$5M'
        WHEN {col} < 10e6  THEN '$5M-$10M'
        WHEN {col} < 25e6  THEN '$10M-$25M'
        WHEN {col} < 100e6 THEN '$25M-$100M'
        WHEN {col} < 500e6 THEN '$100M-$500M'
        ELSE '$500M+' END"""


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


def _publish(con, table: str, uri: str, btree: list[str], built_from: str,
             as_of: str, opt: dict) -> None:
    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        '{built_from}' AS built_from_version, '{PARAM_SET_ID}' AS param_set_id
        FROM {table}""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, uri, [(c, "BTREE") for c in btree], storage_options=opt)
    print(f"wrote {uri}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)


def build() -> int:
    opt = so()
    today = date.today()
    as_of = today.isoformat()
    w12 = (today - timedelta(days=365)).isoformat()
    w24 = (today - timedelta(days=730)).isoformat()
    w60 = (today - timedelta(days=1826)).isoformat()
    con = _cx()

    # ── code references (titles for lanes + entity rollups) ───────────────────
    nref = lance.dataset(f"{A}/naics_reference/", storage_options=opt)
    con.register("_nref", nref.scanner(columns=["naics_code", "naics_title"]).to_reader())
    con.execute("""CREATE TABLE nref AS
        SELECT naics_code, any_value(naics_title) AS naics_title FROM _nref GROUP BY 1""")
    pref = lance.dataset(f"{A}/psc_reference/", storage_options=opt)
    con.register("_pref", pref.scanner(
        columns=["psc_code", "psc_name", "is_active"]).to_reader())
    con.execute("""CREATE TABLE pref AS
        SELECT psc_code, arg_max(psc_name, is_active::INT) AS psc_title
        FROM _pref WHERE psc_name IS NOT NULL GROUP BY 1""")

    # ── gtm_sub_combo_lanes ───────────────────────────────────────────────────
    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["subawardee_uei", "prime_awardee_uei", "subaward_amount",
                 "subaward_action_date", "prime_award_naics_code",
                 "prime_award_product_or_service_code"],
        filter="subawardee_uei IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE sub_lanes AS
        SELECT subawardee_uei AS uei,
               prime_award_naics_code AS naics_code,
               prime_award_product_or_service_code AS psc_code,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w12}') AS sub_amt_12mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w24}') AS sub_amt_24mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w60}') AS sub_amt_60mo,
               SUM(subaward_amount) AS sub_amt_lifetime,
               COUNT(*) FILTER (subaward_action_date >= DATE '{w24}') AS n_subawards_24mo,
               COUNT(*) AS n_subawards_lifetime,
               COUNT(DISTINCT prime_awardee_uei) AS n_distinct_primes_lifetime,
               MIN(subaward_action_date) AS first_action_date,
               MAX(subaward_action_date) AS last_action_date
        FROM _se GROUP BY 1, 2, 3""")
    con.execute("""CREATE TABLE sub_lanes_t AS
        SELECT l.*, n.naics_title, p.psc_title
        FROM sub_lanes l
        LEFT JOIN nref n USING (naics_code)
        LEFT JOIN pref p USING (psc_code)""")
    con.execute("DROP TABLE sub_lanes"); con.execute("ALTER TABLE sub_lanes_t RENAME TO sub_lanes")
    n = con.execute("SELECT COUNT(*) FROM sub_lanes").fetchone()[0]
    print(f"sub_lanes: {n:,} rows", flush=True)

    # second single-pass reader: subaward PoP for entity-level primary state/county
    con.register("_se_pop", se.scanner(
        columns=["subawardee_uei", "subaward_amount",
                 "subaward_primary_place_of_performance_state_code",
                 "sub_place_of_perform_county_name"],
        filter="subawardee_uei IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE sub_pop AS
        SELECT subawardee_uei AS uei,
               subaward_primary_place_of_performance_state_code AS pop_state,
               sub_place_of_perform_county_name AS pop_county,
               SUM(subaward_amount) AS amt
        FROM _se_pop GROUP BY 1, 2, 3""")

    # ── gtm_prime_combo_lanes ─────────────────────────────────────────────────
    tx = lance.dataset(f"{A}/usaspending_fpds_canonical_txn/", storage_options=opt)
    con.register("_tx", tx.scanner(
        columns=["recipient_uei", "naics_code", "product_or_service_code",
                 "federal_action_obligation", "action_date"],
        filter="recipient_uei IS NOT NULL").to_reader())
    con.execute(f"""CREATE TABLE prime_lanes AS
        SELECT recipient_uei AS uei,
               naics_code,
               product_or_service_code AS psc_code,
               SUM(federal_action_obligation) FILTER (action_date >= DATE '{w12}') AS prime_obl_12mo,
               SUM(federal_action_obligation) FILTER (action_date >= DATE '{w24}') AS prime_obl_24mo,
               SUM(federal_action_obligation) FILTER (action_date >= DATE '{w60}') AS prime_obl_60mo,
               SUM(federal_action_obligation) AS prime_obl_lifetime,
               COUNT(*) FILTER (action_date >= DATE '{w24}') AS n_txns_24mo,
               COUNT(*) AS n_txns_lifetime,
               MIN(action_date) AS first_action_date,
               MAX(action_date) AS last_action_date
        FROM _tx GROUP BY 1, 2, 3""")
    con.execute("""CREATE TABLE prime_lanes_t AS
        SELECT l.*, n.naics_title, p.psc_title
        FROM prime_lanes l
        LEFT JOIN nref n USING (naics_code)
        LEFT JOIN pref p USING (psc_code)""")
    con.execute("DROP TABLE prime_lanes"); con.execute("ALTER TABLE prime_lanes_t RENAME TO prime_lanes")
    n = con.execute("SELECT COUNT(*) FROM prime_lanes").fetchone()[0]
    print(f"prime_lanes: {n:,} rows", flush=True)

    # second single-pass reader: prime PoP for entity-level primary state/county
    con.register("_tx_pop", tx.scanner(
        columns=["recipient_uei", "federal_action_obligation",
                 "primary_place_of_performance_state_code", "pop_county_name"],
        filter="recipient_uei IS NOT NULL").to_reader())
    con.execute("""CREATE TABLE prime_pop AS
        SELECT recipient_uei AS uei,
               primary_place_of_performance_state_code AS pop_state,
               pop_county_name AS pop_county,
               SUM(federal_action_obligation) AS amt
        FROM _tx_pop GROUP BY 1, 2, 3""")

    # primary PoP per entity: modal by lifetime $ across prime + sub activity
    con.execute("""CREATE TABLE pop_primary AS
        WITH u AS (SELECT * FROM prime_pop UNION ALL SELECT * FROM sub_pop),
        st AS (
            SELECT uei, arg_max(pop_state, amt) AS primary_pop_state FROM (
                SELECT uei, pop_state, SUM(amt) AS amt FROM u
                WHERE pop_state IS NOT NULL GROUP BY 1, 2) GROUP BY 1),
        co AS (
            SELECT uei, arg_max(pop_county, amt) AS primary_pop_county FROM (
                SELECT uei, pop_county, SUM(amt) AS amt FROM u
                WHERE pop_county IS NOT NULL GROUP BY 1, 2) GROUP BY 1)
        SELECT coalesce(st.uei, co.uei) AS uei, st.primary_pop_state, co.primary_pop_county
        FROM st FULL JOIN co ON st.uei = co.uei""")

    # ── gtm_audience_people ───────────────────────────────────────────────────
    ppl = lance.dataset(f"{A}/gtm_sam_people/", storage_options=opt)
    con.register("_ppl", ppl.scanner(columns=[
        "sam_person_id", "uei", "display_name", "first_name", "last_name",
        "best_title", "is_govt_poc", "is_ebiz_poc", "is_dsbs_contact",
        "is_dsbs_principal", "is_exec_officer_prime", "is_exec_officer_sub",
        "max_officer_amount", "n_sources"]).to_reader())
    con.execute("CREATE TABLE ppl AS SELECT * FROM _ppl")

    titles = lance.dataset(f"{A}/gtm_sam_person_titles/", storage_options=opt)
    con.register("_ti", titles.scanner(
        columns=["sam_person_id", "title_verbatim", "dm_class"]).to_reader())
    con.execute("""CREATE TABLE ti AS
        SELECT sam_person_id, any_value(title_verbatim) AS title_verbatim,
               any_value(dm_class) AS dm_class
        FROM _ti GROUP BY 1""")

    ident = lance.dataset(f"{A}/gtm_sam_person_identity/", storage_options=opt)
    con.register("_id", ident.scanner(
        columns=["sam_person_id", "person_linkedin_url_norm"]).to_reader())
    con.execute("""CREATE TABLE ident AS
        SELECT sam_person_id, any_value(person_linkedin_url_norm) AS person_linkedin_url_norm
        FROM _id GROUP BY 1""")

    ct = lance.dataset(f"{A}/gtm_sam_person_contactability/", storage_options=opt)
    con.register("_ct", ct.scanner(columns=[
        "sam_person_id", "phone", "phone_status", "phone_type", "phone_source_vendor",
        "phone_resolved_at", "email", "email_verification_status",
        "email_source_vendor", "mv_result", "mv_quality"]).to_reader())
    con.execute("CREATE TABLE ct AS SELECT * FROM _ct")

    dm_hit = dm_title_hit_sql("coalesce(t.title_verbatim, p.best_title)")
    con.execute(f"""CREATE TABLE people_out AS
        SELECT p.sam_person_id, p.uei, p.display_name, p.first_name, p.last_name,
               coalesce(t.title_verbatim, p.best_title) AS title,
               t.dm_class,
               CASE WHEN t.dm_class = 'dm' OR {dm_hit} THEN 'dm'
                    ELSE t.dm_class END AS dm_class_v2,
               p.is_govt_poc, p.is_ebiz_poc, p.is_dsbs_contact, p.is_dsbs_principal,
               p.is_exec_officer_prime, p.is_exec_officer_sub, p.max_officer_amount,
               p.n_sources,
               i.person_linkedin_url_norm,
               c.phone, c.phone_status, c.phone_type, c.phone_source_vendor,
               c.phone_resolved_at,
               c.email, c.email_verification_status, c.email_source_vendor,
               c.mv_result, c.mv_quality,
               CASE WHEN c.phone IS NOT NULL AND c.phone_status = 'found' THEN 'dialable'
                    WHEN c.email IS NOT NULL THEN 'emailable'
                    WHEN i.person_linkedin_url_norm IS NOT NULL THEN 'bridged_no_contacts'
                    ELSE 'unbridged' END AS enrichment_state
        FROM ppl p
        LEFT JOIN ti t USING (sam_person_id)
        LEFT JOIN ident i USING (sam_person_id)
        LEFT JOIN ct c USING (sam_person_id)""")
    n = con.execute("SELECT COUNT(*) FROM people_out").fetchone()[0]
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT sam_person_id FROM people_out GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"people grain not unique: {dup} dups"
    states = dict(con.execute(
        "SELECT enrichment_state, COUNT(*) FROM people_out GROUP BY 1").fetchall())
    print(f"people_out: {n:,} rows  states={states}", flush=True)

    # ── gtm_audience_entities ─────────────────────────────────────────────────
    ents = lance.dataset(f"{A}/gtm_sam_entities/", storage_options=opt)
    con.register("_g", ents.scanner(columns=[
        "uei", "legal_business_name", "normalized_domain", "physical_city",
        "physical_state", "primary_naics", "in_dsbs", "sam_is_active",
        "is_subawardee", "is_prime_recipient"]).to_reader())
    con.execute("""CREATE TABLE g AS
        SELECT uei, any_value(legal_business_name) AS legal_business_name,
               any_value(normalized_domain) AS normalized_domain,
               any_value(physical_city) AS physical_city,
               any_value(physical_state) AS physical_state,
               any_value(primary_naics) AS primary_naics,
               bool_or(in_dsbs) AS in_dsbs,
               bool_or(sam_is_active) AS sam_is_active,
               bool_or(is_subawardee) AS is_subawardee,
               bool_or(is_prime_recipient) AS is_prime_recipient
        FROM _g GROUP BY 1""")

    fb = lance.dataset(f"{A}/firmographics_blitz/", storage_options=opt)
    con.register("_fb", fb.scanner(
        columns=["domain_norm", "employee_size_band", "employees_on_linkedin"]).to_reader())
    con.execute("""CREATE TABLE fb AS
        SELECT domain_norm, any_value(employee_size_band) AS employee_size_band,
               any_value(employees_on_linkedin) AS employees_on_linkedin
        FROM _fb WHERE domain_norm IS NOT NULL GROUP BY 1""")

    ffata = lance.dataset(f"{A}/ffata_exec_comp/", storage_options=opt)
    con.register("_ff", ffata.scanner(columns=["recipient_uei"]).to_reader())
    con.execute("CREATE TABLE ff AS SELECT DISTINCT recipient_uei AS uei FROM _ff")

    dsbs = lance.dataset(f"{A}/sba_dsbs_certified_firms/", storage_options=opt)
    con.register("_ds", dsbs.scanner(columns=[
        "uei", "active_8a_boolean", "active_hz_boolean", "active_wosb_boolean",
        "active_edwosb_boolean", "active_sdvosb_boolean", "active_vosb_boolean"]).to_reader())
    con.execute("""CREATE TABLE dsbs_flags AS
        SELECT uei, bool_or(active_8a_boolean) AS dsbs_8a,
               bool_or(active_hz_boolean) AS dsbs_hubzone,
               bool_or(active_wosb_boolean) AS dsbs_wosb,
               bool_or(active_edwosb_boolean) AS dsbs_edwosb,
               bool_or(active_sdvosb_boolean) AS dsbs_sdvosb,
               bool_or(active_vosb_boolean) AS dsbs_vosb
        FROM _ds WHERE uei IS NOT NULL GROUP BY 1""")

    desg = lance.dataset(f"{A}/govcon_subawardee_designations/", storage_options=opt)
    con.register("_dg", desg.scanner(columns=[
        "subawardee_uei", "service_disabled_veteran_owned_business", "veteran_owned_business",
        "women_owned_small_business", "economically_disadvantaged_women_owned_small_business",
        "woman_owned_business", "historically_underutilized_business_zone_hubzone_firm",
        "c8a_program_participant", "small_disadvantaged_business",
        "self_certified_small_disadvantaged_business", "minority_owned_business",
        "any_socioeconomic_designation", "designation_count"]).to_reader())
    con.execute("""CREATE TABLE fsrs_flags AS
        SELECT subawardee_uei AS uei,
               bool_or(service_disabled_veteran_owned_business) AS fsrs_sdvosb,
               bool_or(veteran_owned_business) AS fsrs_vosb,
               bool_or(women_owned_small_business) AS fsrs_wosb,
               bool_or(economically_disadvantaged_women_owned_small_business) AS fsrs_edwosb,
               bool_or(woman_owned_business) AS fsrs_woman_owned,
               bool_or(historically_underutilized_business_zone_hubzone_firm) AS fsrs_hubzone,
               bool_or(c8a_program_participant) AS fsrs_8a,
               bool_or(small_disadvantaged_business) AS fsrs_sdb,
               bool_or(self_certified_small_disadvantaged_business) AS fsrs_self_sdb,
               bool_or(minority_owned_business) AS fsrs_minority,
               bool_or(any_socioeconomic_designation) AS fsrs_any_designation,
               MAX(designation_count) AS fsrs_designation_count
        FROM _dg WHERE subawardee_uei IS NOT NULL GROUP BY 1""")

    band_cols = ",\n               ".join(
        f"{_band(c)} AS {c}_band"
        for c in ("sub_amt_12mo", "sub_amt_24mo", "sub_amt_60mo", "sub_amt_lifetime",
                  "prime_obl_12mo", "prime_obl_24mo", "prime_obl_60mo", "prime_obl_lifetime"))
    con.execute(f"""CREATE TABLE entities_out AS
        WITH sub_tot AS (
            SELECT uei, SUM(sub_amt_12mo) AS sub_amt_12mo,
                   SUM(sub_amt_24mo) AS sub_amt_24mo,
                   SUM(sub_amt_60mo) AS sub_amt_60mo,
                   SUM(sub_amt_lifetime) AS sub_amt_lifetime,
                   SUM(n_subawards_24mo) AS n_subawards_24mo,
                   MAX(last_action_date) AS sub_last_action_date
            FROM sub_lanes GROUP BY 1),
        prime_tot AS (
            SELECT uei, SUM(prime_obl_12mo) AS prime_obl_12mo,
                   SUM(prime_obl_24mo) AS prime_obl_24mo,
                   SUM(prime_obl_60mo) AS prime_obl_60mo,
                   SUM(prime_obl_lifetime) AS prime_obl_lifetime,
                   SUM(n_txns_24mo) AS n_prime_txns_24mo,
                   MAX(last_action_date) AS prime_last_action_date
            FROM prime_lanes GROUP BY 1),
        ppl_tot AS (
            SELECT uei, COUNT(*) AS n_sam_people,
                   COUNT(*) FILTER (person_linkedin_url_norm IS NOT NULL) AS n_bridged,
                   COUNT(*) FILTER (enrichment_state = 'dialable') AS n_dialable,
                   COUNT(*) FILTER (enrichment_state = 'emailable') AS n_emailable,
                   COUNT(*) FILTER (enrichment_state = 'bridged_no_contacts') AS n_bridged_no_contacts,
                   COUNT(*) FILTER (enrichment_state = 'unbridged') AS n_unbridged,
                   COUNT(*) FILTER (email_verification_status = 'verified') AS n_email_verified,
                   COUNT(*) FILTER (dm_class = 'dm') AS n_dm
            FROM people_out GROUP BY 1),
        base AS (
        SELECT g.*, fb.employee_size_band, fb.employees_on_linkedin,
               st.sub_amt_12mo, st.sub_amt_24mo, st.sub_amt_60mo, st.sub_amt_lifetime,
               st.n_subawards_24mo, st.sub_last_action_date,
               pt.prime_obl_12mo, pt.prime_obl_24mo, pt.prime_obl_60mo,
               pt.prime_obl_lifetime, pt.n_prime_txns_24mo, pt.prime_last_action_date,
               po.primary_pop_state, po.primary_pop_county,
               (ff.uei IS NOT NULL) AS is_ffata_disclosing,
               df.dsbs_8a, df.dsbs_hubzone, df.dsbs_wosb, df.dsbs_edwosb,
               df.dsbs_sdvosb, df.dsbs_vosb,
               fs.fsrs_sdvosb, fs.fsrs_vosb, fs.fsrs_wosb, fs.fsrs_edwosb,
               fs.fsrs_woman_owned, fs.fsrs_hubzone, fs.fsrs_8a, fs.fsrs_sdb,
               fs.fsrs_self_sdb, fs.fsrs_minority, fs.fsrs_any_designation,
               fs.fsrs_designation_count,
               coalesce(pp.n_sam_people, 0) AS n_sam_people,
               coalesce(pp.n_bridged, 0) AS n_bridged,
               coalesce(pp.n_dialable, 0) AS n_dialable,
               coalesce(pp.n_emailable, 0) AS n_emailable,
               coalesce(pp.n_bridged_no_contacts, 0) AS n_bridged_no_contacts,
               coalesce(pp.n_unbridged, 0) AS n_unbridged,
               coalesce(pp.n_email_verified, 0) AS n_email_verified,
               coalesce(pp.n_dm, 0) AS n_dm
        FROM g
        LEFT JOIN fb ON fb.domain_norm = g.normalized_domain
        LEFT JOIN sub_tot st USING (uei)
        LEFT JOIN prime_tot pt USING (uei)
        LEFT JOIN pop_primary po USING (uei)
        LEFT JOIN ff USING (uei)
        LEFT JOIN dsbs_flags df USING (uei)
        LEFT JOIN fsrs_flags fs USING (uei)
        LEFT JOIN ppl_tot pp USING (uei))
        SELECT b.*,
               {band_cols},
               CASE WHEN n2.naics_title IS NOT NULL
                    THEN n2.naics_code || ' - ' || n2.naics_title END AS naics_2,
               CASE WHEN n3.naics_title IS NOT NULL
                    THEN n3.naics_code || ' - ' || n3.naics_title END AS naics_3,
               CASE WHEN n4.naics_title IS NOT NULL
                    THEN n4.naics_code || ' - ' || n4.naics_title END AS naics_4,
               CASE WHEN n5.naics_title IS NOT NULL
                    THEN n5.naics_code || ' - ' || n5.naics_title END AS naics_5,
               CASE WHEN n6.naics_title IS NOT NULL
                    THEN n6.naics_code || ' - ' || n6.naics_title END AS naics_6
        FROM base b
        LEFT JOIN nref n2 ON n2.naics_code = substr(b.primary_naics, 1, 2)
        LEFT JOIN nref n3 ON n3.naics_code = substr(b.primary_naics, 1, 3)
        LEFT JOIN nref n4 ON n4.naics_code = substr(b.primary_naics, 1, 4)
        LEFT JOIN nref n5 ON n5.naics_code = substr(b.primary_naics, 1, 5)
        LEFT JOIN nref n6 ON n6.naics_code = substr(b.primary_naics, 1, 6)""")
    n = con.execute("SELECT COUNT(*) FROM entities_out").fetchone()[0]
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT uei FROM entities_out GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"entity grain not unique: {dup} dups"
    banded = con.execute(
        "SELECT COUNT(*) FROM entities_out WHERE employee_size_band IS NOT NULL").fetchone()[0]
    print(f"entities_out: {n:,} rows  banded={banded:,}", flush=True)

    # ── publish (order: lanes → people → entities) ────────────────────────────
    refs = f"naics_reference:v{nref.version}|psc_reference:v{pref.version}"
    _publish(con, "sub_lanes", OUT_SUB_LANES, LANE_BTREE,
             f"usaspending_subaward_canonical:v{se.version}|{refs}", as_of, opt)
    _publish(con, "prime_lanes", OUT_PRIME_LANES, LANE_BTREE,
             f"usaspending_fpds_canonical_txn:v{tx.version}|{refs}", as_of, opt)
    _publish(con, "people_out", OUT_PEOPLE, PEOPLE_BTREE,
             f"gtm_sam_people:v{ppl.version}|gtm_sam_person_titles:v{titles.version}|"
             f"gtm_sam_person_identity:v{ident.version}|gtm_sam_person_contactability:v{ct.version}",
             as_of, opt)
    _publish(con, "entities_out", OUT_ENTITIES, ENTITY_BTREE,
             f"gtm_sam_entities:v{ents.version}|firmographics_blitz:v{fb.version}|"
             f"usaspending_subaward_canonical:v{se.version}|usaspending_fpds_canonical_txn:v{tx.version}|"
             f"gtm_sam_people:v{ppl.version}|ffata_exec_comp:v{ffata.version}|"
             f"sba_dsbs_certified_firms:v{dsbs.version}|govcon_subawardee_designations:v{desg.version}|{refs}",
             as_of, opt)
    return 0


def verify() -> int:
    """Structural checks + the acid test: today's 347-entity cut, off the marts alone."""
    import time
    opt = so()
    ok = True
    checks = [(OUT_SUB_LANES, LANE_BTREE, 100_000), (OUT_PRIME_LANES, LANE_BTREE, 1_000_000),
              (OUT_ENTITIES, ENTITY_BTREE, 1_000_000), (OUT_PEOPLE, PEOPLE_BTREE, 1_000_000)]
    for uri, btree, floor in checks:
        ds = lance.dataset(uri, storage_options=opt)
        rows = ds.count_rows()
        idx = [i["name"] for i in ds.list_indices()]
        good = rows >= floor and all(any(c in i2 for i2 in idx) for c in btree)
        ok &= good
        print(f"{uri}: rows={rows:,} indices={idx} -> {'OK' if good else 'FAIL'}", flush=True)

    t0 = time.time()
    con = _cx()
    for name, uri in (("sl", OUT_SUB_LANES), ("pl", OUT_PRIME_LANES), ("ae", OUT_ENTITIES)):
        con.register(f"_l_{name}", lance.dataset(uri, storage_options=opt))
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM _l_{name}")
    n = con.execute("""
        SELECT COUNT(*) FROM ae
        WHERE uei IN (SELECT uei FROM sl GROUP BY 1
                      HAVING SUM(sub_amt_24mo) >= 1e6 AND SUM(sub_amt_24mo) < 100e6)
          AND uei IN (SELECT uei FROM pl
                      WHERE naics_code = '541330' AND psc_code = 'R425'
                      GROUP BY 1 HAVING SUM(prime_obl_24mo) >= 1e6)""").fetchone()[0]
    dt = time.time() - t0
    good = 300 <= n <= 400
    ok &= good
    print(f"acid test (subs $1-100M 24mo × primed 541330×R425 >=$1M 24mo): "
          f"{n} entities in {dt:.1f}s -> {'OK' if good else 'FAIL (expected ~347)'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
