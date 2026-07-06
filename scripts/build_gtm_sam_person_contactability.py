#!/usr/bin/env python3
"""gtm_sam_person_contactability — per-SAM-person contact assets, provider values verbatim.

SoR  s3://data-sink/active/gtm_sam_person_contactability/
     (Lance; derived, snapshot-overwrite; BTREE sam_person_id / uei)

WHAT THIS IS
One row per bridged SAM person (gtm_sam_person_identity population) answering: do we
already hold a mobile and/or a work email for this person, from which vendor, linked how.
Serving rollup for the Close loader and enrichment-queue logic — fully derivable from the
SoRs, rebuild anytime, never written to directly.

PROVIDER VALUES ARE VERBATIM. phone / email / statuses / MillionVerifier fields
(mv_resultcode, mv_result, mv_quality, mv_subresult) pass through untouched — no
normalization, no casing, no filtering. Audience-level filtering (e.g. verified-only,
exclude risky) is a COMPOSE-TIME decision made by the reader, never baked here.
The LinkedIn slug used to reconcile is a derived JOIN KEY only; it overwrites nothing.

LINKAGE (labeled per asset):
  linkedin_slug_direct       resolution row carried the person's LinkedIn URL
  linkedin_slug_via_people   resolution row reached via people-spine person_id → LinkedIn
Future enrichment runs stamped with subject sam_person_id at request time join as
'direct_stamped' when those columns land on the contact SoRs.

BEST-PICK (deterministic, alternatives never hidden): phones prefer phone_status='found'
then latest resolved_at; emails prefer verification_status 'verified' > 'risky' > other,
then email string as tiebreak. n_phones_matched / n_emails_matched carry the collapsed
row counts.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_sam_person_contactability.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
IDENTITY_URI = f"{A}/gtm_sam_person_identity/"
PHONES_URI = f"{A}/phone_resolutions/"
EMAILS_URI = f"{A}/work_emails/"
PEOPLE_URI = f"{A}/people/"
OUT = f"{A}/gtm_sam_person_contactability/"
PARAM_SET_ID = "v1"
BTREE_COLS = ["sam_person_id", "uei"]
# join-key derivation only — provider values are never rewritten with this
SLUG = "lower(regexp_extract({col}, 'in/([^/?#]+)', 1))"


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    ident = lance.dataset(IDENTITY_URI, storage_options=opt)
    con.register("_i", ident.scanner(columns=[
        "sam_person_id", "uei", "person_linkedin_url_norm", "match_source",
        "match_method", "match_score"]).to_reader())
    con.execute(f"""CREATE TABLE bridge AS
        SELECT *, {SLUG.format(col='person_linkedin_url_norm')} AS slug FROM _i""")

    people = lance.dataset(PEOPLE_URI, storage_options=opt)
    con.register("_p", people.scanner(columns=["person_id", "person_linkedin_url"]).to_reader())
    con.execute(f"""CREATE TABLE spine AS
        SELECT person_id, {SLUG.format(col='person_linkedin_url')} AS slug
        FROM _p WHERE person_linkedin_url IS NOT NULL""")

    phones = lance.dataset(PHONES_URI, storage_options=opt)
    con.register("_ph", phones.scanner(columns=[
        "person_id", "person_linkedin_url", "phone", "phone_status", "phone_type",
        "source_vendor", "batch_label", "resolved_at"]).to_reader())
    con.execute(f"""CREATE TABLE ph AS
        SELECT *, {SLUG.format(col='person_linkedin_url')} AS own_slug FROM _ph""")
    # slug per phone row: direct URL wins; else via people spine — basis labeled
    con.execute("""CREATE TABLE ph_linked AS
        SELECT p.phone, p.phone_status, p.phone_type, p.source_vendor, p.batch_label,
               p.resolved_at,
               COALESCE(NULLIF(p.own_slug, ''), s.slug) AS slug,
               CASE WHEN NULLIF(p.own_slug, '') IS NOT NULL THEN 'linkedin_slug_direct'
                    WHEN s.slug IS NOT NULL THEN 'linkedin_slug_via_people' END AS linkage
        FROM ph p LEFT JOIN spine s USING (person_id)
        WHERE COALESCE(NULLIF(p.own_slug, ''), s.slug) IS NOT NULL""")

    emails = lance.dataset(EMAILS_URI, storage_options=opt)
    con.register("_em", emails.scanner(columns=[
        "person_id", "email", "verification_status", "source_vendor",
        "mv_resultcode", "mv_result", "mv_quality", "mv_subresult"]).to_reader())
    con.execute("""CREATE TABLE em_linked AS
        SELECT e.email, e.verification_status, e.source_vendor,
               e.mv_resultcode, e.mv_result, e.mv_quality, e.mv_subresult,
               s.slug, 'linkedin_slug_via_people' AS linkage
        FROM _em e JOIN spine s USING (person_id)
        WHERE e.email IS NOT NULL AND s.slug != ''""")

    con.execute("""CREATE TABLE best_phone AS
        SELECT slug, phone, phone_status, phone_type, source_vendor, batch_label,
               resolved_at, linkage, n FROM (
          SELECT *, COUNT(*) OVER (PARTITION BY slug) AS n,
                 ROW_NUMBER() OVER (PARTITION BY slug ORDER BY
                   (phone_status = 'found') DESC, resolved_at DESC NULLS LAST, phone) AS rn
          FROM ph_linked) WHERE rn = 1""")
    con.execute("""CREATE TABLE best_email AS
        SELECT slug, email, verification_status, source_vendor,
               mv_resultcode, mv_result, mv_quality, mv_subresult, linkage, n FROM (
          SELECT *, COUNT(*) OVER (PARTITION BY slug) AS n,
                 ROW_NUMBER() OVER (PARTITION BY slug ORDER BY
                   CASE verification_status WHEN 'verified' THEN 0 WHEN 'risky' THEN 1
                        ELSE 2 END, email) AS rn
          FROM em_linked) WHERE rn = 1""")

    con.execute("""CREATE TABLE out AS
        SELECT b.sam_person_id, b.uei, b.person_linkedin_url_norm,
               b.match_source AS linkedin_match_source,
               b.match_method AS linkedin_match_method,
               b.match_score  AS linkedin_match_score,
               bp.phone, bp.phone_status, bp.phone_type,
               bp.source_vendor AS phone_source_vendor,
               bp.batch_label   AS phone_batch_label,
               bp.resolved_at   AS phone_resolved_at,
               bp.linkage       AS phone_linkage_basis,
               COALESCE(bp.n, 0) AS n_phones_matched,
               be.email, be.verification_status AS email_verification_status,
               be.source_vendor AS email_source_vendor,
               be.mv_resultcode, be.mv_result, be.mv_quality, be.mv_subresult,
               be.linkage       AS email_linkage_basis,
               COALESCE(be.n, 0) AS n_emails_matched
        FROM bridge b
        LEFT JOIN best_phone bp USING (slug)
        LEFT JOIN best_email be USING (slug)""")

    n = con.execute("SELECT COUNT(*) FROM out").fetchone()[0]
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT sam_person_id FROM out GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"
    stats = con.execute("""SELECT
        COUNT(*) FILTER (phone IS NOT NULL AND phone_status = 'found'),
        COUNT(*) FILTER (email IS NOT NULL),
        COUNT(*) FILTER (email_verification_status = 'verified')
        FROM out""").fetchone()
    print(f"rows: {n:,}  mobile_found: {stats[0]:,}  email: {stats[1]:,}  "
          f"email_verified: {stats[2]:,}", flush=True)

    built_from = (f"gtm_sam_person_identity:v{ident.version}|phone_resolutions:v{phones.version}|"
                  f"work_emails:v{emails.version}|people:v{people.version}")
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM out""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE_COLS],
                               storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    rows = ds.count_rows()
    idx = [i["name"] for i in ds.list_indices()]
    mob = ds.count_rows(filter="phone IS NOT NULL AND phone_status = 'found'")
    em = ds.count_rows(filter="email IS NOT NULL")
    ok = rows > 100_000 and mob > 5_000 and em > 5_000 and all(
        any(c in i2 for i2 in idx) for c in BTREE_COLS)
    print(f"{OUT}: rows={rows:,} mobile_found={mob:,} email={em:,} indices={idx} "
          f"-> {'OK' if ok else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
