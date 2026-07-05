"""DMU Email Finder results -> s3://data-sink/active/icypeas_dmu_work_emails/.

Custody for the 2026-07-05 Icypeas Email Finder UI bulk batch over the "dmu"
cohort: DSBS + facilities-proven-subawardee decision-makers/unknown-title people
who (a) have no gtm_sam_person_identity match (Profile URL Finder exhausted) and
(b) hold no strong-tier real-domain firm-email ruling. 30,494 people, 30,338
distinct finder payloads (exact-duplicate payloads collapsed at cut time).

Result items were drained VERBATIM from the Icypeas API
(/bulk-single-searchs/read, mode:"bulk") because the UI CSV export truncates at
~5.8k rows. Each item carries ``order`` = 1-based row position in the uploaded
CSV — the join back to people is POSITIONAL (order -> template row -> custody
people), immune to the echo-mutation hazards of name-string joins. Alignment is
verified on EVERY row (item firstname/lastname must equal the template row's).

Grain: 1 row per sam_person_id (email nullable — the dataset is also the
attempt ledger: DEBITED = found, DEBITED_NOT_FOUND = miss).

Run:
    LANCE_BYPASS_SPILLING=true doppler run -- python \
        pipelines/enrichment_icypeas/ingest_dmu_work_emails.py
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

import duckdb
import lance

URI = "s3://data-sink/active/icypeas_dmu_work_emails/"
DATA_STORAGE_VERSION = "2.1"
BATCH_LABEL = "dmu-email-finder-2026-07-05"

ITEMS_JSONL = os.environ.get(
    "DMU_EMAIL_ITEMS_JSONL",
    "/private/tmp/claude-501/-Users-benjamincrane-core-x--claude-worktrees-bold-mclaren-ee0237/"
    "b39d16d1-ecb6-449b-a6d2-777716ca17f3/scratchpad/dmu_email_finder_items.jsonl")
TEMPLATE_CSV = os.environ.get(
    "DMU_EMAIL_TEMPLATE_CSV", "/Users/benjamincrane/Desktop/dmu_email_finder_2026-07-05.csv")
CUSTODY_CSV = os.environ.get(
    "DMU_EMAIL_CUSTODY_CSV", "/Users/benjamincrane/Desktop/dmu_email_finder_custody_2026-07-05.csv")


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


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    now = datetime.now(timezone.utc).isoformat()
    items_sha = _sha256(ITEMS_JSONL)
    con = duckdb.connect()

    con.execute(f"""
        CREATE TABLE it AS
        SELECT "order" AS ord, status,
               results.firstname AS echo_fn, results.lastname AS echo_ln,
               results.emails[1].email      AS email,
               results.emails[1].certainty  AS email_certainty,
               results.emails[1].mxProvider AS mx_provider
        FROM read_json('{ITEMS_JSONL}', format='newline_delimited')""")
    n_it = con.execute("SELECT count(*) FROM it").fetchone()[0]

    con.execute(f"""
        CREATE TABLE tpl AS
        SELECT row_number() OVER () AS ord, "FIRSTNAME" fn, "LASTNAME" ln, "DOMAIN" AS k
        FROM read_csv('{TEMPLATE_CSV}', header=true, all_varchar=true)""")
    n_tpl = con.execute("SELECT count(*) FROM tpl").fetchone()[0]
    if n_it != n_tpl:
        raise RuntimeError(f"item/template count mismatch: {n_it} vs {n_tpl}")

    # positional alignment verified on every row (case-insensitive: Icypeas normalizes case)
    misaligned = con.execute("""
        SELECT count(*) FROM it JOIN tpl USING (ord)
        WHERE lower(trim(it.echo_fn)) <> lower(trim(tpl.fn))
           OR lower(trim(it.echo_ln)) <> lower(trim(tpl.ln))""").fetchone()[0]
    if misaligned:
        raise RuntimeError(f"positional alignment broken on {misaligned} rows")

    con.execute(f"""
        CREATE TABLE cust AS
        SELECT sam_person_id, uei, first_name, last_name, legal_business_name,
               nullif(trim(best_domain),'') AS best_domain,
               known_email, known_email_tier,
               coalesce(nullif(trim(best_domain),''), legal_business_name) AS k
        FROM read_csv('{CUSTODY_CSV}', header=true, all_varchar=true)""")
    n_cust = con.execute("SELECT count(*) FROM cust").fetchone()[0]

    con.execute("""
        CREATE TABLE out AS
        SELECT c.sam_person_id, c.uei, c.first_name, c.last_name,
               c.legal_business_name, c.best_domain,
               i.email, i.email_certainty, i.mx_provider, i.status AS finder_status,
               t.ord AS order_in_batch,
               c.known_email, c.known_email_tier
        FROM cust c
        JOIN tpl t ON t.fn = c.first_name AND t.ln = c.last_name AND t.k = c.k
        JOIN it i USING (ord)
        -- a few identical (fn,ln,k) payloads ran more than once (the cut's DISTINCT was over
        -- the 4-tuple incl. NULL-vs-filled domain); keep the best result per person
        QUALIFY row_number() OVER (PARTITION BY c.sam_person_id
            ORDER BY (i.email IS NOT NULL) DESC,
                     (i.email_certainty = 'ultra_sure') DESC, t.ord) = 1""")
    n_out, n_ppl, n_found = con.execute(
        "SELECT count(*), count(DISTINCT sam_person_id), count(email) FROM out").fetchone()
    if n_out != n_cust or n_ppl != n_cust:
        raise RuntimeError(f"custody fan mismatch: out={n_out} people={n_ppl} custody={n_cust}")

    con.execute(f"""
        CREATE TABLE final AS
        SELECT *, '{BATCH_LABEL}' AS batch_label,
               '{os.path.basename(ITEMS_JSONL)}' AS source_items_file,
               '{items_sha}' AS source_items_sha256,
               '{now}' AS materialized_at
        FROM out ORDER BY order_in_batch, sam_person_id""")
    print(f"people={n_out:,} found={n_found:,} ({n_found/n_out:.1%})")
    print(con.execute("SELECT email_certainty, count(*) FROM final WHERE email IS NOT NULL GROUP BY 1").fetchall())

    table = con.execute("SELECT * FROM final").to_arrow_table()
    so = _r2_storage_options()
    try:
        v_before = lance.dataset(URI, storage_options=so).version
    except Exception:  # noqa: BLE001 — first write
        v_before = None
    lance.write_dataset(table, URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    for col in ("sam_person_id", "uei"):
        ds.create_scalar_index(col, index_type="BTREE")
        print(f"  BTREE ✓ {col}")
    print(f"{URI} v{v_before} -> v{ds.version} rows={ds.count_rows():,}")


if __name__ == "__main__":
    main()
