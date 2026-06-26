"""SAM.gov 90-day attachment — GTM SCOPE RESOLVER ("Strained Middle" de-contamination gate).

Verdicts every downloaded attachment (sam_attachment_files) as in_scope / out_of_scope for the
mid-market, labor/capital-intensive GTM cohort, and materializes a tiny per-resource scope table that
the extraction engine (sam_attachment_extract.py Phase 1) consults to skip out-of-scope files
BEFORE spending parse/chunk/embed compute on them.

WHY a separate resolver (not inline in the extractor): the GTM business logic — the NAICS target set,
the dollar band, the frequency cap — is a *lens*, not infrastructure. Keeping it here means the cohort
can be re-pointed (new verticals, new thresholds) by rebuilding ONE small table, with zero edits to the
parser, and the same scope verdict is reusable by the Phase-4 embedding pass and any downstream audience
query. (Separation of concerns: the lake stays universal; the lens is swappable.)

THE BRIDGE (3 datasets, read-only):
  usaspending_api_fresh/contract_prime_txn   prime AWARD substrate (transaction grain, ~90d last_modified)
        └─ contract_award_unique_key ──┐     resolved to award grain (latest last_modified per award key)
  sam_opps_attachment_manifest_winners │  resource_id ⇄ contract_award_unique_key / award_keys[]
        └─ resource_id ────────────────┘
  sam_attachment_files                 the downloaded-resource universe to verdict (status='downloaded')

THE RULES (defended against the source directive):
  A  band   : current_total_value_of_award ∈ [$500k, $15M]  (the "Award Amount" — committed value, the
              column proven 100%-populated and uncorrupted; NOT federal_action_obligation, a signed txn delta).
  B  naics  : 6-digit AWARD naics ∈ TARGET_NAICS (the velocity-analysis target codes — applied to the
              AWARD naics from usaspending, NOT the solicitation naics on the SAM ledger, which drifts).
  C  freq   : entities with > FREQ_CAP in-band (A∧B) awards over the window are dropped wholesale
              (failed_frequency_cap) — this is the implementable de-contamination lever that drops the
              high-frequency distributors/OEMs. The directive's "wholesale/dealer/distributor/manufacturer
              flag" exclusion is NOT implementable from FPDS (no clean distributor flag; manufacturer_of_goods
              would wrongly nuke legit fabrication shops) and is deliberately omitted.

OUTPUT  s3://data-sink/active/sam_attachment_gtm_scope/ (Lance v2.1):
  resource_id, gtm_scope ∈ {in_scope, out_of_scope}, scope_reason ∈ {in_scope, out_of_scope_naics,
  below_band, above_band, failed_frequency_cap, no_award_link}, matched_award_key, recipient_uei,
  matched_naics, matched_amount, run_id, created_at.  BTREE(resource_id) · BITMAP(gtm_scope, scope_reason).
  Overwrite-snapshot (idempotent full rebuild; deterministic). Ledger: ops.sam_attachment_gtm_scope_runs.

Run (read-only except the one additive output dataset):
    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with duckdb --with 'psycopg[binary]' \
      python pipelines/sam_gov/sam_attachment_gtm_scope.py            # build + index + ledger
    ... --dry-run                                                           # funnel only, no write
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

# ── inputs (read-only) + output ───────────────────────────────────────────────────────────────────
FRESH_URI = os.environ.get(
    "USASPENDING_API_FRESH_URI", "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/")
MANIFEST_URI = os.environ.get(
    "SAM_MANIFEST_URI", "s3://data-sink/active/sam_opps_attachment_manifest_winners/")
FILES_LEDGER_URI = os.environ.get(
    "SAM_FILES_URI", "s3://data-sink/active/sam_attachment_files/")
SCOPE_GATE_URI = os.environ.get(
    "SAM_SCOPE_GATE_URI", "s3://data-sink/active/sam_attachment_gtm_scope/")

FEED = "sam_attachment_gtm_scope"

# ── gate config (Rule A/B/C — the swappable lens) ──────────────────────────────────────────────────
BAND_LO = int(os.environ.get("GTM_BAND_LO", "500000"))
BAND_HI = int(os.environ.get("GTM_BAND_HI", "15000000"))
FREQ_CAP = int(os.environ.get("GTM_FREQ_CAP", "30"))
TARGET_NAICS = [
    "236220",                                  # Commercial & Institutional Building
    "238210", "238220", "238290", "238990",    # Specialty Trade: Electrical, HVAC/Plumbing, Equipment, Other
    "237120", "237990",                        # Heavy-Civil mobilization: Pipeline, Other heavy-civil
    "562112", "562211", "562910",              # Environmental: Hazardous-Waste Collection, Treatment/Disposal, Remediation
]


# ── R2 ──────────────────────────────────────────────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def _scope_schema():
    import pyarrow as pa
    return pa.schema([
        ("resource_id", pa.string()), ("gtm_scope", pa.string()), ("scope_reason", pa.string()),
        ("matched_award_key", pa.string()), ("recipient_uei", pa.string()),
        ("matched_naics", pa.string()), ("matched_amount", pa.float64()),
        ("run_id", pa.string()), ("created_at", pa.timestamp("us", tz="UTC")),
    ])


# ── core resolve ──────────────────────────────────────────────────────────────────────────────────
def _resolve(so: dict):
    """Read the three substrates, score awards (A/B/C), bridge to resources, and return
    (scope_arrow_table, metrics_dict). Pure read; no write."""
    import duckdb
    import lance
    import pyarrow as pa

    fresh = lance.dataset(FRESH_URI, storage_options=so).to_table(
        columns=["contract_award_unique_key", "recipient_uei", "current_total_value_of_award",
                 "naics_code", "last_modified_date", "award_or_idv_flag"])
    man_ds = lance.dataset(MANIFEST_URI, storage_options=so)
    ak_field = man_ds.schema.field("award_keys").type
    ak_is_list = pa.types.is_list(ak_field) or pa.types.is_large_list(ak_field)
    manifest = man_ds.to_table(columns=["resource_id", "contract_award_unique_key", "award_keys"])
    files = lance.dataset(FILES_LEDGER_URI, storage_options=so).to_table(columns=["resource_id", "status"])

    con = duckdb.connect()
    con.execute("PRAGMA threads=4;")
    con.register("fresh", fresh)
    con.register("manifest", manifest)
    con.register("files", files)

    codes = "(" + ",".join(f"'{c}'" for c in TARGET_NAICS) + ")"

    # resource_id -> award_key map. Prefer the complete award_keys[] set when present (a notice can host
    # attachments for several awards); always union the singular primary key. UNNEST only when list-typed.
    if ak_is_list:
        map_sql = f"""
          SELECT DISTINCT resource_id, contract_award_unique_key AS award_key
            FROM manifest WHERE contract_award_unique_key IS NOT NULL AND contract_award_unique_key <> ''
          UNION
          SELECT DISTINCT resource_id, UNNEST(award_keys) AS award_key
            FROM manifest WHERE award_keys IS NOT NULL
        """
    else:
        map_sql = """
          SELECT DISTINCT resource_id, contract_award_unique_key AS award_key
            FROM manifest WHERE contract_award_unique_key IS NOT NULL AND contract_award_unique_key <> ''
        """
    con.execute(f"CREATE TEMP TABLE res_award_map AS {map_sql}")

    con.execute(f"""
        CREATE TEMP TABLE award_verdict AS
        WITH awards AS (   -- transaction grain -> 1 row/award, latest last_modified wins; AWARD only (no IDV vehicles)
          SELECT contract_award_unique_key AS award_key, recipient_uei,
                 TRY_CAST(current_total_value_of_award AS DOUBLE) AS amount, naics_code AS award_naics
          FROM fresh WHERE award_or_idv_flag = 'AWARD'
          QUALIFY row_number() OVER (PARTITION BY contract_award_unique_key
                                     ORDER BY last_modified_date DESC) = 1
        ),
        flagged AS (
          SELECT award_key, recipient_uei, amount, award_naics,
                 (award_naics IN {codes}) AS pass_naics,
                 (amount IS NOT NULL AND amount >= {BAND_LO} AND amount <= {BAND_HI}) AS pass_band,
                 (amount IS NOT NULL AND amount <  {BAND_LO}) AS below,
                 (amount IS NOT NULL AND amount >  {BAND_HI}) AS above
          FROM awards
        ),
        freq AS (   -- Rule C base: in-band (A∧B) award count per entity
          SELECT recipient_uei, count(*) AS in_band_awards
          FROM flagged WHERE pass_naics AND pass_band GROUP BY recipient_uei
        )
        SELECT f.award_key, f.recipient_uei, f.amount, f.award_naics,
               CASE
                 WHEN NOT f.pass_naics                          THEN 'out_of_scope_naics'
                 WHEN f.below                                   THEN 'below_band'
                 WHEN f.above                                   THEN 'above_band'
                 WHEN coalesce(fr.in_band_awards, 0) > {FREQ_CAP} THEN 'failed_frequency_cap'
                 ELSE 'in_scope'
               END AS award_reason
        FROM flagged f LEFT JOIN freq fr USING (recipient_uei)
    """)

    scope = con.execute("""
        WITH res_award AS (
          SELECT m.resource_id, v.award_reason, v.award_key, v.amount, v.award_naics, v.recipient_uei,
                 CASE v.award_reason WHEN 'in_scope' THEN 0 WHEN 'failed_frequency_cap' THEN 1
                      WHEN 'below_band' THEN 2 WHEN 'above_band' THEN 2
                      WHEN 'out_of_scope_naics' THEN 3 ELSE 4 END AS rk
          FROM res_award_map m JOIN award_verdict v ON m.award_key = v.award_key
        ),
        res_best AS (   -- a resource is in_scope if ANY linked award is in_scope; else the closest-to-passing reason
          SELECT resource_id, award_reason, award_key, amount, award_naics, recipient_uei
          FROM res_award
          QUALIFY row_number() OVER (PARTITION BY resource_id ORDER BY rk, amount DESC NULLS LAST) = 1
        ),
        universe AS (SELECT DISTINCT resource_id FROM files WHERE status = 'downloaded')
        SELECT u.resource_id,
               CASE WHEN rb.award_reason = 'in_scope' THEN 'in_scope' ELSE 'out_of_scope' END AS gtm_scope,
               COALESCE(rb.award_reason, 'no_award_link') AS scope_reason,
               rb.award_key      AS matched_award_key,
               rb.recipient_uei  AS recipient_uei,
               rb.award_naics    AS matched_naics,
               rb.amount         AS matched_amount
        FROM universe u LEFT JOIN res_best rb ON u.resource_id = rb.resource_id
    """).to_arrow_table()

    metrics = {
        "universe_files": scope.num_rows,
        "cohort_in_band_awards": con.execute(
            "SELECT count(*) FROM award_verdict WHERE award_reason='in_scope'").fetchone()[0]
        + con.execute("SELECT count(*) FROM award_verdict WHERE award_reason='failed_frequency_cap'").fetchone()[0],
        "capped_entities": con.execute(
            f"""SELECT count(*) FROM (SELECT recipient_uei FROM award_verdict
                WHERE award_reason <> 'out_of_scope_naics' GROUP BY recipient_uei) x
                WHERE recipient_uei IN (SELECT recipient_uei FROM award_verdict
                WHERE award_reason='failed_frequency_cap')""").fetchone()[0],
    }
    con.register("scope_t", scope)
    by_reason = dict(con.execute("SELECT scope_reason, count(*) FROM scope_t GROUP BY 1").fetchall())
    in_scope_n = con.execute("SELECT count(*) FROM scope_t WHERE gtm_scope='in_scope'").fetchone()[0]
    metrics.update({"in_scope": in_scope_n, "out_of_scope": scope.num_rows - in_scope_n,
                    "by_reason": by_reason})
    con.close()
    return scope, metrics


# ── ops ledger ────────────────────────────────────────────────────────────────────────────────────
def _ops_ddl() -> str:
    from pathlib import Path
    return Path(__file__).with_name("ops_sam_attachment_gtm_scope_runs.sql").read_text()


def _record_run(metrics: dict, run_id: str, status: str, error: str | None,
                started_at, completed_at, dsn: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    r = metrics.get("by_reason", {})
    row = {
        "run_id": run_id, "universe_files": metrics.get("universe_files", 0),
        "in_scope": metrics.get("in_scope", 0), "out_of_scope": metrics.get("out_of_scope", 0),
        "r_out_of_scope_naics": r.get("out_of_scope_naics", 0), "r_below_band": r.get("below_band", 0),
        "r_above_band": r.get("above_band", 0), "r_failed_frequency_cap": r.get("failed_frequency_cap", 0),
        "r_no_award_link": r.get("no_award_link", 0),
        "cohort_in_band_awards": metrics.get("cohort_in_band_awards", 0),
        "capped_entities": metrics.get("capped_entities", 0),
        "band_lo": BAND_LO, "band_hi": BAND_HI, "freq_cap": FREQ_CAP,
        "target_naics": TARGET_NAICS, "status": status, "error": (error or "")[:2000] or None,
        "started_at": started_at, "completed_at": completed_at,
    }
    try:
        import psycopg
        from psycopg.types.json import Json
        row["target_naics"] = Json(row["target_naics"])
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(_ops_ddl())
            cur.execute("""
                INSERT INTO ops.sam_attachment_gtm_scope_runs
                  (run_id, universe_files, in_scope, out_of_scope, r_out_of_scope_naics, r_below_band,
                   r_above_band, r_failed_frequency_cap, r_no_award_link, cohort_in_band_awards,
                   capped_entities, band_lo, band_hi, freq_cap, target_naics, status, error,
                   started_at, completed_at)
                VALUES (%(run_id)s,%(universe_files)s,%(in_scope)s,%(out_of_scope)s,%(r_out_of_scope_naics)s,
                   %(r_below_band)s,%(r_above_band)s,%(r_failed_frequency_cap)s,%(r_no_award_link)s,
                   %(cohort_in_band_awards)s,%(capped_entities)s,%(band_lo)s,%(band_hi)s,%(freq_cap)s,
                   %(target_naics)s,%(status)s,%(error)s,%(started_at)s,%(completed_at)s)
                """, row)
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the resolve
        print(f"WARN: ops.* write failed: {exc}", flush=True)


# ── build ─────────────────────────────────────────────────────────────────────────────────────────
def build(*, so: dict, run_id: str, dsn: str | None, dry_run: bool) -> dict:
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    status, error = "error", None
    metrics: dict = {}
    try:
        scope, metrics = _resolve(so)
        br = metrics["by_reason"]
        print(f"[{FEED}] universe={metrics['universe_files']:,}  in_scope={metrics['in_scope']:,}  "
              f"out_of_scope={metrics['out_of_scope']:,}", flush=True)
        print(f"  cohort_in_band_awards={metrics['cohort_in_band_awards']:,}  "
              f"capped_entities={metrics['capped_entities']:,}", flush=True)
        for k in ("in_scope", "failed_frequency_cap", "below_band", "above_band",
                  "out_of_scope_naics", "no_award_link"):
            print(f"    {k:22} {br.get(k, 0):>9,}", flush=True)

        if dry_run:
            print("DRY-RUN: no dataset written.", flush=True)
            status = "success"
            return metrics

        import pyarrow as pa
        now = dt.datetime.now(dt.timezone.utc)
        n = scope.num_rows
        out = scope.append_column("run_id", pa.array([run_id] * n, pa.string())) \
                   .append_column("created_at", pa.array([now] * n, pa.timestamp("us", tz="UTC")))
        out = out.cast(_scope_schema())
        lance.write_dataset(out, SCOPE_GATE_URI, mode="overwrite",
                            data_storage_version="2.1", storage_options=so)
        ds = lance.dataset(SCOPE_GATE_URI, storage_options=so)
        for col, typ in [("resource_id", "BTREE"), ("gtm_scope", "BITMAP"), ("scope_reason", "BITMAP")]:
            ds.create_scalar_index(col, index_type=typ)
            print(f"  index {typ:6} {col}", flush=True)
        print(f"wrote {n:,} rows -> {SCOPE_GATE_URI}", flush=True)
        status = "success"
        return metrics
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        status = "error"
        raise
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        if not dry_run:
            _record_run(metrics, run_id, status, error, started, completed, dsn)


def _cli() -> None:
    p = argparse.ArgumentParser(description="SAM.gov 90-day attachment GTM scope resolver (Strained Middle gate).")
    p.add_argument("--dry-run", action="store_true", help="compute + print the funnel; write nothing")
    p.add_argument("--run-id", default=None)
    a = p.parse_args()

    so = _r2_storage_options()
    for name, uri in [("fresh", FRESH_URI), ("manifest", MANIFEST_URI), ("files", FILES_LEDGER_URI)]:
        if not _dataset_exists(uri, so):
            print(f"FATAL: input {name} missing: {uri}", flush=True)
            sys.exit(2)
    run_id = a.run_id or f"gtm-scope-{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}"
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    build(so=so, run_id=run_id, dsn=dsn, dry_run=a.dry_run)


if __name__ == "__main__":
    _cli()
