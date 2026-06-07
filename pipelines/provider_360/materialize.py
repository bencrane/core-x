"""Compute worker — provider_360 serving layer (+ Tier-1 giant rollups + practice_group_360).

Modal app ``provider-360-pipelines``. Builds the deterministic, 1-row-per-NPI entity-360 serving
layer over the landed CMS/NPPES graph, united ONLY on published keys (npi, ENRLMT_ID). No fuzzy
resolution, no corporate-identity columns. Implements docs/plans/provider_360_materialization_plan.md
(v2 — adversarial-review-applied).

Two tiers (plan §2):
  TIER 1 — per-NPI rollups of the giants (each a standalone active/ leaf dataset, 1 row/NPI):
    cms_physician_service_rollup  ← A2 cms_physician_provider_service (78.5M → ~1.1M)
    cms_partd_drug_rollup         ← B2 cms_partd_provider_drug      (304M → ~1.3M)
    cms_dme_supplier_rollup       ← C3 cms_dme_supplier_service     (5.5M → ~99k)
    (cms_provider_payment_rollup already exists — Open Payments.)
  TIER 2 — active/provider_360/snapshot=YYYY-MM/ : base nppes_provider LEFT JOIN inline A1/B1/
    taxonomy/identifier aggregates + the Tier-1 rollups + QPP + the ENRLMT_ID practice graph.
  CAPSTONE — active/practice_group_360/snapshot=YYYY-MM/ : 1 row per buyable group (rcv ENRLMT_ID),
    rolling member provider_360 economics up to the group (plan §12).

v2 remediations baked in (all live-validated, plan §13):
  * QPP collapsed via a mandatory GROUP BY npi pre-agg (86,952 npi-year dups, ≤15× — would fan the base);
    participation_type is 100% NULL post-2022 → coalesce(participation_option, reporting_option).
  * A2/C3 money: line totals in DOUBLE, final per-NPI scalar cast to DECIMAL(18,2) (avg×srvcs overflows
    DECIMAL and NULLs the biggest providers); *_lines_suppressed counters surface the NULL-line undercount.
  * Practice: BOTH poles (smallest/largest fan-in group) + is_independent_candidate — "largest fan-in"
    alone assigns a solo doc to the hospital they're cross-credentialed at (inverts the PE signal).
  * Panel economics added from A1 (bene_avg_risk_scre / dual-share / chronic-disease %).
  * Redundant cms_ownership re-agg cut (the Open Payments rollup already carries ownership).

Reuses: pipelines/cms_medicare/ingest.py (_publish_full_swap, _verify_published, _create-index FATAL-btree),
pipelines/nppes/materialize_analytical.py (ORDER BY npi clustering, per-snapshot serving layer + gates).

    modal run pipelines/provider_360/materialize.py::init_state
    modal run pipelines/provider_360/materialize.py::rollup --which service   # service|drug|dme|all
    modal run pipelines/provider_360/materialize.py::build_360 --snapshot-month 2026-06
    modal run pipelines/provider_360/materialize.py::build_groups --snapshot-month 2026-06
    modal run pipelines/provider_360/materialize.py::verify --dataset provider_360 --snapshot-month 2026-06
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
ACTIVE = "active/"
FEED = "provider_360"
NPPES_SNAPSHOT = os.environ.get("NPPES_SNAPSHOT", "2026-05")
DEFAULT_SNAPSHOT = os.environ.get("PROVIDER_360_SNAPSHOT", "2026-06")

SCRATCH = "/tmp/provider_360"
SPILL = os.path.join(SCRATCH, "spill")
LOCAL = os.path.join(SCRATCH, "lance")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
READ_BATCH = 1 << 20

_R2_PUBLISH_ATTEMPTS = 4
_R2_RETRY_BACKOFF_S = 8
_R2_SINGLE_COPY_MAX = 5 * 1024**3

BASE_NPPES = f"nppes_provider/snapshot={NPPES_SNAPSHOT}"
BASE_ROWS_EXPECTED = 9_551_447

# ── Tier-1 rollup catalog ──
ROLLUPS = {
    "service": {"out": "cms_physician_service_rollup", "src": "cms_physician_provider_service",
                "npi_src": "rndrng_npi", "btree": ["npi"], "bitmap": []},
    "drug":    {"out": "cms_partd_drug_rollup", "src": "cms_partd_provider_drug",
                "npi_src": "prscrbr_npi", "btree": ["npi"], "bitmap": []},
    "dme":     {"out": "cms_dme_supplier_rollup", "src": "cms_dme_supplier_service",
                "npi_src": "suplr_npi", "btree": ["npi"], "bitmap": ["is_dme_supplier"]},
}

PROVIDER_360_BTREE = ["npi", "practice_zip5", "last_name", "smallest_practice_group_enrlmt_id"]
PROVIDER_360_BITMAP = ["entity_type_code", "is_active", "practice_state", "primary_taxonomy_code",
                       "med_a1_provider_type", "med_a1_latest_year", "mips_final_score_year",
                       "is_independent_candidate"]
GROUP_360_BTREE = ["group_enrlmt_id", "org_name"]
GROUP_360_BITMAP = ["group_state"]

DUCKDB_MEM = os.environ.get("PROVIDER_360_MEM", "24GB")

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.provider_360_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feed text NOT NULL DEFAULT 'provider_360',
  artifact text NOT NULL, snapshot_month text,
  dataset_uri text, rows_written bigint, indices text,
  attach_rates text, gates text,
  status text NOT NULL, error text,
  started_at timestamptz, completed_at timestamptz,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS provider_360_runs_uq ON ops.provider_360_runs (artifact, snapshot_month);
CREATE INDEX IF NOT EXISTS provider_360_runs_recorded_idx ON ops.provider_360_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "boto3>=1.35", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("provider-360-pipelines", image=image)


# ─────────────────────────────── R2 / publish / verify (reused) ───────────────────────────────
def _so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def _s3():
    import boto3
    from botocore.config import Config
    o = _so()
    return boto3.client("s3", endpoint_url=o["endpoint"], aws_access_key_id=o["aws_access_key_id"],
                        aws_secret_access_key=o["aws_secret_access_key"], region_name="auto",
                        config=Config(signature_version="s3v4", request_checksum_calculation="when_required",
                                      response_checksum_validation="when_required", retries={"max_attempts": 5, "mode": "standard"}))


def _staging_prefix(p): return p.rstrip("/") + "__staging/"
def _rank(rel): return 4 if rel.startswith("_versions/") else 3 if rel.startswith("_transactions/") else 2 if rel.startswith("_indices/") else 1
def _ordered(rels): return sorted(rels, key=lambda r: (_rank(r), r))


def _prefix_sizes(s3, prefix):
    out = {}
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in pg.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if rel:
                out[rel] = o["Size"]
    return out


def _local_sizes(d):
    out = {}
    for root, _, files in os.walk(d):
        for fn in files:
            lp = os.path.join(root, fn)
            out[os.path.relpath(lp, d).replace(os.sep, "/")] = os.path.getsize(lp)
    return out


def _upload_retry(s3, lp, key):
    import time
    last = None
    for a in range(1, _R2_PUBLISH_ATTEMPTS + 1):
        try:
            s3.upload_file(lp, BUCKET, key); return
        except Exception as e:  # noqa: BLE001
            last = e; print(f"    upload {key} attempt {a} failed: {str(e)[:160]}")
            if a < _R2_PUBLISH_ATTEMPTS: time.sleep(_R2_RETRY_BACKOFF_S * a)
    raise RuntimeError(f"upload failed: {key}: {last}")


def _copy_retry(s3, src, dst):
    import time
    last = None
    for a in range(1, _R2_PUBLISH_ATTEMPTS + 1):
        try:
            s3.copy_object(Bucket=BUCKET, Key=dst, CopySource={"Bucket": BUCKET, "Key": src}); return
        except Exception as e:  # noqa: BLE001
            last = e
            if a < _R2_PUBLISH_ATTEMPTS: time.sleep(_R2_RETRY_BACKOFF_S * a)
    raise RuntimeError(f"copy failed: {src}->{dst}: {last}")


def _del_keys(s3, keys):
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})


def _del_prefix(s3, prefix):
    keys = []
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in pg.get("Contents", []):
            keys.append(o["Key"])
    if keys: _del_keys(s3, keys)
    return len(keys)


def _publish_full_swap(s3, live_prefix, local_dir):
    """stage → verify staging==local (live untouched) → swap → verify live (cms_medicare pattern)."""
    local = _local_sizes(local_dir)
    if not local: raise RuntimeError(f"refusing empty publish to {live_prefix}")
    staging = _staging_prefix(live_prefix)
    ordered = _ordered(local)
    sb = _prefix_sizes(s3, staging)
    for rel in ordered:
        if sb.get(rel) != local[rel]:
            _upload_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), staging + rel)
    stale = [staging + rel for rel in sb if rel not in local]
    if stale: _del_keys(s3, stale)
    sa = _prefix_sizes(s3, staging)
    if sa != local:
        raise RuntimeError(f"staging verify failed {staging}: LIVE INTACT. {sum(1 for r in local if sa.get(r)!=local[r])} mismatched")
    _del_prefix(s3, live_prefix)
    for rel in ordered:
        if local[rel] > _R2_SINGLE_COPY_MAX:
            _upload_retry(s3, os.path.join(local_dir, rel.replace("/", os.sep)), live_prefix + rel)
        else:
            _copy_retry(s3, staging + rel, live_prefix + rel)
    la = _prefix_sizes(s3, live_prefix)
    if la != local:
        raise RuntimeError(f"live verify failed {live_prefix}: re-run. {sum(1 for r in local if la.get(r)!=local[r])} mismatched")
    try: _del_prefix(s3, staging)
    except Exception: pass  # noqa: BLE001
    print(f"  published {len(local)} files → s3://{BUCKET}/{live_prefix}")
    return len(local)


def _list_indices(ds):
    out = []
    for ix in ds.list_indices():
        d = dict(ix) if isinstance(ix, dict) else {"name": getattr(ix, "name", None),
            "type": str(getattr(ix, "type", None)), "fields": getattr(ix, "fields", None)}
        out.append(d)
    return out


def _verify_published(uri, expected_rows, expected_indices, probe_col, so):
    import lance
    ds = lance.dataset(uri, storage_options=so)
    rows = ds.count_rows()
    if rows != expected_rows:
        raise RuntimeError(f"verify {uri}: rows={rows:,} != expected {expected_rows:,}")
    nidx = len(_list_indices(ds))
    if expected_indices is not None and nidx != expected_indices:
        raise RuntimeError(f"verify {uri}: indices={nidx} != {expected_indices}")
    if probe_col:
        n = ds.scanner(filter=f"{probe_col} IS NOT NULL", columns=[probe_col], limit=1, prefilter=True).to_table().num_rows
        if n < 1: raise RuntimeError(f"verify {uri}: probe {probe_col} 0 rows")
    return {"rows": rows, "indices": nidx}


def _new_con(mem: str = DUCKDB_MEM):
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 8")
    con.execute(f"SET memory_limit='{mem}'")
    os.makedirs(SPILL, exist_ok=True)
    con.execute(f"SET temp_directory='{SPILL}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _reg(con, name, path, cols, so):
    import lance
    ds = lance.dataset(f"s3://{BUCKET}/{ACTIVE}{path}/", storage_options=so)
    con.register(f"_r_{name}", ds.scanner(columns=cols).to_reader())
    con.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM _r_{name}")
    con.unregister(f"_r_{name}")
    return con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]


def _write_sorted(con, sql, local_ds, sort_by):
    import lance
    import shutil
    shutil.rmtree(local_ds, ignore_errors=True)
    reader = con.execute(f"SELECT * FROM ({sql}) ORDER BY {sort_by}").to_arrow_reader(READ_BATCH)
    lance.write_dataset(reader, local_ds, schema=reader.schema, mode="create",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE)
    return lance.dataset(local_ds).count_rows()


def _create_indexes(local_ds, btree, bitmap):
    import lance
    ds = lance.dataset(local_ds)
    built = []
    for col in btree:  # resolution keys — FATAL if they fail (abort before publish)
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"BTREE:{col}"); print(f"  BTREE  ✓ {col}")
    for col in bitmap:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}"); print(f"  BITMAP ✓ {col}")
        except Exception as e:  # noqa: BLE001
            print(f"  WARN BITMAP {col}: {e}")
    return built


# ─────────────────────────────── Tier-1 rollup SQL ───────────────────────────────
def _service_sql():  # A2 — money in DOUBLE, final scalar DECIMAL(18,2); RBCS top category
    return """
    WITH base AS (
      SELECT npi,
        CAST(sum(CAST(avg_mdcr_pymt_amt AS DOUBLE) * tot_srvcs) AS DECIMAL(18,2)) AS svc_total_medicare_paid_usd,
        count(*) FILTER (WHERE avg_mdcr_pymt_amt IS NULL OR tot_srvcs IS NULL) AS svc_lines_suppressed,
        sum(tot_srvcs) AS svc_total_services,
        sum(tot_benes) AS svc_total_bene_lines,
        count(DISTINCT hcpcs_cd) AS svc_distinct_hcpcs,
        count(DISTINCT program_year) AS svc_active_years,
        min(program_year) AS svc_first_year, max(program_year) AS svc_last_year,
        CAST(sum(CASE WHEN hcpcs_drug_ind='Y' THEN CAST(avg_mdcr_pymt_amt AS DOUBLE)*tot_srvcs END) AS DECIMAL(18,2)) AS svc_partb_drug_paid_usd,
        coalesce(bool_or(hcpcs_drug_ind='Y'), false) AS has_partb_administered_drugs,
        100.0 * (sum(CAST(avg_mdcr_pymt_amt AS DOUBLE)*tot_srvcs) FILTER (WHERE program_year=2024)
          - sum(CAST(avg_mdcr_pymt_amt AS DOUBLE)*tot_srvcs) FILTER (WHERE program_year=2023))
          / nullif(sum(CAST(avg_mdcr_pymt_amt AS DOUBLE)*tot_srvcs) FILTER (WHERE program_year=2023), 0) AS svc_paid_yoy_pct
      FROM a2 GROUP BY npi),
    catline AS (
      SELECT a.npi, r.rbcs_cat AS cat, sum(CAST(a.avg_mdcr_pymt_amt AS DOUBLE)*a.tot_srvcs) AS paid
      FROM a2 a LEFT JOIN rbcs r ON r.hcpcs_cd = a.hcpcs_cd GROUP BY 1,2),
    topcat AS (
      SELECT npi, arg_max(cat, paid) AS svc_top_rbcs_cat, CAST(max(paid) AS DECIMAL(18,2)) AS svc_top_rbcs_cat_paid_usd
      FROM catline WHERE cat IS NOT NULL GROUP BY npi)
    SELECT b.*, t.svc_top_rbcs_cat, t.svc_top_rbcs_cat_paid_usd
    FROM base b LEFT JOIN topcat t USING (npi)
    """


def _drug_sql():  # B2 — totals already (DECIMAL); top generics by cost
    return """
    WITH base AS (
      SELECT npi,
        CAST(sum(CAST(tot_drug_cst AS DOUBLE)) AS DECIMAL(18,2)) AS rx_total_drug_cost_usd,
        sum(tot_clms) AS rx_total_claims, sum(tot_30day_fills) AS rx_total_30day_fills, sum(tot_day_suply) AS rx_total_day_supply,
        count(DISTINCT gnrc_name) AS rx_distinct_generics, count(DISTINCT brnd_name) AS rx_distinct_brands,
        count(DISTINCT program_year) AS rx_active_years, min(program_year) AS rx_first_year, max(program_year) AS rx_last_year,
        100.0 * (sum(CAST(tot_drug_cst AS DOUBLE)) FILTER (WHERE program_year=2024)
          - sum(CAST(tot_drug_cst AS DOUBLE)) FILTER (WHERE program_year=2023))
          / nullif(sum(CAST(tot_drug_cst AS DOUBLE)) FILTER (WHERE program_year=2023), 0) AS rx_cost_yoy_pct
      FROM b2 GROUP BY npi),
    g AS (SELECT npi, gnrc_name, sum(CAST(tot_drug_cst AS DOUBLE)) AS cost FROM b2 WHERE gnrc_name IS NOT NULL GROUP BY 1,2),
    top AS (
      SELECT npi, arg_max(gnrc_name, cost) AS rx_top1_generic, CAST(max(cost) AS DECIMAL(18,2)) AS rx_top1_generic_cost_usd,
        to_json((list(gnrc_name ORDER BY cost DESC))[1:3]) AS rx_top3_generics
      FROM g GROUP BY npi)
    SELECT b.*, t.rx_top1_generic, t.rx_top1_generic_cost_usd, t.rx_top3_generics
    FROM base b LEFT JOIN top t USING (npi)
    """


def _dme_sql():  # C3
    return """
    SELECT npi,
      CAST(sum(CAST(avg_suplr_mdcr_pymt_amt AS DOUBLE) * tot_suplr_srvcs) AS DECIMAL(18,2)) AS dme_supplied_total_paid_usd,
      count(*) FILTER (WHERE avg_suplr_mdcr_pymt_amt IS NULL OR tot_suplr_srvcs IS NULL) AS dme_lines_suppressed,
      sum(tot_suplr_clms) AS dme_supplied_claims,
      count(DISTINCT hcpcs_cd) AS dme_distinct_hcpcs,
      CAST(avg(CASE WHEN suplr_rentl_ind='Y' THEN 1.0 ELSE 0.0 END) AS DOUBLE) AS dme_rental_share,
      100.0 * (sum(CAST(avg_suplr_mdcr_pymt_amt AS DOUBLE)*tot_suplr_srvcs) FILTER (WHERE program_year=2023)
        - sum(CAST(avg_suplr_mdcr_pymt_amt AS DOUBLE)*tot_suplr_srvcs) FILTER (WHERE program_year=2022))
        / nullif(sum(CAST(avg_suplr_mdcr_pymt_amt AS DOUBLE)*tot_suplr_srvcs) FILTER (WHERE program_year=2022), 0) AS dme_paid_yoy_pct,
      true AS is_dme_supplier
    FROM c3 GROUP BY npi
    """


# ─────────────────────────────── provider_360 inline-aggregate SQL ───────────────────────────────
A1_AGG = """
a1 AS (
  SELECT npi, true AS has_a1,
    max(program_year) AS med_a1_latest_year,
    arg_max(tot_mdcr_pymt_amt, program_year) AS med_a1_latest_mdcr_pymt,
    arg_max(tot_mdcr_stdzd_amt, program_year) AS med_a1_latest_stdzd,
    arg_max(tot_benes, program_year) AS med_a1_latest_benes,
    arg_max(tot_srvcs, program_year) AS med_a1_latest_srvcs,
    min(program_year) AS med_a1_first_year, max(program_year) AS med_a1_last_year,
    count(DISTINCT program_year) AS med_a1_active_years,
    CAST(sum(tot_mdcr_pymt_amt) AS DECIMAL(38,2)) AS med_a1_lifetime_mdcr_pymt,
    sum(tot_benes) AS med_a1_lifetime_bene_years,
    arg_max(rndrng_prvdr_type, program_year) AS med_a1_provider_type,
    arg_max(rndrng_prvdr_ent_cd, program_year) AS med_a1_entity_code,
    arg_max(drug_sprsn_ind, program_year) AS med_a1_drug_sprsn_latest,
    arg_max(med_sprsn_ind, program_year) AS med_a1_med_sprsn_latest,
    arg_max(bene_avg_risk_scre, program_year) AS med_a1_panel_avg_risk_score,
    arg_max(bene_avg_age, program_year) AS med_a1_panel_avg_age,
    arg_max(CASE WHEN tot_benes>0 THEN CAST(bene_dual_cnt AS DOUBLE)/tot_benes END, program_year) AS med_a1_dual_share,
    arg_max(bene_cc_ph_diabetes_v2_pct, program_year) AS med_a1_dx_diabetes_pct,
    arg_max(bene_cc_ph_hf_nonihd_v2_pct, program_year) AS med_a1_dx_chf_pct,
    arg_max(bene_cc_ph_ckd_v2_pct, program_year) AS med_a1_dx_ckd_pct,
    100.0 * (arg_max(CAST(tot_mdcr_pymt_amt AS DOUBLE), program_year) - max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2019))
      / nullif(max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2019), 0) AS med_a1_pymt_growth_2019_latest_pct,
    100.0 * (max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2024) - max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2023))
      / nullif(max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2023), 0) AS med_a1_pymt_yoy_pct,
    100.0 * (power(CAST(max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2024) AS DOUBLE)
      / nullif(max(tot_mdcr_pymt_amt) FILTER (WHERE program_year=2021), 0), 1.0/3) - 1) AS med_a1_pymt_cagr_3yr_pct,
    100.0 * (max(tot_benes) FILTER (WHERE program_year=2024) - max(tot_benes) FILTER (WHERE program_year=2023))
      / nullif(max(tot_benes) FILTER (WHERE program_year=2023), 0) AS med_a1_benes_yoy_pct,
    (max(program_year) = 2024) AS med_a1_active_latest
  FROM a1 GROUP BY npi)
"""

B1_AGG = """
b1 AS (
  SELECT npi, true AS has_b1,
    max(program_year) AS med_b1_latest_year,
    arg_max(tot_drug_cst, program_year) AS med_b1_latest_drug_cost,
    arg_max(tot_clms, program_year) AS med_b1_latest_clms,
    arg_max(tot_benes, program_year) AS med_b1_latest_benes,
    arg_max(tot_30day_fills, program_year) AS med_b1_latest_30day_fills,
    arg_max(tot_day_suply, program_year) AS med_b1_latest_day_suply,
    min(program_year) AS med_b1_first_year, max(program_year) AS med_b1_last_year,
    count(DISTINCT program_year) AS med_b1_active_years,
    CAST(sum(tot_drug_cst) AS DECIMAL(38,2)) AS med_b1_lifetime_drug_cost,
    sum(tot_clms) AS med_b1_lifetime_clms,
    arg_max(prscrbr_type, program_year) AS med_b1_prescriber_type,
    arg_max(opioid_tot_clms, program_year) AS med_b1_latest_opioid_clms,
    arg_max(opioid_prscrbr_rate, program_year) AS med_b1_latest_opioid_rate,
    arg_max(opioid_la_tot_clms, program_year) AS med_b1_latest_la_opioid_clms
  FROM b1 GROUP BY npi)
"""

TAX_AGG = """
tax AS (
  SELECT npi, list(DISTINCT taxonomy_code) AS all_taxonomy_codes,
    count(*) AS taxonomy_slot_count,
    max(license_state) FILTER (WHERE is_primary) AS primary_license_state
  FROM taxonomy GROUP BY npi)
"""

ID_AGG = """
ids AS (
  SELECT npi, true AS has_secondary_identifiers,
    coalesce(bool_or(identifier_type_code='05'), false) AS has_medicaid_id
  FROM identifier GROUP BY npi)
"""

# QPP — mandatory pre-agg to 1 row/npi; participation_type dead → coalesce(participation_option, reporting_option)
QPP_AGG = """
qpp AS (
  SELECT npi, true AS has_mips,
    arg_max(TRY_CAST(final_score AS DOUBLE), (program_year, TRY_CAST(final_score AS DOUBLE))) AS mips_final_score,
    max(program_year) AS mips_final_score_year,
    arg_max(coalesce(participation_option, reporting_option), (program_year, TRY_CAST(final_score AS DOUBLE))) AS mips_participation_option,
    arg_max(clinician_specialty, (program_year, TRY_CAST(final_score AS DOUBLE))) AS mips_clinician_specialty
  FROM qpp_src GROUP BY npi)
"""

# Practice graph — dual-pole (smallest = candidate independent practice; largest = system affiliation)
PRACTICE_AGG = """
gsize AS (SELECT rcv_bnft_enrlmt_id AS grp, count(DISTINCT reasgn_bnft_enrlmt_id) AS fanin
          FROM reassignment GROUP BY 1),
orgname AS (SELECT enrlmt_id AS grp, any_value(org_name) AS org_name FROM enrollment WHERE org_name IS NOT NULL GROUP BY 1),
npi_grp AS (
  SELECT e.npi, r.rcv_bnft_enrlmt_id AS grp, gs.fanin
  FROM enrollment e JOIN reassignment r ON e.enrlmt_id = r.reasgn_bnft_enrlmt_id
  JOIN gsize gs ON gs.grp = r.rcv_bnft_enrlmt_id),
enr AS (
  SELECT npi, true AS has_ffs_enrollment,
    list(DISTINCT enrlmt_id) AS enrollment_enrlmt_ids, any_value(pecos_asct_cntl_id) AS pecos_asct_cntl_id
  FROM enrollment GROUP BY npi),
practice AS (
  SELECT npi,
    count(DISTINCT grp) AS practice_group_count,
    arg_min(grp, fanin) AS smallest_practice_group_enrlmt_id,
    min(fanin) AS smallest_practice_group_size,
    arg_max(grp, fanin) AS largest_practice_group_enrlmt_id,
    max(fanin) AS largest_practice_group_size,
    (min(fanin) <= 10) AS is_independent_candidate
  FROM npi_grp GROUP BY npi)
"""


def _provider_360_sql():
    """Final assembly: base nppes_provider LEFT JOIN every per-NPI aggregate + Tier-1 rollups."""
    return f"""
    WITH {A1_AGG},
    {B1_AGG},
    {TAX_AGG},
    {ID_AGG},
    {QPP_AGG},
    {PRACTICE_AGG}
    SELECT
      b.npi, b.entity_type_code, b.entity_type, b.is_active,
      b.provider_name, b.organization_name, b.last_name, b.first_name, b.middle_name,
      b.name_prefix, b.name_suffix, b.credential, b.sex_code,
      b.is_sole_proprietor, b.is_organization_subpart,
      b.primary_taxonomy_code, tax.all_taxonomy_codes, tax.taxonomy_slot_count, tax.primary_license_state,
      b.practice_state, b.practice_zip5, b.practice_address_line1, b.practice_city, b.practice_country,
      b.mailing_state, b.mailing_city, b.mailing_zip5,
      b.enumeration_date, b.enumeration_year, b.last_update_date, b.deactivation_date, b.reactivation_date,
      b.authorized_official_last_name, b.authorized_official_first_name, b.authorized_official_title,
      b.parent_organization_lbn,
      coalesce(ids.has_secondary_identifiers, false) AS has_secondary_identifiers,
      coalesce(ids.has_medicaid_id, false) AS has_medicaid_id,
      -- Medicare Part B totals + panel economics (A1)
      coalesce(a1.has_a1, false) AS has_a1, a1.med_a1_latest_year, a1.med_a1_latest_mdcr_pymt, a1.med_a1_latest_stdzd,
      a1.med_a1_latest_benes, a1.med_a1_latest_srvcs, a1.med_a1_first_year, a1.med_a1_last_year, a1.med_a1_active_years,
      a1.med_a1_lifetime_mdcr_pymt, a1.med_a1_lifetime_bene_years, a1.med_a1_provider_type, a1.med_a1_entity_code,
      a1.med_a1_drug_sprsn_latest, a1.med_a1_med_sprsn_latest,
      a1.med_a1_panel_avg_risk_score, a1.med_a1_panel_avg_age, a1.med_a1_dual_share,
      a1.med_a1_dx_diabetes_pct, a1.med_a1_dx_chf_pct, a1.med_a1_dx_ckd_pct, a1.med_a1_pymt_growth_2019_latest_pct,
      a1.med_a1_pymt_yoy_pct, a1.med_a1_pymt_cagr_3yr_pct, a1.med_a1_benes_yoy_pct,
      coalesce(a1.med_a1_active_latest, false) AS med_a1_active_latest,
      -- Part D prescriber totals (B1, ≤2020)
      coalesce(b1.has_b1, false) AS has_b1, b1.med_b1_latest_year, b1.med_b1_latest_drug_cost, b1.med_b1_latest_clms,
      b1.med_b1_latest_benes, b1.med_b1_latest_30day_fills, b1.med_b1_latest_day_suply,
      b1.med_b1_first_year, b1.med_b1_last_year, b1.med_b1_active_years,
      b1.med_b1_lifetime_drug_cost, b1.med_b1_lifetime_clms, b1.med_b1_prescriber_type,
      b1.med_b1_latest_opioid_clms, b1.med_b1_latest_opioid_rate, b1.med_b1_latest_la_opioid_clms,
      -- Tier-1 rollups (service / drug / dme / open payments)
      (svc.npi IS NOT NULL) AS has_svc, svc.svc_total_medicare_paid_usd, svc.svc_lines_suppressed, svc.svc_total_services,
      svc.svc_total_bene_lines, svc.svc_distinct_hcpcs, svc.svc_active_years, svc.svc_first_year, svc.svc_last_year,
      svc.svc_partb_drug_paid_usd, svc.has_partb_administered_drugs, svc.svc_top_rbcs_cat, svc.svc_top_rbcs_cat_paid_usd,
      svc.svc_paid_yoy_pct,
      (rx.npi IS NOT NULL) AS has_rx, rx.rx_total_drug_cost_usd, rx.rx_total_claims, rx.rx_total_30day_fills,
      rx.rx_total_day_supply, rx.rx_distinct_generics, rx.rx_distinct_brands, rx.rx_active_years, rx.rx_first_year,
      rx.rx_last_year, rx.rx_top1_generic, rx.rx_top1_generic_cost_usd, rx.rx_top3_generics, rx.rx_cost_yoy_pct,
      coalesce(dme.is_dme_supplier, false) AS is_dme_supplier, dme.dme_supplied_total_paid_usd, dme.dme_supplied_claims,
      dme.dme_distinct_hcpcs, dme.dme_rental_share, dme.dme_lines_suppressed, dme.dme_paid_yoy_pct,
      (op.npi IS NOT NULL) AS has_op, op.total_payments_usd AS op_total_payments_usd,
      op.general_total_usd AS op_general_total_usd, op.research_total_usd AS op_research_total_usd,
      op.distinct_manufacturers AS op_distinct_manufacturers, op.has_ownership_interest AS op_has_ownership_interest,
      op.ownership_total_value_usd AS op_ownership_total_value_usd,
      op.first_payment_year AS op_first_payment_year, op.last_payment_year AS op_last_payment_year,
      op.recipient_type AS op_recipient_type,
      -- Quality
      coalesce(qpp.has_mips, false) AS has_mips, qpp.mips_final_score, qpp.mips_final_score_year,
      qpp.mips_participation_option, qpp.mips_clinician_specialty,
      -- Practice affiliation (dual-pole, deterministic)
      coalesce(enr.has_ffs_enrollment, false) AS has_ffs_enrollment, enr.enrollment_enrlmt_ids, enr.pecos_asct_cntl_id,
      practice.practice_group_count,
      practice.smallest_practice_group_enrlmt_id, practice.smallest_practice_group_size, sorg.org_name AS smallest_practice_org_name,
      practice.largest_practice_group_enrlmt_id, practice.largest_practice_group_size, lorg.org_name AS largest_practice_org_name,
      coalesce(practice.is_independent_candidate, false) AS is_independent_candidate,
      '{{snap}}' AS provider_360_snapshot, '{NPPES_SNAPSHOT}' AS nppes_snapshot
    FROM base b
    LEFT JOIN a1 USING (npi)
    LEFT JOIN b1 USING (npi)
    LEFT JOIN tax USING (npi)
    LEFT JOIN ids USING (npi)
    LEFT JOIN qpp USING (npi)
    LEFT JOIN enr USING (npi)
    LEFT JOIN practice USING (npi)
    LEFT JOIN svc ON svc.npi = b.npi
    LEFT JOIN rx ON rx.npi = b.npi
    LEFT JOIN dme ON dme.npi = b.npi
    LEFT JOIN op ON op.npi = b.npi
    LEFT JOIN orgname sorg ON sorg.grp = practice.smallest_practice_group_enrlmt_id
    LEFT JOIN orgname lorg ON lorg.grp = practice.largest_practice_group_enrlmt_id
    """


def _group_360_sql():
    """practice_group_360 — 1 row per buyable group (rcv ENRLMT_ID), rolling member provider_360 up."""
    return """
    WITH members AS (
      SELECT r.rcv_bnft_enrlmt_id AS group_enrlmt_id, e.npi
      FROM cms_provider_enrollment_reassignment_t r
      JOIN enrollment_t e ON e.enrlmt_id = r.reasgn_bnft_enrlmt_id
      WHERE e.npi IS NOT NULL),
    joined AS (
      SELECT m.group_enrlmt_id, m.npi,
        p.med_a1_lifetime_mdcr_pymt, p.rx_total_drug_cost_usd, p.med_a1_panel_avg_risk_score,
        p.med_a1_dual_share, p.primary_taxonomy_code, p.mips_final_score, p.op_total_payments_usd,
        p.is_independent_candidate, p.practice_state,
        p.med_a1_pymt_yoy_pct, p.med_a1_pymt_cagr_3yr_pct, p.med_a1_active_latest
      FROM members m JOIN p360 p ON p.npi = m.npi)
    SELECT
      j.group_enrlmt_id,
      org.org_name, org.state_cd AS group_state,
      count(DISTINCT j.npi) AS member_count,
      list(DISTINCT j.npi) AS member_npis,
      CAST(sum(j.med_a1_lifetime_mdcr_pymt) AS DECIMAL(38,2)) AS total_medicare_paid_usd,
      CAST(sum(j.rx_total_drug_cost_usd) AS DECIMAL(38,2)) AS total_rx_cost_usd,
      avg(j.med_a1_panel_avg_risk_score) AS avg_panel_risk_score,
      avg(j.med_a1_dual_share) AS avg_dual_share,
      mode(j.primary_taxonomy_code) AS top_specialty,
      count(DISTINCT j.primary_taxonomy_code) AS distinct_specialties,
      avg(j.mips_final_score) AS avg_mips_score,
      sum(CASE WHEN j.is_independent_candidate THEN 1 ELSE 0 END) AS independent_member_count,
      CAST(sum(j.op_total_payments_usd) AS DECIMAL(38,2)) AS total_op_payments_usd,
      count(DISTINCT j.practice_state) AS n_states,
      avg(j.med_a1_pymt_yoy_pct) AS avg_pymt_yoy_pct,
      avg(j.med_a1_pymt_cagr_3yr_pct) AS avg_pymt_cagr_3yr_pct,
      sum(CASE WHEN j.med_a1_active_latest THEN 1 ELSE 0 END) AS active_member_count
    FROM joined j
    LEFT JOIN (SELECT enrlmt_id, any_value(org_name) org_name, any_value(state_cd) state_cd
               FROM enrollment_t WHERE org_name IS NOT NULL GROUP BY enrlmt_id) org
      ON org.enrlmt_id = j.group_enrlmt_id
    GROUP BY j.group_enrlmt_id, org.org_name, org.state_cd
    """


# ─────────────────────────────── ops ───────────────────────────────
def _record(*, artifact, snapshot_month, dataset_uri, rows, indices, attach_rates, gates, status, error, started, completed):
    import json
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED unset; skip ops"); return
    cols = ("feed", "artifact", "snapshot_month", "dataset_uri", "rows_written", "indices", "attach_rates",
            "gates", "status", "error", "started_at", "completed_at")
    vals = [FEED, artifact, snapshot_month, dataset_uri, rows, ", ".join(indices or []),
            json.dumps(attach_rates) if attach_rates else None, json.dumps(gates) if gates else None,
            status, error, started, completed]
    upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "feed")
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f"INSERT INTO ops.provider_360_runs ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))}) "
                        f"ON CONFLICT (artifact, snapshot_month) DO UPDATE SET {upd}, recorded_at=now()", vals)
            conn.commit()
    except Exception as e:  # noqa: BLE001
        print(f"WARN ops write: {e}")


# ─────────────────────────────── workers ───────────────────────────────
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn: raise RuntimeError("HQX_DB_URL_POOLED unset")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL); conn.commit()
        cur.execute("SELECT to_regclass('ops.provider_360_runs')"); present = cur.fetchone()[0]
    return {"status": "success", "present": str(present)}


def _build_rollup(which: str) -> dict:
    """Build one Tier-1 giant rollup: scan the giant once → GROUP BY npi → 1-row-per-NPI Lance → publish."""
    import datetime as dt
    import shutil
    cfg = ROLLUPS[which]
    started = dt.datetime.now(dt.timezone.utc)
    shutil.rmtree(SCRATCH, ignore_errors=True); os.makedirs(SPILL, exist_ok=True)
    so = _so()
    con = _new_con(mem="100GB" if which == "drug" else DUCKDB_MEM)  # B2 (npi,generic) groupby on the 128 GiB container
    out_local = os.path.join(LOCAL, cfg["out"])
    live = f"{ACTIVE}{cfg['out']}/"
    try:
        if which == "service":
            n = _reg(con, "a2", cfg["src"], ["rndrng_npi", "hcpcs_cd", "hcpcs_drug_ind", "program_year",
                     "tot_srvcs", "tot_benes", "avg_mdcr_pymt_amt"], so)
            con.execute("ALTER TABLE a2 RENAME COLUMN rndrng_npi TO npi")
            _reg(con, "rbcs", "ref_rbcs_taxonomy", ["hcpcs_cd", "rbcs_cat"], so)
            con.execute("CREATE TEMP TABLE rbcs2 AS SELECT hcpcs_cd, any_value(rbcs_cat) rbcs_cat FROM rbcs GROUP BY hcpcs_cd")
            con.execute("DROP TABLE rbcs"); con.execute("ALTER TABLE rbcs2 RENAME TO rbcs")
            sql = _service_sql()
        elif which == "drug":
            n = _reg(con, "b2", cfg["src"], ["prscrbr_npi", "gnrc_name", "brnd_name", "program_year",
                     "tot_drug_cst", "tot_clms", "tot_30day_fills", "tot_day_suply"], so)
            con.execute("ALTER TABLE b2 RENAME COLUMN prscrbr_npi TO npi")
            sql = _drug_sql()
        else:  # dme
            n = _reg(con, "c3", cfg["src"], ["suplr_npi", "hcpcs_cd", "suplr_rentl_ind", "program_year",
                     "avg_suplr_mdcr_pymt_amt", "tot_suplr_srvcs", "tot_suplr_clms"], so)
            con.execute("ALTER TABLE c3 RENAME COLUMN suplr_npi TO npi")
            sql = _dme_sql()
        print(f"[{cfg['out']}] scanned {n:,} source rows")
        rows = _write_sorted(con, sql, out_local, "npi")
        con.close()
        built = _create_indexes(out_local, cfg["btree"], cfg["bitmap"])
        s3 = _s3()
        _publish_full_swap(s3, live, out_local)
        uri = f"s3://{BUCKET}/{live}"
        vr = _verify_published(uri, rows, len(cfg["btree"]) + len(cfg["bitmap"]), "npi", so)
        completed = dt.datetime.now(dt.timezone.utc)
        _record(artifact=cfg["out"], snapshot_month=None, dataset_uri=uri, rows=rows, indices=built,
                attach_rates=None, gates=None, status="success", error=None, started=started, completed=completed)
        shutil.rmtree(SCRATCH, ignore_errors=True)
        return {"status": "success", "artifact": cfg["out"], "rows": rows, "indices": built, "verify": vr}
    except Exception as exc:
        _record(artifact=cfg["out"], snapshot_month=None, dataset_uri=f"s3://{BUCKET}/{live}", rows=0,
                indices=[], attach_rates=None, gates=None, status="error", error=str(exc)[:2000],
                started=started, completed=dt.datetime.now(dt.timezone.utc))
        raise


@app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 4, memory=49152, cpu=8.0, ephemeral_disk=524288, retries=1)
def build_rollup_service() -> dict:
    return _build_rollup("service")


@app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 6, memory=131072, cpu=8.0, ephemeral_disk=524288, retries=1)
def build_rollup_drug() -> dict:  # B2 304M — the (npi,generic) groupby is the heavy one → 128 GiB
    return _build_rollup("drug")


@app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 2, memory=32768, cpu=8.0, ephemeral_disk=524288, retries=1)
def build_rollup_dme() -> dict:
    return _build_rollup("dme")


@app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 5, memory=98304, cpu=8.0, ephemeral_disk=524288, retries=1)
def build_provider_360(snapshot_month: str = DEFAULT_SNAPSHOT) -> dict:
    """Assemble provider_360: base nppes_provider LEFT JOIN all per-NPI aggregates + Tier-1 rollups."""
    import datetime as dt
    import shutil
    started = dt.datetime.now(dt.timezone.utc)
    shutil.rmtree(SCRATCH, ignore_errors=True); os.makedirs(SPILL, exist_ok=True)
    so = _so()
    con = _new_con()
    out_local = os.path.join(LOCAL, "provider_360")
    live = f"{ACTIVE}{FEED}/snapshot={snapshot_month}/"
    try:
        base_cols = ["npi", "entity_type_code", "entity_type", "is_active", "provider_name", "organization_name",
                     "last_name", "first_name", "middle_name", "name_prefix", "name_suffix", "credential", "sex_code",
                     "is_sole_proprietor", "is_organization_subpart", "primary_taxonomy_code", "practice_state",
                     "practice_zip5", "practice_address_line1", "practice_city", "practice_country", "mailing_state",
                     "mailing_city", "mailing_zip5", "enumeration_date", "enumeration_year", "last_update_date",
                     "deactivation_date", "reactivation_date", "authorized_official_last_name",
                     "authorized_official_first_name", "authorized_official_title", "parent_organization_lbn"]
        nb = _reg(con, "base", BASE_NPPES, base_cols, so)
        print(f"[provider_360] base nppes_provider {nb:,}")
        _reg(con, "a1", "cms_physician_provider", ["rndrng_npi", "program_year", "tot_mdcr_pymt_amt", "tot_mdcr_stdzd_amt",
             "tot_benes", "tot_srvcs", "rndrng_prvdr_type", "rndrng_prvdr_ent_cd", "drug_sprsn_ind", "med_sprsn_ind",
             "bene_avg_risk_scre", "bene_avg_age", "bene_dual_cnt", "bene_cc_ph_diabetes_v2_pct",
             "bene_cc_ph_hf_nonihd_v2_pct", "bene_cc_ph_ckd_v2_pct"], so)
        con.execute("ALTER TABLE a1 RENAME COLUMN rndrng_npi TO npi")
        _reg(con, "b1", "cms_partd_provider", ["prscrbr_npi", "program_year", "tot_drug_cst", "tot_clms", "tot_benes",
             "tot_30day_fills", "tot_day_suply", "prscrbr_type", "opioid_tot_clms", "opioid_prscrbr_rate", "opioid_la_tot_clms"], so)
        con.execute("ALTER TABLE b1 RENAME COLUMN prscrbr_npi TO npi")
        _reg(con, "taxonomy", f"nppes_provider_taxonomy/snapshot={NPPES_SNAPSHOT}", ["npi", "taxonomy_code", "is_primary", "license_state"], so)
        _reg(con, "identifier", f"nppes_provider_identifier/snapshot={NPPES_SNAPSHOT}", ["npi", "identifier_type_code"], so)
        _reg(con, "qpp_src", "cms_qpp_experience", ["npi", "program_year", "final_score", "participation_option", "reporting_option", "clinician_specialty"], so)
        _reg(con, "enrollment", "cms_provider_enrollment", ["npi", "enrlmt_id", "pecos_asct_cntl_id", "org_name"], so)
        _reg(con, "reassignment", "cms_provider_enrollment_reassignment", ["reasgn_bnft_enrlmt_id", "rcv_bnft_enrlmt_id"], so)
        # Tier-1 rollups + open payments — small leaf datasets, register directly as temp tables.
        _reg(con, "svc", "cms_physician_service_rollup",
             ["npi", "svc_total_medicare_paid_usd", "svc_lines_suppressed", "svc_total_services", "svc_total_bene_lines",
              "svc_distinct_hcpcs", "svc_active_years", "svc_first_year", "svc_last_year", "svc_partb_drug_paid_usd",
              "has_partb_administered_drugs", "svc_top_rbcs_cat", "svc_top_rbcs_cat_paid_usd", "svc_paid_yoy_pct"], so)
        _reg(con, "rx", "cms_partd_drug_rollup", ["npi", "rx_total_drug_cost_usd", "rx_total_claims", "rx_total_30day_fills",
             "rx_total_day_supply", "rx_distinct_generics", "rx_distinct_brands", "rx_active_years", "rx_first_year",
             "rx_last_year", "rx_top1_generic", "rx_top1_generic_cost_usd", "rx_top3_generics", "rx_cost_yoy_pct"], so)
        _reg(con, "dme", "cms_dme_supplier_rollup", ["npi", "dme_supplied_total_paid_usd", "dme_lines_suppressed",
             "dme_supplied_claims", "dme_distinct_hcpcs", "dme_rental_share", "dme_paid_yoy_pct", "is_dme_supplier"], so)
        _reg(con, "op", "cms_provider_payment_rollup", ["npi", "total_payments_usd", "general_total_usd", "research_total_usd",
             "distinct_manufacturers", "has_ownership_interest", "ownership_total_value_usd", "first_payment_year",
             "last_payment_year", "recipient_type"], so)

        sql = _provider_360_sql().replace("{snap}", snapshot_month)
        rows = _write_sorted(con, sql, out_local, "npi")
        print(f"[provider_360] assembled {rows:,} rows")

        # Gates (plan §7) — on the local stage before publish.
        gates = _run_gates(con, out_local)
        attach = _attach_rates(con, out_local)
        con.close()
        if rows != BASE_ROWS_EXPECTED:
            raise RuntimeError(f"G1 FAIL: rows={rows:,} != base {BASE_ROWS_EXPECTED:,} (join fan-out?)")

        built = _create_indexes(out_local, PROVIDER_360_BTREE, PROVIDER_360_BITMAP)
        s3 = _s3()
        _publish_full_swap(s3, live, out_local)
        uri = f"s3://{BUCKET}/{live}"
        vr = _verify_published(uri, rows, len(PROVIDER_360_BTREE) + len(PROVIDER_360_BITMAP), "npi", so)
        completed = dt.datetime.now(dt.timezone.utc)
        _record(artifact="provider_360", snapshot_month=snapshot_month, dataset_uri=uri, rows=rows, indices=built,
                attach_rates=attach, gates=gates, status="success", error=None, started=started, completed=completed)
        shutil.rmtree(SCRATCH, ignore_errors=True)
        return {"status": "success", "rows": rows, "indices": built, "attach_rates": attach, "gates": gates, "verify": vr, "dataset_uri": uri}
    except Exception as exc:
        _record(artifact="provider_360", snapshot_month=snapshot_month, dataset_uri=f"s3://{BUCKET}/{live}", rows=0,
                indices=[], attach_rates=None, gates=None, status="error", error=str(exc)[:2000],
                started=started, completed=dt.datetime.now(dt.timezone.utc))
        raise


def _attach_rates(con, local_ds) -> dict:
    import lance
    ds = lance.dataset(local_ds)
    con.register("p", ds.scanner(columns=["has_a1", "has_b1", "has_svc", "has_rx", "has_op", "has_mips", "has_ffs_enrollment",
                                          "is_dme_supplier", "is_independent_candidate"]).to_reader())
    r = con.execute("""SELECT count(*) n,
        count(*) FILTER (WHERE has_a1) a1, count(*) FILTER (WHERE has_b1) b1, count(*) FILTER (WHERE has_svc) svc,
        count(*) FILTER (WHERE has_rx) rx, count(*) FILTER (WHERE has_op) op, count(*) FILTER (WHERE has_mips) mips,
        count(*) FILTER (WHERE has_ffs_enrollment) ffs, count(*) FILTER (WHERE is_dme_supplier) dme,
        count(*) FILTER (WHERE is_independent_candidate) indep FROM p""").fetchone()
    con.unregister("p")
    keys = ["n", "a1", "b1", "svc", "rx", "op", "mips", "ffs", "dme", "indep"]
    return dict(zip(keys, [int(x) for x in r]))


def _run_gates(con, local_ds) -> dict:
    """Plan §7 gates + the v2 additions (G-new-1..5) on the local stage."""
    import lance
    ds = lance.dataset(local_ds)
    g = {}
    g["rows"] = ds.count_rows()
    g["G1_rowcount_eq_base"] = (g["rows"] == BASE_ROWS_EXPECTED)
    con.register("p", ds.scanner(columns=["npi", "is_active", "has_svc", "svc_total_medicare_paid_usd",
                 "med_a1_latest_year", "med_b1_latest_year", "smallest_practice_group_size",
                 "smallest_practice_group_enrlmt_id", "smallest_practice_org_name"]).to_reader())
    con.execute("CREATE TEMP TABLE pg AS SELECT * FROM p"); con.unregister("p")
    g["G2_npi_unique"] = (con.execute("SELECT count(*)=count(DISTINCT npi) FROM pg").fetchone()[0])
    g["G8_deactivated_kept"] = (con.execute("SELECT count(*) FROM pg WHERE is_active=false").fetchone()[0])
    # G-new-2: every has_svc row has a non-null svc total (catches the DECIMAL-overflow NULL)
    g["Gnew2_svc_money_complete"] = (con.execute("SELECT count(*) FROM pg WHERE has_svc AND svc_total_medicare_paid_usd IS NULL").fetchone()[0] == 0)
    # G-new-5: no latest_year exceeds its source max
    g["Gnew5_a1_year_ok"] = (con.execute("SELECT count(*) FROM pg WHERE med_a1_latest_year > 2024").fetchone()[0] == 0)
    g["Gnew5_b1_year_ok"] = (con.execute("SELECT count(*) FROM pg WHERE med_b1_latest_year > 2020").fetchone()[0] == 0)
    # G-new-3: practice group resolves with a size>=1
    g["Gnew3_practice_size_ok"] = (con.execute("SELECT count(*) FROM pg WHERE smallest_practice_group_enrlmt_id IS NOT NULL AND smallest_practice_group_size < 1").fetchone()[0] == 0)
    con.execute("DROP TABLE pg")
    return g


@app.function(secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
              timeout=60 * 60 * 3, memory=65536, cpu=8.0, ephemeral_disk=524288, retries=1)
def build_practice_group_360(snapshot_month: str = DEFAULT_SNAPSHOT) -> dict:
    """Roll provider_360 member economics up to the buyable group (rcv ENRLMT_ID)."""
    import datetime as dt
    import shutil
    started = dt.datetime.now(dt.timezone.utc)
    shutil.rmtree(SCRATCH, ignore_errors=True); os.makedirs(SPILL, exist_ok=True)
    so = _so()
    con = _new_con()
    out_local = os.path.join(LOCAL, "practice_group_360")
    live = f"{ACTIVE}practice_group_360/snapshot={snapshot_month}/"
    try:
        _reg(con, "cms_provider_enrollment_reassignment_t", "cms_provider_enrollment_reassignment", ["reasgn_bnft_enrlmt_id", "rcv_bnft_enrlmt_id"], so)
        _reg(con, "enrollment_t", "cms_provider_enrollment", ["npi", "enrlmt_id", "org_name", "state_cd"], so)
        _reg(con, "p360", f"provider_360/snapshot={snapshot_month}", ["npi", "med_a1_lifetime_mdcr_pymt", "rx_total_drug_cost_usd",
             "med_a1_panel_avg_risk_score", "med_a1_dual_share", "primary_taxonomy_code", "mips_final_score",
             "op_total_payments_usd", "is_independent_candidate", "practice_state",
             "med_a1_pymt_yoy_pct", "med_a1_pymt_cagr_3yr_pct", "med_a1_active_latest"], so)
        rows = _write_sorted(con, _group_360_sql(), out_local, "group_enrlmt_id")
        con.close()
        print(f"[practice_group_360] {rows:,} groups")
        built = _create_indexes(out_local, GROUP_360_BTREE, GROUP_360_BITMAP)
        s3 = _s3()
        _publish_full_swap(s3, live, out_local)
        uri = f"s3://{BUCKET}/{live}"
        vr = _verify_published(uri, rows, len(GROUP_360_BTREE) + len(GROUP_360_BITMAP), "group_enrlmt_id", so)
        completed = dt.datetime.now(dt.timezone.utc)
        _record(artifact="practice_group_360", snapshot_month=snapshot_month, dataset_uri=uri, rows=rows, indices=built,
                attach_rates=None, gates=None, status="success", error=None, started=started, completed=completed)
        shutil.rmtree(SCRATCH, ignore_errors=True)
        return {"status": "success", "rows": rows, "indices": built, "verify": vr, "dataset_uri": uri}
    except Exception as exc:
        _record(artifact="practice_group_360", snapshot_month=snapshot_month, dataset_uri=f"s3://{BUCKET}/{live}", rows=0,
                indices=[], attach_rates=None, gates=None, status="error", error=str(exc)[:2000],
                started=started, completed=dt.datetime.now(dt.timezone.utc))
        raise


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15, memory=8192)
def verify_artifact(dataset: str, snapshot_month: str = DEFAULT_SNAPSHOT) -> dict:
    import lance
    so = _so()
    path = dataset if dataset in (r["out"] for r in ROLLUPS.values()) else f"{dataset}/snapshot={snapshot_month}"
    uri = f"s3://{BUCKET}/{ACTIVE}{path}/"
    ds = lance.dataset(uri, storage_options=so)
    return {"dataset": dataset, "uri": uri, "rows": ds.count_rows(), "n_cols": len(ds.schema), "indices": _list_indices(ds)}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 20) -> list:
    import psycopg
    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, artifact, snapshot_month, rows_written, status, attach_rates, gates, error, recorded_at "
                    "FROM ops.provider_360_runs ORDER BY id DESC LIMIT %s", (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ─────────────────────────────── local entrypoints ───────────────────────────────
@app.local_entrypoint()
def init_state() -> None:
    import json
    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def rollup(which: str = "all") -> None:
    import json
    fns = {"service": build_rollup_service, "drug": build_rollup_drug, "dme": build_rollup_dme}
    if which == "all":
        for k, fn in fns.items():
            print(json.dumps(fn.remote(), indent=2, default=str))
    else:
        print(json.dumps(fns[which].remote(), indent=2, default=str))


@app.local_entrypoint()
def build_360(snapshot_month: str = DEFAULT_SNAPSHOT) -> None:
    import json
    print(json.dumps(build_provider_360.remote(snapshot_month), indent=2, default=str))


@app.local_entrypoint()
def build_groups(snapshot_month: str = DEFAULT_SNAPSHOT) -> None:
    import json
    print(json.dumps(build_practice_group_360.remote(snapshot_month), indent=2, default=str))


@app.local_entrypoint()
def verify(dataset: str, snapshot_month: str = DEFAULT_SNAPSHOT) -> None:
    import json
    print(json.dumps(verify_artifact.remote(dataset, snapshot_month), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 20) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
