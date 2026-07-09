"""query-sidecar builder — export the frozen Phase 0 mart manifest into one sorted .duckdb artifact.

Phase 1 of the query-sidecar plan (docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md).
Reads each manifest mart from the Lance SoR (s3://data-sink/active/), streams it
through DuckDB, materializes it as a NATIVE DuckDB table physically clustered by
its hop key (CREATE TABLE ... AS SELECT ... ORDER BY), and publishes a single
versioned .duckdb file to R2 under s3://data-sink/query-sidecar/ with a LATEST
pointer (blue-green: new file first, pointer swap second, old files retained).

Naming note: "sidecar" elsewhere in this repo means a derived LANCE dataset
(e.g. pdl_normalized_companies). THIS artifact is different — a DuckDB-native
read-only query file for the warm serving process. Hence "query-sidecar".

Doctrine (docs/reference/03_modal_compute.md):
- standalone Modal app, `modal run` invoked; NO dispatcher, NO Trigger schedule;
- NO modal.Volume — all scratch on the container's ephemeral NVMe at /tmp;
- Python is I/O only; DuckDB performs 100% of transform; Arrow is the only
  interchange (Lance scanner reader -> DuckDB register -> CTAS);
- ops ledger row written on terminal state (success AND failure), never masks
  the build; manual runs skip the Trigger callback.

Parity: every mart's DuckDB count must equal ds.count_rows() at the PINNED Lance
version read at build start. Any mismatch fails the run before publish.

Entrypoints:
  modal run pipelines/query_sidecar/build_query_sidecar.py::initdb
  modal run pipelines/query_sidecar/build_query_sidecar.py::run          # full A,B,D + publish
  modal run pipelines/query_sidecar/build_query_sidecar.py::smoke       # Tier A only, smoke/ prefix, no LATEST
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time

import modal

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",   # to_arrow_reader / register surface; below the v2.0 break
        "pylance>=7",        # provides `import lance`; lancedb does NOT re-export it
        "pyarrow>=17",
        "psycopg[binary]>=3.2",
        "boto3>=1.35",
    )
)

app = modal.App("query-sidecar", image=image)

LANCE_BASE = "s3://data-sink/active/"
R2_BUCKET = "data-sink"
R2_PREFIX = "query-sidecar"
SCRATCH_ROOT = "/tmp/query_sidecar"
READ_BATCH_ROWS = 131_072

# ── The frozen manifest (docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md) ──────────
# (dataset, tier, sort_keys, columns_projection_or_None, dest_table_or_None)
# columns=None -> SELECT *. dest defaults to the dataset name.

_SUBAWARD_COLS = [
    # 35-column projection, evidence-cited from every consuming catalyst store
    # (sub_universe_pairs/full pool scans, subout_store rules, lance_store
    # subaward history) + all BTREE'd keys + filter axes. subaward_amount is a
    # source VARCHAR — kept verbatim, with a deliberate numeric cast alongside.
    "subaward_unique_key", "prime_award_unique_key", "subaward_number",
    "prime_award_piid", "prime_award_parent_piid", "usaspending_permalink",
    "subawardee_uei", "subawardee_parent_uei", "subawardee_name",
    "prime_awardee_uei", "prime_awardee_parent_uei", "prime_awardee_name",
    "subaward_amount", "subaward_action_date", "subaward_last_modified_date",
    "subaward_action_date_fiscal_year", "prime_award_naics_code",
    "prime_award_product_or_service_code", "prime_award_awarding_agency_code",
    "prime_award_awarding_agency_name", "prime_award_awarding_sub_agency_code",
    "prime_award_awarding_sub_agency_name", "subawardee_state_code",
    "subawardee_zip_code", "subawardee_country_code",
    "subaward_primary_place_of_performance_state_code",
    "subaward_primary_place_of_performance_country_code",
    "subaward_primary_place_of_performance_address_zip_code",
    "sub_place_of_perform_county_code", "sub_place_of_perform_county_name",
    "prime_awardee_state_code", "prime_awardee_country_code",
    "prime_award_primary_place_of_performance_state_code",
    "prime_award_primary_place_of_performance_country_code",
    "subaward_description",
]

MANIFEST: list[dict] = [
    # ── Tier A — market-grain core ────────────────────────────────────────────
    {"ds": "gtm_entity_behavior_rollup", "tier": "A", "sort": ["uei"]},
    {"ds": "gtm_sam_entities", "tier": "A", "sort": ["uei"]},
    {"ds": "gtm_entity_code_lanes", "tier": "A", "sort": ["uei", "code"]},
    {"ds": "gtm_entity_geo", "tier": "A", "sort": ["uei"]},
    {"ds": "gtm_naics_psc_pairs", "tier": "A", "sort": ["naics_code", "psc_code"]},
    {"ds": "naics_reference", "tier": "A", "sort": ["naics_code"]},
    {"ds": "psc_reference", "tier": "A", "sort": ["psc_code"]},
    # ── Tier B — Cycle B rollups (built-but-unwired; this is their serving lane)
    {"ds": "gtm_txn_events_slim", "tier": "B", "sort": ["uei", "action_date"]},
    {"ds": "gtm_txn_recipient_month_rollup", "tier": "B", "sort": ["uei"]},
    {"ds": "gtm_award_recipient_rollup", "tier": "B", "sort": ["uei"]},
    {"ds": "gtm_award_expiry_months", "tier": "B", "sort": ["uei", "end_month"]},
    {"ds": "gtm_prime_pop_lanes", "tier": "B", "sort": ["uei"]},
    # ── Tier C — benchmark-promoted giants (Phase 2 verdicts) ────────────────
    # award-grain rows + exact expiring: 96s live-lane -> ms-class local; also
    # removes the expiry_months month-grain approximation on two-lane phrases.
    {"ds": "usaspending_fpds_prime_award_state", "tier": "C", "sort": ["current_end_date"]},
    # inferred-code semi-join legs: sorted by (code_type, code) so a code
    # predicate prunes to a handful of row groups instead of a 263M/160M scan.
    {"ds": "gtm_entity_inferred_primeable_codes", "tier": "C", "sort": ["code_type", "code"]},
    {"ds": "gtm_entity_inferred_subbable_codes", "tier": "C", "sort": ["code_type", "code"]},
    # gtm_subaward_recipient_code_evidence (92M) stays OUT: no phrase.v2 shape
    # touches it (subout drill-down only) — remains gated pending a workload.
    # ── Tier D — recipe/relationship substrate ────────────────────────────────
    {"ds": "gtm_prime_sub_pairs", "tier": "D", "sort": ["prime_uei"]},
    {"ds": "gtm_prime_sub_pairs", "tier": "D", "sort": ["sub_uei"],
     "dest": "gtm_prime_sub_pairs_by_sub"},          # 2nd copy, sub-side clustering (269k rows — free)
    {"ds": "gtm_sub_universe_pairs", "tier": "D", "sort": ["target_uei"]},
    {"ds": "gtm_sub_universe_targets", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_prime_combo_lanes", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_sub_combo_lanes", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_prime_farmout_combo_lanes", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_prime_vehicle_lanes", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_open_awards", "tier": "D", "sort": ["recipient_uei"]},
    {"ds": "gtm_prime_demand_events", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_primes_by_recipient_code", "tier": "D", "sort": ["recipient_code"]},
    {"ds": "gtm_prime_subout_by_recipient_code", "tier": "D", "sort": ["prime_awardee_uei"]},
    {"ds": "gtm_subbed_under_to_primed_in_cooccurrence", "tier": "D", "sort": ["subbed_under_code"]},
    {"ds": "gtm_sub_profiles", "tier": "D", "sort": ["uei"]},
    {"ds": "govcon_subawardee_profiles", "tier": "D", "sort": ["sub_uei"]},
    {"ds": "usaspending_subaward_canonical", "tier": "D", "sort": ["prime_awardee_uei"],
     "cols": _SUBAWARD_COLS, "dest": "subaward_canonical_slim",
     "extra_select": "TRY_CAST(subaward_amount AS DOUBLE) AS subaward_amount_num"},
    {"ds": "usaspending_subaward_canonical", "tier": "D", "sort": ["subawardee_uei"],
     "cols": _SUBAWARD_COLS, "dest": "subaward_canonical_slim_by_sub",
     "extra_select": "TRY_CAST(subaward_amount AS DOUBLE) AS subaward_amount_num"},
    {"ds": "federal_sites_lance", "tier": "D", "sort": ["state_code", "zip5"]},
    {"ds": "firmographics_blitz", "tier": "D", "sort": ["domain_norm"]},
    {"ds": "gtm_sam_people", "tier": "D", "sort": ["uei"]},
    {"ds": "gtm_sam_person_contactability", "tier": "D", "sort": ["sam_person_id"]},
    {"ds": "sam_pocs", "tier": "D", "sort": ["uei"]},
    {"ds": "sam_master_entities", "tier": "D", "sort": ["uei"]},
    {"ds": "people_canonical", "tier": "D", "sort": ["canonical_person_id"]},
]

# agency vocab: deduped (code, name) off usaspending_award_canonical — mirrors
# market_store._dedupe_agency_pairs (NULL-guarded, majority name per code,
# lexicographic tiebreak). ~136 rows off 30.7M — streamed, never materialized wide.
_AGENCY_VOCAB_SQL = """
CREATE TABLE agency_vocab AS
WITH pairs AS (
    SELECT awarding_agency_code AS code, awarding_agency_name AS name, count(*) AS n
    FROM src
    WHERE awarding_agency_code IS NOT NULL AND awarding_agency_code <> ''
      AND awarding_agency_name IS NOT NULL AND awarding_agency_name <> ''
    GROUP BY 1, 2
)
SELECT code, name
FROM (SELECT code, name,
             row_number() OVER (PARTITION BY code ORDER BY n DESC, name) AS rn
      FROM pairs)
WHERE rn = 1
ORDER BY code
"""

_VIEWS: dict[str, str] = {
    # entity universe: identity + behavior posture + HQ geo, one row/uei
    "v_entity_universe": """
        CREATE VIEW v_entity_universe AS
        SELECT e.*, b.* EXCLUDE (uei), g.* EXCLUDE (uei)
        FROM gtm_sam_entities e
        LEFT JOIN gtm_entity_behavior_rollup b USING (uei)
        LEFT JOIN gtm_entity_geo g USING (uei)
    """,
    # teaming edges with both-side clustering available underneath
    "v_prime_sub_edges": """
        CREATE VIEW v_prime_sub_edges AS
        SELECT * FROM gtm_prime_sub_pairs
    """,
}

_CREATE_LEDGER_SQL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.query_sidecar_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text NOT NULL DEFAULT 'query_sidecar',
    tiers          text,
    marts          integer,
    rows_total     bigint,
    file_bytes     bigint,
    r2_key         text,
    latest_updated boolean,
    status         text NOT NULL,
    error_message  text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS query_sidecar_runs_status_idx ON ops.query_sidecar_runs (status);
CREATE INDEX IF NOT EXISTS query_sidecar_runs_recorded_idx ON ops.query_sidecar_runs (recorded_at DESC);
"""


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret."""
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


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    return boto3.client(
        "s3",
        endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _record_run(**fields) -> None:
    """Terminal-state ledger row. WARN-and-return on any failure — audit must not mask the build."""
    try:
        import psycopg

        dsn = os.environ.get("HQX_DB_URL_POOLED")
        if not dsn:
            print("[warn] HQX_DB_URL_POOLED unset; skipping ops ledger row")
            return
        cols = ", ".join(fields)
        ph = ", ".join(["%s"] * len(fields))
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO ops.query_sidecar_runs ({cols}) VALUES ({ph})",
                tuple(fields.values()),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] ops ledger write failed (non-fatal): {exc}")


def _build_one(con, so: dict[str, str], spec: dict) -> dict:
    """Stream one Lance mart into a sorted native DuckDB table. Returns the parity row."""
    import lance

    name, dest = spec["ds"], spec.get("dest", spec["ds"])
    ds = lance.dataset(f"{LANCE_BASE}{name}/", storage_options=so)
    pinned_version = ds.version
    lance_rows = ds.count_rows()

    cols = spec.get("cols")
    scanner = ds.scanner(columns=cols, batch_size=READ_BATCH_ROWS)
    reader = scanner.to_reader()          # single-pass — consumed exactly once by the CTAS
    con.register("src", reader)

    t0 = time.monotonic()
    if spec.get("agency_vocab"):
        con.execute(_AGENCY_VOCAB_SQL)
    else:
        extra = spec.get("extra_select")
        select = "SELECT *" + (f", {extra}" if extra else "")
        order = ", ".join(spec["sort"])
        con.execute(f'CREATE TABLE "{dest}" AS {select} FROM src ORDER BY {order}')
    con.unregister("src")

    duck_rows = con.execute(f'SELECT count(*) FROM "{dest}"').fetchone()[0]
    elapsed = round(time.monotonic() - t0, 1)
    # Aggregate tables (e.g. agency_vocab) REDUCE the source — their row count
    # can never equal the source count; parity there is non-emptiness.
    aggregate = bool(spec.get("agency_vocab"))
    row = {
        "table": dest, "dataset": name, "tier": spec["tier"],
        "sort": ",".join(spec.get("sort", [])) or None,
        "lance_version": pinned_version, "lance_rows": lance_rows,
        "duck_rows": duck_rows,
        "parity_ok": (duck_rows > 0) if aggregate else (duck_rows == lance_rows),
        "seconds": elapsed,
    }
    print(f"[mart] {dest}: {duck_rows:,} rows in {elapsed}s "
          f"(lance v{pinned_version}={lance_rows:,}) parity={'OK' if row['parity_ok'] else 'MISMATCH'}")
    return row


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    memory=131_072,          # 128 GiB — the >100M-row sort precedent (cms_medicare giant)
    cpu=8.0,
    ephemeral_disk=524_288,  # 512 GiB local NVMe: DuckDB spill + the output file
    timeout=60 * 60 * 12,
)
def build(tiers: str = "A,B,C,D", publish: bool = True, smoke: bool = False,
          trigger_callback_url: str | None = None) -> dict:
    """Build the query-sidecar .duckdb for the requested tiers; publish blue-green to R2."""
    import duckdb

    started_at = dt.datetime.now(dt.timezone.utc)
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    wanted = {t.strip().upper() for t in tiers.split(",") if t.strip()}
    specs = [s for s in MANIFEST if s["tier"] in wanted]
    # agency vocab rides with Tier D (its consumers are the market/phrase lanes)
    if "D" in wanted:
        specs.append({"ds": "usaspending_award_canonical", "tier": "D",
                      "cols": ["awarding_agency_code", "awarding_agency_name"],
                      "dest": "agency_vocab", "agency_vocab": True, "sort": []})

    os.makedirs(f"{SCRATCH_ROOT}/spill", exist_ok=True)
    db_path = f"{SCRATCH_ROOT}/query_sidecar_{stamp}.duckdb"
    so = _r2_storage_options()

    status, error_message, r2_key, latest_updated = "success", None, None, False
    parity: list[dict] = []
    file_bytes = 0
    try:
        con = duckdb.connect(db_path)
        try:
            # Out-of-core sort config — memory_limit BELOW the container cap
            # (cgroup auto-detect misreads), spill on local NVMe, and
            # preserve_insertion_order=true so the CTAS ORDER BY survives the
            # parallel insert (this is the one place true is required).
            con.execute(f"""
                SET memory_limit='96GB';
                SET threads=8;
                SET temp_directory='{SCRATCH_ROOT}/spill';
                SET max_temp_directory_size='400GB';
                SET preserve_insertion_order=true;
            """)
            for spec in specs:
                parity.append(_build_one(con, so, spec))

            mismatches = [p["table"] for p in parity if not p["parity_ok"]]
            if mismatches:
                raise RuntimeError(f"row-count parity failed for: {mismatches}")

            # bake build metadata + the parity manifest into the file itself
            con.execute("CREATE TABLE _sidecar_meta (built_at VARCHAR, tiers VARCHAR, source VARCHAR)")
            con.execute("INSERT INTO _sidecar_meta VALUES (?, ?, ?)",
                        [started_at.isoformat(), ",".join(sorted(wanted)),
                         "docs/plans/SIDECAR_PHASE0_MART_MANIFEST.md"])
            con.execute("""CREATE TABLE _sidecar_manifest (
                table_name VARCHAR, dataset VARCHAR, tier VARCHAR, sort_key VARCHAR,
                lance_version BIGINT, lance_rows BIGINT, duck_rows BIGINT, seconds DOUBLE)""")
            con.executemany(
                "INSERT INTO _sidecar_manifest VALUES (?,?,?,?,?,?,?,?)",
                [(p["table"], p["dataset"], p["tier"], p["sort"], p["lance_version"],
                  p["lance_rows"], p["duck_rows"], p["seconds"]) for p in parity])
            if "A" in wanted and "D" in wanted:
                for _vname, vsql in _VIEWS.items():
                    con.execute(vsql)
            con.execute("CHECKPOINT")
        finally:
            con.close()

        file_bytes = os.path.getsize(db_path)
        print(f"[build] {db_path}: {file_bytes/2**30:.2f} GiB, {len(parity)} tables")

        if publish:
            s3 = _s3_client()
            prefix = f"{R2_PREFIX}/smoke" if smoke else R2_PREFIX
            r2_key = f"{prefix}/query_sidecar_{stamp}.duckdb"
            s3.upload_file(db_path, R2_BUCKET, r2_key)   # boto3 multipart handles the size
            print(f"[publish] s3://{R2_BUCKET}/{r2_key}")
            if not smoke:
                pointer = {"key": r2_key, "built_at": started_at.isoformat(),
                           "file_bytes": file_bytes, "tiers": sorted(wanted),
                           "tables": [p["table"] for p in parity]}
                s3.put_object(Bucket=R2_BUCKET, Key=f"{R2_PREFIX}/LATEST.json",
                              Body=json.dumps(pointer, indent=1).encode(),
                              ContentType="application/json")
                latest_updated = True
                print(f"[publish] LATEST.json -> {r2_key}")
    except Exception as exc:  # noqa: BLE001
        status, error_message = "error", str(exc)[:2000]
        raise
    finally:
        _record_run(
            tiers=",".join(sorted(wanted)), marts=len(parity),
            rows_total=sum(p["duck_rows"] for p in parity),
            file_bytes=file_bytes, r2_key=r2_key, latest_updated=latest_updated,
            status=status, error_message=error_message,
            started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc),
        )
        if trigger_callback_url:
            _post_callback(trigger_callback_url, status, parity)

    return {"status": status, "r2_key": r2_key, "file_bytes": file_bytes,
            "tables": len(parity), "parity": parity}


def _post_callback(url: str, status: str, parity: list[dict]) -> None:
    import requests

    payload = {"status": status, "feed": "query_sidecar",
               "rows": sum(p["duck_rows"] for p in parity)}
    for attempt in range(3):
        try:
            requests.post(url, json=payload, timeout=30).raise_for_status()
            return
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] callback attempt {attempt + 1} failed: {exc}")
            time.sleep(2 ** attempt)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_schema() -> None:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_CREATE_LEDGER_SQL)
        conn.commit()
    print("[initdb] ops.query_sidecar_runs ready")


@app.local_entrypoint()
def initdb():
    init_schema.remote()


@app.local_entrypoint()
def run(tiers: str = "A,B,C,D"):
    result = build.remote(tiers=tiers, publish=True, smoke=False, trigger_callback_url=None)
    print(json.dumps({k: v for k, v in result.items() if k != "parity"}, indent=1))


@app.local_entrypoint()
def smoke():
    result = build.remote(tiers="A", publish=True, smoke=True, trigger_callback_url=None)
    print(json.dumps(result, indent=1, default=str))
