"""SUBAWARDEE SOLICITATION BRIDGE (Tier 1/2) — offline join + live resources probe.

Builds the solicitation-document substrate that layers on top of Tier 0 (subawardee_work_profile).
For every distinct subawardee_uei winning in the 90-day feed, resolve the three-hop bridge:

  subaward.prime_award_unique_key
    ⋈  transaction_search_fpds.generated_unique_award_id   → solicitation_identifier   (hop 1, offline)
    ⋈  sam-gov-opps.solicitation_number (normalized)       → notice_id                 (hop 2, offline)
    →  GET opps/v3/opportunities/{notice_id}/resources      → attachment manifest      (hop 3, live)

This is the TIER 1/2 phase: offline bridge first, then live resources probe.
Output grain is one row per (subawardee_uei, notice_id, attachment), with downloadable URLs.

Applies the diagnostic constraints (anomaly quarantine, base_type ranking) and
enumerates the winning notice per solicitation (prefer highest-tier base_type with attachments).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/usaspending/subawardee_solicitations.py <build_bridge|probe_resources|init_ops> [years]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time

# ─────────────────────────── constants ───────────────────────────

FEED = "subawardee_solicitations"

# URIs
PROFILE_URI = os.environ.get(
    "SUBAWARDEE_WORK_PROFILE_URI",
    "s3://data-sink/active/subawardee_work_profile",
).rstrip("/") + "/"

SUB_FRESH_URI = os.environ.get(
    "USASPENDING_API_SUBAWARD_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_subaward",
).rstrip("/") + "/"

FPDS_URI = "s3://data-sink/active/usaspending/transaction_search_fpds/"
SUBS_URI = "s3://data-sink/active/usaspending/subaward_search/"
SAM_OPPS_ACTIVE_URI = os.environ.get(
    "SAM_OPPS_LANCE_URI", "s3://data-sink/sam-gov-opps/active/"
)
SAM_OPPS_ARCHIVED_URI = "s3://data-sink/sam-gov-opps/archived/"

# Bridge & manifest output
BRIDGE_URI = os.environ.get(
    "SUBAWARDEE_SOLICITATIONS_BRIDGE_URI",
    "s3://data-sink/active/subawardee_solicitations_bridge",
).rstrip("/") + "/"

MANIFEST_URI = os.environ.get(
    "SUBAWARDEE_SOLICITATIONS_MANIFEST_URI",
    "s3://data-sink/active/subawardee_solicitations_manifest",
).rstrip("/") + "/"

DEFAULT_YEARS = 5
BATCH = 4000  # UEIs per index-pushdown scan
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
SCRATCH = os.environ.get("SUSOL_SCRATCH", "/tmp/subawardee_solicitations")

RESOURCES_URL = "https://sam.gov/api/prod/opps/v3/opportunities/{nid}/resources"
RESOURCES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://sam.gov",
}

# Anomaly constraints (from diagnostic)
MIN_SOLNUM_LEN = 8  # Filter out placeholder/umbrella solnums
MAX_NOTICES_PER_SOLNUM = 20  # Quarantine umbrella GWACs with explosive fanout
BASE_TYPE_PREFERENCE = [
    "Combined Synopsis/Solicitation",
    "Solicitation",
    "Presolicitation",
    "Sources Sought",
    "Special Notice",
    "Award Notice",
]


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID")
        else None
    )
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": ep,
        "region": "auto",
    }


def _inlist(vals):
    return ",".join("'" + v.replace("'", "''") + "'" for v in vals)


def _batched(seq, n):
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def _normalize_solnum(s):
    """Normalize solicitation number for SAM.gov opps join (upper, strip non-alphanumeric)."""
    if not s:
        return None
    return "".join(c for c in str(s).upper() if c.isalnum())


# ─────────────────────────── build bridge (offline) ───────────────────────────


def build_bridge(years=DEFAULT_YEARS):
    """Materialize the offline three-hop bridge without live API calls."""
    import duckdb
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    os.makedirs(SCRATCH, exist_ok=True)

    con = None
    status = "unknown"
    error = None
    bridge_count = 0

    try:
        con = duckdb.connect(":memory:")
        today = dt.datetime.now(dt.timezone.utc).date()
        cutoff = today - dt.timedelta(days=365 * years)

        log(f"build_bridge: {years}y window -> {cutoff}")

        # ── 1. Load subawardee_work_profile (25,450 entities) ──
        log("Loading subawardee_work_profile...")
        profile_ds = lance.dataset(PROFILE_URI, storage_options=so)
        profile_tbl = profile_ds.scanner(columns=["subawardee_uei"]).to_table()
        con.register("profile", profile_tbl)
        profile_count = con.execute("SELECT count(DISTINCT subawardee_uei) FROM profile").fetchone()[0]
        log(f"  profile: {profile_count} distinct subawardee UEIs")

        # ── 2. Load 90-day API-fresh subaward feed ──
        log("Loading 90-day API-fresh subaward feed...")
        fresh_ds = lance.dataset(SUB_FRESH_URI, storage_options=so)
        fresh_cols = [
            "subawardee_uei",
            "prime_award_unique_key",
            "subaward_number",
            "prime_awardee_uei",
            "subaward_action_date",
        ]
        fresh_tbl = fresh_ds.scanner(columns=fresh_cols).to_table()
        con.register("fresh", fresh_tbl)
        fresh_count = con.execute("SELECT count(DISTINCT subawardee_uei) FROM fresh").fetchone()[0]
        log(f"  fresh: {fresh_count} distinct subawardees")

        # ── 3. Build target universe: intersect profile UEIs with fresh subawards ──
        con.execute(f"""CREATE TABLE target_subawards AS
          SELECT
            f.subawardee_uei,
            f.prime_award_unique_key,
            f.subaward_number,
            f.prime_awardee_uei AS prime_uei,
            CAST(f.subaward_action_date AS DATE) AS sub_action_date
          FROM fresh f
          WHERE f.subawardee_uei IN (SELECT DISTINCT subawardee_uei FROM profile)
          AND CAST(f.subaward_action_date AS DATE) >= DATE '{cutoff}'
        """)
        target_count = con.execute("SELECT count(*) FROM target_subawards").fetchone()[0]
        log(f"  target: {target_count} subawards for {profile_count} subawardees")

        # ── 4. HOP 1: resolve solicitation_identifier via FPDS (prime_award_unique_key → generated_unique_award_id) ──
        log("HOP 1: resolving solicitation_identifier via FPDS...")
        fpds_ds = lance.dataset(FPDS_URI, storage_options=so)

        # Get unique prime_award_unique_keys to scan
        prime_keys = con.execute(
            "SELECT DISTINCT prime_award_unique_key FROM target_subawards ORDER BY 1"
        ).fetchall()
        prime_keys = [str(pk[0]) for pk in prime_keys]
        log(f"  scanning {len(prime_keys)} unique prime_award_unique_keys...")

        con.execute("""CREATE TABLE fpds_solnums(
          prime_award_unique_key VARCHAR,
          solicitation_identifier VARCHAR,
          action_date DATE
        )""")

        t0 = time.time()
        for bi, batch in enumerate(_batched(prime_keys, BATCH)):
            flt = f"generated_unique_award_id IN ({_inlist(batch)})"
            fpds_tbl = fpds_ds.scanner(
                columns=[
                    "generated_unique_award_id",
                    "solicitation_identifier",
                    "action_date",
                ],
                filter=flt,
            ).to_table()
            con.register("f", fpds_tbl)
            con.execute("""INSERT INTO fpds_solnums SELECT
              trim(generated_unique_award_id),
              nullif(trim(solicitation_identifier), ''),
              CAST(action_date AS DATE)
            FROM f""")
            con.unregister("f")
            log(f"  batch {bi+1}/{len(list(_batched(prime_keys, BATCH)))}: {fpds_tbl.num_rows} rows")

        # For each prime_award_unique_key, select the most recent solicitation_identifier
        con.execute("""CREATE TABLE fpds_best_solnum AS
          WITH r AS (SELECT *, row_number() OVER (
                PARTITION BY prime_award_unique_key ORDER BY action_date DESC) rn
            FROM fpds_solnums)
          SELECT prime_award_unique_key, solicitation_identifier FROM r WHERE rn = 1
        """)
        hop1_count = con.execute(
            "SELECT count(*) FILTER (WHERE solicitation_identifier IS NOT NULL) FROM fpds_best_solnum"
        ).fetchone()[0]
        log(f"  HOP 1 complete: {hop1_count} primes with solicitation_identifier in {(time.time()-t0)/60:.1f}m")

        # ── 5. Join target subawards with solnums ──
        con.execute("""CREATE TABLE subs_with_solnum AS
          SELECT
            ts.subawardee_uei,
            ts.prime_award_unique_key,
            ts.subaward_number,
            ts.prime_uei,
            ts.sub_action_date,
            fs.solicitation_identifier
          FROM target_subawards ts
          LEFT JOIN fpds_best_solnum fs USING (prime_award_unique_key)
          WHERE fs.solicitation_identifier IS NOT NULL
        """)
        subs_with_sol = con.execute("SELECT count(*) FROM subs_with_solnum").fetchone()[0]
        log(f"  subawards with solicitation_identifier: {subs_with_sol}")

        # ── 6. HOP 2: resolve notice_id via sam-gov-opps (solicitation_identifier → notice_id) ──
        log("HOP 2: resolving notice_id via sam-gov-opps...")

        # Get unique, clean solicitation_identifiers
        solnums_raw = con.execute(
            "SELECT DISTINCT solicitation_identifier FROM subs_with_solnum"
        ).fetchall()
        solnums_raw = [str(s[0]) for s in solnums_raw if s[0]]

        # Apply anomaly filter: ≥8 chars, strip non-alphanumeric
        solnums_clean = []
        for s in solnums_raw:
            norm = _normalize_solnum(s)
            if norm and len(norm) >= MIN_SOLNUM_LEN:
                solnums_clean.append(norm)

        log(f"  {len(solnums_clean)} clean solnums (filtered from {len(solnums_raw)} raw)")

        con.execute("""CREATE TABLE sam_opps_scan(
          solicitation_identifier_normalized VARCHAR,
          notice_id VARCHAR,
          base_type VARCHAR,
          notice_type VARCHAR,
          posted_date DATE
        )""")

        # Scan active SAM.gov opps
        # Note: SAM.gov uses solicitation_number (not solicitation_identifier_normalized);
        # we normalize both at join time
        for uri, label in [(SAM_OPPS_ACTIVE_URI, "active"), (SAM_OPPS_ARCHIVED_URI, "archived")]:
            try:
                sam_ds = lance.dataset(uri, storage_options=so)
                # Get all opps and normalize their solicitation numbers locally
                sam_all = sam_ds.scanner(columns=[
                    "solicitation_number",
                    "notice_id",
                    "base_type",
                    "notice_type",
                    "posted_date",
                ]).to_table()

                if sam_all.num_rows > 0:
                    con.register("s_all", sam_all)
                    con.execute(f"""INSERT INTO sam_opps_scan SELECT
                      upper(regexp_replace(solicitation_number, '[^A-Z0-9]', '', 'g')),
                      trim(notice_id),
                      nullif(trim(base_type), ''),
                      nullif(trim(notice_type), ''),
                      CAST(posted_date AS DATE)
                    FROM s_all
                    WHERE upper(regexp_replace(solicitation_number, '[^A-Z0-9]', '', 'g')) IN ({_inlist(solnums_clean)})
                    """)
                    con.unregister("s_all")
                    log(f"  {label}: {sam_all.num_rows} rows scanned")
            except Exception as e:
                log(f"  {label} scan failed (optional): {e}")

        # Filter out umbrella solnums (>MAX_NOTICES_PER_SOLNUM per solnum)
        con.execute(f"""CREATE TABLE sam_opps_filtered AS
          WITH cts AS (
            SELECT solicitation_identifier_normalized, count(DISTINCT notice_id) cnt
            FROM sam_opps_scan GROUP BY 1)
          SELECT s.* FROM sam_opps_scan s
          INNER JOIN cts ON cts.solicitation_identifier_normalized = s.solicitation_identifier_normalized
          WHERE cts.cnt <= {MAX_NOTICES_PER_SOLNUM}
        """)
        hop2_count = con.execute("SELECT count(DISTINCT notice_id) FROM sam_opps_filtered").fetchone()[0]
        log(f"  HOP 2 complete: {hop2_count} distinct notices resolved")

        # ── 7. Normalize solnums and join with notices ──
        con.execute("""CREATE TABLE subs_normalized AS
          SELECT
            subawardee_uei,
            prime_award_unique_key,
            subaward_number,
            solicitation_identifier,
            upper(regexp_replace(solicitation_identifier, '[^A-Z0-9]', '', 'g')) AS solnum_normalized,
            sub_action_date
          FROM subs_with_solnum
        """)

        con.execute("""CREATE TABLE bridge_raw AS
          SELECT
            sn.subawardee_uei,
            sn.prime_award_unique_key,
            sn.subaward_number,
            sn.solicitation_identifier,
            sn.sub_action_date,
            sf.notice_id,
            sf.base_type,
            sf.notice_type,
            sf.posted_date,
            row_number() OVER (
              PARTITION BY sn.solicitation_identifier ORDER BY
              CASE WHEN sf.base_type = 'Combined Synopsis/Solicitation' THEN 0
                   WHEN sf.base_type = 'Solicitation' THEN 1
                   WHEN sf.base_type = 'Presolicitation' THEN 2
                   WHEN sf.base_type = 'Sources Sought' THEN 3
                   WHEN sf.base_type = 'Special Notice' THEN 4
                   ELSE 5 END,
              sf.posted_date DESC) AS notice_rank
          FROM subs_normalized sn
          LEFT JOIN sam_opps_filtered sf ON
            sn.solnum_normalized = sf.solicitation_identifier_normalized
        """)

        # Select best notice per subaward (highest base_type tier)
        con.execute("""CREATE TABLE bridge AS
          SELECT
            subawardee_uei,
            prime_award_unique_key,
            subaward_number,
            solicitation_identifier,
            sub_action_date,
            notice_id,
            base_type,
            notice_type,
            posted_date
          FROM bridge_raw
          WHERE notice_rank = 1 AND notice_id IS NOT NULL
        """)

        bridge_count = con.execute("SELECT count(*) FROM bridge").fetchone()[0]
        bridge_notices = con.execute("SELECT count(DISTINCT notice_id) FROM bridge").fetchone()[0]
        log(f"  bridge materialized: {bridge_count} subawards → {bridge_notices} notices")

        # ── 8. Write bridge to Lance ──
        log("Writing bridge to Lance...")
        bridge_tbl = con.execute("SELECT * FROM bridge").arrow()
        if isinstance(bridge_tbl, pa.RecordBatchReader):
            bridge_tbl = bridge_tbl.read_all()

        lance.write_dataset(
            bridge_tbl,
            BRIDGE_URI,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            storage_options=so,
        )
        ds = lance.dataset(BRIDGE_URI, storage_options=so)
        ds.create_scalar_index("subawardee_uei", index_type="BTREE")
        ds.create_scalar_index("notice_id", index_type="BTREE")
        log(f"  BTREE subawardee_uei, notice_id")

        status = "bridge_built"
        log(f"DONE bridge: {bridge_count} rows → {BRIDGE_URI}")

    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
        log(f"ERROR: {error}")
        status = "bridge_failed"
        raise
    finally:
        if con:
            con.close()
        _record_run(
            years=years,
            phase="build_bridge",
            row_count=bridge_count,
            status=status,
            error=error,
            started=started,
            completed=dt.datetime.now(dt.timezone.utc),
        )


# ─────────────────────────── probe resources (live) ───────────────────────────


def probe_resources(inter_call_sleep=0.12):
    """Probe live SAM.gov resources endpoint and materialize manifest."""
    import duckdb
    import lance
    import pyarrow as pa
    import requests

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()

    con = None
    session = None
    status = "unknown"
    error = None
    manifest_tbl = None

    try:
        con = duckdb.connect(":memory:")
        session = requests.Session()

        # ── 1. Load bridge ──
        log("Loading bridge...")
        bridge_ds = lance.dataset(BRIDGE_URI, storage_options=so)
        bridge_tbl = bridge_ds.scanner(columns=[
            "subawardee_uei", "notice_id", "base_type", "posted_date"
        ]).to_table()
        con.register("bridge", bridge_tbl)

        # Build notice→subawardee_uei map (one subawardee per notice, preferring recent)
        con.execute("""CREATE TABLE notice_subawardees AS
          WITH r AS (
            SELECT notice_id, subawardee_uei, posted_date,
              row_number() OVER (PARTITION BY notice_id ORDER BY posted_date DESC) rn
            FROM bridge)
          SELECT notice_id, subawardee_uei FROM r WHERE rn = 1
        """)

        notices = con.execute(
            "SELECT DISTINCT notice_id FROM bridge ORDER BY 1"
        ).fetchall()
        notices = [str(n[0]) for n in notices]
        log(f"  {len(notices)} distinct notices to probe")

        # ── 2. Probe resources endpoint ──
        log("Probing resources endpoints...")
        con.execute("""CREATE TABLE manifest(
          subawardee_uei VARCHAR,
          notice_id VARCHAR,
          resource_id VARCHAR,
          attachment_name VARCHAR,
          mime_type VARCHAR,
          size_bytes BIGINT,
          download_url VARCHAR,
          access_level VARCHAR
        )""")

        succeeded = 0
        failed = 0
        total_attachments = 0

        for i, nid in enumerate(notices):
            try:
                url = RESOURCES_URL.format(nid=nid)
                headers = {**RESOURCES_HEADERS, "Referer": f"https://sam.gov/opp/{nid}/view"}

                resp = session.get(url, headers=headers, timeout=10)
                resp.raise_for_status()

                data = resp.json()
                attachments = []

                # Extract attachments from nested structure
                if "_embedded" in data and "opportunityAttachmentList" in data["_embedded"]:
                    for oal in data["_embedded"]["opportunityAttachmentList"]:
                        if "attachments" in oal:
                            attachments.extend(oal["attachments"])

                if attachments:
                    # Get subawardee_uei for this notice
                    subawardee_row = con.execute(
                        "SELECT subawardee_uei FROM notice_subawardees WHERE notice_id = ?",
                        (nid,)
                    ).fetchone()
                    subawardee_uei = subawardee_row[0] if subawardee_row else None

                    for att in attachments:
                        con.execute(
                            """INSERT INTO manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (subawardee_uei, nid, att.get("resourceId"), att.get("name"),
                             att.get("mimeType"), att.get("size"),
                             f"https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{att.get('resourceId')}/download",
                             att.get("accessLevel"))
                        )
                    total_attachments += len(attachments)

                succeeded += 1
                if (i + 1) % 100 == 0:
                    log(f"  probed {i+1}/{len(notices)}: {succeeded} ok, {failed} fail, {total_attachments} attachments")

            except Exception as e:  # noqa: BLE001
                failed += 1
                log(f"  notice {nid}: {type(e).__name__}: {str(e)[:100]}")

            # Rate limiting
            time.sleep(inter_call_sleep)

        log(f"  probing complete: {succeeded} ok, {failed} fail, {total_attachments} total attachments")

        # ── 3. Write manifest to Lance ──
        log("Writing manifest to Lance...")
        manifest_tbl = con.execute("SELECT * FROM manifest").arrow()
        if isinstance(manifest_tbl, pa.RecordBatchReader):
            manifest_tbl = manifest_tbl.read_all()

        if manifest_tbl.num_rows > 0:
            lance.write_dataset(
                manifest_tbl,
                MANIFEST_URI,
                mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE,
                storage_options=so,
            )
            ds = lance.dataset(MANIFEST_URI, storage_options=so)
            ds.create_scalar_index("notice_id", index_type="BTREE")
            ds.create_scalar_index("subawardee_uei", index_type="BTREE")
            log(f"  BTREE notice_id, subawardee_uei")

        status = "resources_probed"
        log(f"DONE resources: {manifest_tbl.num_rows} attachments → {MANIFEST_URI}")

    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
        log(f"ERROR: {error}")
        status = "resources_failed"
        raise
    finally:
        if con:
            con.close()
        if session:
            session.close()
        _record_run(
            phase="probe_resources",
            row_count=manifest_tbl.num_rows if manifest_tbl else 0,
            status=status,
            error=error,
            started=started,
            completed=dt.datetime.now(dt.timezone.utc),
        )


# ─────────────────────────── ops ledger ───────────────────────────


def _record_run(
    *,
    phase,
    row_count,
    status,
    error,
    started,
    completed,
    years=None,
):
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping ops row")
        return

    if status not in ("bridge_built", "resources_probed"):
        if not error:
            error = "unknown terminal failure"

    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS ops.subawardee_solicitations_runs (
                   id bigserial PRIMARY KEY, feed text NOT NULL, phase text,
                   row_count int, status text NOT NULL, error_message text,
                   started_at timestamptz, executed_at timestamptz)"""
            )
            cur.execute(
                """INSERT INTO ops.subawardee_solicitations_runs
                   (feed, phase, row_count, status, error_message, started_at, executed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    FEED,
                    phase,
                    int(row_count),
                    status,
                    (error or "")[:2000] or None,
                    started,
                    completed,
                ),
            )
            c.commit()
        log(f"ops row: phase={phase} status={status}")
    except Exception as e:  # noqa: BLE001
        log(f"WARN: ops write failed: {e}")


def init_ops():
    """Initialize ops table schema."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        log("WARN: no HQX dsn; skipping init")
        return

    try:
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS ops.subawardee_solicitations_runs (
                   id bigserial PRIMARY KEY, feed text NOT NULL, phase text,
                   row_count int, status text NOT NULL, error_message text,
                   started_at timestamptz, executed_at timestamptz)"""
            )
            c.commit()
        log("ops table initialized")
    except Exception as e:  # noqa: BLE001
        log(f"WARN: init failed: {e}")


# ─────────────────────────── cli ───────────────────────────


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build_bridge"
    years = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_YEARS

    if cmd == "init_ops":
        init_ops()
    elif cmd == "build_bridge":
        build_bridge(years)
    elif cmd == "probe_resources":
        probe_resources()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
