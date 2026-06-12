"""SUBAWARDEE SOLICITATION BRIDGE (Tier 1/2) — offline join + manifest materialization.

Builds the solicitation-document substrate that layers on top of Tier 0 (subawardee_work_profile).
For every distinct subawardee_uei winning in the 90-day feed, resolve the three-hop bridge:

  subaward.prime_award_unique_key
    ⋈  transaction_search_fpds.generated_unique_award_id   → solicitation_identifier   (hop 1, offline)
    ⋈  sam-gov-opps.solicitation_number (normalized)       → notice_id                 (hop 2, offline)
    →  attachment manifest at (notice_id, resource_id)      → subawardee_solicitations_manifest (hop 3)

GRAIN LAW (plan Phase-0 item 5 — the 670-row defect this file used to carry):
  * BRIDGE grain = one row per (subawardee_uei, prime_award_unique_key, subaward_number) — the
    notice election window partitions by the FULL subaward grain, never by solicitation_identifier
    alone (that collapsed 15–20K subaward rows to one row per solnum). UEI↔notice mapping stays
    many-to-many; grain uniqueness is hard-asserted before write.
  * Notice election = highest base_type tier WITH attachments: known-attachment notices (signal =
    union of already-harvested sam_opps_attachment_manifest* datasets + this module's own manifest)
    rank ahead of unknown ones, then base_type tier, then posted_date DESC. Pure tier ranking picks
    an attachment-empty notice ~14% of the time (plan §Phase-0).
  * MANIFEST grain = one row per (notice_id, resource_id) — no rn=1 subawardee collapse, no UEI
    column; consumers join bridge→manifest on notice_id. Populated OFFLINE-FIRST from the harvested
    sam_opps_attachment_manifest* datasets; only the residual notice gap goes to the live SAM
    resources endpoint — single-threaded, JSONL-checkpointed ({SCRATCH}/manifest_probe.jsonl, one
    line per probed notice), resumable by re-running build_manifest (terminal-status notices skip).

Hop-1 FPDS scan results are window-cached to parquet in SCRATCH (same resume pattern as
subawardee_work_profile.py) so a crashed bridge build never re-scans.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 pipelines/usaspending/subawardee_solicitations.py <build_bridge|build_manifest|init_ops> [years|offline_only]
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

# Bridge row-count band predicted by the plan (§1) — logged gate, never a hard fail:
# the actual measured count is always reported.
BRIDGE_BAND = (15_000, 20_000)

# Offline attachment-manifest datasets (already harvested) — the attachment-presence prior for
# notice election AND the offline-first source for the hop-3 manifest. Order = preference order
# when the same (notice_id, resource_id) appears in several sources (after 'live').
OFFLINE_MANIFEST_SOURCES = [
    ("offline:sam_opps_attachment_manifest_90day_winners",
     "s3://data-sink/active/sam_opps_attachment_manifest_90day_winners/"),
    ("offline:sam_opps_attachment_manifest",
     "s3://data-sink/active/sam_opps_attachment_manifest/"),
]

# Live-probe JSONL checkpoint: one line per probed notice; resume skips terminal statuses,
# retries transient ones.
PROBE_CHECKPOINT = os.path.join(SCRATCH, "manifest_probe.jsonl")
PROBE_TERMINAL_STATUSES = {"ok", "empty", "http_404"}


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


# SQL twin of _normalize_solnum. upper() MUST run before regexp_replace — the old inverted order
# stripped lowercase letters before uppercasing, silently breaking the join for any solnum with
# lowercase characters (the "raw join path" defect; both sides now use this one expression).
SQL_NORM = "regexp_replace(upper({col}), '[^A-Z0-9]', '', 'g')"


def _known_attachment_notice_ids(so):
    """Attachment-presence prior for notice election: distinct notice_ids across every
    already-harvested attachment manifest (incl. this module's own, so post-live-probe rebuilds
    self-improve). Sources are optional — absence logs and continues."""
    import lance

    ids = set()
    for label, uri in [*OFFLINE_MANIFEST_SOURCES, ("self:subawardee_solicitations_manifest", MANIFEST_URI)]:
        try:
            ds = lance.dataset(uri, storage_options=so)
            tbl = ds.scanner(columns=["notice_id"]).to_table()
            n0 = len(ids)
            ids.update(v for v in tbl.column("notice_id").to_pylist() if v)
            log(f"  attachment signal {label}: +{len(ids) - n0} notice_ids ({tbl.num_rows} rows)")
        except Exception as e:  # noqa: BLE001
            log(f"  attachment signal {label} unavailable (optional): {type(e).__name__}")
    return ids


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
        # Collapse FFATA/FSRS re-pull duplicates to true subaward grain
        # (subawardee_uei, prime_award_unique_key, subaward_number) — the bridge grain.
        con.execute(f"""CREATE TABLE target_subawards AS
          SELECT
            subawardee_uei,
            prime_award_unique_key,
            subaward_number,
            arg_max(prime_uei, sub_action_date) AS prime_uei,
            max(sub_action_date)                AS sub_action_date
          FROM (
            SELECT
              trim(f.subawardee_uei)               AS subawardee_uei,
              trim(f.prime_award_unique_key)       AS prime_award_unique_key,
              trim(f.subaward_number)              AS subaward_number,
              f.prime_awardee_uei                  AS prime_uei,
              CAST(f.subaward_action_date AS DATE) AS sub_action_date
            FROM fresh f
            WHERE f.subawardee_uei IN (SELECT DISTINCT subawardee_uei FROM profile)
            AND CAST(f.subaward_action_date AS DATE) >= DATE '{cutoff}'
          ) GROUP BY 1, 2, 3
        """)
        target_count = con.execute("SELECT count(*) FROM target_subawards").fetchone()[0]
        log(f"  target: {target_count} distinct subawards for {profile_count} subawardees")

        # ── 4. HOP 1: resolve solicitation_identifier via FPDS (prime_award_unique_key → generated_unique_award_id) ──
        log("HOP 1: resolving solicitation_identifier via FPDS...")
        fpds_ds = lance.dataset(FPDS_URI, storage_options=so)

        # Get unique prime_award_unique_keys to scan
        prime_keys = con.execute(
            "SELECT DISTINCT prime_award_unique_key FROM target_subawards ORDER BY 1"
        ).fetchall()
        prime_keys = [str(pk[0]) for pk in prime_keys]
        log(f"  scanning {len(prime_keys)} unique prime_award_unique_keys...")

        # Window-cached parquet checkpoint (resume pattern from subawardee_work_profile.py):
        # a crashed/killed build never re-scans FPDS for the same window.
        fpds_cache = f"{SCRATCH}/fpds_solnums_{cutoff}.parquet"
        if os.path.exists(fpds_cache):
            con.execute(f"CREATE TABLE fpds_solnums AS SELECT * FROM '{fpds_cache}'")
            log(f"  HOP 1: loaded cache {fpds_cache}")
        else:
            con.execute("""CREATE TABLE fpds_solnums(
              prime_award_unique_key VARCHAR,
              solicitation_identifier VARCHAR,
              action_date DATE
            )""")

            t0 = time.time()
            batches = _batched(prime_keys, BATCH)
            for bi, batch in enumerate(batches):
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
                log(f"  batch {bi+1}/{len(batches)}: {fpds_tbl.num_rows} rows")
            con.execute(f"COPY fpds_solnums TO '{fpds_cache}' (FORMAT parquet)")
            log(f"  HOP 1 scan done in {(time.time()-t0)/60:.1f}m -> cached {fpds_cache}")

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
        log(f"  HOP 1 complete: {hop1_count} primes with solicitation_identifier")

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
                    norm = SQL_NORM.format(col="solicitation_number")
                    con.register("s_all", sam_all)
                    con.execute(f"""INSERT INTO sam_opps_scan SELECT
                      {norm},
                      trim(notice_id),
                      nullif(trim(base_type), ''),
                      nullif(trim(notice_type), ''),
                      CAST(posted_date AS DATE)
                    FROM s_all
                    WHERE {norm} IN ({_inlist(solnums_clean)})
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
        con.execute(f"""CREATE TABLE subs_normalized AS
          SELECT
            subawardee_uei,
            prime_award_unique_key,
            subaward_number,
            solicitation_identifier,
            {SQL_NORM.format(col="solicitation_identifier")} AS solnum_normalized,
            sub_action_date
          FROM subs_with_solnum
        """)

        # Attachment-presence prior (offline manifests + own manifest) for notice election.
        log("Loading attachment-presence signal for notice election...")
        att_ids = sorted(_known_attachment_notice_ids(so))
        att_tbl = pa.table({"notice_id": pa.array(att_ids, type=pa.string())})
        con.register("att_src", att_tbl)
        con.execute("CREATE TABLE notices_with_attachments AS SELECT DISTINCT notice_id FROM att_src")
        con.unregister("att_src")
        log(f"  {len(att_ids)} notice_ids with known attachments")

        # Notice election (plan Phase-0 item 5): PARTITION BY the FULL subaward grain — never by
        # solicitation_identifier alone (the 670-row collapse). Rank: known-attachment first
        # (pure tier ranking picks an attachment-empty notice ~14% of the time), then base_type
        # tier, then posted_date DESC.
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
            (nwa.notice_id IS NOT NULL) AS has_known_attachments,
            row_number() OVER (
              PARTITION BY sn.subawardee_uei, sn.prime_award_unique_key, sn.subaward_number
              ORDER BY
              (nwa.notice_id IS NOT NULL) DESC,
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
          LEFT JOIN notices_with_attachments nwa ON
            nwa.notice_id = sf.notice_id
        """)

        # Select best notice per subaward (attachment-first, then highest base_type tier)
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
            posted_date,
            has_known_attachments
          FROM bridge_raw
          WHERE notice_rank = 1 AND notice_id IS NOT NULL
        """)

        bridge_count = con.execute("SELECT count(*) FROM bridge").fetchone()[0]
        bridge_notices = con.execute("SELECT count(DISTINCT notice_id) FROM bridge").fetchone()[0]
        bridge_with_att = con.execute(
            "SELECT count(*) FROM bridge WHERE has_known_attachments").fetchone()[0]
        log(f"  bridge materialized: {bridge_count} subawards → {bridge_notices} notices "
            f"({bridge_with_att} rows on known-attachment notices)")

        # HARD grain-uniqueness assert: bridge grain is (subawardee_uei, prime_award_unique_key,
        # subaward_number) — any duplicate means the election window regressed.
        grain_dupes = con.execute("""SELECT count(*) - count(DISTINCT
            (subawardee_uei, prime_award_unique_key, subaward_number)) FROM bridge""").fetchone()[0]
        if grain_dupes:
            raise RuntimeError(
                f"bridge grain violation: {grain_dupes} duplicate "
                f"(subawardee_uei, prime_award_unique_key, subaward_number) rows")
        log("  grain-uniqueness assert: PASS")

        # Plan band gate (15–20K) — logged, never fabricated: the measured count stands.
        lo, hi = BRIDGE_BAND
        if lo <= bridge_count <= hi:
            log(f"  band gate: PASS ({bridge_count} in [{lo}, {hi}])")
        else:
            log(f"  band gate: OUTSIDE BAND ({bridge_count} not in [{lo}, {hi}]) — investigate before relying on coverage claims")

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


# ─────────────── manifest (hop 3): offline-first + checkpointed live residual ───────────────


def _load_probe_checkpoint():
    """Read the JSONL probe checkpoint → {notice_id: last_status}. Malformed lines are skipped
    (a crash mid-write leaves at most one torn trailing line)."""
    seen = {}
    if os.path.exists(PROBE_CHECKPOINT):
        with open(PROBE_CHECKPOINT, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    seen[rec["notice_id"]] = rec.get("status", "error")
                except (json.JSONDecodeError, KeyError):
                    continue
    return seen


def _probe_notices_live(notices, inter_call_sleep=0.12):
    """Probe the live SAM.gov resources endpoint for the given notice_ids — single-threaded,
    JSONL-checkpointed per notice (append + flush), resumable. stdlib urllib only (no third-party
    HTTP dependency). Returns (probed_ok, failed)."""
    import urllib.error
    import urllib.request

    seen = _load_probe_checkpoint()
    todo = [n for n in notices if seen.get(n) not in PROBE_TERMINAL_STATUSES]
    log(f"  live probe: {len(notices)} residual notices, {len(notices) - len(todo)} already "
        f"terminal in checkpoint, {len(todo)} to probe")

    probed_ok = failed = 0
    with open(PROBE_CHECKPOINT, "a", encoding="utf-8") as ck:
        for i, nid in enumerate(todo):
            rec = {"notice_id": nid,
                   "ts": dt.datetime.now(dt.timezone.utc).isoformat()}
            try:
                url = RESOURCES_URL.format(nid=nid)
                headers = {**RESOURCES_HEADERS, "Referer": f"https://sam.gov/opp/{nid}/view"}
                req = urllib.request.Request(url, headers=headers)
                try:
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                    atts = []
                    for oal in data.get("_embedded", {}).get("opportunityAttachmentList", []):
                        atts.extend(oal.get("attachments") or [])
                    rec["status"] = "ok" if atts else "empty"
                    rec["attachments"] = [
                        {"resource_id": a.get("resourceId"), "name": a.get("name"),
                         "mime_type": a.get("mimeType"), "size_bytes": a.get("size"),
                         "access_level": a.get("accessLevel")}
                        for a in atts]
                except urllib.error.HTTPError as he:
                    if he.code == 404:
                        rec["status"] = "http_404"
                    else:
                        raise
                probed_ok += 1
            except Exception as e:  # noqa: BLE001
                rec["status"] = f"error:{type(e).__name__}"
                failed += 1
                log(f"  notice {nid}: {type(e).__name__}: {str(e)[:100]}")
            ck.write(json.dumps(rec) + "\n")
            ck.flush()
            if (i + 1) % 100 == 0:
                log(f"  probed {i+1}/{len(todo)}: {probed_ok} ok, {failed} fail")
            time.sleep(inter_call_sleep)
    log(f"  live probe complete: {probed_ok} ok, {failed} fail")
    return probed_ok, failed


def build_manifest(offline_only=False, inter_call_sleep=0.12):
    """Materialize subawardee_solicitations_manifest at (notice_id, resource_id) grain.

    OFFLINE-FIRST: bridge notice_ids are resolved against the already-harvested
    sam_opps_attachment_manifest* datasets; only the residual notice gap hits the live SAM
    resources endpoint (single-threaded, JSONL-checkpointed, resumable — re-run this command to
    resume). No rn=1 subawardee collapse: the manifest carries NO UEI column; the UEI↔notice
    mapping stays many-to-many in the bridge.
    """
    import duckdb
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    os.makedirs(SCRATCH, exist_ok=True)
    run_id = f"susol_manifest_{started.strftime('%Y%m%dT%H%M%SZ')}"

    con = None
    status = "unknown"
    error = None
    manifest_rows = 0
    coverage = {}

    try:
        con = duckdb.connect(":memory:")

        # ── 1. Bridge notice universe ──
        log("Loading bridge notice_ids...")
        bridge_ds = lance.dataset(BRIDGE_URI, storage_options=so)
        bridge_tbl = bridge_ds.scanner(columns=["notice_id"]).to_table()
        notices = sorted({v for v in bridge_tbl.column("notice_id").to_pylist() if v})
        log(f"  {len(notices)} distinct bridge notices")

        # ── 2. Offline pass over harvested manifests ──
        con.execute("""CREATE TABLE manifest_pool(
          notice_id VARCHAR, resource_id VARCHAR, attachment_name VARCHAR,
          mime_type VARCHAR, size_bytes BIGINT, download_url VARCHAR,
          access_level VARCHAR, source VARCHAR, source_rank INTEGER
        )""")
        for rank, (label, uri) in enumerate(OFFLINE_MANIFEST_SOURCES, start=1):
            try:
                ds = lance.dataset(uri, storage_options=so)
                got = 0
                for batch in _batched(notices, BATCH):
                    tbl = ds.scanner(
                        columns=["notice_id", "resource_id", "file_name", "mime_type",
                                 "size_bytes", "download_url", "access_level"],
                        filter=f"notice_id IN ({_inlist(batch)})",
                    ).to_table()
                    if tbl.num_rows:
                        con.register("o", tbl)
                        con.execute(f"""INSERT INTO manifest_pool SELECT
                          trim(notice_id), nullif(trim(resource_id), ''), file_name,
                          mime_type, size_bytes, download_url, access_level,
                          '{label}', {rank}
                        FROM o WHERE nullif(trim(resource_id), '') IS NOT NULL""")
                        con.unregister("o")
                        got += tbl.num_rows
                log(f"  {label}: {got} attachment rows matched")
            except Exception as e:  # noqa: BLE001
                log(f"  {label} unavailable (optional): {type(e).__name__}")

        covered_offline = {r[0] for r in con.execute(
            "SELECT DISTINCT notice_id FROM manifest_pool").fetchall()}
        log(f"  offline coverage: {len(covered_offline)}/{len(notices)} notices")

        # ── 3. Live residual (checkpointed, resumable) ──
        residual = [n for n in notices if n not in covered_offline]
        if offline_only:
            log(f"  offline_only: skipping live probe of {len(residual)} residual notices")
        elif residual:
            _probe_notices_live(residual, inter_call_sleep=inter_call_sleep)

        # Fold ALL checkpointed live results in (also from prior runs), residual notices only.
        residual_set = set(residual)
        live_notices = set()
        if os.path.exists(PROBE_CHECKPOINT):
            live_rows = []
            for nid, status_str, atts in _iter_checkpoint_attachments():
                if nid not in residual_set or status_str != "ok":
                    continue
                live_notices.add(nid)
                for a in atts:
                    rid = (a.get("resource_id") or "").strip()
                    if not rid:
                        continue
                    live_rows.append((
                        nid, rid, a.get("name"), a.get("mime_type"),
                        a.get("size_bytes"),
                        f"https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{rid}/download",
                        a.get("access_level"), "live", 0))
            if live_rows:
                con.executemany(
                    "INSERT INTO manifest_pool VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", live_rows)
            log(f"  live checkpoint folded in: {len(live_rows) if live_rows else 0} rows "
                f"from {len(live_notices)} notices")

        # ── 4. Materialize at (notice_id, resource_id) grain ──
        con.execute(f"""CREATE TABLE manifest AS
          WITH r AS (
            SELECT *, row_number() OVER (
              PARTITION BY notice_id, resource_id
              ORDER BY source_rank ASC) rn
            FROM manifest_pool)
          SELECT notice_id, resource_id, attachment_name, mime_type, size_bytes,
                 download_url, access_level, source,
                 TIMESTAMPTZ '{started.isoformat()}' AS harvested_at,
                 '{run_id}' AS run_id
          FROM r WHERE rn = 1
        """)

        manifest_rows = con.execute("SELECT count(*) FROM manifest").fetchone()[0]
        grain_dupes = con.execute(
            "SELECT count(*) - count(DISTINCT (notice_id, resource_id)) FROM manifest"
        ).fetchone()[0]
        if grain_dupes:
            raise RuntimeError(f"manifest grain violation: {grain_dupes} duplicate "
                               f"(notice_id, resource_id) rows")

        covered_total = {r[0] for r in con.execute(
            "SELECT DISTINCT notice_id FROM manifest").fetchall()}
        coverage = {
            "bridge_notices": len(notices),
            "covered_offline": len(covered_offline),
            "covered_live": len(covered_total - covered_offline),
            "covered_total": len(covered_total),
            "coverage_fraction": round(len(covered_total) / max(len(notices), 1), 4),
        }
        log(f"  coverage: {json.dumps(coverage)}")

        # ── 5. Write manifest to Lance ──
        if manifest_rows == 0:
            raise RuntimeError("0 manifest rows assembled — refusing to overwrite")
        log("Writing manifest to Lance...")
        manifest_tbl = con.execute("SELECT * FROM manifest").arrow()
        if isinstance(manifest_tbl, pa.RecordBatchReader):
            manifest_tbl = manifest_tbl.read_all()
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
        ds.create_scalar_index("resource_id", index_type="BTREE")
        log("  BTREE notice_id, resource_id")

        status = "manifest_built"
        log(f"DONE manifest: {manifest_rows} rows → {MANIFEST_URI}")

    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
        log(f"ERROR: {error}")
        status = "manifest_failed"
        raise
    finally:
        if con:
            con.close()
        _record_run(
            phase="build_manifest",
            row_count=manifest_rows,
            status=status,
            error=error,
            started=started,
            completed=dt.datetime.now(dt.timezone.utc),
        )
    return coverage


def _iter_checkpoint_attachments():
    """Yield (notice_id, status, attachments) for the LAST checkpoint record per notice
    (later lines supersede earlier retries)."""
    last = {}
    with open(PROBE_CHECKPOINT, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                last[rec["notice_id"]] = rec
            except (json.JSONDecodeError, KeyError):
                continue
    for nid, rec in last.items():
        yield nid, rec.get("status", "error"), rec.get("attachments") or []


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

    if status not in ("bridge_built", "resources_probed", "manifest_built"):
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
    arg2 = sys.argv[2] if len(sys.argv) > 2 else None

    if cmd == "init_ops":
        init_ops()
    elif cmd == "build_bridge":
        build_bridge(int(arg2) if arg2 else DEFAULT_YEARS)
    elif cmd in ("build_manifest", "probe_resources"):  # probe_resources kept as legacy alias
        build_manifest(offline_only=(arg2 == "offline_only"))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
