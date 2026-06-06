"""Compute worker — SAM entity master family (v2-only, faithful mirror), fail-safe + dispatcher-ready.

Builds three Lance datasets from raw ``entity_registrations`` in ONE v2 scan:
  - sam_master_entities  : 1 row / uei, latest-row-per-uei across all v2 snapshots, every
                           public field named per the FROZEN dictionary map, + parsed
                           array siblings, is_active, and cheap tenure aggregates.
  - sam_master_contacts  : the 6 POC blocks unpivoted to ≤6 rows / uei.
  - sam_master_domains   : entity_url normalized → (normalized_domain, uei) index.

Column names are the EXACT SAM dictionary field names (faithful slug); the only alias is
``uei`` for UNIQUE ENTITY ID. The projection is generated from
``pipelines/sam_gov/reference/sam_v2_public_field_map.py`` — the worker hardcodes no positions.

Dispatcher-ready: the field map + sam_labels are MOUNTED into the image, so when ``sql`` is
omitted (the dispatcher path) the function generates its own SQL in-container. The local
entrypoint still passes ``sql=`` (harmless). Fail-safe: pre-write gates abort before any of
the three writes; write + indexing + post-write gates run under one rollback guard that
restores all three datasets to their pre-write versions (net-new partial failures raise loud).

    modal run    pipelines/sam_gov/sam_master.py --dry-run   # gates + counts, no write
    modal run    pipelines/sam_gov/sam_master.py             # build + publish (prod)
    modal deploy pipelines/sam_gov/sam_master.py             # dispatcher-resolvable
"""
from __future__ import annotations

import os

import modal

from core.ops_alert import alert

SRC_URI = "s3://data-sink/active/entity_registrations/"
_PROD_PREFIX = "s3://data-sink/active/"


def _uris_for(prefix: str) -> dict[str, str]:
    return {"entities": prefix + "sam_master_entities/",
            "contacts": prefix + "sam_master_contacts/",
            "domains": prefix + "sam_master_domains/"}


def _feed_for(prefix: str) -> str:
    """Ledger feed tag: prod prefix → 'sam_master'; any override (scratch/validation) →
    'sam_master_scratch' so validation never poisons the prod Δ-baseline."""
    return "sam_master" if prefix == _PROD_PREFIX else "sam_master_scratch"


FEED = "sam_master"  # prod default; the effective feed is derived per-run from the prefix

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

# ── gate constants (live ops.sam_master_runs: entities 1,541,566 · contacts 4,373,319 ·
# domains 709,546; floors ~20-30% below live, per-family Δ ±25%) ──
ENTITIES_ROW_FLOOR = 1_400_000        # tight floor on the critical 1:1 spine
CONTACTS_FLOOR = 3_000_000            # catastrophic floor BELOW the ±25% Δ-band (Δ-lower ~3.28M),
DOMAINS_FLOOR = 450_000              # so the per-family Δ is the binding sensitive check (catches
                                     # a 25-30% projection-regression collapse the floor would miss)
BASELINE_MIN_ENTITIES = 1_450_000     # a success must clear this to qualify as a Δ baseline
DELTA_GUARD = 0.25                    # ±25% per-family vs prior healthy success
NAME_ALPHA_MIN = 0.95                 # legal_business_name alpha-frac (positional-offset defense)
NAICS_NUMERIC_MIN = 0.95              # primary_naics numeric-frac (gated only if fill high enough)
NAICS_FILL_MIN_FOR_GATE = 0.60        # below this, NAICS-numeric is observational, not gated (D8)

# BTREE keys per dataset.
ENTITIES_BTREE = ["uei", "primary_naics", "cage_code"]
CONTACTS_BTREE = ["uei"]
DOMAINS_BTREE = ["normalized_domain", "uei"]

DUCKDB_MEMORY_LIMIT = "110GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/ddspill"

# Structural roles within the 142-field public layout (positions are 1-based).
POC_POSITIONS = range(47, 113)            # six 11-field POC blocks → contacts
DROP_POSITIONS = {2} | set(range(123, 143))  # blank-deprecated + parent-EVS + flex + end-marker
POC_BLOCKS = ["govt_business", "alt_govt_business", "past_performance",
              "alt_past_performance", "electronic_business", "alt_electronic_business"]
CONTACT_COLS = ["first_name", "middle_initial", "last_name", "title", "st_add_1", "st_add_2",
                "city", "zip_postal_code", "zip_code_4", "country_code", "state_or_province"]
# verbatim `*_string` column → (parsed-sibling name, naics_strip?)
LIST_SIBLINGS = {
    "bus_type_string": ("business_types", False),
    "naics_code_string": ("naics_codes", True),
    "psc_code_string": ("psc_codes", False),
    "naics_exception_string": ("naics_exception_codes", True),
    "sba_business_types_string": ("sba_business_type_codes", False),
    "disaster_response_string": ("disaster_response_codes", False),
}

# entity_url domain blocklist — universal junk (mailbox + placeholder tiers). The richer
# regional-ISP curation lives in sam_fmcsa_domain_spine.CONSUMER_BLOCK (FMCSA-join specific).
DOMAIN_BLOCK = (
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "comcast.net", "sbcglobal.net", "att.net", "verizon.net", "bellsouth.net",
    "me.com", "mac.com", "cox.net", "charter.net", "earthlink.net", "gmx.com", "mail.com",
    "protonmail.com", "proton.me", "frontier.com", "windstream.net", "centurylink.net",
    "test.com", "example.com", "example.org", "example.net", "none.com", "na.com", "xxx.com",
    "company.com", "noemail.com", "nomail.com", "website.com", "domain.com", "sample.com",
)

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=0.19", "pyarrow>=17", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source(
    "core.ops_alert",
    "pipelines.sam_gov.reference.sam_v2_public_field_map",
    "pipelines.sam_gov.reference.sam_labels",
)

app = modal.App("sam-gov-master-pipelines", image=image)

# `dataset_uri` added by ALTER (CREATE IF NOT EXISTS won't add a column to an existing table);
# runs in every _record_run since sam_master has no init_ops. The existing prod success row is
# backfilled by a one-time UPDATE (see the build plan §4.7) so the first hardened run's Δ-guards
# are armed, not skipped.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.sam_master_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text NOT NULL,
    sam_label       text,
    entities_rows   bigint,
    contacts_rows   bigint,
    domains_rows    bigint,
    distinct_uei    bigint,
    status          text NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE ops.sam_master_runs ADD COLUMN IF NOT EXISTS dataset_uri text;
CREATE INDEX IF NOT EXISTS sam_master_runs_recorded_at_idx ON ops.sam_master_runs (recorded_at DESC);
"""


# --------------------------------------------------------------------------- #
# R2
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint, "region": "auto",
    }


# --------------------------------------------------------------------------- #
# SQL generation (pure; importable; runs locally in the entrypoint OR in-container)
# --------------------------------------------------------------------------- #
def _scalar_expr(pos: int, col: str, is_date: bool) -> str:
    cell = f"nullif(trim(pipe_fields[{pos}]), '')"
    if is_date:
        return f"TRY_CAST(TRY_STRPTIME({cell}, '%Y%m%d') AS DATE) AS {col}"
    return f"{cell} AS {col}"


def _list_expr(src_col: str, sibling: str, naics_strip: bool) -> str:
    inner = (f"regexp_extract(trim(x), '^[0-9]+')" if naics_strip else "trim(x)")
    return (f"list_filter(list_transform(string_split(coalesce({src_col}, ''), '~'), "
            f"x -> {inner}), e -> e <> '') AS {sibling}")


def build_sql(field_map: list[dict], date_positions: list[int]) -> dict:
    """Generate the full SQL pipeline from the frozen field map. Returns the strings the
    Modal function executes against the registered `reg` scan."""
    from pipelines.sam_gov.reference.sam_labels import snap_key_sql

    by_pos = {f["pos"]: f for f in field_map}
    dates = set(date_positions)

    entity_positions = [p for p in sorted(by_pos)
                        if p != 1 and p not in POC_POSITIONS and p not in DROP_POSITIONS]
    entity_cols = [by_pos[p]["column_name"] for p in entity_positions]

    proj_exprs = ["uei", "extract_label", "source_file"]
    proj_exprs += [_scalar_expr(p, by_pos[p]["column_name"], p in dates) for p in entity_positions]
    proj_exprs += [f"nullif(trim(pipe_fields[{p}]), '') AS poc_{p}" for p in POC_POSITIONS]
    proj_sql = ("SELECT\n  " + ",\n  ".join(proj_exprs)
                + "\nFROM reg\nWHERE nullif(trim(uei), '') IS NOT NULL")

    snap = snap_key_sql("extract_label")
    # D11 — deterministic dedup: source_file DESC final tiebreak (a uei is unique within one
    # source_file) so rebuilds over a fixed source are bit-stable.
    latest_sql = (
        "SELECT * EXCLUDE (_rn) FROM (SELECT *, row_number() OVER (PARTITION BY uei "
        f"ORDER BY last_update_date DESC NULLS LAST, initial_registration_date DESC NULLS LAST, "
        f"{snap} DESC, source_file DESC NULLS LAST) AS _rn FROM proj) WHERE _rn = 1"
    )

    # is_active is a PRESENCE check across all of a uei's rows (bool_or), not the winning row —
    # robust to near-simultaneous duplicate snapshots (2026_MAY ≡ 20260503).
    tenure_sql = (
        f"SELECT uei, arg_min(extract_label, _snap) AS first_seen_label, "
        f"arg_max(extract_label, _snap) AS last_seen_label, "
        f"count(DISTINCT extract_label) AS snapshot_count, "
        f"bool_or(sam_extract_code IS DISTINCT FROM 'A') AS ever_inactive, "
        f"bool_or(extract_label = {{LATEST}} AND sam_extract_code = 'A') AS is_active "
        f"FROM (SELECT uei, extract_label, sam_extract_code, {snap} AS _snap FROM proj) GROUP BY uei"
    )

    sibling_exprs = [_list_expr(f"l.{c}", s, strip) for c, (s, strip) in LIST_SIBLINGS.items()
                     if c in entity_cols]
    entity_select = (["l.uei"] + [f"l.{c}" for c in entity_cols] + sibling_exprs + [
        "t.is_active",
        "t.first_seen_label", "t.last_seen_label", "t.snapshot_count", "t.ever_inactive",
        "l.extract_label AS sam_extract_label", "l.source_file",
    ])
    entities_sql = ("SELECT\n  " + ",\n  ".join(entity_select)
                    + "\nFROM latest l JOIN tenure t USING (uei)")

    blocks = []
    for b, name in enumerate(POC_BLOCKS):
        base = 47 + 11 * b
        cols = ", ".join(f"l.poc_{base + i} AS {CONTACT_COLS[i]}" for i in range(11))
        nonnull = "coalesce(" + ", ".join(f"l.poc_{base + i}" for i in range(11)) + ") IS NOT NULL"
        blocks.append(f"SELECT l.uei, '{name}' AS poc_type, {cols} FROM latest l WHERE {nonnull}")
    contacts_sql = "\nUNION ALL\n".join(blocks)

    host = (r"trim(regexp_replace(regexp_replace(regexp_replace(lower(trim(entity_url)),"
            r"'^https?://',''),'^www\.',''),'[/:?#].*$',''),'.')")
    block_in = "(" + ",".join("'" + d.replace("'", "''") + "'" for d in DOMAIN_BLOCK) + ")"
    valid = (f"CASE WHEN host LIKE '%.%' AND length(host) BETWEEN 4 AND 253 AND host NOT LIKE '% %' "
             f"AND regexp_matches(host, '\\.[a-z]{{2,}}$') AND host NOT IN {block_in} THEN host END")
    domains_sql = (
        f"WITH h AS (SELECT uei, legal_business_name, cage_code, sam_extract_code, {host} AS host "
        f"FROM latest WHERE entity_url IS NOT NULL AND trim(entity_url) <> '') "
        f"SELECT DISTINCT normalized_domain, uei, legal_business_name, cage_code, sam_extract_code "
        f"FROM (SELECT {valid} AS normalized_domain, uei, legal_business_name, cage_code, "
        f"sam_extract_code FROM h) WHERE normalized_domain IS NOT NULL"
    )

    return {"proj": proj_sql, "latest": latest_sql, "tenure": tenure_sql,
            "entities": entities_sql, "contacts": contacts_sql, "domains": domains_sql}


# --------------------------------------------------------------------------- #
# pre-write gates (pure — no R2/Modal/PG; unit-tested core of the safety net)
# --------------------------------------------------------------------------- #
def _within(value: int, target: int, tol: float) -> bool:
    return abs(value - target) <= target * tol


def assert_pre_write_gates(metrics: dict, baseline: dict | None) -> list[str]:
    """Gates on in-memory metrics, BEFORE any of the 3 writes. Raises on first hard failure;
    returns the check log on success. `baseline` is the floor-qualified, prod-URI prior."""
    e = metrics["entities_rows"]
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(e >= ENTITIES_ROW_FLOOR, f"1 entities floor: {e:,} >= {ENTITIES_ROW_FLOOR:,}")
    gate(metrics["distinct_uei"] == e, f"2 uei uniqueness: {metrics['distinct_uei']:,} == {e:,}")
    gate(metrics["contacts_rows"] >= CONTACTS_FLOOR,
         f"3 contacts floor: {metrics['contacts_rows']:,} >= {CONTACTS_FLOOR:,}")
    gate(metrics["domains_rows"] >= DOMAINS_FLOOR,
         f"4 domains floor: {metrics['domains_rows']:,} >= {DOMAINS_FLOOR:,}")
    if baseline:
        gate(_within(e, baseline["entities_rows"], DELTA_GUARD),
             f"5 entities Δ: {e:,} ~ ±{DELTA_GUARD:.0%} of {baseline['entities_rows']:,}")
        gate(_within(metrics["contacts_rows"], baseline["contacts_rows"], DELTA_GUARD),
             f"6 contacts Δ: {metrics['contacts_rows']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['contacts_rows']:,}")
        gate(_within(metrics["domains_rows"], baseline["domains_rows"], DELTA_GUARD),
             f"7 domains Δ: {metrics['domains_rows']:,} ~ ±{DELTA_GUARD:.0%} of {baseline['domains_rows']:,}")
    else:
        checks.append("SKIP  5-7 Δ-guards: no floor-qualified prior success")
    gate(metrics["name_alpha_frac"] >= NAME_ALPHA_MIN,
         f"8 name-alpha: {metrics['name_alpha_frac']:.4%} >= {NAME_ALPHA_MIN:.0%}")
    # D8 — NAICS-numeric gated only when primary_naics is well-filled; else observational.
    if metrics["primary_naics_fill"] >= NAICS_FILL_MIN_FOR_GATE:
        gate(metrics["naics_numeric_frac"] >= NAICS_NUMERIC_MIN,
             f"9 naics-numeric: {metrics['naics_numeric_frac']:.4%} >= {NAICS_NUMERIC_MIN:.0%} "
             f"(fill {metrics['primary_naics_fill']:.1%})")
    else:
        checks.append(f"SKIP  9 naics-numeric: primary_naics fill {metrics['primary_naics_fill']:.1%} "
                      f"< {NAICS_FILL_MIN_FOR_GATE:.0%} (observational, not gated)")
    return checks


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* read/write.")
        return None
    return psycopg.connect(dsn)


def _prior_success_baseline(dataset_uri: str) -> dict | None:
    """Latest success at `dataset_uri` clearing BASELINE_MIN_ENTITIES — the per-family Δ
    baseline. Floor-qualified (no ratchet) and dataset_uri-scoped (no scratch poison)."""
    conn = _pg_connect()
    if conn is None:
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                "SELECT entities_rows, contacts_rows, domains_rows, distinct_uei "
                "FROM ops.sam_master_runs "
                "WHERE status='success' AND dataset_uri = %s AND entities_rows >= %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (dataset_uri, BASELINE_MIN_ENTITIES),
            )
            r = cur.fetchone()
            return None if not r else {
                "entities_rows": int(r[0]), "contacts_rows": int(r[1]),
                "domains_rows": int(r[2]), "distinct_uei": int(r[3])}
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: baseline lookup failed: {exc}")
        return None
    finally:
        conn.close()


def _record_run(*, feed, dataset_uri, sam_label, metrics, status, error,
                started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.sam_master_runs
                   (feed, dataset_uri, sam_label, entities_rows, contacts_rows, domains_rows,
                    distinct_uei, status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (feed, dataset_uri, sam_label, metrics.get("entities_rows"),
                 metrics.get("contacts_rows"), metrics.get("domains_rows"),
                 metrics.get("distinct_uei"), status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops ledger write failed: {exc}")
    finally:
        conn.close()


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Modal build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres"),
             modal.Secret.from_name("ops-alerts")],
    timeout=2 * 60 * 60, memory=131072, cpu=8.0,
)
def build_sam_master(sql: dict | None = None, dry_run: bool = False,
                     dest_prefix: str | None = None, skip_if_current: bool = True,
                     trigger_callback_url: str | None = None) -> dict:
    """Build the 3-dataset master family — fail-safe + dispatcher-ready.

    sql=None (the dispatcher path) → generate SQL in-container from the mounted field map.
    dest_prefix overrides the write location (scratch validation); None → prod. skip_if_current
    no-ops when sam_master_entities already reflects the latest entity_registrations snapshot."""
    import datetime as dt

    import duckdb
    import lance

    from pipelines.sam_gov.reference.sam_labels import snap_key_sql

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    prefix = dest_prefix or _PROD_PREFIX
    uris = _uris_for(prefix)
    feed = _feed_for(prefix)
    status, error, sam_label, metrics = "error", None, None, {}
    try:
        if sql is None:  # dispatcher / in-container path
            from pipelines.sam_gov.reference.sam_v2_public_field_map import DATE_POSITIONS, PUBLIC_FIELD_MAP
            sql = build_sql(PUBLIC_FIELD_MAP, DATE_POSITIONS)

        os.makedirs(SPILL_DIR, exist_ok=True)
        ds = lance.dataset(SRC_URI, storage_options=so)

        con = duckdb.connect(":memory:")
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads TO {DUCKDB_THREADS}")
        con.execute(f"SET temp_directory='{SPILL_DIR}'")
        con.execute("SET preserve_insertion_order=false")

        # latest v2 snapshot label (drives is_active) — cheap one-column scan.
        con.register("lbl", ds.scanner(columns=["extract_label", "format_family"],
                                       filter="format_family='v2'").to_reader())
        sam_label = con.execute(
            f"SELECT extract_label FROM lbl ORDER BY {snap_key_sql()} DESC LIMIT 1"
        ).fetchone()[0]
        con.unregister("lbl")
        print(f"latest v2 label = {sam_label}  target={prefix} feed={feed}")

        # ── skip_if_current (D12, snap-key normalized) — before the heavy scan ──
        if skip_if_current and not dry_run:
            try:
                cur_label = lance.dataset(uris["entities"], storage_options=so).scanner(
                    columns=["sam_extract_label"], limit=1).to_table().to_pylist()
                cur_label = cur_label[0]["sam_extract_label"] if cur_label else None
            except Exception:  # noqa: BLE001 — net-new/missing target → not current
                cur_label = None
            if cur_label is not None:
                same = con.execute(
                    f"SELECT {snap_key_sql('a')} = {snap_key_sql('b')} FROM (SELECT ?::text a, ?::text b)",
                    [str(sam_label), str(cur_label)]).fetchone()[0]
                if same:
                    con.close()
                    status = "skipped"
                    print(f"skip_if_current: sam_master_entities already at {cur_label} (≡ {sam_label})")
                    return {"feed": feed, "label": sam_label, "status": "skipped", "dataset": uris["entities"]}

        # one heavy scan → proj (flat projection), then latest + tenure.
        con.register("reg", ds.scanner(
            columns=["uei", "extract_label", "source_file", "pipe_fields"],
            filter="format_family='v2'").to_reader())
        con.execute(f"CREATE TEMP TABLE proj AS {sql['proj']}")
        con.unregister("reg")
        con.execute(f"CREATE TEMP TABLE latest AS {sql['latest']}")
        _lit = "'" + str(sam_label).replace("'", "''") + "'"
        con.execute(f"CREATE TEMP TABLE tenure AS {sql['tenure'].replace('{LATEST}', _lit)}")

        con.execute(f"CREATE TEMP TABLE entities AS {sql['entities']}")
        con.execute(f"CREATE TEMP TABLE contacts AS {sql['contacts']}")
        con.execute(f"CREATE TEMP TABLE domains AS {sql['domains']}")

        # ── single-pass metrics: counts + content plausibility + intersection probe (D8, D9) ──
        (e_rows, d_uei, naics_num, naics_nn, name_alpha, name_nn) = con.execute("""
            SELECT count(*), count(DISTINCT uei),
                   count(*) FILTER (WHERE primary_naics IS NOT NULL AND regexp_matches(primary_naics, '^[0-9]{2,6}$')),
                   count(*) FILTER (WHERE primary_naics IS NOT NULL),
                   count(*) FILTER (WHERE legal_business_name IS NOT NULL AND regexp_matches(legal_business_name, '[A-Za-z]')),
                   count(*) FILTER (WHERE legal_business_name IS NOT NULL)
            FROM entities
        """).fetchone()
        c_rows = con.execute("SELECT count(*) FROM contacts").fetchone()[0]
        dom_rows = con.execute("SELECT count(*) FROM domains").fetchone()[0]
        # probe present in BOTH entities and contacts (D9), deterministic via uei tiebreak.
        probe = con.execute("""
            SELECT c.uei FROM (SELECT uei, count(*) n FROM contacts WHERE uei IS NOT NULL GROUP BY uei) c
            JOIN (SELECT DISTINCT uei FROM entities WHERE uei IS NOT NULL) e USING (uei)
            ORDER BY c.n DESC, c.uei LIMIT 1
        """).fetchone()

        entities = con.sql("SELECT * FROM entities").to_arrow_table()
        contacts = con.sql("SELECT * FROM contacts").to_arrow_table()
        domains = con.sql("SELECT * FROM domains").to_arrow_table()
        con.close()

        metrics = {
            "entities_rows": int(e_rows), "contacts_rows": int(c_rows), "domains_rows": int(dom_rows),
            "distinct_uei": int(d_uei),
            "naics_numeric_frac": (naics_num / naics_nn) if naics_nn else 0.0,
            "primary_naics_fill": (naics_nn / e_rows) if e_rows else 0.0,
            "name_alpha_frac": (name_alpha / name_nn) if name_nn else 0.0,
            "probe_uei": probe[0] if probe else None,
        }
        baseline = _prior_success_baseline(uris["entities"])
        print(f"materialized {metrics} (label={sam_label}) baseline={baseline}")

        # ── pre-write gates — abort before any write ──
        for line in assert_pre_write_gates(metrics, baseline):
            print("  ", line)

        if dry_run:
            status = "dry_run"
            return {"feed": feed, "label": sam_label, "dry_run": True, **{k: v for k, v in metrics.items() if k != "probe_uei"}}

        # ── capture pre-write versions for ALL THREE (None for net-new) ──
        v_before = {}
        for name, uri in uris.items():
            try:
                v_before[name] = lance.dataset(uri, storage_options=so).version
            except Exception:  # noqa: BLE001
                v_before[name] = None
        print(f"v_before = {v_before}")

        # ── write + index + post-write gates, ALL under one 3-dataset rollback guard ──
        written: list[str] = []
        try:
            for table, name, btree in ((entities, "entities", ENTITIES_BTREE),
                                       (contacts, "contacts", CONTACTS_BTREE),
                                       (domains, "domains", DOMAINS_BTREE)):
                lance.write_dataset(table, uris[name], storage_options=so, mode="overwrite",
                                    data_storage_version=DATA_STORAGE_VERSION,
                                    max_rows_per_file=MAX_ROWS_PER_FILE)
                written.append(name)
                d = lance.dataset(uris[name], storage_options=so)
                present = set(d.schema.names)
                for col in btree:
                    if col in present:
                        d.create_scalar_index(col, index_type="BTREE")
                        print(f"  BTREE ✓ {name}.{col}")
            # post-write gates (correctness, NOT timing — D1).
            ent = lance.dataset(uris["entities"], storage_options=so)
            if ent.count_rows() != metrics["entities_rows"]:
                raise RuntimeError(f"gate: entities write-integrity {ent.count_rows():,} != {metrics['entities_rows']:,}")
            idx = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))) for i in ent.list_indices()}
            if not {f"{c}_idx" for c in ENTITIES_BTREE}.issubset(idx):
                raise RuntimeError(f"gate: entities indices missing (have {sorted(idx)})")
            pr = metrics["probe_uei"]
            if ent.scanner(columns=["uei"], filter=f"uei = '{pr}'").to_table().num_rows < 1:
                raise RuntimeError(f"gate: entities probe {pr} returned 0 rows")
            con_ds = lance.dataset(uris["contacts"], storage_options=so)
            if con_ds.scanner(columns=["uei"], filter=f"uei = '{pr}'").to_table().num_rows < 1:
                raise RuntimeError(f"gate: contacts probe {pr} returned 0 rows")
            print(f"post-write gates PASS — committed entities={ent.count_rows():,} idx={sorted(idx)} probe={pr}")
        except Exception as werr:  # noqa: BLE001
            restored, orphaned = [], []
            for name, uri in uris.items():
                if v_before[name] is not None:
                    try:
                        lance.dataset(uri, storage_options=so, version=v_before[name]).restore()
                        restored.append(f"{name}->v{v_before[name]}")
                    except Exception as rerr:  # noqa: BLE001
                        raise RuntimeError(f"ROLLBACK FAILED {name}->v{v_before[name]}: {rerr}; original: {werr}")
                elif name in written:
                    orphaned.append(name)  # net-new + written + failed → cannot roll back
            if orphaned:
                raise RuntimeError(f"NET-NEW partial-family failure: inspect/drop {orphaned} at {prefix}; "
                                   f"restored {restored}; original: {werr}")
            raise RuntimeError(f"write/index/gate failed → rolled back {restored}: {werr}")

        status = "success"
        return {"feed": feed, "label": sam_label, "datasets": list(uris.values()),
                **{k: v for k, v in metrics.items() if k != "probe_uei"}}
    except Exception as exc:  # noqa: BLE001
        error = str(exc)[:500]
        alert(f"[sam_master] {feed} build {status}: {error}")
        raise
    finally:
        _record_run(feed=feed, dataset_uri=uris["entities"], sam_label=sam_label, metrics=metrics,
                    status=status, error=error, started_at=started_at,
                    completed_at=dt.datetime.now(dt.timezone.utc))
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": feed, "label": sam_label,
                        "dataset_uri": uris["entities"],
                        "entities_rows": metrics.get("entities_rows"),
                        "distinct_uei": metrics.get("distinct_uei")})


@app.local_entrypoint()
def build(dry_run: bool = False):
    import sys
    from pathlib import Path

    # Local-only: make the frozen field map importable regardless of CWD. (The container path
    # uses the mounted module via sql=None; here we pre-build to keep the manual run cheap.)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pipelines.sam_gov.reference.sam_v2_public_field_map import DATE_POSITIONS, PUBLIC_FIELD_MAP

    sql = build_sql(PUBLIC_FIELD_MAP, DATE_POSITIONS)
    dest_prefix = os.environ.get("SAM_MASTER_DEST_PREFIX")  # None → prod
    print(build_sam_master.remote(sql=sql, dry_run=dry_run, dest_prefix=dest_prefix))


@app.local_entrypoint()
def build_dispatched(dry_run: bool = False):
    """Simulate the dispatcher path locally: sql=None → the container self-generates SQL from
    the mounted field map (validates dispatcher-readiness, the same call fn.spawn makes).
    dest_prefix via SAM_MASTER_DEST_PREFIX (None → prod)."""
    dest_prefix = os.environ.get("SAM_MASTER_DEST_PREFIX")
    print(build_sam_master.remote(sql=None, dry_run=dry_run, dest_prefix=dest_prefix,
                                  skip_if_current=False))
