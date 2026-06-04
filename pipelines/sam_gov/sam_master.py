"""Compute worker — SAM entity master family (v2-only, faithful mirror).

Builds three Lance datasets from raw ``entity_registrations`` in ONE v2 scan:
  - sam_master_entities  : 1 row / uei, latest-row-per-uei across all v2 snapshots, every
                           public field named per the FROZEN dictionary map, + parsed
                           array siblings, is_active, and cheap tenure aggregates.
  - sam_master_contacts  : the 6 POC blocks unpivoted to ≤6 rows / uei.
  - sam_master_domains   : entity_url normalized → (normalized_domain, uei) index.

Column names are the EXACT SAM dictionary field names (faithful slug); the only alias is
``uei`` for UNIQUE ENTITY ID. The projection is generated from
``pipelines/sam_gov/reference/sam_v2_public_field_map.py`` — the worker hardcodes no positions.

Topology (Directive: build the datasets; consumers/raw untouched): the SQL is generated LOCALLY
in the entrypoint from the frozen map and passed to the Modal function as strings, so the
container needs no repo source. All three Arrow tables are materialized BEFORE any write
(transform failure aborts before touching R2).

    modal run    pipelines/sam_gov/sam_master.py --dry-run   # counts + KIPPER spot-check, no write
    modal run    pipelines/sam_gov/sam_master.py             # build + publish
    modal deploy pipelines/sam_gov/sam_master.py
"""
from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
SRC_URI = "s3://data-sink/active/entity_registrations/"
ENTITIES_URI = os.environ.get("SAM_MASTER_ENTITIES_URI", "s3://data-sink/active/sam_master_entities/")
CONTACTS_URI = os.environ.get("SAM_MASTER_CONTACTS_URI", "s3://data-sink/active/sam_master_contacts/")
DOMAINS_URI = os.environ.get("SAM_MASTER_DOMAINS_URI", "s3://data-sink/active/sam_master_domains/")
FEED = "sam_master"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
ENTITIES_ROW_FLOOR = 1_400_000

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
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("sam-gov-master-pipelines", image=image)

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
# SQL generation (pure; runs locally in the entrypoint from the frozen map)
# --------------------------------------------------------------------------- #
def _scalar_expr(pos: int, col: str, is_date: bool) -> str:
    cell = f"nullif(trim(pipe_fields[{pos}]), '')"
    if is_date:
        return f"TRY_CAST(TRY_STRPTIME({cell}, '%Y%m%d') AS DATE) AS {col}"
    return f"{cell} AS {col}"


def _snap_key_sql(col: str = "extract_label") -> str:
    months = ("WHEN 'JAN' THEN '01' WHEN 'FEB' THEN '02' WHEN 'MAR' THEN '03' WHEN 'APR' THEN '04' "
              "WHEN 'MAY' THEN '05' WHEN 'JUN' THEN '06' WHEN 'JUL' THEN '07' WHEN 'AUG' THEN '08' "
              "WHEN 'SEP' THEN '09' WHEN 'OCT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEC' THEN '12'")
    return (f"CASE WHEN regexp_matches({col}, '^[0-9]{{8}}$') THEN CAST({col} AS BIGINT) "
            f"ELSE CAST(substr({col},1,4) || CASE upper(substr({col},6,3)) {months} ELSE '00' END "
            f"|| '00' AS BIGINT) END")


def _list_expr(src_col: str, sibling: str, naics_strip: bool) -> str:
    inner = (f"regexp_extract(trim(x), '^[0-9]+')" if naics_strip else "trim(x)")
    return (f"list_filter(list_transform(string_split(coalesce({src_col}, ''), '~'), "
            f"x -> {inner}), e -> e <> '') AS {sibling}")


def build_sql(field_map: list[dict], date_positions: list[int]) -> dict:
    """Generate the full SQL pipeline from the frozen field map. Returns the strings the
    Modal function executes against the registered `reg` scan."""
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

    snap = _snap_key_sql("extract_label")
    latest_sql = (
        "SELECT * EXCLUDE (_rn) FROM (SELECT *, row_number() OVER (PARTITION BY uei "
        f"ORDER BY last_update_date DESC NULLS LAST, initial_registration_date DESC NULLS LAST, "
        f"{snap} DESC) AS _rn FROM proj) WHERE _rn = 1"
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
# ops ledger
# --------------------------------------------------------------------------- #
def _record_run(*, sam_label, metrics, status, error, started_at, completed_at) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    import psycopg
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.sam_master_runs
                   (feed, sam_label, entities_rows, contacts_rows, domains_rows, distinct_uei,
                    status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, sam_label, metrics.get("entities_rows"), metrics.get("contacts_rows"),
                 metrics.get("domains_rows"), metrics.get("distinct_uei"), status, error,
                 started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops ledger write failed: {exc}")


# --------------------------------------------------------------------------- #
# Modal build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=2 * 60 * 60, memory=131072, cpu=8.0,
)
def build_sam_master(sql: dict, dry_run: bool = False) -> dict:
    import datetime as dt

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    status, error, sam_label, metrics = "error", None, None, {}
    so = _r2_storage_options()
    try:
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
            f"SELECT extract_label FROM lbl ORDER BY {_snap_key_sql()} DESC LIMIT 1"
        ).fetchone()[0]
        con.unregister("lbl")
        print(f"latest v2 label = {sam_label}")

        # one heavy scan → proj (flat projection), then latest + tenure.
        con.register("reg", ds.scanner(
            columns=["uei", "extract_label", "source_file", "pipe_fields"],
            filter="format_family='v2'").to_reader())
        con.execute(f"CREATE TEMP TABLE proj AS {sql['proj']}")
        con.unregister("reg")
        con.execute(f"CREATE TEMP TABLE latest AS {sql['latest']}")
        _lit = "'" + str(sam_label).replace("'", "''") + "'"
        con.execute(f"CREATE TEMP TABLE tenure AS {sql['tenure'].replace('{LATEST}', _lit)}")

        entities = con.sql(sql["entities"]).to_arrow_table()
        contacts = con.sql(sql["contacts"]).to_arrow_table()
        domains = con.sql(sql["domains"]).to_arrow_table()
        con.close()

        metrics = {
            "entities_rows": entities.num_rows, "contacts_rows": contacts.num_rows,
            "domains_rows": domains.num_rows,
            "distinct_uei": len(set(entities.column("uei").to_pylist())),
        }
        print(f"materialized {metrics} (label={sam_label})")

        # validation gates
        if metrics["entities_rows"] < ENTITIES_ROW_FLOOR:
            raise RuntimeError(f"entities row floor breached: {metrics['entities_rows']} < {ENTITIES_ROW_FLOOR}")
        if metrics["entities_rows"] != metrics["distinct_uei"]:
            raise RuntimeError(f"uei not unique: {metrics['entities_rows']} rows vs {metrics['distinct_uei']} distinct")
        if metrics["contacts_rows"] == 0 or metrics["domains_rows"] == 0:
            raise RuntimeError(f"empty satellite: {metrics}")

        if dry_run:
            status = "dry_run"
            _kipper = [r for r in entities.to_pylist() if r["uei"] == "DD1BCRF2QQG8"]
            if _kipper:
                k = _kipper[0]
                print("KIPPER:", {c: k.get(c) for c in
                                  ("legal_business_name", "cage_code", "entity_url", "primary_naics",
                                   "physical_address_line_1", "is_active", "snapshot_count")})
            return {"feed": FEED, "label": sam_label, "dry_run": True, **metrics}

        # atomic-ish: all three Arrow tables exist before any write.
        for table, uri, btree in ((entities, ENTITIES_URI, ENTITIES_BTREE),
                                  (contacts, CONTACTS_URI, CONTACTS_BTREE),
                                  (domains, DOMAINS_URI, DOMAINS_BTREE)):
            lance.write_dataset(table, uri, storage_options=so, mode="overwrite",
                                data_storage_version=DATA_STORAGE_VERSION,
                                max_rows_per_file=MAX_ROWS_PER_FILE)
            d = lance.dataset(uri, storage_options=so)
            present = set(d.schema.names)
            for col in btree:
                if col in present:
                    d.create_scalar_index(col, index_type="BTREE")
                    print(f"  BTREE ✓ {uri.rsplit('/', 2)[-2]}.{col}")
        status = "success"
        return {"feed": FEED, "label": sam_label,
                "datasets": [ENTITIES_URI, CONTACTS_URI, DOMAINS_URI], **metrics}
    except Exception as exc:  # noqa: BLE001
        error = str(exc)[:500]
        raise
    finally:
        _record_run(sam_label=sam_label, metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=dt.datetime.now(dt.timezone.utc))


@app.local_entrypoint()
def build(dry_run: bool = False):
    import sys
    from pathlib import Path

    # Local-only: make the frozen field map importable regardless of CWD. (Runs on the
    # operator's machine, not in the Modal container — the container is passed finished SQL.)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pipelines.sam_gov.reference.sam_v2_public_field_map import DATE_POSITIONS, PUBLIC_FIELD_MAP

    sql = build_sql(PUBLIC_FIELD_MAP, DATE_POSITIONS)
    print(build_sam_master.remote(sql=sql, dry_run=dry_run))
