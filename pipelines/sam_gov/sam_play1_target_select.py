"""Play-1 (PE A&D / gov-services target profiling) — entity-anchored target selector.

Builds the harvest worklist for the attachment-manifest harvester by intersecting
USASpending FPDS (clean ``recipient_uei`` spine) with the SAM opportunity universe
(active + archived). Output is a TARGET UNIVERSE Lance dataset (same shape the
manifest harvester reads) so the existing ``sam_attachment_manifest.py`` runs over it
UNCHANGED via ``SAM_OPPS_LANCE_URI``. Read-only on all inputs; writes one scratch
dataset.

THE CHAIN (§9a):
  QUALIFY   entities "currently winning govt $": award_search rows in the §9b A&D NAICS
            set with a positive obligation and action_date >= now-Q years  ->  recipient_uei set
  FOOTPRINT those UEIs' A&D contract activity over now-F years (transaction_search_fpds)
            ->  distinct PIID + solicitation_identifier
  JOIN      SAM universe (active + archived) on award_number = PIID OR
            solicitation_number = solicitation_identifier  ->  distinct notice_id
  WRITE     the matched SAM rows (harvester's 7 columns) -> target universe Lance

Why UEI not awardee name: SAM ``awardee`` is a name string (0/12,107 are UEI-shaped);
FPDS ``recipient_uei`` is the clean key. PIID (award_number) is the join spine; sol#
is the secondary join (catches the solicitation companion notice).

MEMORY: out-of-core. NAICS predicate is pushed into the Lance scan (cuts the 107M-row
FPDS transaction table to the A&D slice before materialize); DuckDB spills to
``--temp-dir``. Set ``--memory-limit`` to ~half of box RAM.

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with duckdb python \
      pipelines/sam_gov/sam_play1_target_select.py --qualify-years 2 --footprint-years 6
"""
from __future__ import annotations

import argparse
import datetime as dt
import os

# §9b mandate set — A&D manufacturing + engineering + IT/cyber + gov services + env/infra.
AND_NAICS = [
    "336411", "336412", "336413", "336414", "336415", "336419",
    "332992", "332993", "332994", "336992", "336611", "334511",
    "541330", "541310", "541370", "541380", "541715", "541713", "541714",
    "541511", "541512", "541513", "541519", "518210", "517311", "541690",
    "541611", "541618", "541614", "541990", "561210", "561110", "561612", "561621",
    "488190", "488390", "811210", "562910", "541620",
    "237110", "237130", "237310", "237990",
]

# Environmental / hazard remediation services — the "EPA violation hits -> who gets the
# call" set: assess (consulting/testing) -> remediate -> haul & dispose.
REMEDIATION_NAICS = [
    "562910",  # Remediation Services (bullseye)
    "541620",  # Environmental Consulting Services
    "541380",  # Testing Laboratories
    "562112",  # Hazardous Waste Collection
    "562211",  # Hazardous Waste Treatment and Disposal
    "562219",  # Other Nonhazardous Waste Treatment and Disposal
    "562111",  # Solid Waste Collection
    "562212",  # Solid Waste Landfill
    "562998",  # All Other Miscellaneous Waste Management Services
    "562920",  # Materials Recovery Facilities
    "238910",  # Site Preparation Contractors (excavation/abatement/demolition for cleanup)
]

# Equipment rental & leasing (construction-serving). NOTE: largely a PRIVATE-sector
# business — SAM/FPDS only captures the federal-contracting slice. See module sizing.
EQUIP_RENTAL_NAICS = [
    "532412",  # Construction, Mining & Forestry Machinery & Equipment Rental (bullseye)
    "532490",  # Other Commercial & Industrial Machinery & Equipment Rental
    "532310",  # General Rental Centers
    "532120",  # Truck, Utility Trailer & RV Rental & Leasing
]

NAICS_SETS = {"ad": AND_NAICS, "remediation": REMEDIATION_NAICS,
              "equipment_rental": EQUIP_RENTAL_NAICS}

AWARD_SEARCH = "s3://data-sink/active/usaspending/award_search/"
FPDS_TXNS = "s3://data-sink/active/usaspending/transaction_search_fpds/"
# Hardcoded (NOT env): P3 sets SAM_OPPS_LANCE_URI=target for the harvester, so the
# selector must not read its source universe through that same env var.
SAM_ACTIVE = "s3://data-sink/sam-gov-opps/active/"
SAM_ARCHIVED = "s3://data-sink/sam-gov-opps/archived/"
TARGET_URI = os.environ.get("SAM_PLAY1_TARGET_URI", "s3://data-sink/active/_play1_target_universe/")


def _so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint, "region": "auto",
    }


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _s3_client():
    """boto3 R2 client — checksum 'when_required' (botocore default breaks R2)."""
    import boto3
    from botocore.config import Config

    so = _so()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["endpoint"], region_name="auto",
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"], config=cfg)


def _publish_to_r2(dataset_uri: str, local_dir: str) -> int:
    """Wipe R2 prefix, upload local Lance via boto3 (uniform parts; R2-safe — Lance's
    object-store writer escalates multipart parts mid-upload, which R2 rejects)."""
    bucket, prefix = dataset_uri[len("s3://"):].split("/", 1)
    if not prefix.endswith("/"):
        prefix += "/"
    s3 = _s3_client()
    to_del: list[dict] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_del, "Quiet": True})
    up = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, bucket, prefix + rel)
            up += 1
    return up


def main() -> None:
    import duckdb
    import lance
    import pyarrow as pa

    p = argparse.ArgumentParser()
    p.add_argument("--qualify-years", type=int, default=2)
    p.add_argument("--footprint-years", type=int, default=6)
    p.add_argument("--naics-set", choices=sorted(NAICS_SETS), default="ad",
                   help="which NAICS bundle to target (ad | remediation | equipment_rental)")
    p.add_argument("--target-uri", default=None,
                   help="default: _play1_target_universe_<set>/ ('ad' keeps the original path)")
    p.add_argument("--memory-limit", default="10GB")
    p.add_argument("--temp-dir", default="/tmp/duckdb_spill")
    p.add_argument("--dry-run", action="store_true", help="size only; do not write target universe")
    a = p.parse_args()

    so = _so()
    today = dt.date.today()
    q_cut = (today - dt.timedelta(days=365 * a.qualify_years)).isoformat()
    f_cut = (today - dt.timedelta(days=365 * a.footprint_years)).isoformat()
    codes = NAICS_SETS[a.naics_set]
    a.target_uri = a.target_uri or (TARGET_URI if a.naics_set == "ad"
                                    else f"s3://data-sink/active/_play1_target_universe_{a.naics_set}/")
    naics_filter = "naics_code IN (" + ",".join(f"'{c}'" for c in codes) + ")"  # pushdown predicate
    print(f"[{_now()}] naics_set={a.naics_set} ({len(codes)} codes) target={a.target_uri}", flush=True)

    os.makedirs(a.temp_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute(f"SET memory_limit='{a.memory_limit}'")
    con.execute(f"SET temp_directory='{a.temp_dir}'")
    print(f"[{_now()}] qualify>= {q_cut} ({a.qualify_years}y)  footprint>= {f_cut} ({a.footprint_years}y)", flush=True)

    def reader(uri, columns, flt):
        return lance.dataset(uri, storage_options=so).scanner(
            columns=columns, filter=flt, batch_size=131072).to_reader()

    # ---- 1) QUALIFY: A&D award_search rows -> recipient_uei "winning govt $" -------
    print(f"[{_now()}] scan award_search (A&D NAICS pushdown) ...", flush=True)
    con.register("aw_r", reader(AWARD_SEARCH,
                 ["recipient_uei", "naics_code", "action_date", "total_obligation",
                  "generated_pragmatic_obligation", "type"], naics_filter))
    con.execute("CREATE TABLE aw AS SELECT * FROM aw_r")
    con.execute(f"""
        CREATE TABLE q_uei AS
        SELECT DISTINCT recipient_uei FROM aw
        WHERE recipient_uei IS NOT NULL AND length(trim(recipient_uei)) = 12
          AND TRY_CAST(action_date AS DATE) >= DATE '{q_cut}'
          AND coalesce(TRY_CAST(total_obligation AS DOUBLE),
                       TRY_CAST(generated_pragmatic_obligation AS DOUBLE), 0) > 0
    """)
    n_aw = con.execute("SELECT count(*) FROM aw").fetchone()[0]
    n_q = con.execute("SELECT count(*) FROM q_uei").fetchone()[0]
    print(f"  award_search A&D rows={n_aw:,}  qualified UEIs={n_q:,}", flush=True)

    # ---- 2) FOOTPRINT: those UEIs' A&D txns over F years -> PIID + sol id ----------
    print(f"[{_now()}] scan transaction_search_fpds (A&D NAICS pushdown) ...", flush=True)
    con.register("fp_r", reader(FPDS_TXNS,
                 ["recipient_uei", "piid", "solicitation_identifier", "action_date", "naics_code"],
                 naics_filter))
    con.execute(f"""
        CREATE TABLE fp AS
        SELECT recipient_uei, piid, solicitation_identifier
        FROM fp_r
        WHERE TRY_CAST(action_date AS DATE) >= DATE '{f_cut}'
          AND recipient_uei IN (SELECT recipient_uei FROM q_uei)
    """)
    con.execute("CREATE TABLE f_piid AS SELECT DISTINCT upper(trim(piid)) AS k FROM fp WHERE piid IS NOT NULL")
    con.execute("CREATE TABLE f_sol AS SELECT DISTINCT upper(trim(solicitation_identifier)) AS k FROM fp WHERE solicitation_identifier IS NOT NULL")
    n_fp = con.execute("SELECT count(*) FROM fp").fetchone()[0]
    n_piid = con.execute("SELECT count(*) FROM f_piid").fetchone()[0]
    n_sol = con.execute("SELECT count(*) FROM f_sol").fetchone()[0]
    print(f"  footprint txns={n_fp:,}  distinct PIID={n_piid:,}  distinct sol_id={n_sol:,}", flush=True)

    # ---- 3) JOIN to SAM universe (active + archived) -> distinct notice_id ---------
    cols = ["notice_id", "solicitation_number", "naics_code", "classification_code",
            "title", "posted_date", "link", "award_number"]
    print(f"[{_now()}] scan SAM active + archived ...", flush=True)
    con.register("sam_active_r", reader(SAM_ACTIVE, cols, None))
    con.execute("CREATE TABLE sam AS SELECT *, 'active' AS sam_source FROM sam_active_r")
    try:
        con.register("sam_arch_r", reader(SAM_ARCHIVED, cols, None))
        con.execute("INSERT INTO sam SELECT *, 'archived' AS sam_source FROM sam_arch_r")
    except Exception as exc:  # noqa: BLE001 — archived may not be built yet
        print(f"  WARN: archived universe unavailable ({exc}); active-only join", flush=True)

    con.execute("""
        CREATE TABLE matched AS
        SELECT *,
               (upper(trim(award_number)) IN (SELECT k FROM f_piid)) AS via_piid,
               (upper(trim(solicitation_number)) IN (SELECT k FROM f_sol)) AS via_sol
        FROM sam
        WHERE upper(trim(award_number)) IN (SELECT k FROM f_piid)
           OR upper(trim(solicitation_number)) IN (SELECT k FROM f_sol)
    """)
    n_match = con.execute("SELECT count(*) FROM matched").fetchone()[0]
    n_notice = con.execute("SELECT count(DISTINCT notice_id) FROM matched").fetchone()[0]
    print("\n=== SIZING ===", flush=True)
    for r in con.execute("""
        SELECT sam_source, count(DISTINCT notice_id) AS notices,
               count(DISTINCT notice_id) FILTER (WHERE via_piid) AS via_piid,
               count(DISTINCT notice_id) FILTER (WHERE via_sol)  AS via_sol
        FROM matched GROUP BY 1 ORDER BY 1""").fetchall():
        print(f"  source={r[0]:9s} notices={r[1]:>8,}  via_piid={r[2]:>8,}  via_sol={r[3]:>8,}", flush=True)
    print(f"  TOTAL distinct notice_id (harvest worklist) = {n_notice:,}", flush=True)
    hrs = n_notice / 4 / 3600
    print(f"  harvest estimate @4 req/s ≈ {hrs:.1f} hours", flush=True)

    # ---- 4) WRITE target universe (harvester's 7 cols, dedup notice_id) ------------
    if a.dry_run:
        print(f"[{_now()}] --dry-run: not writing target universe.", flush=True)
        return
    import shutil
    local_t = "/tmp/play1_target_lance"
    shutil.rmtree(local_t, ignore_errors=True)
    print(f"[{_now()}] building target universe LOCAL -> publish {a.target_uri}", flush=True)
    at = con.execute("""
        SELECT notice_id, any_value(solicitation_number) AS solicitation_number,
               any_value(naics_code) AS naics_code, any_value(classification_code) AS classification_code,
               any_value(title) AS title, any_value(posted_date) AS posted_date,
               any_value(link) AS link
        FROM matched WHERE notice_id IS NOT NULL GROUP BY notice_id
    """).fetch_arrow_table()
    lance.write_dataset(at, local_t, mode="overwrite", data_storage_version="2.0",
                        max_rows_per_file=250_000)
    n_files = _publish_to_r2(a.target_uri, local_t)
    chk = lance.dataset(a.target_uri, storage_options=so).count_rows()
    print(f"[{_now()}] wrote {at.num_rows:,} target notices, published {n_files} files; R2 verify={chk:,}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
