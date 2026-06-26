"""90-day Prime-Award → SAM.gov attachment manifest — offline bridge + live harvest.

Builds the substrate pointer layer for the recent (90-day, last_modified_date)
API-fresh prime-award feed, using the OFFLINE-JOIN bridge validated in
``docs/reference/PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md``. No live SAM *search*
API is touched — the solnum→notice_id translation is a pure DuckDB join against the
SAM opportunities bulk we already hold. Only the per-notice attachment list is fetched
live. Stage 3 (PDF byte download) is intentionally OUT OF SCOPE here.

Pipeline (4 stages, run in order):

  1. BRIDGE (offline)   distinct ``solicitation_identifier`` from
     ``usaspending_api_fresh/contract_prime_txn`` INNER JOIN ``sam-gov-opps``
     (active ∪ archived) on alphanumeric-normalized solnum
     (``upper``; strip ``[^A-Z0-9]``) — accounts for FPDS↔SAM formatting drift.

  2. MULTIPLICITY       one WINNER notice_id per normalized solnum, ranked by
     ``base_type`` (the document-host identity, NOT ``notice_type`` which flips to
     "Award Notice" once a solicitation is awarded):
       Combined Synopsis/Solicitation > Solicitation > Presolicitation > Special
       Notice > Modification > Justification > Award Notice > Sources Sought > other.
     Award Notice / Sources Sought are chosen ONLY when no higher tier exists.

  3. HARVEST (live)     GET the frontend resources endpoint per winner — no api_key,
     no developer quota (api.sam.gov SI-NONFED caps ~10/day; useless for a sweep).
     Single-threaded, residential IP (datacenter egress is 429'd), polite 0.12 s
     pace. Crash-safe + RESUMABLE via a JSONL checkpoint (skip completed on restart):
       GET https://sam.gov/api/prod/opps/v3/opportunities/{notice_id}/resources
         -> _embedded.opportunityAttachmentList[].attachments[]
            {resourceId, name, mimeType, size, accessLevel, attachmentOrder, ...}

  4. SINK               attachment-grain (one row per file) → Lance v2.1 at
     ``s3://data-sink/active/sam_opps_attachment_manifest_winners/``, retaining
     the FPDS linkage (``solicitation_identifier`` + ``contract_award_unique_key`` +
     ``award_keys[]``). BTREE on the resolution keys.

PERF NOTE (verified): do NOT materialize the 2.88M-row archived opps set into a
DuckDB table and aggregate — it stalls. Push the predicate (or the solnum IN-list)
into the Lance scanner / union scan. ``size_bytes`` is a LOWER BOUND (corrupt for
files ≥10 MB; see ``sam_attachment_manifest.py``) — enforce real size at fetch.

    # 1-2 offline bridge → winners.parquet
    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with 'duckdb>=1.5,<2' \
      python pipelines/sam_gov/sam_opps_attachment_manifest_winners.py bridge
    # 3 live harvest (resumable + self-detaching for the multi-hour sweep)
    OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES ... python ...winners.py harvest --daemon
    # 4 sink + index + verify
    ... python ...winners.py sink
    ... python ...winners.py verify
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time

# ─────────────────────────── constants ───────────────────────────

FPDS_URI = os.environ.get(
    "USASPENDING_API_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn",
).rstrip("/") + "/"
SAM_ACTIVE_URI = os.environ.get("SAM_OPPS_ACTIVE_URI", "s3://data-sink/sam-gov-opps/active/")
SAM_ARCHIVED_URI = os.environ.get("SAM_OPPS_ARCHIVED_URI", "s3://data-sink/sam-gov-opps/archived/")
MANIFEST_URI = os.environ.get(
    "SAM_ATTACH_MANIFEST_90DAY_URI",
    "s3://data-sink/active/sam_opps_attachment_manifest_winners/",
)

WORKDIR = os.environ.get("WINNERS_WORKDIR", "/tmp/sam_winners")
WINNERS_PARQUET = os.path.join(WORKDIR, "winners.parquet")
HARVEST_CKPT = os.path.join(WORKDIR, "harvest_ckpt.jsonl")

RESOURCES_URL = "https://sam.gov/api/prod/opps/v3/opportunities/{nid}/resources"
DOWNLOAD_URL = "https://sam.gov/api/prod/opps/v3/opportunities/resources/files/{rid}/download"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")
INTER_CALL_SLEEP = 0.12  # proven-safe residential pace

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
INDEX_COLS = ["notice_id", "sol_norm", "contract_award_unique_key",
              "solicitation_identifier", "resource_id"]

# normalized-solnum SQL template; X is replaced with the column expression
NORM = "regexp_replace(upper(trim({c})), '[^A-Z0-9]', '', 'g')"
# base_type → rank (lower = higher-value document host)
RANK_SQL = """CASE upper(coalesce({bt}, {nt}, ''))
  WHEN 'COMBINED SYNOPSIS/SOLICITATION' THEN 1
  WHEN 'SOLICITATION' THEN 2
  WHEN 'PRESOLICITATION' THEN 3
  WHEN 'SPECIAL NOTICE' THEN 4
  WHEN 'MODIFICATION/AMENDMENT/CANCEL' THEN 5
  WHEN 'JUSTIFICATION' THEN 6
  WHEN 'JUSTIFICATION AND APPROVAL (J&A)' THEN 6
  WHEN 'AWARD NOTICE' THEN 7
  WHEN 'SOURCES SOUGHT' THEN 8
  ELSE 9 END"""


# ─────────────────────────── daemonize (resume-safe multi-hour harvest) ───────────────────────────

def _daemonize() -> None:
    """Double-fork + os.setsid so the multi-hour harvest outlives the launching shell /
    a session resume. macOS-safe: there is NO `setsid` binary (use os.setsid); the fork
    happens HERE while the process is still single-threaded — duckdb/requests are imported
    lazily inside harvest(), AFTER this fork, which avoids the macOS objc fork-safety abort
    (`+[NSNumber initialize] ... Crashing instead`). Pair with --resume (the JSONL
    checkpoint) so a kill at any point continues cleanly. Launch with
    OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES as a backstop."""
    os.makedirs(WORKDIR, exist_ok=True)
    logpath = os.path.join(WORKDIR, "harvest_daemon.log")
    if os.fork() > 0:
        os._exit(0)
    os.setsid()
    if os.fork() > 0:
        os._exit(0)
    f = open(logpath, "a", buffering=1)
    os.dup2(f.fileno(), 1)
    os.dup2(f.fileno(), 2)
    try:
        os.dup2(open(os.devnull).fileno(), 0)
    except OSError:
        pass


# ─────────────────────────── R2 ───────────────────────────

def r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _headers(nid: str) -> dict:
    return {"User-Agent": UA, "Accept": "application/json, text/plain, */*",
            "Origin": "https://sam.gov", "Referer": f"https://sam.gov/opp/{nid}/view"}


# ─────────────────────────── 1-2. bridge + multiplicity ───────────────────────────

def build_bridge() -> dict:
    """Offline join + winner-per-solnum. Writes winners.parquet. No live calls."""
    import duckdb
    import lance

    os.makedirs(WORKDIR, exist_ok=True)
    so = r2_storage_options()
    con = duckdb.connect(os.path.join(WORKDIR, "bridge.duckdb"))
    con.execute("PRAGMA threads=4;")
    os.makedirs(os.path.join(WORKDIR, "spill"), exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{WORKDIR}/spill';")

    fp = lance.dataset(FPDS_URI, storage_options=so)
    con.register("fp_src", fp.scanner(columns=[
        "solicitation_identifier", "contract_award_unique_key", "action_date"]).to_reader())
    con.execute(f"""
    CREATE TABLE fpds AS
    WITH base AS (
      SELECT {NORM.format(c='solicitation_identifier')} AS sol_norm,
             nullif(trim(solicitation_identifier),'')   AS sol_orig,
             nullif(trim(contract_award_unique_key),'')  AS award_key,
             nullif(trim(action_date),'')                AS action_date
      FROM fp_src WHERE {NORM.format(c='solicitation_identifier')} <> '')
    SELECT sol_norm,
           arg_max(sol_orig, action_date)  AS solicitation_identifier,
           arg_max(award_key, action_date) AS contract_award_unique_key,
           list(DISTINCT award_key) FILTER (WHERE award_key IS NOT NULL) AS award_keys,
           count(DISTINCT award_key)       AS award_count
    FROM base GROUP BY sol_norm;""")
    con.unregister("fp_src")

    cols = ["notice_id", "solicitation_number", "notice_type", "base_type",
            "posted_date", "title", "classification_code", "naics_code"]
    for tag, uri in (("active", SAM_ACTIVE_URI), ("archived", SAM_ARCHIVED_URI)):
        ds = lance.dataset(uri, storage_options=so)
        have = set(ds.schema.names)
        use = [c for c in cols if c in have]
        con.register(f"s_{tag}", ds.scanner(columns=use).to_reader())
        sel = ", ".join(f'"{c}"' if c in have else f"NULL AS {c}" for c in cols)
        con.execute(f"CREATE TABLE sam_{tag} AS SELECT {sel} FROM s_{tag};")
        con.unregister(f"s_{tag}")

    con.execute(f"""
    CREATE TABLE sam AS
    WITH u AS (SELECT * FROM sam_active UNION ALL SELECT * FROM sam_archived)
    SELECT notice_id, nullif(trim(solicitation_number),'') AS solicitation_number,
           {NORM.format(c='solicitation_number')} AS sol_norm,
           notice_type, base_type, posted_date, title, classification_code, naics_code
    FROM u WHERE notice_id IS NOT NULL AND {NORM.format(c='solicitation_number')} <> '';""")

    con.execute(f"""
    CREATE TABLE joined AS
    SELECT f.sol_norm, f.solicitation_identifier, f.contract_award_unique_key,
           f.award_keys, f.award_count,
           s.notice_id, s.solicitation_number, s.notice_type, s.base_type,
           s.posted_date, s.title, s.classification_code, s.naics_code,
           {RANK_SQL.format(bt='s.base_type', nt='s.notice_type')} AS type_rank
    FROM fpds f JOIN sam s USING (sol_norm);""")

    con.execute("""
    CREATE TABLE winners AS
    SELECT * EXCLUDE (rn) FROM (
      SELECT *, row_number() OVER (PARTITION BY sol_norm
        ORDER BY type_rank ASC, posted_date DESC NULLS LAST, notice_id ASC) AS rn
      FROM joined) WHERE rn = 1;""")
    con.execute("ALTER TABLE winners ADD COLUMN is_primary_target BOOLEAN;")
    con.execute("UPDATE winners SET is_primary_target = (type_rank <= 3);")
    con.execute(f"COPY winners TO '{WINNERS_PARQUET}' (FORMAT parquet);")

    stats = con.execute("""SELECT count(*), count(*) FILTER (WHERE is_primary_target),
        (SELECT count(*) FROM fpds) FROM winners""").fetchone()
    con.close()
    out = {"fpds_distinct_solnorm": stats[2], "winners": stats[0],
           "primary_target": stats[1],
           "resolution_rate_pct": round(100.0 * stats[0] / stats[2], 2)}
    print(json.dumps(out, indent=2), flush=True)
    return out


# ─────────────────────────── 3. live harvest (resumable) ───────────────────────────

def harvest(resume: bool = True) -> dict:
    import duckdb
    import requests

    con = duckdb.connect()
    nids = [r[0] for r in con.execute(
        f"SELECT notice_id FROM '{WINNERS_PARQUET}' "
        f"ORDER BY is_primary_target DESC, type_rank ASC, notice_id").fetchall()]
    con.close()

    done: set[str] = set()
    if resume and os.path.exists(HARVEST_CKPT):
        with open(HARVEST_CKPT) as fh:
            for line in fh:
                try:
                    done.add(json.loads(line)["notice_id"])
                except Exception:  # noqa: BLE001
                    pass
    todo = [n for n in nids if n not in done]
    print(f"winners={len(nids)} done={len(done)} todo={len(todo)}", flush=True)

    s = requests.Session()
    ck = open(HARVEST_CKPT, "a", buffering=1)

    def fetch(nid, retries=6):
        last = None
        for a in range(retries):
            try:
                r = s.get(RESOURCES_URL.format(nid=nid), headers=_headers(nid), timeout=(15, 45))
                if r.status_code == 200:
                    return 200, r.json(), None
                if r.status_code == 404:
                    return 404, None, "404"
                if r.status_code in (403, 429, 503) or r.status_code >= 500:
                    last = f"http{r.status_code}"; time.sleep(min(120, 4 * 2 ** a)); continue
                return r.status_code, None, f"http{r.status_code}"
            except Exception as e:  # noqa: BLE001
                last = str(e)[:60]; time.sleep(min(60, 2 ** a))
        return 0, None, f"exhausted:{last}"

    n_ok = n_empty = n_err = n_att = 0
    t0 = time.time()
    for i, nid in enumerate(todo):
        code, j, err = fetch(nid)
        atts = []
        if code == 200:
            for blk in (j or {}).get("_embedded", {}).get("opportunityAttachmentList", []) or []:
                for a in (blk.get("attachments") or []):
                    rid = a.get("resourceId")
                    atts.append({"resource_id": rid, "file_name": a.get("name"),
                                 "mime_type": a.get("mimeType"), "size_bytes": a.get("size"),
                                 "access_level": a.get("accessLevel"),
                                 "attachment_order": a.get("attachmentOrder"),
                                 "download_url": DOWNLOAD_URL.format(rid=rid) if rid else None})
            rec = {"notice_id": nid, "status": "ok" if atts else "empty",
                   "http": 200, "n_attach": len(atts), "attachments": atts}
            n_att += len(atts); n_ok += bool(atts); n_empty += (not atts)
        else:
            rec = {"notice_id": nid, "status": "err", "http": code, "n_attach": 0,
                   "attachments": [], "err": err}
            n_err += 1
        ck.write(json.dumps(rec) + "\n")
        if (i + 1) % 1000 == 0:
            rate = (i + 1) / (time.time() - t0)
            print(f"  {i+1}/{len(todo)} ok={n_ok} empty={n_empty} err={n_err} att={n_att} "
                  f"{rate:.1f}/s ETA {((len(todo)-(i+1))/rate/60):.0f}m", flush=True)
        time.sleep(INTER_CALL_SLEEP)
    ck.close()
    out = {"processed": len(todo), "notices_with_attach": n_ok, "notices_empty": n_empty,
           "notices_err": n_err, "total_attachments": n_att}
    print(json.dumps(out, indent=2), flush=True)
    return out


# ─────────────────────────── 4. sink → Lance ───────────────────────────

def sink() -> dict:
    import duckdb
    import lance
    import pyarrow as pa

    con = duckdb.connect()
    wrows = con.execute(f"""SELECT notice_id, sol_norm, solicitation_number, notice_type,
        base_type, posted_date, title, classification_code, naics_code,
        solicitation_identifier, contract_award_unique_key, award_keys, award_count,
        is_primary_target FROM '{WINNERS_PARQUET}'""").arrow().to_pylist()
    con.close()
    W = {r["notice_id"]: r for r in wrows}

    def to_int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    harvested_at = dt.datetime.now(dt.timezone.utc)
    rows = []
    with open(HARVEST_CKPT) as fh:
        for line in fh:
            try:
                rec = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            w = W.get(rec["notice_id"])
            if w is None:
                continue
            for a in (rec.get("attachments") or []):
                rows.append({
                    "resource_id": a.get("resource_id"), "file_name": a.get("file_name"),
                    "mime_type": a.get("mime_type"), "size_bytes": to_int(a.get("size_bytes")),
                    "access_level": a.get("access_level"),
                    "attachment_order": to_int(a.get("attachment_order")),
                    "download_url": a.get("download_url"), "notice_id": rec["notice_id"],
                    "solicitation_number": w["solicitation_number"], "sol_norm": w["sol_norm"],
                    "notice_type": w["notice_type"], "base_type": w["base_type"],
                    "notice_posted_date": (str(w["posted_date"]) if w["posted_date"] is not None else None),
                    "notice_title": w["title"], "classification_code": w["classification_code"],
                    "naics_code": w["naics_code"], "is_primary_target": bool(w["is_primary_target"]),
                    "solicitation_identifier": w["solicitation_identifier"],
                    "contract_award_unique_key": w["contract_award_unique_key"],
                    "award_keys": list(w["award_keys"]) if w["award_keys"] is not None else [],
                    "award_count": to_int(w["award_count"]), "harvested_at": harvested_at})
    if not rows:
        raise RuntimeError("no attachment rows assembled — run harvest first")

    schema = pa.schema([
        ("resource_id", pa.string()), ("file_name", pa.string()), ("mime_type", pa.string()),
        ("size_bytes", pa.int64()), ("access_level", pa.string()), ("attachment_order", pa.int64()),
        ("download_url", pa.string()), ("notice_id", pa.string()),
        ("solicitation_number", pa.string()), ("sol_norm", pa.string()),
        ("notice_type", pa.string()), ("base_type", pa.string()),
        ("notice_posted_date", pa.string()), ("notice_title", pa.string()),
        ("classification_code", pa.string()), ("naics_code", pa.string()),
        ("is_primary_target", pa.bool_()), ("solicitation_identifier", pa.string()),
        ("contract_award_unique_key", pa.string()), ("award_keys", pa.list_(pa.string())),
        ("award_count", pa.int64()), ("harvested_at", pa.timestamp("us", tz="UTC"))])
    tbl = pa.Table.from_pylist(rows, schema=schema)
    so = r2_storage_options()
    lance.write_dataset(tbl, MANIFEST_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
    ds = lance.dataset(MANIFEST_URI, storage_options=so)
    for col in INDEX_COLS:
        ds.create_scalar_index(col, index_type="BTREE")
    ds = lance.dataset(MANIFEST_URI, storage_options=so)
    out = {"uri": MANIFEST_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names)}
    print(json.dumps(out, indent=2), flush=True)
    return out


def verify() -> dict:
    import duckdb
    import lance
    so = r2_storage_options()
    ds = lance.dataset(MANIFEST_URI, storage_options=so)
    con = duckdb.connect()
    con.register("msrc", ds.scanner().to_reader())
    con.execute("CREATE TABLE m AS SELECT * FROM msrc;")
    con.unregister("msrc")
    out = con.execute("""SELECT count(*), count(DISTINCT notice_id), count(DISTINCT resource_id),
        count(*) FILTER (WHERE resource_id IS NULL OR download_url IS NULL
                         OR contract_award_unique_key IS NULL OR solicitation_identifier IS NULL)
        FROM m""").fetchone()
    con.close()
    res = {"rows": out[0], "distinct_notices": out[1], "distinct_resource_ids": out[2],
           "null_keyfields": out[3], "indices": [getattr(i, "name", str(i)) for i in ds.list_indices()]}
    print(json.dumps(res, indent=2), flush=True)
    return res


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("stage", choices=["bridge", "harvest", "sink", "verify"])
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--daemon", action="store_true",
                    help="detach the harvest (double-fork+setsid) so it survives a "
                         "session resume; logs to WORKDIR/harvest_daemon.log")
    a = ap.parse_args()
    if a.stage == "bridge":
        build_bridge()
    elif a.stage == "harvest":
        if a.daemon:
            _daemonize()
        harvest(resume=a.resume)
    elif a.stage == "sink":
        sink()
    else:
        verify()


if __name__ == "__main__":
    main()
