"""SBA DSBS certified-firm registry ingest — R2 raw land + Lance product table.

SoR  s3://data-sink/active/sba_dsbs_certified_firms/   (Lance v2.1; one row / UEI; BTREE on uei)
Raw  s3://data-sink/active/sba_dsbs_raw/<program>.json  (full upstream bulk response per program)

WHY THIS EXISTS
The factory already carries small-business *designation booleans*, but every existing source is
SAM-derived (decoded from SAM Reps & Certs) or FPDS award-stamped self-certs (only firms that already
won a prime). This lands the AUTHORITATIVE federal certification registry itself — who is actually
certified 8(a) / HUBZone / WOSB / EDWOSB / VOSB / SDVOSB right now — decoupled from SAM and prior award
activity, plus the net-new cert OVERLAY (status + entrance/exit dates) the SAM booleans never carried.

SOURCE / API CONTRACT (captured live 2026-06-20 from residential egress)
  dsbs.sba.gov now 301-redirects to https://search.certifications.sba.gov/ (the SBA "certifications
  search" SPA, a React bundle over a same-origin JSON API reverse-proxied at /_api/, Meilisearch-backed).
  The old dsbs.sba.gov host the directive probed 503'd datacenter egress; the new host serves residential.

  Discovery endpoint:  POST https://search.certifications.sba.gov/_api/v2/search
    headers: Content-Type: application/json ; browser UA ; Referer/Origin = the SPA origin (WAF politeness)
    body:    the full SPA filter-state object (see _filter_body). Only sbaCertifications.activeCerts varies.
    The server translates activeCerts -> a Meili filter on active_*_boolean (e.g. value "3" -> active_hz).
    There is NO server pagination: each program filter returns its ENTIRE active set in one response
    (HUBZone 5,233 rows / 24 MB ... VOSB 40,824 rows / 169 MB). The whole crawl is 6 bulk calls.

  activeCerts value codes (label -> value, from the bundle):
    8(a)/8(a)-JV "1,4" | HUBZone "3" | WOSB "5" | EDWOSB "6" | VOSB/VOSB-JV "7,8" | SDVOSB/SDVOSB-JV "9,10"
    NB: the WOSB filter ("5") is a superset (active_wosb OR active_edwosb) and VOSB ("7,8") includes JV.
    Program membership is therefore derived from each record's active_*_boolean flags, NOT from which
    query returned it.

  Each result record carries (123 fields): uei (always present, 0 null), cage_code, entity_detail_id,
  legal_business_name, firmographics, the per-program active_*/prev_* booleans, the flat cert overlay
  (certStatus_*/certDateStart_*/certDateExit_*), and the canonical structured `certs` array
  [{name, active, entranceDate, exitDate, status, caseNumber, servicingOffice, suspended, pending}].
  There is NO duns field (DSBS is UEI-native post-DUNS retirement). 67,234 distinct UEIs across the union;
  304 UEIs carry >1 entity_detail_id (multiple DSBS profiles) -> collapsed to one row, flags OR'd.

USAGE
    doppler run --project core-x --config prd -- \
        uv run --with requests --with pyarrow --with pylance --with duckdb --with boto3 \
        python pipelines/sba_dsbs/dsbs_ingest.py                 # full ingest (6 calls -> R2 raw -> Lance)
    doppler run --project core-x --config prd -- ... python pipelines/sba_dsbs/dsbs_ingest.py --verify
    ...  --smoke hubzone        # one program only (smoke); writes nothing unless combined with a run
    ...  --skip-fetch           # reparse cached /tmp raw responses instead of re-hitting the API
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import time
import uuid

API_URL = "https://search.certifications.sba.gov/_api/v2/search"
ORIGIN = "https://search.certifications.sba.gov"
SERVING_URI = os.environ.get("SBA_DSBS_FIRMS_URI", "s3://data-sink/active/sba_dsbs_certified_firms/")
RAW_PREFIX = os.environ.get("SBA_DSBS_RAW_PREFIX", "s3://data-sink/active/sba_dsbs_raw/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_VERSION = "dsbs_search_certifications_sba_gov_v2"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# slug -> (UI label, activeCerts value code)  — the 6 certified-program filters.
PROGRAMS: list[tuple[str, str, str]] = [
    ("8a",      "8(a) or 8(a) Joint Venture", "1,4"),
    ("hubzone", "HUBZone", "3"),
    ("wosb",    "Women-Owned Small Business (WOSB)", "5"),
    ("edwosb",  "Economically-Disadvantaged Women-Owned Small Business (EDWOSB)", "6"),
    ("vosb",    "Veteran-Owned Small Business (VOSB)", "7,8"),
    ("sdvosb",  "Service-Disabled Veteran-Owned Small Business (SDVOSB)", "9,10"),
]

# canonical program slug -> the active_*_boolean record fields that, if true, make the firm a member.
ACTIVE_FLAGS: dict[str, tuple[str, ...]] = {
    "8a":      ("active_8a_boolean", "active_8a_jv_boolean"),
    "hubzone": ("active_hz_boolean",),
    "wosb":    ("active_wosb_boolean",),
    "edwosb":  ("active_edwosb_boolean",),
    "vosb":    ("active_vosb_boolean", "active_vosb_jv_boolean"),
    "sdvosb":  ("active_sdvosb_boolean", "active_sdvosb_jv_boolean"),
}
# expected distinct-UEI floors per program (enumeration probe 2026-06-20) — smoke/sanity gate.
EXPECTED_MIN = {"8a": 3000, "hubzone": 4000, "wosb": 18000, "edwosb": 6000, "vosb": 32000, "sdvosb": 28000}


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


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _split_s3(uri: str) -> tuple[str, str]:
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def _filter_body(label: str, value: str) -> dict:
    """The SPA's full filter-state object; only sbaCertifications.activeCerts is populated."""
    return {
        "searchProfiles": {"searchTerm": ""},
        "location": {"states": [], "zipCodes": [], "counties": [], "districts": [], "msas": []},
        "sbaCertifications": {"activeCerts": [{"label": label, "value": value}],
                              "isPreviousCert": False, "operatorType": "OR"},
        "naics": {"codes": [], "isPrimary": False, "operatorType": "OR"},
        "selfCertifications": {"certifications": [], "operatorType": "OR"},
        "keywords": {"list": [], "operatorType": "OR"},
        "lastUpdated": {"date": {"label": "Anytime", "value": "anytime"}},
        "samStatus": {"isActiveSAM": False},
        "qualityAssuranceStandards": {"qas": []},
        "bondingLevels": {"constructionIndividual": "", "constructionAggregate": "",
                          "serviceIndividual": "", "serviceAggregate": ""},
        "businessSize": {"relationOperator": "AT_LEAST", "numberOfEmployees": ""},
        "annualRevenue": {"relationOperator": "AT_LEAST", "annualGrossRevenue": ""},
        "entityDetailId": "",
    }


def _fetch_program(slug: str, label: str, value: str, *, sleep: float, max_attempts: int = 6) -> bytes:
    """POST one program filter; bounded exponential backoff on 429/503/5xx/network (L55 WAF politeness)."""
    import requests

    headers = {"User-Agent": UA, "Content-Type": "application/json",
               "Accept": "application/json", "Origin": ORIGIN, "Referer": ORIGIN + "/"}
    body = json.dumps(_filter_body(label, value))
    for attempt in range(max_attempts):
        try:
            r = requests.post(API_URL, data=body, headers=headers, timeout=300)
        except requests.RequestException as exc:
            wait = min(60, 2 ** attempt)
            print(f"  [{slug}] net error {type(exc).__name__}; backoff {wait}s", flush=True)
            time.sleep(wait)
            continue
        if r.status_code == 200:
            time.sleep(sleep)
            return r.content
        if r.status_code in (429, 503) or r.status_code >= 500:
            wait = min(120, 5 * 2 ** attempt)
            print(f"  [{slug}] HTTP {r.status_code}; backoff {wait}s "
                  f"(attempt {attempt + 1}/{max_attempts})", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"[{slug}] terminal HTTP {r.status_code}: {r.text[:200]}")
    raise RuntimeError(f"[{slug}] exhausted {max_attempts} attempts")


def fetch(programs: list[str], *, sleep: float, skip_fetch: bool) -> dict[str, bytes]:
    """Return {slug: raw_response_bytes}. With skip_fetch, reload cached /tmp/dsbs_<slug>.json."""
    raw: dict[str, bytes] = {}
    for slug, label, value in PROGRAMS:
        if slug not in programs:
            continue
        cache = f"/tmp/dsbs_{slug}.json"
        if skip_fetch and os.path.exists(cache):
            with open(cache, "rb") as f:
                raw[slug] = f.read()
            print(f"  [{slug}] cached {len(raw[slug]):,} bytes", flush=True)
            continue
        content = _fetch_program(slug, label, value, sleep=sleep)
        with open(cache, "wb") as f:
            f.write(content)
        n = len(json.loads(content).get("results", []))
        print(f"  [{slug}] HTTP 200  results={n:,}  bytes={len(content):,}", flush=True)
        raw[slug] = content
    return raw


def land_raw(raw: dict[str, bytes]) -> dict[str, str]:
    """Land each program's full bulk response to R2 (ContentType application/json; NO ContentEncoding, L42)."""
    s3 = _s3_client()
    bucket, prefix = _split_s3(RAW_PREFIX)
    keys: dict[str, str] = {}
    for slug, content in raw.items():
        key = f"{prefix}{slug}.json"
        s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType="application/json")
        keys[slug] = f"s3://{bucket}/{key}"
        print(f"  raw -> {keys[slug]} ({len(content):,} bytes)", flush=True)
    return keys


# ---- record-level extraction -------------------------------------------------

def _s(v) -> str | None:
    """Coerce a scalar to VARCHAR (L9), preserving None."""
    if v is None:
        return None
    if isinstance(v, str):
        return v if v != "" else None
    return str(v)


def _pipe(values) -> str | None:
    """Pipe-join a list of strings (L54), dropping empties; None if empty."""
    if not values:
        return None
    out = [str(x) for x in values if x not in (None, "")]
    return "|".join(out) if out else None


def _active_programs(rec: dict) -> list[str]:
    return [slug for slug, flags in ACTIVE_FLAGS.items() if any(rec.get(f) for f in flags)]


def _row_from_record(rec: dict, query_slug: str, raw_key: str) -> dict:
    """Project one upstream record to the product-table shape (pre-dedup)."""
    return {
        "uei": _s(rec.get("uei")),
        "cage_code": _s(rec.get("cage_code")),
        "entity_detail_id": _s(rec.get("entity_detail_id")),
        "legal_business_name": _s(rec.get("legal_business_name")),
        "dba_name": _s(rec.get("dba_name")),
        # active designation booleans (canonical program membership)
        "active_8a": bool(rec.get("active_8a_boolean")),
        "active_8a_jv": bool(rec.get("active_8a_jv_boolean")),
        "active_hubzone": bool(rec.get("active_hz_boolean")),
        "active_wosb": bool(rec.get("active_wosb_boolean")),
        "active_edwosb": bool(rec.get("active_edwosb_boolean")),
        "active_vosb": bool(rec.get("active_vosb_boolean")),
        "active_vosb_jv": bool(rec.get("active_vosb_jv_boolean")),
        "active_sdvosb": bool(rec.get("active_sdvosb_boolean")),
        "active_sdvosb_jv": bool(rec.get("active_sdvosb_jv_boolean")),
        "prev_8a": bool(rec.get("prev_8a_boolean")),
        "prev_hubzone": bool(rec.get("prev_hz_boolean")),
        "prev_wosb": bool(rec.get("prev_wosb_boolean")),
        "prev_edwosb": bool(rec.get("prev_edwosb_boolean")),
        "prev_vosb": bool(rec.get("prev_vosb_boolean")),
        "prev_sdvosb": bool(rec.get("prev_sdvosb_boolean")),
        # flat cert overlay (the net-new signal)
        "cert_status_8a": _s(rec.get("certStatus_8a")),
        "cert_status_hubzone": _s(rec.get("certStatus_HZ")),
        "cert_status_wosb": _s(rec.get("certStatus_WOSB")),
        "cert_status_edwosb": _s(rec.get("certStatus_EDWOSB")),
        "cert_status_vosb": _s(rec.get("certStatus_VOSB")),
        "cert_status_sdvosb": _s(rec.get("certStatus_SDVOSB")),
        "cert_start_8a": _s(rec.get("certDateStart_8a")),
        "cert_start_hubzone": _s(rec.get("certDateStart_HZ")),
        "cert_start_wosb": _s(rec.get("certDateStart_WOSB")),
        "cert_start_edwosb": _s(rec.get("certDateStart_EDWOSB")),
        "cert_start_vosb": _s(rec.get("certDateStart_VOSB")),
        "cert_start_sdvosb": _s(rec.get("certDateStart_SDVOSB")),
        "cert_exit_8a": _s(rec.get("certDateExit_8a")),
        "cert_exit_vosb": _s(rec.get("certDateExit_VOSB")),
        "cert_exit_sdvosb": _s(rec.get("certDateExit_SDVOSB")),
        # full structured cert overlay (entrance/exit/status/caseNumber/servicingOffice per program)
        "certs_json": json.dumps(rec.get("certs"), separators=(",", ":")) if rec.get("certs") else None,
        # firmographics
        "naics_primary": _s(rec.get("naics_primary")),
        "naics_all_codes": _pipe(rec.get("naics_all_codes")),
        "address_1": _s(rec.get("address_1")),
        "city": _s(rec.get("city")),
        "state": _s(rec.get("state")),
        "zipcode": _s(rec.get("zipcode")),
        "county": _s(rec.get("county")),
        "congressional_district": _s(rec.get("concat_state_congressional_district")),
        "phone": _s(rec.get("phone")),
        "email": _s(rec.get("email")),
        "website": _s(rec.get("website")),
        "year_established": _s(rec.get("year_established")),
        "legal_structure": _s(rec.get("legal_structure")),
        "capabilities_narrative": _s(rec.get("capabilities_narrative")),
        "last_update_date": int(rec["last_update_date"]) if rec.get("last_update_date") else None,
        # provenance (per-record; merged at dedup)
        "_query_slug": query_slug,
        "_raw_key": raw_key,
    }


_BOOL_COLS = ("active_8a", "active_8a_jv", "active_hubzone", "active_wosb", "active_edwosb",
              "active_vosb", "active_vosb_jv", "active_sdvosb", "active_sdvosb_jv",
              "prev_8a", "prev_hubzone", "prev_wosb", "prev_edwosb", "prev_vosb", "prev_sdvosb")
_STR_COLS = ("uei", "cage_code", "entity_detail_id", "legal_business_name", "dba_name",
             "cert_status_8a", "cert_status_hubzone", "cert_status_wosb", "cert_status_edwosb",
             "cert_status_vosb", "cert_status_sdvosb",
             "cert_start_8a", "cert_start_hubzone", "cert_start_wosb", "cert_start_edwosb",
             "cert_start_vosb", "cert_start_sdvosb", "cert_exit_8a", "cert_exit_vosb", "cert_exit_sdvosb",
             "certs_json", "cert_programs",
             "naics_primary", "naics_all_codes", "address_1", "city", "state", "zipcode", "county",
             "congressional_district", "phone", "email", "website", "year_established",
             "legal_structure", "capabilities_narrative",
             "source_query_programs", "raw_keys", "scrape_run_id", "source_version")


def _active_count(row: dict) -> int:
    return sum(1 for c in _BOOL_COLS if c.startswith("active_") and row.get(c))


def dedup(rows: list[dict]) -> list[dict]:
    """Collapse to one row per UEI: OR the boolean flags, take scalars from the 'best' record
    (most active certs, then most-recent last_update_date), union provenance. Most UEIs collapse
    multiple program-query rows; a small tail (~300) carry >1 DSBS profile (distinct entity_detail_id)."""
    by_uei: dict[str, list[dict]] = {}
    for r in rows:
        if r.get("uei"):
            by_uei.setdefault(r["uei"], []).append(r)
    out: list[dict] = []
    for uei, recs in by_uei.items():
        best = max(recs, key=lambda r: (_active_count(r), r.get("last_update_date") or 0))
        merged = dict(best)
        for col in _BOOL_COLS:
            merged[col] = any(r.get(col) for r in recs)
        merged["cert_programs"] = _pipe(_active_programs_from_row(merged))
        merged["source_query_programs"] = _pipe(sorted({r["_query_slug"] for r in recs}))
        merged["raw_keys"] = _pipe(sorted({r["_raw_key"] for r in recs}))
        merged["n_source_records"] = len(recs)
        merged.pop("_query_slug", None)
        merged.pop("_raw_key", None)
        out.append(merged)
    return out


def _active_programs_from_row(row: dict) -> list[str]:
    slugs = []
    if row.get("active_8a") or row.get("active_8a_jv"):
        slugs.append("8a")
    if row.get("active_hubzone"):
        slugs.append("hubzone")
    if row.get("active_wosb"):
        slugs.append("wosb")
    if row.get("active_edwosb"):
        slugs.append("edwosb")
    if row.get("active_vosb") or row.get("active_vosb_jv"):
        slugs.append("vosb")
    if row.get("active_sdvosb") or row.get("active_sdvosb_jv"):
        slugs.append("sdvosb")
    return slugs


def _arrow_schema():
    import pyarrow as pa

    fields = [(c, pa.string()) for c in _STR_COLS]
    fields += [(c, pa.bool_()) for c in _BOOL_COLS]
    fields += [("last_update_date", pa.int64()), ("n_source_records", pa.int32()),
               ("fetched_at", pa.timestamp("us", tz="UTC"))]
    # stable column order: identity/str, bool, numeric, ts
    return pa.schema(fields)


def build(programs: list[str], *, sleep: float, skip_fetch: bool, smoke: str | None) -> dict:
    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    run_id = str(uuid.uuid4())
    fetched_at = dt.datetime.now(dt.UTC)
    sel = [smoke] if smoke else programs
    print(f"scrape_run_id={run_id}  programs={sel}", flush=True)

    raw = fetch(sel, sleep=sleep, skip_fetch=skip_fetch)
    raw_keys = land_raw(raw)

    rows: list[dict] = []
    for slug, content in raw.items():
        d = json.loads(content)
        results = d.get("results", [])
        seen = set()
        for rec in results:
            if not rec.get("uei"):
                continue
            rows.append(_row_from_record(rec, slug, raw_keys[slug]))
            seen.add(rec["uei"])
        floor = EXPECTED_MIN.get(slug, 0)
        assert len(seen) >= floor, f"floor gate [{slug}]: {len(seen)} distinct UEI < expected {floor}"
        print(f"  [{slug}] distinct UEI in query = {len(seen):,} (floor {floor:,})", flush=True)

    deduped = dedup(rows)
    for r in deduped:
        r["scrape_run_id"] = run_id
        r["source_version"] = SOURCE_VERSION
        r["fetched_at"] = fetched_at
    n = len(deduped)
    assert n > 0, "no rows produced"
    # n_source_records = how many source rows collapsed onto this UEI (mostly multi-program
    # query appearances; a small tail are true multi-profile UEIs with >1 entity_detail_id).
    multi = sum(1 for r in deduped if r["n_source_records"] > 1)
    print(f"distinct UEIs = {n:,}  (UEIs collapsing >1 source row = {multi:,})", flush=True)

    schema = _arrow_schema()
    tbl = pa.Table.from_pylist([{f.name: r.get(f.name) for f in schema} for r in deduped], schema=schema)
    assert tbl.num_rows == n

    # smoke writes to a throwaway URI so it can never clobber the real SoR.
    target = SERVING_URI.rstrip("/") + "_smoke/" if smoke else SERVING_URI
    lance.write_dataset(tbl, target, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(target, storage_options=so)
    ds.create_scalar_index("uei", index_type="BTREE")
    print("  BTREE ✓ uei", flush=True)

    back = ds.count_rows()
    assert back == n, f"write-integrity gate: {back} != {n}"
    import duckdb
    con = duckdb.connect(":memory:")
    con.register("f", ds.to_table())
    distinct = con.execute("SELECT count(DISTINCT uei) FROM f").fetchone()[0]
    assert distinct == back, f"grain gate: {distinct} distinct uei != {back} rows"
    print(f"WROTE {target}  rows={back:,}  distinct_uei={distinct:,}  cols={len(ds.schema)}", flush=True)
    return {"uri": target, "rows": back, "run_id": run_id, "raw_keys": raw_keys}


def verify() -> None:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    n = ds.count_rows()
    idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                 for i in ds.list_indices())
    print(f"{SERVING_URI}  rows={n:,}  cols={len(ds.schema)}  indices={idx}")
    con = duckdb.connect(":memory:")
    con.register("f", ds.to_table())
    print("\n-- distinct uei == rows --")
    print(con.execute("SELECT count(*) AS n_rows, count(DISTINCT uei) AS distinct_uei FROM f").df().to_string(index=False))
    print("\n-- active firms per program (distinct UEI) --")
    print(con.execute("""
        SELECT
          sum((active_8a OR active_8a_jv)::int)        AS p_8a,
          sum(active_hubzone::int)                     AS p_hubzone,
          sum(active_wosb::int)                        AS p_wosb,
          sum(active_edwosb::int)                      AS p_edwosb,
          sum((active_vosb OR active_vosb_jv)::int)    AS p_vosb,
          sum((active_sdvosb OR active_sdvosb_jv)::int) AS p_sdvosb
        FROM f""").df().to_string(index=False))
    print("\n-- cert overlay coverage (non-null status) --")
    print(con.execute("""
        SELECT
          count(*) FILTER (WHERE cert_status_hubzone IS NOT NULL) hubzone_status,
          count(*) FILTER (WHERE cert_status_8a IS NOT NULL)      a8a_status,
          count(*) FILTER (WHERE certs_json IS NOT NULL)          has_certs_json,
          count(*) FILTER (WHERE cert_programs IS NOT NULL)       has_cert_programs
        FROM f""").df().to_string(index=False))
    print("\n-- sample rows --")
    print(con.execute("""
        SELECT uei, legal_business_name, cert_programs, cert_status_hubzone, cert_start_hubzone, state
        FROM f WHERE active_hubzone ORDER BY uei LIMIT 5""").df().to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="SBA DSBS certified-firm registry ingest (R2 raw + Lance).")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--smoke", default=None, help="run a single program slug only (smoke)")
    ap.add_argument("--skip-fetch", action="store_true", help="reparse cached /tmp/dsbs_<slug>.json")
    ap.add_argument("--inter-call-sleep", type=float, default=1.0)
    ap.add_argument("--programs", default=",".join(s for s, _, _ in PROGRAMS))
    args = ap.parse_args()

    if args.verify:
        verify()
        sys.exit(0)
    progs = [p.strip() for p in args.programs.split(",") if p.strip()]
    build(progs, sleep=args.inter_call_sleep, skip_fetch=args.skip_fetch, smoke=args.smoke)
