"""SBA DSBS certified-firm registry ingest — R2 raw land + Lance product table.

SoR  s3://data-sink/active/sba_dsbs_certified_firms/   (Lance v2.1; one row / UEI; FULL-FIDELITY —
     every upstream field captured, only the per-query _rankingScore dropped; BTREE on uei/cage_code/
     entity_detail_id/meili_primary_key/naics_primary/zipcode/county, BITMAP on state/county_code/CD/
     cert_programs/msa + the active/prev/self_* program+ownership flags)
Raw  s3://data-sink/active/sba_dsbs_raw/<program>.json  (full upstream bulk response per program)

SCHEMA NOTES
- alt_cage_codes / alt_entity_detail_ids / alt_meili_primary_keys: ~300 UEIs carry >1 DSBS profile;
  the scalar key holds the deterministic primary, the alt_* pipe column preserves the rest (no loss).
- 'certs' (JSON) is the AUTHORITATIVE cert overlay — carries entranceDate/exitDate/status/caseNumber/
  servicingOffice/suspended/pending per program (ISO dates). The flat certStatus_*/certDateStart_*/
  certDateExit_* columns are a normalized-to-ISO convenience subset (exit only present for 8a/VOSB/SDVOSB).
- int64: last_update_date (Unix epoch s), construction/service_bonding_* (dollar amounts). All else VARCHAR.
- Structurally-null on this source (kept for forward-compat, not missing data): annual_revenue,
  business_size, certStatus_VOSBJV/SDVOSBJV, certDateStart_VOSBJV/SDVOSBJV, size_determination_description.
- Opaque codes left raw (decode is Phase-2, cross-source): business_type_codes, legal_structure,
  exporter_status, sam_extract_code, naics_exception_codes.

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
import json
import os
import re
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


# Full-fidelity capture: land EVERY upstream field. Only drop is _rankingScore — a per-query
# Meilisearch relevance score (an artifact of the filter call, not a firm attribute).
_DROP_KEYS = frozenset({"_rankingScore"})

# Derived/provenance columns added on top of the native fields (all VARCHAR except where noted).
# alt_* preserve every distinct resolution key a multi-profile UEI carried across its source rows
# (the scalar column holds the deterministic primary; the alt_* pipe column holds the rest — no loss).
_DERIVED_STR = ("cert_programs", "alt_cage_codes", "alt_entity_detail_ids", "alt_meili_primary_keys",
                "source_query_programs", "raw_keys", "scrape_run_id", "source_version")
# Preferred lead-column order for readability; every remaining native field follows alphabetically.
_LEAD = ("uei", "cage_code", "alt_cage_codes", "entity_detail_id", "meili_primary_key",
         "legal_business_name", "dba_name", "contact_person", "current_principals",
         "cert_programs", "email", "phone", "fax", "website", "additional_website",
         "address_1", "address_2", "city", "state", "zipcode", "county", "naics_primary")
_PA_TYPE = {"bool": "bool_", "list": "string", "json": "string", "str": "string", "int": "int64"}
# Precedence on mixed/null types. 'null' is below everything so a later real type always wins over a
# null-seeded default (else a field first seen as null, then bool — e.g. certPending_* — stays text).
_PREC = {"json": 4, "list": 3, "str": 2, "bool": 1, "null": 0}
# Native integer fields pinned to int64 (clean ints used in magnitude compares/sort; VARCHAR would
# sort lexicographically — '9' > '10000000'). last_update_date is Unix epoch seconds.
NUMERIC_INT64 = frozenset({"last_update_date", "construction_bonding_aggregate",
                           "construction_bonding_per_contract", "service_bonding_aggregate",
                           "service_bonding_contract"})
_DATE_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def _is_designation(key: str) -> bool:
    return key.startswith("active_") or key.startswith("prev_")


def classify_fields(records: list[dict]) -> dict[str, str]:
    """Assign each native key a stable kind from the data: 'bool' | 'int' | 'list' | 'json' | 'str'.
    Precedence (json > list > str > bool > null): a key is 'bool' only if every non-null value is
    bool; any nested value forces JSON; a list whose scalar elements can contain the pipe delimiter
    is escalated to JSON (else the pipe-join is irreversible — e.g. keywords like 'C | C++'); a key
    seen only as null defaults to VARCHAR (forward-compatible) but a later real type still wins.
    Numbers/strings -> VARCHAR per L9, except the NUMERIC_INT64 fields which are pinned to int64."""
    kinds: dict[str, str] = {}
    for r in records:
        for k, v in r.items():
            if k in _DROP_KEYS:
                continue
            if v is None:
                kinds.setdefault(k, "null")
                continue
            if isinstance(v, bool):
                kk = "bool"
            elif isinstance(v, list):
                pipe_unsafe = any("|" in x for x in v if isinstance(x, str))
                nested = any(isinstance(x, (dict, list)) for x in v)
                kk = "json" if (nested or pipe_unsafe) else "list"
            elif isinstance(v, dict):
                kk = "json"
            else:                       # int / float / str -> VARCHAR per L9 (TRY_CAST downstream, L29)
                kk = "str"
            cur = kinds.get(k)
            if cur is None or _PREC[kk] > _PREC[cur]:
                kinds[k] = kk
    kinds = {k: ("str" if kind == "null" else kind) for k, kind in kinds.items()}
    for k in NUMERIC_INT64:
        if k in kinds:
            kinds[k] = "int"
    return kinds


def _iso_date(v):
    """MM/DD/YYYY -> YYYY-MM-DD (so the flat cert dates match the ISO dates inside certs and sort as
    text). Pass through anything that isn't that exact shape."""
    if not isinstance(v, str):
        return v
    m = _DATE_RE.match(v)
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)}" if m else v


def _cast(v, kind: str):
    if kind == "bool":
        return None if v is None else bool(v)
    if kind == "int":
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    if kind == "list":
        return _pipe(v)
    if kind == "json":
        return json.dumps(v, separators=(",", ":")) if v else None
    return _s(v)


def project(rec: dict, kinds: dict, query_slug: str, raw_key: str) -> dict:
    """Full-fidelity per-record projection: every native key cast per its kind + transient provenance."""
    row = {k: _cast(rec.get(k), kind) for k, kind in kinds.items()}
    for k in row:                       # normalize the flat cert dates to ISO for sort + consistency
        if k.startswith("certDateStart_") or k.startswith("certDateExit_"):
            row[k] = _iso_date(row[k])
    row["_query_slug"] = query_slug
    row["_raw_key"] = raw_key
    row["_lud"] = rec["last_update_date"] if isinstance(rec.get("last_update_date"), int) else 0
    return row


def _active_count(row: dict) -> int:
    return sum(1 for k, v in row.items() if k.startswith("active_") and v)


def _active_programs_from_native(row: dict) -> list[str]:
    slugs = []
    if row.get("active_8a_boolean") or row.get("active_8a_jv_boolean"):
        slugs.append("8a")
    if row.get("active_hz_boolean"):
        slugs.append("hubzone")
    if row.get("active_wosb_boolean"):
        slugs.append("wosb")
    if row.get("active_edwosb_boolean"):
        slugs.append("edwosb")
    if row.get("active_vosb_boolean") or row.get("active_vosb_jv_boolean"):
        slugs.append("vosb")
    if row.get("active_sdvosb_boolean") or row.get("active_sdvosb_jv_boolean"):
        slugs.append("sdvosb")
    return slugs


def dedup(rows: list[dict], kinds: dict) -> list[dict]:
    """Collapse to one row per UEI: OR the designation flags (active_*/prev_*) across the UEI's
    source rows, take all other fields from the 'best' record (most active certs, then most-recent
    last_update_date), union provenance. Most UEIs collapse multiple program-query rows; a small
    tail (~300) carry >1 DSBS profile (distinct entity_detail_id)."""
    desig = [k for k, kind in kinds.items() if _is_designation(k) and kind == "bool"]
    by_uei: dict[str, list[dict]] = {}
    for r in rows:
        if r.get("uei"):
            by_uei.setdefault(r["uei"], []).append(r)
    out: list[dict] = []
    for uei, recs in by_uei.items():
        # deterministic winner (reproducible across re-runs): most active certs, then most-recent
        # update, then entity_detail_id / cage_code as stable tiebreakers.
        best = max(recs, key=lambda r: (_active_count(r), r.get("_lud") or 0,
                                        r.get("entity_detail_id") or "", r.get("cage_code") or ""))
        merged = dict(best)
        for col in desig:
            merged[col] = any(r.get(col) for r in recs)
        merged["cert_programs"] = _pipe(_active_programs_from_native(merged))
        # preserve EVERY distinct resolution key across the UEI's source profiles (no silent loss).
        merged["alt_cage_codes"] = _pipe(sorted({r.get("cage_code") for r in recs if r.get("cage_code")}))
        merged["alt_entity_detail_ids"] = _pipe(sorted({r.get("entity_detail_id") for r in recs if r.get("entity_detail_id")}))
        merged["alt_meili_primary_keys"] = _pipe(sorted({r.get("meili_primary_key") for r in recs if r.get("meili_primary_key")}))
        merged["source_query_programs"] = _pipe(sorted({r["_query_slug"] for r in recs}))
        merged["raw_keys"] = _pipe(sorted({r["_raw_key"] for r in recs}))
        merged["n_source_records"] = len(recs)
        for k in ("_query_slug", "_raw_key", "_lud"):
            merged.pop(k, None)
        out.append(merged)
    return out


def _ordered_columns(kinds: dict) -> list[str]:
    lead = [k for k in _LEAD if k in kinds or k in _DERIVED_STR]
    placed = set(lead)
    rest_native = sorted(k for k in kinds if k not in placed)
    rest_derived = [k for k in _DERIVED_STR if k not in placed]
    return lead + rest_native + rest_derived


def _arrow_schema(kinds: dict):
    import pyarrow as pa

    def pa_type(name: str):
        kind = kinds.get(name) or ("str" if name in _DERIVED_STR else "str")
        return getattr(pa, _PA_TYPE[kind])()

    fields = [(c, pa_type(c)) for c in _ordered_columns(kinds)]
    fields += [("n_source_records", pa.int32()), ("fetched_at", pa.timestamp("us", tz="UTC"))]
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

    # parse all program responses, keeping the raw upstream records (full fidelity).
    parsed: list[tuple[dict, str, str]] = []
    for slug, content in raw.items():
        results = json.loads(content).get("results", [])
        seen = set()
        for rec in results:
            if not rec.get("uei"):
                continue
            parsed.append((rec, slug, raw_keys[slug]))
            seen.add(rec["uei"])
        floor = EXPECTED_MIN.get(slug, 0)
        assert len(seen) >= floor, f"floor gate [{slug}]: {len(seen)} distinct UEI < expected {floor}"
        print(f"  [{slug}] distinct UEI in query = {len(seen):,} (floor {floor:,})", flush=True)

    # classify every native field once, then project full-fidelity rows.
    kinds = classify_fields([rec for rec, _, _ in parsed])
    print(f"native fields captured = {len(kinds)} (+ derived/provenance)", flush=True)
    rows = [project(rec, kinds, slug, rk) for rec, slug, rk in parsed]

    deduped = dedup(rows, kinds)
    for r in deduped:
        r["scrape_run_id"] = run_id
        r["source_version"] = SOURCE_VERSION
        r["fetched_at"] = fetched_at
    n = len(deduped)
    assert n > 0, "no rows produced"
    multi = sum(1 for r in deduped if r["n_source_records"] > 1)
    print(f"distinct UEIs = {n:,}  (UEIs collapsing >1 source row = {multi:,})", flush=True)

    # CAGE-preservation gate: every distinct source CAGE survives in cage_code ∪ alt_cage_codes.
    raw_cages = {rec.get("cage_code") for rec, _, _ in parsed if rec.get("cage_code")}
    landed_cages: set = set()
    for r in deduped:
        if r.get("cage_code"):
            landed_cages.add(r["cage_code"])
        if r.get("alt_cage_codes"):
            landed_cages.update(r["alt_cage_codes"].split("|"))
    dropped = raw_cages - landed_cages
    assert not dropped, f"cage-preservation gate: {len(dropped)} source CAGE codes dropped"
    print(f"CAGE preserved: raw_distinct={len(raw_cages):,} landed(primary+alt)={len(landed_cages):,}", flush=True)

    # warn (not fatal): multi-profile UEIs whose certs diverge across source rows — the 'best record'
    # pick keeps one; today these are pure dupes (0), but flag if a future snapshot diverges.
    certs_by_uei: dict[str, set] = {}
    for rec, _, _ in parsed:
        sig = json.dumps(rec.get("certs"), separators=(",", ":")) if rec.get("certs") else ""
        certs_by_uei.setdefault(rec["uei"], set()).add(sig)
    divergent = sum(1 for sigs in certs_by_uei.values() if len(sigs) > 1)
    if divergent:
        print(f"WARN: {divergent} UEIs have divergent certs across source profiles (best-record kept)", flush=True)

    schema = _arrow_schema(kinds)
    tbl = pa.Table.from_pylist([{f.name: r.get(f.name) for f in schema} for r in deduped], schema=schema)
    assert tbl.num_rows == n

    # smoke writes to a throwaway URI so it can never clobber the real SoR.
    target = SERVING_URI.rstrip("/") + "_smoke/" if smoke else SERVING_URI
    lance.write_dataset(tbl, target, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(target, storage_options=so)

    # index generously — storage is cheap. BTREE on resolution/join + high-cardinality geo keys;
    # BITMAP on low-cardinality filter/facet columns (program, geo, ownership, win-back).
    btree = [c for c in ("uei", "cage_code", "entity_detail_id", "meili_primary_key",
                         "naics_primary", "zipcode", "county")
             if c in schema.names]
    bitmap = [c for c in (
        # geo + program faceting
        "state", "county_code", "concat_state_congressional_district", "cert_programs", "msa",
        # active program membership (vosb_jv/sdvosb_jv are 100% false on this source -> omitted)
        "active_8a_boolean", "active_8a_jv_boolean", "active_hz_boolean", "active_wosb_boolean",
        "active_edwosb_boolean", "active_vosb_boolean", "active_sdvosb_boolean",
        # previously-certified (win-back segment) + distinct self-cert ownership flags
        "prev_8a_boolean", "prev_hz_boolean", "prev_wosb_boolean", "prev_edwosb_boolean",
        "prev_vosb_boolean", "prev_sdvosb_boolean",
        "self_native_owned_boolean", "self_american_indian_owned_boolean", "self_tribal_owned_boolean",
        "self_anc_owned_boolean", "self_nho_boolean", "self_hubzone_jv_boolean", "self_cdc_boolean")
        if c in schema.names]
    for col in btree:
        ds.create_scalar_index(col, index_type="BTREE")
        print(f"  BTREE  ✓ {col}", flush=True)
    for col in bitmap:
        try:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}", flush=True)
        except Exception as exc:  # noqa: BLE001 — bitmap optional; don't fail the run
            print(f"  BITMAP skip {col}: {exc}", flush=True)

    back = ds.count_rows()
    assert back == n, f"write-integrity gate: {back} != {n}"
    import duckdb
    con = duckdb.connect(":memory:")
    con.register("f", ds.to_table())
    distinct = con.execute("SELECT count(DISTINCT uei) FROM f").fetchone()[0]
    assert distinct == back, f"grain gate: {distinct} distinct uei != {back} rows"
    # completeness gate: every captured native field is a column in the written dataset.
    missing = [k for k in kinds if k not in schema.names]
    assert not missing, f"completeness gate: native fields dropped from schema: {missing}"
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
    print(f"columns ({len(ds.schema.names)}):", ds.schema.names)
    print("\n-- active firms per program (distinct UEI) --")
    print(con.execute("""
        SELECT
          sum((active_8a_boolean OR active_8a_jv_boolean)::int)        AS p_8a,
          sum(active_hz_boolean::int)                                  AS p_hubzone,
          sum(active_wosb_boolean::int)                                AS p_wosb,
          sum(active_edwosb_boolean::int)                              AS p_edwosb,
          sum((active_vosb_boolean OR active_vosb_jv_boolean)::int)    AS p_vosb,
          sum((active_sdvosb_boolean OR active_sdvosb_jv_boolean)::int) AS p_sdvosb
        FROM f""").df().to_string(index=False))
    print("\n-- coverage of high-value fields (non-null) --")
    print(con.execute("""
        SELECT
          count(email) email, count(phone) phone, count(website) website,
          count(contact_person) contact_person, count(current_principals) principals,
          count(capabilities_narrative) cap_narr, count(keywords) keywords,
          count(certStatus_HZ) hz_status, count(certs) certs_json, count(cert_programs) cert_programs
        FROM f""").df().to_string(index=False))
    print("\n-- sample rows --")
    print(con.execute("""
        SELECT uei, legal_business_name, cert_programs, contact_person, email, certStatus_HZ, state
        FROM f WHERE active_hz_boolean ORDER BY uei LIMIT 5""").df().to_string(index=False))


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
