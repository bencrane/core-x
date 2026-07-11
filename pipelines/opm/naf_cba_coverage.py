"""NAF CBA coverage crosswalk — organized federal NAF bargaining units → wage areas.

Links each Department-of-Defense **NAF (nonappropriated-fund) instrumentality** CBA in
`opm_cba_index` to the NAF wage area / installation that prices it, joining the labor-
relations layer to the wage layer already in the SoR:

    opm_cba_index  ──(this crosswalk)──▶  naf_wage_area_geography  ──▶  naf_wage_rates
    (who is organized,                    (installation → wage_area)     (wage_area → $/hr
     under which union/agreement)                                         by series/grade/step)

WHY A CROSSWALK, NOT A WAGE EXTRACTION (measured 2026-07-11, full 89-PDF scan):
NAF CBAs do NOT carry negotiated wage tables — 82% (73/89) explicitly defer base pay to the
DoD NAF Wage Schedule Division (WSD) survey process, i.e. they ADOPT the standard NAF wage
schedule that IS `naf_wage_rates`; only 4/89 contain a rate table at all. The genuinely
negotiated dollar content (shift/night/Sunday differential, commission, allowances) is thin
and heterogeneous prose in ~a dozen CBAs — not a structured appendix. The defensible,
deterministic product this corpus supports is therefore the COVERAGE crosswalk: which
installations have organized NAF labor, under which agreement, joinable to the wage schedule
that governs them. (Evidence: docs/reference/OPM_CBA_INGEST.md; this module's ops row.)

NAF slice = `opm_cba_index` rows with agency_name='Department of Defense' AND a strong NAF-
instrumentality token (NAF|nonappropriated|AAFES|NEXCOM|MWR|MCCS|morale|commissary|exchange)
in sub-agency / region / filename / union. ~91 documents.

MATCH METHOD (deterministic, tiered; reverse-dictionary against the 555-installation NAF geo
vocabulary rather than brittle filename parsing):
  T1 filename_exact — a full normalized geo installation name appears in the CBA filename.
  T2 text_exact     — …appears in the CBA's extracted PDF text (full body).
  T3 token          — the CBA's salient place token(s) hit a geo installation's distinctive
                      tokens; a LONE generic token (df>3 across geo, e.g. "ACADEMY") is
                      rejected to avoid cross-installation collisions.
  T4 enterprise     — AAFES/NEXCOM master agreements with no single installation → agency scope
                      (wage_area null; covers the whole employer's geography).
  T5 unmatched      — no installation derivable (union-only redacted filenames, HQ elements,
                      installations absent from NAF wage geography). Recorded honestly.
Installation→wage_area is near-unique in geo (only 16 generic names span >1 area); a multi-
area match emits one row per wage_area flagged ambiguous_wage_area.

Output (Lance v2.1, Gen-3 active/ SoR):
    s3://data-sink/active/naf_cba_coverage/   one row per (cba_id × wage_area); enterprise/
        unmatched carry wage_area=NULL. Ledger: ops.opm_cba_runs.

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with boto3 --with pypdf --with 'psycopg[binary]' \
        python -m pipelines.opm.naf_cba_coverage
    ... --coverage-uri s3://data-sink/active/_smoke_naf_cba_coverage/   # smoke
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import re
from collections import Counter, defaultdict

from pipelines.bls.ingest import (  # noqa: E402
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)
from pipelines.opm.opm_cba import BLOB_PREFIX, _record_run  # reuse blob prefix + ledger writer

BUCKET = "data-sink"
INDEX_URI = os.environ.get("OPM_CBA_INDEX_URI", f"s3://{BUCKET}/active/opm_cba_index/")
GEO_URI = os.environ.get("NAF_GEO_URI", f"s3://{BUCKET}/active/naf_wage_area_geography/")
COVERAGE_URI = os.environ.get("NAF_CBA_COVERAGE_URI", f"s3://{BUCKET}/active/naf_cba_coverage/")

SOURCE = "naf_cba_coverage (opm_cba_index NAF slice ⋈ naf_wage_area_geography)"

# NAF-instrumentality slice selector (DoD + strong NAF token).
_STRONG_NAF = re.compile(
    r"\bNAF\b|nonappropriated|AAFES|NEXCOM|\bMWR\b|MCCS|morale|commissary|\bexchange\b", re.I)
# Enterprise (agency-wide) master agreements with no single installation.
_ENTERPRISE = re.compile(r"AAFES|ARMY (AND|&) AIR FORCE EXCHANGE", re.I)
_BASE_HINT = re.compile(
    r"AFB|FORT|\bFT |NAVAL|\bNAS |NEXCOM|STATION|DEPOT|BASE|ANDREWS|LANGLEY|RANDOLPH|"
    r"LACKLAND|SHAFTER|ACADEMY", re.I)

# Abbreviation expansion applied identically to both sides before matching.
_ABBR = [
    (r"\bAFB\b", "AIR FORCE BASE"), (r"\bAFS\b", "AIR FORCE STATION"),
    (r"\bARB\b", "AIR RESERVE BASE"), (r"\bNAS\b", "NAVAL AIR STATION"),
    (r"\bNSA\b", "NAVAL SUPPORT ACTIVITY"), (r"\bNSB\b", "NAVAL SUBMARINE BASE"),
    (r"\bNS\b", "NAVAL STATION"), (r"\bJB\b", "JOINT BASE"), (r"\bFT\b", "FORT"),
    (r"\bMCAS\b", "MARINE CORPS AIR STATION"), (r"\bMCB\b", "MARINE CORPS BASE"),
    (r"\bINTL\b", "INTERNATIONAL"), (r"\bPEARLHARBOR\b", "PEARL HARBOR"),  # all-caps glue _decat can't split
]
# Facility-type / org tokens that must never be the SOLE basis of a token match.
_STOP = {
    "NAF", "CBA", "AGREEMENT", "COLLECTIVE", "BARGAINING", "LOCAL", "UNION", "AFGE", "NAGE",
    "NAIL", "IAM", "IAMAW", "SEIU", "LIUNA", "NFFE", "IFPTE", "HCDCU", "DEPARTMENT", "OF",
    "THE", "AND", "ARMY", "NAVY", "AIR", "FORCE", "USAF", "DOD", "DEPT", "NEXCOM", "AAFES",
    "MWR", "MCCS", "EXCHANGE", "SERVICE", "SERVICES", "BASE", "FORT", "JOINT", "NAVAL",
    "STATION", "AIRFIELD", "REDACTED", "IN", "NEGOTIATIONS", "CENTER", "CORPS", "MARINE",
    "DISTRIBUTION", "NORTHEASTERN", "DECA", "SUPPORT", "ACTIVITY", "HQ", "NA", "NG",
    "ACADEMY", "HOSPITAL", "CLUB", "MESS", "OFFICERS", "RESERVE", "DEPOT", "REGIONAL",
    # document-boilerplate + branch/org acronyms — never place names
    "MASTER", "LABOR", "AGREEMENT", "VERSION", "COMPLIANT", "AREA", "USMC", "USN", "USA",
    "VENDING", "SUBMISSION", "FORM", "LOGISTICS", "COMMAND", "FACILITY", "ARMY", "AIRPORT",
}
# Generic installation names that are NOT place-distinctive — they resolve to whatever wage
# area they happen to sit in and recur across many CBA bodies. Never match on these.
_GENERIC = {
    "NAVAL HOSPITAL", "FISHER HOUSE", "NAVAL SUPPORT ACTIVITY", "ARMY RESERVE CENTER",
    "NAVAL SUBMARINE BASE", "NAVAL AIR STATION", "MARINE CORPS EXCHANGE", "NAVAL STATION",
    "UNITED STATES MARINE CORPS EXCHANG", "NAVAL BASE", "NAVY EXCHANGE SERVICE CENTER",
    "NAVY EXCHANGE SERVICE COMMAND", "NAVAL REGIONAL MEDICAL CENTER", "NAVAL WEAPONS STATION",
    "NAVY EXCHANGE", "MARINE CORPS BASE", "MARINE CORPS AIR STATION", "NAVAL SUPPORT FACILITY",
    "NAVAL MEDICAL CENTER", "NAVAL AMPHIBIOUS BASE", "NAVAL COMPUTER AND TELECOMMUNICATIONS",
}
_TOKEN_DF_MAX = 1  # a LONE overlapping token is trusted only if it uniquely identifies 1 installation


def _decat(s: str) -> str:
    """Split glued filename tokens: camelCase and digit↔letter boundaries.
    "1143ArmyAnniston NAFCBA04122022" → "1143 Army Anniston NAFCBA 04122022"."""
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    s = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", s)
    s = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", s)
    return s


def _norm(s: str | None) -> str:
    s = _decat(s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    for a, b in _ABBR:
        s = re.sub(a, b, s)
    return re.sub(r"\s+", " ", s).strip()


def _s(v) -> str | None:
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def _naf_employer(sub: str | None, file_name: str | None, geo_agency: str | None) -> str | None:
    """Coarse NAF-employer category for the BITMAP facet."""
    blob = f"{sub or ''} {file_name or ''} {geo_agency or ''}".upper()
    if "AAFES" in blob or "AIR FORCE EXCHANGE" in blob:
        return "AAFES"
    if "NEXCOM" in blob or "NAVY EXCHANGE" in blob:
        return "NEXCOM"
    if "COMMISSARY" in blob or "DECA" in blob:
        return "DeCA"
    if "MARINE" in blob or "MCCS" in blob:
        return "MCCS"
    if "NAVY" in blob or "NAVAL" in blob:
        return "Navy MWR"
    if "AIR FORCE" in blob or "USAF" in blob:
        return "AF Services/MWR"
    if "ARMY" in blob:
        return "Army MWR"
    return None


# ── geo vocabulary ──────────────────────────────────────────────────────────────────
def _build_geo_vocab(geo_rows: list) -> tuple[dict, dict]:
    """normalized installation → {wa:set, agency:Counter, st:set}; and distinctive token → {inst}."""
    voc: dict = defaultdict(lambda: {"wa": set(), "agency": Counter(), "st": set()})
    for r in geo_rows:
        if r["row_kind"] != "installation" or not r["installation"]:
            continue
        n = _norm(r["installation"])
        if len(n) < 5:
            continue
        voc[n]["wa"].add(r["wage_area"])
        voc[n]["agency"][r["agency"]] += 1
        if r["state"]:
            voc[n]["st"].add(r["state"])
    tok2inst: dict = defaultdict(set)
    for n in voc:
        for t in n.split():
            if t in _STOP or len(t) < 3 or t.isdigit():
                continue
            tok2inst[t].add(n)
    return dict(voc), tok2inst


def _place_tokens(file_name: str) -> list:
    s = _norm(re.sub(r"\d{6,}", "", file_name or ""))
    return [t for t in s.split() if t not in _STOP and len(t) >= 3 and not t.isdigit()]


def _token_match(toks: list, voc: dict, tok2inst: dict):
    """Best distinctive-token candidate, or None.

    Accept when: (a) ≥2 tokens overlap one installation; (b) a lone token uniquely identifies
    one installation (df==1); or (c) a lone token hits several installations that ALL resolve
    to the SAME wage_area — the installation identity is then approximate but the wage-area
    bind (the join key) is exact (e.g. "NORFOLK" hits 3 Norfolk facilities, all wa=111).
    A lone token spanning >1 wage_area (e.g. "PORTSMOUTH" VA vs NH) is rejected."""
    cand: dict = defaultdict(int)
    for t in toks:
        for n in tok2inst.get(t, ()):
            if n in _GENERIC:
                continue
            cand[n] += 1
    if not cand:
        return None
    n, overlap = max(cand.items(), key=lambda kv: (kv[1], len(kv[0])))
    if overlap >= 2:
        return n, voc[n]
    shared = [t for t in toks if n in tok2inst.get(t, ())]
    if not shared:
        return None
    if all(len(tok2inst[t]) <= _TOKEN_DF_MAX for t in shared):
        return n, voc[n]
    # (c) same-wage-area collapse over the sharpest shared token
    t = min(shared, key=lambda t: len(tok2inst[t]))
    others = [m for m in tok2inst[t] if m not in _GENERIC]
    wa_union = set()
    for m in others:
        wa_union |= voc[m]["wa"]
    if others and len(wa_union) == 1:
        best = max(others, key=len)
        return best, voc[best]
    return None


def _match(file_name: str, text: str, vocab_sorted: list, voc: dict, tok2inst: dict):
    """Return (installation, meta, tier) or None.

    Filename-primary: the CBA filename is the authoritative installation signal (these CBAs are
    named by location); the PDF body mentions many incidental bases and must NOT drive identity.
      T1 filename_exact  — full geo installation name in the filename.
      T2 filename_token  — distinctive place token(s) from the filename hit a geo installation.
      T3 text_lowconf    — final fallback for the still-unmatched: a full geo name in the CBA's
                           RECOGNITION ARTICLE (first ~3500 chars, where the bargaining unit /
                           covering installation is defined). Restricted to that window and to
                           non-generic names to avoid binding on incidental body mentions of
                           nearby bases. Flagged low confidence.
    """
    hay_fn = _norm(file_name)
    for n, meta in vocab_sorted:
        if n in _GENERIC or len(n) < 8:
            continue
        if n in hay_fn:
            return n, meta, "T1_filename_exact"
    toks = _place_tokens(file_name)
    m = _token_match(toks, voc, tok2inst)
    if m:
        return m[0], m[1], "T2_filename_token"
    hay_txt = _norm(text[:3500])
    for n, meta in vocab_sorted:
        if n in _GENERIC or len(n) < 10:
            continue
        if n in hay_txt:
            return n, meta, "T3_text_lowconf"
    return None


# ── PDF text ────────────────────────────────────────────────────────────────────────
def _extract_text(s3, doc_id: str) -> str:
    from pypdf import PdfReader
    try:
        b = s3.get_object(Bucket=BUCKET, Key=f"{BLOB_PREFIX}{doc_id}.pdf")["Body"].read()
        rd = PdfReader(io.BytesIO(b))
        return "".join((p.extract_text() or "") for p in rd.pages)
    except Exception:  # noqa: BLE001 — a bad blob just yields no text (→ filename/token tiers only)
        return ""


def _coverage_schema():
    import pyarrow as pa
    return pa.schema([
        ("cba_id", pa.string()), ("agency_name", pa.string()),
        ("sub_agency_or_component", pa.string()), ("labor_union_name", pa.string()),
        ("local", pa.string()), ("expiration_date", pa.string()), ("file_name", pa.string()),
        ("naf_employer", pa.string()), ("installation", pa.string()),
        ("wage_area", pa.string()), ("naf_area", pa.string()), ("geo_agency", pa.string()),
        ("geo_state", pa.string()), ("match_tier", pa.string()),
        ("match_confidence", pa.string()), ("ambiguous_wage_area", pa.bool_()),
        ("source", pa.string()), ("built_at", pa.timestamp("us", tz="UTC")),
    ])


_CONF = {"T1_filename_exact": "high", "T2_filename_token": "high", "T3_text_lowconf": "low",
         "T4_enterprise": "enterprise", "T5_unmatched": "none"}


def run(*, storage_options: dict, index_uri: str = INDEX_URI, geo_uri: str = GEO_URI,
        coverage_uri: str = COVERAGE_URI) -> dict:
    import lance
    import pyarrow as pa

    started_at = dt.datetime.now(dt.timezone.utc)
    so = storage_options
    s3 = _s3_client()

    idx = lance.dataset(index_uri, storage_options=so).to_table().to_pylist()
    geo = lance.dataset(geo_uri, storage_options=so).to_table().to_pylist()
    voc, tok2inst = _build_geo_vocab(geo)
    vocab_sorted = sorted(voc.items(), key=lambda kv: -len(kv[0]))
    # naf_area lookup per wage_area (geo carries both; naf_area == wage_area in practice but kept)
    wa_naf = {}
    for r in geo:
        if r["wage_area"] and r["wage_area"] not in wa_naf:
            wa_naf[r["wage_area"]] = r["naf_area"]

    slice_ = [r for r in idx if r["agency_name"] == "Department of Defense"
              and _STRONG_NAF.search(" ".join(str(r.get(k) or "") for k in
                  ("sub_agency_or_component", "activity_office_region", "file_name",
                   "labor_union_name")))]
    print(f"NAF slice: {len(slice_)} CBAs", flush=True)

    rows = []
    tiers: Counter = Counter()
    for r in slice_:
        did, fn = r["id"], r["file_name"] or ""
        text = _extract_text(s3, did)
        m = _match(fn, text, vocab_sorted, voc, tok2inst)
        base = {
            "cba_id": did, "agency_name": _s(r["agency_name"]),
            "sub_agency_or_component": _s(r["sub_agency_or_component"]),
            "labor_union_name": _s(r["labor_union_name"]), "local": _s(r["local"]),
            "expiration_date": _s(r["expiration_date"]), "file_name": _s(fn),
            "source": SOURCE, "built_at": started_at,
        }
        if m:
            n, meta, tier = m
            was = sorted(meta["wa"])
            geo_agency = _s(dict(meta["agency"].most_common(1)).popitem()[0]) if meta["agency"] else None
            geo_state = ";".join(sorted(meta["st"])) or None
            emp = _naf_employer(r["sub_agency_or_component"], fn, geo_agency)
            for wa in was:
                rows.append({**base, "naf_employer": emp, "installation": n, "wage_area": wa,
                             "naf_area": wa_naf.get(wa), "geo_agency": geo_agency,
                             "geo_state": geo_state, "match_tier": tier,
                             "match_confidence": _CONF[tier], "ambiguous_wage_area": len(was) > 1})
            tiers[tier] += 1
        elif _ENTERPRISE.search(fn) and not _BASE_HINT.search(fn):
            emp = _naf_employer(r["sub_agency_or_component"], fn, None)
            rows.append({**base, "naf_employer": emp, "installation": None, "wage_area": None,
                         "naf_area": None, "geo_agency": None, "geo_state": None,
                         "match_tier": "T4_enterprise", "match_confidence": "enterprise",
                         "ambiguous_wage_area": False})
            tiers["T4_enterprise"] += 1
        else:
            emp = _naf_employer(r["sub_agency_or_component"], fn, None)
            rows.append({**base, "naf_employer": emp, "installation": None, "wage_area": None,
                         "naf_area": None, "geo_agency": None, "geo_state": None,
                         "match_tier": "T5_unmatched", "match_confidence": "none",
                         "ambiguous_wage_area": False})
            tiers["T5_unmatched"] += 1

    schema = _coverage_schema()
    status, error_text, built = "error", None, []
    try:
        tbl = pa.Table.from_pylist(rows, schema=schema)
        lance.write_dataset(tbl, coverage_uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        print(f"wrote {tbl.num_rows} coverage rows -> {coverage_uri}", flush=True)
        built = _build_indexes(coverage_uri, btree=["cba_id", "wage_area"],
                               bitmap=["match_tier", "naf_employer", "labor_union_name"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc); print(f"FATAL: {exc}", flush=True); raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        bound = sum(v for k, v in tiers.items()
                    if k in ("T1_filename_exact", "T2_filename_token", "T3_text_lowconf"))
        cov = {"slice_cbas": len(slice_), "coverage_rows": len(rows),
               "tiers": dict(tiers), "bound_to_wage_area": bound,
               "bind_rate": round(bound / max(1, len(slice_)), 4),
               "distinct_wage_areas": len({r["wage_area"] for r in rows if r["wage_area"]})}
        _record_run("naf_cba_coverage", coverage_uri, len(rows), built, status, error_text,
                    cov, started_at, completed_at)
        print(f"COVERAGE SUMMARY: {cov} status={status}", flush=True)
    return {"status": status, "rows": len(rows), "tiers": dict(tiers), "indexes": built}


def _cli() -> None:
    p = argparse.ArgumentParser(description="NAF CBA coverage crosswalk (opm_cba ⋈ naf geography).")
    p.add_argument("--index-uri", default=INDEX_URI)
    p.add_argument("--geo-uri", default=GEO_URI)
    p.add_argument("--coverage-uri", default=COVERAGE_URI)
    a = p.parse_args()
    out = run(storage_options=_storage_options(), index_uri=a.index_uri, geo_uri=a.geo_uri,
              coverage_uri=a.coverage_uri)
    print("RESULT:", json.dumps(out), flush=True)


if __name__ == "__main__":
    _cli()
