"""SAM WD county geography → Census county FIPS crosswalk (sam_county_fips_crosswalk).

Gap-2 of the labor wiring plan: `sam_wd_county_coverage.county_code` is a
SAM-internal code, NOT a Census FIPS — a naive equality join to the spine's
`pop_county_fips` fabricates ~150 spurious county matches (~$83B of garbage
obligation). This module binds the ONLY clean SAM keys — `(state_code,
county_name)` — to true 5-digit Census FIPS via the Census
`national_county2020` gazetteer, deterministically.

Match ladder (all deterministic; ZERO LLM, zero fuzzy scoring):
  1. normalized equality — accent-fold, punctuation strip, SAINT→ST,
     `De Kalb`→DEKALB-style prefix collapse, legal-suffix strip on both sides
     (County/Parish/Borough/Census Area/Municipio/city/District/Island...),
     SAM state `CM` → Census `MP`.
  2. independent-city disambiguation — SAM marks independent cities with a
     trailing `*` (e.g. `Alexandria*`); on a city/county name collision
     (Baltimore, Richmond, Fairfax, St. Louis...) the star picks the
     `... city` gazetteer row, bare picks the county.
  3. explicit ALIAS table — the enumerated residual: truncations
     (`Fairbanks North`), typos (`Northwest Artic`, `Luqillo`), renames
     (`Wade Hampton`→Kusilvak), retired AK areas mapped to ALL successor
     FIPS (multi-row), and authority pins (Dade→Miami-Dade, GA
     Columbus→Muscogee, Washington D.C., Wake Island).
  4. non-county rows — `Statewide` / territory-wide coverage rows get
     resolution_status='statewide' with county_fips NULL (state-level bind
     only); known SAM wrong-state errors and retired-FIPS geographies are
     kept with explicit unresolved statuses, never guessed.

RECONCILIATION (fail-closed): every distinct (state_code, county_name) pair in
`sam_wd_county_coverage` must land in exactly one bucket; any pair that is
neither matched, aliased, statewide, nor on the pinned unresolved allowlist
aborts the run — new drift must be reviewed into the alias table, not absorbed.

Run:
    doppler run -p core-x -c prd -- \
      uv run --with pylance --with pyarrow --with requests --with 'psycopg[binary]' \
      python -m pipelines.sam_gov.sam_wd_county_fips --run
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import unicodedata

from pipelines.bls.ingest import (  # noqa: E402 — fleet R2/index plumbing, verbatim
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _storage_options,
)

BUCKET = "data-sink"
COVERAGE_URI = os.environ.get(
    "SAM_WD_COUNTY_LANCE_URI", f"s3://{BUCKET}/active/sam_wd_county_coverage/")
OUT_URI = os.environ.get(
    "SAM_COUNTY_FIPS_URI", f"s3://{BUCKET}/active/sam_county_fips_crosswalk/")
GAZETTEER_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
FEED = "sam_county_fips_crosswalk"
SOURCE = f"(state_code, county_name) x {GAZETTEER_URL} (deterministic, no LLM)"

_LEGAL_SFX = (r"\s+(COUNTY|PARISH|BOROUGH|CENSUS AREA|MUNICIPALITY|MUNICIPIO|"
              r"CITY AND BOROUGH|CITY|DISTRICT|ISLAND|ISLANDS)$")


def _fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def norm(s: str) -> str:
    s = _fold(s).strip().upper()
    s = re.sub(r"[.’'(),]", "", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^SAINT ", "ST ", s)
    s = re.sub(r"^(DE|DU|LA) (?=[A-Z])", r"\1", s)  # De Kalb / Du Page / La Salle
    return s


def strip_suffix(s: str) -> str:
    prev = None
    while prev != s:
        prev = s
        s = re.sub(_LEGAL_SFX, "", s)
    return s


# ── Explicit alias table (reviewed code, not runtime fuzz) ───────────────────
# (sam_state, normalized SAM name) → list of (county_fips, method)
ALIASES: dict[tuple[str, str], list[tuple[str, str]]] = {
    # AK truncations / typos (SAM field-width artifacts)
    ("AK", "FAIRBANKS NORTH"): [("02090", "alias_truncation")],        # Fairbanks North Star
    ("AK", "SOUTHEAST FAIRB"): [("02240", "alias_truncation")],        # Southeast Fairbanks
    ("AK", "NORTHWEST ARTIC"): [("02188", "alias_typo")],              # Northwest Arctic
    ("AK", "PENINSULA & LAKE"): [("02164", "alias_truncation")],       # Lake and Peninsula
    # AK renames / retired areas → ALL 2020 successors (multi-row)
    ("AK", "WADE HAMPTON"): [("02158", "alias_rename")],               # → Kusilvak (2015)
    ("AK", "VALDEZ-CORDOVA"): [("02063", "alias_successor"),           # → Chugach
                               ("02066", "alias_successor")],          # → Copper River
    ("AK", "WRANGELL-PETERSBURG"): [("02275", "alias_successor"),      # → Wrangell
                                    ("02195", "alias_successor")],     # → Petersburg
    ("AK", "SKAGWAY-YAKUTAT-ANGOON"): [("02230", "alias_successor"),   # → Skagway
                                       ("02282", "alias_successor"),   # → Yakutat
                                       ("02105", "alias_successor")],  # → Hoonah-Angoon
    ("AK", "PRINCE OF WALES-OUTER KETCHIKA"): [("02198", "alias_successor")],  # → POW-Hyder
    # Authority pins
    ("DC", "WASHINGTON DC"): [("11001", "alias_authority")],
    ("FL", "DADE"): [("12086", "alias_authority")],                    # → Miami-Dade
    ("GA", "COLUMBUS"): [("13215", "alias_authority")],                # → Muscogee (consolidated)
    ("6", "WAKE"): [("74450", "alias_authority")],                     # UM Wake Island
    ("MN", "LAKE OF THE WOO"): [("27077", "alias_truncation")],        # Lake of the Woods
    ("PR", "LUQILLO"): [("72089", "alias_typo")],                      # Luquillo
    # VA typos / retired independent cities → successor jurisdiction
    ("VA", "ALBERMARLE"): [("51003", "alias_typo")],                   # Albemarle
    ("VA", "COLONIAL HGHTS"): [("51570", "alias_truncation")],         # Colonial Heights city
    ("VA", "CLIFTON FORGE"): [("51005", "alias_successor")],           # → Alleghany (2001)
    ("VA", "SOUTH BOSTON"): [("51083", "alias_successor")],            # → Halifax (1995)
}

# Pairs that legitimately resolve to NO county FIPS. Anything unresolved and
# NOT on this list aborts the run.
UNRESOLVED_ALLOWLIST: dict[tuple[str, str], str] = {
    ("MD", "FAIRFAX"): "unresolved_wrong_state",          # VA city filed under MD
    ("MD", "FREDERICKSBURG"): "unresolved_wrong_state",   # VA city filed under MD
    ("MT", "YELLOWSTONE NATIONAL PARK"): "unresolved_retired",  # FIPS 30113, abolished 1997
    ("CM", "MARIANA"): "statewide",                       # territory-wide coverage
    ("CM", "NORTHERN MARIANAS"): "statewide",
}


def load_gazetteer(text: str) -> dict[tuple[str, str], list[tuple[str, str, str]]]:
    """(state, normalized-base-name) → [(fips, official_name, classfp)]."""
    out: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    lines = text.splitlines()
    if not lines or not lines[0].startswith("STATE|"):
        raise SystemExit("gazetteer header mismatch — refusing to parse")
    for ln in lines[1:]:
        if not ln.strip():
            continue
        st, sfp, cfp, _ns, cname, cls, _fn = ln.split("|")
        stx = "CM" if st == "MP" else st  # SAM uses CM for Northern Marianas
        out.setdefault((stx, strip_suffix(norm(cname))), []).append((sfp + cfp, cname, cls))
    return out


def resolve(state: str, county_name: str,
            gaz: dict) -> list[dict]:
    """One SAM (state, county_name) pair → >=1 crosswalk rows."""
    star = county_name.rstrip().endswith("*")
    base = county_name.rstrip().rstrip("*").strip()
    key = (state, strip_suffix(norm(base)))
    common = {"state_code": state, "sam_county_name": county_name,
              "sam_is_city_flag": star}
    if key[1] == "STATEWIDE" or UNRESOLVED_ALLOWLIST.get(key) == "statewide":
        return [{**common, "county_fips": None, "gazetteer_name": None,
                 "match_method": "non_county", "resolution_status": "statewide"}]
    if key in UNRESOLVED_ALLOWLIST:
        return [{**common, "county_fips": None, "gazetteer_name": None,
                 "match_method": "allowlist", "resolution_status": UNRESOLVED_ALLOWLIST[key]}]
    if key in ALIASES:
        return [{**common, "county_fips": fips, "gazetteer_name": None,
                 "match_method": method, "resolution_status": "matched"}
                for fips, method in ALIASES[key]]
    hits = gaz.get(key)
    if not hits:
        return [{**common, "county_fips": None, "gazetteer_name": None,
                 "match_method": "none", "resolution_status": "UNRESOLVED_NEW"}]
    if len(hits) > 1:
        cities = [h for h in hits if h[1].endswith("city")]
        counties = [h for h in hits if not h[1].endswith("city")]
        pick = cities if star else counties
        if len(pick) != 1:
            return [{**common, "county_fips": None, "gazetteer_name": None,
                     "match_method": "none", "resolution_status": "UNRESOLVED_NEW"}]
        hit = pick[0]
        return [{**common, "county_fips": hit[0], "gazetteer_name": hit[1],
                 "match_method": "city_county_disambiguated",
                 "resolution_status": "matched"}]
    hit = hits[0]
    return [{**common, "county_fips": hit[0], "gazetteer_name": hit[1],
             "match_method": "normalized_equality", "resolution_status": "matched"}]


def _schema():
    import pyarrow as pa
    return pa.schema([
        ("state_code", pa.string()), ("sam_county_name", pa.string()),
        ("sam_is_city_flag", pa.bool_()), ("county_fips", pa.string()),
        ("gazetteer_name", pa.string()), ("match_method", pa.string()),
        ("resolution_status", pa.string()), ("source", pa.string()),
        ("built_at", pa.string()),
    ])


def _record_run(stats: dict, dsn: str | None) -> None:
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(
                """
                INSERT INTO ops.sam_wage_determination_runs
                  (feed,status,active_total,wd_rows,county_rows,sca,dba,cba,
                   stateless_wds,dedup_dropped,api_calls,wd_uri,county_uri,
                   indexes_built,stats,error,started_at,completed_at)
                VALUES (%(feed)s,%(status)s,%(active_total)s,%(wd_rows)s,NULL,
                   NULL,NULL,NULL,NULL,NULL,NULL,%(wd_uri)s,NULL,
                   %(indexes_built)s,%(stats)s,%(error)s,%(started_at)s,%(completed_at)s)
                """,
                {**stats, "stats": json.dumps(stats.get("stats", {}))},
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def run(out_uri: str, coverage_uri: str, gazetteer_path: str | None) -> int:
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _storage_options()

    if gazetteer_path:
        gz_text = open(gazetteer_path, encoding="utf-8").read()
    else:
        import requests
        r = requests.get(GAZETTEER_URL, timeout=60)
        r.raise_for_status()
        r.encoding = "utf-8"  # file is UTF-8 (Añasco, Doña Ana); latin-1 mangles it
        gz_text = r.text
    gaz = load_gazetteer(gz_text)
    print(f"gazetteer: {sum(len(v) for v in gaz.values())} county rows", flush=True)

    cov = lance.dataset(coverage_uri, storage_options=so).to_table(
        columns=["state_code", "county_name"]).to_pylist()
    pairs = sorted({(c["state_code"], c["county_name"]) for c in cov
                    if c["state_code"] and c["county_name"]})
    print(f"coverage: {len(pairs)} distinct (state, county_name) pairs", flush=True)

    built_at = started.isoformat()
    rows: list[dict] = []
    for st, cn in pairs:
        for r in resolve(st, cn, gaz):
            rows.append({**r, "source": SOURCE, "built_at": built_at})

    new_unresolved = [r for r in rows if r["resolution_status"] == "UNRESOLVED_NEW"]
    if new_unresolved:
        for r in new_unresolved[:25]:
            print(f"  UNRESOLVED_NEW: {(r['state_code'], r['sam_county_name'])}", flush=True)
        raise SystemExit(
            f"FAIL-CLOSED: {len(new_unresolved)} pairs neither matched, aliased, "
            "statewide, nor allowlisted — review into ALIASES/UNRESOLVED_ALLOWLIST.")

    from collections import Counter
    by_status = Counter(r["resolution_status"] for r in rows)
    by_method = Counter(r["match_method"] for r in rows)
    print(f"rows {len(rows)} | status {dict(by_status)} | method {dict(by_method)}", flush=True)

    tbl = pa.Table.from_pylist(rows, schema=_schema())
    lance.write_dataset(tbl, out_uri, mode="overwrite", storage_options=so,
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE)
    built = _build_indexes(out_uri,
                           btree=["state_code", "sam_county_name", "county_fips"],
                           bitmap=["resolution_status", "match_method"], so=so)
    print(f"wrote {tbl.num_rows} rows → {out_uri} (indexes: {built})", flush=True)

    _record_run({
        "feed": FEED, "status": "ok", "active_total": len(pairs),
        "wd_rows": len(rows), "wd_uri": out_uri, "indexes_built": built,
        "error": None, "started_at": started,
        "completed_at": dt.datetime.now(dt.timezone.utc),
        "stats": {"by_status": dict(by_status), "by_method": dict(by_method)},
    }, os.environ.get("HQX_DB_URL_POOLED"))
    return 0


def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", action="store_true")
    p.add_argument("--coverage-uri", default=COVERAGE_URI)
    p.add_argument("--out-uri", default=OUT_URI)
    p.add_argument("--gazetteer", default=None,
                   help="local gazetteer path (default: fetch from census.gov)")
    a = p.parse_args()
    if not a.run:
        p.print_help()
        sys.exit(2)
    sys.exit(run(a.out_uri, a.coverage_uri, a.gazetteer))


if __name__ == "__main__":
    _cli()
