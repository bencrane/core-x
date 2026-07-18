"""Normalize staffing website-research payloads → ``staffing_market_inputs`` Lance sidecar.

Reads the verbatim mirror (``active/staffing_website_research/``) and emits one
row per landed payload with the four market-composition axes normalized
DETERMINISTICALLY (measured on the first 178: geo 94%, placement 100%,
clearance regex-sufficient, roles two-level):

  - geo:        is_national + states[] (state names/abbrevs + region map +
                metro gazetteer). Residual strings kept in geo_unresolved.
  - roles:      two-level. Occupation-level tokens → exact ``alias_norm`` hits
                against ``active/occupation_alias_lookup`` (singular fallback)
                → soc_codes[]; function-level tokens (practice areas, not
                titles) → soc_major_groups[] via the FUNCTION_MAP. Residual in
                roles_unresolved (the future LLM slot).
  - placement:  closed vocab {contract, direct_hire, temp, temp_to_perm,
                travel, w2, c1099}.
  - clearance:  has_clearance_language / has_vehicle_language /
                has_set_aside_language regex flags.

Provenance: every axis is deterministic in this version; the ``llm``
provenance value is reserved for the future residual pass. Source datasets are
read-only; output is a pure derived sidecar (overwrite snapshot):
``s3://data-sink/active/staffing_market_inputs/``, BTREE(uei).

Run:
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/normalize_staffing_research.py
"""
from __future__ import annotations

import json
import os
import re
import sys

import lance
import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

SRC_URI = "s3://data-sink/active/staffing_website_research/"
ALIAS_URI = "s3://data-sink/active/occupation_alias_lookup/"
TOKEN_MAP_URI = "s3://data-sink/active/staffing_role_token_map/"
DATASET_URI = "s3://data-sink/active/staffing_market_inputs/"

STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY", "district of columbia": "DC", "washington dc": "DC",
    "washington d.c": "DC", "puerto rico": "PR",
}
STATE_ABBRS = set(STATES.values())

# Census-style regions + common vernacular, expressed as state sets.
REGION_MAP = {
    "midwest": ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI"],
    "southeast": ["AL", "AR", "FL", "GA", "KY", "LA", "MS", "NC", "SC", "TN", "VA", "WV"],
    "northeast": ["CT", "ME", "MA", "NH", "NJ", "NY", "PA", "RI", "VT"],
    "southwest": ["AZ", "NM", "OK", "TX"],
    "west coast": ["CA", "OR", "WA"],
    "pacific northwest": ["OR", "WA"],
    "mid-atlantic": ["DE", "MD", "NJ", "NY", "PA", "VA", "DC"],
    "mid atlantic": ["DE", "MD", "NJ", "NY", "PA", "VA", "DC"],
    "new england": ["CT", "ME", "MA", "NH", "RI", "VT"],
    "gulf coast": ["AL", "FL", "LA", "MS", "TX"],
    "rocky mountain": ["CO", "ID", "MT", "UT", "WY"],
    "deep south": ["AL", "GA", "LA", "MS", "SC"],
}

# Metro / city → state. Head of the observed distribution; extend as residuals surface.
METRO_MAP = {
    "dallas-fort worth": "TX", "dfw": "TX", "dallas": "TX", "fort worth": "TX",
    "austin": "TX", "houston": "TX", "san antonio": "TX",
    "atlanta": "GA", "lilburn": "GA",
    "chicago": "IL", "oak brook": "IL", "schaumburg": "IL",
    "milwaukee": "WI", "minneapolis": "MN", "st. louis": "MO", "kansas city": "MO",
    "denver": "CO", "phoenix": "AZ", "las vegas": "NV", "salt lake city": "UT",
    "seattle": "WA", "portland": "OR",
    "los angeles": "CA", "san francisco": "CA", "san diego": "CA", "sacramento": "CA",
    "bay area": "CA", "silicon valley": "CA", "orange county": "CA",
    "new york city": "NY", "nyc": "NY", "long island": "NY",
    "boston": "MA", "philadelphia": "PA", "pittsburgh": "PA",
    "baltimore": "MD", "columbia md": "MD", "baltimore/washington area": "MD",
    "northern virginia": "VA", "nova": "VA", "richmond": "VA", "hampton roads": "VA",
    "charlotte": "NC", "raleigh": "NC", "research triangle": "NC", "nashville": "TN",
    "memphis": "TN", "detroit": "MI", "columbus": "OH", "cleveland": "OH",
    "cincinnati": "OH", "indianapolis": "IN", "louisville": "KY", "new orleans": "LA",
    "oklahoma city": "OK", "tulsa": "OK", "albuquerque": "NM", "boise": "ID",
    "miami": "FL", "tampa": "FL", "orlando": "FL", "jacksonville": "FL",
    "daytona": "FL", "daytona beach": "FL",
    "wichita": "KS", "omaha": "NE", "des moines": "IA",
}

NATIONAL_RE = re.compile(
    r"\b(usa|u\.s\.?a?\.?|united states|nationwide|national|all 50 states|"
    r"across the (us|u\.s\.|country)|coast to coast)\b", re.I)

# Function-level practice areas → SOC major group (2-digit). The vocabulary firms
# actually use when they describe departments rather than titles.
FUNCTION_MAP = {
    "engineering": "17", "engineers": "17", "technical": "17", "electrical": "17",
    "mechanical": "17", "civil": "17",
    "accounting": "13", "finance": "13", "financial": "13", "audit": "13",
    "accounting and finance": "13", "hr": "13", "human resources": "13",
    "marketing": "13", "sales": "41", "business development": "13",
    "it": "15", "information technology": "15", "it professionals": "15",
    "technology": "15", "software": "15", "cybersecurity": "15", "data": "15",
    "management": "11", "executive": "11", "executives": "11", "leadership": "11",
    "operations": "11", "project management": "11",
    "legal": "23", "attorneys": "23", "paralegal": "23",
    "healthcare": "29", "medical": "29", "clinical": "29", "nursing": "29",
    "allied health": "29", "behavioral health": "21",
    "scientific": "19", "science": "19", "laboratory": "19", "lab": "19",
    "administrative": "43", "administration": "43", "clerical": "43",
    "office support": "43", "office": "43", "call center": "43",
    "customer service": "43",
    "general labor": "53", "light industrial": "51", "industrial": "51",
    "manufacturing": "51", "production": "51", "warehouse": "53",
    "logistics": "53", "distribution": "53", "drivers": "53", "transportation": "53",
    "skilled trades": "47", "construction": "47", "trades": "47",
    "maintenance": "49", "facilities": "37", "janitorial": "37", "custodial": "37",
    "hospitality": "35", "food service": "35", "culinary": "35",
    "security": "33", "education": "25", "teachers": "25",
    "creative": "27", "design": "27",
}

PLACEMENT_MAP = {
    "contract": "contract", "contracts": "contract", "contract staffing": "contract",
    "direct-hire": "direct_hire", "direct hire": "direct_hire", "permanent": "direct_hire",
    "perm": "direct_hire", "direct placement": "direct_hire",
    "temp": "temp", "temporary": "temp", "locum tenens": "temp",
    "temp-to-perm": "temp_to_perm", "temp-to-hire": "temp_to_perm",
    "temp to perm": "temp_to_perm", "temp to hire": "temp_to_perm",
    "travel": "travel", "w-2": "w2", "w2": "w2", "1099": "c1099",
}

CLEAR_RE = re.compile(r"\b(ts/?sci|top secret|secret|public trust|cleared|clearance)\b", re.I)
VEHICLE_RE = re.compile(r"\b(gsa|schedule \d+|mas\b|oasis|sewp|seaport|stars|cio-sp)\b", re.I)
SET_ASIDE_RE = re.compile(
    r"\b(8\(a\)|wosb|edwosb|sdvosb|vosb|hubzone|wbe|mbe|sdb|"
    r"woman[- ]owned|veteran[- ]owned|minority[- ]owned)\b", re.I)

_EMPTYISH = {"", "none", "n/a", "unknown", "not specified", "not stated"}


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _split(v: str | None) -> list[str]:
    return [t.strip() for t in re.split(r"[,;/]| and ", v or "") if t.strip()]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s.lower())).strip()


def _singular(t: str) -> str:
    return t[:-1] if t.endswith("s") and not t.endswith("ss") else t


def norm_geo(raw: str | None) -> tuple[bool, list[str], list[str]]:
    """→ (is_national, states, unresolved_tokens)."""
    if not raw or raw.strip().lower() in _EMPTYISH:
        return False, [], []
    is_national = bool(NATIONAL_RE.search(raw))
    states: set[str] = set()
    unresolved: list[str] = []
    for tok in _split(raw):
        tl = tok.lower().strip(". ")
        tl_paren = re.sub(r"\(.*?\)", "", tl).strip()  # "georgia (lilburn)" → "georgia"
        if tl in STATES or tl_paren in STATES:
            states.add(STATES.get(tl) or STATES[tl_paren])
        elif len(tok.strip()) == 2 and tok.strip().upper() in STATE_ABBRS:
            states.add(tok.strip().upper())
        elif tl in REGION_MAP or tl_paren in REGION_MAP:
            states.update(REGION_MAP.get(tl) or REGION_MAP[tl_paren])
        elif tl in METRO_MAP or tl_paren in METRO_MAP:
            states.add(METRO_MAP.get(tl) or METRO_MAP[tl_paren])
        else:
            # "columbia md" / trailing-abbrev form
            m = re.search(r"\b([a-z]{2})$", tl)
            if m and m.group(1).upper() in STATE_ABBRS:
                states.add(m.group(1).upper())
            elif not NATIONAL_RE.search(tok):
                unresolved.append(tok)
    return is_national, sorted(states), unresolved


def norm_roles(
    raw: str | None,
    alias_to_soc: dict[str, set[str]],
    token_map: dict[str, dict],
) -> tuple[list[str], list[str], list[str], bool]:
    """→ (soc_codes, soc_major_groups, unresolved_tokens, used_llm_map).

    Resolution ladder per token: exact alias (SOC) → FUNCTION_MAP (major group)
    → LLM token map (grounded SOC codes and/or major; non-occupational tokens
    drop silently) → unresolved."""
    socs: set[str] = set()
    majors: set[str] = set()
    unresolved: list[str] = []
    used_llm = False
    for tok in _split(raw):
        n = _norm(tok)
        if not n or n in _EMPTYISH:
            continue
        hit = alias_to_soc.get(n) or alias_to_soc.get(_singular(n))
        if hit:
            socs.update(hit)
        elif n in FUNCTION_MAP:
            majors.add(FUNCTION_MAP[n])
        elif _singular(n) in FUNCTION_MAP:
            majors.add(FUNCTION_MAP[_singular(n)])
        elif n in token_map:
            m = token_map[n]
            used_llm = True
            if not m["occupational"]:
                continue  # classified non-role language — drop, not unresolved
            if m["soc_codes"]:
                socs.update(m["soc_codes"])
            if m["soc_major"]:
                majors.add(m["soc_major"])
            if not m["soc_codes"] and not m["soc_major"]:
                unresolved.append(tok)
        else:
            unresolved.append(tok)
    # occupations imply their major group too
    majors.update(s[:2] for s in socs)
    return sorted(socs), sorted(majors), unresolved, used_llm


def norm_placement(raw: str | None) -> list[str]:
    out = {PLACEMENT_MAP[t.strip().lower()]
           for t in (raw or "").split(",")
           if t.strip().lower() in PLACEMENT_MAP}
    return sorted(out)


def main() -> None:
    so = _r2_storage_options()
    src = lance.dataset(SRC_URI, storage_options=so)
    rows = src.to_table(columns=["record_id", "uei", "raw_payload", "landed_at"]).to_pylist()

    token_map: dict[str, dict] = {}
    try:
        tm = lance.dataset(TOKEN_MAP_URI, storage_options=so)
        token_map = {r["token_norm"]: r for r in tm.to_table().to_pylist()}
    except Exception:
        pass  # map not built yet — pure deterministic run

    alias_ds = lance.dataset(ALIAS_URI, storage_options=so)
    alias_to_soc: dict[str, set[str]] = {}
    for r in alias_ds.to_table(columns=["alias_norm", "code_type", "code"]).to_pylist():
        if r["code_type"] == "soc" and r["alias_norm"] and r["code"]:
            alias_to_soc.setdefault(r["alias_norm"], set()).add(r["code"])

    out = []
    for r in rows:
        p = json.loads(r["raw_payload"])
        is_national, states, geo_un = norm_geo(p.get("geographiesServed"))
        socs, majors, roles_un, used_llm = norm_roles(p.get("rolesPlaced"), alias_to_soc, token_map)
        cf = p.get("clearanceAndFederalIntent") or ""
        out.append({
            "record_id": r["record_id"],
            "uei": r["uei"],
            "is_national": is_national,
            "states": states,
            "geo_unresolved": geo_un,
            "soc_codes": socs,
            "soc_major_groups": majors,
            "roles_unresolved": roles_un,
            "placement_models": norm_placement(p.get("placementModel")),
            "has_clearance_language": bool(CLEAR_RE.search(cf)),
            "has_vehicle_language": bool(VEHICLE_RE.search(cf)),
            "has_set_aside_language": bool(SET_ASIDE_RE.search(cf)),
            "confidence": p.get("confidence"),
            "provenance": "deterministic_v1+llm_map_v1" if used_llm else "deterministic_v1",
            "landed_at": r["landed_at"],
        })

    schema = pa.schema([
        ("record_id", pa.string()), ("uei", pa.string()),
        ("is_national", pa.bool_()), ("states", pa.list_(pa.string())),
        ("geo_unresolved", pa.list_(pa.string())),
        ("soc_codes", pa.list_(pa.string())), ("soc_major_groups", pa.list_(pa.string())),
        ("roles_unresolved", pa.list_(pa.string())),
        ("placement_models", pa.list_(pa.string())),
        ("has_clearance_language", pa.bool_()), ("has_vehicle_language", pa.bool_()),
        ("has_set_aside_language", pa.bool_()),
        ("confidence", pa.string()), ("provenance", pa.string()),
        ("landed_at", pa.timestamp("us", tz="UTC")),
    ])
    tbl = pa.Table.from_pylist(out, schema=schema)
    ds = write_indexed_dataset(tbl, DATASET_URI, [("uei", "BTREE")], so)

    n = len(out)
    geo_ok = sum(1 for o in out if o["is_national"] or o["states"])
    roles_any = sum(1 for o in out if o["soc_codes"] or o["soc_major_groups"])
    roles_clean = sum(1 for o in out if not o["roles_unresolved"])
    print(f"published {DATASET_URI} rows={ds.count_rows():,}")
    print(f"geo resolved (national or >=1 state): {geo_ok}/{n}")
    print(f"roles: any SOC signal {roles_any}/{n} | fully resolved {roles_clean}/{n}")
    print(f"placement mapped: {sum(1 for o in out if o['placement_models'])}/{n}")


if __name__ == "__main__":
    main()
