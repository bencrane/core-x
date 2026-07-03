"""L1 derived — olms_cba_crosswalk: a scored crosswalk resolving OLMS CBA employers to canonical
SAM UEIs (T1 exact + T2 fuzzy), plus a per-candidate sidecar for multi-registrant disambiguation.

TARGET   s3://data-sink/active/olms_cba_crosswalk/  (MAIN, 1 row per OLMS doc_id — every one of the
         4,844 distinct doc_ids present so the surface is a complete LEFT-joinable spine; uei/tier
         NULL for unmatched) and s3://data-sink/active/olms_cba_uei_candidates/ (SIDECAR, 1 row per
         (doc_id, candidate_uei) so a same-name/different-registrant pick is auditable).

         The deliverable concern is FALSE POSITIVES. Only the two high-precision tiers are kept —
         T1 EXACT and T2 FUZZY (geo-confirmed). The prior workflow's T3 (precision 0.667) and the
         SoS state-registry hop (0 incremental yield) are DROPPED. Three hardening guards below are
         mandatory and are the whole point of the tiering.

         GEO-GATE IS UEI-SCOPED, NOT NAME-SCOPED. When corroboration is required (guard-1 generic /
         initialism base, and EVERY T2 accept), the geo check is enforced against the *selected*
         registrant's own source_state — not the union of states across all same-name registrants.
         A name that geo-confirms on registrant-in-NY can never write registrant-in-AL: selection is
         restricted to the state-matched registrant subset, and if none survive the doc drops to
         unmatched. selected_uei_state is persisted on the MAIN so the invariant is auditable in-Lance.

SOURCES  (all s3://data-sink/active, read-only Lance scans — no landing fetch, no external HTTP):
   olms_cba_index            4,849 rows, but doc_id is reused across 5 records (doc_ids 7,8,9,10,14
                             carry an expired 2003-2008 CBA AND a distinct current 2024-2027 record):
                             4,844 DISTINCT doc_id. Because doc_id is the crosswalk grain, the 5
                             collisions are collapsed to the newer record (max cba_pub_id) -> 4,844
                             rows on the MAIN. emp_name (free-text, 100% non-null),
                             union_name, location (city/state blob, ~99% non-null — NO clean state
                             column; the trailing/leading 2-letter token yields state on 96.5% of
                             rows), exp_date, cba_pub_id.
   sam_normalized_entities   1,541,566 rows. THE resolution target. Join on the SAME normalizer
                             applied to normalized_legal_name (primary) OR legal_name_base
                             (secondary). Carries uei, is_active, source_state, primary_naics.
                             source_state (2-letter USPS) is the per-uei geo-gate state; ~9,700
                             non-2-letter values (foreign / full state names) never match parse_state
                             output and are fail-closed-safe (they simply cannot corroborate).
   sam_master_entities       1,541,566 rows. Not read in build() — sam_normalized_entities.source_state
                             already carries the registrant state used by the geo-gate, joined on the
                             same uei, so the master scan is avoided. (physical_address_province_or_state
                             is the master equivalent if source_state were ever absent.)
   usaspending_fpds_canonical_txn  108,181,354 rows. The distinct recipient_uei set (769,663 UEIs)
                             is the on_spine universe — a selected UEI that has ever transacted on the
                             FPDS spine flags on_spine=True and is preferred at selection tie-break.

NORMALIZER (apply IDENTICALLY to the OLMS emp_name and to both SAM name columns before matching):
   uppercase; un-invert comma-form government names ('MASSACHUSETTS, COMMONWEALTH OF (...)' ->
   'COMMONWEALTH OF MASSACHUSETTS ...'); drop parenthesized bargaining-unit / facility tags
   ('(LOUISVILLE FACILITY)', '(NATIONAL MASTER AGMT)'); '&' -> ' AND '; strip non-alphanumerics to
   spaces; strip a leading 'THE'; repeatedly strip trailing legal suffixes (INC, LLC, L L C, CO, CORP,
   CORPORATION, COMPANY, LP, LLP, LTD, INCORPORATED); collapse whitespace.

TIERS (T1 + T2 only):
   T1 EXACT  OLMS-normalized == a SAM normalized name (normalized_legal_name primary, legal_name_base
             secondary). A normalized name mapping to multiple registrants keeps ONE selected UEI on
             the main row (active-first, then on-spine-first, then uei asc — plus a ground-truth pin
             for hand-verified anchors) and emits the FULL candidate set to the sidecar. When the base
             needs corroboration, selection is restricted to registrants whose source_state == the
             OLMS-parsed state; if none survive, the doc drops to unmatched.
   T2 FUZZY  For an OLMS employer with NO exact SAM hit: block by first normalized token, rapidfuzz
             token_sort_ratio >= 95, token-count ratio >= 0.6, and geo-confirm AGAINST THE SELECTED
             registrant (OLMS-parsed state must equal the selected UEI's source_state). Selection is
             restricted to the state-matched registrant subset; if none survive, no T2 accept.

THREE MANDATORY HARDENING GUARDS:
   1. T1 common-word / initialism guard — REJECT an exact match whose normalized base is a 1-2 token
      generic word or a pure initialism (ABC, AGC, ...) UNLESS the SELECTED registrant's own state
      corroborates (source_state == OLMS state). Kills the 'ABC INC. (DISNEY)' -> 'ABC CORPORATION'
      class. Both 'ABC INC.' docs (443, 2172) resolve to tier='unmatched'.
   2. Geo-gate for state-anchorless names — UEI-scoped: the corroborator for guard 1 and a hard
      requirement for every T2 accept is that the SELECTED registrant's source_state equals the OLMS
      state. Closes cross-state collisions ('FARMINGTON' / same-name-different-state) at SELECTION,
      not merely at the name level.
   3. Multi-UEI disambiguation sidecar — the main row carries the single selected UEI; the sidecar
      emits every candidate UEI the normalized name maps to (is_active + is_selected + on_spine) so a
      same-name/different-registrant pick is fully auditable.

GRAIN / GATE (fail-closed BEFORE write):
   MAIN    1 row per olms doc_id == 4,844 distinct; every tier!='unmatched' row has a non-null uei;
           every corroboration-required T1 row and every T2 row has selected_uei_state == olms_state
           (UEI-scoped geo invariant — 0 cross-state FPs); the ABC-class FP is unmatched; matched-
           employer + on_spine counts inside the measured bands.
   SIDECAR 1 row per (doc_id, candidate_uei) — grain 1:1 on the composite key.

TABLES
   olms_cba_crosswalk        BTREE: [doc_id, uei]
     doc_id, cba_pub_id, emp_name, emp_name_normalized, olms_state, union_name, exp_date,
     uei (nullable), matched_legal_name (nullable, the SELECTED SAM registrant's normalized name),
     selected_uei_state (nullable, source_state of the selected UEI), tier ('T1'|'T2'|'unmatched'),
     score (double), candidate_uei_count (int32), on_spine (bool), is_active (bool, of selected UEI),
     geo_corroborated (bool), source, ingested_at.
   olms_cba_uei_candidates   BTREE: [doc_id, candidate_uei]
     doc_id, emp_name_normalized, candidate_uei, is_active, is_selected, on_spine.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      --with 'rapidfuzz>=3' python3 -m pipelines.reference.materialize_olms_cba_crosswalk build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      --with 'rapidfuzz>=3' python3 -m pipelines.reference.materialize_olms_cba_crosswalk verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
from collections import defaultdict

# Reuse the repo's R2 plumbing + Lance index builder by import — do NOT re-implement.
from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"

# Source Lance datasets (SoR) — env-overridable for staging.
OLMS_URI = os.environ.get("OLMS_CBA_INDEX_URI", f"{ACTIVE}/olms_cba_index")
SAM_NORM_URI = os.environ.get("SAM_NORMALIZED_ENTITIES_URI", f"{ACTIVE}/sam_normalized_entities")
SPINE_URI = os.environ.get("FPDS_CANONICAL_TXN_URI", f"{ACTIVE}/usaspending_fpds_canonical_txn")

# Output datasets.
MAIN_URI = os.environ.get("OLMS_CBA_CROSSWALK_URI", f"{ACTIVE}/olms_cba_crosswalk/")
SIDECAR_URI = os.environ.get("OLMS_CBA_UEI_CANDIDATES_URI", f"{ACTIVE}/olms_cba_uei_candidates/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance datasets -> pin current default storage version
SOURCE = "OLMS_CBA_CROSSWALK"

MAIN_BTREE = ["doc_id", "uei"]
SIDECAR_BTREE = ["doc_id", "candidate_uei"]
BITMAP: list[str] = []

# Matching thresholds.
FUZZ_MIN = 95          # rapidfuzz token_sort_ratio floor for T2
TOKCOUNT_MIN = 0.6     # token-count ratio guard for T2

# ── Publish bands — the D1 UEI-scoped geo-gate removes the cross-state FPs (4 T1 + 3 T2) that the
# prior name-scoped gate admitted, so matched / on-spine counts drop slightly and the guard-1 drop
# count rises (state-anchorless names that previously survived on the name-union now fail selection).
# Bands are widened on the low side to absorb the correction while still tripping on a normalizer /
# guard / block regression. doc grain is exact after collapsing 5 reused doc_ids -> 4,844 distinct.
EXPECTED_MAIN_ROWS = 4_844
MAIN_ROW_BAND = (4_844, 4_844)                 # complete LEFT-joinable surface, 1 row/doc_id — exact
DISTINCT_MATCHED_EMP_BAND = (760, 900)         # ~840 pre-fix; a handful of cross-state names drop
MATCHED_DOC_ROWS_BAND = (1_340, 1_620)         # ~1,478 pre-fix minus the 7 cross-state FPs + collateral
T1_DOC_ROWS_BAND = (1_280, 1_560)              # ~1,434 pre-fix minus corroboration-required cross-state
T2_DOC_ROWS_BAND = (10, 90)                    # ~44 pre-fix minus the 3 cross-state T2 FPs
ONSPINE_DOC_ROWS_BAND = (800, 1_120)           # ~954 pre-fix, drops with the removed FPs
GUARD1_DROP_BAND = (25, 140)                    # ~47 pre-fix; rises — name-union survivors now fail
STATE_PARSE_MIN = 0.94                          # measured 0.9654
CAND_UEI_EXPANSION_BAND = (2_000, 3_200)       # candidate UEIs over matched T1 names (sidecar breadth)

# ── Ground-truth pins — hand-verified in the prior resolution+audit workflow. A normalized name that
# maps to a large active registrant set (Raytheon -> 152 UEIs) has no data-derivable ordering that
# lands on the audited-true registrant; pin it so selection is deterministic and anchor-stable. The
# FULL candidate set is still emitted to the sidecar, so the pin is auditable. Mirrors the ANCHORS
# pattern in materialize_sca_wd_rates.py. Pins are honored ONLY when the pinned UEI is present in the
# (post-geo-filter, when corroboration is required) candidate set — a pin can never bypass the geo
# invariant. ───────────────────────────────────────────────────────────────────────────────────────
GROUND_TRUTH_UEI: dict[str, str] = {
    "RAYTHEON": "KLW3ZYL15493",              # 'RAYTHEON COMPANY (LOUISVILLE FACILITY)' -> Raytheon Company
    "UNITED PARCEL SERVICE": "YF8QFWJLNBV8", # 'UNITED PARCEL SERVICE (NATIONAL MASTER AGMT)' -> UPS Co
}

# Confirmed-TRUE anchors asserted in verify(): emp_name -> (uei, tier, on_spine).
ANCHORS: dict[str, tuple[str, str, bool]] = {
    "RAYTHEON COMPANY (LOUISVILLE FACILITY)": ("KLW3ZYL15493", "T1", True),
    "UNITED PARCEL SERVICE (NATIONAL MASTER AGMT)": ("YF8QFWJLNBV8", "T1", True),
}
# Guarded FALSE-POSITIVE: must NOT resolve to an ABC registrant. Its location is NATIONAL (no parsed
# state), so the UEI-scoped geo-gate denies the corroborator and the initialism guard drops it.
GUARDED_FP_NAME = "ABC INC. (DISNEY- WAS CAPITAL CITIES)"
ABC_CORP_UEI_FORBIDDEN = "ABC CORPORATION"   # verify() asserts matched_legal_name is not this

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


# ── normalizer ─────────────────────────────────────────────────────────────────────────────
_PAREN = re.compile(r"\([^)]*\)")
_SUFFIX = re.compile(r"\b(INCORPORATED|INC|LLC|L\s?L\s?C|CORPORATION|CORP|COMPANY|CO|LLP|LP|LTD)\b\.?")
_PUNCT = re.compile(r"[^A-Z0-9 ]+")
_WS = re.compile(r"\s+")
# comma-inverted government form: '<name>, STATE OF (...)' -> 'STATE OF <name> (...)'
_GOV_INV = re.compile(
    r"^(.*?),\s*(STATE OF|COMMONWEALTH OF|THE STATE OF|CITY OF|COUNTY OF|TOWN OF)\b(.*)$"
)


def normalize(name: str | None) -> str:
    """Canonical match key. Applied IDENTICALLY to OLMS emp_name and both SAM name columns."""
    if not name:
        return ""
    s = name.upper().strip()
    m = _GOV_INV.match(s)
    if m:                                    # un-invert comma-form gov name
        s = f"{m.group(2)} {m.group(1)} {m.group(3)}"
    s = _PAREN.sub(" ", s)                    # drop bargaining-unit / facility parentheticals
    s = s.replace("&", " AND ")
    s = _PUNCT.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    if s.startswith("THE "):
        s = s[4:]
    prev = None
    while prev != s:                          # strip stacked legal suffixes ('... CO INC')
        prev = s
        s = _SUFFIX.sub(" ", s)
        s = _WS.sub(" ", s).strip()
    return s


# ── OLMS state parse ───────────────────────────────────────────────────────────────────────
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY", "DC", "PR", "VI", "GU",
}


def parse_state(location: str | None) -> str | None:
    """Extract a 2-letter USPS state from the OLMS location blob. The state is usually the LEADING
    token ('KY', 'MO KANSAS CITY', 'IN ANDERSON'); fall back to the trailing token. 'NATIONAL' /
    unrecognized -> None (which correctly denies the geo corroborator)."""
    if not location:
        return None
    toks = location.upper().replace(",", " ").split()
    if not toks:
        return None
    if toks[0] in US_STATES:
        return toks[0]
    if toks[-1] in US_STATES:
        return toks[-1]
    return None


# ── guard 1: common-word / pure-initialism predicate ───────────────────────────────────────
# A 1-2 token generic word or a single pure initialism needs a corroborator (UEI-scoped geo) or the
# exact hit is dropped. RAYTHEON (distinctive, multi-vowel proper noun) does NOT trip this; ABC
# (pure initialism) does.
GENERIC_TOKENS = {
    "ABC", "AGC", "ABM", "AAA", "USA", "US", "NATIONAL", "GENERAL", "SERVICE", "SERVICES",
    "GROUP", "SYSTEMS", "ENTERPRISES", "ASSOCIATES", "HOLDINGS", "INDUSTRIES", "COMPANIES",
    "CORP", "AMERICA",
}


_VOWELS = set("AEIOU")


def _is_acronymish(tok: str) -> bool:
    """Acronym-ish = <=1 vowel (ABC/AGC/ABM). RAYTHEON (many vowels) is excluded here; single-word
    brands with a normal vowel profile are never mistaken for acronyms."""
    vowels = sum(1 for c in tok if c in _VOWELS)
    return vowels <= 1


def _is_initialism(tok: str) -> bool:
    """Pure initialism: a short all-alpha uppercase token (ABC, AGC) with an acronym-ish vowel
    profile. Kept tight so distinctive single-word brands (RAYTHEON, BOEING) are never caught."""
    return 2 <= len(tok) <= 4 and tok.isalpha() and tok == tok.upper() and _is_acronymish(tok)


def needs_corroboration(nm: str) -> bool:
    toks = nm.split()
    if not toks:
        return True
    if len(toks) <= 2:
        if any(t in GENERIC_TOKENS for t in toks):
            return True
        if len(toks) == 1 and _is_initialism(toks[0]):
            return True
    return False


def _tokcount_ratio(a: str, b: str) -> float:
    la, lb = len(a.split()), len(b.split())
    return (min(la, lb) / max(la, lb)) if max(la, lb) else 0.0


# ── source loaders ─────────────────────────────────────────────────────────────────────────
def _load_sam(so: dict):
    """Return (name2rows, block).
    name2rows[norm] = list[(uei, is_active, source_state, sam_norm_name)] — one entry per
      (uei, name-column) with the SELECTED registrant's normalized SAM name carried for audit;
    block[first_token] = [norm names] for T2 blocking."""
    import lance

    sam = lance.dataset(SAM_NORM_URI, storage_options=so)
    tbl = sam.scanner(
        columns=["uei", "normalized_legal_name", "legal_name_base", "is_active", "source_state"]
    ).to_table()
    ueis = tbl.column("uei").to_pylist()
    nln = tbl.column("normalized_legal_name").to_pylist()
    lnb = tbl.column("legal_name_base").to_pylist()
    act = tbl.column("is_active").to_pylist()
    sst = tbl.column("source_state").to_pylist()

    name2rows: dict[str, list[tuple[str, bool, str | None, str]]] = defaultdict(list)
    for u, a, b, ac, ss in zip(ueis, nln, lnb, act, sst):
        if not u:
            continue
        seen_for_uei: set[str] = set()
        for raw in (a, b):
            nm = normalize(raw)
            if not nm or nm in seen_for_uei:
                continue
            seen_for_uei.add(nm)
            # sam_norm_name == nm here (this IS the SAM registrant's normalized name for this uei).
            name2rows[nm].append((u, bool(ac), ss, nm))
    block: dict[str, list[str]] = defaultdict(list)
    for nm in name2rows:
        t = nm.split()
        if t:
            block[t[0]].append(nm)
    log(f"loaded SAM: {tbl.num_rows:,} registrant rows -> {len(name2rows):,} distinct normalized names")
    return name2rows, block


def _load_spine_ueis(so: dict) -> set:
    """Distinct recipient_uei across the 108M-row FPDS spine (the on_spine universe)."""
    import lance
    import pyarrow.compute as pc

    sp = lance.dataset(SPINE_URI, storage_options=so)
    out: set = set()
    for batch in sp.to_batches(columns=["recipient_uei"]):
        arr = batch.column("recipient_uei")
        arr = arr.filter(pc.is_valid(arr))
        if len(arr):
            out |= set(pc.unique(arr).to_pylist())
    log(f"loaded spine: {len(out):,} distinct recipient_uei (on_spine universe)")
    return out


# ── selection ──────────────────────────────────────────────────────────────────────────────
def _select_uei(
    norm_name: str,
    cand_rows: list[tuple[str, bool, str | None, str]],
    spine_ueis: set,
    geo_state: str | None,
):
    """Pick the main-row UEI and return the full candidate set for the sidecar.

    D1 UEI-SCOPED GEO: when geo_state is not None, selection is restricted to registrants whose OWN
    source_state == geo_state — the selected UEI is guaranteed state-matched, closing the cross-state
    decoupling. When geo_state is None the full candidate set is eligible (corroboration not required).

    A ground-truth pin is honored ONLY if it survives the (geo-filtered, when applicable) candidate
    set, so a pin can never bypass the geo invariant.

    Order: pin -> active-first -> on-spine-first -> uei asc.
    Returns (selected_uei, selected_is_active, selected_source_state,
             [(uei, is_active, on_spine)...] over the FULL unfiltered candidate set)."""
    # Full aggregate (unfiltered) for the sidecar — one row per candidate UEI.
    agg_all: dict[str, list] = {}                    # uei -> [any_active, on_spine, source_state]
    for u, a, ss, _nm in cand_rows:
        slot = agg_all.setdefault(u, [False, False, None])
        if a:
            slot[0] = True
        if u in spine_ueis:
            slot[1] = True
        if slot[2] is None and ss:
            slot[2] = ss
    if not agg_all:
        return None, False, None, []
    candidates = [(u, v[0], v[1]) for u, v in agg_all.items()]

    # Eligible set for MAIN selection: geo-filtered to the OLMS state when corroboration is required.
    if geo_state is not None:
        eligible = {u: v for u, v in agg_all.items()
                    if any(ss == geo_state for (uu, _a, ss, _n) in cand_rows if uu == u)}
    else:
        eligible = agg_all
    if not eligible:
        return None, False, None, candidates   # geo denied every candidate -> no MAIN pick

    pin = GROUND_TRUTH_UEI.get(norm_name)
    if pin and pin in eligible:
        sel = pin
    else:
        sel = sorted(eligible.items(), key=lambda kv: (not kv[1][0], not kv[1][1], kv[0]))[0][0]
    return sel, agg_all[sel][0], agg_all[sel][2], candidates


# ── resolution ─────────────────────────────────────────────────────────────────────────────
def _resolve(rows, name2rows, block, spine_ueis):
    """Resolve every OLMS doc row to (main record, sidecar records). rows is a list of dicts with
    doc_id, cba_pub_id, emp_name, union_name, exp_date, location."""
    main: list[dict] = []
    sidecar: list[dict] = []
    fuzz_cache: dict[str, tuple[str, float] | None] = {}

    from rapidfuzz import fuzz, process

    def fuzzy(nm: str):
        if nm in fuzz_cache:
            return fuzz_cache[nm]
        res = None
        t = nm.split()
        if t:
            cands = block.get(t[0])
            if cands:
                best = process.extractOne(nm, cands, scorer=fuzz.token_sort_ratio,
                                          score_cutoff=FUZZ_MIN)
                if best and _tokcount_ratio(nm, best[0]) >= TOKCOUNT_MIN:
                    res = (best[0], float(best[1]))
        fuzz_cache[nm] = res
        return res

    stats = dict(t1=0, t2=0, unmatched=0, guard1_drop=0, state_parsed=0,
                 t1_on=0, t2_on=0, geo_corroborated=0, matched_names=set(), cand_uei_names=set())

    for r in rows:
        doc_id = r["doc_id"]
        emp = r["emp_name"]
        nm = normalize(emp)
        ps = parse_state(r["location"])
        if ps:
            stats["state_parsed"] += 1

        tier = "unmatched"
        uei = None
        matched_legal = None
        selected_state = None
        score = 0.0
        is_active = None
        on_spine = False
        geo_corroborated = False
        cand_count = 0
        cand_rows_for_sidecar: list[tuple[str, bool, bool]] = []
        sidecar_norm = nm

        # ---- T1 exact ----------------------------------------------------------------------
        exact = name2rows.get(nm)
        if exact:
            corroborate = needs_corroboration(nm)
            # D1: when corroboration is required, geo_state is the OLMS state and selection is
            # restricted to state-matched registrants. ps=None here -> eligible is empty -> drop.
            geo_state = ps if corroborate else None
            if corroborate and ps is None:
                # Corroboration required (generic / initialism base) but the OLMS location yielded no
                # state -> cannot corroborate -> drop to unmatched. Never select an uncorroborated UEI
                # (passing geo_state=None to _select_uei would wrongly select freely, not drop).
                sel, sel_active, sel_state, cands = None, False, None, []
            else:
                sel, sel_active, sel_state, cands = _select_uei(nm, exact, spine_ueis, geo_state)
            if corroborate and sel is None:
                stats["guard1_drop"] += 1        # generic/initialism with no state-matched registrant
            elif sel:
                tier = "T1"
                uei = sel
                matched_legal = nm               # D4: SAM registrant's normalized name (T1 == OLMS key)
                selected_state = sel_state
                score = 100.0
                is_active = sel_active
                on_spine = sel in spine_ueis
                geo_corroborated = corroborate    # True only when the geo gate licensed this row
                cand_rows_for_sidecar = cands
                cand_count = len(cands)
                stats["matched_names"].add(nm)
                stats["cand_uei_names"].add(nm)
                if geo_corroborated:
                    stats["geo_corroborated"] += 1

        # ---- T2 fuzzy (only if exact missed / was guarded to unmatched) ---------------------
        if tier == "unmatched" and nm and ps is not None:
            fm = fuzzy(nm)
            if fm:
                cand_nm, sc = fm
                # D1/GUARD 2: geo is mandatory AND UEI-scoped for every T2 — selection restricted to
                # registrants whose source_state == ps; if none, no T2 accept.
                sel, sel_active, sel_state, cands = _select_uei(
                    cand_nm, name2rows[cand_nm], spine_ueis, ps)
                if sel:
                    tier = "T2"
                    uei = sel
                    matched_legal = cand_nm       # D4: SAM side (correct)
                    selected_state = sel_state
                    score = sc
                    is_active = sel_active
                    on_spine = sel in spine_ueis
                    geo_corroborated = True
                    cand_rows_for_sidecar = cands
                    cand_count = len(cands)
                    sidecar_norm = cand_nm
                    stats["matched_names"].add(nm)
                    stats["geo_corroborated"] += 1

        if tier == "T1":
            stats["t1"] += 1
            stats["t1_on"] += int(on_spine)
        elif tier == "T2":
            stats["t2"] += 1
            stats["t2_on"] += int(on_spine)
        else:
            stats["unmatched"] += 1

        main.append({
            "doc_id": doc_id,
            "cba_pub_id": r["cba_pub_id"],
            "emp_name": emp,
            "emp_name_normalized": nm or None,
            "olms_state": ps,
            "union_name": r["union_name"],
            "exp_date": r["exp_date"],
            "uei": uei,
            "matched_legal_name": matched_legal,
            "selected_uei_state": selected_state,
            "tier": tier,
            "score": float(score),
            "candidate_uei_count": int(cand_count),
            "on_spine": bool(on_spine),
            "is_active": is_active,
            "geo_corroborated": bool(geo_corroborated),
        })
        # ---- GUARD 3: full candidate set to the sidecar ------------------------------------
        for cu, ca, con in cand_rows_for_sidecar:
            sidecar.append({
                "doc_id": doc_id,
                "emp_name_normalized": sidecar_norm or None,
                "candidate_uei": cu,
                "is_active": bool(ca),
                "is_selected": cu == uei,
                "on_spine": bool(con),
            })
    return main, sidecar, stats


def build() -> dict:
    import lance
    import pyarrow as pa

    so = _storage_options()

    olms = lance.dataset(OLMS_URI, storage_options=so)
    ot = olms.scanner(
        columns=["doc_id", "cba_pub_id", "emp_name", "union_name", "exp_date", "location"]
    ).to_table()
    raw_rows = ot.to_pylist()
    n_raw = len(raw_rows)
    # doc_id is the crosswalk grain but the source reuses 5 doc_ids (an expired CBA + a distinct
    # current record share a key). Collapse to 1 row per doc_id, keeping the newer record
    # (max cba_pub_id) so the MAIN is a clean 1:1 surface.
    by_doc: dict[int, dict] = {}
    for r in raw_rows:
        d = r["doc_id"]
        prev = by_doc.get(d)
        if prev is None or (r.get("cba_pub_id") or -1) > (prev.get("cba_pub_id") or -1):
            by_doc[d] = r
    rows = list(by_doc.values())
    n_docs = len(rows)
    log(f"scanned {n_raw:,} OLMS rows from {OLMS_URI}; collapsed {n_raw - n_docs} reused doc_id(s) "
        f"-> {n_docs:,} distinct doc_id")

    name2rows, block = _load_sam(so)
    spine_ueis = _load_spine_ueis(so)

    main, sidecar, stats = _resolve(rows, name2rows, block, spine_ueis)
    now = dt.datetime.now(dt.timezone.utc)
    for m in main:
        m["source"] = SOURCE
        m["ingested_at"] = now

    distinct_matched = len(stats["matched_names"])
    matched_doc_rows = stats["t1"] + stats["t2"]
    onspine_doc_rows = stats["t1_on"] + stats["t2_on"]
    cand_uei_expansion = sum(len({r[0] for r in name2rows[n]}) for n in stats["cand_uei_names"])
    state_rate = stats["state_parsed"] / n_docs if n_docs else 0.0

    log(f"resolved: T1={stats['t1']:,} (on_spine={stats['t1_on']:,}) "
        f"T2={stats['t2']:,} (on_spine={stats['t2_on']:,}) unmatched={stats['unmatched']:,} | "
        f"guard1_drops={stats['guard1_drop']:,} geo_corroborated={stats['geo_corroborated']:,} "
        f"distinct_matched_emp={distinct_matched:,} cand_uei_expansion={cand_uei_expansion:,} "
        f"state_parse={state_rate:.4f}")

    # ── FAIL-CLOSED gate — structural grain + guard proof + measured bands ──────────────────
    main_ids = [m["doc_id"] for m in main]
    n_main = len(main)
    n_main_distinct = len(set(main_ids))
    n_null_tier_uei = sum(1 for m in main if m["tier"] != "unmatched" and not m["uei"])
    side_keys = [(s["doc_id"], s["candidate_uei"]) for s in sidecar]
    n_side_distinct = len(set(side_keys))

    # D1/D3 CLASS-LEVEL geo invariant: every corroboration-required T1 row and every T2 row must have
    # selected_uei_state == olms_state. This is the class-level FP-absent proof the mandate requires.
    geo_violations = [
        m for m in main
        if (m["tier"] == "T2" or (m["tier"] == "T1" and m["geo_corroborated"]))
        and m["selected_uei_state"] != m["olms_state"]
    ]
    n_geo_violations = len(geo_violations)

    # ABC-class guard proof (D2): every ABC-family doc must be unmatched, OR (post-D1) its selected
    # UEI's state equals the OLMS state — no dead tautology escape hatch.
    abc = next((m for m in main if m["emp_name"] == GUARDED_FP_NAME), None)
    abc_ok = abc is not None and (
        abc["tier"] == "unmatched"
        or (abc["matched_legal_name"] or "") != normalize(ABC_CORP_UEI_FORBIDDEN)
    )
    abc_family = [m for m in main if m["emp_name"] and m["emp_name"].upper().startswith("ABC INC")]
    abc_family_ok = all(
        m["tier"] == "unmatched" or m["selected_uei_state"] == m["olms_state"]
        for m in abc_family
    )

    def _band(v, lo, hi):
        return lo <= v <= hi

    if n_null_tier_uei:
        raise RuntimeError(f"GATE FAIL: {n_null_tier_uei} matched rows with NULL uei")
    if n_geo_violations:
        ex = geo_violations[0]
        raise RuntimeError(
            f"GATE FAIL: {n_geo_violations} cross-state FP(s) — selected_uei_state != olms_state on a "
            f"geo-required row (e.g. doc {ex['doc_id']} olms={ex['olms_state']} "
            f"selected={ex['selected_uei_state']} tier={ex['tier']})")
    if n_main != n_main_distinct:
        raise RuntimeError(
            f"GATE FAIL: MAIN grain not 1:1 — rows={n_main} != distinct(doc_id)={n_main_distinct}")
    if not _band(n_main, *MAIN_ROW_BAND):
        raise RuntimeError(f"GATE FAIL: MAIN rows {n_main} outside {MAIN_ROW_BAND}")
    if len(side_keys) != n_side_distinct:
        raise RuntimeError(
            f"GATE FAIL: SIDECAR grain not 1:1 — rows={len(side_keys)} != "
            f"distinct(doc_id,candidate_uei)={n_side_distinct}")
    if not abc_ok:
        raise RuntimeError(
            f"GATE FAIL: guarded FP {GUARDED_FP_NAME!r} resolved to "
            f"tier={abc['tier'] if abc else None} matched={abc['matched_legal_name'] if abc else None}")
    if not abc_family_ok:
        raise RuntimeError(
            "GATE FAIL: an 'ABC INC.' doc matched with selected_uei_state != olms_state")
    if not _band(distinct_matched, *DISTINCT_MATCHED_EMP_BAND):
        raise RuntimeError(
            f"GATE FAIL: distinct matched employers {distinct_matched} outside {DISTINCT_MATCHED_EMP_BAND}")
    if not _band(matched_doc_rows, *MATCHED_DOC_ROWS_BAND):
        raise RuntimeError(
            f"GATE FAIL: matched doc rows {matched_doc_rows} outside {MATCHED_DOC_ROWS_BAND}")
    if not _band(stats["t1"], *T1_DOC_ROWS_BAND):
        raise RuntimeError(f"GATE FAIL: T1 doc rows {stats['t1']} outside {T1_DOC_ROWS_BAND}")
    if not _band(stats["t2"], *T2_DOC_ROWS_BAND):
        raise RuntimeError(f"GATE FAIL: T2 doc rows {stats['t2']} outside {T2_DOC_ROWS_BAND}")
    if not _band(onspine_doc_rows, *ONSPINE_DOC_ROWS_BAND):
        raise RuntimeError(
            f"GATE FAIL: on_spine doc rows {onspine_doc_rows} outside {ONSPINE_DOC_ROWS_BAND}")
    if not _band(stats["guard1_drop"], *GUARD1_DROP_BAND):
        raise RuntimeError(
            f"GATE FAIL: guard1 drops {stats['guard1_drop']} outside {GUARD1_DROP_BAND}")
    if not _band(cand_uei_expansion, *CAND_UEI_EXPANSION_BAND):
        raise RuntimeError(
            f"GATE FAIL: candidate UEI expansion {cand_uei_expansion} outside {CAND_UEI_EXPANSION_BAND}")
    if state_rate < STATE_PARSE_MIN:
        raise RuntimeError(f"GATE FAIL: state-parse rate {state_rate:.4f} < {STATE_PARSE_MIN}")
    log(f"GATE PASS: main={n_main:,} sidecar={len(side_keys):,} matched_emp={distinct_matched:,} "
        f"matched_docs={matched_doc_rows:,} on_spine={onspine_doc_rows:,} "
        f"guard1_drops={stats['guard1_drop']:,} geo_violations=0 ABC_guarded=True")

    # ── typed Arrow tables (explicit schema so nullable columns are stable) ─────────────────
    main_schema = pa.schema([
        ("doc_id", pa.int32()),
        ("cba_pub_id", pa.int32()),
        ("emp_name", pa.string()),
        ("emp_name_normalized", pa.string()),
        ("olms_state", pa.string()),
        ("union_name", pa.string()),
        ("exp_date", pa.string()),
        ("uei", pa.string()),
        ("matched_legal_name", pa.string()),
        ("selected_uei_state", pa.string()),
        ("tier", pa.string()),
        ("score", pa.float64()),
        ("candidate_uei_count", pa.int32()),
        ("on_spine", pa.bool_()),
        ("is_active", pa.bool_()),
        ("geo_corroborated", pa.bool_()),
        ("source", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    side_schema = pa.schema([
        ("doc_id", pa.int32()),
        ("emp_name_normalized", pa.string()),
        ("candidate_uei", pa.string()),
        ("is_active", pa.bool_()),
        ("is_selected", pa.bool_()),
        ("on_spine", pa.bool_()),
    ])
    main_tbl = pa.Table.from_pylist(main, schema=main_schema)
    side_tbl = pa.Table.from_pylist(sidecar, schema=side_schema)

    lance.write_dataset(main_tbl, MAIN_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance MAIN -> {MAIN_URI} ({main_tbl.num_rows:,} rows)")
    built_main = _build_indexes(MAIN_URI, MAIN_BTREE, BITMAP, so)

    lance.write_dataset(side_tbl, SIDECAR_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance SIDECAR -> {SIDECAR_URI} ({side_tbl.num_rows:,} rows)")
    built_side = _build_indexes(SIDECAR_URI, SIDECAR_BTREE, BITMAP, so)

    # reopen + count assert
    m_written = lance.dataset(MAIN_URI, storage_options=so).count_rows()
    s_written = lance.dataset(SIDECAR_URI, storage_options=so).count_rows()
    if m_written != n_main:
        raise RuntimeError(f"POST-WRITE FAIL: MAIN lance {m_written} != assembled {n_main}")
    if s_written != len(sidecar):
        raise RuntimeError(f"POST-WRITE FAIL: SIDECAR lance {s_written} != assembled {len(sidecar)}")

    results = {
        "main_uri": MAIN_URI,
        "sidecar_uri": SIDECAR_URI,
        "olms_doc_rows": n_docs,
        "main_rows": m_written,
        "main_distinct_doc_id": n_main_distinct,
        "main_grain_1to1": m_written == n_main_distinct,
        "sidecar_rows": s_written,
        "sidecar_distinct_keys": n_side_distinct,
        "sidecar_grain_1to1": len(side_keys) == n_side_distinct,
        "t1_doc_rows": stats["t1"],
        "t1_on_spine": stats["t1_on"],
        "t2_doc_rows": stats["t2"],
        "t2_on_spine": stats["t2_on"],
        "unmatched_doc_rows": stats["unmatched"],
        "distinct_matched_employers": distinct_matched,
        "matched_doc_rows": matched_doc_rows,
        "on_spine_doc_rows": onspine_doc_rows,
        "guard1_common_word_drops": stats["guard1_drop"],
        "geo_corroborated_rows": stats["geo_corroborated"],
        "cross_state_geo_violations": n_geo_violations,
        "candidate_uei_expansion": cand_uei_expansion,
        "state_parse_rate": round(state_rate, 4),
        "abc_class_fp_guarded": abc_ok and abc_family_ok,
        "main_indexes": built_main,
        "sidecar_indexes": built_side,
        "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    main = lance.dataset(MAIN_URI, storage_options=so)
    side = lance.dataset(SIDECAR_URI, storage_options=so)

    m_rows = main.count_rows()
    s_rows = side.count_rows()

    mk = main.scanner(columns=["doc_id"]).to_table()
    m_distinct = len(set(mk.column("doc_id").to_pylist()))
    sk = side.scanner(columns=["doc_id", "candidate_uei"]).to_table()
    s_pairs = list(zip(sk.column("doc_id").to_pylist(), sk.column("candidate_uei").to_pylist()))
    s_distinct = len(set(s_pairs))

    # index fields (Lance names scalar indices <col>_idx; assert on declared `fields`).
    def _idx_fields(ds):
        fields = set()
        for i in ds.list_indices():
            fs = i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None)
            for f in (fs or []):
                fields.add(f)
        return fields

    m_idx = _idx_fields(main)
    s_idx = _idx_fields(side)

    # every matched row has a non-null uei
    matched_null_uei = main.count_rows(filter="tier != 'unmatched' AND uei IS NULL")
    t1 = main.count_rows(filter="tier = 'T1'")
    t2 = main.count_rows(filter="tier = 'T2'")
    on_spine = main.count_rows(filter="on_spine = true")

    # D3 CLASS-LEVEL FP-ABSENT CHECK — in-Lance, no SAM re-join. Every T2 row and every geo-
    # corroborated T1 row must have selected_uei_state == olms_state. Any row where they differ is a
    # cross-state false positive of exactly the class the mandate forbids.
    # Lance's filter planner does not support `IS DISTINCT FROM`; express the NULL-safe inequality
    # explicitly. On a geo-required row a NULL on either side is itself a violation (both must be a
    # present, equal state), so treat any NULL as distinct.
    cross_state_fps = main.count_rows(
        filter="(tier = 'T2' OR (tier = 'T1' AND geo_corroborated = true)) "
               "AND (selected_uei_state != olms_state "
               "OR selected_uei_state IS NULL OR olms_state IS NULL)")

    # anchors + guarded FP
    anchor_names = list(ANCHORS) + [GUARDED_FP_NAME, "ABC INC."]
    lit = ", ".join("'" + n.replace("'", "''") + "'" for n in anchor_names)
    spot = main.scanner(
        columns=["emp_name", "uei", "matched_legal_name", "selected_uei_state", "olms_state",
                 "tier", "on_spine", "geo_corroborated"],
        filter=f"emp_name IN ({lit})",
    ).to_table().to_pylist()
    by_name = {r["emp_name"]: r for r in spot}

    anchor_report = {}
    for name, exp in ANCHORS.items():
        g = by_name.get(name)
        anchor_report[name] = {
            "expected": {"uei": exp[0], "tier": exp[1], "on_spine": exp[2]},
            "got": {"uei": g["uei"], "tier": g["tier"], "on_spine": g["on_spine"]} if g else None,
        }
    abc = by_name.get(GUARDED_FP_NAME)
    abc_report = {
        "emp_name": GUARDED_FP_NAME,
        "tier": abc["tier"] if abc else None,
        "uei": abc["uei"] if abc else None,
        "matched_legal_name": abc["matched_legal_name"] if abc else None,
    }

    out = {
        "main_uri": MAIN_URI,
        "sidecar_uri": SIDECAR_URI,
        "main_rows": m_rows,
        "main_distinct_doc_id": m_distinct,
        "main_grain_1to1": m_rows == m_distinct,
        "sidecar_rows": s_rows,
        "sidecar_distinct_keys": s_distinct,
        "sidecar_grain_1to1": len(s_pairs) == s_distinct,
        "main_index_fields": sorted(m_idx),
        "sidecar_index_fields": sorted(s_idx),
        "t1_rows": t1,
        "t2_rows": t2,
        "on_spine_rows": on_spine,
        "matched_rows_with_null_uei": matched_null_uei,
        "cross_state_geo_fps": cross_state_fps,
        "anchors": anchor_report,
        "guarded_fp": abc_report,
        "main_columns": main.schema.names,
        "sidecar_columns": side.schema.names,
    }
    print(json.dumps(out, indent=2, default=str))

    # ── FAIL-LOUD verify ────────────────────────────────────────────────────────────────────
    errors: list[str] = []
    if m_rows != m_distinct:
        errors.append(f"MAIN grain broken: rows={m_rows} != distinct(doc_id)={m_distinct}")
    if not (MAIN_ROW_BAND[0] <= m_rows <= MAIN_ROW_BAND[1]):
        errors.append(f"MAIN rows {m_rows} outside {MAIN_ROW_BAND}")
    if len(s_pairs) != s_distinct:
        errors.append(f"SIDECAR grain broken: rows={len(s_pairs)} != distinct(doc_id,uei)={s_distinct}")
    if matched_null_uei:
        errors.append(f"{matched_null_uei} matched rows have NULL uei")
    # D3: class-level cross-state FP-absent invariant.
    if cross_state_fps:
        errors.append(
            f"{cross_state_fps} cross-state FP(s): selected_uei_state != olms_state on a geo-required row")
    for col in MAIN_BTREE:
        if col not in m_idx:
            errors.append(f"MAIN BTREE {col} absent — index_fields={sorted(m_idx)}")
    for col in SIDECAR_BTREE:
        if col not in s_idx:
            errors.append(f"SIDECAR BTREE {col} absent — index_fields={sorted(s_idx)}")
    for name, exp in ANCHORS.items():
        g = by_name.get(name)
        if g is None:
            errors.append(f"anchor {name!r} missing from MAIN")
            continue
        if (g["uei"], g["tier"], g["on_spine"]) != exp:
            errors.append(
                f"anchor {name!r} expected {exp} got "
                f"({g['uei']!r}, {g['tier']!r}, {g['on_spine']})")
    # guarded FP: must NOT resolve to an ABC registrant (unmatched here — location NATIONAL, no state).
    if abc is not None:
        if abc["tier"] != "unmatched" and (abc["matched_legal_name"] or "") == normalize(ABC_CORP_UEI_FORBIDDEN):
            errors.append(f"guarded FP {GUARDED_FP_NAME!r} resolved to ABC CORPORATION ({abc['uei']})")
        if abc["tier"] != "unmatched" and abc["selected_uei_state"] != abc["olms_state"]:
            errors.append(f"guarded FP {GUARDED_FP_NAME!r} is a cross-state FP ({abc['uei']})")
    if errors:
        raise RuntimeError("VERIFY FAIL: " + " | ".join(errors))
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
