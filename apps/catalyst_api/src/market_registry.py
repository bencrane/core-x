"""Market query registry — the entity-grain field allowlist over the spine-derived L2
datasets (the replace-forward successor to the parked pre-spine serving marts).

REGISTRY AS DATA: one declarative structure, consumed three ways —
  1. the compiler (market_store.compile_market_filters) takes columns/types/ops/enums
     ONLY from here (never from the caller — the same security stance as map_decoders);
  2. the fields payload (fields_payload) projects it verbatim into the shape the query
     workbench already parses (name/type/ops/enum/index/gated), plus the description;
  3. the executor hydrates result rows from RESULT_COLUMNS per source table.

THE DESCRIPTIONS ARE THE PRODUCT. Every field states its GRAIN and its UNIVERSE
explicitly, per the semantics doctrine of the rollup build:

  • Money universe: CONTRACTS ONLY — USAspending prime awards with category='contract'
    OR a NULL category carrying a PIID; IDV vehicle parents are EXCLUDED (an IDV
    ceiling is not obligated money). Grants/loans/other assistance are out of universe.
  • Windows bind to action_date — "awarded in the trailing N months", never period of
    performance. There are NO "active contract" fields here (upstream PoP defect):
    do not fabricate them.
  • Sub-side short windows are FLOORS: FSRS subaward reporting lags, so a trailing
    24/60-month sub figure undercounts recent activity. Lifetime is the honest total.
  • Grain: one row per UEI (rollup + entities are both 1 row/uei; lane predicates
    collapse the (uei, side, code_type, code) lane grain back to a UEI set).

Schemas + indices probed live 2026-07-05 (never trusted from docs):
  gtm_entity_behavior_rollup   261,316 rows, BTREE uei
  gtm_entity_code_lanes      1,670,905 rows, BTREE uei+code, BITMAP side+code_type
  gtm_sam_entities           2,025,707 rows, BTREE uei/normalized_domain/primary_naics,
                             BITMAP in_sam/sam_is_active/in_dsbs/is_subawardee/
                             is_prime_recipient/physical_state

Bump REGISTRY_VERSION on ANY change to fields/enums/descriptions — it is the
decoderVersion the workbench caches against.
"""
from __future__ import annotations

from dataclasses import dataclass

# Version key surfaced as decoderVersion in the fields payload.
# v4: capability-inference axes — inferred_primeable_*/inferred_subbable_* pseudo-fields
# (semi-joins against the pre-weighting cooccurrence projections) + is_sub_only_lifetime
# (registry-level compiled expression over the rollup).
# v3: HQ geo sidecar (gtm_entity_geo) LEFT-joined at hydration — result rows carry
# latitude/longitude/geo_precision; the map adapter emits real Point geometry.
# v2: state → closed enum (live-probed USPS codes); `codes` typeahead attribute on the
# code-valued fields (naics/psc/agency); agency added to the /market/codes systems.
REGISTRY_VERSION = "entities.v4"

# The two scalar source tables (keys into market_store's URI map). Lane predicates are
# a third, non-scalar source compiled separately (see LANE below).
SOURCES = ("rollup", "entities")

# Ops grammar is the map compiler's (compile discipline reused verbatim).
OPS = ("=", ">=", "<=", "in", "between")

# Code systems the /api/v1/market/codes typeahead serves. A field carrying a `codes`
# attribute (MarketFieldSpec.codes / the lane pseudo-fields) names one of these — the
# workbench renders a typeahead for it, querying /market/codes?type=<system>.
CODE_SYSTEMS = ("naics", "psc", "agency")

# US state/territory/military USPS codes OBSERVED LIVE in gtm_sam_entities.physical_state
# (probed 2026-07-05: 2,774 raw distinct values — free-text foreign provinces included —
# filtered to 2-char uppercase ∩ the canonical USPS set; 61 remain, sorted). The state
# field is a CLOSED enum over these: an off-list value (a foreign province, a spelled-out
# state) is a 422, never a silent zero-row scan.
US_STATE_CODES = (
    "AA", "AE", "AK", "AL", "AP", "AR", "AS", "AZ", "CA", "CO", "CT", "DC", "DE",
    "FL", "FM", "GA", "GU", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
    "MD", "ME", "MH", "MI", "MN", "MO", "MP", "MS", "MT", "NC", "ND", "NE", "NH",
    "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
    "TX", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY",
)


@dataclass(frozen=True)
class MarketFieldSpec:
    """One entity-grain queryable axis. ``column`` is the hardcoded Lance column (NEVER
    from the caller); ``source`` names the table the predicate compiles against.
    Shape-compatible with lance_store._map_clause_sql (column/type/ops/enum)."""

    source: str                       # "rollup" | "entities"
    column: str                       # hardcoded Lance column
    type: str                         # "string" | "int" | "float" | "bool" | "days_ago"
    ops: tuple[str, ...]              # subset of OPS
    description: str                  # grain + universe, stated explicitly (the product)
    enum: tuple | None = None         # closed vocabulary; None = open-valued
    index: str | None = None          # "BTREE" | "BITMAP" | None — observability only
    gated: bool = False               # parity with the map FieldSpec shape (always False here)
    codes: str | None = None          # CODE_SYSTEMS member → the workbench renders a
                                      # /market/codes typeahead for this field; None =
                                      # not code-valued (payload OMITS the key entirely)
    expr: tuple[str, str] | None = None  # (sql_when_true, sql_when_false) — a registry-
                                      # level COMPILED boolean expression ('=' bool only;
                                      # column anchors the schema check). The two branches
                                      # are written out explicitly (no NOT()) so NULL
                                      # semantics stay exact.


# Shared doctrine strings — composed into every money/window description so the
# semantics can never drift apart field-to-field.
_PRIME_UNIVERSE = (
    "USD obligated to this entity as PRIME recipient on contract awards (task orders + "
    "definitive contracts; IDV vehicle parents excluded; contracts-only universe — "
    "category='contract' or NULL-category rows carrying a PIID; grants/assistance out of scope)"
)
_SUB_UNIVERSE = (
    "USD of FSRS-reported subaward dollars flowing to this entity as SUBAWARDEE under "
    "prime contract awards (same contracts-only universe as the prime fields)"
)
_WINDOW = "with action_date in the trailing {n} months (awarded-in-window; NOT period of performance)"
_SUB_FLOOR = (
    " Short-window sub figures are FLOORS: FSRS reporting lag undercounts recent months."
)
_GRAIN = " Grain: one row per UEI."


ENTITY_FIELDS: dict[str, MarketFieldSpec] = {
    # ── rollup: prime-side money (contracts-only, action_date windows) ─────────
    "prime_obl_24mo": MarketFieldSpec(
        "rollup", "prime_obl_24mo", "float", (">=", "<=", "between"),
        f"{_PRIME_UNIVERSE} {_WINDOW.format(n=24)}.{_GRAIN}"),
    "prime_obl_60mo": MarketFieldSpec(
        "rollup", "prime_obl_60mo", "float", (">=", "<=", "between"),
        f"{_PRIME_UNIVERSE} {_WINDOW.format(n=60)}.{_GRAIN}"),
    "prime_obl_lifetime": MarketFieldSpec(
        "rollup", "prime_obl_lifetime", "float", (">=", "<=", "between"),
        f"{_PRIME_UNIVERSE} over the full USAspending history loaded.{_GRAIN}"),
    "prime_award_ct_24mo": MarketFieldSpec(
        "rollup", "prime_award_ct_24mo", "int", (">=", "<=", "between"),
        "Count of DISTINCT prime contract awards (task orders + definitive contracts; IDV "
        f"parents excluded) with at least one action {_WINDOW.format(n=24)}.{_GRAIN}"),
    "prime_award_ct_lifetime": MarketFieldSpec(
        "rollup", "prime_award_ct_lifetime", "int", (">=", "<=", "between"),
        "Count of DISTINCT prime contract awards (task orders + definitive contracts; IDV "
        f"parents excluded) over the full history loaded.{_GRAIN}"),
    # ── rollup: sub-side money (FSRS; short windows are floors) ────────────────
    "sub_amt_24mo": MarketFieldSpec(
        "rollup", "sub_amt_24mo", "float", (">=", "<=", "between"),
        f"{_SUB_UNIVERSE} {_WINDOW.format(n=24)}.{_SUB_FLOOR}{_GRAIN}"),
    "sub_amt_60mo": MarketFieldSpec(
        "rollup", "sub_amt_60mo", "float", (">=", "<=", "between"),
        f"{_SUB_UNIVERSE} {_WINDOW.format(n=60)}.{_SUB_FLOOR}{_GRAIN}"),
    "sub_amt_lifetime": MarketFieldSpec(
        "rollup", "sub_amt_lifetime", "float", (">=", "<=", "between"),
        f"{_SUB_UNIVERSE} over the full FSRS history loaded (the honest sub total).{_GRAIN}"),
    # ── rollup: recency + breadth ──────────────────────────────────────────────
    "days_since_last_action": MarketFieldSpec(
        "rollup", "days_since_last_action", "int", (">=", "<=", "between"),
        "Whole days since the entity's most recent contract action on EITHER side (prime "
        "obligation or FSRS subaward), materialized at rollup build time (as_of). For a "
        f"request-time-resolved axis use last_action_date.{_GRAIN}"),
    "last_action_date": MarketFieldSpec(
        "rollup", "last_action_date", "days_ago", ("<=", ">=", "between"),
        "Relative-time axis over the most recent contract action date (either side). The "
        "value is a whole-day count resolved against today at REQUEST time: '<= 90' means "
        "acted within the last 90 days; '>= 365' means no action in over a year; "
        f"between [lo, hi] is a days-ago window.{_GRAIN}", index="BTREE"),
    "distinct_naics_ct": MarketFieldSpec(
        "rollup", "distinct_naics_ct", "int", (">=", "<=", "between"),
        "Count of distinct NAICS codes across the entity's contract lanes (both sides, "
        f"lifetime) — a codes-breadth signal.{_GRAIN}"),
    "distinct_agency_ct": MarketFieldSpec(
        "rollup", "distinct_agency_ct", "int", (">=", "<=", "between"),
        "Count of distinct awarding agencies across the entity's prime contract history "
        f"(lifetime) — an agency-diversification signal.{_GRAIN}"),
    "top_naics": MarketFieldSpec(
        "rollup", "top_naics", "string", ("=", "in"),
        "The entity's top NAICS code by lifetime contract dollars (one code; exact match). "
        f"For 'has ANY lane on code X' use the lane fields (prime_naics / sub_naics).{_GRAIN}",
        codes="naics"),
    "top_agency_code": MarketFieldSpec(
        "rollup", "top_agency_code", "string", ("=", "in"),
        "The entity's top awarding agency (toptier agency CODE, e.g. '097') by lifetime "
        f"prime contract dollars.{_GRAIN}",
        codes="agency"),
    # ── rollup: posture flags ──────────────────────────────────────────────────
    "is_prime_24mo": MarketFieldSpec(
        "rollup", "is_prime_24mo", "bool", ("=",),
        "True when the entity has ANY prime contract obligation with action_date in the "
        f"trailing 24 months.{_GRAIN}"),
    "is_sub_60mo": MarketFieldSpec(
        "rollup", "is_sub_60mo", "bool", ("=",),
        "True when the entity has ANY FSRS-reported subaward with action_date in the "
        f"trailing 60 months (a floor — FSRS lag).{_GRAIN}"),
    "prime_and_sub": MarketFieldSpec(
        "rollup", "prime_and_sub", "bool", ("=",),
        "True when the entity has BOTH lifetime prime contract obligations AND lifetime "
        f"FSRS subaward dollars (plays both sides).{_GRAIN}"),
    "is_sub_only_lifetime": MarketFieldSpec(
        "rollup", "sub_ct_lifetime", "bool", ("=",),
        "True when the entity has FSRS subaward history and ZERO prime contract awards, "
        "lifetime. Exact compiled predicate: (sub_ct_lifetime > 0 AND "
        "prime_award_ct_lifetime = 0); false compiles to (sub_ct_lifetime = 0 OR "
        f"prime_award_ct_lifetime > 0). A registry-level expression over the behavior "
        f"rollup — no rebuilt column.{_GRAIN}",
        expr=("(sub_ct_lifetime > 0 AND prime_award_ct_lifetime = 0)",
              "(sub_ct_lifetime = 0 OR prime_award_ct_lifetime > 0)")),
    # ── entities: SAM identity axes (universe: the full 2.03M-UEI SAM∪DSBS∪FSRS spine) ──
    "state": MarketFieldSpec(
        "entities", "physical_state", "string", ("=", "in"),
        "Physical address state (2-letter USPS code) from the SAM registration. CLOSED "
        "enum: the US state/territory/military codes observed live in the spine (the raw "
        "column also carries free-text foreign provinces — those are not selectable). "
        f"Universe: the full SAM∪DSBS∪FSRS entity spine (2.03M UEIs), not just award "
        f"winners.{_GRAIN}",
        enum=US_STATE_CODES, index="BITMAP"),
    "in_dsbs": MarketFieldSpec(
        "entities", "in_dsbs", "bool", ("=",),
        "True when the entity appears in SBA's Dynamic Small Business Search (DSBS) — the "
        f"small-business self-registration universe.{_GRAIN}", index="BITMAP"),
    "in_sam": MarketFieldSpec(
        "entities", "in_sam", "bool", ("=",),
        f"True when the entity appears in the SAM.gov entity extract.{_GRAIN}", index="BITMAP"),
    "sam_is_active": MarketFieldSpec(
        "entities", "sam_is_active", "bool", ("=",),
        "True when the SAM registration is currently ACTIVE (registration status; says "
        f"nothing about contract activity — use the rollup axes for that).{_GRAIN}",
        index="BITMAP"),
    "is_prime_recipient": MarketFieldSpec(
        "entities", "is_prime_recipient", "bool", ("=",),
        f"True when the entity appears as a prime recipient anywhere in the loaded "
        f"USAspending history (lifetime flag on the entity spine).{_GRAIN}", index="BITMAP"),
    "is_subawardee": MarketFieldSpec(
        "entities", "is_subawardee", "bool", ("=",),
        f"True when the entity appears as a subawardee anywhere in the loaded FSRS history "
        f"(lifetime flag on the entity spine).{_GRAIN}", index="BITMAP"),
    "normalized_domain": MarketFieldSpec(
        "entities", "normalized_domain", "string", ("=", "in"),
        "The entity's normalized web domain (lowercase, scheme/www stripped; DSBS-first "
        f"precedence). Exact match; empty/NULL when no domain is known.{_GRAIN}",
        index="BTREE"),
}


# ── Lane predicate (the (uei, side, code_type, code) lane grain → UEI set) ────
# A lane predicate selects entities BY THEIR CODE LANES: "has a prime NAICS lane on one
# of these codes [with at least $X obligated in the window]". It scans
# gtm_entity_code_lanes and collapses matching rows to a UEI set, which INTERSECTS the
# scalar predicates. Wire form (POST /api/v1/market/query filters):
#   {"lane": {"side": "prime"|"sub", "code_type": "naics"|"psc", "codes": ["541512", ...],
#             "min_obl_24mo": N, "min_obl_60mo": N, "min_obl_lifetime": N}}
# side + code_type + non-empty codes are REQUIRED; the min_obl_* thresholds are optional
# and apply to the LANE's obligation (that entity × side × code slice), not the entity
# total. Lane money semantics are the rollup's: contracts-only, IDV parents excluded,
# windows bind to action_date, sub-side short windows are floors.
LANE_SIDES = ("prime", "sub")
LANE_CODE_TYPES = ("naics", "psc")
# min_obl_* wire key → lanes-table column.
LANE_MIN_OBL_COLUMNS = {
    "min_obl_24mo": "obl_24mo",
    "min_obl_60mo": "obl_60mo",
    "min_obl_lifetime": "obl_lifetime",
}
LANE_REQUIRED_KEYS = ("side", "code_type", "codes")
LANE_ALLOWED_KEYS = frozenset(LANE_REQUIRED_KEYS) | frozenset(LANE_MIN_OBL_COLUMNS)

# Workbench-composable lane pseudo-fields: a scalar {field, op, value} clause on one of
# these desugars to a lane predicate (codes only, no threshold — thresholds need the
# explicit lane object). This is what makes lane cuts reachable from the existing
# workbench UI with zero UI changes.
LANE_PSEUDO_FIELDS: dict[str, tuple[str, str]] = {
    "prime_naics": ("prime", "naics"),
    "sub_naics": ("sub", "naics"),
    "prime_psc": ("prime", "psc"),
    "sub_psc": ("sub", "psc"),
}
_LANE_PSEUDO_DESC = (
    "Lane cut: the entity has a {side}-side {ct} lane on this EXACT code (a lane = that "
    "entity's contract activity on one (side, code) slice; contracts-only universe, IDV "
    "parents excluded). '=' one code; 'in' any of several. Dollar thresholds per lane "
    "(min_obl_24mo/60mo/lifetime) are available via the lane object on "
    "POST /api/v1/market/query. Code values are searchable via GET /api/v1/market/codes."
    " Grain: one row per UEI (lanes collapse to the entity)."
)
LANE_PSEUDO_DESCRIPTIONS = {
    name: _LANE_PSEUDO_DESC.format(side=side, ct=ct.upper())
    for name, (side, ct) in LANE_PSEUDO_FIELDS.items()
}


# ── Inferred-capability predicates (the capability-inference layer) ───────────
# VERB DOCTRINE (contractual): registered_* = SAM claims; primed_in_* = demonstrated as
# prime; subbed_under_* = the PRIME award's codes on subawards the firm delivered under
# — where the firm worked as a sub, NEVER a claim of what it can prime; inferred_* =
# cooccurrence evidence from both-sider firms, NOT a demonstration.
#
# An inferred predicate semi-joins one of the two pre-weighting projections built by
# scripts/build_gtm_capability_inference.py (grain (uei, code_type, code); a row exists
# only where the firm does NOT already demonstrate the code):
#   primeable → gtm_entity_inferred_primeable_codes  (evidence the firm could PRIME in
#               the code: both-sider firms sharing its subbed_under codes prime there)
#   subbable  → gtm_entity_inferred_subbable_codes   (the mirror: evidence the firm
#               could win SUB work under the code)
# Wire form ({"inferred": {...}} on POST /api/v1/market/query):
#   {"kind": "primeable"|"subbable", "code_type": "naics"|"psc", "codes": [...],
#    "min_supporting_bothsider_firm_ct": N?}
# The support floor is OPTIONAL and caller-supplied — never defaulted above 1
# (pre-weighting doctrine: the registry serves raw evidence; consumers weight at read).
# supporting_bothsider_firm_ct is the PAIR-SUM of cooccurring both-sider firm counts
# across the firm's matched codes (a both-sider supporting via k shared codes counts k
# times; the exact distinct-firm union is not materialized at this grain).
INFERRED_KINDS = ("primeable", "subbable")
INFERRED_MIN_SUPPORT_KEY = "min_supporting_bothsider_firm_ct"
INFERRED_REQUIRED_KEYS = ("kind", "code_type", "codes")
INFERRED_ALLOWED_KEYS = frozenset(INFERRED_REQUIRED_KEYS) | {INFERRED_MIN_SUPPORT_KEY}

# Workbench-composable pseudo-fields (codes only; the support floor needs the explicit
# inferred object). name → (kind, code_type).
INFERRED_PSEUDO_FIELDS: dict[str, tuple[str, str]] = {
    "inferred_primeable_naics": ("primeable", "naics"),
    "inferred_primeable_psc": ("primeable", "psc"),
    "inferred_subbable_naics": ("subbable", "naics"),
    "inferred_subbable_psc": ("subbable", "psc"),
}
_INFERRED_PSEUDO_DESC = {
    "primeable": (
        "INFERRED capability cut ({ct}): both-sider evidence suggests the entity could "
        "PRIME in this exact code — firms that demonstrably prime in it also worked as "
        "subs under the entity's subbed_under codes. subbed_under codes are the PRIME "
        "award's codes on subawards the firm delivered under (where it worked as a sub, "
        "never a claim of what it can prime); inferred is cooccurrence evidence, NOT a "
        "demonstration, and a row exists only where the entity does not already prime "
        "in the code. Support floor ({minkey}) available via the inferred object on "
        "POST /api/v1/market/query. Grain: one row per UEI."
    ),
    "subbable": (
        "INFERRED capability cut ({ct}), mirror direction: both-sider evidence suggests "
        "the entity could win SUB work under this exact code — firms that demonstrably "
        "subbed under it also prime in the entity's primed_in codes. inferred is "
        "cooccurrence evidence, NOT a demonstration, and a row exists only where the "
        "entity does not already sub under the code. Support floor ({minkey}) available "
        "via the inferred object on POST /api/v1/market/query. Grain: one row per UEI."
    ),
}
INFERRED_PSEUDO_DESCRIPTIONS = {
    name: _INFERRED_PSEUDO_DESC[kind].format(ct=ct.upper(),
                                             minkey=INFERRED_MIN_SUPPORT_KEY)
    for name, (kind, ct) in INFERRED_PSEUDO_FIELDS.items()
}


# ── Result row (hydration projection per source table, in wire column order) ──
# uei leads; entities identity columns next; rollup behavior columns after.
RESULT_COLUMNS_ENTITIES = ("uei", "legal_business_name", "physical_state",
                           "normalized_domain", "in_dsbs")
RESULT_COLUMNS_ROLLUP = ("uei", "prime_obl_24mo", "prime_obl_60mo", "prime_obl_lifetime",
                         "sub_amt_lifetime", "last_action_date", "top_naics",
                         "top_agency_code", "is_prime_24mo", "is_sub_60mo", "prime_and_sub")
# HQ geo sidecar (gtm_entity_geo, 1 row/uei, built by scripts/build_gtm_entity_geo.py).
# LEFT-joined at hydration: an entity absent from the sidecar carries NULL coordinates
# (the map adapter emits geometry:null — never a fake centroid). geo_precision is
# 'address' (geocode_xwalk rooftop) or 'county' (dominant-ZCTA county centroid).
RESULT_COLUMNS_GEO = ("uei", "latitude", "longitude", "geo_precision")
# The wire row = this exact key order. legal_business_name is display-only by design
# (v1: not filterable — name matching belongs to a resolution surface, not a filter).
RESULT_ROW_ORDER = (
    "uei", "legal_business_name", "physical_state", "normalized_domain", "in_dsbs",
    "prime_obl_24mo", "prime_obl_60mo", "prime_obl_lifetime", "sub_amt_lifetime",
    "last_action_date", "top_naics", "top_agency_code",
    "is_prime_24mo", "is_sub_60mo", "prime_and_sub",
    "latitude", "longitude", "geo_precision",
)


def fields_payload() -> dict:
    """The entities dataset entry for the fields payloads (/api/v1/market/fields and the
    'entities' key inside /api/v1/map/fields). Projected VERBATIM from the registry —
    never hand-maintained. Shape-compatible with the workbench catalog parser
    (name/type/ops/enum/index/gated), with description + source riding along and the
    lane contract published under 'lane'. A code-valued field carries ``"codes":
    "naics"|"psc"|"agency"`` (the workbench renders a /market/codes typeahead for it);
    the key is OMITTED ENTIRELY on non-code fields — presence IS the signal."""
    fields = []
    for qname, spec in ENTITY_FIELDS.items():
        f = {
            "name": qname,
            "type": spec.type,
            "ops": list(spec.ops),
            "enum": list(spec.enum) if spec.enum is not None else None,
            "index": spec.index,
            "gated": spec.gated,
            "source": spec.source,
            "description": spec.description,
        }
        if spec.codes is not None:
            f["codes"] = spec.codes
        fields.append(f)
    # Lane pseudo-fields: workbench-composable code cuts (string, = / in, open-valued).
    # Each carries its lane code system as `codes` (prime_naics/sub_naics → naics,
    # prime_psc/sub_psc → psc) so the workbench typeahead lights up on them too.
    fields.extend(
        {
            "name": name,
            "type": "string",
            "ops": ["=", "in"],
            "enum": None,
            "index": "BTREE",          # lanes.code carries the BTREE; side/code_type BITMAP
            "gated": False,
            "source": "lane",
            "description": LANE_PSEUDO_DESCRIPTIONS[name],
            "codes": code_type,
        }
        for name, (_side, code_type) in LANE_PSEUDO_FIELDS.items()
    )
    # Inferred-capability pseudo-fields: cooccurrence-evidence code cuts (semi-joins
    # against the pre-weighting projections; verb doctrine in each description).
    fields.extend(
        {
            "name": name,
            "type": "string",
            "ops": ["=", "in"],
            "enum": None,
            "index": "BTREE",          # projections carry BTREE uei + code
            "gated": False,
            "source": "inferred",
            "description": INFERRED_PSEUDO_DESCRIPTIONS[name],
            "codes": code_type,
        }
        for name, (_kind, code_type) in INFERRED_PSEUDO_FIELDS.items()
    )
    return {
        "decoderVersion": REGISTRY_VERSION,
        "grain": "entity",
        "legacy": False,
        "fields": fields,
        "aggregate": None,
        "lane": {
            "sides": list(LANE_SIDES),
            "codeTypes": list(LANE_CODE_TYPES),
            "requiredKeys": list(LANE_REQUIRED_KEYS),
            "minOblKeys": list(LANE_MIN_OBL_COLUMNS),
            "semantics": (
                "Selects entities by code lane: side + code_type + codes[] required; "
                "optional min_obl_24mo/min_obl_60mo/min_obl_lifetime apply to the lane's "
                "obligation (contracts-only; IDV parents excluded; action_date windows; "
                "sub-side short windows are floors)."
            ),
        },
        "inferred": {
            "kinds": list(INFERRED_KINDS),
            "codeTypes": list(LANE_CODE_TYPES),
            "requiredKeys": list(INFERRED_REQUIRED_KEYS),
            "minSupportKey": INFERRED_MIN_SUPPORT_KEY,
            "semantics": (
                "Selects entities by INFERRED capability (cooccurrence evidence from "
                "both-sider firms — never a demonstration; rows exist only where the "
                "entity does not already demonstrate the code). kind + code_type + "
                "codes[] required; optional min_supporting_bothsider_firm_ct is a "
                "caller-supplied support floor over the pair-sum evidence count "
                "(pre-weighting: nothing is thresholded for you)."
            ),
        },
        "resultColumns": list(RESULT_ROW_ORDER),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE GRAINS — single-table filter surfaces over the two FPDS canonicals.
# Unlike the entity grain (multi-table UEI-set intersection), a table grain compiles
# ONE predicate over ONE table and streams rows. Both tables are huge (82.9M / 108M
# rows), so require_filter=True: an empty filter list is a 422, never a full scan.
# Schemas + enum vocabularies probed live 2026-07-05 (never trusted from docs).
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class TableGrainSpec:
    """One single-table query grain. ``fields`` reuse MarketFieldSpec (their ``source``
    is the dataset_key, informational); ``result_columns`` is the wire row projection
    (geometry hydration appends latitude/longitude/geo_precision when ``geometry`` is
    set). ``require_filter`` guards the full-scan footgun on the 100M-row tables."""

    grain: str                        # wire grain value for POST /api/v1/market/query
    dataset_key: str                  # fields-payload key + /api/v1/map/{key}/query
    version: str                      # decoderVersion (bump on any field/enum change)
    source: str                       # market_store TABLE_GRAIN_URIS key
    description: str                  # grain + universe, stated explicitly
    fields: dict[str, MarketFieldSpec]
    result_columns: tuple[str, ...]
    require_filter: bool = True
    geometry: str | None = None       # "recipient_hq" (gtm_entity_geo join) | None


# ── prime_awards: one row per award, topology-aware ───────────────────────────
# award_topology IS the grain-honesty axis: FPDS mixes self-contained contracts,
# IDV vehicles, and the orders placed against them — a naive $ filter double-counts
# vehicles + their orders. Probed live: vehicle_order 64.81M | standalone 17.07M |
# vehicle 0.99M.
AWARD_TOPOLOGIES = ("standalone", "vehicle", "vehicle_order")
_PA_GRAIN = " Grain: one row per contract_award_unique_key (award, not action)."
_PA_UNIVERSE = ("Universe: the FPDS prime-award state table (82.87M awards — definitive "
                "contracts, purchase orders, IDV vehicles, and orders under them). ")

PRIME_AWARD_FIELDS: dict[str, MarketFieldSpec] = {
    "award_topology": MarketFieldSpec(
        "prime_awards", "award_topology", "string", ("=", "in"),
        "THE grain-honesty axis. 'standalone' = a self-contained definitive contract / "
        "purchase order (its own money, no parent vehicle). 'vehicle' = an IDV parent "
        "(IDIQ/GWAC/BPA/FSS/BOA) — a contracting CHANNEL: its own ledger is mostly "
        "ceiling, the real money rides on its orders (use rollup_obligated / "
        "rollup_order_count for the vehicle's aggregated order money). 'vehicle_order' "
        "= a task/delivery order placed AGAINST a vehicle (its own obligations; "
        "parent_award_id_piid names the vehicle). Filter this axis to avoid mixing "
        f"grains in $ comparisons.{_PA_UNIVERSE}{_PA_GRAIN}",
        enum=AWARD_TOPOLOGIES, index="BITMAP"),
    "recipient_uei": MarketFieldSpec(
        "prime_awards", "recipient_uei", "string", ("=", "in"),
        f"The awarded entity's SAM UEI (exact, 12-char). {_PA_UNIVERSE}{_PA_GRAIN}",
        index="BTREE"),
    "award_id_piid": MarketFieldSpec(
        "prime_awards", "award_id_piid", "string", ("=",),
        f"The award's PIID (procurement instrument identifier), exact match. "
        f"{_PA_UNIVERSE}{_PA_GRAIN}", index="BTREE"),
    "parent_award_id_piid": MarketFieldSpec(
        "prime_awards", "parent_award_id_piid", "string", ("=", "in"),
        "The parent VEHICLE's PIID (populated on vehicle_order rows) — 'every order "
        f"under this IDV'. {_PA_UNIVERSE}{_PA_GRAIN}"),
    "rollup_obligated": MarketFieldSpec(
        "prime_awards", "rollup_obligated", "float", (">=", "<=", "between"),
        "VEHICLE-ROLLUP money: Σ life-to-date obligated USD across the orders resolved "
        "to this award as their parent vehicle. Meaningful on award_topology='vehicle' "
        "rows (the vehicle's real throughput — its own ledger is mostly ceiling); 0 on "
        f"awards with no resolved child orders. {_PA_UNIVERSE}{_PA_GRAIN}"),
    "rollup_order_count": MarketFieldSpec(
        "prime_awards", "rollup_order_count", "int", (">=", "<=", "between"),
        "VEHICLE-ROLLUP order count: number of child orders resolved to this award as "
        f"their parent vehicle (topology='vehicle' semantics; 0 = no resolved orders). "
        f"{_PA_UNIVERSE}{_PA_GRAIN}"),
    "life_to_date_obligated": MarketFieldSpec(
        "prime_awards", "life_to_date_obligated", "float", (">=", "<=", "between"),
        "The award's OWN life-to-date obligated USD (Σ of its FPDS action obligations). "
        "On a vehicle row this is the parent's own ledger, NOT its orders — use "
        f"rollup_obligated for vehicle throughput. {_PA_UNIVERSE}{_PA_GRAIN}"),
    "current_total_value_of_award": MarketFieldSpec(
        "prime_awards", "current_total_value_of_award", "float", (">=", "<=", "between"),
        f"Current total value of the award (base + exercised options) as stated on the "
        f"latest action. {_PA_UNIVERSE}{_PA_GRAIN}"),
    "potential_ceiling": MarketFieldSpec(
        "prime_awards", "potential_ceiling", "float", (">=", "<=", "between"),
        "Potential ceiling USD (base and all options / potential total value; a "
        "reconciled fallback fills gaps per potential_ceiling_is_fallback upstream). "
        f"Ceiling ≠ money spent. {_PA_UNIVERSE}{_PA_GRAIN}"),
    "naics_code": MarketFieldSpec(
        "prime_awards", "naics_code", "string", ("=", "in"),
        f"The award's NAICS code (exact). {_PA_UNIVERSE}{_PA_GRAIN}", codes="naics"),
    "psc_code": MarketFieldSpec(
        "prime_awards", "product_or_service_code", "string", ("=", "in"),
        f"The award's Product/Service Code (exact). {_PA_UNIVERSE}{_PA_GRAIN}",
        codes="psc"),
    "awarding_agency_code": MarketFieldSpec(
        "prime_awards", "awarding_agency_code", "string", ("=", "in"),
        f"Awarding toptier agency CODE (e.g. '097'). {_PA_UNIVERSE}{_PA_GRAIN}",
        index="BITMAP", codes="agency"),
    "last_action_date": MarketFieldSpec(
        "prime_awards", "last_action_date", "days_ago", ("<=", ">=", "between"),
        "Relative-time axis over the award's most recent FPDS action date, resolved "
        "against today at REQUEST time ('<= 90' = acted within the last 90 days). "
        f"{_PA_UNIVERSE}{_PA_GRAIN}"),
}
PRIME_AWARD_RESULT_COLUMNS = (
    "contract_award_unique_key", "award_topology", "award_id_piid",
    "parent_award_id_piid", "recipient_uei", "recipient_name", "naics_code",
    "product_or_service_code", "awarding_agency_code", "life_to_date_obligated",
    "current_total_value_of_award", "potential_ceiling", "rollup_obligated",
    "rollup_order_count", "first_action_date", "last_action_date", "current_end_date",
)

PRIME_AWARDS = TableGrainSpec(
    grain="prime_award",
    dataset_key="prime_awards",
    version="prime_awards.v1",
    source="prime_awards",
    description=(
        "Award-grain FPDS query surface (one row per contract_award_unique_key, "
        "topology-aware). GEOMETRY NOTE: the state table carries NO place-of-performance "
        "columns (probed 2026-07-05) — map dots are the RECIPIENT'S HQ via the "
        "gtm_entity_geo sidecar (geo_precision 'address'|'county'), not the work site."
    ),
    fields=PRIME_AWARD_FIELDS,
    result_columns=PRIME_AWARD_RESULT_COLUMNS,
    require_filter=True,
    geometry="recipient_hq",
)


# ── transactions: one row per raw FPDS action ─────────────────────────────────
# Amounts are PER-ACTION DELTAS (a modification's change, possibly negative), never
# award totals — the award-total axis lives on the prime_awards grain.
_TX_GRAIN = " Grain: one row per contract_transaction_unique_key (raw FPDS action)."
_TX_UNIVERSE = ("Universe: the canonical FPDS transaction spine (107.96M actions, "
                "2008+ as reconciled). Amounts are PER-ACTION deltas, not award totals. ")

# Subcontracting-plan government definitions, resolved live from dec_code_domain_ref
# (db_element='subcontracting_plan', dec v31) — probed live codes A..H (H is DoD-only;
# ~83M actions carry no code at all and are not selectable).
SUBCONTRACTING_PLANS = ("A", "B", "C", "D", "E", "F", "G", "H")
_SUBK_DEFS = (
    "A = PLAN NOT INCLUDED - NO SUBCONTRACTING POSSIBILITIES (FAR 19.705-2I). "
    "B = PLAN NOT REQUIRED (below FAR 19.702(a) thresholds). "
    "C = PLAN REQUIRED - INCENTIVE NOT INCLUDED (FAR 19.702(a)/19.708(c); end-dated May 2015). "
    "D = PLAN REQUIRED - INCENTIVE INCLUDED (FAR 19.702(a)/19.708(c)/DFARS 219.708(c); end-dated May 2015). "
    "E = PLAN REQUIRED (PRE 2004). "
    "F = INDIVIDUAL SUBCONTRACT PLAN (contract-specific goals covering the entire period incl. options, FAR 19.701). "
    "G = COMMERCIAL SUBCONTRACT PLAN (company/division-wide commercial-items plan for the offeror's fiscal year, FAR 19.701). "
    "H = DOD COMPREHENSIVE SUBCONTRACT PLAN (plant/division/company-wide; DoD only, DFARS 219.702)."
)
# FPDS 'reason for modification' contract domain — the 21 observed codes match the
# dec_code_domain_ref ActionType contract vocabulary exactly. ~83M original-award rows
# carry an empty code (not a mod) and are not selectable on this axis.
ACTION_TYPE_CODES = ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M",
                     "N", "P", "R", "S", "T", "V", "W", "X", "Y")
_ACTION_TYPE_DEFS = (
    "A=ADDITIONAL WORK (new agreement, justification required), B=SUPPLEMENTAL "
    "AGREEMENT FOR WORK WITHIN SCOPE, C=FUNDING ONLY ACTION, D=CHANGE ORDER, "
    "E=TERMINATE FOR DEFAULT, F=TERMINATE FOR CONVENIENCE, G=EXERCISE AN OPTION, "
    "H=DEFINITIZE LETTER CONTRACT, J=NOVATION AGREEMENT, K=CLOSE OUT, L=DEFINITIZE "
    "CHANGE ORDER, M=OTHER ADMINISTRATIVE ACTION, N=LEGAL CONTRACT CANCELLATION, "
    "P=REREPRESENTATION OF NON-NOVATED MERGER/ACQUISITION, R=REREPRESENTATION, "
    "S=CHANGE PIID, T=TRANSFER ACTION, V=UEI/LEGAL BUSINESS NAME CHANGE (non-novation), "
    "W=ENTITY ADDRESS CHANGE, X=TERMINATE FOR CAUSE, Y=ADD SUBCONTRACT PLAN. "
    "NULL/empty = the original award action (not a modification) — not selectable."
)

TRANSACTION_FIELDS: dict[str, MarketFieldSpec] = {
    "subcontracting_plan": MarketFieldSpec(
        "transactions", "subcontracting_plan", "string", ("=", "in"),
        f"FPDS Subcontracting Plan code on the action. {_SUBK_DEFS} C/D/F/G flag a plan "
        "attached to the action (the SUBK trigger set adds H for DoD-wide plans). "
        f"{_TX_UNIVERSE}{_TX_GRAIN}",
        enum=SUBCONTRACTING_PLANS, index="BITMAP"),
    "action_type_code": MarketFieldSpec(
        "transactions", "action_type_code", "string", ("=", "in"),
        f"FPDS reason-for-modification code. {_ACTION_TYPE_DEFS} {_TX_UNIVERSE}{_TX_GRAIN}",
        enum=ACTION_TYPE_CODES),
    "action_date": MarketFieldSpec(
        "transactions", "action_date", "days_ago", ("<=", ">=", "between"),
        "Relative-time axis over the action's date, resolved against today at REQUEST "
        f"time ('<= 90' = actions in the trailing 90 days). {_TX_UNIVERSE}{_TX_GRAIN}",
        index="BTREE"),
    "federal_action_obligation": MarketFieldSpec(
        "transactions", "federal_action_obligation", "float", (">=", "<=", "between"),
        "USD obligated BY THIS ACTION alone (a delta: mods can be negative "
        f"de-obligations; never an award total). {_TX_UNIVERSE}{_TX_GRAIN}",
        index="BTREE"),
    "base_and_all_options_value": MarketFieldSpec(
        "transactions", "base_and_all_options_value", "float", (">=", "<=", "between"),
        "Base-and-all-options (potential ceiling) USD AS REPORTED ON THIS ACTION (a "
        f"per-action delta/statement, not the award's reconciled ceiling). "
        f"{_TX_UNIVERSE}{_TX_GRAIN}"),
    "recipient_uei": MarketFieldSpec(
        "transactions", "recipient_uei", "string", ("=", "in"),
        f"The awarded entity's SAM UEI (exact, 12-char). {_TX_UNIVERSE}{_TX_GRAIN}",
        index="BTREE"),
    "naics_code": MarketFieldSpec(
        "transactions", "naics_code", "string", ("=", "in"),
        f"The action's NAICS code (exact). {_TX_UNIVERSE}{_TX_GRAIN}",
        index="BTREE", codes="naics"),
    "psc_code": MarketFieldSpec(
        "transactions", "product_or_service_code", "string", ("=", "in"),
        f"The action's Product/Service Code (exact). {_TX_UNIVERSE}{_TX_GRAIN}",
        index="BTREE", codes="psc"),
    "awarding_agency_code": MarketFieldSpec(
        "transactions", "awarding_agency_code", "string", ("=", "in"),
        f"Awarding toptier agency CODE (e.g. '097'). {_TX_UNIVERSE}{_TX_GRAIN}",
        index="BITMAP", codes="agency"),
}
TRANSACTION_RESULT_COLUMNS = (
    "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid",
    "recipient_uei", "recipient_name", "action_date", "action_type_code",
    "action_type_description", "subcontracting_plan", "subcontracting_plan_desc",
    "federal_action_obligation", "base_and_all_options_value", "naics_code",
    "product_or_service_code", "awarding_agency_code", "awarding_agency_name",
)

TRANSACTIONS = TableGrainSpec(
    grain="transaction",
    dataset_key="transactions",
    version="transactions.v1",
    source="transactions",
    description=(
        "Raw FPDS action-grain query surface (one row per transaction; amounts are "
        "per-action deltas). Geometry is null in v1 — award dots live on prime_awards."
    ),
    fields=TRANSACTION_FIELDS,
    result_columns=TRANSACTION_RESULT_COLUMNS,
    require_filter=True,
    geometry=None,
)

# Wire-grain → spec dispatch (POST /api/v1/market/query accepts both the singular
# grain and the dataset_key plural; the map adapters use the dataset_key paths).
TABLE_GRAINS: dict[str, TableGrainSpec] = {
    PRIME_AWARDS.grain: PRIME_AWARDS,
    PRIME_AWARDS.dataset_key: PRIME_AWARDS,
    TRANSACTIONS.grain: TRANSACTIONS,
    TRANSACTIONS.dataset_key: TRANSACTIONS,
}


def table_fields_payload(spec: TableGrainSpec) -> dict:
    """Fields-payload entry for a table grain — same workbench-parsable shape as the
    entities entry (name/type/ops/enum/index/gated + codes-when-code-valued +
    description), plus the safety/geometry contract (requireFilter, geometry)."""
    fields = []
    for qname, fspec in spec.fields.items():
        f = {
            "name": qname,
            "type": fspec.type,
            "ops": list(fspec.ops),
            "enum": list(fspec.enum) if fspec.enum is not None else None,
            "index": fspec.index,
            "gated": fspec.gated,
            "source": fspec.source,
            "description": fspec.description,
        }
        if fspec.codes is not None:
            f["codes"] = fspec.codes
        fields.append(f)
    cols = list(spec.result_columns)
    if spec.geometry is not None:
        cols += ["latitude", "longitude", "geo_precision"]
    return {
        "decoderVersion": spec.version,
        "grain": spec.grain,
        "legacy": False,
        "fields": fields,
        "aggregate": None,
        "requireFilter": spec.require_filter,
        "geometry": spec.geometry,
        "description": spec.description,
        "resultColumns": cols,
    }
