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
REGISTRY_VERSION = "entities.v1"

# The two scalar source tables (keys into market_store's URI map). Lane predicates are
# a third, non-scalar source compiled separately (see LANE below).
SOURCES = ("rollup", "entities")

# Ops grammar is the map compiler's (compile discipline reused verbatim).
OPS = ("=", ">=", "<=", "in", "between")


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
        f"For 'has ANY lane on code X' use the lane fields (prime_naics / sub_naics).{_GRAIN}"),
    "top_agency_code": MarketFieldSpec(
        "rollup", "top_agency_code", "string", ("=", "in"),
        "The entity's top awarding agency (toptier agency CODE, e.g. '097') by lifetime "
        f"prime contract dollars.{_GRAIN}"),
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
    # ── entities: SAM identity axes (universe: the full 2.03M-UEI SAM∪DSBS∪FSRS spine) ──
    "state": MarketFieldSpec(
        "entities", "physical_state", "string", ("=", "in"),
        "Physical address state (2-letter USPS code) from the SAM registration. Universe: "
        f"the full SAM∪DSBS∪FSRS entity spine (2.03M UEIs), not just award winners.{_GRAIN}",
        index="BITMAP"),
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


# ── Result row (hydration projection per source table, in wire column order) ──
# uei leads; entities identity columns next; rollup behavior columns after.
RESULT_COLUMNS_ENTITIES = ("uei", "legal_business_name", "physical_state",
                           "normalized_domain", "in_dsbs")
RESULT_COLUMNS_ROLLUP = ("uei", "prime_obl_24mo", "prime_obl_60mo", "prime_obl_lifetime",
                         "sub_amt_lifetime", "last_action_date", "top_naics",
                         "top_agency_code", "is_prime_24mo", "is_sub_60mo", "prime_and_sub")
# The wire row = this exact key order. legal_business_name is display-only by design
# (v1: not filterable — name matching belongs to a resolution surface, not a filter).
RESULT_ROW_ORDER = (
    "uei", "legal_business_name", "physical_state", "normalized_domain", "in_dsbs",
    "prime_obl_24mo", "prime_obl_60mo", "prime_obl_lifetime", "sub_amt_lifetime",
    "last_action_date", "top_naics", "top_agency_code",
    "is_prime_24mo", "is_sub_60mo", "prime_and_sub",
)


def fields_payload() -> dict:
    """The entities dataset entry for the fields payloads (/api/v1/market/fields and the
    'entities' key inside /api/v1/map/fields). Projected VERBATIM from the registry —
    never hand-maintained. Shape-compatible with the workbench catalog parser
    (name/type/ops/enum/index/gated), with description + source riding along and the
    lane contract published under 'lane'."""
    fields = [
        {
            "name": qname,
            "type": spec.type,
            "ops": list(spec.ops),
            "enum": list(spec.enum) if spec.enum is not None else None,
            "index": spec.index,
            "gated": spec.gated,
            "source": spec.source,
            "description": spec.description,
        }
        for qname, spec in ENTITY_FIELDS.items()
    ]
    # Lane pseudo-fields: workbench-composable code cuts (string, = / in, open-valued).
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
        }
        for name in LANE_PSEUDO_FIELDS
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
        "resultColumns": list(RESULT_ROW_ORDER),
    }
