"""Map query decoders for catalyst_api — the field/op/type allowlist that turns a
compiled filter object into a safe Lance scanner predicate.

One decoder per map serving table. This is the load-bearing security artifact: the
EXECUTE path (``lance_store.compile_map_filter``) rejects any field/op/value not
declared here, and column names come ONLY from ``FieldSpec.column`` (never from the
caller). The prompt-facing subset (field names, ops, enums, synonyms) is what
``edge_api`` renders into the ``emit_filter`` tool schema in TRANSLATE (Phase 3).

Bump a decoder's ``version`` on ANY change to its fields/enums/synonyms — it is the
cache-busting key for edge_api's translation memo.

Field types are verified against the live serving-table Arrow schemas: every string
column is ``string``; ``total_obligation``/``total_active_obligations`` are ``double``;
``award_count`` is ``int64``; ``founded_year`` is ``int32``; ``has_federal_awards`` and
``is_active`` are ``bool``.

The ``days_ago`` field type is the RELATIVE-TIME axis: the caller supplies a whole-day
count and EXECUTE resolves it against ``date.today()`` at request time, compiling to a
``DATE 'YYYY-MM-DD'`` literal over the underlying action-date column (date32 on company,
ISO string on winners — DataFusion coerces both, verified live). Resolving the date at
EXECUTE time — not at TRANSLATE time — keeps edge_api's translation memo safe: a cached
"this week" sentence re-resolves to the current week on every execution.
"""
from __future__ import annotations

from dataclasses import dataclass

# Global op enum (matches NL_QUERY_MAP_COMPILER_STRATEGY.md). A field's own `ops`
# tuple is a subset of this; the compiler rejects anything outside it.
OPS = ("=", ">=", "<=", "in", "between")


@dataclass(frozen=True)
class FieldSpec:
    column: str                       # hardcoded Lance column (NEVER from the caller)
    type: str                         # "string" | "int" | "float" | "bool"
    ops: tuple[str, ...]              # ops valid for THIS field (subset of OPS)
    enum: tuple | None = None         # allowed values; None = open-valued (still type-checked)
    index: str | None = None          # "BTREE" | "BITMAP" | None — observability only


@dataclass(frozen=True)
class Decoder:
    dataset_key: str                  # key into config.MAP_DATASET_URIS
    version: str                      # bump on any field/enum/synonym change
    geometry: tuple[str, str]         # (lon_col, lat_col)
    properties: tuple[str, ...]       # thin property set emitted per GeoJSON feature
    fields: dict[str, FieldSpec]      # query-name -> spec
    synonyms: dict[str, dict]         # NL term -> {"field","op","value"} (canned + prompt rows)


WINNERS = Decoder(
    dataset_key="winners",
    version="winners.v2",
    geometry=("longitude", "latitude"),
    properties=("winner_uei", "winner_name", "winner_type", "naics_code", "naics2",
                "state", "total_obligation", "award_count", "last_action_date"),
    fields={
        "naics2":           FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "state":            FieldSpec("state", "string", ("=", "in"), index="BITMAP"),
        "winner_type":      FieldSpec("winner_type", "string", ("=", "in"),
                                      enum=("prime_recipient", "subawardee"), index="BITMAP"),
        "naics_code":       FieldSpec("naics_code", "string", ("=", "in")),
        "total_obligation": FieldSpec("total_obligation", "float", (">=", "<=", "between")),
        "award_count":      FieldSpec("award_count", "int", (">=", "<=", "between")),
        # ISO-string action date; days_ago resolves to a DATE literal at request time.
        "days_since_last_award": FieldSpec("last_action_date", "days_ago", ("<=", ">=", "between")),
    },
    synonyms={
        "construction": {"field": "naics2", "op": "=", "value": "23"},
        "subawards":    {"field": "winner_type", "op": "=", "value": "subawardee"},
        "prime awards": {"field": "winner_type", "op": "=", "value": "prime_recipient"},
        "this week":    {"field": "days_since_last_award", "op": "<=", "value": 7},
        "won recently": {"field": "days_since_last_award", "op": "<=", "value": 30},
    },
)


COMPANY = Decoder(
    dataset_key="company",
    version="company.v3",
    geometry=("longitude", "latitude"),
    properties=("uei", "company_name", "industry", "employee_size_band", "company_type",
                "naics2", "primary_naics", "hq_city", "hq_state", "has_federal_awards",
                "total_active_obligations", "award_count", "latest_award_action_date"),
    fields={
        "naics2":             FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "industry":           FieldSpec("industry", "string", ("=", "in"), index="BITMAP"),
        "employee_size_band": FieldSpec("employee_size_band", "string", ("=", "in"),
                                        enum=("1-10", "11-50", "51-200", "201-500", "501-1000",
                                              "1001-5000", "5001-10000", "10001+"), index="BITMAP"),
        "company_type":       FieldSpec("company_type", "string", ("=", "in"),
                                        enum=("Educational", "Educational Institution", "Government Agency",
                                              "Nonprofit", "Partnership", "Privately Held", "Public Company",
                                              "Self-Employed", "Self-Owned", "Sole Proprietorship"), index="BITMAP"),
        # query-name `state` maps to the indexed column `physical_address_state`
        "state":              FieldSpec("physical_address_state", "string", ("=", "in"), index="BITMAP"),
        "has_federal_awards": FieldSpec("has_federal_awards", "bool", ("=",), index="BITMAP"),
        "is_active":          FieldSpec("is_active", "bool", ("=",)),
        "primary_naics":      FieldSpec("primary_naics", "string", ("=", "in"), index="BTREE"),
        "founded_year":       FieldSpec("founded_year", "int", (">=", "<=", "between")),
        "active_obligations": FieldSpec("total_active_obligations", "float", (">=", "<=", "between")),
        "award_count":        FieldSpec("award_count", "int", (">=", "<=", "between")),
        # date32 most-recent prime/subaward action date (materialize_company_map.py recency
        # join); days_ago resolves to a DATE literal at request time. BTREE serves the ranges.
        "days_since_last_award": FieldSpec("latest_award_action_date", "days_ago",
                                           ("<=", ">=", "between"), index="BTREE"),
    },
    synonyms={
        "construction":        {"field": "naics2", "op": "=", "value": "23"},
        "federal contractors": {"field": "has_federal_awards", "op": "=", "value": True},
        "active":              {"field": "is_active", "op": "=", "value": True},
        "this week":           {"field": "days_since_last_award", "op": "<=", "value": 7},
        "won recently":        {"field": "days_since_last_award", "op": "<=", "value": 30},
    },
)


DECODERS: dict[str, Decoder] = {"winners": WINNERS, "company": COMPANY}
