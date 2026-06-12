"""Prompt-facing map decoders for edge_api — the field/op/enum/synonym subset the
TRANSLATE step renders into the ``emit_filter`` tool schema + cached system block.

Hand-mirrored from ``apps/catalyst_api/src/map_decoders.py`` (the AUTHORITATIVE
allowlist that EXECUTE enforces). The ``version`` MUST match catalyst's per dataset —
it is the cache-busting key for the translation memo, and a drift means the model is
prompted against a different allowlist than EXECUTE enforces (a hallucinated field
still gets rejected by EXECUTE, but the prompt should not invite it). The repo test
``tests/test_map_ask.py`` asserts the versions match.

This carries ONLY the prompt-facing data (field names, types, ops, enums, synonyms) —
never the Lance column names. Column resolution + the security allowlist live in
catalyst_api; this side only shapes the model's output space.
"""
from __future__ import annotations

OPS = ("=", ">=", "<=", "in", "between")

DECODERS: dict[str, dict] = {
    "winners": {
        "version": "winners.v2",
        "description": "Federal-contract WINNERS — one row per entity that won a prime contract or a subaward in the rolling window.",
        "fields": {
            "naics2":           {"type": "string", "ops": ("=", "in"), "desc": "2-digit NAICS sector ('23' = construction)"},
            "state":            {"type": "string", "ops": ("=", "in"), "desc": "2-letter US state of the winner"},
            "winner_type":      {"type": "string", "ops": ("=", "in"), "enum": ("prime_recipient", "subawardee")},
            "naics_code":       {"type": "string", "ops": ("=", "in"), "desc": "full NAICS code"},
            "total_obligation": {"type": "float",  "ops": (">=", "<=", "between"), "desc": "summed federal obligation, USD"},
            "award_count":      {"type": "int",    "ops": (">=", "<=", "between")},
            "days_since_last_award": {"type": "days_ago", "ops": ("<=", ">=", "between"),
                                      "desc": "whole days since the entity's most recent award action (integer; 0 = today). Time windows map here: 'won in the last N days' / 'past week' / 'this month' → days_since_last_award <= N"},
        },
        "synonyms": {
            "construction":  {"field": "naics2", "op": "=", "value": "23"},
            "subawardees":   {"field": "winner_type", "op": "=", "value": "subawardee"},
            "prime winners": {"field": "winner_type", "op": "=", "value": "prime_recipient"},
            "this week":     {"field": "days_since_last_award", "op": "<=", "value": 7},
            "won recently":  {"field": "days_since_last_award", "op": "<=", "value": 30},
        },
    },
    "company": {
        "version": "company.v3",
        "description": "Companies in the firmographics target universe that are SAM-registered — one row per company.",
        "fields": {
            "naics2":             {"type": "string", "ops": ("=", "in"), "desc": "2-digit NAICS sector ('23' = construction)"},
            "industry":           {"type": "string", "ops": ("=", "in"), "desc": "LinkedIn-style industry label"},
            "employee_size_band": {"type": "string", "ops": ("=", "in"), "enum": ("1-10", "11-50", "51-200", "201-500", "501-1000", "1001-5000", "5001-10000", "10001+"), "desc": "headcount band, e.g. '11-50', '51-200'"},
            "company_type":       {"type": "string", "ops": ("=", "in"), "enum": ("Educational", "Educational Institution", "Government Agency", "Nonprofit", "Partnership", "Privately Held", "Public Company", "Self-Employed", "Self-Owned", "Sole Proprietorship")},
            "state":              {"type": "string", "ops": ("=", "in"), "desc": "2-letter US state (physical address)"},
            "has_federal_awards": {"type": "bool",   "ops": ("=",), "desc": "true = the company holds federal awards"},
            "is_active":          {"type": "bool",   "ops": ("=",), "desc": "true = active SAM registration"},
            "primary_naics":      {"type": "string", "ops": ("=", "in")},
            "founded_year":       {"type": "int",    "ops": (">=", "<=", "between")},
            "active_obligations": {"type": "float",  "ops": (">=", "<=", "between"), "desc": "total active federal obligations, USD"},
            "award_count":        {"type": "int",    "ops": (">=", "<=", "between")},
            "days_since_last_award": {"type": "days_ago", "ops": ("<=", ">=", "between"),
                                      "desc": "whole days since the company's most recent federal award action (integer; 0 = today). Time windows map here: 'won in the last N days' / 'past week' / 'this month' → days_since_last_award <= N"},
        },
        "synonyms": {
            "construction":        {"field": "naics2", "op": "=", "value": "23"},
            "federal contractors": {"field": "has_federal_awards", "op": "=", "value": True},
            "active":              {"field": "is_active", "op": "=", "value": True},
            "this week":           {"field": "days_since_last_award", "op": "<=", "value": 7},
            "won recently":        {"field": "days_since_last_award", "op": "<=", "value": 30},
        },
    },
}


def render_decoder_prompt(dataset: str) -> str:
    """The cached system block: what the table is, the allowlisted fields with types +
    ops (+ enum/desc), the known phrase→filter rows, and the hard output rules."""
    d = DECODERS[dataset]
    lines = [
        f"You translate a natural-language map query into a constrained filter for: {d['description']}",
        "",
        "Allowed fields (use ONLY these; choose the field whose meaning matches the query):",
    ]
    for name, spec in d["fields"].items():
        bits = [f"- {name} ({spec['type']}) ops={list(spec['ops'])}"]
        if spec.get("enum"):
            bits.append(f"allowed values={list(spec['enum'])}")
        if spec.get("desc"):
            bits.append(f"— {spec['desc']}")
        lines.append(" ".join(bits))
    lines += ["", "Known phrase → filter mappings (apply when the phrase appears):"]
    for term, clause in d["synonyms"].items():
        lines.append(f'- "{term}" → {clause}')
    lines += [
        "",
        "Rules:",
        "- Emit ONLY via the emit_filter tool. Never prose.",
        "- Use ONLY the listed fields and their listed ops. For an enum field use only its allowed values.",
        "- Numeric value for >= and <=; [lo, hi] for between; an array for in; bare true/false for bool.",
        "- A days_ago field takes a whole-day INTEGER count, never a calendar date.",
        "- Combine multiple conditions as separate filter clauses (they are AND-combined).",
        "- NEVER silently drop part of the query. Any constraint you cannot express with the"
        " listed fields goes into the unmapped array as a short verbatim phrase from the query.",
        "- If the query implies no usable filter, return an empty filters array (and record"
        " whatever you could not map in unmapped).",
        "- If everything mapped, return an empty unmapped array.",
    ]
    return "\n".join(lines)


def build_emit_filter_tool(dataset: str) -> dict:
    """The forced tool: ``field`` + ``op`` are enum-bounded at the schema level (the
    first gate; EXECUTE's typed allowlist is the authoritative one)."""
    d = DECODERS[dataset]
    return {
        "name": "emit_filter",
        "description": "Translate the user's map query into a constrained filter object.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "a short human label for the query"},
                "filters": {
                    "type": "array",
                    "description": "AND-combined filter clauses",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string", "enum": list(d["fields"])},
                            "op": {"type": "string", "enum": list(OPS)},
                            "value": {"description": "scalar for =,>=,<=; array for in/between; true/false for bool; whole-day integer for days_ago"},
                        },
                        "required": ["field", "op", "value"],
                    },
                },
                # The honesty contract: a constraint the allowlist cannot express is SURFACED,
                # never silently dropped — the UI renders these as "not applied".
                "unmapped": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "verbatim phrases from the query that could NOT be mapped to any allowed field (empty when everything mapped)",
                },
            },
            "required": ["title", "filters", "unmapped"],
        },
    }
