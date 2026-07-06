"""Deterministic phrase compiler — phrase.v1 (POST /api/v1/market/phrase).

A CLOSED GRAMMAR over CLOSED VOCABULARIES, zero LLM, zero fuzz: the phrase is
lexed, every token span is longest-matched against the vocabulary tables below,
each span binds to exactly one (axis, op, value) — and any token that binds to
nothing REFUSES the whole phrase naming the token (``MapCompileError`` → 422).
Determinism is the product: same phrase + same vocabulary + same ``today`` →
same plan, always. Plan + acceptance fixtures:
~/Desktop/hq/plans/2026-07-06-phrase-compiler-plan.md (+ the examples CSV — every
CSV row is pinned verbatim in tests/test_phrase_compiler.py).

PIPELINE: lex → match/bind → grain-resolve → emit → execute → disclose.
The emitted plan steps are EXACTLY ``POST /api/v1/market/query`` bodies — this
layer adds no query capability, only compilation. Cross-grain phrases
("companies that <event>…") emit the Cycle-1 pipeline: a table-grain step with
``collapse: "recipients"`` then an entity step with chunked ``uei in`` (≤
market_store.UEI_IN_CAP per chunk).

GRAMMAR RULES (the deterministic edge cases, per the v1.1 amendments):
  • SUBJECT REQUIRED: a phrase with no subject token (companies/awards/vehicles/
    orders/actions) refuses — never a defaulted grain. First subject token fixes
    the grain; later subject-vocabulary tokens are connective no-ops
    ("…on construction awards…" after "companies").
  • BOOLEANS: ``or/not/excluding/except/without/nor/vs/minus`` are RESERVED —
    they bind to REFUSE with the fix named, never silently AND. One carve-out:
    ``or`` between two values that bind to the SAME axis ("236220 or 237310",
    "VA or MD") merges into that axis's in-list. ``and`` is a connective no-op
    (AND is the engine's default semantics).
  • CODE LETTERS by context word, never by guess: ``code <A-Y>`` binds
    action_type_code ONLY when the phrase carries a mod/action context token;
    ``plan <A-H>`` binds subcontracting_plan. A bare letter never binds.
  • TIME: trailing windows ("last 90 days", "last quarter" = 90, "last year" =
    365, "just/recently" = 90) → ``<= N``; bounded windows ("in 2024", "in
    fy25", "since march 2025") → days-ago ``between``/``<=`` computed against
    the injectable ``today``. Years 1990-2035 bind as CALENDAR YEARS — a PSC
    code in that numeric range must be written "psc 2024". Entity-grain money
    is build-windowed, so FY/calendar-bounded MONEY on companies-without-events
    refuses with the reason.
  • CODE LITERALS validated against the in-memory reference dimensions (a bare
    unknown 6-digit/4-char token refuses as "not a known NAICS/PSC").
  • The token ``or`` is always boolean and ``in`` is always a connective — so
    the states OR (Oregon) and IN (Indiana) are unreachable as bare tokens in v1
    (documented determinism trade; write the entity filter directly).

Known token→axis conflicts are resolved by FIXED precedence, spelled out at the
match sites below. Vocabulary changes are code changes: reviewed, versioned
(COMPILER_VERSION bumps), and re-pinned in the acceptance tests.
"""
from __future__ import annotations

import re
from datetime import date as dt_date
from typing import Any, Callable

from . import market_registry, market_store
from .lance_store import MapCompileError

COMPILER_VERSION = "phrase.v1"

PHRASE_MAX_CHARS = 400
DEFAULT_STEP_LIMIT = 1_000            # emitted bodies use the engine's hard row cap

# ── Vocabulary: subjects → grain + shape ───────────────────────────────────────
SUBJECTS: dict[str, dict[str, Any]] = {
    "companies": {"grain": "entity", "collapse": True},
    "firms": {"grain": "entity", "collapse": True},
    "entities": {"grain": "entity", "collapse": True},
    "recipients": {"grain": "entity", "collapse": True},
    "awards": {"grain": "prime_award"},
    "vehicles": {"grain": "prime_award", "topology": "vehicle"},
    "idvs": {"grain": "prime_award", "topology": "vehicle"},
    "idiqs": {"grain": "prime_award", "topology": "vehicle"},
    "orders": {"grain": "prime_award", "topology": "vehicle_order"},
    "task orders": {"grain": "prime_award", "topology": "vehicle_order"},
    "actions": {"grain": "transaction"},
    "mods": {"grain": "transaction"},
    "modifications": {"grain": "transaction"},
    "transactions": {"grain": "transaction"},
}

# ── Vocabulary: FPDS event phrases → action_type_code (registry enum) ─────────
EVENTS: dict[str, str] = {
    "additional work": "A",
    "funding action": "C",
    "funded": "C",
    "change order": "D",
    "change orders": "D",
    "terminated for default": "E",
    "terminated for convenience": "F",
    "exercised an option": "G",
    "option exercised": "G",
    "option was exercised": "G",
    "definitized": "H",
    "novated": "J",
    "novation": "J",
    "closed out": "K",
    "close out": "K",
    "re-represented": "R",
    "rerepresentation": "R",
    "subk plan added": "Y",
}

# ── Vocabulary: subcontracting-plan phrases ────────────────────────────────────
PLAN_ATTACHED_SET = ["C", "D", "E", "F", "G", "H"]      # FAR 19.7 attached set
PLAN_PHRASES: dict[str, Any] = {
    "with a subcontracting plan": PLAN_ATTACHED_SET,
    "plan attached": PLAN_ATTACHED_SET,
    "plan not required": "B",
    "no subcontracting possibilities": "A",
}

# ── Vocabulary: sector aliases → FROZEN 6-digit NAICS sets ─────────────────────
# Frozen in the module by design (plan §3.3): additions are reviewed vocabulary
# PRs. The construction set = the 6-digit sector-23 leaves (acceptance CSV EX01).
CONSTRUCTION_NAICS = (
    "236115", "236116", "236117", "236118", "236210", "236220", "237110",
    "237120", "237130", "237210", "237310", "237990", "238110", "238120",
    "238130", "238140", "238150", "238160", "238170", "238190", "238210",
    "238220", "238290", "238310", "238320", "238330", "238340", "238350",
    "238390", "238910", "238990",
)
SECTORS: dict[str, tuple[str, ...]] = {
    "construction": CONSTRUCTION_NAICS,
    "it services": ("541511", "541512", "541513", "541519"),
    "engineering": ("541330",),
    "security services": ("561612", "561621", "541690"),
    "janitorial": ("561720", "561210"),
    "facilities": ("561720", "561210"),
    "landscaping": ("561730",),
}

# ── Vocabulary: toptier agency aliases (from the 136 live deduped pairs) ──────
AGENCIES: dict[str, str] = {
    "dod": "097", "defense": "097", "navy": None, "army": None, "air force": None,
    "gsa": "047", "va": None,  # 'va' the STATE wins (precedence note at the match site)
    "veterans affairs": "036", "dhs": "070", "homeland": "070",
    "homeland security": "070", "doe": "089", "energy": "089", "usda": "012",
    "agriculture": "012", "hhs": "075", "interior": "014", "doi": "014",
    "army corps": "096",
}
# Sub-toptier names refuse with the Cycle-3 pointer (amendment C).
_SUBTIER_REFUSALS = {"navy", "army", "air force", "navair", "navfac", "disa",
                     "socom", "usace"}

# ── Vocabulary: entity qualifiers → entities.v5 fields ─────────────────────────
QUALIFIERS: dict[str, dict[str, Any]] = {
    "in dsbs": {"field": "in_dsbs", "op": "=", "value": True},
    "dsbs": {"field": "in_dsbs", "op": "=", "value": True},
    "sam-active": {"field": "sam_is_active", "op": "=", "value": True},
    "sam active": {"field": "sam_is_active", "op": "=", "value": True},
    "sub-only": {"field": "is_sub_only_lifetime", "op": "=", "value": True},
    "sub only": {"field": "is_sub_only_lifetime", "op": "=", "value": True},
    "never primed": {"field": "is_sub_only_lifetime", "op": "=", "value": True},
    "also sub": {"field": "prime_and_sub", "op": "=", "value": True},
    "also subs": {"field": "prime_and_sub", "op": "=", "value": True},
    "also primes": {"field": "prime_and_sub", "op": "=", "value": True},
    "also prime": {"field": "prime_and_sub", "op": "=", "value": True},
    "both sides": {"field": "prime_and_sub", "op": "=", "value": True},
}

# Capability-context phrases: the NEXT code literal binds to these entity fields
# (naics → first name, psc → second).
CAPABILITY_CONTEXTS: dict[str, tuple[str, str]] = {
    "delivered subs under": ("sub_naics", "sub_psc"),
    "subbed under": ("sub_naics", "sub_psc"),
    "primed in": ("prime_naics", "prime_psc"),
    "inferred primeable": ("inferred_primeable_naics", "inferred_primeable_psc"),
    "inferred subbable": ("inferred_subbable_naics", "inferred_subbable_psc"),
}

# ── Reserved booleans (amendment A) — bind to REFUSE, never silently AND ──────
RESERVED_BOOLEAN = {"not", "excluding", "except", "without", "nor", "vs",
                    "versus", "minus", "and/or"}

# Connective no-ops: consumed, disclosed, never semantic.
CONNECTIVES = {
    "that", "which", "who", "whose", "the", "a", "an", "with", "in", "on", "of",
    "from", "for", "to", "by", "and", "carrying", "acted", "received", "have",
    "has", "had", "were", "was", "their", "its", "any", "just", "recently",
    "under",
}
# 'just'/'recently' double as a time default when NO other time clause binds.
RECENCY_DEFAULT_DAYS = 90

# Context words that license the "code <letter>" → action_type_code binding.
CODE_CONTEXT_WORDS = {"mod", "mods", "modification", "modifications",
                      "action", "actions", "received"}

_MONEY_RE = re.compile(r"^\$?(\d+(?:\.\d+)?)([kmb])?$")
_MONEY_MULT = {"k": 1e3, "m": 1e6, "b": 1e9, None: 1.0}
_PIID_RE = re.compile(r"^[A-Z0-9]{6,20}$")
_NAICS_RE = re.compile(r"^\d{6}$")
_NAICS_PREFIX_RE = re.compile(r"^(\d{2,5})\*?$")
_PSC_RE = re.compile(r"^[A-Z0-9]{4}$", re.I)
_YEAR_RE = re.compile(r"^(19[9]\d|20[0-3]\d)$")
_FY_RE = re.compile(r"^fy(\d{2}|\d{4})$")

US_STATES = set(market_registry.US_STATE_CODES)

FISCAL_YEAR_START_MONTH = 10          # FFY N = Oct 1 (N-1) → Sep 30 N
_MONTHS = {m: i + 1 for i, m in enumerate(
    ("january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"))}


# ── Reference-dimension seams (monkeypatched hermetic in tests) ────────────────
def _known_naics() -> set[str]:
    return set(dict(market_store._codes_for("naics")))


def _known_psc() -> set[str]:
    return {c.upper() for c in dict(market_store._codes_for("psc"))}


def _refuse(token: str, reason: str) -> None:
    raise MapCompileError(f"phrase refused: token '{token}' — {reason}")


# ── 1. LEX ─────────────────────────────────────────────────────────────────────
def _lex(phrase: str) -> list[str]:
    """Lowercase, strip clause punctuation, split on whitespace. Tokens keep
    $ . * / - so money, prefixes, and hyphenated vocabulary survive."""
    if not isinstance(phrase, str) or not phrase.strip():
        raise MapCompileError("phrase refused: empty phrase")
    if len(phrase) > PHRASE_MAX_CHARS:
        raise MapCompileError(f"phrase refused: over {PHRASE_MAX_CHARS} chars")
    cleaned = re.sub(r"[,;:()\"'?!]", " ", phrase.lower())
    return [t for t in cleaned.split() if t]


# ── 2+3. MATCH + BIND ──────────────────────────────────────────────────────────
# A binding = {"tokens": [...], "axis": ..., "op": ..., "value": ...}. Axes are
# compiler-internal; the grain resolver maps them onto registry fields.
def _bind(tokens: list[str], today: dt_date) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    phrase_has_code_context = any(t in CODE_CONTEXT_WORDS for t in tokens)
    i = 0
    n = len(tokens)

    def _multiword(vocab: dict, max_len: int = 4) -> tuple[str, Any] | None:
        for ln in range(min(max_len, n - i), 0, -1):
            span = " ".join(tokens[i:i + ln])
            if span in vocab:
                return span, vocab[span]
        return None

    def _add(span_tokens: list[str], axis: str, op: str | None, value: Any) -> None:
        bindings.append({"tokens": span_tokens, "axis": axis, "op": op, "value": value})

    subject_bound = False
    while i < n:
        tok = tokens[i]

        # RESERVED BOOLEANS (amendment A) — before anything else can claim them.
        if tok in RESERVED_BOOLEAN:
            _refuse(tok, "negation / cross-axis boolean is not served in v1 — "
                         "same-axis alternatives compile as a list ('236220 or "
                         "237310'); cross-axis OR and NOT need separate phrases")
        if tok == "or":
            # carve-out: legal ONLY between two same-axis values — the LEFT value
            # must already be bound and the RIGHT value must bind to the same
            # axis. Handled at the value sites below via _pending_or.
            if not bindings or bindings[-1]["axis"] not in (
                    "naics", "psc", "state"):
                _refuse("or", "cross-axis OR is not served in v1 — 'or' is legal "
                              "only between two codes or two states")
            i += 1
            _pending_or_axis = bindings[-1]["axis"]
            # the next token MUST bind to the same axis
            nxt = tokens[i] if i < n else ""
            if _pending_or_axis == "state" and nxt.upper() in US_STATES:
                bindings[-1]["value"] = sorted(set(bindings[-1]["value"]) | {nxt.upper()})
                bindings[-1]["tokens"].extend(["or", nxt])
                i += 1
                continue
            if _pending_or_axis == "naics" and _NAICS_RE.match(nxt):
                if nxt not in _known_naics():
                    _refuse(nxt, "not a known NAICS code")
                prior = bindings[-1]["value"]
                prior = prior if isinstance(prior, list) else [prior]
                bindings[-1]["value"] = sorted(set(prior) | {nxt})
                bindings[-1]["op"] = "in"
                bindings[-1]["tokens"].extend(["or", nxt])
                i += 1
                continue
            if _pending_or_axis == "psc" and _PSC_RE.match(nxt) \
                    and nxt.upper() in _known_psc():
                prior = bindings[-1]["value"]
                prior = prior if isinstance(prior, list) else [prior]
                bindings[-1]["value"] = sorted(set(prior) | {nxt.upper()})
                bindings[-1]["op"] = "in"
                bindings[-1]["tokens"].extend(["or", nxt])
                i += 1
                continue
            _refuse("or", f"'or' must join two values of the same axis "
                          f"({_pending_or_axis}) — '{nxt}' does not bind to it")

        # SUBTIER agency names — refuse with the Cycle-3 pointer (amendment C).
        hit = _multiword({k: k for k in _SUBTIER_REFUSALS}, 2)
        if hit is not None:
            _refuse(hit[0], "needs sub-agency/office-tier resolution (Cycle 3); "
                            "toptier works today: 'from dod', 'from gsa', …")

        # SUBJECT — first one fixes the grain; later subject words are no-ops.
        hit = _multiword(SUBJECTS, 2)
        if hit is not None:
            span, shape = hit
            # "code <X> mod": the mod/action token right after a code letter is
            # CONTEXT, not a subject — consumed by the code-letter rule below,
            # so a subject here is only bound when not immediately post-code.
            if not subject_bound:
                _add(span.split(), "subject", None, span)
                subject_bound = True
            else:
                _add(span.split(), "connective", None, None)
            i += len(span.split())
            continue

        # EVENTS (multi-word first — longest match wins by construction).
        hit = _multiword(EVENTS, 4)
        if hit is not None:
            span, letter = hit
            _add(span.split(), "action_type", "=", letter)
            i += len(span.split())
            continue

        # SUBK-PLAN phrases.
        hit = _multiword(PLAN_PHRASES, 4)
        if hit is not None:
            span, val = hit
            _add(span.split(), "subcontracting_plan",
                 "in" if isinstance(val, list) else "=", val)
            i += len(span.split())
            continue

        # SECTOR aliases → frozen NAICS sets.
        hit = _multiword(SECTORS, 2)
        if hit is not None:
            span, codes = hit
            _add(span.split(), "naics", "in", list(codes))
            i += len(span.split())
            continue

        # ENTITY qualifiers (multi-word before connectives: "in dsbs" beats "in").
        hit = _multiword(QUALIFIERS, 2)
        if hit is not None:
            span, spec = hit
            _add(span.split(), "entity_qualifier", spec["op"],
                 {"field": spec["field"], "value": spec["value"]})
            i += len(span.split())
            continue

        # CAPABILITY contexts: the following code literal binds to the entity field.
        hit = _multiword(CAPABILITY_CONTEXTS, 3)
        if hit is not None:
            span, (naics_field, psc_field) = hit
            j = i + len(span.split())
            code_tok = tokens[j] if j < n else ""
            if code_tok == "naics" or code_tok == "psc":       # optional system word
                j += 1
                code_tok = tokens[j] if j < n else ""
            if _NAICS_RE.match(code_tok):
                if code_tok not in _known_naics():
                    _refuse(code_tok, "not a known NAICS code")
                _add(tokens[i:j + 1], "entity_qualifier", "=",
                     {"field": naics_field, "value": code_tok})
            elif _PSC_RE.match(code_tok) and code_tok.upper() in _known_psc():
                _add(tokens[i:j + 1], "entity_qualifier", "=",
                     {"field": psc_field, "value": code_tok.upper()})
            else:
                _refuse(code_tok or span,
                        f"'{span}' must be followed by a NAICS or PSC code")
            i = j + 1
            continue

        # AGENCY aliases ("va" is claimed by the STATE below — fixed precedence;
        # use 'veterans affairs' for the department).
        hit = _multiword({k: v for k, v in AGENCIES.items() if v is not None}, 2)
        if hit is not None and hit[0] != "va":
            span, code = hit
            _add(span.split(), "agency", "=", code)
            i += len(span.split())
            continue

        # "code <A-Y>" → action_type (context-licensed); "plan <A-H>" → subk plan.
        if tok == "code" and i + 1 < n and re.match(r"^[a-y]$", tokens[i + 1]):
            if not phrase_has_code_context:
                _refuse(f"code {tokens[i + 1]}",
                        "a bare 'code X' needs a mod/action context word "
                        "(…received a code X mod…)")
            letter = tokens[i + 1].upper()
            if letter not in market_registry.ACTION_TYPE_CODES:
                _refuse(f"code {tokens[i + 1]}", "not an FPDS action-type code")
            span_toks = tokens[i:i + 2]
            i += 2
            # consume the immediate context word (mod/modification/action)
            if i < n and tokens[i] in CODE_CONTEXT_WORDS:
                span_toks = span_toks + [tokens[i]]
                i += 1
            _add(span_toks, "action_type", "=", letter)
            continue
        if tok == "plan" and i + 1 < n and re.match(r"^[a-h]$", tokens[i + 1]):
            _add(tokens[i:i + 2], "subcontracting_plan", "=", tokens[i + 1].upper())
            i += 2
            continue

        # "naics <code|prefix>" / "psc <code>" / bare validated literals.
        if tok == "naics" and i + 1 < n:
            nxt = tokens[i + 1]
            if _NAICS_RE.match(nxt):
                if nxt not in _known_naics():
                    _refuse(nxt, "not a known NAICS code")
                _add(tokens[i:i + 2], "naics", "=", nxt)
                i += 2
                continue
            m = _NAICS_PREFIX_RE.match(nxt)
            if m and len(m.group(1)) < 6:
                leaves = sorted(c for c in _known_naics()
                                if len(c) == 6 and c.startswith(m.group(1)))
                if not leaves:
                    _refuse(nxt, "no 6-digit NAICS under that prefix")
                _add(tokens[i:i + 2], "naics", "in", leaves)
                i += 2
                continue
            _refuse(nxt, "not a NAICS code or prefix")
        if tok == "psc" and i + 1 < n:
            nxt = tokens[i + 1]
            if _PSC_RE.match(nxt) and nxt.upper() in _known_psc():
                _add(tokens[i:i + 2], "psc", "=", nxt.upper())
                i += 2
                continue
            _refuse(nxt, "not a known PSC code")

        # "vehicle <PIID>" (also reached via "under vehicle <PIID>" — 'under' is
        # a connective).
        if tok == "vehicle" and i + 1 < n and _PIID_RE.match(tokens[i + 1].upper()):
            _add(tokens[i:i + 2], "parent_piid", "=", tokens[i + 1].upper())
            i += 2
            continue

        # MONEY: "over/above/at least $X" → >= ; "under/below $X" → <= .
        if tok in ("over", "above", "under", "below") or \
                (tok == "at" and i + 1 < n and tokens[i + 1] == "least"):
            cmp_op = "<=" if tok in ("under", "below") else ">="
            j = i + (2 if tok == "at" else 1)
            mtok = tokens[j] if j < n else ""
            m = _MONEY_RE.match(mtok)
            if m:
                amount = float(m.group(1)) * _MONEY_MULT[m.group(2)]
                _add(tokens[i:j + 1], "money", cmp_op, amount)
                i = j + 1
                continue
            if tok == "under":                # "under vehicle …" / plain connective
                _add([tok], "connective", None, None)
                i += 1
                continue
            _refuse(mtok or tok, "expected a dollar amount like $5m")

        # SUB-INCOME money variant: "sub income over $X".
        if tok == "sub" and i + 1 < n and tokens[i + 1] == "income":
            _add(tokens[i:i + 2], "money_basis", None, "sub")
            i += 2
            continue

        # TIME — trailing windows.
        if tok == "last" and i + 1 < n:
            nxt = tokens[i + 1]
            if nxt == "quarter":
                _add(tokens[i:i + 2], "time", "<=", 90)
                i += 2
                continue
            if nxt == "year":
                _add(tokens[i:i + 2], "time", "<=", 365)
                i += 2
                continue
            if nxt.isdigit() and i + 2 < n:
                unit = tokens[i + 2]
                mult = {"day": 1, "days": 1, "month": 30, "months": 30,
                        "year": 365, "years": 365}.get(unit)
                if mult is not None:
                    _add(tokens[i:i + 3], "time", "<=", int(nxt) * mult)
                    i += 3
                    continue
            _refuse(" ".join(tokens[i:i + 2]), "unrecognized time window")

        # TIME — bounded windows (amendment B): "in 2024" / "fy25" / "since march 2025".
        if _YEAR_RE.match(tok):
            y = int(tok)
            _add([tok], "time", "between",
                 _days_between(dt_date(y, 1, 1), dt_date(y, 12, 31), today))
            i += 1
            continue
        m = _FY_RE.match(tok)
        if m:
            y = int(m.group(1))
            y = y + 2000 if y < 100 else y
            _add([tok], "time", "between",
                 _days_between(dt_date(y - 1, FISCAL_YEAR_START_MONTH, 1),
                               dt_date(y, 9, 30), today))
            i += 1
            continue
        if tok in ("fy", "fiscal") and i + 1 < n:
            j = i + (2 if tok == "fiscal" and tokens[i + 1] == "year" else 1)
            ytok = tokens[j] if j < n else ""
            if ytok.isdigit():
                y = int(ytok)
                y = y + 2000 if y < 100 else y
                _add(tokens[i:j + 1], "time", "between",
                     _days_between(dt_date(y - 1, FISCAL_YEAR_START_MONTH, 1),
                                   dt_date(y, 9, 30), today))
                i = j + 1
                continue
            _refuse(ytok or tok, "expected a fiscal year like fy25")
        if tok == "since" and i + 1 < n:
            j = i + 1
            month = _MONTHS.get(tokens[j])
            if month is not None and j + 1 < n and _YEAR_RE.match(tokens[j + 1]):
                start = dt_date(int(tokens[j + 1]), month, 1)
                _add(tokens[i:j + 2], "time", "<=", max(0, (today - start).days))
                i = j + 2
                continue
            if _YEAR_RE.match(tokens[j]):
                start = dt_date(int(tokens[j]), 1, 1)
                _add(tokens[i:j + 1], "time", "<=", max(0, (today - start).days))
                i = j + 1
                continue
            _refuse(tokens[j], "expected 'since <month> <year>' or 'since <year>'")

        # STATES (fixed precedence: beats the 'va' agency alias; CONNECTIVE words
        # never bind as states — so Indiana (IN), like Oregon (OR), is unreachable
        # as a bare token in v1: a documented determinism trade, per the module
        # docstring).
        if tok.upper() in US_STATES and len(tok) == 2 and tok not in CONNECTIVES:
            _add([tok], "state", "in", [tok.upper()])
            i += 1
            continue

        # BARE code literals, validated against the reference dims.
        if _NAICS_RE.match(tok):
            if tok not in _known_naics():
                _refuse(tok, "not a known NAICS code")
            _add([tok], "naics", "=", tok)
            i += 1
            continue
        if _PSC_RE.match(tok) and not tok.isdigit() and tok.upper() in _known_psc():
            _add([tok], "psc", "=", tok.upper())
            i += 1
            continue

        # CONNECTIVES last — every remaining meaningful word must have bound.
        if tok in CONNECTIVES or tok in CODE_CONTEXT_WORDS:
            _add([tok], "connective", None, None)
            i += 1
            continue

        _refuse(tok, "not in the phrase vocabulary")

    return bindings


def _days_between(start: dt_date, end: dt_date, today: dt_date) -> list[int]:
    """Calendar window → days-ago ``between [lo, hi]`` (lo = days since END,
    hi = days since START), clamped at 0 for windows that reach the future."""
    lo = max(0, (today - end).days)
    hi = max(0, (today - start).days)
    return [lo, hi]


# ── 4. RESOLVE + 5. EMIT ───────────────────────────────────────────────────────
# Axis → home grain(s). Axes present decide single-step vs the collapse pipeline.
_TXN_ONLY = {"action_type", "subcontracting_plan"}
_AWARD_ONLY = {"parent_piid", "topology"}
_ENTITY_ONLY = {"entity_qualifier", "state"}


def compile_phrase(phrase: str, today: "dt_date | None" = None) -> dict[str, Any]:
    """phrase → {bindings, grain, plan} (the exact market-query bodies), or
    ``MapCompileError`` naming the refusing token. Pure given ``today``."""
    today = today or dt_date.today()
    tokens = _lex(phrase)
    bindings = _bind(tokens, today)

    subjects = [b for b in bindings if b["axis"] == "subject"]
    if not subjects:
        raise MapCompileError(
            "phrase refused: no subject — name one of companies | awards | "
            "vehicles | orders | actions")
    shape = SUBJECTS[subjects[0]["value"]]
    terminal_grain = shape["grain"]

    axes = {b["axis"] for b in bindings}
    naics = [b for b in bindings if b["axis"] == "naics"]
    psc = [b for b in bindings if b["axis"] == "psc"]
    times = [b for b in bindings if b["axis"] == "time"]
    moneys = [b for b in bindings if b["axis"] == "money"]
    agencies = [b for b in bindings if b["axis"] == "agency"]
    quals = [b for b in bindings if b["axis"] == "entity_qualifier"]
    states = [b for b in bindings if b["axis"] == "state"]

    def _code_filters(naics_field: str, psc_field: str) -> list[dict[str, Any]]:
        out = []
        for b in naics:
            out.append({"field": naics_field, "op": b["op"], "value": b["value"]})
        for b in psc:
            out.append({"field": psc_field, "op": b["op"], "value": b["value"]})
        return out

    def _time_filter(field: str) -> list[dict[str, Any]]:
        if len(times) > 1:
            raise MapCompileError("phrase refused: more than one time window")
        return [{"field": field, "op": t["op"], "value": t["value"]} for t in times]

    plan: list[dict[str, Any]] = []
    if terminal_grain == "entity":
        # any event axis OR any time window on companies routes through the
        # transaction grain (entity money is build-windowed — amendment B)
        needs_pipeline = bool(axes & _TXN_ONLY) or bool(times) or bool(agencies)
        if needs_pipeline:
            if any(b["axis"] == "money_basis" for b in bindings):
                raise MapCompileError(
                    "phrase refused: 'sub income' is an entity lifetime axis — it "
                    "cannot combine with event/time clauses (amendment B)")
            step1_filters: list[dict[str, Any]] = []
            for b in bindings:
                if b["axis"] == "action_type":
                    step1_filters.append({"field": "action_type_code",
                                          "op": b["op"], "value": b["value"]})
                if b["axis"] == "subcontracting_plan":
                    step1_filters.append({"field": "subcontracting_plan",
                                          "op": b["op"], "value": b["value"]})
            step1_filters += _code_filters("naics_code", "psc_code")
            for b in agencies:
                step1_filters.append({"field": "awarding_agency_code",
                                      "op": "=", "value": b["value"]})
            for b in moneys:
                step1_filters.append({"field": "federal_action_obligation",
                                      "op": b["op"], "value": b["value"]})
            step1_filters += _time_filter("action_date")
            if not step1_filters:
                raise MapCompileError(
                    "phrase refused: companies-pipeline needs at least one event/"
                    "code/time clause (the transaction grain requires a filter)")
            plan.append({"grain": "transaction", "collapse": "recipients",
                         "filters": step1_filters, "limit": DEFAULT_STEP_LIMIT})
            step2_filters: list[dict[str, Any]] = [
                {"field": "uei", "op": "in", "value": ["<step1 ueis, chunks of <=500>"]}]
            for b in quals:
                step2_filters.append({"field": b["value"]["field"], "op": b["op"],
                                      "value": b["value"]["value"]})
            for b in states:
                step2_filters.append(_state_filter(b))
            plan.append({"grain": "entity", "filters": step2_filters,
                         "limit": DEFAULT_STEP_LIMIT})
        else:
            # single entity-grain step (lane/inferred/identity cuts)
            filters: list[dict[str, Any]] = []
            for b in quals:
                filters.append({"field": b["value"]["field"], "op": b["op"],
                                "value": b["value"]["value"]})
            for b in states:
                filters.append(_state_filter(b))
            # bare codes on the entity grain are ambiguous between lanes —
            # require a capability context ("primed in" / "delivered subs under")
            if naics or psc:
                raise MapCompileError(
                    "phrase refused: a bare code on companies needs a capability "
                    "context — 'primed in 541690' or 'delivered subs under 236220'")
            if moneys:
                basis = "sub" if any(b["axis"] == "money_basis" for b in bindings) \
                    else "prime"
                field = "sub_amt_lifetime" if basis == "sub" else "prime_obl_lifetime"
                for b in moneys:
                    filters.append({"field": field, "op": b["op"], "value": b["value"]})
            if not filters:
                raise MapCompileError(
                    "phrase refused: no entity clauses bound — nothing to select on")
            plan.append({"grain": "entity", "filters": filters,
                         "limit": DEFAULT_STEP_LIMIT})
    elif terminal_grain == "prime_award":
        if axes & _TXN_ONLY:
            raise MapCompileError(
                "phrase refused: action/plan codes live on the transaction grain — "
                "say 'actions …' or 'companies …', not 'awards …'")
        filters = []
        if "topology" in shape:
            filters.append({"field": "award_topology", "op": "=",
                            "value": shape["topology"]})
        for b in bindings:
            if b["axis"] == "parent_piid":
                filters.append({"field": "parent_award_id_piid", "op": "=",
                                "value": b["value"]})
        filters += _code_filters("naics_code", "psc_code")
        for b in agencies:
            filters.append({"field": "awarding_agency_code", "op": "=",
                            "value": b["value"]})
        for b in moneys:
            filters.append({"field": "life_to_date_obligated", "op": b["op"],
                            "value": b["value"]})
        filters += _time_filter("last_action_date")
        if quals or states:
            raise MapCompileError(
                "phrase refused: entity qualifiers (dsbs/state/…) need the "
                "companies subject")
        plan.append({"grain": "prime_award", "filters": filters,
                     "limit": DEFAULT_STEP_LIMIT})
    else:  # transaction
        filters = []
        for b in bindings:
            if b["axis"] == "action_type":
                filters.append({"field": "action_type_code", "op": b["op"],
                                "value": b["value"]})
            if b["axis"] == "subcontracting_plan":
                filters.append({"field": "subcontracting_plan", "op": b["op"],
                                "value": b["value"]})
        filters += _code_filters("naics_code", "psc_code")
        for b in agencies:
            filters.append({"field": "awarding_agency_code", "op": "=",
                            "value": b["value"]})
        for b in moneys:
            filters.append({"field": "federal_action_obligation", "op": b["op"],
                            "value": b["value"]})
        filters += _time_filter("action_date")
        if quals or states:
            raise MapCompileError(
                "phrase refused: entity qualifiers (dsbs/state/…) need the "
                "companies subject")
        plan.append({"grain": "transaction", "filters": filters,
                     "limit": DEFAULT_STEP_LIMIT})

    return {
        "compilerVersion": COMPILER_VERSION,
        "registryVersion": market_registry.REGISTRY_VERSION,
        "phrase": phrase,
        "bindings": [{k: v for k, v in b.items()} for b in bindings],
        "grain": terminal_grain,
        "plan": plan,
    }


def _state_filter(b: dict[str, Any]) -> dict[str, Any]:
    vals = b["value"]
    if len(vals) == 1:
        return {"field": "state", "op": "=", "value": vals[0]}
    return {"field": "state", "op": "in", "value": vals}


# ── 6. EXECUTE (the emitted bodies run through the existing executors) ─────────
def execute_plan(plan: list[dict[str, Any]],
                 today: "dt_date | None" = None) -> dict[str, Any]:
    """Run the compiled plan. Single step → that executor's result verbatim.
    Pipeline → step-1 collapse, then the entity step with the RESOLVED uei set
    in chunks of ≤ market_store.UEI_IN_CAP, rows unioned (deduped by uei). The
    resolved uei list replaces the placeholder in the returned plan copy."""
    step1 = plan[0]
    if len(plan) == 1:
        if step1["grain"] == "entity":
            return {"steps": [None],
                    "result": market_store.execute_entity_query(
                        step1["filters"], step1["limit"], today)}
        spec = market_registry.TABLE_GRAINS[step1["grain"]]
        if step1.get("collapse"):
            return {"steps": [None],
                    "result": market_store.execute_table_collapse(
                        spec, step1["filters"], step1["limit"], today)}
        return {"steps": [None],
                "result": market_store.execute_table_query(
                    spec, step1["filters"], step1["limit"], today)}

    spec = market_registry.TABLE_GRAINS[step1["grain"]]
    collapsed = market_store.execute_table_collapse(
        spec, step1["filters"], step1["limit"], today)
    ueis = [r["uei"] for r in collapsed["rows"]]
    step2 = {**plan[1], "filters": [dict(f) for f in plan[1]["filters"]]}
    if not ueis:
        return {"steps": [collapsed, None], "resolved_ueis": [],
                "result": {"rows": [], "total": 0, "returned": 0, "capped": False,
                           "executed": {"grain": "entity", "filters": [],
                                        "note": "step 1 matched no recipients"}}}
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    total = 0
    cap = market_store.UEI_IN_CAP
    for k in range(0, len(ueis), cap):
        chunk = ueis[k:k + cap]
        step2["filters"][0] = {"field": "uei", "op": "in", "value": chunk}
        res = market_store.execute_entity_query(step2["filters"], step2["limit"], today)
        total += res["total"]
        for r in res["rows"]:
            u = r.get("uei")
            if u and u not in seen:
                seen.add(u)
                rows.append(r)
    step2["filters"][0] = {"field": "uei", "op": "in", "value": ueis}
    return {"steps": [collapsed, None], "resolved_ueis": ueis,
            "resolved_step2": step2,
            "result": {"rows": rows, "total": total, "returned": len(rows),
                       "capped": total > len(rows),
                       "executed": {"grain": "entity",
                                    "filters": step2["filters"],
                                    "limit": step2["limit"]}}}


def compile_and_execute(body: Any, today: "dt_date | None" = None) -> dict[str, Any]:
    """The route entry: validate the envelope, compile, execute, disclose."""
    if not isinstance(body, dict):
        raise MapCompileError("request body must be an object")
    unknown = set(body) - {"phrase"}
    if unknown:
        raise MapCompileError(f"unknown body key(s) {sorted(unknown)!r}")
    compiled = compile_phrase(body.get("phrase"), today)
    executed = execute_plan(compiled["plan"], today)
    meta: dict[str, Any] = {
        "compilerVersion": compiled["compilerVersion"],
        "registryVersion": compiled["registryVersion"],
        "phrase": compiled["phrase"],
        "bindings": compiled["bindings"],
        "grain": compiled["grain"],
        "plan": compiled["plan"],
        "refused": None,
    }
    if "resolved_step2" in executed:
        meta["plan"] = [compiled["plan"][0], executed["resolved_step2"]]
        meta["step1"] = {k: executed["steps"][0][k] for k in
                         ("total_rows", "distinct_recipients", "scan_capped")}
    result = executed["result"]
    meta["returned"] = result.get("returned")
    meta["total"] = result.get("total")
    meta["capped"] = result.get("capped")
    return {"meta": meta, "data": {"rows": result.get("rows", [])}}
