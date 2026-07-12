"""Deterministic AGGREGATE phrase mode — phrase-agg.v1 (POST /api/v1/market/phrase).

The retrieval grammar (phrase_compiler) answers "WHICH rows match"; this module
answers "HOW MUCH, grouped by what" — the demo/portrait surface. Same doctrine,
separate closed grammar: the opener token ``total`` routes the phrase here
(phrase_compiler.compile_and_execute delegates before the retrieval lex), every
span longest-matches a closed vocabulary, and any unbound token refuses the
whole phrase naming the token. Zero LLM. Same phrase + artifact → same bars.

v1 GRAMMAR — exactly one production, everything else refuses:

    total <measure> <group> <window>

    measure  awarded | award value | obligated | obligations | spend
             → sum(prime_obl) over the combo×FY portrait
    group    by industry | across industries
             → NAICS sector rollup (2-digit, 31-33/44-45/48-49 merged)
    window   fy23 to fy25 | fy2023 to fy2025 | fy24        (REQUIRED)
    connectives: from, in, of, the, federal, $  — consumed, disclosed, never semantic

Execution: ONE GROUP BY on the sidecar's ``v_combo_fy`` (combo×FY baked
portrait). Results are cached in-process keyed by the normalized phrase —
the response is deterministic w.r.t. the artifact, and the artifact stamp is
carried in the response meta so a stale cache is visible, never silent.
"""
from __future__ import annotations

import re
import threading
import time
from typing import Any

from .lance_store import MapCompileError
from . import sidecar_executor

AGG_COMPILER_VERSION = "phrase-agg.v1"

# ── closed vocabularies ────────────────────────────────────────────────────────
MEASURES: dict[str, str] = {
    "awarded": "prime_obl_sum",
    "award value": "prime_obl_sum",
    "obligated": "prime_obl_sum",
    "obligations": "prime_obl_sum",
    "spend": "prime_obl_sum",
}
GROUPS: dict[str, str] = {
    "by industry": "industry",
    "across industries": "industry",
}
CONNECTIVES = {"from", "in", "of", "the", "federal", "$"}

# NAICS sector rollup — official 2-digit sectors; the 3 ranged sectors merge.
_SECTOR_MERGE = {"31": "31-33", "32": "31-33", "33": "31-33",
                 "44": "44-45", "45": "44-45", "48": "48-49", "49": "48-49"}
SECTOR_LABELS: dict[str, str] = {
    "11": "Agriculture, Forestry & Fishing",
    "21": "Mining & Oil/Gas Extraction",
    "22": "Utilities",
    "23": "Construction",
    "31-33": "Manufacturing",
    "42": "Wholesale Trade",
    "44-45": "Retail Trade",
    "48-49": "Transportation & Warehousing",
    "51": "Information",
    "52": "Finance & Insurance",
    "53": "Real Estate & Leasing",
    "54": "Professional, Scientific & Technical",
    "55": "Management of Companies",
    "56": "Administrative & Support Services",
    "61": "Educational Services",
    "62": "Health Care & Social Assistance",
    "71": "Arts, Entertainment & Recreation",
    "72": "Accommodation & Food Services",
    "81": "Other Services",
    "92": "Public Administration",
}

_FY_RE = re.compile(r"^fy(\d{2}|\d{4})$")
_FY_MIN, _FY_MAX = 2008, 2035


def is_aggregate_phrase(phrase: Any) -> bool:
    """The mode router: an aggregate phrase OPENS with the reserved token
    ``total``. (``total`` is not retrieval vocabulary, so this never shadows
    an existing phrase.)"""
    return isinstance(phrase, str) and phrase.strip().lower().split()[:1] == ["total"]


def _fy(tok: str) -> int | None:
    m = _FY_RE.match(tok)
    if not m:
        return None
    y = int(m.group(1))
    return y + 2000 if y < 100 else y


# ── compile: lex → bind → spec ─────────────────────────────────────────────────
def compile_aggregate(phrase: str) -> dict[str, Any]:
    """phrase → {bindings, spec} or MapCompileError naming the refusing token."""
    if not isinstance(phrase, str) or not phrase.strip():
        raise MapCompileError("phrase refused: empty phrase")
    tokens = [t for t in re.sub(r"[,;:()\"'?!]", " ", phrase.lower()).split() if t]
    if tokens[:1] != ["total"]:
        raise MapCompileError("phrase refused: aggregate phrases open with 'total'")

    bindings: list[dict[str, Any]] = [
        {"tokens": ["total"], "axis": "mode", "op": None, "value": "aggregate"}]
    spec: dict[str, Any] = {"measure": None, "group_by": None,
                            "fy_lo": None, "fy_hi": None}
    i, n = 1, len(tokens)

    def _multiword(vocab: dict[str, str], max_len: int = 2) -> tuple[str, str] | None:
        for ln in range(min(max_len, n - i), 0, -1):
            span = " ".join(tokens[i:i + ln])
            if span in vocab:
                return span, vocab[span]
        return None

    while i < n:
        tok = tokens[i]

        hit = _multiword(MEASURES)
        if hit is not None:
            span, measure = hit
            if spec["measure"] is not None:
                raise MapCompileError("phrase refused: more than one measure")
            spec["measure"] = measure
            bindings.append({"tokens": span.split(), "axis": "measure",
                             "op": None, "value": measure})
            i += len(span.split())
            continue

        hit = _multiword(GROUPS)
        if hit is not None:
            span, group = hit
            if spec["group_by"] is not None:
                raise MapCompileError("phrase refused: more than one group axis")
            spec["group_by"] = group
            bindings.append({"tokens": span.split(), "axis": "group_by",
                             "op": None, "value": group})
            i += len(span.split())
            continue

        # window: "fy23 to fy25" (range) or a single "fy24".
        fy = _fy(tok)
        if fy is not None:
            if spec["fy_lo"] is not None:
                raise MapCompileError("phrase refused: more than one fiscal window")
            if not (_FY_MIN <= fy <= _FY_MAX):
                raise MapCompileError(f"phrase refused: token '{tok}' — fiscal year "
                                      f"outside {_FY_MIN}-{_FY_MAX}")
            span_toks = [tok]
            hi = fy
            if i + 2 < n and tokens[i + 1] == "to":
                fy2 = _fy(tokens[i + 2])
                if fy2 is None or not (_FY_MIN <= fy2 <= _FY_MAX):
                    raise MapCompileError(
                        f"phrase refused: token '{tokens[i + 2]}' — expected a "
                        "fiscal year like fy25 after 'to'")
                if fy2 < fy:
                    raise MapCompileError("phrase refused: fiscal window is reversed")
                hi = fy2
                span_toks = tokens[i:i + 3]
            spec["fy_lo"], spec["fy_hi"] = fy, hi
            bindings.append({"tokens": span_toks, "axis": "window",
                             "op": "between", "value": [fy, hi]})
            i += len(span_toks)
            continue

        # 'by <x>' / 'across <x>' with an unserved group axis: refuse naming
        # the axis and the served alternatives, not the connective.
        if tok in ("by", "across") and i + 1 < n:
            served = sorted({g.split(" ", 1)[1] for g in GROUPS})
            raise MapCompileError(
                f"phrase refused: token '{tok} {tokens[i + 1]}' — group axis "
                f"not served; v1 serves: {', '.join(served)}")

        if tok in CONNECTIVES:
            bindings.append({"tokens": [tok], "axis": "connective",
                             "op": None, "value": None})
            i += 1
            continue

        raise MapCompileError(
            f"phrase refused: token '{tok}' — not in the aggregate vocabulary")

    if spec["measure"] is None:
        raise MapCompileError("phrase refused: no measure — say 'total awarded …'")
    if spec["group_by"] is None:
        raise MapCompileError("phrase refused: no group axis — say '… by industry'")
    if spec["fy_lo"] is None:
        raise MapCompileError("phrase refused: no fiscal window — say '… fy23 to "
                              "fy25' (an unbounded aggregate is not served)")
    return {"bindings": bindings, "spec": spec}


# ── execute: one sidecar GROUP BY, sector rollup in-process ────────────────────
_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL_S = 3600.0


def _sector_sql(spec: dict[str, Any]) -> str:
    return (
        "SELECT substr(naics_code, 1, 2) AS sector2, "
        "sum(prime_obl) AS obl, sum(actions) AS actions "
        f"FROM v_combo_fy WHERE fy BETWEEN {int(spec['fy_lo'])} AND "
        f"{int(spec['fy_hi'])} AND naics_code IS NOT NULL "
        "GROUP BY 1")


def execute_aggregate(spec: dict[str, Any], phrase: str) -> dict[str, Any]:
    if not sidecar_executor.enabled():
        raise MapCompileError(
            "aggregate execution requires the query-sidecar (QUERY_SIDECAR_EXECUTE)")

    key = " ".join(phrase.lower().split())
    now = time.monotonic()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and now - hit["at"] < _CACHE_TTL_S:
            return hit["result"]

    payload = sidecar_executor._sql(_sector_sql(spec), limit=100)
    cols = payload["columns"]
    rolled: dict[str, dict[str, float]] = {}
    for row in payload["rows"]:
        r = dict(zip(cols, row))
        sector = _SECTOR_MERGE.get(r["sector2"], r["sector2"])
        if sector not in SECTOR_LABELS:
            continue  # non-sector residue (malformed codes) stays out of the chart
        agg = rolled.setdefault(sector, {"obl": 0.0, "actions": 0})
        agg["obl"] += r["obl"] or 0.0
        agg["actions"] += r["actions"] or 0
    bars = [{"key": k, "label": SECTOR_LABELS[k],
             "total": round(v["obl"], 2), "count": int(v["actions"])}
            for k, v in sorted(rolled.items(), key=lambda kv: -kv[1]["obl"])]
    result = {
        "bars": bars,
        "matched_rows": sum(b["count"] for b in bars),
        "total_groups": len(bars),
        "artifact": payload.get("artifact"),
        "elapsed_ms": payload.get("elapsed_ms"),
    }
    with _CACHE_LOCK:
        _CACHE[key] = {"at": now, "result": result}
    return result


def compile_and_execute(body: Any) -> dict[str, Any]:
    """The route entry for aggregate-mode phrases: validate, compile, execute,
    disclose. Response mirrors the retrieval envelope: meta (bindings + plan)
    + data — but data carries BARS, and meta.mode says so."""
    if not isinstance(body, dict):
        raise MapCompileError("request body must be an object")
    unknown = set(body) - {"phrase"}
    if unknown:
        raise MapCompileError(f"unknown body key(s) {sorted(unknown)!r}")
    phrase = body.get("phrase")
    compiled = compile_aggregate(phrase)
    spec = compiled["spec"]
    executed = execute_aggregate(spec, phrase)
    window = (f"FY{spec['fy_lo'] % 100:02d}" if spec["fy_lo"] == spec["fy_hi"]
              else f"FY{spec['fy_lo'] % 100:02d}–FY{spec['fy_hi'] % 100:02d}")
    return {
        "meta": {
            "compilerVersion": AGG_COMPILER_VERSION,
            "mode": "aggregate",
            "phrase": phrase,
            "bindings": compiled["bindings"],
            "plan": [{"grain": "aggregate", "source": "v_combo_fy",
                      "measure": spec["measure"], "group_by": spec["group_by"],
                      "fy": [spec["fy_lo"], spec["fy_hi"]]}],
            "title": f"Total awarded by industry · {window}",
            "unitLabel": "USD obligated",
            "matchedRows": executed["matched_rows"],
            "totalGroups": executed["total_groups"],
            "artifact": executed["artifact"],
            "elapsedMs": executed["elapsed_ms"],
            "refused": None,
        },
        "data": {"bars": executed["bars"]},
    }
