"""Deterministic AGGREGATE phrase mode — phrase-agg.v2 (POST /api/v1/market/phrase).

The retrieval grammar (phrase_compiler) answers "WHICH rows match"; this module
answers "HOW MUCH, grouped by what" — the demo/portrait surface. Same doctrine,
separate closed grammar: the opener token ``total`` routes the phrase here
(phrase_compiler.compile_and_execute delegates before the retrieval lex), every
span longest-matches a closed vocabulary, and any unbound token refuses the
whole phrase naming the token. Zero LLM. Same phrase + artifact → same bars.

v2 GRAMMAR — exactly two productions, everything else refuses:

    total <measure> <group> <window>                            (v1 · portrait)
    total active <measure> near <zip5> within <N> miles by equipment   (v2 · yard)

    measure  awarded | awards | award value | obligated | obligations | spend
             v1 → sum(prime_obl) over the combo×FY portrait
             v2 → sum(total_obligation) over open awards
    group    by industry | across industries       (v1: NAICS sector rollup)
             by equipment | across equipment       (v2: equipment-need bucket)
    window   fy23 to fy25 | fy2023 to fy2025 | fy24    (v1 REQUIRED; v2 refused —
             active awards are point-in-time, not FY-windowed)
    active   v2 scope: open awards (gtm_open_awards), place-of-performance
    near     v2 anchor: 5-digit zip → award-PoP centroid (refuses unknown zips)
    within   v2 radius: '<N> miles' haversine from the anchor (REQUIRED)
    equipment scope: the curated NAICS×PSC heavy-iron combos
             (naics_psc_equipment_needs WHERE in_scope), bucketed
    connectives: from, in, of, the, federal, $  — consumed, disclosed, never semantic

Execution: v1 is ONE GROUP BY on the sidecar's ``v_combo_fy``; v2 is a centroid
point-lookup + ONE bbox/haversine GROUP BY on ``gtm_open_awards`` joined to the
equipment-combo verdicts. Results are cached in-process keyed by the normalized
phrase — the response is deterministic w.r.t. the artifact, and the artifact
stamp is carried in the response meta so a stale cache is visible, never silent.
"""
from __future__ import annotations

import math
import re
import threading
import time
from typing import Any

from .lance_store import MapCompileError
from . import sidecar_executor

AGG_COMPILER_VERSION = "phrase-agg.v2"

# ── closed vocabularies ────────────────────────────────────────────────────────
MEASURES: dict[str, str] = {
    "awarded": "prime_obl_sum",
    "awards": "prime_obl_sum",
    "award value": "prime_obl_sum",
    "obligated": "prime_obl_sum",
    "obligations": "prime_obl_sum",
    "spend": "prime_obl_sum",
}
GROUPS: dict[str, str] = {
    "by industry": "industry",
    "across industries": "industry",
    "by equipment": "equipment",
    "across equipment": "equipment",
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

_ZIP_RE = re.compile(r"^\d{5}$")
_RADIUS_RE = re.compile(r"^\d{1,3}(\.\d+)?$")
_RADIUS_MIN, _RADIUS_MAX = 1.0, 500.0
_MILE_TOKENS = {"miles", "mile", "mi"}


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
                            "fy_lo": None, "fy_hi": None,
                            "active": False, "zip": None, "radius_mi": None}
    i, n = 1, len(tokens)

    def _multiword(vocab: dict[str, str], max_len: int = 2) -> tuple[str, str] | None:
        for ln in range(min(max_len, n - i), 0, -1):
            span = " ".join(tokens[i:i + ln])
            if span in vocab:
                return span, vocab[span]
        return None

    while i < n:
        tok = tokens[i]

        # v2 scope: 'active' — open awards, point-in-time.
        if tok == "active":
            if spec["active"]:
                raise MapCompileError("phrase refused: 'active' bound twice")
            spec["active"] = True
            bindings.append({"tokens": ["active"], "axis": "scope",
                             "op": None, "value": "open_awards"})
            i += 1
            continue

        # v2 anchor: 'near <zip5>'.
        if tok == "near":
            if i + 1 >= n or not _ZIP_RE.match(tokens[i + 1]):
                raise MapCompileError(
                    "phrase refused: 'near' expects a 5-digit zip — say 'near 79925'")
            if spec["zip"] is not None:
                raise MapCompileError("phrase refused: more than one anchor zip")
            spec["zip"] = tokens[i + 1]
            bindings.append({"tokens": tokens[i:i + 2], "axis": "anchor",
                             "op": "zip_centroid", "value": spec["zip"]})
            i += 2
            continue

        # v2 radius: 'within <N> miles'.
        if tok == "within":
            if (i + 2 >= n or not _RADIUS_RE.match(tokens[i + 1])
                    or tokens[i + 2] not in _MILE_TOKENS):
                raise MapCompileError(
                    "phrase refused: 'within' expects '<N> miles' — say 'within 50 miles'")
            radius = float(tokens[i + 1])
            if not (_RADIUS_MIN <= radius <= _RADIUS_MAX):
                raise MapCompileError(
                    f"phrase refused: radius {tokens[i + 1]} outside "
                    f"{_RADIUS_MIN:g}-{_RADIUS_MAX:g} miles")
            if spec["radius_mi"] is not None:
                raise MapCompileError("phrase refused: more than one radius")
            spec["radius_mi"] = radius
            bindings.append({"tokens": tokens[i:i + 3], "axis": "radius",
                             "op": "haversine_mi", "value": radius})
            i += 3
            continue

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
        raise MapCompileError("phrase refused: no group axis — say '… by industry' "
                              "or (active) '… by equipment'")

    if spec["active"]:
        # v2 · yard production: active + near + within + by equipment; no window.
        if spec["fy_lo"] is not None:
            raise MapCompileError(
                "phrase refused: active awards are point-in-time — drop the "
                "fiscal window")
        if spec["zip"] is None:
            raise MapCompileError(
                "phrase refused: active mode needs an anchor — say 'near <zip5>'")
        if spec["radius_mi"] is None:
            raise MapCompileError(
                "phrase refused: active mode needs a radius — say 'within 50 miles'")
        if spec["group_by"] != "equipment":
            raise MapCompileError(
                "phrase refused: active mode serves 'by equipment' only")
    else:
        # v1 · portrait production: measure + industry + window; no geo.
        if spec["zip"] is not None or spec["radius_mi"] is not None:
            raise MapCompileError(
                "phrase refused: 'near'/'within' require active scope — open "
                "with 'total active …'")
        if spec["group_by"] == "equipment":
            raise MapCompileError(
                "phrase refused: 'by equipment' requires active scope — open "
                "with 'total active …'")
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


def _centroid_sql(zip5: str) -> str:
    # zip5 is regex-pinned to \d{5} at compile — safe to inline.
    return ("SELECT count(*) AS pops, avg(latitude) AS lat, avg(longitude) AS lon "
            f"FROM usaspending_award_pop_centroids WHERE zip5 = '{zip5}'")


def _local_equipment_sql(lat: float, lon: float, radius_mi: float) -> str:
    # Same anchor math as the equipment-yard deck: bbox prefilter (index-friendly)
    # then exact haversine. Scope = curated heavy-iron NAICS×PSC combo verdicts.
    dlat = radius_mi / 68.97
    dlon = radius_mi / (69.17 * math.cos(math.radians(lat)))
    hav = (f"3958.8*2*asin(sqrt(pow(sin(radians(latitude-{lat})/2),2)"
           f"+cos(radians({lat}))*cos(radians(latitude))"
           f"*pow(sin(radians(longitude-({lon}))/2),2)))")
    return (
        "SELECT e.primary_bucket AS bucket, count(*) AS awards, "
        "sum(a.total_obligation) AS obl "
        "FROM gtm_open_awards a "
        "JOIN naics_psc_equipment_needs e ON a.naics_code = e.naics_code "
        "AND a.product_or_service_code = e.psc_code AND e.in_scope "
        f"WHERE a.latitude BETWEEN {lat - dlat:.3f} AND {lat + dlat:.3f} "
        f"AND a.longitude BETWEEN {lon - dlon:.3f} AND {lon + dlon:.3f} "
        f"AND {hav} <= {radius_mi:g} "
        "GROUP BY 1")


def _bucket_label(bucket: str) -> str:
    return bucket.replace("_", " ").title().replace(" And ", " & ")


def _execute_active_equipment(spec: dict[str, Any]) -> dict[str, Any]:
    cent = sidecar_executor._sql(_centroid_sql(spec["zip"]), limit=1)
    crow = dict(zip(cent["columns"], cent["rows"][0])) if cent["rows"] else {}
    if not crow.get("pops"):
        raise MapCompileError(
            f"phrase refused: zip {spec['zip']} — no award place-of-performance "
            "centroid on record")
    lat, lon = float(crow["lat"]), float(crow["lon"])

    payload = sidecar_executor._sql(
        _local_equipment_sql(lat, lon, float(spec["radius_mi"])), limit=100)
    cols = payload["columns"]
    bars = []
    for row in payload["rows"]:
        r = dict(zip(cols, row))
        if not r["bucket"]:
            continue
        bars.append({"key": r["bucket"], "label": _bucket_label(r["bucket"]),
                     "total": round(r["obl"] or 0.0, 2), "count": int(r["awards"])})
    bars.sort(key=lambda b: -b["total"])
    return {
        "bars": bars,
        "matched_rows": sum(b["count"] for b in bars),
        "total_groups": len(bars),
        "anchor": {"zip": spec["zip"], "lat": round(lat, 5), "lon": round(lon, 5),
                   "radius_mi": float(spec["radius_mi"])},
        "artifact": payload.get("artifact"),
        "elapsed_ms": payload.get("elapsed_ms"),
    }


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

    if spec["active"]:
        result = _execute_active_equipment(spec)
        with _CACHE_LOCK:
            _CACHE[key] = {"at": now, "result": result}
        return result

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
    if spec["active"]:
        plan = [{"grain": "aggregate",
                 "source": "gtm_open_awards×naics_psc_equipment_needs",
                 "measure": "active_obl_sum", "group_by": spec["group_by"],
                 "fy": None, "anchor": executed["anchor"]}]
        title = (f"Active equipment-scope awards · {spec['zip']} · "
                 f"{spec['radius_mi']:g} mi")
    else:
        window = (f"FY{spec['fy_lo'] % 100:02d}" if spec["fy_lo"] == spec["fy_hi"]
                  else f"FY{spec['fy_lo'] % 100:02d}–FY{spec['fy_hi'] % 100:02d}")
        plan = [{"grain": "aggregate", "source": "v_combo_fy",
                 "measure": spec["measure"], "group_by": spec["group_by"],
                 "fy": [spec["fy_lo"], spec["fy_hi"]]}]
        title = f"Total awarded by industry · {window}"
    return {
        "meta": {
            "compilerVersion": AGG_COMPILER_VERSION,
            "mode": "aggregate",
            "phrase": phrase,
            "bindings": compiled["bindings"],
            "plan": plan,
            "title": title,
            "unitLabel": "USD obligated",
            "matchedRows": executed["matched_rows"],
            "totalGroups": executed["total_groups"],
            "artifact": executed["artifact"],
            "elapsedMs": executed["elapsed_ms"],
            "refused": None,
        },
        "data": {"bars": executed["bars"]},
    }
