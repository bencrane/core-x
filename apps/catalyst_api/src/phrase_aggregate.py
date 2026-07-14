"""Deterministic AGGREGATE phrase mode — phrase-agg.v2 (POST /api/v1/market/phrase).

The retrieval grammar (phrase_compiler) answers "WHICH rows match"; this module
answers "HOW MUCH, grouped by what" — the demo/portrait surface. Same doctrine,
separate closed grammar: the opener token ``total`` routes the phrase here
(phrase_compiler.compile_and_execute delegates before the retrieval lex), every
span longest-matches a closed vocabulary, and any unbound token refuses the
whole phrase naming the token. Zero LLM. Same phrase + artifact → same bars.

v3 GRAMMAR — exactly three productions, everything else refuses:

    total <measure> <group> <window>                            (v1 · portrait)
    total active <measure> near <zip5> within <N> miles by equipment   (v2 · yard)
    total active labor for <role text> by combo|industry        (v3 · labor)

    measure  awarded | awards | award value | obligated | obligations | spend
             v1 → sum(prime_obl) over the combo×FY portrait
             v2 → sum(total_obligation) over open awards
             labor (v3) → sum(active_obligated × category_award_share): the
             expected-labor slice of active award dollars
    group    by industry | across industries       (v1/v3: NAICS sector rollup)
             by equipment | across equipment       (v2: equipment-need bucket)
             by combo | across combos              (v3: NAICS×PSC combo)
    window   fy23 to fy25 | fy2023 to fy2025 | fy24    (v1 REQUIRED; v2/v3 refused —
             active awards are point-in-time, not FY-windowed)
    active   v2/v3 scope: open/active awards, point-in-time
    near     v2 anchor: 5-digit zip → award-PoP centroid (refuses unknown zips)
    within   v2 radius: '<N> miles' haversine from the anchor (REQUIRED)
    equipment scope: the curated NAICS×PSC heavy-iron combos
             (naics_psc_equipment_needs WHERE in_scope), bucketed
    for      v3 role span: every token after 'for' until the group axis is the
             free-text role name; it resolves against occupation_alias_lookup
             at execute (exact normalized alias, then a depluralized retry) to
             ONE code — in_combo_layer first, then source_priority, then code —
             and an unresolvable role refuses WITH deterministic suggestions
             (the same closed-world doctrine as v2's unknown-zip refusal)
    connectives: from, in, of, the, federal, $  — consumed, disclosed, never semantic

Execution: v1 is ONE GROUP BY on the sidecar's ``v_combo_fy``; v2 is a centroid
point-lookup + ONE bbox/haversine GROUP BY on ``gtm_open_awards`` joined to the
equipment-combo verdicts; v3 is an alias point-resolution + ONE GROUP BY on
``v_role_priced_combos`` ⋈ ``combo_award_active_state`` — category_award_share
already bakes the pct_of_industry/100, so the labor slice is a pure multiply.
Results are cached in-process keyed by the normalized phrase — the response is
deterministic w.r.t. the artifact, and the artifact stamp is carried in the
response meta so a stale cache is visible, never silent.
"""
from __future__ import annotations

import math
import re
import threading
import time
from typing import Any

from .lance_store import MapCompileError
from . import sidecar_executor

AGG_COMPILER_VERSION = "phrase-agg.v3"

# ── closed vocabularies ────────────────────────────────────────────────────────
MEASURES: dict[str, str] = {
    "awarded": "prime_obl_sum",
    "awards": "prime_obl_sum",
    "award value": "prime_obl_sum",
    "obligated": "prime_obl_sum",
    "obligations": "prime_obl_sum",
    "spend": "prime_obl_sum",
    "labor": "labor_expected_sum",
}
GROUPS: dict[str, str] = {
    "by industry": "industry",
    "across industries": "industry",
    "by equipment": "equipment",
    "across equipment": "equipment",
    "by combo": "combo",
    "across combos": "combo",
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
                            "active": False, "zip": None, "radius_mi": None,
                            "role": None}
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

        # v3 role span: 'for <role text…>' — consume every token up to the
        # group axis ('by'/'across'). The span is free text HERE; it binds
        # against the closed alias world at execute (unknown role refuses).
        if tok == "for":
            if spec["role"] is not None:
                raise MapCompileError("phrase refused: more than one role span")
            j = i + 1
            role_toks: list[str] = []
            # the span stops at every reserved axis token so misplaced dials
            # (near/within/fy) bind — and refuse — on their own axis
            while j < n and tokens[j] not in ("by", "across", "near", "within") \
                    and _fy(tokens[j]) is None:
                role_toks.append(tokens[j])
                j += 1
            if not role_toks:
                raise MapCompileError(
                    "phrase refused: 'for' expects a role name — say "
                    "'for registered nurses'")
            spec["role"] = " ".join(role_toks)
            bindings.append({"tokens": tokens[i:j], "axis": "role",
                             "op": "alias_resolve", "value": spec["role"]})
            i = j
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

    if spec["measure"] == "labor_expected_sum":
        # v3 · labor production: active + for <role> + by combo|industry.
        if not spec["active"]:
            raise MapCompileError(
                "phrase refused: labor serves active awards — open with "
                "'total active labor …'")
        if spec["role"] is None:
            raise MapCompileError(
                "phrase refused: labor needs a role — say 'for registered nurses'")
        if spec["fy_lo"] is not None:
            raise MapCompileError(
                "phrase refused: active awards are point-in-time — drop the "
                "fiscal window")
        if spec["zip"] is not None or spec["radius_mi"] is not None:
            raise MapCompileError(
                "phrase refused: labor mode is not geo-anchored — drop "
                "'near'/'within'")
        if spec["group_by"] not in ("combo", "industry"):
            raise MapCompileError(
                "phrase refused: labor mode serves 'by combo' or 'by industry'")
    elif spec["role"] is not None:
        raise MapCompileError(
            "phrase refused: 'for <role>' requires the labor measure — say "
            "'total active labor for …'")
    elif spec["active"]:
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
        if spec["group_by"] == "combo":
            raise MapCompileError(
                "phrase refused: 'by combo' serves the labor production — open "
                "with 'total active labor for <role> …'")
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


_ROLE_NORM_RE = re.compile(r"[^a-z0-9]+")


def _role_norm(role: str) -> str:
    """Mirror occupation_alias_lookup.alias_norm: lowercase, punctuation →
    space, collapsed. Output is [a-z0-9 ] only — safe to inline in SQL."""
    return _ROLE_NORM_RE.sub(" ", role.lower()).strip()


def _role_candidates(norm: str) -> list[str]:
    """Deterministic match ladder: exact, then last-token depluralized, then
    every token depluralized ('registered nurses' → 'registered nurse')."""
    cands = [norm]
    toks = norm.split()
    if toks and toks[-1].endswith("s"):
        cands.append(" ".join(toks[:-1] + [toks[-1][:-1]]))
    allsing = " ".join(t[:-1] if t.endswith("s") else t for t in toks)
    if allsing not in cands:
        cands.append(allsing)
    return cands


def _resolve_role_sql(norm: str) -> str:
    return (
        "SELECT alias_norm, code_type, code, occupation_title "
        f"FROM occupation_alias_lookup WHERE alias_norm = '{norm}' "
        "ORDER BY in_combo_layer DESC, source_priority ASC, code ASC LIMIT 1")


def _role_suggest_sql(norm: str) -> str:
    # suggestion probe for the refusal message: longest token, prefix-ish LIKE.
    tok = max(norm.split(), key=len)
    return (
        "SELECT alias_norm, count(*) AS n FROM occupation_alias_lookup "
        f"WHERE alias_norm LIKE '%{tok}%' AND in_combo_layer "
        "GROUP BY 1 ORDER BY length(alias_norm) ASC, alias_norm ASC LIMIT 5")


def _labor_sql(code_type: str, code: str, norm: str, group_by: str) -> str:
    # category_award_share already bakes pct_of_industry/100; the labor slice
    # of active dollars is a pure multiply. Pinning (alias_norm, code_type,
    # code) selects exactly one alias row per combo — no soc/sca double count.
    dims = ("substr(v.naics_code, 1, 2) AS sector2"
            if group_by == "industry"
            else "v.naics_code, v.psc_code")
    group = "1" if group_by == "industry" else "1, 2"
    return (
        f"SELECT {dims}, "
        "sum(c.active_obligated * v.category_award_share) AS expected_labor, "
        "sum(c.active_obligated) AS active_obl, "
        "sum(c.active_award_ct) AS awards "
        "FROM v_role_priced_combos v "
        "JOIN combo_award_active_state c "
        "ON c.naics_code = v.naics_code AND c.psc_code = v.psc_code "
        f"WHERE v.alias_norm = '{norm}' AND v.code_type = '{code_type}' "
        f"AND v.code = '{code}' AND v.category_award_share IS NOT NULL "
        f"GROUP BY {group}")


def _execute_labor(spec: dict[str, Any]) -> dict[str, Any]:
    norm = _role_norm(spec["role"])
    if not norm:
        raise MapCompileError("phrase refused: empty role after normalization")
    resolved = None
    for cand in _role_candidates(norm):
        payload = sidecar_executor._sql(_resolve_role_sql(cand), limit=1)
        if payload["rows"]:
            resolved = dict(zip(payload["columns"], payload["rows"][0]))
            break
    if resolved is None:
        sug = sidecar_executor._sql(_role_suggest_sql(norm), limit=5)
        names = [r[0] for r in sug["rows"]]
        hint = f" — nearest served roles: {', '.join(names)}" if names else ""
        raise MapCompileError(
            f"phrase refused: role '{spec['role']}' — no alias on record{hint}")

    payload = sidecar_executor._sql(
        _labor_sql(resolved["code_type"], resolved["code"],
                   resolved["alias_norm"], spec["group_by"]), limit=500)
    cols = payload["columns"]
    bars = []
    if spec["group_by"] == "industry":
        rolled: dict[str, dict[str, float]] = {}
        for row in payload["rows"]:
            r = dict(zip(cols, row))
            sector = _SECTOR_MERGE.get(r["sector2"], r["sector2"])
            if sector not in SECTOR_LABELS:
                continue
            agg = rolled.setdefault(sector, {"labor": 0.0, "awards": 0})
            agg["labor"] += r["expected_labor"] or 0.0
            agg["awards"] += r["awards"] or 0
        bars = [{"key": k, "label": SECTOR_LABELS[k],
                 "total": round(v["labor"], 2), "count": int(v["awards"])}
                for k, v in sorted(rolled.items(), key=lambda kv: -kv[1]["labor"])]
    else:
        for row in payload["rows"]:
            r = dict(zip(cols, row))
            if r["expected_labor"] is None:
                continue
            key = f"{r['naics_code']}×{r['psc_code']}"
            bars.append({"key": key, "label": key,
                         "total": round(r["expected_labor"], 2),
                         "count": int(r["awards"] or 0)})
        bars.sort(key=lambda b: -b["total"])
    return {
        "bars": bars,
        "matched_rows": sum(b["count"] for b in bars),
        "total_groups": len(bars),
        "role": {"input": spec["role"], "alias_norm": resolved["alias_norm"],
                 "code_type": resolved["code_type"], "code": resolved["code"],
                 "occupation_title": resolved["occupation_title"]},
        "artifact": payload.get("artifact"),
        "elapsed_ms": payload.get("elapsed_ms"),
    }


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

    if spec["measure"] == "labor_expected_sum":
        result = _execute_labor(spec)
        with _CACHE_LOCK:
            _CACHE[key] = {"at": now, "result": result}
        return result

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
    if spec["measure"] == "labor_expected_sum":
        role = executed["role"]
        plan = [{"grain": "aggregate",
                 "source": "v_role_priced_combos×combo_award_active_state",
                 "measure": "labor_expected_sum", "group_by": spec["group_by"],
                 "fy": None, "role": role}]
        title = (f"Expected labor $ in active awards · "
                 f"{role['occupation_title']} ({role['code']})")
    elif spec["active"]:
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
            "unitLabel": ("USD expected labor"
                          if spec["measure"] == "labor_expected_sum"
                          else "USD obligated"),
            "matchedRows": executed["matched_rows"],
            "totalGroups": executed["total_groups"],
            "artifact": executed["artifact"],
            "elapsedMs": executed["elapsed_ms"],
            "refused": None,
        },
        "data": {"bars": executed["bars"]},
    }
