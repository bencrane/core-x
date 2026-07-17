"""market-spec — live market definition, executed against the audience spine.

The market-spec instrument (program record: hq/AUDIENCE_SPEC_FORM_PROGRAM.md,
reframed 2026-07-16 as "market-spec"): on a call, the operator fills a form that
defines a partner's market — geo, aggregate federal $, designations,
firmographics — and the count of entities fitting that definition renders live.
Every omitted section means "All". Contactability is NOT part of this surface
by operator ruling (2026-07-16): the quoted number is the market, never our
current contact-data coverage.

Endpoint (service-token gated):
  POST /api/v1/market-spec/count
    {"geo"?:            {"basis"?: "hq"|"pop" (default "hq"), "states": ["TX", …]},
     "dollars"?:        {"window"?: "12mo"|"24mo"|"60mo"|"lifetime" (default "24mo"),
                         "side"?: "total"|"prime"|"sub" (default "total" = sub+prime
                         combined), "min"?: number, "max"?: number},
     "designations"?:   ["dsbs_8a", "fsrs_any_designation", …]  (ANDed flags),
     "firmographics"?:  {"employee_bands": ["1-10", …]}}
  → {"count": N, "spec": <echo>, "elapsed_ms", "artifact"}

Execution: one COUNT(*) over `gtm_audience_entities` on the query-sidecar
(2.03M-row entity spine, single table, no joins — ~50 ms). Deterministic:
every token validated against a frozen server-side vocabulary; unknown tokens
refuse with the token named. All SQL fragments come from server-side maps —
user input is never interpolated.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/market-spec", tags=["market-spec"])

SIDECAR_URL = os.environ.get("QUERY_SIDECAR_URL", "https://query-sidecar-api.onrender.com")

_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
    "GU", "VI", "AS", "MP",
}

# geo basis → the spine's state column. "hq" = where the company is based
# (SAM physical address); "pop" = where its federal work is performed
# (dominant place-of-performance state).
_GEO_BASIS_COL = {"hq": "physical_state", "pop": "primary_pop_state"}

# (side, window) → spine column. "total" = sub+prime combined (the default —
# the operator never adds columns up by hand). "36mo" intentionally absent:
# the source mart lacks it until the next audience-mart rebuild.
_DOLLAR_COL = {
    ("total", "12mo"): "total_amt_12mo",
    ("total", "24mo"): "total_amt_24mo",
    ("total", "60mo"): "total_amt_60mo",
    ("total", "lifetime"): "total_amt_lifetime",
    ("prime", "12mo"): "prime_obl_12mo",
    ("prime", "24mo"): "prime_obl_24mo",
    ("prime", "60mo"): "prime_obl_60mo",
    ("prime", "lifetime"): "prime_obl_lifetime",
    ("sub", "12mo"): "sub_amt_12mo",
    ("sub", "24mo"): "sub_amt_24mo",
    ("sub", "60mo"): "sub_amt_60mo",
    ("sub", "lifetime"): "sub_amt_lifetime",
}

# Designation tokens → spine BOOLEAN columns. Multiple tokens AND together.
_DESIGNATION_COLS = {
    "dsbs_8a": "dsbs_8a",
    "dsbs_hubzone": "dsbs_hubzone",
    "dsbs_wosb": "dsbs_wosb",
    "dsbs_edwosb": "dsbs_edwosb",
    "dsbs_sdvosb": "dsbs_sdvosb",
    "dsbs_vosb": "dsbs_vosb",
    "fsrs_sdvosb": "fsrs_sdvosb",
    "fsrs_vosb": "fsrs_vosb",
    "fsrs_wosb": "fsrs_wosb",
    "fsrs_edwosb": "fsrs_edwosb",
    "fsrs_woman_owned": "fsrs_woman_owned",
    "fsrs_hubzone": "fsrs_hubzone",
    "fsrs_8a": "fsrs_8a",
    "fsrs_sdb": "fsrs_sdb",
    "fsrs_minority": "fsrs_minority",
    "fsrs_any_designation": "fsrs_any_designation",
    "in_dsbs": "in_dsbs",
}

# The spine's actual employee_size_band values (verified live 2026-07-16).
# NULL band (~87% of the spine) is excluded whenever bands are specified —
# a band filter is an affirmative criterion on known size.
_EMPLOYEE_BANDS = {
    "1-10", "11-50", "51-200", "201-500", "501-1000",
    "1001-5000", "5001-10000", "10001+",
}


def _refuse(detail: str) -> HTTPException:
    return HTTPException(status_code=422, detail=detail)


def compile_spec(body: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Compile a market-spec body into (WHERE clause, echo). Refuses unknown tokens.

    The predicate is the spec's single source of truth — count today, stats and
    materialization later all share it.
    """
    preds: list[str] = []
    echo: dict[str, Any] = {}

    geo = body.get("geo")
    if geo is not None:
        if not isinstance(geo, dict):
            raise _refuse("geo must be an object")
        basis = geo.get("basis", "hq")
        col = _GEO_BASIS_COL.get(basis)
        if col is None:
            raise _refuse(f"unknown geo basis '{basis}' (hq | pop)")
        states_in = geo.get("states")
        if not isinstance(states_in, list) or not states_in:
            raise _refuse("geo.states must be a non-empty list (omit geo entirely for All)")
        states = []
        for s in states_in:
            code = str(s).strip().upper()
            if code not in _US_STATES:
                raise _refuse(f"unknown state '{s}'")
            states.append(code)
        in_list = ",".join(f"'{s}'" for s in sorted(set(states)))
        preds.append(f"{col} IN ({in_list})")
        echo["geo"] = {"basis": basis, "states": sorted(set(states))}

    dollars = body.get("dollars")
    if dollars is not None:
        if not isinstance(dollars, dict):
            raise _refuse("dollars must be an object")
        side = dollars.get("side", "total")
        window = dollars.get("window", "24mo")
        col = _DOLLAR_COL.get((side, window))
        if col is None:
            raise _refuse(
                f"unknown dollars side/window '{side}'/'{window}' "
                "(side: total|prime|sub · window: 12mo|24mo|60mo|lifetime)"
            )
        d_min = dollars.get("min")
        d_max = dollars.get("max")
        if d_min is None and d_max is None:
            raise _refuse("dollars needs min and/or max (omit dollars entirely for All)")
        for name, v in (("min", d_min), ("max", d_max)):
            if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0):
                raise _refuse(f"dollars.{name} must be a non-negative number")
        if d_min is not None:
            preds.append(f"COALESCE({col}, 0) >= {float(d_min)}")
        if d_max is not None:
            preds.append(f"COALESCE({col}, 0) <= {float(d_max)}")
        echo["dollars"] = {"side": side, "window": window, "min": d_min, "max": d_max}

    designations = body.get("designations")
    if designations is not None:
        if not isinstance(designations, list) or not designations:
            raise _refuse("designations must be a non-empty list (omit for All)")
        seen = []
        for token in designations:
            col = _DESIGNATION_COLS.get(str(token))
            if col is None:
                raise _refuse(f"unknown designation '{token}'")
            preds.append(f"{col} = TRUE")
            seen.append(str(token))
        echo["designations"] = seen

    firmo = body.get("firmographics")
    if firmo is not None:
        if not isinstance(firmo, dict):
            raise _refuse("firmographics must be an object")
        bands = firmo.get("employee_bands")
        if not isinstance(bands, list) or not bands:
            raise _refuse("firmographics.employee_bands must be a non-empty list (omit for All)")
        for b in bands:
            if str(b) not in _EMPLOYEE_BANDS:
                raise _refuse(f"unknown employee band '{b}'")
        in_list = ",".join(f"'{b}'" for b in sorted(set(str(b) for b in bands)))
        preds.append(f"employee_size_band IN ({in_list})")
        echo["firmographics"] = {"employee_bands": sorted(set(str(b) for b in bands))}

    where = (" WHERE " + " AND ".join(preds)) if preds else ""
    return where, echo


@router.post("/count", dependencies=[Depends(require_service_token)])
async def market_spec_count(body: dict[str, Any]) -> dict[str, Any]:
    where, echo = compile_spec(body if isinstance(body, dict) else {})
    sql = f"SELECT COUNT(*) AS n FROM gtm_audience_entities{where}"
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="QUERY_SIDECAR_TOKEN not configured")
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{SIDECAR_URL}/api/v1/sql",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql, "limit": 1},
        )
    if r.status_code != 200:
        logger.error("sidecar market-spec count failed: %s %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail="sidecar query failed")
    payload = r.json()
    count = payload["rows"][0][0] if payload.get("rows") else 0
    return {
        "count": count,
        "spec": echo,
        "elapsed_ms": payload.get("elapsed_ms"),
        "artifact": payload.get("artifact"),
    }
