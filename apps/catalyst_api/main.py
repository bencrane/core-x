"""catalyst_api entrypoint — Gen-3 read API: domain → US federal award profile.

A lightweight FastAPI gateway over the committed R2 Lance sink. One headline
capability: resolve a web domain to its federal contracting profile via native
``BTREE`` point-lookups (``firmographics_blitz.domain_norm`` → ``uei`` →
``contractor_award_summary.recipient_uei``), with prime award line items from
``usaspending/award_search`` available as an opt-in detail.

Run (locally and on Railway, from the repo root):

    python -m apps.catalyst_api.main

Deployed as a standalone **core-x** Railway service (its own ``catalyst-api``
project — not nested in any product). It is a shared gateway, so it is reachable
on a public Railway domain and every ``/api/v1`` route is gated by an operator
bearer token (``CATALYST_API_TOKEN``) that each consuming BFF presents as Bearer.
The token — not network isolation — is the auth boundary: boot is fail-closed, so
an unset token in a deployed env refuses to start (see ``config.auth_required``).
``/healthz`` and ``/`` stay open for liveness probes.
"""

from __future__ import annotations

import hmac
import logging
from contextlib import asynccontextmanager

from datetime import date

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

from .src import config, lance_store
from .src.map_decoders import DECODERS
from .src.models import (
    ActiveContract,
    ActiveContractsResponse,
    AwardProfile,
    AwardProfileResponse,
    Company,
    MapQueryRequest,
    OverviewResponse,
    PastPerformanceResponse,
    RecentAward,
    SamProfileResponse,
)

log = logging.getLogger("catalyst_api")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Warm + fail-fast surface: confirm R2 credentials open the anchor manifest at
    # boot rather than mid-request. A failure is logged (not fatal) so /healthz can
    # still report the degraded state.
    if config.operator_token() is None:
        # Fail-closed in any non-local deploy: the private gateway must never serve
        # the R2 sink unauthenticated. Local dev (no deploy markers) still warns + allows.
        if config.auth_required():
            raise RuntimeError(
                "CATALYST_API_TOKEN is unset in a deployed environment — refusing to "
                "start an unauthenticated gateway. Set the token (Doppler core-x/prd)."
            )
        log.warning("CATALYST_API_TOKEN unset — /api/v1 routes are UNAUTHENTICATED (local dev only).")
    try:
        ok = lance_store.reachable()
        log.info("catalyst_api: R2 anchor dataset reachable=%s", ok)
    except Exception as exc:  # noqa: BLE001 — boot probe is best-effort
        log.warning("catalyst_api: R2 anchor probe failed at boot: %s", exc)
    # Surface reachability map — a wrong/unmaterialized URI is LOUD here at boot instead of
    # silently 404-ing every request (the failure mode that masked a misrouted SAM URI).
    try:
        surfaces = lance_store.probe_surfaces()
        log.info("catalyst_api: surface datasets reachable=%s", surfaces)
        unreachable = [n for n, ok in surfaces.items() if not ok]
        if unreachable:
            log.warning("catalyst_api: UNREACHABLE surface datasets (check *_LANCE_URI): %s", unreachable)
    except Exception as exc:  # noqa: BLE001 — best-effort
        log.warning("catalyst_api: surface probe failed at boot: %s", exc)
    # Decoder schema/index contract check (R-09): assert every FieldSpec.column + geometry
    # column exists in the live schema and every DECLARED index exists. Drift is LOUD here at
    # boot and surfaced on /healthz. Default is observe-only (log + /healthz 503, boot
    # proceeds) so a false-positive can never brick EXECUTE; CATALYST_CONTRACT_STRICT promotes
    # a real violation to a fatal boot abort. The whole block is best-effort so a checker bug
    # cannot brick boot in non-strict mode.
    contract_report: dict[str, dict[str, list[str]]] = {}
    try:
        contract_report = lance_store.check_decoder_contracts()
        hard = {k: f["violations"] for k, f in contract_report.items() if f.get("violations")}
        notes = {k: f["notes"] for k, f in contract_report.items() if f.get("notes")}
        if notes:  # degraded-not-broken — surfaced but never aborts boot
            log.warning("catalyst_api: decoder contract NOTES (non-fatal): %s", notes)
        if hard:
            log.error("catalyst_api: DECODER CONTRACT DRIFT (R-09): %s", hard)
            if config.contract_check_strict():
                raise RuntimeError(f"decoder schema/index contract violated at boot: {hard}")
        else:
            log.info("catalyst_api: decoder contracts OK for %s", list(contract_report))
    except RuntimeError:
        raise  # strict-mode abort — fail the deploy (mirror the auth fail-closed path)
    except Exception as exc:  # noqa: BLE001 — checker bug must never brick boot (non-strict)
        log.warning("catalyst_api: contract check failed to run at boot: %s", exc)
    _app.state.contract_report = contract_report
    yield


app = FastAPI(title="catalyst_api", version="1.0.0", lifespan=lifespan)


# ── Operator service-token gate (BFF → catalyst_api) ─────────────────────────
def require_operator(authorization: str | None = Header(default=None)) -> None:
    """Validate ``Authorization: Bearer <CATALYST_API_TOKEN>`` in constant time.
    Unset token (local dev) allows; production sets it, so enforcement is live."""
    expected = config.operator_token()
    if expected is None:
        return
    provided = ""
    if authorization and authorization.lower().startswith("bearer "):
        provided = authorization[len("bearer ") :].strip()
    if not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="unauthorized", headers={"WWW-Authenticate": "Bearer"})


def _info() -> dict:
    return {
        "service": "catalyst_api",
        "status": "ok",
        "endpoints": {
            "award_profile": "/api/v1/award-profile/{domain}",
            "award_profile_with_awards": "/api/v1/award-profile/{domain}?awards=N",
            "sam_profile": "/api/v1/entities/{uei}/sam-profile",
            "active_contracts": "/api/v1/entities/{uei}/active-contracts?limit=N",
            "overview": "/api/v1/entities/{uei}/overview",
            "past_performance": "/api/v1/entities/{uei}/past-performance?limit=N",
            "map_query": "/api/v1/map/{dataset}/query  (POST: {filters:[{field,op,value}]})",
        },
        "map_datasets": list(DECODERS),
    }


def _require_uei(uei: str) -> str:
    """Trim + charset-validate the path UEI before it reaches a Lance filter. The
    BFF resolves it from the trusted session, but the gateway validates regardless."""
    uei = (uei or "").strip()
    if not lance_store.valid_uei(uei):
        raise HTTPException(status_code=400, detail="invalid uei")
    return uei


def _envelope(model) -> JSONResponse:
    """The wire envelope shared with the award-profile route: ``{"data": <camelCase>}``."""
    return JSONResponse({"data": model.model_dump(by_alias=True, exclude_none=False)})


@app.get("/")
def root() -> dict:
    return _info()


@app.get("/healthz")
def healthz(request: Request) -> JSONResponse:
    """Liveness + R2 reachability + decoder contract status. Open (no token) for platform
    probes. A 503 reflects EITHER an unreachable R2 anchor OR a HARD decoder contract
    violation at boot (R-09: a missing column/index OR a 0-row serving table). Soft contract
    NOTES (e.g. a type-mismatch or unindexed rows) are surfaced but do NOT flip the gate —
    they are degraded-not-broken."""
    ok = lance_store.reachable()
    report = getattr(request.app.state, "contract_report", {}) or {}
    contract_ok = all(not f.get("violations") for f in report.values())
    body = {
        **_info(),
        "r2_reachable": ok,
        "contract_ok": contract_ok,
        "contracts": {
            k: (
                "ok"
                if not (f.get("violations") or f.get("notes"))
                else {"violations": f.get("violations", []), "notes": f.get("notes", [])}
            )
            for k, f in report.items()
        },
    }
    return JSONResponse(body, status_code=200 if (ok and contract_ok) else 503)


# ── The read surface ─────────────────────────────────────────────────────────
@app.get("/api/v1/award-profile/{domain}", response_model=None, dependencies=[Depends(require_operator)])
def award_profile(
    domain: str = Path(..., description="Company web domain, e.g. rotochopper.com"),
    awards: int = Query(0, ge=0, le=100, description="If >0, also return up to N prime award line items."),
) -> JSONResponse:
    """domain → federal award profile.

    404 when the domain resolves to no known company. 200 with ``awardProfile:
    null`` and ``isFederalContractor: false`` when the company is known but has no
    federal contracting footprint (a valid outcome). ``recentAwards`` is present
    only when ``?awards=N`` (N>0) is requested.
    """
    norm = lance_store.normalize_domain(domain)
    if not norm or not lance_store.valid_domain(norm):
        raise HTTPException(status_code=400, detail="invalid domain")

    company_row = lance_store.resolve_company_by_domain(norm)
    if company_row is None:
        raise HTTPException(status_code=404, detail=f"no company found for domain {norm!r}")

    company = Company.from_row(company_row)
    uei = company.uei

    summary_row = lance_store.award_summary_by_uei(uei) if uei else None
    profile = AwardProfile.from_row(summary_row) if summary_row else None

    recent: list[RecentAward] | None = None
    if awards > 0 and uei:
        recent = [RecentAward.from_row(r) for r in lance_store.recent_awards_by_uei(uei, awards)]

    resp = AwardProfileResponse(
        domain=norm,
        matched=True,
        is_federal_contractor=profile is not None,
        company=company,
        award_profile=profile,
        recent_awards=recent,
    )
    return JSONResponse({"data": resp.model_dump(by_alias=True, exclude_none=False)})


# ── Entity UI surfaces (UEI-keyed; BFF resolves the UEI from the session) ─────
@app.get("/api/v1/entities/{uei}/sam-profile", response_model=None, dependencies=[Depends(require_operator)])
def sam_profile(uei: str = Path(..., description="12-char SAM.gov UEI")) -> JSONResponse:
    """SAM.gov registration profile for a UEI: status + expiry (with days-remaining),
    NAICS/PSC, raw business_types, physical city/state/zip, and government POC slots.
    404 when the UEI has no active SAM registration."""
    uei = _require_uei(uei)
    entity = lance_store.sam_entity_by_uei(uei)
    if entity is None:
        raise HTTPException(status_code=404, detail=f"no SAM registration for uei {uei!r}")
    pocs = lance_store.sam_pocs_by_uei(uei)
    return _envelope(SamProfileResponse.from_row(entity, pocs, date.today()))


@app.get("/api/v1/entities/{uei}/active-contracts", response_model=None, dependencies=[Depends(require_operator)])
def active_contracts(
    uei: str = Path(..., description="12-char SAM.gov UEI"),
    limit: int = Query(25, ge=1, le=100, description="Max prime award line items to return."),
) -> JSONResponse:
    """Active prime contracts (PoP not elapsed). count + totalObligated are read off
    the pre-materialized entity_profile_gold row (no aggregate); line items are a point-lookup
    on entity_award_lines_gold (pre-classified active list, obligation desc)."""
    uei = _require_uei(uei)
    gold = lance_store.entity_profile_by_uei(uei) or {}
    today = date.today()
    items = [ActiveContract.from_row(r, today, "active")
             for r in lance_store.entity_award_lines_by_uei(uei, "active", limit)]
    agencies = sorted({i.awarding_agency for i in items if i.awarding_agency})
    resp = ActiveContractsResponse(
        count=gold.get("active_award_count"),
        total_obligated=gold.get("total_active_obligations"),
        agencies=agencies,
        contracts=items,
    )
    return _envelope(resp)


@app.get("/api/v1/entities/{uei}/overview", response_model=None, dependencies=[Depends(require_operator)])
def overview(uei: str = Path(..., description="12-char SAM.gov UEI")) -> JSONResponse:
    """Overview aggregates — a pure point-lookup off entity_profile_gold (lifetime
    value, total/active counts, active value). 404 when the UEI is absent from gold."""
    uei = _require_uei(uei)
    gold = lance_store.entity_profile_by_uei(uei)
    if gold is None:
        raise HTTPException(status_code=404, detail=f"no entity profile for uei {uei!r}")
    return _envelope(OverviewResponse.from_row(gold))


@app.get("/api/v1/entities/{uei}/past-performance", response_model=None, dependencies=[Depends(require_operator)])
def past_performance(
    uei: str = Path(..., description="12-char SAM.gov UEI"),
    limit: int = Query(25, ge=1, le=100, description="Max closed prime award line items to return."),
) -> JSONResponse:
    """Past performance — closed prime contracts (PoP elapsed). Lifetime headline +
    closed count come off the gold row; CPARS / exclusions / recompetes are GAPs
    (no source dataset) and surface as nulls."""
    uei = _require_uei(uei)
    gold = lance_store.entity_profile_by_uei(uei) or {}
    today = date.today()
    items = [ActiveContract.from_row(r, today, "completed")
             for r in lance_store.entity_award_lines_by_uei(uei, "closed", limit)]
    total = gold.get("award_count")
    active = gold.get("active_award_count")
    closed = (total - active) if isinstance(total, int) and isinstance(active, int) else None
    resp = PastPerformanceResponse(
        closed_count=closed,
        awards_lifetime=gold.get("total_lifetime_obligations"),
        contracts_lifetime=total,
        contracts=items,
    )
    return _envelope(resp)


# ── Map EXECUTE surface (deterministic filter-and-render; no LLM, no SQL engine) ──
@app.post("/api/v1/map/{dataset}/query", response_model=None, dependencies=[Depends(require_operator)])
def map_query(
    dataset: str = Path(..., description="Map serving table: 'winners' | 'company'"),
    body: MapQueryRequest = Body(default=MapQueryRequest()),
) -> JSONResponse:
    """Compiled filter object → Lance scanner predicate → GeoJSON FeatureCollection.

    The deterministic EXECUTE side of the portal map: the body is a constrained
    ``{filters:[{field,op,value}]}`` object (never NL, never SQL). Filters are
    AND-combined and the scan is restricted to plottable rows. An off-allowlist
    field/op or a mistyped value is a 422; an unknown dataset is a 404. The response
    is ``{"data": <FeatureCollection>, "meta": {...}}``; ``meta.total`` is the EXACT
    match count (``count_rows`` pushdown) and ``meta.capped`` flags a result truncated
    at the row bound — derived from ``total``, never from a sentinel row (the prior
    ``limit+1`` probe under-reported with the pylance limited-scan planner)."""
    decoder = DECODERS.get(dataset)
    if decoder is None:
        raise HTTPException(status_code=404, detail=f"unknown map dataset {dataset!r}")
    try:
        predicate = lance_store.compile_map_filter(
            decoder, [c.model_dump() for c in body.filters]
        )
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid filter: {exc}")
    cap = lance_store.MAP_HARD_ROW_CAP
    limit = min(body.limit or cap, cap)
    total = lance_store.map_count(decoder, predicate)
    rows = lance_store.map_query(decoder, predicate, limit)
    fc = lance_store.to_geojson(decoder, rows)
    return JSONResponse({
        "data": fc,
        "meta": {
            "dataset": dataset,
            "decoderVersion": decoder.version,
            "returned": len(fc["features"]),
            "total": total,
            "capped": total > limit,
        },
    })


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.host(), port=config.port())


if __name__ == "__main__":
    main()
