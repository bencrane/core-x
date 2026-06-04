"""catalyst_api entrypoint — Gen-3 read API: domain → US federal award profile.

A lightweight FastAPI gateway over the committed R2 Lance sink. One headline
capability: resolve a web domain to its federal contracting profile via native
``BTREE`` point-lookups (``firmographics_blitz.domain_norm`` → ``uei`` →
``contractor_award_summary.recipient_uei``), with prime award line items from
``usaspending/award_search`` available as an opt-in detail.

Run (locally and on Railway, from the repo root):

    python -m apps.catalyst_api.main

Deployed as a Railway service co-located with the platform-api BFF, on the
project's PRIVATE network (no public domain) — it binds ``::`` (IPv6; Railway's
private net is IPv6-only) on a fixed ``$PORT`` (8080). Every ``/api/v1`` route is
gated by an operator bearer token (``CATALYST_API_TOKEN``) that the BFF presents
as Bearer — the service is NOT exposed to the public web; the BFF is the only
caller. ``/healthz`` and ``/`` stay open for liveness probes.
"""

from __future__ import annotations

import hmac
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from .src import config, lance_store
from .src.models import AwardProfile, AwardProfileResponse, Company, RecentAward

log = logging.getLogger("catalyst_api")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Warm + fail-fast surface: confirm R2 credentials open the anchor manifest at
    # boot rather than mid-request. A failure is logged (not fatal) so /healthz can
    # still report the degraded state.
    if config.operator_token() is None:
        log.warning("CATALYST_API_TOKEN unset — /api/v1 routes are UNAUTHENTICATED (local dev only).")
    try:
        ok = lance_store.reachable()
        log.info("catalyst_api: R2 anchor dataset reachable=%s", ok)
    except Exception as exc:  # noqa: BLE001 — boot probe is best-effort
        log.warning("catalyst_api: R2 anchor probe failed at boot: %s", exc)
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
        },
    }


@app.get("/")
def root() -> dict:
    return _info()


@app.get("/healthz")
def healthz() -> JSONResponse:
    """Liveness + R2 reachability. Open (no token) for platform probes."""
    ok = lance_store.reachable()
    body = {**_info(), "r2_reachable": ok}
    return JSONResponse(body, status_code=200 if ok else 503)


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


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.host(), port=config.port())


if __name__ == "__main__":
    main()
