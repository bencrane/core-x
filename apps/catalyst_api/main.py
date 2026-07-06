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
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from datetime import date

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Path, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.middleware.gzip import GZipMiddleware

from .src import config, dossier, lance_store, market_registry, market_store, subout_store
from .src.map_decoders import DECODERS
from .src.card_html import render_card, render_not_found
from .src.models import (
    ActiveContract,
    ActiveContractsResponse,
    AwardProfile,
    AwardProfileResponse,
    CapabilityProfileResponse,
    Company,
    DossierBatchRequest,
    EntityDossierResponse,
    MapAggregateRequest,
    MapQueryRequest,
    MarketQueryRequest,
    OverviewResponse,
    PastPerformanceResponse,
    PersonByLinkedInRequest,
    PersonByLinkedInResponse,
    RecentAward,
    SamProfileResponse,
    SubawardProfileResponse,
)

log = logging.getLogger("catalyst_api")

# Fan-out executor for the dossier's three independent point-lookups. Sized for a
# few concurrent drawer opens (3 lookups each); the lookups are R2-I/O-bound.
_DOSSIER_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="dossier")

# Batch executor: the batch route FLATTENS every dossier into its three lookups and
# submits all 3·N to this pool (submits happen in ROUTE code, never from inside a
# pooled task — no nested-submit deadlock). Lookups are R2-I/O-bound waits on warm
# BTREE handles (pylance releases the GIL), so a wide pool is parked threads, not CPU:
# measured live, 8 serial-compose workers ran a 50-batch at ~4.9 s; flattened over 32
# threads the same batch is sub-second. Override with CATALYST_BATCH_LOOKUP_THREADS.
_BATCH_POOL = ThreadPoolExecutor(
    max_workers=int(os.environ.get("CATALYST_BATCH_LOOKUP_THREADS", "32")),
    thread_name_prefix="dossier-batch",
)


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
    # Prewarm the dossier-critical handles + their key BTREEs so the FIRST request
    # after a deploy is already warm (the probes above seeded the handle cache; this
    # pages the indices). Best-effort — a warm failure never delays or blocks boot.
    try:
        warm = lance_store.prewarm_dossier_surfaces()
        log.info("catalyst_api: dossier prewarm seconds=%s", warm)
    except Exception as exc:  # noqa: BLE001
        log.warning("catalyst_api: dossier prewarm failed: %s", exc)
    # Prewarm the subout-opportunities in-process caches (gtm_open_awards + the cube
    # marginal) on a daemon thread — the build is tens of seconds of R2 streaming, so
    # it must never delay boot; the first request either finds it warm or builds cold.
    import threading as _threading

    _threading.Thread(target=subout_store.prewarm_caches, daemon=True,
                      name="subout-cache-prewarm").start()
    yield


app = FastAPI(title="catalyst_api", version="1.0.0", lifespan=lifespan)
# Gzip responses >1 KB. The map GeoJSON FeatureCollection is highly repetitive JSON
# (~85-90% smaller gzipped); edge_api's httpx client auto-decodes. Internal hop, but free.
app.add_middleware(GZipMiddleware, minimum_size=1000)


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
            "capability_profile": "/api/v1/entities/{uei}/capability-profile",
            "dossier": "/api/v1/entities/{uei}/dossier?actions=N",
            "dossiers_batch": "/api/v1/entities/dossiers  (POST: {ueis:[...], actions:N})",
            "subaward_profile": "/api/v1/entities/{uei}/subaward-profile?history=N",
            "person_by_linkedin": "/api/v1/people/by-linkedin  (POST: {url})",
            "map_query": "/api/v1/map/{dataset}/query  (POST: {filters:[{field,op,value}]})",
            "market_fields": "/api/v1/market/fields",
            "market_query": "/api/v1/market/query  (POST: {grain:'entity'|'prime_award'|'transaction', filters:[...], limit:N})",
            "market_codes": "/api/v1/market/codes?type=naics|psc|agency&q=<text>&limit=20",
            "market_subout_opportunities": (
                "/api/v1/market/subout-opportunities  (POST: {uei, lenses?, "
                "codes_override?, code_type?, limit?, include_peers?})"),
        },
        "map_datasets": [*DECODERS, "entities", "prime_awards", "transactions"],
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


@app.get("/card/{uei}", response_class=HTMLResponse)
def capability_card(uei: str = Path(..., description="12-char SAM.gov UEI")) -> HTMLResponse:
    """OPEN, self-contained HTML render of the capability profile card — the standalone
    prototype look, deliberately NOT the /api/v1 JSON surface or any product design system.
    Public (no bearer): a UEI in the URL renders a shareable card. Data is public-record
    SAM/USAspending-derived capability, same posture as the consuming BFF's public routes."""
    u = (uei or "").strip()
    if not lance_store.valid_uei(u):
        return HTMLResponse(render_not_found(u), status_code=400)
    row = lance_store.capability_profile_by_uei(u)
    if row is None:
        return HTMLResponse(render_not_found(u), status_code=404)
    return HTMLResponse(render_card(row))


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


@app.get("/api/v1/entities/{uei}/capability-profile", response_model=None, dependencies=[Depends(require_operator)])
def capability_profile(uei: str = Path(..., description="12-char SAM.gov UEI")) -> JSONResponse:
    """The per-firm capability profile card - ONE BTREE point-lookup on
    capability_profile.uei. Returns identity + designations + sub/prime activity + the
    evidence-tiered recommended NAICS+PSC lanes (each lane names the primes who sub it
    out). Covers active subs and never-subbed DSBS alike. 404 when the UEI has no profile."""
    uei = _require_uei(uei)
    row = lance_store.capability_profile_by_uei(uei)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no capability profile for uei {uei!r}")
    return _envelope(CapabilityProfileResponse.from_row(row))


@app.get("/api/v1/entities/{uei}/dossier", response_model=None, dependencies=[Depends(require_operator)])
def entity_dossier(
    uei: str = Path(..., description="12-char SAM.gov UEI"),
    actions: int = Query(10, ge=1, le=25, description="Max recent award actions to return."),
) -> JSONResponse:
    """The cockpit side-panel dossier — THREE BTREE point-lookups composed:
    entity_profile_gold (identity + address + rollups + POC slots),
    contractor_award_summary (most-recent action date + top agencies), and the
    rolling-90d awards serving table (per-action recent activity, newest first).
    404 when the UEI is absent from the gold spine. POCs carry name/title/geo only
    — the SAM source has no email/phone columns, so none can be exposed."""
    uei = _require_uei(uei)
    # The three lookups are independent — run them concurrently (the Lance scanner
    # API is sync; the shared executor fans them out). 404 still keys off gold.
    f_gold = _DOSSIER_POOL.submit(lance_store.entity_dossier_gold, uei)
    f_cas = _DOSSIER_POOL.submit(lance_store.award_summary_by_uei, uei)
    f_recent = _DOSSIER_POOL.submit(lance_store.recent_award_actions_by_uei, uei, actions)
    gold = f_gold.result()
    cas, recent = f_cas.result(), f_recent.result()
    if gold is None:
        raise HTTPException(status_code=404, detail=f"no entity profile for uei {uei!r}")
    return _envelope(EntityDossierResponse.from_parts(gold, cas, recent, date.today()))


@app.post("/api/v1/entities/dossiers", response_model=None, dependencies=[Depends(require_operator)])
def entity_dossiers(body: DossierBatchRequest = Body(...)) -> JSONResponse:
    """BATCH dossier read — the cockpit's eager-prefetch path: up to 100 UEIs per
    call, each composed exactly as the single route (dossier.compose_dossier) and
    fanned out on the batch pool over the warm handle cache. PARTIAL SUCCESS by
    contract: the response maps every requested UEI to its dossier or ``null``
    (unknown / invalid-format) — never all-or-nothing. Same public-record posture
    as the single route; 422 only for a malformed/over-bound request envelope."""
    ueis, err = dossier.prepare_batch(body.ueis)
    if err:
        raise HTTPException(status_code=422, detail=err)
    actions = max(1, min(body.actions or dossier.ACTIONS_DEFAULT, dossier.ACTIONS_MAX))
    today = date.today()
    # FLATTENED fan-out: all 3·N lookups go to the pool at once (gold ∥ CAS ∥ actions
    # per UEI) — the serial-compose shape throttled a 50-batch to ~4.9 s live. The
    # composition itself (from_parts) is the same one the single route and
    # dossier.compose_dossier use, so payload parity holds.
    futures = {
        u: (
            _BATCH_POOL.submit(lance_store.entity_dossier_gold, u),
            _BATCH_POOL.submit(lance_store.award_summary_by_uei, u),
            _BATCH_POOL.submit(lance_store.recent_award_actions_by_uei, u, actions),
        )
        for u in ueis
        if lance_store.valid_uei(u)
    }
    data: dict = {}
    found = 0
    for u in ueis:
        trio = futures.get(u)
        if trio is None:                  # invalid-format UEI → null (partial success)
            data[u] = None
            continue
        gold = trio[0].result()
        cas, recent = trio[1].result(), trio[2].result()
        if gold is None:                  # unknown UEI → null (partial success)
            data[u] = None
            continue
        model = EntityDossierResponse.from_parts(gold, cas, recent, today)
        found += 1
        data[u] = model.model_dump(by_alias=True, exclude_none=False)
    return JSONResponse({"data": data, "meta": {"requested": len(ueis), "found": found}})


@app.get("/api/v1/entities/{uei}/subaward-profile", response_model=None, dependencies=[Depends(require_operator)])
def subaward_profile(
    uei: str = Path(..., description="12-char SAM.gov UEI (of the subawardee)"),
    history: int = Query(25, ge=1, le=200, description="Max prime-contract (subaward) history rows."),
) -> JSONResponse:
    """Subawardee drill-down — what the sub CAN DO + the prime contracts it won work UNDER. Composes a
    BTREE point-lookup on govcon_subawardee_profiles (the capability block: scope_summary,
    solicitation_scope_tags, clearance/cert/labor, teaming, POC — present only for bridge-universe subs) with a
    point-lookup on contract_subaward (the prime-contract history: which prime, $, description, agency,
    date — present for all subs). 404 only when the UEI has NEITHER a profile NOR any subaward history."""
    uei = _require_uei(uei)
    f_prof = _DOSSIER_POOL.submit(lance_store.subaward_profile_by_uei, uei)
    f_hist = _DOSSIER_POOL.submit(lance_store.subaward_history_by_uei, uei, history)
    profile = f_prof.result()
    hist = f_hist.result()
    if profile is None and not hist:
        raise HTTPException(status_code=404, detail=f"no subaward profile or history for uei {uei!r}")
    return _envelope(SubawardProfileResponse.from_parts(uei, profile, hist))


# ── Person surface (LinkedIn URL → active/people title + identity) ───────────
@app.post("/api/v1/people/by-linkedin", response_model=None, dependencies=[Depends(require_operator)])
def person_by_linkedin(body: PersonByLinkedInRequest = Body(...)) -> JSONResponse:
    """LinkedIn URL (in the POST body, ``{"url": ...}``) → the person's job title (+ identity)
    from ``active/people``.

    A BTREE point-lookup on ``person_linkedin_url`` (canonical-variant ``IN`` pushdown —
    tolerant of scheme/www/trailing-slash drift in the caller's URL). ALWAYS 200 (an
    enrichment surface: a no-match is normal data, not an error — so a Clay/automation row
    never fails on it). ``found`` is the flag: false when the URL is empty, not a parseable
    ``linkedin.com/in/`` url, OR resolves to no stored person; true otherwise, with the
    headline ``title`` + every distinct match (a URL can recur across re-observations /
    companies), so a title/company conflict is never hidden. (``person_by_linkedin_url``
    already returns [] for an unparseable/empty url, which composes to ``found: false``.)"""
    rows = lance_store.person_by_linkedin_url(body.url)
    return _envelope(PersonByLinkedInResponse.from_rows(body.url, rows))


# ── Map EXECUTE surface (deterministic filter-and-render; no LLM, no SQL engine) ──
@app.get("/api/v1/map/fields", response_model=None, dependencies=[Depends(require_operator)])
def map_fields() -> JSONResponse:
    """Field-registry introspection: every dataset's queryable axes, projected verbatim
    from the frozen decoder specs (never hand-maintained). One payload for all datasets
    so a filter-builder UI learns fields, per-field ops, enum vocabularies, and the
    aggregate allowlist in a single fetch. An axis absent here is by definition not yet
    configured — the honest 'what works' contract for the query workbench.

    The five pre-spine map datasets carry ``legacy: true`` (the workbench uses it to
    hide their tabs; the public /ask still executes against them — the decoders stay).
    The spine-backed ``entities`` dataset (the market query engine, ``legacy: false``)
    rides in the SAME payload so the workbench learns it with zero extra fetches; its
    queries route to POST /api/v1/map/entities/query (the adapter below)."""
    out = {}
    for name, decoder in DECODERS.items():
        out[name] = {
            "decoderVersion": decoder.version,
            "legacy": True,
            "fields": [
                {
                    "name": qname,
                    "type": spec.type,
                    "ops": list(spec.ops),
                    "enum": list(spec.enum) if spec.enum is not None else None,
                    "index": spec.index,
                    "gated": spec.gated,
                }
                for qname, spec in decoder.fields.items()
            ],
            "aggregate": (
                {
                    "measure": decoder.aggregate.measure,
                    "dims": list(decoder.aggregate.dims.keys()),
                    "metrics": list(decoder.aggregate.metrics),
                    "defaultLimit": decoder.aggregate.default_limit,
                    "maxLimit": decoder.aggregate.max_limit,
                }
                if decoder.aggregate is not None else None
            ),
        }
    out["entities"] = market_registry.fields_payload()
    out["prime_awards"] = market_registry.table_fields_payload(market_registry.PRIME_AWARDS)
    out["transactions"] = market_registry.table_fields_payload(market_registry.TRANSACTIONS)
    return JSONResponse({"data": {"datasets": out}})


# ── Market query engine (spine-backed — the replace-forward surface) ──────────
def _market_datasets_payload() -> dict:
    return {
        "entities": market_registry.fields_payload(),
        "prime_awards": market_registry.table_fields_payload(market_registry.PRIME_AWARDS),
        "transactions": market_registry.table_fields_payload(market_registry.TRANSACTIONS),
    }


@app.get("/api/v1/market/fields", response_model=None, dependencies=[Depends(require_operator)])
def market_fields() -> JSONResponse:
    """The market registry, projected verbatim in the same ``{"data": {"datasets":
    {...}}}`` shape as /api/v1/map/fields — the clean alias for consumers that only
    want the spine-backed grains (entities + prime_awards + transactions)."""
    return JSONResponse({"data": {"datasets": _market_datasets_payload()}})


def _run_market_query(grain: str, filters, limit: int | None) -> dict:
    """Grain dispatch shared by /market/query and the map adapters: 'entity' → the
    multi-table UEI-intersection executor; a table grain (prime_award[s] /
    transaction[s]) → the single-table executor. MapCompileError → 422 at the caller."""
    if grain == "entity":
        return market_store.execute_entity_query(filters, limit)
    spec = market_registry.TABLE_GRAINS.get(grain)
    if spec is None:
        raise HTTPException(
            status_code=422,
            detail=f"unknown grain {grain!r} — serves 'entity', 'prime_award', 'transaction'")
    return market_store.execute_table_query(spec, filters, limit)


@app.post("/api/v1/market/query", response_model=None, dependencies=[Depends(require_operator)])
def market_query(body: MarketQueryRequest = Body(default=MarketQueryRequest())) -> JSONResponse:
    """Deterministic query over the spine-backed market grains. The body is a
    constrained ``{grain, filters, limit}`` object — scalar ``{field, op, value}``
    clauses (registry-validated per grain) plus, on the entity grain only,
    ``{"lane": {...}}`` predicates. Grains: 'entity' (UEI-set intersection over the
    L2 spine datasets), 'prime_award' (one FPDS award row, topology-aware), and
    'transaction' (one raw FPDS action). The 100M-row table grains REQUIRE at least
    one filter (422 otherwise). 422 on any off-registry field/op/enum; ``meta.executed``
    echoes the validated filter object, ``meta.total`` is exact."""
    grain = body.grain
    try:
        result = _run_market_query(grain, [c.model_dump() for c in body.filters], body.limit)
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid filter: {exc}")
    version = (market_registry.REGISTRY_VERSION if grain == "entity"
               else market_registry.TABLE_GRAINS[grain].version)
    return JSONResponse({
        "data": {"rows": result["rows"]},
        "meta": {
            "grain": result["executed"]["grain"],
            "registryVersion": version,
            "returned": result["returned"],
            "total": result["total"],
            "capped": result["capped"],
            "executed": result["executed"],
        },
    })


@app.post("/api/v1/market/subout-opportunities", response_model=None, dependencies=[Depends(require_operator)])
def market_subout_opportunities(body: dict = Body(...)) -> JSONResponse:
    """The subout-opportunities recipe (subout_opportunities.v1): given a target UEI,
    the open prime awards most likely to be subbed out to companies with its code
    profile. Probe lenses (all four by default): awarded_prime_contracts_in_code /
    delivered_subawards_under_code / sam_registered_naics / inferred_primeable, plus
    caller_declared via ``codes_override``. Every score rides with its explicit
    components (name, raw_value, weight, contribution). The body is validated
    fail-closed against the recipe contract in ``subout_store.validate_request`` —
    an unknown key is a 422, never silently ignored. A UEI with no code signals is a
    200 with empty data + ``meta.reason`` (an empty market is an answer, not an
    error); per-stage wall times ride ``meta.timings_ms``.

    HOT PATH IS IN-PROCESS: the open-award table + the sub-out cube marginal are
    process-memory caches (lazy, TTL-refreshed in the background; ``meta.cache_state``
    = cold | warm | unavailable, ``meta.cache_build_ms`` alongside). Per-request
    remote reads are BTREE uei point-lookups only. ``include_peers`` defaults FALSE —
    the peers lookup is the one remote non-point query (opt-in; adds latency)."""
    try:
        result = subout_store.execute_subout_opportunities(body)
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid filter: {exc}")
    return JSONResponse(result)


@app.get("/api/v1/market/codes", response_model=None, dependencies=[Depends(require_operator)])
def market_codes(
    type: str = Query(..., description="Code system: 'naics' | 'psc' | 'agency'"),
    q: str = Query("", description="Search text: code prefix OR description substring."),
    limit: int = Query(market_store.CODES_DEFAULT_LIMIT, ge=1, le=market_store.CODES_MAX_LIMIT),
) -> JSONResponse:
    """Code typeahead for every field the registry marks ``codes`` (the workbench
    renders a typeahead for those): ranked match over the in-memory code dimensions
    (naics_reference / psc_reference; agency = DISTINCT awarding agency code+name pairs
    from usaspending_award_canonical, majority-name deduped). Code-PREFIX matches rank
    above description-SUBSTRING matches. Empty q / bad type → 422."""
    try:
        codes = market_store.code_search(type, q, limit)
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid code search: {exc}")
    return JSONResponse({
        "data": {"codes": codes},
        "meta": {"type": type, "q": q.strip(), "returned": len(codes)},
    })


# NOTE: registered BEFORE the parametrized /api/v1/map/{dataset}/query route below —
# FastAPI matches in registration order, so the literal path wins for dataset='entities'.
@app.post("/api/v1/map/entities/query", response_model=None, dependencies=[Depends(require_operator)])
def map_entities_query(body: MarketQueryRequest = Body(default=MarketQueryRequest())) -> JSONResponse:
    """Workbench adapter over the market query engine: accepts the SAME body the map
    datasets take (``{filters, limit}`` — lane objects also pass) and returns the SAME
    envelope the workbench parses. Features carry REAL ``Point [lon, lat]`` geometry
    when the entity has a row in the gtm_entity_geo HQ sidecar (hydrated LEFT-join;
    ``properties.geo_precision`` = 'address' | 'county') and ``geometry: null``
    otherwise — the table stays complete, the dot layer skips the nulls, and
    ``meta.plottable`` counts the dots honestly. The clean row-shaped surface is
    POST /api/v1/market/query."""
    if body.grain != "entity":
        raise HTTPException(status_code=422, detail=f"unknown grain {body.grain!r} — v1 serves 'entity'")
    try:
        result = market_store.execute_entity_query(
            [c.model_dump() for c in body.filters], body.limit
        )
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid filter: {exc}")
    features, plottable = market_store.to_entity_features(result["rows"])
    return JSONResponse({
        "data": {"type": "FeatureCollection", "features": features},
        "meta": {
            "dataset": "entities",
            "decoderVersion": market_registry.REGISTRY_VERSION,
            "returned": result["returned"],
            "plottable": plottable,
            "total": result["total"],
            "capped": result["capped"],
            "executed": result["executed"],
        },
    })


def _table_grain_adapter(spec, body: MarketQueryRequest) -> JSONResponse:
    """Shared workbench adapter for the table grains: run the single-table executor,
    emit the exact FeatureCollection + meta envelope the workbench parses. prime_awards
    rows carry recipient-HQ geometry (gtm_entity_geo LEFT-join; ``geo_precision`` in
    properties — the award state table has NO place-of-performance columns, so the dot
    is the recipient's HQ, not the work site); transactions are geometry:null in v1."""
    grain_ok = body.grain in ("entity", spec.grain, spec.dataset_key)  # 'entity' = model default
    if not grain_ok:
        raise HTTPException(status_code=422, detail=f"unknown grain {body.grain!r} for {spec.dataset_key}")
    try:
        result = market_store.execute_table_query(
            spec, [c.model_dump() for c in body.filters], body.limit)
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid filter: {exc}")
    features, plottable = market_store.to_entity_features(result["rows"])
    return JSONResponse({
        "data": {"type": "FeatureCollection", "features": features},
        "meta": {
            "dataset": spec.dataset_key,
            "decoderVersion": spec.version,
            "returned": result["returned"],
            "plottable": plottable,
            "total": result["total"],
            "capped": result["capped"],
            "executed": result["executed"],
        },
    })


@app.post("/api/v1/map/prime_awards/query", response_model=None, dependencies=[Depends(require_operator)])
def map_prime_awards_query(body: MarketQueryRequest = Body(default=MarketQueryRequest())) -> JSONResponse:
    """Workbench adapter — award-grain FPDS queries (topology-aware; min one filter).
    Dots = recipient HQ (geo_precision in properties); see market_registry.PRIME_AWARDS."""
    return _table_grain_adapter(market_registry.PRIME_AWARDS, body)


@app.post("/api/v1/map/transactions/query", response_model=None, dependencies=[Depends(require_operator)])
def map_transactions_query(body: MarketQueryRequest = Body(default=MarketQueryRequest())) -> JSONResponse:
    """Workbench adapter — raw FPDS action-grain queries (per-action deltas; min one
    filter). geometry:null in v1 — award dots live on prime_awards."""
    return _table_grain_adapter(market_registry.TRANSACTIONS, body)


@app.post("/api/v1/map/{dataset}/query", response_model=None, dependencies=[Depends(require_operator)])
def map_query(
    dataset: str = Path(..., description="Map serving table: 'winners' | 'company' | 'awards'"),
    body: MapQueryRequest = Body(default=MapQueryRequest()),
) -> JSONResponse:
    """Compiled filter object → Lance scanner predicate → GeoJSON FeatureCollection.

    The deterministic EXECUTE side of the portal map: the body is a constrained
    ``{filters:[{field,op,value}]}`` object (never NL, never SQL). Filters are
    AND-combined. The scan is NOT restricted to plottable rows — a qualifying row
    whose address did not geocode is served as a ``geometry: null`` feature so the
    TABLE view stays complete; the dot layer skips it. An off-allowlist field/op or
    a mistyped value is a 422; an unknown dataset is a 404. The response is
    ``{"data": <FeatureCollection>, "meta": {...}}``; ``meta.total`` is the EXACT
    match count (``count_rows`` pushdown), ``meta.plottable`` the served features
    that carry geometry, and ``meta.capped`` flags a result truncated at the row
    bound — derived from ``total``, never from a sentinel row (the prior ``limit+1``
    probe under-reported with the pylance limited-scan planner)."""
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
            "plottable": sum(1 for f in fc["features"] if f["geometry"] is not None),
            "total": total,
            "capped": total > limit,
        },
    })


@app.post("/api/v1/map/{dataset}/aggregate", response_model=None, dependencies=[Depends(require_operator)])
def map_aggregate(
    dataset: str = Path(..., description="Map serving table (must declare an AggregateSpec; 'awards')"),
    body: MapAggregateRequest = Body(...),
) -> JSONResponse:
    """Compiled filter + group-by → metric rows. The aggregate EXECUTE surface: the SAME
    ``{filters:[{field,op,value}]}`` allowlist as ``/query`` (so the time window is a
    query-driven ``days_since_action`` clause, never hardcoded), plus an ``aggregate``
    intent ``{group_by, metrics, limit}``. ``group_by`` is a dataset dim or a pseudo-dim
    ('winner' top-N | 'size_band' histogram); ``metrics`` ⊆ the decoder allowlist
    (count/sum/avg/median/p90). Off-allowlist filter OR aggregate → 422; unknown or
    aggregation-unsupported dataset → 404/422. Response: ``{"data": {"aggregate": {...}},
    "meta": {...}}`` — groups are top-``limit`` by the primary metric (size_band ascending)."""
    decoder = DECODERS.get(dataset)
    if decoder is None:
        raise HTTPException(status_code=404, detail=f"unknown map dataset {dataset!r}")
    try:
        predicate = lance_store.compile_map_filter(decoder, [c.model_dump() for c in body.filters])
        result = lance_store.map_aggregate(
            decoder, predicate, body.aggregate.group_by, body.aggregate.metrics, body.aggregate.limit
        )
    except lance_store.MapCompileError as exc:
        raise HTTPException(status_code=422, detail=f"invalid aggregate: {exc}")
    return JSONResponse({
        "data": {"aggregate": result},
        "meta": {
            "dataset": dataset,
            "decoderVersion": decoder.version,
            "groupBy": result["group_by"],
            "matchedRows": result["matched_rows"],
            "totalGroups": result["total_groups"],
            "returned": len(result["groups"]),
        },
    })


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.host(), port=config.port())


if __name__ == "__main__":
    main()
