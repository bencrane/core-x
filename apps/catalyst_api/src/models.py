"""Response contract for catalyst_api.

The JSON shape is the contract the platform-app frontend builds against. Fields
are declared snake_case (Python house style) but serialize camelCase via the
alias generator — idiomatic TypeScript on the wire. Builders map raw Lance rows
to the model and are pure (no R2), so the composition is unit-testable.

Dates (Lance ``date32[day]``) are emitted as ISO ``YYYY-MM-DD`` strings; money is
raw USD floats — formatting is a presentation concern left to the frontend.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class _Model(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


def _iso(v: Any) -> str | None:
    return v.isoformat() if isinstance(v, date) else (v or None)


# ── Map query request contract (POST body for /api/v1/map/{dataset}/query) ──
class MapFilterClause(_Model):
    """One AND-combined filter clause. ``field``/``op``/``value`` are validated against
    the dataset's decoder in ``lance_store.compile_map_filter`` — NOT here (Pydantic
    only checks JSON shape; the typed allowlist is the security boundary)."""

    field: str
    op: str
    value: Any = None                 # scalar | list (for `in` / `between`)


class MapQueryRequest(_Model):
    """Compiled filter object — never NL, never SQL. ``filters`` are AND-combined; an
    empty list returns the whole (plottable) table up to the hard row cap."""

    title: str | None = None          # echo-through label from the compiler (unused by EXECUTE)
    filters: list[MapFilterClause] = []
    limit: int | None = None          # caller hint, clamped to MAP_HARD_ROW_CAP


class MarketFilterClause(_Model):
    """One entity-grain filter: EITHER a scalar ``{field, op, value}`` clause (validated
    against the market registry in ``market_store.compile_market_filters`` — Pydantic
    checks JSON shape only) OR a ``{"lane": {...}}`` lane predicate. The compiler
    rejects a clause carrying both (or neither) with a 422."""

    field: str | None = None
    op: str | None = None
    value: Any = None                 # scalar | list (for `in` / `between`)
    lane: dict | None = None          # {side, code_type, codes[], min_obl_*} — validated fail-closed
    inferred: dict | None = None      # {kind, code_type, codes[], min_supporting_bothsider_firm_ct?}
                                      # — cooccurrence-evidence predicate, validated fail-closed


class MarketQueryRequest(_Model):
    """POST body for the market query engine (/api/v1/market/query and the
    /api/v1/map/entities/query workbench adapter). ``grain`` is 'entity' (the only v1
    grain — anything else is a 422); ``filters`` are AND-combined."""

    grain: str = "entity"
    title: str | None = None          # echo-through label (unused by EXECUTE)
    filters: list[MarketFilterClause] = []
    limit: int | None = None          # clamped to market_store.MARKET_HARD_ROW_CAP
    # Recipient collapse (table grains only): "recipients" returns the DISTINCT
    # recipients behind the matching rows instead of the raw rows. Off-vocabulary
    # values / entity grain / the map adapters reject it (422).
    collapse: str | None = None


class AggregateIntent(_Model):
    """The group-by + metrics of an aggregate request. ``group_by`` is a dataset dim or a
    pseudo-dim ('winner' | 'size_band'); ``metrics`` ⊆ the decoder's allowlist. Validated
    in ``lance_store.validate_aggregate`` — NOT here (Pydantic checks JSON shape only)."""

    group_by: str
    metrics: list[str] = []           # empty → EXECUTE defaults (count, sum)
    limit: int | None = None          # top-N groups, clamped to the spec's max


class MapAggregateRequest(_Model):
    """Compiled filter + aggregate intent. The filter allowlist is identical to
    ``MapQueryRequest`` — the time window rides as a ``days_since_action`` clause inside
    ``filters`` (query-driven), never as a hardcoded lookback in EXECUTE."""

    title: str | None = None
    filters: list[MapFilterClause] = []
    aggregate: AggregateIntent


class DossierBatchRequest(_Model):
    """POST body for /api/v1/entities/dossiers — the eager-prefetch batch read.
    ``ueis`` is validated/deduped/bounded by ``dossier.prepare_batch`` (not here:
    Pydantic checks JSON shape only); ``actions`` mirrors the single route's knob."""

    ueis: list[str] = []
    actions: int = 10


class Company(_Model):
    name: str | None = None
    uei: str | None = None
    website: str | None = None
    industry: str | None = None
    employee_size_band: str | None = None
    founded_year: int | None = None
    hq_city: str | None = None
    hq_state: str | None = None
    hq_region: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "Company":
        return cls(
            name=r.get("company_name"),
            uei=(r.get("uei") or None),
            website=r.get("website") or r.get("domain_raw"),
            industry=r.get("industry"),
            employee_size_band=r.get("employee_size_band"),
            founded_year=r.get("founded_year"),
            hq_city=r.get("hq_city"),
            hq_state=r.get("hq_state"),
            hq_region=r.get("hq_region"),
        )


class TopAgency(_Model):
    name: str
    dollars: float


class AwardProfile(_Model):
    lifetime_prime_obligated: float | None = None
    lifetime_subaward_obligated: float | None = None
    total_combined_obligated: float | None = None
    contract_dollars: float | None = None
    grant_dollars: float | None = None
    other_dollars: float | None = None
    prime_total_awards: int | None = None
    prime_active_awards: int | None = None
    prime_closed_awards: int | None = None
    subaward_total: int | None = None
    subaward_active: int | None = None
    subaward_closed: int | None = None
    total_combined_awards: int | None = None
    primary_naics: str | None = None
    primary_psc: str | None = None
    first_award_date: str | None = None
    most_recent_action_date: str | None = None
    most_recent_obligation: float | None = None
    top_agencies: list[TopAgency] = []
    as_of_date: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "AwardProfile":
        agencies: list[TopAgency] = []
        for name_key, dollar_key in (
            ("top_agency_1_name", "top_agency_1_dollars"),
            ("top_agency_2_name", "top_agency_2_dollars"),
            ("top_agency_3_name", "top_agency_3_dollars"),
        ):
            name = r.get(name_key)
            if name:
                agencies.append(TopAgency(name=name, dollars=float(r.get(dollar_key) or 0.0)))
        return cls(
            lifetime_prime_obligated=r.get("lifetime_prime_obligated"),
            lifetime_subaward_obligated=r.get("lifetime_subaward_obligated"),
            total_combined_obligated=r.get("total_combined_obligated"),
            contract_dollars=r.get("contract_dollars"),
            grant_dollars=r.get("grant_dollars"),
            other_dollars=r.get("other_dollars"),
            prime_total_awards=r.get("prime_total_awards"),
            prime_active_awards=r.get("prime_active_awards"),
            prime_closed_awards=r.get("prime_closed_awards"),
            subaward_total=r.get("subaward_total"),
            subaward_active=r.get("subaward_active"),
            subaward_closed=r.get("subaward_closed"),
            total_combined_awards=r.get("total_combined_awards"),
            primary_naics=r.get("primary_naics"),
            primary_psc=r.get("primary_psc"),
            first_award_date=_iso(r.get("prime_first_award_date")),
            most_recent_action_date=_iso(r.get("prime_most_recent_action_date")),
            most_recent_obligation=r.get("prime_most_recent_obligation"),
            top_agencies=agencies,
            as_of_date=_iso(r.get("summary_as_of_date")),
        )


class RecentAward(_Model):
    award_id: str | None = None
    display_award_id: str | None = None
    category: str | None = None
    type: str | None = None
    obligation: float | None = None
    amount: float | None = None
    naics: str | None = None
    naics_description: str | None = None
    psc_description: str | None = None
    funding_agency: str | None = None
    awarding_agency: str | None = None
    period_start: str | None = None
    period_end: str | None = None
    set_aside: str | None = None
    description: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "RecentAward":
        return cls(
            award_id=r.get("generated_unique_award_id"),
            display_award_id=r.get("display_award_id"),
            category=r.get("category"),
            type=r.get("type_description"),
            obligation=r.get("total_obligation"),
            amount=r.get("award_amount"),
            naics=r.get("naics_code"),
            naics_description=r.get("naics_description"),
            psc_description=r.get("product_or_service_description"),
            funding_agency=r.get("funding_toptier_agency_name"),
            awarding_agency=r.get("awarding_toptier_agency_name"),
            period_start=_iso(r.get("period_of_performance_start_date")),
            period_end=_iso(r.get("period_of_performance_current_end_date")),
            set_aside=r.get("type_set_aside"),
            description=r.get("description"),
        )


class AwardProfileResponse(_Model):
    """The full domain→award-profile payload. ``matched`` is the company-resolution
    flag; ``award_profile`` is ``None`` when the company resolves but has no federal
    contracting footprint (a valid, common outcome — not an error)."""

    domain: str
    matched: bool
    is_federal_contractor: bool
    company: Company | None = None
    award_profile: AwardProfile | None = None
    recent_awards: list[RecentAward] | None = None


# --------------------------------------------------------------------------- #
# Entity UI surfaces (UEI-keyed): SAM profile · active contracts · overview ·
# past performance. The BFF resolves the UEI from the trusted session and proxies
# these 1:1 to the frontend (camelCase wire). Gaps are emitted as explicit nulls
# (the gold layer does not carry the field) — never fabricated or parsed.
# --------------------------------------------------------------------------- #
def _days_between(target: Any, today: date) -> int | None:
    return (target - today).days if isinstance(target, date) else None


class Address(_Model):
    # sam_entity_master carries only city/state/zip5 (lifted from entity_registrations
    # pipe_fields). No street line, no mailing address at source — both stay null.
    street: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None


class SamPoc(_Model):
    type: str | None = None    # wire key `type` (sam_pocs.poc_type)
    poc_slot_no: int | None = None
    full_name: str | None = None
    title: str | None = None
    city: str | None = None
    state: str | None = None
    email: str | None = None   # GAP — no email column in sam_pocs
    phone: str | None = None   # GAP — no phone column in sam_pocs

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "SamPoc":
        name = r.get("full_name") or " ".join(
            p for p in (r.get("first_name"), r.get("last_name")) if p
        ) or None
        return cls(
            type=r.get("poc_type"),
            poc_slot_no=r.get("poc_slot_no"),
            full_name=name,
            title=r.get("title"),
            city=r.get("city"),
            state=r.get("state"),
        )


class SamProfileResponse(_Model):
    uei: str | None = None
    cage_code: str | None = None
    legal_business_name: str | None = None
    registration_status: str | None = None      # DERIVED: 'active' | 'expired' | 'inactive'
    registration_date: str | None = None
    activation_date: str | None = None
    expiration_date: str | None = None
    days_until_expiration: int | None = None     # DERIVED
    primary_naics: str | None = None
    secondary_naics: list[str] = []              # naics_codes minus the primary
    psc_codes: list[str] = []
    business_types_raw: str | None = None        # raw ~-delimited; NOT parsed (operator ruling)
    physical_address: Address = Address()
    mailing_address: Address | None = None       # GAP — no mailing address at source
    government_pocs: list[SamPoc] = []

    @classmethod
    def from_row(
        cls, entity: dict[str, Any], pocs: list[dict[str, Any]], today: date
    ) -> "SamProfileResponse":
        # Source columns are sam_master_entities' schema. registration_status has no source
        # column: derive 'expired' (PoP-style: expiry elapsed) > 'inactive' (is_active False)
        # > 'active'. registration_date ← initial_registration_date; expiration ←
        # registration_expiration_date; business_types_raw ← raw bus_type_string (no parse).
        exp = entity.get("registration_expiration_date")
        is_active = entity.get("is_active")
        primary = (entity.get("primary_naics") or None)
        naics = [c for c in (entity.get("naics_codes") or []) if c]
        secondary = [c for c in naics if c != primary]
        if isinstance(exp, date) and exp < today:
            status = "expired"
        elif is_active is False:
            status = "inactive"
        else:
            status = "active"
        return cls(
            uei=entity.get("uei"),
            cage_code=entity.get("cage_code"),
            legal_business_name=entity.get("legal_business_name"),
            registration_status=status,
            registration_date=_iso(entity.get("initial_registration_date")),
            activation_date=_iso(entity.get("activation_date")),
            expiration_date=_iso(exp),
            days_until_expiration=_days_between(exp, today),
            primary_naics=primary,
            secondary_naics=secondary,
            psc_codes=[c for c in (entity.get("psc_codes") or []) if c],
            business_types_raw=entity.get("bus_type_string"),
            physical_address=Address(
                city=entity.get("physical_address_city"),
                state=entity.get("physical_address_province_or_state"),
                zip=entity.get("physical_address_zip_postal_code"),
            ),
            government_pocs=[SamPoc.from_row(p) for p in pocs],
        )


class ActiveContract(RecentAward):
    award_date: str | None = None                 # action_date
    days_to_end: int | None = None                # DERIVED
    status: str | None = None                     # DERIVED: 'active' | 'completed'
    is_active: bool | None = None
    sub_agency: str | None = None                 # GAP — no subtier column in award_search
    option_year_decision_window: str | None = None  # GAP
    agency_poc: str | None = None                 # GAP — no contracting-officer fields
    modifications: list[Any] | None = None        # GAP — mod history not materialized

    @classmethod
    def from_row(cls, r: dict[str, Any], today: date, status: str) -> "ActiveContract":
        base = RecentAward.from_row(r).model_dump()
        return cls(
            **base,
            award_date=_iso(r.get("action_date")),
            days_to_end=_days_between(r.get("period_of_performance_current_end_date"), today),
            status=status,
            is_active=(status == "active"),
        )


class ActiveContractsResponse(_Model):
    count: int | None = None             # entity_profile_gold.active_award_count
    total_obligated: float | None = None  # entity_profile_gold.total_active_obligations
    agencies: list[str] = []             # distinct awarding agency over the returned items
    contracts: list[ActiveContract] = []


class OverviewResponse(_Model):
    uei: str | None = None
    lifetime_award_value: float | None = None    # total_lifetime_obligations
    active_contract_value: float | None = None   # total_active_obligations
    total_contract_count: int | None = None      # award_count
    active_contract_count: int | None = None     # active_award_count
    has_federal_awards: bool | None = None
    as_of_date: str | None = None                # profile_as_of_date

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "OverviewResponse":
        return cls(
            uei=r.get("uei"),
            lifetime_award_value=r.get("total_lifetime_obligations"),
            active_contract_value=r.get("total_active_obligations"),
            total_contract_count=r.get("award_count"),
            active_contract_count=r.get("active_award_count"),
            has_federal_awards=r.get("has_federal_awards"),
            as_of_date=_iso(r.get("profile_as_of_date")),
        )


class PastPerformanceResponse(_Model):
    closed_count: int | None = None              # award_count − active_award_count
    awards_lifetime: float | None = None         # total_lifetime_obligations
    contracts_lifetime: int | None = None        # award_count
    contracts: list[ActiveContract] = []         # status 'completed'
    cpars_ratings: list[Any] = []                # GAP — no CPARS dataset
    cpars_average_rating: float | None = None    # GAP
    exclusions_status: str | None = None         # GAP — no SAM exclusions dataset
    exclusions_last_checked: str | None = None   # GAP
    recompetes_in_12mo: int | None = None        # GAP — no option/recompete metadata


# ── Entity dossier (the cockpit side-panel read) ──────────────────────────────
class DossierIdentity(_Model):
    uei: str | None = None
    cage_code: str | None = None
    legal_business_name: str | None = None
    dba_name: str | None = None
    is_active: bool | None = None
    primary_naics: str | None = None
    address: Address | None = None


class DossierPosture(_Model):
    total_lifetime_obligations: float | None = None
    total_active_obligations: float | None = None
    award_count: int | None = None
    active_award_count: int | None = None
    has_federal_awards: bool | None = None
    latest_action_date: str | None = None        # greatest of CAS + the 90d action feed
    days_since_last_action: int | None = None
    top_agencies: list[TopAgency] = []           # contractor_award_summary slots 1–3
    profile_as_of_date: str | None = None


class RecentAwardAction(_Model):
    award_id: str | None = None
    action_date: str | None = None
    amount: float | None = None
    awarding_agency: str | None = None
    awarding_sub_agency: str | None = None
    winner_type: str | None = None
    pop_state: str | None = None
    pop_city: str | None = None
    set_aside: str | None = None
    naics_code: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "RecentAwardAction":
        return cls(
            award_id=r.get("award_id"),
            action_date=_iso(r.get("action_date")),
            amount=r.get("action_obligated_usd"),
            awarding_agency=r.get("awarding_agency"),
            awarding_sub_agency=r.get("awarding_sub_agency"),
            winner_type=r.get("winner_type"),
            pop_state=r.get("pop_state"),
            pop_city=r.get("pop_city"),
            set_aside=r.get("set_aside"),
            naics_code=r.get("naics_code"),
        )


class RecentActivity(_Model):
    """The honest recent-award feed: ``window_days`` states the rolling coverage so
    the UI can label it; an empty ``actions`` list is a truthful no-recent-activity
    state, never an error."""

    window_days: int = 90
    actions: list[RecentAwardAction] = []


# 6 SAM POC slots × (primary + alternate) — same bound sam_pocs_by_uei uses.
_DOSSIER_POC_CAP = 12


class EntityDossierResponse(_Model):
    """The composed side-panel dossier — entity_profile_gold (identity + rollups +
    POC slots) ⊕ contractor_award_summary (recency + top agencies) ⊕ the 90d award
    feed. POCs carry name/title/geo ONLY: the SAM source has no email/phone columns
    (verified — SamPoc.email/phone are declared GAPs), so no contact channel can leak."""

    identity: DossierIdentity
    posture: DossierPosture
    recent_activity: RecentActivity
    pocs: list[SamPoc] = []

    @classmethod
    def from_parts(cls, gold: dict[str, Any], cas: dict[str, Any] | None,
                   actions: list[dict[str, Any]], today: date) -> "EntityDossierResponse":
        identity = DossierIdentity(
            uei=gold.get("uei"),
            cage_code=gold.get("cage_code"),
            legal_business_name=gold.get("legal_business_name"),
            dba_name=gold.get("dba_name"),
            is_active=gold.get("is_active"),
            primary_naics=gold.get("primary_naics"),
            address=Address(
                street=gold.get("physical_address_line_1"),
                city=gold.get("physical_address_city"),
                state=gold.get("physical_address_state"),
                zip=gold.get("physical_address_zip_postal_code"),
            ),
        )
        # Latest action = greatest of the all-time prime summary date and the freshest
        # 90d action (covers subawards + any summary lag). Dates may arrive as date or str.
        candidates = [cas.get("prime_most_recent_action_date") if cas else None]
        candidates += [a.get("action_date") for a in actions]
        iso_dates = [d for d in (_iso(c) for c in candidates) if d]
        latest = max(iso_dates) if iso_dates else None
        # NOTE: _days_between is the expiry countdown (target − today); recency wants
        # the inverse, computed directly so a past date reads as positive days-ago.
        days_since = (today - date.fromisoformat(latest)).days if latest else None
        agencies: list[TopAgency] = []
        for name_key, dollar_key in (
            ("top_agency_1_name", "top_agency_1_dollars"),
            ("top_agency_2_name", "top_agency_2_dollars"),
            ("top_agency_3_name", "top_agency_3_dollars"),
        ):
            name = (cas or {}).get(name_key)
            if name:
                agencies.append(TopAgency(name=name, dollars=float((cas or {}).get(dollar_key) or 0.0)))
        posture = DossierPosture(
            total_lifetime_obligations=gold.get("total_lifetime_obligations"),
            total_active_obligations=gold.get("total_active_obligations"),
            award_count=gold.get("award_count"),
            active_award_count=gold.get("active_award_count"),
            has_federal_awards=gold.get("has_federal_awards"),
            latest_action_date=latest,
            days_since_last_action=days_since,
            top_agencies=agencies,
            profile_as_of_date=_iso(gold.get("profile_as_of_date")),
        )
        pocs = sorted(list(gold.get("pocs") or []), key=lambda r: (r.get("poc_slot_no") or 0))
        return cls(
            identity=identity,
            posture=posture,
            recent_activity=RecentActivity(actions=[RecentAwardAction.from_row(a) for a in actions]),
            pocs=[SamPoc.from_row(p) for p in pocs[:_DOSSIER_POC_CAP]],
        )


# ── Subawardee drill-down: capability profile + prime-contract (subaward) history ──
class SubawardHistoryItem(_Model):
    """One subaward the sub won under a prime contract (a contract_subaward row)."""

    prime_award_unique_key: str | None = None
    prime_award_piid: str | None = None
    prime_uei: str | None = None
    prime_name: str | None = None
    awarding_agency: str | None = None
    subaward_amount: float | None = None
    subaward_number: str | None = None
    action_date: str | None = None
    description: str | None = None       # sub-self-reported (the firm's own SAM subaward report)
    naics_code: str | None = None
    usaspending_permalink: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "SubawardHistoryItem":
        amt = r.get("subaward_amount")
        try:
            amt = float(amt) if amt not in (None, "") else None
        except (TypeError, ValueError):
            amt = None
        adt = r.get("subaward_action_date")
        return cls(
            prime_award_unique_key=r.get("prime_award_unique_key"),
            prime_award_piid=r.get("prime_award_piid"),
            prime_uei=r.get("prime_awardee_uei"),
            prime_name=r.get("prime_awardee_name"),
            awarding_agency=r.get("prime_award_awarding_agency_name"),
            subaward_amount=amt,
            subaward_number=r.get("subaward_number"),
            action_date=(str(adt) if adt not in (None, "") else None),
            description=r.get("subaward_description"),
            naics_code=r.get("prime_award_naics_code"),
            usaspending_permalink=r.get("usaspending_permalink"),
        )


class SubawardCapabilities(_Model):
    """Structured capability block — present only for subs in the bridge universe (profiled=true)."""

    has_extracted_scope: bool = False
    scope_summary: str | None = None
    solicitation_scope_tags: list[str] = []
    requires_clearance: bool = False
    req_clearance_level_max: str | None = None
    requires_cmmc: bool = False
    req_cert_tags: list[str] = []
    top_labor_categories: list[str] = []
    n_scope_solicitations: int | None = None
    # Path B — self-reported capability (the sub's OWN subaward descriptions), separate provenance from
    # the scope-derived solicitation_scope_tags above. Present for the full universe, not just bridge subs.
    subaward_description_tags: list[str] = []
    n_subaward_description_tags: int | None = None
    tag_source: str | None = None   # scope | self_reported | both | none


class SubawardTeaming(_Model):
    n_teaming_primes: int | None = None
    teaming_dollars_5y: float | None = None
    teaming_prime_names: list[str] = []


class SubawardProfileResponse(_Model):
    """Subawardee drill-down: who the sub is + what it can do (profile) + the prime contracts it won
    work under (contract_subaward). ``profiled`` is true when the capability block is populated (the sub
    teamed under a tracked solicitation); identity + ``subaward_history`` are present for any sub with
    fact rows. Carries no solicitation chunk verbatim (CUI-safe by construction)."""

    sub_uei: str | None = None
    sub_name: str | None = None
    profiled: bool = False
    n_subawards: int | None = None                 # full-corpus (profile) when profiled, else null
    n_distinct_primes: int | None = None
    total_subaward_amount: float | None = None
    subaward_history_count: int | None = None      # rows returned (capped — "shown", not total)
    hq_state: str | None = None                    # sub's own reported address (dominant)
    hq_city: str | None = None
    pop_state: str | None = None                   # dominant place-of-performance state
    pop_states: list[str] = []                     # all distinct PoP states the sub performed in
    capabilities: SubawardCapabilities | None = None
    teaming: SubawardTeaming | None = None
    poc: SamPoc | None = None
    subaward_history: list[SubawardHistoryItem] = []

    @classmethod
    def from_parts(
        cls, uei: str, profile: dict[str, Any] | None, history: list[dict[str, Any]]
    ) -> "SubawardProfileResponse":
        prof = profile or {}
        profiled = profile is not None
        sub_name = prof.get("sub_name") or (history[0].get("subawardee_name") if history else None)
        capabilities = None
        teaming = None
        poc = None
        if profiled:
            capabilities = SubawardCapabilities(
                has_extracted_scope=bool(prof.get("has_extracted_scope")),
                scope_summary=prof.get("scope_summary"),
                solicitation_scope_tags=[t for t in (prof.get("solicitation_scope_tags") or []) if t],
                requires_clearance=bool(prof.get("requires_clearance")),
                req_clearance_level_max=prof.get("req_clearance_level_max"),
                requires_cmmc=bool(prof.get("requires_cmmc")),
                req_cert_tags=[t for t in (prof.get("req_cert_tags") or []) if t],
                top_labor_categories=[t for t in (prof.get("top_labor_categories") or []) if t],
                n_scope_solicitations=prof.get("n_scope_solicitations"),
                subaward_description_tags=[t for t in (prof.get("subaward_description_tags") or []) if t],
                n_subaward_description_tags=prof.get("n_subaward_description_tags"),
                tag_source=prof.get("tag_source"),
            )
            teaming = SubawardTeaming(
                n_teaming_primes=prof.get("n_teaming_primes"),
                teaming_dollars_5y=prof.get("teaming_dollars_5y"),
                teaming_prime_names=[t for t in (prof.get("teaming_prime_names") or []) if t],
            )
            if prof.get("poc_available"):
                poc = SamPoc(
                    full_name=prof.get("poc_full_name"), title=prof.get("poc_title"),
                    city=prof.get("poc_city"), state=prof.get("poc_state"),
                )
        return cls(
            sub_uei=uei,
            sub_name=sub_name,
            profiled=profiled,
            n_subawards=prof.get("n_subawards") if profiled else None,
            n_distinct_primes=prof.get("n_distinct_primes_subaward") if profiled else None,
            total_subaward_amount=prof.get("total_subaward_amount") if profiled else None,
            subaward_history_count=len(history),
            hq_state=prof.get("hq_state"),
            hq_city=prof.get("hq_city"),
            pop_state=prof.get("pop_state"),
            pop_states=[s for s in (prof.get("pop_states") or []) if s],
            capabilities=capabilities,
            teaming=teaming,
            poc=poc,
            subaward_history=[SubawardHistoryItem.from_row(h) for h in history],
        )


# ── Person-by-LinkedIn surface (/api/v1/people/by-linkedin) ──────────────────
class PersonByLinkedInRequest(_Model):
    """POST body for /api/v1/people/by-linkedin — the LinkedIn URL travels in the body
    (no query string, no path value). ``url`` is the person's linkedin.com/in/<slug> URL.
    Optional (defaults to empty) so a missing/blank value yields a 200 ``found: false`` rather
    than a 422 — an enrichment row must never hard-fail on a bad input cell."""

    url: str = ""


class PersonMatch(_Model):
    """One ``active/people`` row matched on the LinkedIn URL. A person URL can recur
    (same person re-observed, or present at multiple companies), so every distinct match
    is surfaced rather than collapsed — the caller sees title/company conflicts honestly."""

    title: str | None = None
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    company_id: str | None = None
    normalized_domain: str | None = None
    contact_id: str | None = None
    person_id: str | None = None
    # source_platform moved to the person_source_platforms sidecar. A canonical person can be
    # observed under MULTIPLE sources, so this is a LIST (richer than the old single value),
    # populated from a sidecar join by canonical_person_id. Empty when the sidecar has no row.
    source_platforms: list[str] = []

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "PersonMatch":
        # canonical_person_id is the go-forward person key (sha256 of the canonical LinkedIn
        # URL) — it is returned AS person_id. The response's contact_id is kept for external
        # back-compat and MIRRORS person_id. The physical contact_id column no longer exists.
        pid = r.get("canonical_person_id") or r.get("person_id")
        return cls(
            title=r.get("title"),
            full_name=r.get("full_name"),
            first_name=r.get("first_name"),
            last_name=r.get("last_name"),
            company_id=r.get("company_id"),
            normalized_domain=r.get("normalized_domain"),
            contact_id=pid,
            person_id=pid,
            source_platforms=r.get("source_platforms") or [],
        )


class PersonByLinkedInResponse(_Model):
    """LinkedIn URL → person payload from ``active/people``. ``title`` is the headline the
    caller asked for (the first non-null title across matches); ``fullName`` is its
    companion identity. ``matches`` carries every row the URL resolved to (with
    ``matchCount``), so duplicate/conflicting observations stay visible. ``found`` is False
    only on a 200 produced by a non-resolving (but well-formed) probe — the route 404s the
    no-match case, so in practice ``found`` is always True on a 200."""

    linkedin_url: str
    found: bool
    title: str | None = None
    full_name: str | None = None
    match_count: int = 0
    matches: list[PersonMatch] = []

    @classmethod
    def from_rows(cls, linkedin_url: str, rows: list[dict[str, Any]]) -> "PersonByLinkedInResponse":
        matches = [PersonMatch.from_row(r) for r in rows]
        title = next((m.title for m in matches if m.title), None)
        full_name = next((m.full_name for m in matches if m.full_name), None)
        return cls(
            linkedin_url=linkedin_url,
            found=bool(matches),
            title=title,
            full_name=full_name,
            match_count=len(matches),
            matches=matches,
        )


# Capability profile (the per-firm card object: identity + activity + lanes).
# Assembled from capability_profile (1 row/UEI). count/dollars are normalized across the sub
# side (subawards/amount) and prime side (awards/obligated). Digit-suffixed fields carry an
# explicit clean alias (amount5y) - the to_camel default would emit amount5Y.
from pydantic import Field as _F


class CapPartner(_Model):
    name: str | None = None
    uei: str | None = None
    count: int | None = None
    dollars: float | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "CapPartner":
        return cls(name=r.get("name"), uei=(r.get("uei") or None),
                   count=r.get("subawards"), dollars=r.get("amount"))


class CapCodeStat(_Model):
    code: str | None = None
    description: str | None = None
    count: int | None = None
    dollars: float | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any], count_key: str, dollar_key: str) -> "CapCodeStat":
        return cls(code=r.get("code"), description=r.get("description"),
                   count=r.get(count_key), dollars=r.get(dollar_key))


class CapAgency(_Model):
    agency: str | None = None
    sub_agency: str | None = None
    count: int | None = None
    dollars: float | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "CapAgency":
        return cls(agency=r.get("agency"), sub_agency=r.get("subagency"),
                   count=r.get("awards"), dollars=r.get("obligated"))


class CapLane(_Model):
    """One recommended NAICS+PSC lane. evidence_tier is the why (primed-direct >
    subbed-hop > primed-hop > declared); top_primes are the primes who sub it out."""

    rank: int | None = None
    evidence_tier: str | None = None
    naics: str | None = None
    psc: str | None = None
    naics_description: str | None = None
    psc_description: str | None = None
    score: float | None = None
    lane_primes: int | None = None
    lane_median_amount: float | None = None
    top_primes: list[str] = []

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "CapLane":
        return cls(
            rank=r.get("rank"), evidence_tier=r.get("evidence_tier"),
            naics=r.get("dst_naics"), psc=r.get("dst_psc"),
            naics_description=r.get("naics_desc"), psc_description=r.get("psc_desc"),
            score=r.get("score"), lane_primes=r.get("lane_n_primes"),
            lane_median_amount=r.get("lane_median_amt"),
            top_primes=[p for p in (r.get("top_primes") or []) if p],
        )


class SubActivity(_Model):
    amount_5y: float | None = _F(default=None, alias="amount5y")
    subawards_5y: int | None = _F(default=None, alias="subawards5y")
    distinct_primes_5y: int | None = _F(default=None, alias="distinctPrimes5y")
    distinct_prime_partners_5y: int | None = _F(default=None, alias="distinctPrimePartners5y")
    recent_subawards_90d: int | None = _F(default=None, alias="recentSubawards90d")
    recent_subaward_amount_90d: float | None = _F(default=None, alias="recentSubawardAmount90d")
    recent_latest_action_date: str | None = None
    recent_top_prime_name: str | None = None
    recent_top_naics_code: str | None = None
    recent_top_naics_description: str | None = None
    recent_subaward_scope: str | None = None
    top_prime_partners: list[CapPartner] = []
    top_naics: list[CapCodeStat] = []

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "SubActivity":
        return cls(
            amount_5y=r.get("sub_amount_5y"),
            subawards_5y=r.get("sub_received_5y"),
            distinct_primes_5y=r.get("sub_distinct_primes_5y"),
            distinct_prime_partners_5y=r.get("sub_distinct_prime_partners_5y"),
            recent_subawards_90d=r.get("recent_subawards_90d"),
            recent_subaward_amount_90d=r.get("recent_subaward_amount_90d"),
            recent_latest_action_date=_iso(r.get("recent_latest_action_date")),
            recent_top_prime_name=r.get("recent_top_prime_name"),
            recent_top_naics_code=r.get("recent_top_naics_code"),
            recent_top_naics_description=r.get("recent_top_naics_description"),
            recent_subaward_scope=r.get("recent_subaward_scope"),
            top_prime_partners=[CapPartner.from_row(p) for p in (r.get("sub_top_prime_partners") or [])],
            top_naics=[CapCodeStat.from_row(n, "subawards", "amount") for n in (r.get("sub_top_naics") or [])],
        )


class PrimeActivity(_Model):
    awards_5y: int | None = _F(default=None, alias="awards5y")
    obligated_5y: float | None = _F(default=None, alias="obligated5y")
    competed_awards_5y: int | None = _F(default=None, alias="competedAwards5y")
    distinct_naics_5y: int | None = _F(default=None, alias="distinctNaics5y")
    top_naics: list[CapCodeStat] = []
    top_psc: list[CapCodeStat] = []
    top_agencies: list[CapAgency] = []

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "PrimeActivity":
        return cls(
            awards_5y=r.get("prime_awards_5y"),
            obligated_5y=r.get("prime_obligated_5y"),
            competed_awards_5y=r.get("prime_competed_awards_5y"),
            distinct_naics_5y=r.get("prime_distinct_naics_5y"),
            top_naics=[CapCodeStat.from_row(n, "awards", "obligated") for n in (r.get("prime_top_naics") or [])],
            top_psc=[CapCodeStat.from_row(n, "awards", "obligated") for n in (r.get("prime_top_psc") or [])],
            top_agencies=[CapAgency.from_row(a) for a in (r.get("prime_top_agencies") or [])],
        )


class CapabilityProfileResponse(_Model):
    """The full per-firm card: identity + designations + sub/prime activity + the
    evidence-tiered recommended lanes. sub_activity is None when the firm never subbed;
    prime_activity is None with no prime history. federal_status makes role a status."""

    uei: str | None = None
    firm_name: str | None = None
    state_code: str | None = None
    parent_uei: str | None = None
    federal_status: str | None = None
    is_dsbs: bool | None = None
    has_sub_history: bool | None = None
    has_prime_history: bool | None = None
    designations: list[str] = []
    sub_activity: SubActivity | None = None
    prime_activity: PrimeActivity | None = None
    recommended_lanes: list[CapLane] = []
    n_recommended_lanes: int | None = None
    top_evidence_tier: str | None = None
    materialized_at: str | None = None

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> "CapabilityProfileResponse":
        return cls(
            uei=r.get("uei"), firm_name=r.get("firm_name"), state_code=r.get("state_code"),
            parent_uei=(r.get("parent_uei") or None), federal_status=r.get("federal_status"),
            is_dsbs=r.get("is_dsbs"), has_sub_history=r.get("has_sub_history"),
            has_prime_history=r.get("has_prime_history"),
            designations=[d for d in (r.get("designations") or []) if d],
            sub_activity=SubActivity.from_row(r) if r.get("has_sub_history") else None,
            prime_activity=PrimeActivity.from_row(r) if r.get("has_prime_history") else None,
            recommended_lanes=[CapLane.from_row(lane) for lane in (r.get("recommended_lanes") or [])],
            n_recommended_lanes=r.get("n_recommended_lanes"),
            top_evidence_tier=r.get("top_evidence_tier"),
            materialized_at=_iso(r.get("materialized_at")),
        )
