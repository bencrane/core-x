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
    registration_status: str | None = None      # DERIVED: 'active' | 'expired'
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
        exp = entity.get("expiration_date")
        primary = (entity.get("primary_naics") or None)
        naics = [c for c in (entity.get("naics_codes") or []) if c]
        secondary = [c for c in naics if c != primary]
        return cls(
            uei=entity.get("uei"),
            cage_code=entity.get("cage_code"),
            legal_business_name=entity.get("legal_business_name"),
            registration_status=("expired" if isinstance(exp, date) and exp < today else "active"),
            registration_date=_iso(entity.get("registration_date")),
            activation_date=_iso(entity.get("activation_date")),
            expiration_date=_iso(exp),
            days_until_expiration=_days_between(exp, today),
            primary_naics=primary,
            secondary_naics=secondary,
            psc_codes=[c for c in (entity.get("psc_codes") or []) if c],
            business_types_raw=entity.get("business_types"),
            physical_address=Address(
                city=entity.get("physical_city"),
                state=entity.get("physical_state"),
                zip=entity.get("physical_zip5"),
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
