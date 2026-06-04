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
