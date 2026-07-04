"""Awards subcontract-pricing estimates — empirical sub-carve-out predictions per combo lane.

Serves the canonical subcontract-pricing foundation (combo_prime_sub_pricing, firm_award_value_profile,
firm_combo_prime_profile Lance datasets materialized from 108M prime txns + 627k FSRS subawards 2021+).

Endpoints (mounted at ``/api/v1/awards/subk-pricing``, service-token gated):
  GET  /summary            → meta (prime pool, est subcontractable, blended sub-out rate, definitions)
  GET  /active-demand      → all combos (naics, psc) in active demand with pricing stats
  GET  /combo/{naics}/{psc}  → one combo's pricing distribution + award count + sub-out ratio

Each combo carries:
  prime_pool, est_sub_carveout, sub_to_prime_ratio, typical_sub_size, n_awards (active demand count)

DEFINITIONS:
  prime_award_value: sum(federal_action_obligation) per contract_award_unique_key; distributions over value>0
  sub_to_prime_ratio: sum(subaward_amount 2021+) / sum(prime obligated 2021+) per combo
  est_sub_carveout: active total_dollars_obligated * least(combo ratio, 1.0)
"""
from __future__ import annotations

import json
import logging
from typing import Any

import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException

from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/awards/subk-pricing", tags=["awards-subk-pricing"])

# ── module-level cache: load parquet + summary JSON on first use ──

_PARQUET_PATH = "/Users/benjamincrane/core-x/.claude/worktrees/subk-pricing/reports/dsbs_overlap/active_demand_subk_estimate.parquet"
_SUMMARY_JSON_PATH = "/Users/benjamincrane/core-x/.claude/worktrees/subk-pricing/reports/dsbs_overlap/subk_pricing_summary.json"

_parquet_cache: dict[str, Any] | None = None
_summary_cache: dict[str, Any] | None = None


def _load_parquet() -> list[dict]:
    """Lazy load parquet → list of combo dicts."""
    global _parquet_cache
    if _parquet_cache is not None:
        return _parquet_cache
    try:
        table = pq.read_table(_PARQUET_PATH)
        _parquet_cache = table.to_pylist()
        logger.info(f"Loaded {len(_parquet_cache)} combos from {_PARQUET_PATH}")
        return _parquet_cache
    except Exception as e:
        logger.error(f"Failed to load parquet: {e}")
        raise HTTPException(status_code=503, detail="Parquet data unavailable")


def _load_summary() -> dict:
    """Lazy load summary JSON."""
    global _summary_cache
    if _summary_cache is not None:
        return _summary_cache
    try:
        with open(_SUMMARY_JSON_PATH) as f:
            _summary_cache = json.load(f)
        logger.info(f"Loaded summary from {_SUMMARY_JSON_PATH}")
        return _summary_cache
    except Exception as e:
        logger.error(f"Failed to load summary JSON: {e}")
        raise HTTPException(status_code=503, detail="Summary data unavailable")


@router.get("/summary", dependencies=[Depends(require_service_token)])
async def get_summary() -> dict[str, Any]:
    """Metadata: prime pool, estimated subcontractable, blended sub-out rate, definitions."""
    return _load_summary()


@router.get("/active-demand", dependencies=[Depends(require_service_token)])
async def get_active_demand(limit: int = 500, offset: int = 0) -> dict[str, Any]:
    """All combos (naics, psc) in active demand with pricing stats. Paginated."""
    combos = _load_parquet()
    return {
        "total": len(combos),
        "limit": min(max(limit, 1), 1000),
        "offset": offset,
        "combos": combos[offset : offset + min(max(limit, 1), 1000)]
    }


@router.get("/combo/{naics}/{psc}", dependencies=[Depends(require_service_token)])
async def get_combo(naics: str, psc: str) -> dict[str, Any] | None:
    """One combo's pricing distribution + award count + sub-out ratio."""
    combos = _load_parquet()
    for combo in combos:
        if combo["naics"] == naics and combo["psc"] == psc:
            return combo
    raise HTTPException(status_code=404, detail=f"Combo {naics}/{psc} not found")
