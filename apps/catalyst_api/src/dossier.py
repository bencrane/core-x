"""Composed entity dossier — the per-UEI unit of work behind the batch endpoint.

``compose_dossier`` runs the SAME three lookups + ``EntityDossierResponse.from_parts``
composition the single ``/entities/{uei}/dossier`` route performs, but SERIALLY: each
batch worker owns one UEI end-to-end (submitting per-lookup sub-tasks into the same
pool a batch task runs on is the classic nested-submit deadlock). Payload parity with
the single route is by construction — both terminate in ``from_parts``.

Pure-by-injection: the lookups live in ``lance_store`` (warm handle cache underneath),
so tests monkeypatch them and exercise everything here with no R2 and no FastAPI.
"""

from __future__ import annotations

from datetime import date

from . import lance_store
from .models import EntityDossierResponse

# Per-call UEI ceiling. Sized with the batch pool (main._BATCH_POOL, 8 workers): warm
# BTREE-resident lookups run tens-of-ms, so 100 UEIs ≈ (100/8)·~70 ms ≈ under a second
# — and the frontend prefetcher chunks at 50, so the cap is headroom, not a target.
BATCH_MAX_UEIS = 100
ACTIONS_DEFAULT = 10
ACTIONS_MAX = 25


def prepare_batch(ueis: object) -> tuple[list[str] | None, str | None]:
    """Validate + normalize a batch request's UEI list: trims, drops empties, dedupes
    PRESERVING ORDER (callers send ranked result sets), enforces the per-call bound.
    Returns ``(ordered_unique, None)`` or ``(None, reason)`` for a 422."""
    if not isinstance(ueis, list) or not ueis:
        return None, "ueis must be a non-empty array"
    trimmed = [str(u).strip() for u in ueis]
    if len(trimmed) > BATCH_MAX_UEIS:
        return None, f"at most {BATCH_MAX_UEIS} ueis per call"
    seen: set[str] = set()
    out: list[str] = []
    for u in trimmed:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    if not out:
        return None, "ueis must be a non-empty array"
    return out, None


def compose_dossier(uei: str, actions: int = ACTIONS_DEFAULT,
                    today: date | None = None) -> EntityDossierResponse | None:
    """One entity's composed dossier, or ``None`` for an invalid-format or unknown
    UEI — partial success by contract, this never raises for a bad entry. Identical
    composition to the single route (gold ⊕ award summary ⊕ rolling-90d actions)."""
    today = today or date.today()
    if not lance_store.valid_uei(uei):
        return None
    gold = lance_store.entity_dossier_gold(uei)
    if gold is None:
        return None
    cas = lance_store.award_summary_by_uei(uei)
    recent = lance_store.recent_award_actions_by_uei(uei, max(1, min(actions, ACTIONS_MAX)))
    return EntityDossierResponse.from_parts(gold, cas, recent, today)
