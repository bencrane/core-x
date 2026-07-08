"""sub_universe_serve — the Surface-1 SERVING read path (fetch, not build).

Two indexed point-lookups; no raw-spine access, ever.

  • fetch_blob(uei) -> the HOT blob (gtm_sub_universe_blobs, BTREE uei). ONE fetch
    per call session; ALL sub_universe_node predicates run in-memory over it.

  • fetch_node_detail(uei, node_uei) -> the row-exact drilldown for one node: raw
    event grain (restricted to the target's matched combos, so it reconciles with
    the node's hot monthly buckets) + win_portfolio. The §1 carve-out — ONE
    additional indexed PRECOMPUTE fetch, on drilldown only. It reads the EXISTING
    marts gtm_prime_demand_events (BTREE uei) and gtm_prime_combo_lanes (BTREE uei)
    by point-lookup; those are precompute (the award-event pulse / winners layers),
    not the live spine. No separate events sidecar dataset exists — one would
    duplicate ~106 MB of event rows per target across overlapping universes.
"""
from __future__ import annotations

import json
from typing import Any

import lance

from . import config
from .sub_universe_full import EVENT_ROWS_PER_NODE_CAP, WIN_PORTFOLIO_CAP

_DRILL_EVENT_COLS = ["uei", "award_key", "action_date", "obligation_delta",
                     "naics_code", "psc_code", "action_type_code", "award_type_code",
                     "subcontracting_plan", "type_of_set_aside_code", "extent_competed",
                     "idv_type_code", "is_first_action", "has_disclosed_subs"]


def _dataset(uri: str):
    return lance.dataset(uri, storage_options=config.r2_storage_options())


def _pt(uri: str, col: str, val: str, columns: list[str]) -> list[dict[str, Any]]:
    """Indexed point-lookup on a BTREE column."""
    return (_dataset(uri).scanner(columns=columns, filter=f"{col} = '{val}'")
            .to_table().to_pylist())


def fetch_blob(uei: str) -> dict[str, Any] | None:
    uei = (uei or "").strip().upper()
    if not uei:
        return None
    rows = _pt(config.GTM_SUB_UNIVERSE_BLOBS_URI, "uei", uei, ["blob"])
    return json.loads(rows[0]["blob"]) if rows else None


def _node_combos(blob: dict[str, Any], node_uei: str) -> set[tuple[str, str]] | None:
    """The target's matched combos for a node, from the hot blob — the same
    restriction the node's buckets were built under."""
    for n in blob.get("universe", {}).get("nodes", []):
        if n.get("uei") == node_uei:
            combos = {tuple(k.split("x", 1)) for k in (n.get("gate_facts") or {})}
            for m in (n.get("matched_via") or []):
                combos.add((m.get("naics_code"), m.get("psc_code")))
            return combos
    return None


def fetch_node_detail(uei: str, node_uei: str,
                      blob: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Row-exact drilldown for one node. Pass the already-fetched `blob` to avoid
    a second blob read (serve model = load blob once per session)."""
    uei = (uei or "").strip().upper()
    node_uei = (node_uei or "").strip().upper()
    if not uei or not node_uei:
        return None
    if blob is None:
        blob = fetch_blob(uei)
    combos = _node_combos(blob, node_uei) if blob else None

    ev = _pt(config.GTM_PRIME_DEMAND_EVENTS_URI, "uei", node_uei, _DRILL_EVENT_COLS)
    if combos is not None:
        ev = [e for e in ev if (e["naics_code"], e["psc_code"]) in combos]
    ev.sort(key=lambda e: str(e.get("action_date") or ""), reverse=True)
    events = [{k: (str(e[k])[:10] if k == "action_date" and e[k] is not None else e[k])
               for k in _DRILL_EVENT_COLS if k != "uei"}
              for e in ev[:EVENT_ROWS_PER_NODE_CAP]]

    cl = _pt(config.GTM_PRIME_COMBO_LANES_URI, "uei", node_uei,
             ["naics_code", "psc_code", "prime_obl_60mo"])
    cl.sort(key=lambda r: -(float(r["prime_obl_60mo"] or 0)))
    portfolio = [{"combo": f"{r['naics_code']}x{r['psc_code']}", "naics_code": r["naics_code"],
                  "psc_code": r["psc_code"], "prime_obl_60mo": round(float(r["prime_obl_60mo"] or 0), 2)}
                 for r in cl[:WIN_PORTFOLIO_CAP]]

    return {"target_uei": uei, "node_uei": node_uei,
            "events": events, "events_truncated": len(ev) > EVENT_ROWS_PER_NODE_CAP,
            "win_portfolio": portfolio, "win_portfolio_truncated": len(cl) > WIN_PORTFOLIO_CAP}
