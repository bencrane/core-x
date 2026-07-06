"""Operator entity profile — the MAXIMAL per-UEI surface (everything we hold, one page).

PURPOSE: the curation substrate. This page deliberately shows EVERY per-entity read the
platform can serve — the operator slims the cold-call and prospect surfaces DOWN from it
by deletion, never by wondering what else exists. Each section is labeled with its SOURCE
DATASET and freshness caveats, and carries its raw rows in a collapsible block so nothing
is hidden by the rendering.

Two routes serve the same composition (main.py):
  GET /profile/{uei}?token=...        self-contained HTML (this module's render). Token
                                      REQUIRED when the operator token is set — the page
                                      carries person contact assets, it is never open.
  GET /api/v1/entities/{uei}/profile  the JSON twin (bearer) — the assembly the later
                                      operator/prospect surfaces project from.

DESIGN POSTURE: deliberately NOT the rare-structure cockpit aesthetic and not meant to
match any product design system — a plain, dense, printable document (same stance as
card_html.py, the /card prototype).

COMPOSITION (compose_profile): ~10 independent BTREE point-reads fanned out on a module
pool + one IN-PROCESS subout recipe call. Every section is best-effort: an unreachable
dataset renders as an error note in that section, never a bricked page (the max surface
must show whatever exists). Sections:
  sam_entity      gtm_sam_entities (identity, registration, designholder flags)
  rollup          gtm_entity_behavior_rollup v2 (fresh posture incl. active-award columns)
  geo             gtm_entity_geo (HQ point + precision)
  firmographics   firmographics_blitz via normalized_domain (employee band etc.)
  people          gtm_sam_people × gtm_sam_person_contactability (roles + verbatim
                  contact assets; capped, mention-ranked)
  lanes           gtm_entity_code_lanes (demonstrated, both sides, $-ranked)
  inferred        gtm_entity_inferred_{primeable,subbable}_codes (top-N by support;
                  cooccurrence evidence, NOT demonstration — verb doctrine)
  subout          subout_opportunities recipe IN-PROCESS (top-N scored open awards,
                  components + evidence verbatim; peers when include_peers)
  legacy_card     capability_profile (the 2026-07-01 card — pre-spine/pre-trio; labeled)
  legacy_gold     entity_profile_gold (active counts predate the PoP fix; labeled)
"""
from __future__ import annotations

import html
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date as dt_date
from typing import Any, Callable

from . import config, subout_store
from .lance_store import _dataset, _map_jsonable, _sql_str

log = logging.getLogger("catalyst_api.profile")

# Independent point-reads per compose; IO-bound R2 waits (pylance releases the GIL).
_PROFILE_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="profile")

PEOPLE_CAP = 40          # mention-ranked; the raw block states the true count
LANES_CAP = 60           # $-ranked per side
INFERRED_CAP = 15        # per direction, support-ranked
SUBOUT_LIMIT = 10


# ── I/O seam (monkeypatch target for the hermetic tests) ──────────────────────
def _rows(uri: str, predicate: str, columns: list[str] | None = None) -> list[dict[str, Any]]:
    """One fresh filtered scanner → rows. Every caller passes a BTREE point predicate."""
    return _dataset(uri).scanner(columns=columns, filter=predicate).to_table().to_pylist()


def _jsonable(value: Any) -> Any:
    """Deep JSON-shape (dates → ISO) so sections serialize for both routes."""
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return _map_jsonable(value)


def _section(source: str, note: str | None, loader: Callable[[], Any]) -> dict[str, Any]:
    """Run one section loader best-effort. The max surface never bricks on one dataset."""
    out: dict[str, Any] = {"source": source}
    if note:
        out["note"] = note
    try:
        out["data"] = _jsonable(loader())
    except Exception as exc:  # noqa: BLE001 — degraded section, page still serves
        log.warning("profile section %s failed: %s", source, exc)
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["data"] = None
    return out


# ── Section loaders ────────────────────────────────────────────────────────────
def _one_row(uri: str, key: str, uei: str) -> dict[str, Any] | None:
    rows = _rows(uri, f"{key} = {_sql_str(uei)}")
    return rows[0] if rows else None


def _load_people(uei: str) -> dict[str, Any]:
    people = _rows(config.GTM_SAM_PEOPLE_URI, f"uei = {_sql_str(uei)}")
    people.sort(key=lambda p: (-(p.get("n_mentions") or 0), p.get("display_name") or ""))
    total = len(people)
    people = people[:PEOPLE_CAP]
    contact_by_person: dict[Any, dict[str, Any]] = {}
    if people:
        contacts = _rows(config.GTM_SAM_PERSON_CONTACTABILITY_URI, f"uei = {_sql_str(uei)}")
        contact_by_person = {c.get("sam_person_id"): c for c in contacts}
    for p in people:
        p["contact"] = contact_by_person.get(p.get("sam_person_id"))
    return {"total_people": total, "people": people}


def _load_lanes(uei: str) -> dict[str, Any]:
    lanes = _rows(config.GTM_ENTITY_CODE_LANES_URI, f"uei = {_sql_str(uei)}")
    lanes.sort(key=lambda r: (r.get("side") or "", -(r.get("obl_lifetime") or 0.0)))
    return {"total_lanes": len(lanes), "lanes": lanes[:LANES_CAP]}


def _load_inferred(uri: str, uei: str) -> dict[str, Any]:
    rows = _rows(uri, f"uei = {_sql_str(uei)}",
                 ["uei", "code_type", "code", "supporting_bothsider_firm_ct"])
    rows.sort(key=lambda r: (-(r.get("supporting_bothsider_firm_ct") or 0), r.get("code") or ""))
    return {"total_codes": len(rows), "codes": rows[:INFERRED_CAP]}


def _load_firmographics(sam_entity: dict[str, Any] | None) -> dict[str, Any] | None:
    domain = (sam_entity or {}).get("normalized_domain")
    if not domain:
        return None
    rows = _rows(config.FIRMOGRAPHICS_URI, f"domain_norm = {_sql_str(domain)}")
    return rows[0] if rows else None


def _load_subout(uei: str, include_peers: bool) -> dict[str, Any]:
    return subout_store.execute_subout_opportunities(
        {"uei": uei, "limit": SUBOUT_LIMIT, "include_peers": include_peers})


def compose_profile(uei: str, include_peers: bool = False) -> dict[str, Any]:
    """The full assembly — every per-entity read, fanned out, best-effort per section."""
    pool = _PROFILE_POOL
    f_sam = pool.submit(_one_row, config.GTM_SAM_ENTITIES_URI, "uei", uei)
    f_rollup = pool.submit(_one_row, config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI, "uei", uei)
    f_geo = pool.submit(_one_row, config.GTM_ENTITY_GEO_URI, "uei", uei)
    f_people = pool.submit(_load_people, uei)
    f_lanes = pool.submit(_load_lanes, uei)
    f_inf_p = pool.submit(_load_inferred, config.GTM_INFERRED_PRIMEABLE_URI, uei)
    f_inf_s = pool.submit(_load_inferred, config.GTM_INFERRED_SUBBABLE_URI, uei)
    f_card = pool.submit(_one_row, config.CAPABILITY_PROFILE_URI, "uei", uei)
    f_gold = pool.submit(_one_row, config.ENTITY_PROFILE_GOLD_URI, "uei", uei)
    f_subout = pool.submit(_load_subout, uei, include_peers)

    sam_section = _section("gtm_sam_entities", None, f_sam.result)
    sections: dict[str, Any] = {
        "sam_entity": sam_section,
        "rollup": _section(
            "gtm_entity_behavior_rollup",
            "v2 — spine-fresh; active-posture columns exact vs the rebuilt award spine",
            f_rollup.result),
        "geo": _section("gtm_entity_geo", None, f_geo.result),
        "firmographics": _section(
            "firmographics_blitz (via normalized_domain)",
            "domain-join; absent when the entity has no known domain",
            lambda: _load_firmographics(sam_section.get("data"))),
        "people": _section(
            "gtm_sam_people × gtm_sam_person_contactability",
            "contact assets are PROVIDER VALUES VERBATIM; phone_status='found' is the "
            "dialable filter",
            f_people.result),
        "lanes": _section(
            "gtm_entity_code_lanes",
            "demonstrated only — side='sub' is the PRIME award's code on subawards the "
            "firm delivered under, never a claim of the firm's own work",
            f_lanes.result),
        "inferred_primeable": _section(
            "gtm_entity_inferred_primeable_codes",
            "cooccurrence evidence, NOT a demonstration; support is a pair-sum",
            f_inf_p.result),
        "inferred_subbable": _section(
            "gtm_entity_inferred_subbable_codes",
            "cooccurrence evidence, NOT a demonstration; support is a pair-sum",
            f_inf_s.result),
        "subout_opportunities": _section(
            f"recipe {subout_store.RECIPE_ID} (in-process)",
            "live scored open awards — components + evidence verbatim from the recipe",
            f_subout.result),
        "legacy_capability_card": _section(
            "capability_profile",
            "LEGACY: materialized 2026-07-01 — pre-spine-rebuild, pre-inference-trio; "
            "lanes come from the superseded combo-hop recommender",
            f_card.result),
        "legacy_gold": _section(
            "entity_profile_gold",
            "LEGACY: active-award counts predate the PoP-date fix — suspect until rebuilt",
            f_gold.result),
    }
    return {
        "uei": uei,
        "generated_at": dt_date.today().isoformat(),
        "include_peers": include_peers,
        "sections": sections,
    }


# ── Render (plain, dense, printable — the card_html stance, maximal edition) ──
def _esc(v: Any) -> str:
    return html.escape(str(v)) if v is not None else "—"


def _usd(n: Any) -> str:
    if n is None:
        return "—"
    n = float(n)
    a = abs(n)
    if a >= 1e9:
        return f"${n / 1e9:.2f}B"
    if a >= 1e6:
        return f"${n / 1e6:.1f}M"
    if a >= 1e3:
        return f"${n / 1e3:.0f}K"
    return f"${n:.0f}"


def _raw_block(section: dict[str, Any]) -> str:
    """The nothing-hidden guarantee: every section carries its raw rows, collapsed."""
    payload = json.dumps(section.get("data"), indent=1, default=str)
    return (f"<details><summary>raw</summary><pre>{html.escape(payload)}</pre></details>")


def _sec_head(title: str, section: dict[str, Any]) -> str:
    note = f" · <em>{_esc(section['note'])}</em>" if section.get("note") else ""
    err = (f"<div class='err'>UNAVAILABLE — {_esc(section['error'])}</div>"
           if section.get("error") else "")
    return (f"<h2>{_esc(title)}</h2>"
            f"<div class='src'>source: <code>{_esc(section.get('source'))}</code>{note}</div>"
            f"{err}")


def _kv_table(row: dict[str, Any] | None, keys: list[str] | None = None) -> str:
    if not row:
        return "<div class='empty'>no row</div>"
    keys = keys or list(row.keys())
    cells = "".join(
        f"<tr><td class='k'>{_esc(k)}</td><td>{_esc(row.get(k))}</td></tr>" for k in keys)
    return f"<table>{cells}</table>"


def _people_table(data: dict[str, Any] | None) -> str:
    people = (data or {}).get("people") or []
    if not people:
        return "<div class='empty'>no people</div>"
    rows = []
    for p in people:
        c = p.get("contact") or {}
        roles = [label for flag, label in (
            ("is_govt_poc", "govt POC"), ("is_ebiz_poc", "ebiz POC"),
            ("is_past_perf_poc", "past-perf POC"), ("is_dsbs_contact", "DSBS contact"),
            ("is_dsbs_principal", "DSBS principal"),
            ("is_exec_officer_prime", "exec (prime)"), ("is_exec_officer_sub", "exec (sub)"),
        ) if p.get(flag)]
        phone = c.get("phone")
        phone_txt = (f"{phone} ({c.get('phone_status')})" if phone else "—")
        rows.append(
            "<tr>"
            f"<td>{_esc(p.get('display_name'))}</td>"
            f"<td>{_esc(p.get('best_title'))}</td>"
            f"<td>{_esc(', '.join(roles) or None)}</td>"
            f"<td>{_esc(phone_txt)}</td>"
            f"<td>{_esc(c.get('email'))}</td>"
            f"<td>{_esc(c.get('person_linkedin_url_norm'))}</td>"
            "</tr>")
    head = ("<tr><th>Name</th><th>Title</th><th>Roles</th><th>Mobile</th>"
            "<th>Email</th><th>LinkedIn</th></tr>")
    return f"<table>{head}{''.join(rows)}</table>"


def _lanes_table(data: dict[str, Any] | None) -> str:
    lanes = (data or {}).get("lanes") or []
    if not lanes:
        return "<div class='empty'>no lanes</div>"
    rows = "".join(
        "<tr>"
        f"<td>{_esc(r.get('side'))}</td><td>{_esc(r.get('code_type'))}</td>"
        f"<td>{_esc(r.get('code'))}</td><td class='n'>{_usd(r.get('obl_lifetime'))}</td>"
        "</tr>" for r in lanes)
    return ("<table><tr><th>Side</th><th>Type</th><th>Code</th><th>$ lifetime</th></tr>"
            f"{rows}</table>")


def _inferred_table(data: dict[str, Any] | None) -> str:
    codes = (data or {}).get("codes") or []
    if not codes:
        return "<div class='empty'>none</div>"
    rows = "".join(
        f"<tr><td>{_esc(r.get('code_type'))}</td><td>{_esc(r.get('code'))}</td>"
        f"<td class='n'>{_esc(r.get('supporting_bothsider_firm_ct'))}</td></tr>"
        for r in codes)
    return f"<table><tr><th>Type</th><th>Code</th><th>Support</th></tr>{rows}</table>"


def _subout_table(data: dict[str, Any] | None) -> str:
    opps = ((data or {}).get("data") or {}).get("opportunities") or []
    if not opps:
        meta = (data or {}).get("meta") or {}
        return f"<div class='empty'>no opportunities · {_esc(meta.get('reason'))}</div>"
    rows = []
    for o in opps:
        matched = ", ".join(sorted({f"{m.get('lens')}:{m.get('code')}"
                                    for m in (o.get("matched") or [])}))
        site = o.get("nearest_federal_site") or {}
        rows.append(
            "<tr>"
            f"<td class='n'>{_esc(round(o.get('score', 0), 3))}</td>"
            f"<td>{_esc(o.get('prime_name'))}</td>"
            f"<td>{_esc(o.get('award_id_piid'))}</td>"
            f"<td>{_esc(o.get('awarding_agency_name'))}</td>"
            f"<td class='n'>{_usd(o.get('total_obligation'))}</td>"
            f"<td>{_esc(o.get('period_of_performance_current_end_date') or o.get('ordering_period_end_date'))}</td>"
            f"<td class='n'>{_esc(o.get('distance_mi'))}</td>"
            f"<td>{_esc(site.get('site_name'))}</td>"
            f"<td class='small'>{_esc(matched)}</td>"
            "</tr>")
    head = ("<tr><th>Score</th><th>Prime</th><th>PIID</th><th>Agency</th><th>Obligated</th>"
            "<th>Ends</th><th>Mi</th><th>Nearest site</th><th>Matched (lens:code)</th></tr>")
    return f"<table>{head}{''.join(rows)}</table>"


_CSS = """
body{font:13px/1.45 -apple-system,'Segoe UI',sans-serif;color:#111;background:#fff;
     max-width:1200px;margin:24px auto;padding:0 20px}
h1{font-size:20px;margin:0 0 2px}
h2{font-size:14px;text-transform:uppercase;letter-spacing:.04em;margin:26px 0 2px;
   border-bottom:2px solid #111;padding-bottom:3px}
.src{color:#666;font-size:11px;margin-bottom:6px}
.src code{background:#f2f2f2;padding:1px 4px}
.err{background:#fee;border:1px solid #c66;color:#900;padding:4px 8px;font-size:12px;margin:4px 0}
table{border-collapse:collapse;width:100%;font-size:12px}
td,th{border:1px solid #ddd;padding:3px 7px;text-align:left;vertical-align:top}
th{background:#f5f5f5;font-size:11px;text-transform:uppercase}
td.k{color:#555;width:260px;font-family:ui-monospace,monospace;font-size:11px}
td.n{text-align:right;font-variant-numeric:tabular-nums}
.small{font-size:10px;color:#555}
.empty{color:#888;font-style:italic;padding:4px 0}
details{margin:4px 0 0}
summary{cursor:pointer;color:#888;font-size:10px;text-transform:uppercase}
pre{background:#f8f8f8;border:1px solid #eee;padding:8px;font-size:10px;overflow-x:auto;
    max-height:400px;overflow-y:auto}
.meta{color:#666;font-size:11px;margin-bottom:14px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:0 28px}
@media print{details{display:none}}
"""


def render_profile(profile: dict[str, Any]) -> str:
    """The maximal page: headline, then every section as a labeled table + raw block."""
    s = profile["sections"]
    sam = s["sam_entity"].get("data") or {}
    name = sam.get("legal_business_name") or profile["uei"]

    def block(key: str, title: str, body_html: str) -> str:
        return f"<section>{_sec_head(title, s[key])}{body_html}{_raw_block(s[key])}</section>"

    parts = [
        f"<h1>{_esc(name)}</h1>",
        (f"<div class='meta'>UEI {_esc(profile['uei'])} · generated "
         f"{_esc(profile['generated_at'])} · OPERATOR SURFACE — maximal; every section "
         f"labeled with source + caveats; raw rows under each 'raw' toggle</div>"),
        block("sam_entity", "Identity / SAM registration", _kv_table(sam)),
        block("rollup", "Behavior posture (fresh)", _kv_table(s["rollup"].get("data"))),
        "<div class='grid'>",
        block("geo", "HQ geo", _kv_table(s["geo"].get("data"))),
        block("firmographics", "Firmographics", _kv_table(s["firmographics"].get("data"))),
        "</div>",
        block("people", "People + contactability", _people_table(s["people"].get("data"))),
        block("lanes", "Demonstrated code lanes", _lanes_table(s["lanes"].get("data"))),
        "<div class='grid'>",
        block("inferred_primeable", "Inferred primeable",
              _inferred_table(s["inferred_primeable"].get("data"))),
        block("inferred_subbable", "Inferred subbable",
              _inferred_table(s["inferred_subbable"].get("data"))),
        "</div>",
        block("subout_opportunities", "Sub-out opportunities (live recipe)",
              _subout_table(s["subout_opportunities"].get("data"))),
        block("legacy_capability_card", "Legacy capability card",
              _kv_table(s["legacy_capability_card"].get("data"),
                        ["firm_name", "federal_status", "is_dsbs", "n_recommended_lanes",
                         "top_evidence_tier", "materialized_at"]
                        if s["legacy_capability_card"].get("data") else None)),
        block("legacy_gold", "Legacy gold profile", _kv_table(s["legacy_gold"].get("data"))),
    ]
    return ("<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{_esc(name)} — operator profile</title>"
            f"<style>{_CSS}</style></head><body>{''.join(parts)}</body></html>")
