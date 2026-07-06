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

DESIGN POSTURE: self-contained document with its own light card aesthetic — deliberately
NOT the rare-structure cockpit design system (same independence stance as card_html.py).
Scannable over exhaustive on the surface: headline + stat strip up top, sections as
cards, code TITLES joined from the reference dimensions, matched evidence compacted to
per-lens summaries — while the per-section `raw` toggles keep the exhaustive truth one
click away.

COMPOSITION (compose_profile): ~10 independent BTREE point-reads fanned out on a module
pool + one IN-PROCESS subout recipe call. Every section is best-effort: an unreachable
dataset renders as an error note in that section, never a bricked page (the max surface
must show whatever exists). Code rows are enriched with naics_reference / psc_reference
titles at compose time, so the JSON twin carries them too. Sections:
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

from . import config, market_store, subout_store
from .lance_store import _dataset, _map_jsonable, _sql_str

log = logging.getLogger("catalyst_api.profile")

# Independent point-reads per compose; IO-bound R2 waits (pylance releases the GIL).
_PROFILE_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="profile")

PEOPLE_CAP = 40          # mention-ranked; the raw block states the true count
LANES_CAP = 60           # $-ranked per side
INFERRED_CAP = 15        # per direction, support-ranked
SUBOUT_LIMIT = 10
MATCHED_CODES_SHOWN = 3  # per lens in the compact evidence summary ("+N more" past it)


# ── I/O seams (monkeypatch targets for the hermetic tests) ─────────────────────
def _rows(uri: str, predicate: str, columns: list[str] | None = None) -> list[dict[str, Any]]:
    """One fresh filtered scanner → rows. Every caller passes a BTREE point predicate."""
    return _dataset(uri).scanner(columns=columns, filter=predicate).to_table().to_pylist()


def _load_code_titles() -> dict[str, dict[str, str]]:
    """{'naics': code→title, 'psc': code→name} off the in-memory reference dimensions
    (market_store caches them per process). Best-effort: empty dicts on failure —
    codes then render bare, never a bricked page."""
    out: dict[str, dict[str, str]] = {"naics": {}, "psc": {}}
    for ct in ("naics", "psc"):
        try:
            out[ct] = dict(market_store._codes_for(ct))
        except Exception as exc:  # noqa: BLE001 — titles are decoration
            log.warning("code titles for %s unavailable: %s", ct, exc)
    return out


def _title_for(titles: dict[str, dict[str, str]], code_type: Any, code: Any) -> str | None:
    if not code:
        return None
    if code_type in ("naics", "psc"):
        return titles[code_type].get(code)
    # untyped code (caller_declared without code_type): try both systems
    return titles["naics"].get(code) or titles["psc"].get(code)


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


def _enrich_titles(sections: dict[str, Any], titles: dict[str, dict[str, str]]) -> None:
    """Fold code TITLES onto every code-bearing row (lanes / inferred / subout awards +
    matched evidence) so the human name rides next to the number on BOTH routes."""
    for lane in ((sections["lanes"].get("data") or {}).get("lanes") or []):
        lane["code_title"] = _title_for(titles, lane.get("code_type"), lane.get("code"))
    for key in ("inferred_primeable", "inferred_subbable"):
        for row in ((sections[key].get("data") or {}).get("codes") or []):
            row["code_title"] = _title_for(titles, row.get("code_type"), row.get("code"))
    subout = sections["subout_opportunities"].get("data") or {}
    for opp in ((subout.get("data") or {}).get("opportunities") or []):
        opp["naics_title"] = _title_for(titles, "naics", opp.get("naics_code"))
        opp["psc_title"] = _title_for(titles, "psc", opp.get("product_or_service_code"))
        for m in (opp.get("matched") or []):
            ct = (m.get("evidence") or {}).get("recipient_code_type")
            m["code_title"] = _title_for(titles, ct, m.get("code"))


def compose_profile(uei: str, include_peers: bool = False) -> dict[str, Any]:
    """The full assembly — every per-entity read, fanned out, best-effort per section,
    code rows title-enriched."""
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
    f_titles = pool.submit(_load_code_titles)

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
    try:
        _enrich_titles(sections, f_titles.result())
    except Exception as exc:  # noqa: BLE001 — decoration must never brick the compose
        log.warning("code-title enrichment failed: %s", exc)
    return {
        "uei": uei,
        "generated_at": dt_date.today().isoformat(),
        "include_peers": include_peers,
        "sections": sections,
    }


# ── Render — self-contained light card document (independent of any product DS) ──
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


def _code_cell(code: Any, title: Any) -> str:
    """`541690 — Security Guards and Patrol Services` (title muted; bare code if none)."""
    if code is None:
        return "—"
    t = f" <span class='ct'>— {html.escape(str(title))}</span>" if title else ""
    return f"<span class='code'>{html.escape(str(code))}</span>{t}"


def _raw_block(section: dict[str, Any]) -> str:
    """The nothing-hidden guarantee: every section carries its raw rows, collapsed."""
    payload = json.dumps(section.get("data"), indent=1, default=str)
    return (f"<details><summary>raw rows</summary><pre>{html.escape(payload)}</pre></details>")


def _card(title: str, section: dict[str, Any], body_html: str) -> str:
    note = f"<div class='note'>{_esc(section['note'])}</div>" if section.get("note") else ""
    err = (f"<div class='err'>UNAVAILABLE — {_esc(section['error'])}</div>"
           if section.get("error") else "")
    return ("<section class='card'>"
            f"<div class='card-head'><h2>{_esc(title)}</h2>"
            f"<span class='src'>{_esc(section.get('source'))}</span></div>"
            f"{note}{err}{body_html}{_raw_block(section)}</section>")


def _kv(row: dict[str, Any] | None, keys: list[str] | None = None) -> str:
    if not row:
        return "<div class='empty'>no row</div>"
    keys = keys or list(row.keys())
    cells = "".join(
        f"<div class='k'>{_esc(k)}</div><div class='v'>{_esc(row.get(k))}</div>"
        for k in keys)
    return f"<div class='kv'>{cells}</div>"


_ROLE_LABELS = (
    ("is_govt_poc", "Govt POC"), ("is_ebiz_poc", "eBiz POC"),
    ("is_past_perf_poc", "Past-perf POC"), ("is_dsbs_contact", "DSBS contact"),
    ("is_dsbs_principal", "DSBS principal"),
    ("is_exec_officer_prime", "Exec (prime)"), ("is_exec_officer_sub", "Exec (sub)"),
)


def _people_cards(data: dict[str, Any] | None) -> str:
    people = (data or {}).get("people") or []
    if not people:
        return "<div class='empty'>no people</div>"
    out = []
    for p in people:
        c = p.get("contact") or {}
        chips = "".join(f"<span class='chip'>{label}</span>"
                        for flag, label in _ROLE_LABELS if p.get(flag))
        contact_bits = []
        if c.get("phone"):
            status = f" <span class='ct'>({_esc(c.get('phone_status'))})</span>"
            contact_bits.append(f"<span class='contact'>📞 {_esc(c['phone'])}{status}</span>")
        if c.get("email"):
            contact_bits.append(f"<span class='contact'>✉️ {_esc(c['email'])}</span>")
        if c.get("person_linkedin_url_norm"):
            contact_bits.append(
                f"<span class='contact'>in/ {_esc(c['person_linkedin_url_norm'])}</span>")
        contact_html = ("<div class='contacts'>" + " ".join(contact_bits) + "</div>"
                        if contact_bits else "<div class='contacts none'>no contact assets</div>")
        out.append(
            "<div class='person'>"
            f"<div><div class='pname'>{_esc(p.get('display_name'))}</div>"
            f"<div class='ptitle'>{_esc(p.get('best_title'))}</div>"
            f"<div>{chips}</div></div>"
            f"{contact_html}"
            "</div>")
    return "".join(out)


def _lanes_table(data: dict[str, Any] | None) -> str:
    lanes = (data or {}).get("lanes") or []
    if not lanes:
        return "<div class='empty'>no lanes</div>"
    rows = "".join(
        "<tr>"
        f"<td>{_esc(r.get('side'))}</td>"
        f"<td>{_code_cell(r.get('code'), r.get('code_title'))}</td>"
        f"<td class='n'>{_usd(r.get('obl_lifetime'))}</td>"
        "</tr>" for r in lanes)
    return ("<table><tr><th>Side</th><th>Code</th><th>$ lifetime</th></tr>"
            f"{rows}</table>")


def _inferred_list(data: dict[str, Any] | None) -> str:
    codes = (data or {}).get("codes") or []
    total = (data or {}).get("total_codes") or 0
    if not codes:
        return "<div class='empty'>none</div>"
    rows = "".join(
        "<tr>"
        f"<td>{_code_cell(r.get('code'), r.get('code_title'))}</td>"
        f"<td class='n'>{_esc(r.get('supporting_bothsider_firm_ct'))}</td>"
        "</tr>" for r in codes)
    more = (f"<div class='more'>showing {len(codes)} of {total} (rest in raw)</div>"
            if total > len(codes) else "")
    return f"<table><tr><th>Code</th><th>Support</th></tr>{rows}</table>{more}"


_LENS_ORDER = ("awarded_prime_contracts_in_code", "delivered_subawards_under_code",
               "sam_registered_naics", "caller_declared", "inferred_primeable")
_LENS_SHORT = {
    "awarded_prime_contracts_in_code": "Primed in",
    "delivered_subawards_under_code": "Delivered subs under",
    "sam_registered_naics": "SAM-registered",
    "caller_declared": "Declared",
    "inferred_primeable": "Inferred primeable",
}


def _matched_summary(matched: list[dict[str, Any]]) -> str:
    """The compact WHY: strongest lenses spelled out with titled codes, the inferred
    wall reduced to a count. The exhaustive list stays in the section's raw block."""
    by_lens: dict[str, list[dict[str, Any]]] = {}
    for m in matched or []:
        by_lens.setdefault(m.get("lens") or "?", []).append(m)
    lines = []
    for lens in _LENS_ORDER:
        entries = by_lens.pop(lens, None)
        if not entries:
            continue
        label = _LENS_SHORT.get(lens, lens)
        if lens == "inferred_primeable":
            lines.append(f"<div class='why'><b>{label}:</b> {len(entries)} codes</div>")
            continue
        shown = entries[:MATCHED_CODES_SHOWN]
        codes = ", ".join(_code_cell(m.get("code"), m.get("code_title")) for m in shown)
        extra = f" <span class='ct'>+{len(entries) - len(shown)} more</span>" \
            if len(entries) > len(shown) else ""
        lines.append(f"<div class='why'><b>{label}:</b> {codes}{extra}</div>")
    for lens, entries in by_lens.items():   # future lenses never dropped silently
        lines.append(f"<div class='why'><b>{_esc(lens)}:</b> {len(entries)} codes</div>")
    return "".join(lines)


def _score_badge(score: Any) -> str:
    s = float(score or 0.0)
    tier = "hi" if s >= 0.7 else ("mid" if s >= 0.45 else "lo")
    return f"<div class='score {tier}'>{s:.3f}</div>"


def _opportunity_cards(data: dict[str, Any] | None) -> str:
    opps = ((data or {}).get("data") or {}).get("opportunities") or []
    meta = (data or {}).get("meta") or {}
    if not opps:
        return f"<div class='empty'>no opportunities · {_esc(meta.get('reason'))}</div>"
    total = meta.get("total")
    head = (f"<div class='more'>top {len(opps)} of {total} scored open awards</div>"
            if total and total > len(opps) else "")
    cards = []
    for o in opps:
        end = o.get("period_of_performance_current_end_date") or o.get("ordering_period_end_date")
        site = o.get("nearest_federal_site") or {}
        facts = [
            f"{_usd(o.get('total_obligation'))} obligated",
            f"ends {_esc(end)}" if end else None,
            (f"{o.get('distance_mi'):,} mi from HQ"
             if isinstance(o.get("distance_mi"), (int, float)) else None),
            (f"near {_esc(site.get('site_name'))}"
             + (f" ({site.get('distance_mi')} mi)" if site.get("distance_mi") is not None else "")
             if site.get("site_name") else None),
            f"plan {_esc(o.get('subcontracting_plan_code'))}"
            if o.get("subcontracting_plan_code") else None,
        ]
        fact_line = " · ".join(f for f in facts if f)
        codes = []
        if o.get("naics_code"):
            codes.append(f"NAICS {_code_cell(o.get('naics_code'), o.get('naics_title'))}")
        if o.get("product_or_service_code"):
            codes.append(f"PSC {_code_cell(o.get('product_or_service_code'), o.get('psc_title'))}")
        cards.append(
            "<div class='opp'>"
            f"{_score_badge(o.get('score'))}"
            "<div class='opp-body'>"
            f"<div class='opp-title'>{_esc(o.get('prime_name'))}"
            f" <span class='ct'>· {_esc(o.get('award_id_piid'))} · "
            f"{_esc(o.get('awarding_agency_name'))}</span></div>"
            f"<div class='facts'>{fact_line}</div>"
            f"<div class='facts'>{' · '.join(codes)}</div>"
            f"{_matched_summary(o.get('matched') or [])}"
            "</div></div>")
    return head + "".join(cards)


def _stat(label: str, value: str) -> str:
    return f"<div class='stat'><div class='sv'>{value}</div><div class='sl'>{_esc(label)}</div></div>"


def _header(profile: dict[str, Any]) -> str:
    s = profile["sections"]
    sam = s["sam_entity"].get("data") or {}
    rollup = s["rollup"].get("data") or {}
    ppl = s["people"].get("data") or {}
    subout_meta = ((s["subout_opportunities"].get("data") or {}).get("meta")) or {}
    name = sam.get("legal_business_name") or profile["uei"]
    badges = "".join(f"<span class='chip'>{label}</span>" for cond, label in (
        (sam.get("sam_is_active"), "SAM active"),
        (sam.get("in_dsbs"), "DSBS"),
        (sam.get("is_prime_recipient"), "Has primed"),
        (sam.get("is_subawardee"), "Has subbed"),
    ) if cond)
    meta_bits = [f"UEI {profile['uei']}"]
    if sam.get("normalized_domain"):
        meta_bits.append(str(sam["normalized_domain"]))
    if sam.get("physical_state"):
        meta_bits.append(str(sam["physical_state"]))
    meta_bits.append(f"generated {profile['generated_at']}")
    stats = "".join([
        _stat("Sub $ lifetime", _usd(rollup.get("sub_amt_lifetime"))),
        _stat("Prime $ lifetime", _usd(rollup.get("prime_obl_lifetime"))),
        _stat("Last action", _esc(rollup.get("last_action_date"))),
        _stat("People", _esc(ppl.get("total_people"))),
        _stat("Open opportunities", _esc(subout_meta.get("total"))),
    ])
    return ("<header>"
            f"<h1>{_esc(name)}</h1>"
            f"<div class='hmeta'>{_esc(' · '.join(meta_bits))} {badges}</div>"
            f"<div class='stats'>{stats}</div>"
            "<div class='hnote'>OPERATOR SURFACE — maximal by design: every section "
            "labeled with its source + caveats; complete rows under each “raw rows” "
            "toggle</div>"
            "</header>")


_CSS = """
:root{--ink:#1c2333;--muted:#697386;--faint:#98a1b3;--line:#e6e9f0;--bg:#f3f5f8;
      --card:#fff;--accent:#3b5bdb;--accent-soft:#eef1fd}
*{box-sizing:border-box}
body{font:14px/1.5 -apple-system,'Segoe UI',Helvetica,Arial,sans-serif;color:var(--ink);
     background:var(--bg);margin:0;padding:28px 20px}
.wrap{max-width:1060px;margin:0 auto}
header{margin-bottom:18px}
h1{font-size:26px;font-weight:750;letter-spacing:-.015em;margin:0}
.hmeta{color:var(--muted);font-size:13px;margin:4px 0 12px}
.hnote{color:var(--faint);font-size:11px;margin-top:10px}
.stats{display:flex;gap:10px;flex-wrap:wrap}
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;
      padding:10px 16px;min-width:118px;box-shadow:0 1px 2px rgba(20,30,55,.05)}
.sv{font-size:19px;font-weight:700;font-variant-numeric:tabular-nums}
.sl{font-size:10.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-top:1px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;
      padding:16px 20px 12px;margin:14px 0;box-shadow:0 1px 2px rgba(20,30,55,.05)}
.card-head{display:flex;justify-content:space-between;align-items:baseline;gap:12px;
           flex-wrap:wrap;margin-bottom:6px}
h2{font-size:15px;font-weight:700;margin:0}
.src{font:11px ui-monospace,Menlo,monospace;color:var(--faint)}
.note{color:var(--muted);font-size:12px;font-style:italic;margin:0 0 8px}
.err{background:#fdf0ef;border:1px solid #f0b9b4;color:#a4322a;border-radius:8px;
     padding:6px 10px;font-size:12.5px;margin:6px 0}
.kv{display:grid;grid-template-columns:230px minmax(0,1fr);font-size:13px}
.kv .k{color:var(--muted);font:11.5px ui-monospace,Menlo,monospace;padding:4px 12px 4px 0;
       border-bottom:1px solid var(--line)}
.kv .v{padding:4px 0;border-bottom:1px solid var(--line);word-break:break-word}
table{border-collapse:collapse;width:100%;font-size:13px}
th{color:var(--muted);font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;
   text-align:left;padding:5px 10px 5px 0;border-bottom:1px solid var(--line)}
td{padding:5px 10px 5px 0;border-bottom:1px solid var(--line);vertical-align:top}
td.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.code{font:12.5px ui-monospace,Menlo,monospace;background:var(--accent-soft);
      color:var(--accent);padding:1px 6px;border-radius:5px}
.ct{color:var(--muted);font-size:12px}
.chip{display:inline-block;background:var(--accent-soft);color:var(--accent);
      border-radius:999px;padding:1.5px 9px;font-size:11px;font-weight:600;
      margin:2px 4px 2px 0}
.person{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;
        padding:10px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
.person:last-of-type{border-bottom:none}
.pname{font-weight:650}
.ptitle{color:var(--muted);font-size:12.5px;margin-bottom:2px}
.contacts{text-align:right;font-size:13px;display:flex;flex-direction:column;gap:2px}
.contacts.none{color:var(--faint);font-style:italic}
.contact{white-space:nowrap}
.opp{display:flex;gap:14px;border:1px solid var(--line);border-radius:10px;
     padding:12px 14px;margin:10px 0;background:#fcfcfe}
.score{min-width:58px;height:fit-content;text-align:center;border-radius:8px;
       padding:7px 0;font-weight:750;font-size:15px;font-variant-numeric:tabular-nums}
.score.hi{background:#e5f4ea;color:#1c7c3c}
.score.mid{background:#fdf3e0;color:#9a6b15}
.score.lo{background:#f0f1f4;color:var(--muted)}
.opp-body{min-width:0;flex:1}
.opp-title{font-weight:650;font-size:14.5px}
.facts{color:var(--muted);font-size:12.5px;margin:3px 0}
.why{font-size:12.5px;margin:2px 0}
.why b{font-weight:600;color:var(--muted)}
.more{color:var(--faint);font-size:11.5px;margin:4px 0}
.empty{color:var(--faint);font-style:italic;padding:4px 0}
details{margin:8px 0 2px}
summary{cursor:pointer;color:var(--faint);font-size:10.5px;text-transform:uppercase;
        letter-spacing:.05em}
pre{background:#f7f8fa;border:1px solid var(--line);border-radius:8px;padding:10px;
    font-size:10.5px;overflow:auto;max-height:380px}
@media print{body{background:#fff}details{display:none}.card{box-shadow:none}}
"""


def render_profile(profile: dict[str, Any]) -> str:
    """The maximal page: headline + stat strip, then every section as a labeled card."""
    s = profile["sections"]
    sam = s["sam_entity"].get("data") or {}
    name = sam.get("legal_business_name") or profile["uei"]
    card_data = s["legacy_capability_card"].get("data")
    parts = [
        _header(profile),
        _card("Identity / SAM registration", s["sam_entity"], _kv(sam)),
        _card("Behavior posture (fresh)", s["rollup"], _kv(s["rollup"].get("data"))),
        _card("People + contactability", s["people"], _people_cards(s["people"].get("data"))),
        _card("Sub-out opportunities (live recipe)", s["subout_opportunities"],
              _opportunity_cards(s["subout_opportunities"].get("data"))),
        _card("Demonstrated code lanes", s["lanes"], _lanes_table(s["lanes"].get("data"))),
        _card("Inferred primeable", s["inferred_primeable"],
              _inferred_list(s["inferred_primeable"].get("data"))),
        _card("Inferred subbable", s["inferred_subbable"],
              _inferred_list(s["inferred_subbable"].get("data"))),
        _card("Firmographics", s["firmographics"], _kv(s["firmographics"].get("data"))),
        _card("HQ geo", s["geo"], _kv(s["geo"].get("data"))),
        _card("Legacy capability card", s["legacy_capability_card"],
              _kv(card_data,
                  ["firm_name", "federal_status", "is_dsbs", "n_recommended_lanes",
                   "top_evidence_tier", "materialized_at"] if card_data else None)),
        _card("Legacy gold profile", s["legacy_gold"], _kv(s["legacy_gold"].get("data"))),
    ]
    return ("<!doctype html><html><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            f"<title>{_esc(name)} — operator profile</title>"
            f"<style>{_CSS}</style></head><body><div class='wrap'>"
            f"{''.join(parts)}</div></body></html>")
