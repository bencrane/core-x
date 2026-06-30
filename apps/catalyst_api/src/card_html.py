"""Self-contained HTML render of the capability profile card — the standalone prototype look.

Deliberately NOT the rare-structure cockpit aesthetic: its own complete document, own light
palette, zero shared CSS/design-tokens. Served open by catalyst_api GET /card/{uei} so a UEI in
the URL renders a shareable card. Data is the capability_profile row (snake_case from Lance).
"""
from __future__ import annotations

import html
from typing import Any

_USD0 = "—"


def _esc(v: Any) -> str:
    return html.escape(str(v)) if v is not None else ""


def _usd(n: Any) -> str:
    if n is None:
        return _USD0
    n = float(n)
    a = abs(n)
    if a >= 1e9:
        return f"${n / 1e9:.1f}B"
    if a >= 1e6:
        return f"${n / 1e6:.1f}M"
    if a >= 1e3:
        return f"${n / 1e3:.0f}K"
    return f"${n:.0f}"


def _num(n: Any) -> str:
    return f"{int(n):,}" if n is not None else _USD0


_TIER = {
    "primed-direct": ("Primed · direct", "tier-green"),
    "subbed-hop": ("Subbed · adjacent", "tier-blue"),
    "primed-hop": ("Primed · adjacent", "tier-gray"),
    "declared": ("Declared", "tier-gray"),
}

CSS = """
*{box-sizing:border-box}
html,body{margin:0}
body{background:#f4f3ef;color:#1b1b19;font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;line-height:1.5;-webkit-font-smoothing:antialiased}
.wrap{max-width:940px;margin:0 auto;padding:44px 24px 96px}
.card{background:#fff;border:1px solid #e8e6e0;border-radius:18px;padding:30px 34px;box-shadow:0 1px 2px rgba(20,20,18,.03)}
.head{display:flex;align-items:center;gap:16px}
.avatar{width:52px;height:52px;border-radius:50%;background:#eef2ff;color:#1e40af;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:17px;flex:none}
.head h1{font-size:24px;font-weight:600;margin:0;letter-spacing:-.01em}
.meta{font-size:13px;color:#8a887f;margin-top:3px;font-variant-numeric:tabular-nums}
.status{margin-left:auto;font-size:12px;font-weight:500;padding:5px 12px;border-radius:999px;background:#ecfdf3;color:#15803d;white-space:nowrap}
.status.prospect{background:#eef2ff;color:#1e40af}
.badges{display:flex;flex-wrap:wrap;gap:7px;margin-top:16px}
.badge{font-size:12px;font-weight:500;padding:4px 10px;border-radius:8px;background:#f1f0ec;color:#4b4a44}
.badge.b-accent{background:#eef2ff;color:#1e40af}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:22px}
.metric{background:#faf9f6;border-radius:12px;padding:15px 16px}
.metric .l{font-size:12.5px;color:#8a887f}
.metric .v{font-size:25px;font-weight:600;margin-top:5px;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.metric .h{font-size:12px;color:#a9a79d;margin-left:7px;font-weight:400}
.note{font-size:13px;color:#6b6a64;margin:14px 2px 0}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:26px;margin-top:22px}
@media(max-width:640px){.cols{grid-template-columns:1fr}}
.sec-label{font-size:11px;font-weight:600;letter-spacing:.09em;text-transform:uppercase;color:#a09e94;margin:0 0 9px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{font-size:12.5px;background:#f1f0ec;border-radius:7px;padding:4px 9px;color:#4b4a44}
.kv{width:100%;border-collapse:collapse;font-size:13.5px}
.kv td{padding:4px 0}
.kv td.r{text-align:right;color:#6b6a64}
.callout{margin-top:18px;background:#faf9f6;border-radius:12px;padding:13px 15px;font-size:13.5px;color:#4b4a44}
hr{border:0;border-top:1px solid #eceae4;margin:30px 0 0}
.lanes-h{font-size:19px;font-weight:600;margin:26px 0 3px;letter-spacing:-.01em}
.lanes-sub{font-size:13.5px;color:#6b6a64;margin:0 0 18px}
.lane{border:1px solid #e8e6e0;border-radius:13px;padding:15px 17px;margin-bottom:11px;transition:border-color .12s,background .12s}
.lane.yes{border-color:#bbf7d0;background:#f4fdf6}
.lane.no{opacity:.45}
.lane-top{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:space-between}
.lane-id{display:flex;align-items:center;gap:9px;flex-wrap:wrap}
.code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;font-weight:600;color:#1e40af}
.tier{font-size:11px;font-weight:500;padding:3px 9px;border-radius:7px}
.tier-green{background:#ecfdf3;color:#15803d}.tier-blue{background:#eef2ff;color:#1e40af}.tier-gray{background:#f1f0ec;color:#5b5a53}
.demand{font-size:12px;color:#8a887f;font-variant-numeric:tabular-nums;white-space:nowrap}
.lane-desc{font-size:13.5px;color:#3b3a35;margin-top:9px}
.buyers{font-size:12px;color:#8a887f;margin-top:7px}
.toggle{display:flex;gap:6px;flex:none}
.tg{border:1px solid #ddd9d0;background:#fff;border-radius:8px;width:30px;height:28px;cursor:pointer;font-size:14px;color:#6b6a64;display:flex;align-items:center;justify-content:center;line-height:1}
.tg.on-y{background:#ecfdf3;color:#15803d;border-color:#bbf7d0}
.tg.on-n{background:#f1f0ec;color:#8a887f}
.foot{display:flex;align-items:center;justify-content:space-between;margin-top:20px;padding-top:16px;border-top:1px solid #eceae4}
.count{font-size:13px;color:#8a887f;font-variant-numeric:tabular-nums}
.save{font-size:13.5px;font-weight:500;padding:8px 16px;border-radius:9px;border:1px solid #1b1b19;background:#1b1b19;color:#fff;cursor:pointer}
.save:active{transform:scale(.99)}
.empty{font-size:13.5px;color:#8a887f}
"""

SCRIPT = """
<script>
(function(){
  var lanes=document.getElementById('lanes'); if(!lanes) return;
  function recount(){
    var y=document.querySelectorAll('.lane.yes').length, n=document.querySelectorAll('.lane.no').length;
    document.getElementById('count').textContent=y+' confirmed · '+n+' declined';
  }
  lanes.addEventListener('click',function(e){
    var b=e.target.closest('.tg'); if(!b) return;
    var lane=b.closest('.lane'), v=b.getAttribute('data-v');
    lane.querySelectorAll('.tg').forEach(function(t){t.classList.remove('on-y','on-n')});
    lane.classList.remove('yes','no');
    if(v==='y'){b.classList.add('on-y');lane.classList.add('yes')}else{b.classList.add('on-n');lane.classList.add('no')}
    recount();
  });
  var save=document.getElementById('save');
  if(save) save.addEventListener('click',function(){
    var confirmed=[].map.call(document.querySelectorAll('.lane.yes'),function(l){return l.getAttribute('data-combo')});
    save.textContent='Saved '+confirmed.length+' lanes ✓';
    setTimeout(function(){save.textContent='Save profile'},1800);
  });
})();
</script>
"""


def _initials(name: str | None) -> str:
    parts = [p for p in (name or "").replace(".", " ").split() if p]
    return ("".join(p[0] for p in parts[:2]) or "?").upper()


def render_card(r: dict[str, Any]) -> str:
    name = r.get("firm_name") or r.get("uei") or "Capability profile"
    status = r.get("federal_status")
    status_label = "Active sub" if status == "active_sub" else ("DSBS prospect" if status == "dsbs_prospect" else "Federal")
    status_cls = "" if status == "active_sub" else "prospect"
    meta = " · ".join([x for x in [r.get("uei"), r.get("state_code"), status_label] if x])
    sub = r.get("sub_activity") if isinstance(r.get("sub_activity"), dict) else None
    # the Lance row is flat (snake_case); sub/prime activity are flat fields, not nested.
    has_sub = bool(r.get("has_sub_history"))
    has_prime = bool(r.get("has_prime_history"))

    out: list[str] = []
    out.append('<div class="wrap"><div class="card">')

    # header
    out.append('<div class="head">')
    out.append(f'<div class="avatar">{_esc(_initials(name))}</div>')
    out.append(f'<div><h1>{_esc(name)}</h1><div class="meta">{_esc(meta)}</div></div>')
    out.append(f'<span class="status {status_cls}">{_esc(status_label)}</span>')
    out.append("</div>")

    # designations
    desig = [d for d in (r.get("designations") or []) if d]
    if r.get("is_dsbs") or desig:
        out.append('<div class="badges">')
        if r.get("is_dsbs"):
            out.append('<span class="badge b-accent">DSBS</span>')
        for d in desig:
            out.append(f'<span class="badge">{_esc(d)}</span>')
        out.append("</div>")

    # metrics (sub + prime)
    out.append('<div class="metrics">')
    if has_sub:
        out.append(f'<div class="metric"><div class="l">Subaward $ (5y)</div><div class="v">{_usd(r.get("sub_amount_5y"))}</div></div>')
        out.append(f'<div class="metric"><div class="l">Subawards (5y)</div><div class="v">{_num(r.get("sub_received_5y"))}<span class="h">{_num(r.get("sub_distinct_primes_5y"))} primes</span></div></div>')
        out.append(f'<div class="metric"><div class="l">Recent (90d)</div><div class="v">{_num(r.get("recent_subawards_90d"))}<span class="h">{_usd(r.get("recent_subaward_amount_90d"))}</span></div></div>')
    if has_prime:
        out.append(f'<div class="metric"><div class="l">Prime $ (5y)</div><div class="v">{_usd(r.get("prime_obligated_5y"))}</div></div>')
        out.append(f'<div class="metric"><div class="l">Prime awards (5y)</div><div class="v">{_num(r.get("prime_awards_5y"))}<span class="h">{_num(r.get("prime_competed_awards_5y"))} competed</span></div></div>')
    out.append("</div>")

    # two-column: wheelhouse + partners
    sub_naics = r.get("sub_top_naics") or []
    partners = r.get("sub_top_prime_partners") or []
    if sub_naics or partners:
        out.append('<div class="cols">')
        out.append('<div><div class="sec-label">Proven wheelhouse</div><div class="chips">')
        for n in sub_naics[:6]:
            label = f'{_esc(n.get("code"))} {_esc((n.get("description") or "")[:22])}'
            out.append(f'<span class="chip">{label}</span>')
        if not sub_naics:
            out.append('<span class="chip">—</span>')
        out.append("</div></div>")
        out.append('<div><div class="sec-label">Top prime partners</div><table class="kv">')
        for p in partners[:5]:
            out.append(f'<tr><td>{_esc(p.get("name"))}</td><td class="r">{_usd(p.get("amount"))}</td></tr>')
        if not partners:
            out.append('<tr><td>—</td></tr>')
        out.append("</table></div></div>")

    # becoming-a-prime callout
    if has_prime:
        pnaics = ", ".join(_esc(n.get("code")) for n in (r.get("prime_top_naics") or [])[:3])
        ppsc = ", ".join(_esc(n.get("code")) for n in (r.get("prime_top_psc") or [])[:3])
        agency = (r.get("prime_top_agencies") or [{}])[0].get("agency") if r.get("prime_top_agencies") else None
        out.append(f'<div class="callout"><b>Becoming-a-prime signal</b> — {_num(r.get("prime_awards_5y"))} prime awards / {_usd(r.get("prime_obligated_5y"))}. Primes in {_esc(pnaics) or "—"} · PSC {_esc(ppsc) or "—"}{(" · " + _esc(agency)) if agency else ""}.</div>')

    out.append("<hr>")

    # recommended lanes — the magic moment
    out.append('<div class="lanes-h">Given your past, you\'re a strong fit for these lanes</div>')
    out.append('<div class="lanes-sub">Empirical neighbors of the NAICS+PSC combos you\'ve won — confirm the ones that fit.</div>')
    lanes = r.get("recommended_lanes") or []
    if not lanes:
        out.append('<div class="empty">No recommended lanes for this firm.</div>')
    else:
        out.append('<div id="lanes">')
        for ln in lanes:
            tier_label, tier_cls = _TIER.get(ln.get("evidence_tier") or "declared", (ln.get("evidence_tier") or "—", "tier-gray"))
            combo = f'{_esc(ln.get("dst_naics"))}|{_esc(ln.get("dst_psc"))}'
            buyers = " · ".join(_esc(b) for b in (ln.get("top_primes") or [])[:3])
            out.append(f'<div class="lane" data-combo="{combo}">')
            out.append('<div class="lane-top"><div class="lane-id">')
            out.append(f'<span class="code">{_esc(ln.get("dst_naics"))} · {_esc(ln.get("dst_psc"))}</span>')
            out.append(f'<span class="tier {tier_cls}">{_esc(tier_label)}</span></div>')
            out.append(f'<div class="demand">{_num(ln.get("lane_n_primes"))} primes · {_usd(ln.get("lane_median_amt"))} median</div>')
            out.append("</div>")
            out.append(f'<div class="lane-desc">{_esc(ln.get("naics_desc"))} — {_esc(ln.get("psc_desc"))}</div>')
            if buyers:
                out.append(f'<div class="buyers">Buyers: {buyers}</div>')
            out.append('<div class="toggle" style="margin-top:11px">')
            out.append('<button class="tg" data-v="y" title="Confirm fit">✓</button>')
            out.append('<button class="tg" data-v="n" title="Not a fit">✕</button>')
            out.append("</div></div>")
        out.append("</div>")
        out.append(f'<div class="foot"><span class="count" id="count">0 confirmed · 0 declined</span><button class="save" id="save">Save profile</button></div>')

    out.append("</div></div>")

    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"<title>{_esc(name)} — capability profile</title><style>{CSS}</style></head>"
        f"<body>{''.join(out)}{SCRIPT}</body></html>"
    )


def render_not_found(uei: str) -> str:
    return (
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Not found</title>"
        f"<style>{CSS}</style></head><body><div class=\"wrap\"><div class=\"card\">"
        "<h1 style=\"font-size:22px;margin:0\">Capability profile not found</h1>"
        f"<p class=\"note\">No profile for UEI {_esc(uei)} — no subaward history, and not a DSBS with recommended lanes.</p>"
        "</div></div></body></html>"
    )
