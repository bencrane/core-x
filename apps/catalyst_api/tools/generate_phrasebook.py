"""Verified phrasebook generator — the menu of phrases that ACTUALLY COMPILE.

The phrase grammar is closed and the compiler deterministic, so the set of
working queries is mechanically enumerable: build candidate phrases from the
compiler's own vocabulary tables, compile EVERY one through the real
compile_phrase / compile_aggregate_phrase entrypoints, and emit only the
survivors. Nothing in the output is hand-asserted — if it's listed, it
compiled; refusals are listed separately WITH their refusal text so the
operator learns the grammar's edges from real errors, not guesses.

Run from the repo root (R2 creds required — event-lane compiles load the
registry from R2):

    doppler run -p core-x -c prd -- python3 apps/catalyst_api/tools/generate_phrasebook.py

Writes: apps/catalyst_api/tools/phrasebook.html (self-contained, file:// safe).
Regenerate on every vocabulary cycle (COMPILER_VERSION bump) — the page stamps
the version it was verified against.
"""

from __future__ import annotations

import datetime as dt
import html
import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from apps.catalyst_api.src import phrase_aggregate, phrase_compiler  # noqa: E402

TODAY = dt.date.today()
OUT = Path(__file__).resolve().parent / "phrasebook.html"


# ── Candidate enumeration (from the compiler's OWN vocabulary tables) ─────────

def _candidates() -> list[str]:
    subjects_entity = ["companies"]
    subjects_award = ["awards", "orders", "vehicles"]
    subjects_txn = ["actions", "mods"]
    sectors = sorted(phrase_compiler.SECTORS)
    events = sorted(set(phrase_compiler.EVENTS))
    caps = ["primed in", "delivered subs under", "inferred primeable", "inferred subbable"]
    quals = sorted(set(phrase_compiler.QUALIFIERS))
    agencies = sorted(k for k, v in phrase_compiler.AGENCIES.items() if v)
    times = ["in the last 90 days", "in the last year", "in the last 2 years",
             "this quarter", "fy24", "fy25", "since 2025-07-04"]
    moneys = ["over $5m", "over $250k", "under $1m"]

    c: list[str] = []

    # entity grain — capability lanes. NOTE (learned by compiling): capability
    # contexts require a LITERAL code ('primed in 236220'); sector aliases only
    # bind in sector position ('construction companies that …').
    codes = ["236220", "561210", "561612", "541330", "541512", "5416*", "23*"]
    pscs = ["R499", "S206"]
    for cap, code in itertools.product(caps, codes[:4]):
        c.append(f"companies {cap} {code}")
    c += [f"companies primed in {code}" for code in codes]
    c += [f"companies delivered subs under {p}" for p in pscs]
    c += ["companies primed in 236220 in VA",
          "companies primed in 236220 lifetime over $5m",
          "active companies primed in 236220",
          "companies primed in 236220 in dsbs",
          "companies inferred subbable 561210 in TX"]
    c += [f"companies {q}" for q in quals]
    c += [f"{q} companies primed in 236220" for q in ("dsbs", "sub-only")]
    c.append("companies expiring within 180 days")
    c.append("companies primed in 236220 expiring within 180 days")

    # entity grain — event lane (sector + event + time)
    for sec, ev in itertools.product(sectors[:4], events):
        c.append(f"{sec} companies that {ev} in the last 90 days")
    c += [f"construction companies that {e} {t}"
          for e, t in itertools.product(["novated", "exercised an option"], times)]
    c += [f"{s} companies {t}" for s, t in itertools.product(sectors[:3], times[:3])]
    c += [f"construction companies that received new funding over $5m in the last year"]
    c += [f"{a} construction companies in the last year" for a in agencies[:5]]

    # bare-code refusal teaching pair
    c += [f"{s} companies" for s in sectors]

    # award grain
    for s in subjects_award:
        c.append(f"{s} in construction over $5m")
        c.append(f"active {s} in construction")
        c.append(f"{s} expiring within 180 days")
        c.append(f"construction {s} fy25")
    c += [f"{a} awards over $5m fy25" for a in agencies[:4]]
    c.append("awards with a subcontracting plan expiring within 180 days")

    # transaction grain
    for s in subjects_txn:
        c.append(f"construction {s} in the last 90 days")
        c.append(f"{s} terminated for default in the last year")
    c.append("change orders in construction over $1m in the last year")
    c.append("actions with a subcontracting plan in the last 90 days")

    # aggregate grammar ('total …' opener)
    aggs = [
        "total awarded by industry fy23 to fy25",
        "total awarded by state fy25",
        "total awarded by agency fy25",
        "total active awards by industry",
        "total active awards near 79925 within 50 miles by equipment",
        "total active awards near 79925 within 100 miles by industry",
        "total awarded by industry",
        "total active awards by state",
        "total actions by industry fy25",
    ]
    c += aggs

    # de-dup preserving order
    seen: set[str] = set()
    out = []
    for p in c:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# ── Compile every candidate through the REAL entrypoints ─────────────────────

def _verify(phrases: list[str]):
    ok, refused = [], []
    for p in phrases:
        try:
            if phrase_aggregate.is_aggregate_phrase(p) if hasattr(
                    phrase_aggregate, "is_aggregate_phrase") else p.split()[0] == "total":
                compiled = phrase_aggregate.compile_aggregate_phrase(p, today=TODAY)
                mode = "chart"
                detail = compiled.get("title") or compiled["plan"][0].get("group_by", "")
            else:
                compiled = phrase_compiler.compile_phrase(p, today=TODAY)
                grain = compiled["grain"]
                mode = {"entity": "companies", "prime_award": "awards",
                        "transaction": "actions"}.get(grain, grain)
                detail = f"{len(compiled['plan'])}-step plan"
            ok.append((p, mode, detail))
        except Exception as e:  # refusal or any compile error — record verbatim
            refused.append((p, str(e)))
    return ok, refused


def main() -> None:
    phrases = _candidates()
    ok, refused = _verify(phrases)
    version = phrase_compiler.COMPILER_VERSION
    agg_version = phrase_aggregate.AGG_COMPILER_VERSION
    stamp = TODAY.isoformat()

    groups: dict[str, list[tuple[str, str]]] = {}
    for p, mode, detail in ok:
        groups.setdefault(mode, []).append((p, detail))

    def esc(s: str) -> str:
        return html.escape(s, quote=True)

    rows = []
    order = ["companies", "awards", "actions", "chart"]
    titles = {"companies": "Companies (entity grain)", "awards": "Awards / orders / vehicles",
              "actions": "Actions / mods (transaction grain)", "chart": "Charts (total … aggregate)"}
    for mode in order:
        if mode not in groups:
            continue
        rows.append(f'<h2>{titles[mode]} <span class="ct">{len(groups[mode])} verified</span></h2>')
        rows.append("<ul>")
        for p, detail in groups[mode]:
            rows.append(
                f'<li><code class="p" onclick="copyP(this)" title="click to copy">{esc(p)}</code>'
                f'<span class="d">{esc(detail)}</span></li>')
        rows.append("</ul>")

    ref_rows = "".join(
        f'<li><code class="p bad">{esc(p)}</code><div class="why">{esc(msg)}</div></li>'
        for p, msg in refused)

    OUT.write_text(f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Verified Phrasebook — {esc(version)}</title>
<style>
  :root {{ --bg:#0d1117; --panel:#161b22; --border:#2d3748; --text:#e6edf3;
          --dim:#8b949e; --accent:#58a6ff; --green:#3fb950; --red:#f85149;
          --mono:ui-monospace,SFMono-Regular,Menlo,monospace; }}
  * {{ box-sizing:border-box; margin:0; }}
  body {{ background:var(--bg); color:var(--text);
         font:14px/1.55 -apple-system,"Segoe UI",sans-serif; padding:28px;
         max-width:1000px; margin:0 auto; }}
  h1 {{ font-size:20px; }} h2 {{ font-size:15px; color:var(--accent); margin:26px 0 8px; }}
  .sub {{ color:var(--dim); font-size:13px; margin:6px 0 4px; }}
  .ct {{ color:var(--green); font-size:12px; font-weight:400; margin-left:8px; }}
  ul {{ list-style:none; padding:0; }}
  li {{ padding:5px 0; border-bottom:1px solid var(--border); display:flex;
       gap:12px; align-items:baseline; flex-wrap:wrap; }}
  code.p {{ font-family:var(--mono); font-size:13px; color:var(--text);
           background:var(--panel); border:1px solid var(--border);
           border-radius:6px; padding:3px 9px; cursor:pointer; }}
  code.p:hover {{ border-color:var(--accent); }}
  code.p.bad {{ color:var(--red); cursor:default; }}
  .d {{ color:var(--dim); font-size:12px; font-family:var(--mono); }}
  .why {{ color:var(--dim); font-size:12px; margin:2px 0 4px 4px; }}
  details {{ margin-top:30px; }} summary {{ cursor:pointer; color:var(--red);
            font-size:15px; font-weight:600; }}
  .toast {{ position:fixed; bottom:20px; right:20px; background:var(--panel);
           border:1px solid var(--green); border-radius:8px; padding:8px 14px;
           font-size:13px; display:none; }}
</style></head><body>
<h1>Verified Phrasebook</h1>
<p class="sub">Every phrase below was compiled through the REAL compiler
(<code>{esc(version)}</code> / <code>{esc(agg_version)}</code>) on {stamp} and
returned a valid plan — <b>{len(ok)} verified</b>, {len(refused)} refused (shown
at the bottom with their verbatim refusals). Click any phrase to copy it, then
paste into ⌘K on the HQ tab. Deterministic compiler: what compiled here compiles
there, same vocabulary version.</p>
{"".join(rows)}
<details><summary>Refused ({len(refused)}) — the grammar's edges, verbatim</summary>
<ul>{ref_rows}</ul></details>
<div class="toast" id="toast">copied</div>
<script>
function copyP(el) {{
  navigator.clipboard.writeText(el.textContent).then(() => {{
    const t = document.getElementById('toast');
    t.style.display = 'block'; setTimeout(() => t.style.display = 'none', 900);
  }});
}}
</script>
</body></html>
""")
    print(f"verified={len(ok)} refused={len(refused)} -> {OUT}")


if __name__ == "__main__":
    main()
