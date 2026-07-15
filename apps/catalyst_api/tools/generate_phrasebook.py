"""Verified phrasebook generator — the canonical query library, organized as
FIVE sentence scaffolds.

The phrase grammar is closed and the compiler deterministic, so the set of
working queries is mechanically enumerable AND taxonomizable: every legal
phrase is an instance of one of five structural scaffolds (below). The
generator builds candidate instances from the compiler's own vocabulary
tables, compiles EVERY one through the real compile_phrase /
compile_aggregate_phrase entrypoints, and emits only the survivors — grouped
under their scaffold so the operator memorizes 5 templates, not 150 phrases.

THE FIVE SCAFFOLDS (the whole grammar):
  S1  WHO by capability   [qualifier] companies <capability> <code> [in ST]
                          [lifetime over $X] [active | expiring within N days]
  S2  WHO by event        [agency] <sector> companies that <event> [over $X] <window>
  S3  THE BOOK (awards)   [active] awards|orders|vehicles [in <sector>]
                          [from <agency>] [over $X] [fyNN | expiring within N days]
  S4  THE LOG (actions)   <sector> actions|mods [<event> | with a subcontracting
                          plan] [over $X] <window>
  S5  THE TOTALS (chart)  total <measure> [near <zip> within N miles]
                          by <dimension> [fyA to fyB]

Nothing in the output is hand-asserted — if it's listed, it compiled; refusals
are listed separately WITH their refusal text so the operator learns the
grammar's edges from real errors, not guesses.

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

SCAFFOLDS: dict[str, tuple[str, str]] = {
    "S1": ("WHO — by capability",
           "[qualifier] companies (primed in | delivered subs under | inferred primeable | "
           "inferred subbable) <code> [in <ST>] [lifetime over $X] [active | expiring within N days]"),
    "S2": ("WHO — by event",
           "[<agency>] <sector> companies that <event> [over $X] <time window>"),
    "S3": ("THE BOOK — awards / orders / vehicles",
           "[active] (awards | orders | vehicles) [in <sector> | naics <code> | psc <code>] "
           "[from <agency>] [over $X] [fyNN | expiring within N days]"),
    "S4": ("THE LOG — actions / mods",
           "<sector> (actions | mods) [<event> | with a subcontracting plan] [over $X] <time window>"),
    "S5": ("THE TOTALS — charts",
           "total <measure> [near <zip5> within N miles] by <dimension> [fyA to fyB]"),
}


# ── Candidate enumeration (from the compiler's OWN vocabulary tables) ─────────

def _candidates() -> list[tuple[str, str]]:
    sectors = sorted(phrase_compiler.SECTORS)
    events = sorted(set(phrase_compiler.EVENTS))
    caps = ["primed in", "delivered subs under", "inferred primeable", "inferred subbable"]
    quals = sorted(set(phrase_compiler.QUALIFIERS))
    agencies = sorted(k for k, v in phrase_compiler.AGENCIES.items() if v)
    times = ["in the last 90 days", "in the last year", "in the last 2 years", "fy24", "fy25"]

    c: list[tuple[str, str]] = []

    # S1 — capability lanes. NOTE (learned by compiling): capability contexts
    # require a LITERAL code ('primed in 236220'); sector aliases only bind in
    # sector position ('construction companies that …').
    codes = ["236220", "561210", "561612", "541330", "541512"]
    pscs = ["R499", "S206"]
    for cap, code in itertools.product(caps, codes[:4]):
        c.append(("S1", f"companies {cap} {code}"))
    c += [("S1", f"companies primed in {code}") for code in codes]
    c += [("S1", f"companies delivered subs under {p}") for p in pscs]
    c += [("S1", p) for p in (
        "companies primed in 236220 in VA",
        "companies primed in 236220 lifetime over $5m",
        "active companies primed in 236220",
        "companies primed in 236220 in dsbs",
        "companies inferred subbable 561210 in TX",
        "companies primed in 236220 expiring within 180 days",
        "companies expiring within 180 days")]
    c += [("S1", f"companies {q}") for q in quals]
    c += [("S1", f"{q} companies primed in 236220") for q in ("dsbs", "sub-only")]

    # S2 — event lane (sector + event + time [+ $] [+ agency])
    for sec, ev in itertools.product(sectors[:4], events):
        c.append(("S2", f"{sec} companies that {ev} in the last 90 days"))
    c += [("S2", f"construction companies that {e} {t}")
          for e, t in itertools.product(["novated", "exercised an option"], times)]
    c += [("S2", f"{s} companies {t}") for s, t in itertools.product(sectors[:3], times[:3])]
    c.append(("S2", "construction companies that received new funding over $5m in the last year"))
    c += [("S2", f"{a} construction companies in the last year") for a in agencies[:5]]
    # refusal teaching pair: bare sector on companies
    c += [("S2", f"{s} companies") for s in sectors]

    # S3 — award grain
    for s in ("awards", "orders", "vehicles"):
        c.append(("S3", f"{s} in construction over $5m"))
        c.append(("S3", f"active {s} in construction"))
        c.append(("S3", f"{s} expiring within 180 days"))
        c.append(("S3", f"construction {s} fy25"))
    c += [("S3", f"{a} awards over $5m fy25") for a in agencies[:4]]
    c.append(("S3", "awards with a subcontracting plan expiring within 180 days"))

    # S4 — transaction grain
    for s in ("actions", "mods"):
        c.append(("S4", f"construction {s} in the last 90 days"))
        c.append(("S4", f"{s} terminated for default in the last year"))
    c.append(("S4", "change orders in construction over $1m in the last year"))
    c.append(("S4", "actions with a subcontracting plan in the last 90 days"))

    # S5 — aggregate grammar ('total …' opener)
    c += [("S5", p) for p in (
        "total awarded by industry fy23 to fy25",
        "total awarded by state fy25",
        "total awarded by agency fy25",
        "total active awards by industry",
        "total active awards near 79925 within 50 miles by equipment",
        "total active awards near 79925 within 100 miles by industry",
        "total awarded by industry",
        "total active awards by state",
        "total actions by industry fy25")]

    # de-dup preserving order
    seen: set[str] = set()
    out = []
    for sc, p in c:
        if p not in seen:
            seen.add(p)
            out.append((sc, p))
    return out


# ── Compile every candidate through the REAL entrypoints ─────────────────────

def _verify(cands: list[tuple[str, str]]):
    ok, refused = [], []
    for sc, p in cands:
        try:
            if p.split()[0] == "total":
                compiled = phrase_aggregate.compile_aggregate_phrase(p, today=TODAY)
                detail = compiled.get("title") or compiled["plan"][0].get("group_by", "")
            else:
                compiled = phrase_compiler.compile_phrase(p, today=TODAY)
                detail = f"{compiled['grain']} · {len(compiled['plan'])}-step plan"
            ok.append((sc, p, detail))
        except Exception as e:  # refusal or any compile error — record verbatim
            refused.append((sc, p, str(e)))
    return ok, refused


def main() -> None:
    ok, refused = _verify(_candidates())
    version = phrase_compiler.COMPILER_VERSION
    agg_version = phrase_aggregate.AGG_COMPILER_VERSION
    stamp = TODAY.isoformat()

    def esc(s: str) -> str:
        return html.escape(s, quote=True)

    rows = []
    for sc in SCAFFOLDS:
        inst = [(p, d) for s, p, d in ok if s == sc]
        if not inst:
            continue
        title, template = SCAFFOLDS[sc]
        rows.append(f'<h2>{sc} · {esc(title)} <span class="ct">{len(inst)} verified</span></h2>')
        rows.append(f'<div class="tpl">{esc(template)}</div>')
        rows.append("<ul>")
        for p, detail in inst:
            rows.append(
                f'<li><code class="p" onclick="copyP(this)" title="click to copy">{esc(p)}</code>'
                f'<span class="d">{esc(detail)}</span></li>')
        rows.append("</ul>")

    ref_rows = "".join(
        f'<li><code class="p bad">{esc(p)}</code><div class="why">{esc(msg)}</div></li>'
        for _, p, msg in refused)

    OUT.write_text(f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Verified Phrasebook — {esc(version)}</title>
<style>
  :root {{ --bg:#0d1117; --panel:#161b22; --border:#2d3748; --text:#e6edf3;
          --dim:#8b949e; --accent:#58a6ff; --green:#3fb950; --red:#f85149;
          --purple:#bc8cff; --mono:ui-monospace,SFMono-Regular,Menlo,monospace; }}
  * {{ box-sizing:border-box; margin:0; }}
  body {{ background:var(--bg); color:var(--text);
         font:14px/1.55 -apple-system,"Segoe UI",sans-serif; padding:28px;
         max-width:1000px; margin:0 auto; }}
  h1 {{ font-size:20px; }} h2 {{ font-size:15px; color:var(--accent); margin:28px 0 4px; }}
  .sub {{ color:var(--dim); font-size:13px; margin:6px 0 4px; }}
  .ct {{ color:var(--green); font-size:12px; font-weight:400; margin-left:8px; }}
  .tpl {{ font-family:var(--mono); font-size:12.5px; color:var(--purple);
         background:var(--panel); border:1px solid var(--border); border-radius:8px;
         padding:7px 12px; margin:6px 0 10px; }}
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
<h1>Verified Phrasebook — five scaffolds</h1>
<div style="border:1px solid var(--red); border-radius:8px; background:var(--panel);
     padding:10px 14px; margin:12px 0; font-size:13px; color:var(--red); font-weight:600">
  ⚠ STATUS: PENDING REVIEW (2026-07-15) — the entire phrase grammar is under
  operator revision. Every phrase below compiles TODAY but is TBD: it may be
  cut, replaced, or changed. Do not memorize; do not build against.
</div>
<p class="sub">The whole grammar is FIVE sentence scaffolds. Memorize the five
purple templates; everything below each one is a verified instance — compiled
through the REAL compiler (<code>{esc(version)}</code> /
<code>{esc(agg_version)}</code>) on {stamp}: <b>{len(ok)} verified</b>,
{len(refused)} refused (bottom, with verbatim refusals). Click any phrase to
copy → paste into ⌘K on the HQ tab. Deterministic compiler: what compiled here
compiles there, same vocabulary version.</p>
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
