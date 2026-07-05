"""Resolve firms → LinkedIn /company/ URLs via serper (Google) — company sibling of
pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py.

Icypeas' Company URL Finder is domain-anchored and cleared only 6.5% of this residual tail
(the entities where blitz/pdl already failed on domain match). serper (real Google) resolves by
NAME instead, catching the domain↔LinkedIn mismatches (subsidiary/product domains, legal-name≠brand).

QUERY (calibrated 2026-07-04). Primary = ``{stripped_name} linkedin`` — NO site: filter and NO
quotes: ``"exact name" site:linkedin.com/company`` over-constrains Google to 0 results on ~2/3 of
firms (the LI snippet rarely carries the exact legal name). The bare-name+linkedin query returns a
full organic page every time; we filter to /company/ links and validate in-process.

VALIDATION (precision over recall — a wrong URL would scrape the WRONG company downstream). Accept
the /company/ result with the best NAME-token overlap, and only when the firm's MOST-distinctive
token (longest ≥4-char, non-generic) appears in the slug+title — OR the domain root confirms. Every
candidate's confidence is recorded (domain | strong | name) so the scrape step can gate.

CREDIT SAFETY. serper bills 1 credit/search; the gateway 402s cleanly when the account runs dry
(credits=0, no budget decrement). Ordered best-first (sub$ DESC); ``--budget`` hard-caps the run;
resume skips ueis already in the JSONL. Operator-supervised — run locally via doppler, NOT Modal.

    doppler run -p core-x -c prd -- python3 pipelines/enrichment_company_scrape/resolve_company_linkedin.py \
        --input /Users/benjamincrane/Desktop/icypeas_serper_target_2148_2026-07-04.csv \
        --out   /Users/benjamincrane/Desktop/company_linkedin_resolved.jsonl \
        --budget 2200 --workers 8
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from core.serper_gateway import key_present, search as serper_search  # noqa: E402

_CORP = {"llc", "inc", "incorporated", "corp", "corporation", "co", "company", "ltd", "limited",
         "plc", "pllc", "lp", "llp", "holdings", "holding"}
_STOP = _CORP | {"the", "and", "of", "for", "a", "on", "us", "usa", "group"}
# Semi-generic business words: real tokens, but too common to IDENTIFY a firm on their own — a match
# on ONLY these is what let "VCH Partners"→"CH Investment Partners" and "University Accounting"→
# "Universal Accounting" through. The firm's distinctive token must live OUTSIDE this set.
_SEMI = {"partners", "solutions", "services", "service", "systems", "system", "technologies",
         "technology", "consulting", "consultants", "associates", "international", "enterprises",
         "enterprise", "management", "ventures", "global", "industries", "industry", "development",
         "construction", "contracting", "logistics", "resources", "strategies", "strategy",
         "accounting", "engineering", "professional", "national", "federal", "government", "general",
         "trading", "supply", "products", "corporation", "company", "incorporated"}
_COMP_RE = re.compile(r"linkedin\.com/company/([^/?#]+)", re.I)


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _ascii_tokens(s: str | None) -> list[str]:
    if not s:
        return []
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return [t for t in re.split(r"[^a-z0-9]+", s) if t]


def _strip_suffix(name: str | None) -> str:
    if not name:
        return ""
    s = " ".join(str(name).split()).strip(" .,-")
    toks = s.split()
    while toks and re.sub(r"[^a-z]", "", toks[-1].lower()) in _CORP:
        toks.pop()
    return " ".join(toks).strip(" .,-") or str(name).strip()


def _distinctive(name: str) -> set[str]:
    """Company tokens ≥3 chars, corp designators dropped (keeps acronyms like VCH/SHI/T6-ish)."""
    return {t for t in _ascii_tokens(name) if len(t) >= 3 and t not in _STOP} or {
        t for t in _ascii_tokens(name) if t not in _STOP}


def _domain_root(domain: str | None) -> str | None:
    if not domain:
        return None
    host = domain.lower().strip().split("/")[0]
    parts = [p for p in host.split(".") if p]
    root = parts[-2] if len(parts) >= 2 else (parts[0] if parts else None)
    return root if root and len(root) >= 3 and root not in _STOP else None


def _resolve_one(name: str, domain: str, organic: list[dict]) -> dict:
    """Best /company/ match by name-token overlap; require the key token or domain-root confirm."""
    ntoks = _distinctive(name)
    # The firm's IDENTITY token(s): distinctive tokens OUTSIDE the semi-generic set. A candidate must
    # echo the PRIMARY (longest) identity token — a match on only semi-generic words is not enough.
    core = {t for t in ntoks if t not in _SEMI}
    primary = max(core, key=len) if core else (max(ntoks, key=len) if ntoks else None)
    root = _domain_root(domain)
    best = None  # (score, slug, title, confidence)
    for pos, r in enumerate(organic):
        m = _COMP_RE.search(r.get("link") or "")
        if not m:
            continue
        slug = m.group(1).lower()
        title = r.get("title") or ""
        snippet = r.get("snippet") or ""
        slug_toks = set(_ascii_tokens(slug.replace("-", " ")))
        hay = slug_toks | set(_ascii_tokens(title))
        cat = re.sub(r"[^a-z0-9]", "", (slug + " " + title + " " + snippet).lower())
        overlap = ntoks & hay
        domain_ok = bool(root and len(root) >= 5 and root in cat)
        # primary identity token present as a whole word, or (if ≥4) as a substring of the slug/title.
        primary_ok = bool(primary and (primary in hay or (len(primary) >= 4 and primary in cat)))
        if not (domain_ok or primary_ok):
            continue
        core_ok = bool(core and core <= hay)                 # every identity token present
        score = (len(overlap)
                 + (2 if core_ok else 0)
                 + (3 if domain_ok else 0)
                 + max(0, 3 - pos) * 0.1)
        conf = "domain" if domain_ok else ("strong" if core_ok else "name")
        if best is None or score > best[0]:
            best = (score, slug, title, conf)
    if best is None:
        return {"resolved": False, "linkedin_url": None, "confidence": None, "match_title": None, "slug": None}
    _, slug, title, conf = best
    return {"resolved": True, "linkedin_url": "https://www.linkedin.com/company/" + slug,
            "confidence": conf, "match_title": title[:300], "slug": slug}


def _load(input_path: str) -> list[dict]:
    with open(input_path) as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in ("sub24_usd", "prime24_usd"):
            try:
                r[k] = float(r.get(k) or 0)
            except (ValueError, TypeError):
                r[k] = 0.0
    rows.sort(key=lambda r: r.get("sub24_usd", 0), reverse=True)   # best subawardees first
    return rows


def _done_ueis(out_path: str) -> set[str]:
    if not os.path.exists(out_path):
        return set()
    seen = set()
    with open(out_path) as f:
        for line in f:
            try:
                seen.add(json.loads(line)["uei"])
            except Exception:  # noqa: BLE001
                continue
    return seen


def main() -> None:
    ap = argparse.ArgumentParser(description="Firm → LinkedIn /company/ resolver (serper).")
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--budget", type=int, default=2200, help="max serper credits this run")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not key_present():
        raise RuntimeError("SERPER_API_KEY absent — run under doppler core-x/prd")

    rows = _load(args.input)
    done = _done_ueis(args.out)
    todo = [r for r in rows if r["uei"] not in done][: args.budget]
    log(f"input={len(rows)} done={len(done)} todo={len(todo)} (budget cap {args.budget}, workers {args.workers})")
    if args.dry_run or not todo:
        for r in todo[:10]:
            log(f"  would query: {_strip_suffix(r['name'])} linkedin   [{r['uei']} sub=${int(r['sub24_usd']):,}]")
        log("DRY-RUN — 0 credits")
        return

    lock = threading.Lock()
    counts = {"attempted": 0, "credits": 0, "resolved": 0, "domain": 0, "strong": 0, "name": 0}
    out = open(args.out, "a")

    def one(r):
        q = f"{_strip_suffix(r['name'])} linkedin"
        env = serper_search(q)
        v = _resolve_one(r["name"], r.get("domain") or "", env["organic"])
        return r, q, env, v

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(one, r) for r in todo]
        for fut in as_completed(futs):
            r, q, env, v = fut.result()
            rec = {"uei": r["uei"], "name": r["name"], "domain": r.get("domain"),
                   "sub24_usd": r.get("sub24_usd"), "prime24_usd": r.get("prime24_usd"),
                   "last_sub": r.get("last_sub"), "in_dsbs": r.get("in_dsbs"), "tier": r.get("tier"),
                   "serper_query": q, "credits": env["credits"], "http_status": env["http_status"],
                   "n_organic": len(env["organic"]), "resolved": v["resolved"],
                   "company_linkedin_url": v["linkedin_url"], "confidence": v["confidence"],
                   "match_title": v["match_title"], "li_source": "serper"}
            with lock:
                out.write(json.dumps(rec, default=str) + "\n")
                out.flush()
                counts["attempted"] += 1
                counts["credits"] += env["credits"]
                if v["resolved"]:
                    counts["resolved"] += 1
                    counts[v["confidence"]] += 1
                if counts["attempted"] % 100 == 0:
                    log(f"  {counts['attempted']}/{len(todo)} · credits={counts['credits']} "
                        f"resolved={counts['resolved']} ({counts['resolved']/counts['attempted']:.0%})")
    out.close()
    rate = counts["resolved"] / counts["attempted"] if counts["attempted"] else 0
    log(f"DONE attempted={counts['attempted']} credits={counts['credits']} resolved={counts['resolved']} "
        f"({rate:.1%}) [domain={counts['domain']} strong={counts['strong']} name={counts['name']}] → {args.out}")


if __name__ == "__main__":
    main()
