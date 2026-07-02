"""Compute worker — resolve unresolved DSBS-POC people to LinkedIn ``/in/`` profiles via serper (Google).

Clean-room data plane: DuckDB + pylance read/join the R2 Lance sources; serper.dev supplies the
real-Google search; every credit spent and every validated resolution is committed to Postgres
BEFORE anything touches Lance. Operator-supervised credit burn — run locally via doppler, NOT Modal.

WHY THIS EXISTS
    dsbs_poc_people already resolves the DSBS decision-makers we can match to an existing person
    record (name-matched within uei → person_linkedin_url). The COMPLEMENT — DSBS-declared people we
    have NO person/LinkedIn for — is the find-people worklist. serper (real Google, structured JSON,
    no throttling) resolves the highest-VALUE slice of that complement to a validated LinkedIn /in/
    URL. serper credits are scarce (2,500/account), so spend is ranked top-down and hard-capped.

THE TWO PRECISION LEVERS (why this beats blind scraping / DDG)
  1. Company term = PDL brand, gated. When crosswalk_dsbs_sam.matched_name_consistent AND a PDL
     linkedin_slug+company_name exist, the query company term is the suffix-stripped PDL
     company_name — the LinkedIn-sourced brand the person's profile actually shows ("Glorious
     Cleaning Services", not the SAM legal name "ANOINTED PROFESSIONAL ENTERPRISE"). Otherwise fall
     back to the suffix-stripped SAM/DSBS legal name + domain. ~1 in 4 firms operate under a
     different brand than their legal name — searching the legal name there returns zero/wrong.
  2. Name-AND-company validation. serper's first /in/ link is accepted ONLY when the person name
     tokens overlap the slug/title AND the company core-token or domain-root appears in the
     title/snippet. That company-confirm is what kills namesake false positives; no confident
     match → recorded unresolved, never a banked wrong URL.

HYGIENE GATE (never spend a credit on a non-person). Require first+last alpha tokens ≥2 chars;
    drop org-name-shaped person fields (a corporate designator/"services"/"solutions"/… token, or
    first==last) — e.g. "AGEISS Inc" sitting in the person field.

PRIORITY (spend the scarce credits where accuracy matters most). Tier 0 = won federal $
    (firmographics entity_active_obligated_usd > 0), tier 1 = has_federal_awards, tier 2 = rest;
    within tier: name-consistent first, current_principal over contact_person, more $ first, smaller
    firm (SMB) first, more awards first; subject_id as the stable final tiebreak so "top-K" is
    deterministic and resume-safe.

STORES
    ops.dsbs_poc_linkedin_spend        (PG) — per-subject spend+resolution ledger; the double-spend
                                              guard + resume key. Global ceiling = sum(credits_spent).
    ops.dsbs_poc_linkedin_resolve_runs (PG) — per-run terminal ledger.
    s3://data-sink/active/dsbs_poc_linkedin/  (Lance v2.1) — validated resolutions, materialized
                                              from resolved=true ledger rows. The downstream SoR.

SOURCES (Lance, R2, read-only): dsbs_pocs (the people), dsbs_poc_people (already-resolved, anti-
    joined out), crosswalk_dsbs_sam (PDL match id + name-consistency + legal names + domains),
    pdl_normalized_companies (brand company_name + linkedin_slug, chunked pdl_company_id probe),
    firmographics_company_map_serving (won-$ + employee band + award signal).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' --with 'requests>=2.32' \
      python3 pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py <init_ops|worklist|smoke|resolve|materialize|verify|status>

    resolve flags: --pass primary|fallback  --cap N (this-run limit)  --budget 2500  --reserve 500
                   --workers 8  --dry-run
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

# Repo root on sys.path so `core.serper_gateway` resolves on a local `python3 pipelines/...` run.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from core.serper_gateway import key_present, search as serper_search  # noqa: E402

FEED = "dsbs_poc_linkedin"
_ACTIVE = "s3://data-sink/active"
DATASET_URI = os.environ.get("DSBS_POC_LINKEDIN_URI", f"{_ACTIVE}/dsbs_poc_linkedin/").rstrip("/") + "/"

DSBS_POCS_URI = f"{_ACTIVE}/dsbs_pocs/"
DSBS_POC_PEOPLE_URI = f"{_ACTIVE}/dsbs_poc_people/"
XWALK_URI = f"{_ACTIVE}/crosswalk_dsbs_sam/"
PDL_NORM_URI = f"{_ACTIVE}/pdl_normalized_companies/"
FIRMO_URI = f"{_ACTIVE}/firmographics_company_map_serving/"

DATA_STORAGE_VERSION = "2.1"
PDL_CHUNK = 8000  # pdl_company_id per indexed IN-list probe (never full-scan the ~30M-row PDL)

BTREE_INDEXES = ["subject_id", "uei", "linkedin_url"]
BITMAP_INDEXES = ["company_source", "name_consistent", "poc_type", "priority_tier"]

DEFAULT_BUDGET = int(os.environ.get("SERPER_BUDGET", "2500"))
DEFAULT_RESERVE = int(os.environ.get("SERPER_RESERVE", "500"))
DEFAULT_WORKERS = int(os.environ.get("SERPER_WORKERS", "8"))

DUCKDB_MEMORY_LIMIT = os.environ.get("RESOLVE_MEMORY_LIMIT", "12GB")
DUCKDB_THREADS = int(os.environ.get("RESOLVE_THREADS", "4"))
SCRATCH = os.environ.get("RESOLVE_SCRATCH", "/tmp/dsbs_poc_linkedin")

# Corporate designators + org-shape tokens: their presence in a *person* field means it is a
# company name, not a human. Also the suffix set peeled off a legal name for the query term.
_SUFFIX = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "cpa", "esq", "pe", "mba", "jd", "dds"}
# Honorifics/titles that leak into the DSBS first-name slot ("Dr. Might - CEO" → first="Dr").
# A bare honorific as a name is not a person handle: re-derive from full_name or drop the row.
_HONOR = {"dr", "mr", "mrs", "ms", "miss", "mx", "prof", "professor", "sir", "madam", "rev",
          "hon", "capt", "col", "sgt", "lt", "maj", "gen", "cmdr", "messrs", "mstr"}
_CORP = {"llc", "inc", "incorporated", "corp", "corporation", "co", "company", "ltd", "limited",
         "plc", "pllc", "lp", "llp", "holdings"}
_ORG_TOKENS = _CORP | {"group", "services", "service", "solutions", "enterprise", "enterprises",
                       "associates", "systems", "technologies", "consulting", "partners",
                       "industries", "international", "management", "contracting", "construction"}
_NAME_STOP = _SUFFIX | _CORP
# company core-token stoplist for validation (a token must be distinctive to confirm the firm)
_COMPANY_STOP = _ORG_TOKENS | {"the", "and", "of", "for", "a", "on", "us", "usa"}

# employee_size_band → lower-bound ordinal (SMB = smaller first; unknown sorts last)
_BAND_ORD = {"1-10": 1, "11-50": 11, "51-200": 51, "201-500": 201, "501-1000": 501,
             "1001-5000": 1001, "5001-10000": 5001, "10001+": 10001}


def _dollar_band(obl: float) -> int:
    """Won-$ VALUE band, not raw magnitude. The $1M–$50M sweet spot ranks first: an established
    small/mid contractor whose SBA-declared principal IS the reachable LinkedIn identity. Mega-primes
    (>$50M) are deprioritized below emerging firms — figurehead principals, ultra-common names, and
    lower resolve rates make their credits convert worst. Micro (<$100k) sorts last (dormant/marginal).
    Raw $ is demoted to a deep tiebreak in _sort so it never dominates the head of the list."""
    if 1_000_000 <= obl <= 50_000_000:
        return 0   # sweet spot
    if 100_000 <= obl < 1_000_000:
        return 1   # emerging
    if obl > 50_000_000:
        return 2   # mega-prime
    return 3       # micro / marginal


def log(m: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials).")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED / HQX_DB_URL unset (hqx-postgres).")
    return dsn


# ── string helpers ──────────────────────────────────────────────────────────────
def _ascii_tokens(s: str | None) -> list[str]:
    if not s:
        return []
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return [t for t in re.split(r"[^a-z0-9]+", s) if t]


def _name_token_set(s: str | None) -> set[str]:
    """Firm-name tokens (≥2 chars, corp designators dropped) for the org-in-person-field guard."""
    return {t for t in _ascii_tokens(s) if len(t) >= 2 and t not in _CORP}


def _strip_company_suffix(name: str | None) -> str | None:
    """Peel trailing corporate designators + punctuation off a legal/brand name for the query
    term ("Advanced Systems Technology, Inc." → "Advanced Systems Technology")."""
    if not name:
        return None
    s = " ".join(str(name).split()).strip(" .,-")
    toks = s.split()
    while toks and re.sub(r"[^a-z]", "", toks[-1].lower()) in _CORP:
        toks.pop()
    out = " ".join(toks).strip(" .,-")
    return out or None


def _clean_tok(s: str | None) -> str:
    return re.sub(r"[^a-z]", "", (s or "").lower())


def _valid_pair(f: str, l: str) -> bool:
    return (len(f) >= 2 and len(l) >= 2 and f != l
            and f not in _HONOR and l not in _HONOR
            and f not in _ORG_TOKENS and l not in _ORG_TOKENS)


def _derive_names(first: str | None, last: str | None,
                  full_name: str | None) -> tuple[str, str] | None:
    """Hygiene gate + honorific recovery → display (first, last) or None.

    Accept the explicit first/last when they form a clean human pair. Otherwise (honorific in the
    first slot, single-token, or org-shaped) re-derive from full_name by dropping honorifics and
    suffixes; if <2 real name tokens remain, drop the row rather than spend a credit on a non-name.
    """
    if _valid_pair(_clean_tok(first), _clean_tok(last)):
        return first.strip(), last.strip()
    toks = [(d, _clean_tok(d)) for d in re.split(r"[^A-Za-z]+", full_name or "") if d]
    toks = [(d, c) for d, c in toks if c and c not in _HONOR and c not in _SUFFIX
            and c not in _ORG_TOKENS and len(c) >= 2]
    if len(toks) < 2:
        return None
    (df, cf), (dl, cl) = toks[0], toks[-1]
    return (df, dl) if _valid_pair(cf, cl) else None


def _company_tokens(company_term: str | None) -> set[str]:
    """Distinctive tokens of the company term the profile must echo to confirm the firm."""
    return {t for t in _ascii_tokens(company_term) if len(t) >= 4 and t not in _COMPANY_STOP}


def _domain_root(domain: str | None) -> str | None:
    if not domain:
        return None
    host = domain.lower().strip().split("/")[0]
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        root = parts[-2]
    elif parts:
        root = parts[0]
    else:
        return None
    return root if len(root) >= 3 and root not in _COMPANY_STOP else None


_IN_RE = re.compile(r"linkedin\.com/in/([^/?#]+)", re.I)


def _slug_from_link(link: str | None) -> str | None:
    if not link:
        return None
    m = _IN_RE.search(link)
    return m.group(1).lower() if m else None


def _validate(first: str, last: str, company_term: str | None, domain: str | None,
              organic: list[dict]) -> dict:
    """Accept the first /in/ result whose name AND company both confirm. Precision over recall.

    name_ok    — both first and last appear across (slug + title); handles Google's nickname
                 relaxation because the surname anchors and the slug/title carry the real name.
    company_ok — company core-token OR domain-root appears anywhere in slug+title+snippet.
    Returns {resolved, linkedin_url, match_title, match_snippet, validation_reason}.
    """
    ftok = re.sub(r"[^a-z]", "", first.lower())
    ltok = re.sub(r"[^a-z]", "", last.lower())
    root = _domain_root(domain)
    # Confirm on company tokens DISTINCT from the person's name — an eponymous firm's surname
    # appears on the person's own profile and must not self-confirm.
    comp_toks = _company_tokens(company_term) - {ftok, ltok}
    saw_in = False
    for r in organic:
        slug = _slug_from_link(r.get("link"))
        if not slug:
            continue
        saw_in = True
        title = (r.get("title") or "")
        snippet = (r.get("snippet") or "")
        # Name: LinkedIn slugs concatenate tokens (/in/mardinorman) → substring match on a
        # spaces-removed name haystack (slug + title).
        name_hay = re.sub(r"[^a-z]", "", (slug + " " + title).lower())
        name_ok = ftok in name_hay and ltok in name_hay
        # Company: EXACT word-token match against title/snippet so "tech" does NOT confirm via
        # "technology"; the domain root — distinctive by construction — may confirm as a substring.
        text_toks = set(_ascii_tokens(title + " " + snippet))
        cat = re.sub(r"[^a-z0-9]", "", (slug + " " + title + " " + snippet).lower())
        company_ok = any(c in text_toks for c in comp_toks) or bool(root and len(root) >= 5 and root in cat)
        if name_ok and company_ok:
            return {"resolved": True, "linkedin_url": "https://www.linkedin.com/in/" + slug,
                    "match_title": title[:300], "match_snippet": snippet[:500],
                    "validation_reason": "validated"}
    return {"resolved": False, "linkedin_url": None, "match_title": None, "match_snippet": None,
            "validation_reason": "no_confident_match" if saw_in else "no_in_link"}


def _build_query(first: str, last: str, company_term: str | None, domain: str | None) -> str:
    """Retrieval-optimized primary query. Exact name + site: yields the most /in/ candidates;
    the company/domain are NOT AND-ed into the Google query — calibration showed name+company+site:
    over-constrains Google to ~0 results because it forces the company string onto the profile
    page. Company/domain gate ACCEPTANCE in _validate instead, preserving precision without
    starving retrieval."""
    return f'"{first} {last}" site:linkedin.com/in'


def _fallback_query(first: str, last: str, company_term: str | None, domain: str | None) -> str | None:
    """Miss-rescue via a DIFFERENT retrieval: co-occur the exact name with the firm and NO site:
    filter, so a common-name profile the bare-name query failed to rank can surface through
    company context (serper still returns the /in/ link, which _validate then confirms)."""
    firm = company_term or domain
    return f'"{first} {last}" {firm}' if firm else None


# ── worklist ────────────────────────────────────────────────────────────────────
def _new_con():
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    os.makedirs(f"{SCRATCH}/spill", exist_ok=True)
    con.execute(f"SET temp_directory='{SCRATCH}/spill'")
    con.execute("SET preserve_insertion_order=false")
    return con


def build_worklist() -> list[dict]:
    """Ranked, hygiene-gated, company-term-resolved worklist (best value first). Pure read — no
    credits, no writes. Deterministic ordering (subject_id final tiebreak) so top-K is stable."""
    import lance

    so = _r2_so()
    con = _new_con()

    def reg(name, uri, cols):
        con.register(name, lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader())

    reg("dp", DSBS_POCS_URI, ["uei", "name_key", "first_name", "last_name", "full_name", "title", "poc_type"])
    reg("pp", DSBS_POC_PEOPLE_URI, ["uei", "name_key"])
    reg("xw", XWALK_URI, ["uei", "dsbs_legal_business_name", "sam_legal_business_name",
                          "best_domain", "matched_domain", "matched_pdl_company_id",
                          "matched_name_consistent"])
    reg("fm", FIRMO_URI, ["uei", "entity_active_obligated_usd", "employee_size_band",
                          "has_federal_awards", "award_count"])
    for t in ("dp", "pp", "xw", "fm"):
        con.execute(f"CREATE TEMP TABLE {t}_t AS SELECT * FROM {t}")
        con.unregister(t)

    # 1 row per (uei, name_key); deterministic name/title; poc_type rollup.
    con.execute("""CREATE TEMP TABLE dp_agg AS
        SELECT uei, name_key,
               min(first_name) first_name, min(last_name) last_name,
               max(full_name) full_name, max(title) title,
               CASE WHEN bool_or(poc_type='current_principal') AND bool_or(poc_type='contact_person') THEN 'both'
                    WHEN bool_or(poc_type='current_principal') THEN 'current_principal'
                    ELSE 'contact_person' END poc_type
        FROM dp_t
        WHERE name_key IS NOT NULL AND nullif(trim(first_name),'') IS NOT NULL
          AND nullif(trim(last_name),'') IS NOT NULL
        GROUP BY uei, name_key""")

    # anti-join already-resolved (uei,name_key); left-join firm enrichment.
    rows = con.execute("""
        SELECT d.uei, d.name_key, d.first_name, d.last_name, d.full_name, d.title, d.poc_type,
               x.dsbs_legal_business_name, x.sam_legal_business_name,
               x.best_domain, x.matched_domain, x.matched_pdl_company_id,
               coalesce(x.matched_name_consistent, false) AS name_consistent,
               f.entity_active_obligated_usd AS obligated_usd,
               f.employee_size_band, coalesce(f.has_federal_awards, false) AS has_federal_awards,
               coalesce(f.award_count, 0) AS award_count
        FROM dp_agg d
        LEFT JOIN pp_t pp ON pp.uei=d.uei AND pp.name_key=d.name_key
        LEFT JOIN xw_t x  ON x.uei=d.uei
        LEFT JOIN fm_t f  ON f.uei=d.uei
        WHERE pp.uei IS NULL
    """).to_arrow_table().to_pylist()
    con.close()

    # PDL brand probe — only the matched_pdl_company_id set, chunked (indexed IN-list).
    pdl_ids = sorted({r["matched_pdl_company_id"] for r in rows if r.get("matched_pdl_company_id")})
    pdl: dict[str, tuple] = {}
    if pdl_ids:
        pnc = lance.dataset(PDL_NORM_URI, storage_options=so)
        esc = lambda x: str(x).replace("'", "''")  # noqa: E731
        for i in range(0, len(pdl_ids), PDL_CHUNK):
            ch = pdl_ids[i:i + PDL_CHUNK]
            flt = "pdl_company_id IN (" + ",".join("'%s'" % esc(x) for x in ch) + ")"
            t = pnc.scanner(columns=["pdl_company_id", "company_name", "linkedin_slug"],
                            filter=flt).to_table()
            for pid, cn, sl in zip(t.column("pdl_company_id").to_pylist(),
                                   t.column("company_name").to_pylist(),
                                   t.column("linkedin_slug").to_pylist()):
                if pid and pid not in pdl:
                    pdl[pid] = (cn, sl)
        log(f"pdl brand probe: {len(pdl_ids):,} ids → {len(pdl):,} resolved")

    work: list[dict] = []
    for r in rows:
        dn = _derive_names(r.get("first_name"), r.get("last_name"), r.get("full_name"))
        if not dn:
            continue
        first, last = dn
        pid = r.get("matched_pdl_company_id")
        pdl_name, pdl_slug = pdl.get(pid, (None, None))
        # LEVER 1 — PDL brand when name-consistent + slug present, else stripped legal name.
        if r["name_consistent"] and pdl_slug and pdl_name:
            company_term, company_source = _strip_company_suffix(pdl_name), "pdl"
        else:
            company_term = _strip_company_suffix(r.get("sam_legal_business_name")
                                                 or r.get("dsbs_legal_business_name"))
            company_source = "legal"
        domain = r.get("best_domain") or r.get("matched_domain")
        if not company_term and not domain:
            continue  # nothing to disambiguate on → un-validatable, skip
        # org-in-person-field guard: when BOTH name tokens are already firm-name tokens, the
        # "person" is the company name repeated (e.g. "UIC Government" at "UIC Government Services").
        ctoks = (_name_token_set(company_term) | _name_token_set(r.get("sam_legal_business_name"))
                 | _name_token_set(r.get("dsbs_legal_business_name")))
        if ctoks and _clean_tok(first) in ctoks and _clean_tok(last) in ctoks:
            continue

        obl = float(r["obligated_usd"] or 0.0)
        tier = 0 if obl > 0 else (1 if r["has_federal_awards"] else 2)
        poc_rank = 0 if r["poc_type"] in ("current_principal", "both") else 1
        band_ord = _BAND_ORD.get((r.get("employee_size_band") or "").strip(), 99999)
        subject_id = f"{r['uei']}|{r['name_key']}"
        work.append({
            "subject_id": subject_id, "uei": r["uei"], "name_key": r["name_key"],
            "first_name": first, "last_name": last, "full_name": r.get("full_name"),
            "poc_type": r["poc_type"], "company_term": company_term, "company_source": company_source,
            "domain": domain, "name_consistent": bool(r["name_consistent"]),
            "obligated_usd": obl, "priority_tier": tier, "award_count": int(r["award_count"] or 0),
            # tier → match-confidence → decision-maker → VALUE BAND (sweet spot first) →
            # SMB employee band (small first) → obligated-$ ASCENDING (smaller/more-reachable firm
            # first; raw-$ never leads) → more awards → stable id
            "_sort": (tier, 0 if r["name_consistent"] else 1, poc_rank, _dollar_band(obl),
                      band_ord, obl, -int(r["award_count"] or 0), subject_id),
        })

    work.sort(key=lambda w: w["_sort"])
    for rank, w in enumerate(work, 1):
        w["priority_rank"] = rank
        w.pop("_sort", None)
    return work


# ── ledger / spend ──────────────────────────────────────────────────────────────
def _apply_sql(paths: list[str]) -> None:
    import psycopg
    from pathlib import Path

    d = Path(__file__).parent
    with psycopg.connect(_dsn()) as c, c.cursor() as cur:
        for p in paths:
            cur.execute(d.joinpath(p).read_text())
        c.commit()


def _ledger_state() -> tuple[int, set[str]]:
    """(credits already spent, subject_ids already queried)."""
    import psycopg

    with psycopg.connect(_dsn()) as c, c.cursor() as cur:
        cur.execute("SELECT coalesce(sum(credits_spent),0), coalesce(count(*),0) "
                    "FROM ops.dsbs_poc_linkedin_spend")
        spent, _ = cur.fetchone()
        cur.execute("SELECT subject_id FROM ops.dsbs_poc_linkedin_spend")
        seen = {r[0] for r in cur.fetchall()}
    return int(spent or 0), seen


def _fallback_candidates() -> set[str]:
    import psycopg

    with psycopg.connect(_dsn()) as c, c.cursor() as cur:
        cur.execute("SELECT subject_id FROM ops.dsbs_poc_linkedin_spend "
                    "WHERE resolved=false AND fallback_done=false "
                    "AND domain IS NOT NULL AND name_consistent=true")
        return {r[0] for r in cur.fetchall()}


_INSERT_SQL = """
INSERT INTO ops.dsbs_poc_linkedin_spend
  (subject_id, uei, name_key, first_name, last_name, full_name, poc_type, company_term,
   company_source, domain, name_consistent, obligated_usd, priority_tier, priority_rank,
   query_variant, serper_query, credits_spent, http_status, n_organic, resolved, linkedin_url,
   match_title, match_snippet, validation_reason, raw_json)
VALUES (%(subject_id)s,%(uei)s,%(name_key)s,%(first_name)s,%(last_name)s,%(full_name)s,
   %(poc_type)s,%(company_term)s,%(company_source)s,%(domain)s,%(name_consistent)s,
   %(obligated_usd)s,%(priority_tier)s,%(priority_rank)s,'primary',%(serper_query)s,
   %(credits_spent)s,%(http_status)s,%(n_organic)s,%(resolved)s,%(linkedin_url)s,
   %(match_title)s,%(match_snippet)s,%(validation_reason)s,%(raw_json)s)
ON CONFLICT (subject_id) DO NOTHING
"""

_FALLBACK_SQL = """
UPDATE ops.dsbs_poc_linkedin_spend SET
  fallback_done=true, fallback_query=%(fallback_query)s, query_variant='primary+fallback',
  credits_spent=credits_spent+%(add_credits)s, http_status=%(http_status)s, n_organic=%(n_organic)s,
  resolved=%(resolved)s, linkedin_url=%(linkedin_url)s, match_title=%(match_title)s,
  match_snippet=%(match_snippet)s, validation_reason=%(validation_reason)s, raw_json=%(raw_json)s
WHERE subject_id=%(subject_id)s
"""


def _spend_primary(todo: list[dict], workers: int, write_lock, conn) -> tuple[int, int]:
    from psycopg.types.json import Jsonb

    credits = resolved = 0
    cur = conn.cursor()

    def one(w):
        q = _build_query(w["first_name"], w["last_name"], w["company_term"], w["domain"])
        env = serper_search(q)
        v = _validate(w["first_name"], w["last_name"], w["company_term"], w["domain"], env["organic"])
        return w, q, env, v

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one, w) for w in todo]
        for fut in as_completed(futs):
            w, q, env, v = fut.result()
            row = {**{k: w.get(k) for k in ("subject_id", "uei", "name_key", "first_name",
                     "last_name", "full_name", "poc_type", "company_term", "company_source",
                     "domain", "name_consistent", "obligated_usd", "priority_tier", "priority_rank")},
                   "serper_query": q, "credits_spent": env["credits"], "http_status": env["http_status"],
                   "n_organic": len(env["organic"]), "resolved": v["resolved"],
                   "linkedin_url": v["linkedin_url"], "match_title": v["match_title"],
                   "match_snippet": v["match_snippet"], "validation_reason": v["validation_reason"],
                   "raw_json": Jsonb(env["organic"]) if env["ok"] else None}
            with write_lock:
                cur.execute(_INSERT_SQL, row)
                conn.commit()
            credits += env["credits"]
            resolved += int(v["resolved"])
    return credits, resolved


def _spend_fallback(todo: list[dict], workers: int, write_lock, conn) -> tuple[int, int]:
    from psycopg.types.json import Jsonb

    credits = resolved = 0
    cur = conn.cursor()

    def one(w):
        q = _fallback_query(w["first_name"], w["last_name"], w["company_term"], w["domain"])
        env = serper_search(q)
        v = _validate(w["first_name"], w["last_name"], w["company_term"], w["domain"], env["organic"])
        return w, q, env, v

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one, w) for w in todo]
        for fut in as_completed(futs):
            w, q, env, v = fut.result()
            row = {"subject_id": w["subject_id"], "fallback_query": q, "add_credits": env["credits"],
                   "http_status": env["http_status"], "n_organic": len(env["organic"]),
                   "resolved": v["resolved"], "linkedin_url": v["linkedin_url"],
                   "match_title": v["match_title"], "match_snippet": v["match_snippet"],
                   "validation_reason": v["validation_reason"],
                   "raw_json": Jsonb(env["organic"]) if env["ok"] else None}
            with write_lock:
                cur.execute(_FALLBACK_SQL, row)
                conn.commit()
            credits += env["credits"]
            resolved += int(v["resolved"])
    return credits, resolved


def _record_run(pass_name, budget, reserve, ceiling, prior, attempted, credits, resolved,
                unresolved, status, error, started, completed) -> None:
    import psycopg

    try:
        with psycopg.connect(_dsn()) as c, c.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.dsbs_poc_linkedin_resolve_runs
                     (feed, pass_name, budget, reserve, ceiling, prior_spent, attempted,
                      credits_spent, resolved, unresolved, status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, pass_name, budget, reserve, ceiling, prior, attempted, credits, resolved,
                 unresolved, status, error, started, completed))
            c.commit()
    except Exception as e:  # noqa: BLE001 — audit must not mask the spend
        log(f"WARN: resolve_runs write failed: {e}")


# ── commands ──────────────────────────────────────────────────────────────────
def cmd_init_ops(_args) -> None:
    _apply_sql(["ops_dsbs_poc_linkedin_spend.sql", "ops_dsbs_poc_linkedin_resolve_runs.sql"])
    log("ops.dsbs_poc_linkedin_spend + ops.dsbs_poc_linkedin_resolve_runs DDL applied")


def cmd_worklist(args) -> None:
    import json

    work = build_worklist()
    by_tier: dict[int, int] = {}
    by_src: dict[str, int] = {}
    nc = 0
    for w in work:
        by_tier[w["priority_tier"]] = by_tier.get(w["priority_tier"], 0) + 1
        by_src[w["company_source"]] = by_src.get(w["company_source"], 0) + 1
        nc += int(w["name_consistent"])
    summary = {
        "total_gated": len(work),
        "by_tier": {"0_won_$": by_tier.get(0, 0), "1_has_awards": by_tier.get(1, 0),
                    "2_rest": by_tier.get(2, 0)},
        "name_consistent": nc, "company_term_source": by_src,
        "top_k_default": min(len(work), DEFAULT_BUDGET - DEFAULT_RESERVE),
    }
    print(json.dumps(summary, indent=2))
    print("\n── sample (top 12) ──")
    for w in work[:12]:
        q = _build_query(w["first_name"], w["last_name"], w["company_term"], w["domain"])
        print(f"  #{w['priority_rank']:<5} t{w['priority_tier']} ${int(w['obligated_usd']):>12,} "
              f"nc={int(w['name_consistent'])} {w['company_source']:5} | {q}")
    if args.out:
        import pyarrow as pa
        import pyarrow.parquet as pq
        cols = ["subject_id", "uei", "name_key", "first_name", "last_name", "poc_type",
                "company_term", "company_source", "domain", "name_consistent", "obligated_usd",
                "priority_tier", "priority_rank"]
        pq.write_table(pa.table({c: [w[c] for w in work] for c in cols}), args.out)
        log(f"wrote worklist snapshot → {args.out} ({len(work):,} rows)")


def cmd_smoke(_args) -> None:
    import json

    if not key_present():
        raise RuntimeError("SERPER_API_KEY absent — check doppler core-x/prd injection")
    work = build_worklist()
    w = work[0] if work else {"first_name": "Aaron", "last_name": "Early",
                              "company_term": None, "domain": None}
    q = _build_query(w["first_name"], w["last_name"], w.get("company_term"), w.get("domain"))
    log(f"smoke query (1 credit): {q}")
    env = serper_search(q)
    v = _validate(w["first_name"], w["last_name"], w.get("company_term"), w.get("domain"), env["organic"])
    print(json.dumps({"ok": env["ok"], "http_status": env["http_status"], "credits": env["credits"],
                      "n_organic": len(env["organic"]), "validation": v,
                      "top3": env["organic"][:3]}, indent=2, default=str))


def cmd_resolve(args) -> None:
    import psycopg

    started = dt.datetime.now(dt.timezone.utc)
    budget, reserve = args.budget, args.reserve
    ceiling = budget - reserve
    prior, seen = _ledger_state()
    remaining = ceiling - prior
    log(f"budget={budget} reserve={reserve} ceiling={ceiling} prior_spent={prior} remaining={remaining}")
    if remaining <= 0:
        log("global ceiling reached — nothing to spend"); return
    this_run = min(args.cap, remaining) if args.cap else remaining

    work = build_worklist()
    if args.pass_name == "primary":
        todo = [w for w in work if w["subject_id"] not in seen][:this_run]
    else:
        cands = _fallback_candidates()
        rankmap = {w["subject_id"]: w for w in work}
        todo = [rankmap[s] for s in
                sorted(cands, key=lambda s: rankmap[s]["priority_rank"] if s in rankmap else 1 << 30)
                if s in rankmap][:this_run]
    log(f"pass={args.pass_name} eligible→todo={len(todo)} (this-run cap {this_run})")

    if args.dry_run or not todo:
        for w in todo[:15]:
            q = (_build_query(w["first_name"], w["last_name"], w["company_term"], w["domain"])
                 if args.pass_name == "primary"
                 else _fallback_query(w["first_name"], w["last_name"], w["company_term"], w["domain"]))
            print(f"  #{w['priority_rank']:<5} t{w['priority_tier']} {w['company_source']:5} | {q}")
        log(f"DRY-RUN — would spend ≤{len(todo)} credits (0 spent)")
        return

    if not key_present():
        raise RuntimeError("SERPER_API_KEY absent — aborting before spend")

    write_lock = threading.Lock()
    status, error, credits, resolved = "error", None, 0, 0
    conn = psycopg.connect(_dsn())
    try:
        if args.pass_name == "primary":
            credits, resolved = _spend_primary(todo, args.workers, write_lock, conn)
        else:
            credits, resolved = _spend_fallback(todo, args.workers, write_lock, conn)
        status = "success"
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
        raise
    finally:
        conn.close()
        _record_run(args.pass_name, budget, reserve, ceiling, prior, len(todo), credits, resolved,
                    len(todo) - resolved, status, error, started, dt.datetime.now(dt.timezone.utc))
        log(f"pass={args.pass_name} done: attempted={len(todo)} credits={credits} "
            f"resolved={resolved} rate={resolved / len(todo):.1%}" if todo else "no-op")


def cmd_materialize(_args) -> None:
    import lance
    import psycopg
    import pyarrow as pa

    so = _r2_so()
    cols = ["subject_id", "uei", "name_key", "first_name", "last_name", "full_name", "poc_type",
            "company_term", "company_source", "domain", "name_consistent", "obligated_usd",
            "priority_tier", "priority_rank", "query_variant", "linkedin_url", "match_title",
            "match_snippet", "queried_at"]
    # Only materialize subjects that STILL pass the current gate — drops org-in-person names
    # (e.g. "UIC Government") that were spent under an earlier, looser hygiene gate.
    valid_ids = {w["subject_id"] for w in build_worklist()}
    with psycopg.connect(_dsn()) as c, c.cursor() as cur:
        cur.execute(f"SELECT {','.join(cols)} FROM ops.dsbs_poc_linkedin_spend "
                    "WHERE resolved=true AND linkedin_url IS NOT NULL ORDER BY priority_rank")
        recs = [r for r in cur.fetchall() if r[0] in valid_ids]
    if not recs:
        log("no resolved rows in ledger — nothing to materialize"); return
    data = {c: [r[i] for r in recs] for i, c in enumerate(cols)}
    data["resolved_at"] = data.pop("queried_at")
    tbl = pa.table(data)
    lance.write_dataset(tbl, DATASET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    assert n == len(recs), f"write-integrity {n} != {len(recs)}"
    names = set(ds.schema.names)
    for col in BTREE_INDEXES:
        if col in names:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
    for col in BITMAP_INDEXES:
        if col in names:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
    log(f"WROTE {DATASET_URI} rows={n:,} (resolved LinkedIn /in/ profiles) + indexes")


def cmd_status(_args) -> None:
    import json

    import psycopg

    with psycopg.connect(_dsn()) as c, c.cursor() as cur:
        cur.execute("""SELECT count(*), coalesce(sum(credits_spent),0),
            count(*) FILTER (WHERE resolved), count(*) FILTER (WHERE fallback_done)
            FROM ops.dsbs_poc_linkedin_spend""")
        n, credits, res, fb = cur.fetchone()
        cur.execute("""SELECT priority_tier, count(*), count(*) FILTER (WHERE resolved)
            FROM ops.dsbs_poc_linkedin_spend GROUP BY 1 ORDER BY 1""")
        tiers = cur.fetchall()
    ceiling = DEFAULT_BUDGET - DEFAULT_RESERVE
    print(json.dumps({
        "subjects_queried": n, "credits_spent": int(credits or 0),
        "resolved": res, "resolve_rate": (res / n if n else 0), "fallback_done": fb,
        "ceiling": ceiling, "remaining_to_ceiling": ceiling - int(credits or 0),
        "by_tier": [{"tier": t, "queried": q, "resolved": rr} for t, q, rr in tiers],
    }, indent=2, default=str))


def cmd_revalidate(_args) -> None:
    """Re-apply the CURRENT _validate to every ledger row's stored serper payload — zero credits.
    Validation logic can tighten (precision fixes) and re-flag past resolutions for free."""
    import psycopg

    with psycopg.connect(_dsn()) as c, c.cursor() as cur:
        cur.execute("SELECT subject_id, first_name, last_name, company_term, domain, resolved, "
                    "linkedin_url, raw_json FROM ops.dsbs_poc_linkedin_spend WHERE raw_json IS NOT NULL")
        rows = cur.fetchall()
        changed = 0
        for sid, fn, ln, ct, dom, was_res, was_url, raw in rows:
            if not fn or not ln:
                continue
            v = _validate(fn, ln, ct, dom, raw or [])
            if bool(v["resolved"]) != bool(was_res) or v["linkedin_url"] != was_url:
                cur.execute("UPDATE ops.dsbs_poc_linkedin_spend SET resolved=%s, linkedin_url=%s, "
                            "match_title=%s, match_snippet=%s, validation_reason=%s WHERE subject_id=%s",
                            (v["resolved"], v["linkedin_url"], v["match_title"], v["match_snippet"],
                             v["validation_reason"], sid))
                changed += 1
        c.commit()
    log(f"revalidate: re-scored {len(rows):,} rows, {changed} changed")


def cmd_verify(_args) -> None:
    import json

    import lance

    so = _r2_so()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                 for i in ds.list_indices())
    print(json.dumps({"uri": DATASET_URI, "rows": ds.count_rows(), "cols": len(ds.schema),
                      "schema": [f.name for f in ds.schema], "indices": idx}, indent=2, default=str))


def main() -> None:
    ap = argparse.ArgumentParser(description="DSBS-POC → LinkedIn resolver (serper).")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("init_ops")
    wl = sub.add_parser("worklist"); wl.add_argument("--out", default=None)
    sub.add_parser("smoke")
    rs = sub.add_parser("resolve")
    rs.add_argument("--pass", dest="pass_name", choices=["primary", "fallback"], default="primary")
    rs.add_argument("--cap", type=int, default=0, help="max credits THIS run (0 = up to ceiling)")
    rs.add_argument("--budget", type=int, default=DEFAULT_BUDGET)
    rs.add_argument("--reserve", type=int, default=DEFAULT_RESERVE)
    rs.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    rs.add_argument("--dry-run", action="store_true")
    sub.add_parser("materialize")
    sub.add_parser("status")
    sub.add_parser("revalidate")
    sub.add_parser("verify")
    args = ap.parse_args()

    dispatch = {"init_ops": cmd_init_ops, "worklist": cmd_worklist, "smoke": cmd_smoke,
                "resolve": cmd_resolve, "materialize": cmd_materialize, "status": cmd_status,
                "revalidate": cmd_revalidate, "verify": cmd_verify}
    fn = dispatch.get(args.cmd)
    if not fn:
        ap.print_help(); sys.exit(2)
    fn(args)


if __name__ == "__main__":
    main()
