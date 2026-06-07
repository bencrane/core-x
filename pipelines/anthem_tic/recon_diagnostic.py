#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11,<3.13"
# dependencies = [
#   "pylance>=7",
#   "duckdb>=1.5,<2",
#   "pyarrow>=17",
#   "requests>=2.32",
# ]
# ///
"""Anthem TiC reconnaissance + scale diagnostic — READ-ONLY, public data only.

Decodes the Anthem Transparency-in-Coverage "search by EIN" path (which is a flat
PUBLIC S3 keyspace, NOT a server-side API behind the Akamai-protected www.anthem.com
shell), then sizes a national harvest against the LOCAL Form 5500 EIN universe.

Resolution contract (reverse-engineered from the portal's client-side script.js):
    EIN  XX-XXXXXXX  ──strip dash──▶  {EIN9}
    GET https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/{EIN9}.json
        200 → employer covered; body lists In-Network / Out-of-Network / BCBS-Out-of-Area
              / Carelon file links as [{url, displayname}, ...]
        404 → employer NOT in Anthem's book (uses a different primary payer)
    Enumeration: bucket LIST is AccessDenied, so the complete EIN universe is the
    27 namesearch/{a-z}.json + others.json directory files ({"namesearch":[{ein,name}]}).

Phases:
  1. Pull Anthem's full namesearch book of business → exact set of covered EINs.
  2. Read local Form 5500 EIN universe (main ∪ sf) from the LanceDB lake.
  3. EXACT intersect → true corpus-wide hit rate (not a sampled estimate).
  4. Select the 100-EIN diagnostic cohort: Tier A (50 largest national, by participants)
     + Tier B (50 mid-market in the target state).
  5. Live-probe all 100 against S3 → status / latency / payload bytes / per-category file
     counts. Capture one real hit payload verbatim for the API blueprint.
  6. Schedule A carrier triage — quantify the pre-scrape prune.
  7. Render Markdown report (stdout + docs/reference/ANTHEM_TIC_RECON_DIAGNOSTIC.md).

No mutation of any SoR. Writes only the report file. ~127 public GETs total.
"""
from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import json
import re
import statistics
import string
import sys
import time
from urllib.parse import quote

import duckdb
import lance
import requests

# ── Constants ───────────────────────────────────────────────────────────────────────────
BUCKET_US_EAST1 = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com"
ANTHEM_PREFIX = "anthem/"          # per-EIN file prefix
NAMESEARCH_PREFIX = "namesearch/"  # name→ein directory prefix
LAKE = "/Users/benjamincrane/core-x-lake/active"
TIER_B_STATE = "CA"                # Anthem Blue Cross — largest single commercial book; parameterize as needed
MIDMARKET_LO, MIDMARKET_HI = 100, 2500
COHORT_PER_TIER = 50
PROBE_WORKERS = 10
TIMEOUT = 15
# UA is immaterial on this path (empirically proven: S3 served `python-requests` UA a 206).
# An honest descriptive UA is used deliberately — no spoofing is required to reach the data.
UA = "core-x-tic-recon/1.0 (+public TiC MRF sizing diagnostic; contact ops)"

FILE_CATEGORIES = [
    "In-Network Negotiated Rates Files",
    "Out-of-Network Allowed Amounts Files",
    "Blue Cross Blue Shield Association Out-of-Area Rates Files",
    "Carelon Behavioral Health Rates Files",
]

REPORT_PATH = "docs/reference/ANTHEM_TIC_RECON_DIAGNOSTIC.md"


def digits9(ein: str | None) -> str | None:
    if not ein:
        return None
    d = re.sub(r"[^0-9]", "", ein)
    return d if len(d) == 9 else None


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = UA
    return s


# ── Phase 1: Anthem namesearch universe ──────────────────────────────────────────────────
def fetch_namesearch() -> tuple[dict[str, str], dict[str, int], int]:
    """Returns ({ein9: name}, {key: bytes}, total_bytes) over all 27 directory files."""
    keys = [f"{NAMESEARCH_PREFIX}{c}.json" for c in string.ascii_lowercase] + [
        f"{NAMESEARCH_PREFIX}others.json"
    ]
    ein_to_name: dict[str, str] = {}
    sizes: dict[str, int] = {}

    def pull(key: str):
        s = session()
        url = f"{BUCKET_US_EAST1}/{quote(key)}"
        r = s.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return key, r.status_code, len(r.content), []
        rows = (r.json() or {}).get("namesearch", [])
        recs = [(digits9(x.get("ein")), (x.get("name") or "").strip()) for x in rows]
        return key, 200, len(r.content), recs

    total_bytes = 0
    with cf.ThreadPoolExecutor(max_workers=PROBE_WORKERS) as ex:
        for key, status, nbytes, recs in ex.map(pull, keys):
            sizes[key] = nbytes
            total_bytes += nbytes
            if status == 200:
                for ein9, name in recs:
                    if ein9 and ein9 not in ein_to_name:
                        ein_to_name[ein9] = name
            else:
                print(f"  ! {key} → HTTP {status}", file=sys.stderr)
    return ein_to_name, sizes, total_bytes


# ── Phase 2: local Form 5500 EIN universe ────────────────────────────────────────────────
def load_form5500(con: duckdb.DuckDBPyConnection) -> dict:
    main = lance.dataset(f"{LAKE}/form5500_main.lance").to_table(
        columns=[
            "SPONS_DFE_EIN", "SPONSOR_DFE_NAME",
            "SPONS_DFE_MAIL_US_STATE", "SPONS_DFE_LOC_US_STATE",
            "TOT_PARTCP_BOY_CNT",
        ]
    )
    sf = lance.dataset(f"{LAKE}/form5500_sf.lance").to_table(
        columns=["SF_SPONS_EIN", "SF_SPONSOR_NAME", "SF_SPONS_US_STATE", "SF_TOT_PARTCP_BOY_CNT"]
    )
    con.register("main", main)
    con.register("sf", sf)

    main_eins = {
        d for (e,) in con.execute("SELECT DISTINCT SPONS_DFE_EIN FROM main").fetchall()
        if (d := digits9(e))
    }
    sf_eins = {
        d for (e,) in con.execute("SELECT DISTINCT SF_SPONS_EIN FROM sf").fetchall()
        if (d := digits9(e))
    }
    return {
        "main_rows": main.num_rows, "sf_rows": sf.num_rows,
        "main_eins": main_eins, "sf_eins": sf_eins, "all_eins": main_eins | sf_eins,
    }


# ── Phase 4: cohort selection ────────────────────────────────────────────────────────────
def select_cohort(con: duckdb.DuckDBPyConnection) -> tuple[list[dict], list[dict]]:
    tier_a = con.execute(
        """
        SELECT ein, nm, st, cnt FROM (
          SELECT SPONS_DFE_EIN AS ein, max(SPONSOR_DFE_NAME) AS nm,
                 max(COALESCE(SPONS_DFE_MAIL_US_STATE, SPONS_DFE_LOC_US_STATE)) AS st,
                 max(TOT_PARTCP_BOY_CNT) AS cnt
          FROM main
          WHERE SPONS_DFE_EIN IS NOT NULL
            AND length(regexp_replace(SPONS_DFE_EIN,'[^0-9]','','g'))=9
          GROUP BY 1
        ) WHERE cnt IS NOT NULL
        ORDER BY cnt DESC NULLS LAST
        LIMIT ?
        """,
        [COHORT_PER_TIER],
    ).fetchall()

    tier_b = con.execute(
        """
        SELECT ein, nm, st, cnt FROM (
          SELECT SPONS_DFE_EIN AS ein, max(SPONSOR_DFE_NAME) AS nm,
                 max(COALESCE(SPONS_DFE_MAIL_US_STATE, SPONS_DFE_LOC_US_STATE)) AS st,
                 max(TOT_PARTCP_BOY_CNT) AS cnt
          FROM main
          WHERE COALESCE(SPONS_DFE_MAIL_US_STATE, SPONS_DFE_LOC_US_STATE) = ?
            AND TOT_PARTCP_BOY_CNT BETWEEN ? AND ?
            AND SPONS_DFE_EIN IS NOT NULL
            AND length(regexp_replace(SPONS_DFE_EIN,'[^0-9]','','g'))=9
          GROUP BY 1
        ) ORDER BY cnt DESC NULLS LAST
        LIMIT ?
        """,
        [TIER_B_STATE, MIDMARKET_LO, MIDMARKET_HI, COHORT_PER_TIER],
    ).fetchall()

    def rows(rs, tier):
        return [{"ein": digits9(e), "name": n, "state": s, "participants": c, "tier": tier}
                for (e, n, s, c) in rs]

    a = rows(tier_a, "A")
    b = rows(tier_b, "B")
    # top-up Tier B from sf small plans in-state if main is short
    if len(b) < COHORT_PER_TIER:
        need = COHORT_PER_TIER - len(b)
        have = {x["ein"] for x in b}
        extra = con.execute(
            """
            SELECT ein, nm, st, cnt FROM (
              SELECT SF_SPONS_EIN AS ein, max(SF_SPONSOR_NAME) AS nm, max(SF_SPONS_US_STATE) AS st,
                     max(SF_TOT_PARTCP_BOY_CNT) AS cnt
              FROM sf
              WHERE SF_SPONS_US_STATE = ? AND SF_SPONS_EIN IS NOT NULL
                AND length(regexp_replace(SF_SPONS_EIN,'[^0-9]','','g'))=9
              GROUP BY 1
            ) ORDER BY cnt DESC NULLS LAST LIMIT ?
            """,
            [TIER_B_STATE, need * 4],
        ).fetchall()
        for e, n, s, c in extra:
            d = digits9(e)
            if d and d not in have:
                b.append({"ein": d, "name": n, "state": s, "participants": c, "tier": "B"})
                have.add(d)
                if len(b) >= COHORT_PER_TIER:
                    break
    return a, b


# ── Phase 5: live probe ──────────────────────────────────────────────────────────────────
def probe_one(ein9: str) -> dict:
    s = session()
    url = f"{BUCKET_US_EAST1}/{ANTHEM_PREFIX}{ein9}.json"
    t0 = time.perf_counter()
    try:
        r = s.get(url, timeout=TIMEOUT)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        out = {"ein": ein9, "status": r.status_code, "latency_ms": dt_ms,
               "bytes": len(r.content), "counts": {}, "total_files": 0, "lastupdated": None,
               "payload": None}
        if r.status_code == 200:
            body = r.json()
            out["lastupdated"] = body.get("lastupdated")
            tot = 0
            for cat in FILE_CATEGORIES:
                n = len(body.get(cat) or [])
                out["counts"][cat] = n
                tot += n
            out["total_files"] = tot
            out["payload"] = body
        return out
    except Exception as e:  # noqa: BLE001
        return {"ein": ein9, "status": -1, "latency_ms": (time.perf_counter() - t0) * 1000.0,
                "bytes": 0, "counts": {}, "total_files": 0, "lastupdated": None,
                "payload": None, "error": str(e)}


def probe_cohort(eins: list[str]) -> list[dict]:
    results: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=PROBE_WORKERS) as ex:
        futs = {ex.submit(probe_one, e): e for e in eins}
        for f in cf.as_completed(futs):
            results.append(f.result())
    order = {e: i for i, e in enumerate(eins)}
    results.sort(key=lambda r: order.get(r["ein"], 1e9))
    return results


# ── Phase 6: Schedule A carrier triage ───────────────────────────────────────────────────
ANTHEM_CARRIER_RX = r"(ANTHEM|ELEVANCE|BLUE CROSS|BLUECROSS|BCBS|WELLPOINT|EMPIRE|UNICARE|CARELON|ANTM)"


def carrier_triage(con: duckdb.DuckDBPyConnection, anthem_eins: set[str]) -> dict:
    """Carrier identity lives in the Schedule A HEADER (F_SCH_A: INS_CARRIER_NAME/EIN). Scan every
    local form5500_sch_a* dataset for a carrier-name column; if only the broker Part-1 detail is
    landed, say so plainly (carrier prune needs F_SCH_A) and surface the broker table for color."""
    import glob
    import os

    catalogue: list[tuple[str, list[str]]] = []
    carrier_hit = None
    for path in sorted(glob.glob(f"{LAKE}/form5500_sch_a*.lance")):
        ds = lance.dataset(path)
        cols = [f.name for f in ds.schema]
        catalogue.append((os.path.basename(path), cols))
        cc = next((c for c in cols if re.search(r"CARRIER.*NAME", c, re.I)), None)
        if cc and carrier_hit is None:
            carrier_hit = (path, ds, cc, cols)

    if carrier_hit:
        path, ds, carrier_col, cols = carrier_hit
        ein_col = next((c for c in cols if re.search(r"CARRIER.*EIN", c, re.I)), None)
        con.register("sch_a", ds.to_table(columns=[c for c in dict.fromkeys([carrier_col, ein_col]) if c]))
        top = con.execute(
            f'SELECT upper(trim("{carrier_col}")) AS carrier, count(*) n '
            f'FROM sch_a WHERE "{carrier_col}" IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 15'
        ).fetchall()
        anthem_rows = con.execute(
            f'SELECT count(*) FROM sch_a WHERE upper("{carrier_col}") ~ ?', [ANTHEM_CARRIER_RX]
        ).fetchone()[0]
        total_rows = con.execute("SELECT count(*) FROM sch_a").fetchone()[0]
        return {"mode": "carrier", "carrier_col": carrier_col, "ein_col": ein_col,
                "top": top, "anthem_rows": anthem_rows, "total_rows": total_rows}

    # No carrier header landed — report the broker detail that IS present, transparently.
    broker = None
    for name, cols in catalogue:
        bn = next((c for c in cols if re.search(r"BROKER.*NAME", c, re.I)), None)
        if bn:
            ds = lance.dataset(f"{LAKE}/{name}")
            con.register("sch_a_b", ds.to_table(columns=[bn]))
            top = con.execute(
                f'SELECT upper(trim("{bn}")) AS broker, count(*) n '
                f'FROM sch_a_b WHERE "{bn}" IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 10'
            ).fetchall()
            total = con.execute("SELECT count(*) FROM sch_a_b").fetchone()[0]
            broker = {"table": name, "broker_col": bn, "top": top, "total": total}
            break
    return {"mode": "missing", "datasets": [c[0] for c in catalogue], "broker": broker}


# ── Report ───────────────────────────────────────────────────────────────────────────────
def pct(n, d):
    return f"{(100.0 * n / d):.2f}%" if d else "n/a"


def render(uni, f5500, cohort_results, sample_hit, carrier, ns_total, started, elapsed) -> str:
    all_eins = f5500["all_eins"]
    n_all, n_main, n_sf = len(all_eins), len(f5500["main_eins"]), len(f5500["sf_eins"])
    hit_all = all_eins & uni
    hit_main = f5500["main_eins"] & uni
    hit_sf = f5500["sf_eins"] & uni

    probes = [r for r in cohort_results]
    MISS = (403, 404)  # anonymous bucket has no ListBucket → 403 for absent key (Anthem's JS treats 403≡404)
    ok = [r for r in probes if r["status"] == 200]
    miss = [r for r in probes if r["status"] in MISS]
    err = [r for r in probes if r["status"] not in (200,) + MISS]
    lat = sorted(r["latency_ms"] for r in probes if r["status"] in (200,) + MISS)
    files_per_hit = sorted(r["total_files"] for r in ok)
    bytes_hit = sorted(r["bytes"] for r in ok)

    def p(xs, q):
        if not xs:
            return 0
        i = min(len(xs) - 1, int(round(q * (len(xs) - 1))))
        return xs[i]

    a_ok = [r for r in ok if r["_tier"] == "A"]
    b_ok = [r for r in ok if r["_tier"] == "B"]
    a_tot = [r for r in probes if r["_tier"] == "A"]
    b_tot = [r for r in probes if r["_tier"] == "B"]

    # in-network file counts specifically
    innet = sorted(r["counts"].get(FILE_CATEGORIES[0], 0) for r in ok)

    L: list[str] = []
    L += ["# Anthem TiC — Reconnaissance & Scale Diagnostic", ""]
    L.append(f"**Mode:** read-only · public data only · {len(probes)+27} S3 GETs · "
             f"{elapsed:,.1f}s wall")
    L.append(f"**Run (UTC):** {started.isoformat(timespec='seconds')}")
    L.append(f"**Anthem data path:** `{BUCKET_US_EAST1}/{ANTHEM_PREFIX}{{EIN9}}.json` "
             f"(public S3 · no auth · no bot wall)")
    L.append(f"**Form 5500 plane:** `{LAKE}` (local LanceDB)")
    L.append("")

    # ── Part 1 ──
    L += ["## Part 1 — API Profile & Payload Blueprint", "",
          "### Base contract", "",
          "| Field | Value |", "|---|---|",
          "| Host (primary) | `antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com` |",
          "| Host (failover) | `antm-pt-prod-dataz-nogbd-nophi-us-east2.s3.us-east-2.amazonaws.com` |",
          "| Method | `GET` (plain HTTP; no POST, no body, no token) |",
          "| Per-EIN key | `anthem/{EIN9}.json`  ·  `EIN9` = the 9-digit EIN, dash stripped |",
          "| Name→EIN dir | `namesearch/{a-z}.json` + `namesearch/others.json` (27 files) |",
          "| Master ToC | `anthem/YYYY-MM-01_anthem_index.json.gz` |",
          "| Health probe | `status.json` → `{\"status\":\"true\"}` |",
          "| Auth | **none** — anonymous public-read S3 object |",
          "| Miss code | **403** (bucket has no public `ListBucket` → AccessDenied for absent key) "
          "or 404 = EIN not in Anthem's book; Anthem's own JS treats 403≡404≡\"0 results\" |", "",
          "**Per-EIN payload** = 4 arrays of `{url, displayname}` over a multi-host fan-out: "
          "In-Network & Out-of-Network links resolve back to the **same S3 bucket** "
          "(`anthem/*.json.gz`); BCBS-Out-of-Area links point to **`*.mrf.bcbs.com`** with "
          "**CloudFront-signed** URLs (`Expires`/`Signature`/`Key-Pair-Id` — time-limited, fetch "
          "promptly); Carelon Behavioral Health is a 4th array (often empty).", ""]
    L += ["### Anti-automation audit (empirical)", "",
          "- `www.anthem.com` shell is behind **Akamai Bot Manager** (`_abck`/`bm_sz` cookies, "
          "obfuscated sensor script) + Akamai mPulse RUM. **Irrelevant to the data path.**",
          "- The data path is **raw S3** (`Server: AmazonS3`). Live test: identical object served "
          "`200` to a browser UA and `206` (range honored) to a `python-requests/2.31.0` UA — "
          "**no UA filtering, no cookies, no JS challenge, no token.**",
          "- **Verdict: plain `httpx`/`aiohttp`/`requests` with any UA. A headless browser "
          "(Playwright) is contraindicated** — it only re-introduces the Akamai surface the data "
          "path avoids, at 50–100× the cost per lookup.", ""]
    if sample_hit:
        slim = {"lastupdated": sample_hit.get("lastupdated")}
        for cat in FILE_CATEGORIES:
            arr = sample_hit.get(cat) or []
            slim[cat] = arr[:2] + (["… +%d more" % (len(arr) - 2)] if len(arr) > 2 else [])
        L += ["### Sample response (real hit, truncated to 2 links/category)", "",
              "```json", json.dumps(slim, indent=2)[:2600], "```", ""]

    # ── Part 2 ──
    L += ["## Part 2 — Controlled Sample Test (live)", "",
          f"Cohort: **Tier A** = top {len(a_tot)} national employers by participant count · "
          f"**Tier B** = {len(b_tot)} mid-market in **{TIER_B_STATE}** "
          f"({MIDMARKET_LO}–{MIDMARKET_HI} participants).", "",
          "| Cohort | Probed | HTTP 200 (hit) | HTTP 404 (miss) | Error | Hit rate |",
          "|---|--:|--:|--:|--:|--:|",
          f"| Tier A (national) | {len(a_tot)} | {len(a_ok)} | "
          f"{len([r for r in a_tot if r['status'] in (403,404)])} | "
          f"{len([r for r in a_tot if r['status'] not in (200,403,404)])} | {pct(len(a_ok), len(a_tot))} |",
          f"| Tier B ({TIER_B_STATE} mid) | {len(b_tot)} | {len(b_ok)} | "
          f"{len([r for r in b_tot if r['status'] in (403,404)])} | "
          f"{len([r for r in b_tot if r['status'] not in (200,403,404)])} | {pct(len(b_ok), len(b_tot))} |",
          f"| **Combined** | **{len(probes)}** | **{len(ok)}** | **{len(miss)}** | "
          f"**{len(err)}** | **{pct(len(ok), len(probes))}** |", ""]

    # ── Part 3 ──
    L += ["## Part 3 — Scale, Filtration & Compute Forecast", "",
          "### 3.1 Hit-rate deficit — EXACT (whole local corpus, not sampled)", "",
          "Set-intersection of the entire local Form 5500 EIN universe against Anthem's complete "
          "`namesearch` book of business.", "",
          "| Universe | Distinct EINs | ∩ Anthem | Coverage |",
          "|---|--:|--:|--:|",
          f"| Anthem book of business (namesearch) | {len(uni):,} | — | — |",
          f"| Form 5500 — `main` | {n_main:,} | {len(hit_main):,} | {pct(len(hit_main), n_main)} |",
          f"| Form 5500 — `sf` | {n_sf:,} | {len(hit_sf):,} | {pct(len(hit_sf), n_sf)} |",
          f"| **Form 5500 — union** | **{n_all:,}** | **{len(hit_all):,}** | "
          f"**{pct(len(hit_all), n_all)}** |", "",
          f"> Live-probe hit rate ({pct(len(ok), len(probes))} on {len(probes)} EINs) vs. exact "
          f"corpus coverage ({pct(len(hit_all), n_all)}) — the cohort is participant/geo-skewed, "
          f"so the exact figure is the planning number.", ""]

    L += ["### 3.2 File-volume profile (per covered employer)", "",
          "| Metric | Value |", "|---|--:|",
          f"| In-Network files / hit — p50 | {p(innet,0.5)} |",
          f"| In-Network files / hit — p95 | {p(innet,0.95)} |",
          f"| In-Network files / hit — max | {max(innet) if innet else 0} |",
          f"| All-category files / hit — p50 | {p(files_per_hit,0.5)} |",
          f"| All-category files / hit — p95 | {p(files_per_hit,0.95)} |",
          f"| All-category files / hit — max | {max(files_per_hit) if files_per_hit else 0} |",
          f"| Pointer-JSON bytes / hit — p50 | {p(bytes_hit,0.5):,} |",
          (f"| Mean all-category files / hit | {statistics.mean(files_per_hit):.1f} |"
           if files_per_hit else "| Mean all-category files / hit | 0 |"),
          "",
          "> ⚠️ De-dup to ROOT files: Anthem emits many URL-parameter variants pointing at the "
          "same object (vendor-reported ~1k root vs 10k+ raw). Dedupe on the object path before "
          "counting/downloading. The **per-EIN file is a tiny pointer doc**; the referenced "
          "in-network rate files are the multi-GB payloads.", ""]

    L += [f"### 3.3 Compute footprint (measured per-EIN latency = lookup cost)", "",
          "| Metric | Value |", "|---|--:|",
          f"| Per-lookup latency p50 | {p(lat,0.5):.0f} ms |",
          f"| Per-lookup latency p95 | {p(lat,0.95):.0f} ms |",
          f"| Per-lookup latency max | {max(lat) if lat else 0:.0f} ms |",
          f"| Mean | {statistics.mean(lat):.0f} ms |" if lat else "| Mean | n/a |",
          "",
          "Throttling: the data path is S3. S3 does **not** CAPTCHA; it returns `503 SlowDown` "
          "only above ~3,500 GET/s **per prefix**. Sharding the `anthem/` keyspace across many "
          "prefixes is not available to us (single prefix), but a single prefix sustains "
          "thousands of GET/s — the EIN-resolution phase is **not** the bottleneck.", ""]

    # projection table
    p50 = max(p(lat, 0.5) / 1000.0, 0.001)
    cov = (len(hit_all) / n_all) if n_all else 0
    L += [f"#### Pointer-fetch cost — GET `anthem/{{ein}}.json` for HITS only "
          f"(misses pruned free via namesearch · measured p50 {p50*1000:.0f} ms/GET)", "",
          "| EIN universe | Total EINs | Hits @ exact rate | Pointer GETs | Wall @16 | @64 | @256 |",
          "|---|--:|--:|--:|--:|--:|--:|"]
    for label, n_eins in (("Local Form 5500 union", n_all),
                          ("National Form 5500 (~800k assumed)", 800_000)):
        hits_est = int(n_eins * cov)

        def wall(c, n=hits_est):
            s = n * p50 / c
            return f"{s/60:.1f} min" if s < 3600 else f"{s/3600:.1f} h"

        L.append(f"| {label} | {n_eins:,} | {hits_est:,} | {hits_est:,} | "
                 f"{wall(16)} | {wall(64)} | {wall(256)} |")
    L += ["",
          "> Serverless cost of the resolution phase is **negligible** (it is a few CPU-minutes of "
          "concurrent HTTP + JSON parse; egress is pointer-JSON only, ~hundreds of bytes/hit). "
          "The real spend is **download + parse of the referenced multi-GB in-network rate "
          "files** for covered employers — that is a separate, storage/egress-bound phase sized "
          "off the actual `url` targets, not the EIN scan.", ""]

    # ── Triage ──
    L += ["### 3.4 Triage strategy — prune BEFORE scraping", "",
          f"**The `namesearch` set IS the triage.** It is the exact, authoritative roster of "
          f"Anthem-covered EINs ({len(uni):,}). Production resolution = intersect the target EIN "
          "list against it (a set op over 7.5 MiB) → scrape only true hits, issue **zero** "
          "miss-bound requests. You do **not** process every Form 5500 EIN.", ""]
    if carrier and carrier.get("mode") == "carrier":
        L += [f"Independent cross-check via Schedule A header `{carrier['carrier_col']}`: "
              f"**{carrier['anthem_rows']:,}** of **{carrier['total_rows']:,}** rows "
              f"({pct(carrier['anthem_rows'], carrier['total_rows'])}) name an Anthem-family "
              f"carrier.", "", "| Carrier (normalized) | Rows |", "|---|--:|"]
        for c, n in carrier["top"]:
            L.append(f"| {c[:60]} | {n:,} |")
        L.append("")
    elif carrier and carrier.get("mode") == "missing":
        L += ["**Carrier-field cross-check is not yet computable locally.** The landed Schedule A "
              "table `form5500_sch_a_broker` is EFAST2 **`F_SCH_A_PART1` (broker / commission "
              "detail)** — it carries `INS_BROKER_NAME` but **not** insurer identity. The carrier "
              "name/EIN (`INS_CARRIER_NAME`, `INS_CARRIER_EIN`) lives in the Schedule A **header** "
              "`F_SCH_A`, absent from the current ingest STEMS. One-line fix: add `F_SCH_A` to land "
              "carrier identity and enable a carrier-share prune as a second signal alongside "
              "namesearch.", ""]
        b = carrier.get("broker")
        if b:
            L += [f"Top brokers in `{b['table']}` ({b['total']:,} rows) — intermediaries, not the "
                  "payer network, so broker name is a **weak** triage key (shown for transparency):",
                  "", "| Broker (normalized) | Rows |", "|---|--:|"]
            for c, n in b["top"]:
                L.append(f"| {c[:50]} | {n:,} |")
            L.append("")

    # ── Enumeration footnote ──
    L += ["### 3.5 Enumeration economics", "",
          f"Anthem's complete book of business is **{len(uni):,} EINs** across 27 `namesearch` "
          f"files totaling **{ns_total/1024/1024:.1f} MiB** — pulled here in the run. "
          "**This obviates per-EIN probing of misses entirely**: download the 27 files once, "
          "build the covered-EIN set, and intersect against any target list. You never issue a "
          "miss-bound request. The production resolution step is a set-intersection over ~7.5 MiB, "
          "not millions of HTTP calls.", ""]

    L += ["## Method", "",
          "- Anthem universe: all 27 `namesearch/*.json` files, EINs normalized to 9-digit.",
          "- Hit rate: exact set-intersection (no sampling) for the corpus figure; "
          "100-EIN live GET cohort for latency/file-count/payload telemetry.",
          "- EIN join is byte-clean: Form 5500 keys are zero-padded VARCHAR, identical to "
          "Anthem's 9-digit `ein`.",
          "- Read-only: no SoR mutation; sole write is this report.", ""]
    return "\n".join(L)


# ── Main ─────────────────────────────────────────────────────────────────────────────────
def main() -> int:
    started = dt.datetime.now(dt.timezone.utc)
    t0 = time.perf_counter()
    con = duckdb.connect(":memory:")

    print("phase 1 — Anthem namesearch universe …", file=sys.stderr)
    uni_map, ns_sizes, ns_total = fetch_namesearch()
    uni = set(uni_map)
    print(f"  Anthem covered EINs: {len(uni):,}  ({ns_total/1024/1024:.1f} MiB over 27 files)",
          file=sys.stderr)

    print("phase 2 — local Form 5500 universe …", file=sys.stderr)
    f5500 = load_form5500(con)
    print(f"  F5500 distinct EINs: union={len(f5500['all_eins']):,} "
          f"(main={len(f5500['main_eins']):,}, sf={len(f5500['sf_eins']):,})", file=sys.stderr)

    print("phase 4 — cohort selection …", file=sys.stderr)
    tier_a, tier_b = select_cohort(con)
    cohort = tier_a + tier_b
    seen, dedup = set(), []
    for x in cohort:
        if x["ein"] and x["ein"] not in seen:
            seen.add(x["ein"])
            dedup.append(x)
    tier_of = {x["ein"]: x["tier"] for x in dedup}
    print(f"  cohort: {len(dedup)} EINs (A={len(tier_a)}, B={len(tier_b)})", file=sys.stderr)

    print("phase 5 — live probe …", file=sys.stderr)
    results = probe_cohort([x["ein"] for x in dedup])
    for r in results:
        r["_tier"] = tier_of.get(r["ein"], "?")
    sample_hit = next((r["payload"] for r in results
                       if r["status"] == 200 and r["payload"]
                       and (r["payload"].get(FILE_CATEGORIES[0]))), None)
    if sample_hit is None:
        sample_hit = next((r["payload"] for r in results if r["status"] == 200 and r["payload"]), None)

    print("phase 6 — Schedule A carrier triage …", file=sys.stderr)
    try:
        carrier = carrier_triage(con, uni)
    except Exception as e:  # noqa: BLE001
        print(f"  carrier triage skipped: {e}", file=sys.stderr)
        carrier = None

    elapsed = time.perf_counter() - t0
    report = render(uni, f5500, results, sample_hit, carrier, ns_total, started, elapsed)

    import os
    os.makedirs("docs/reference", exist_ok=True)
    with open(REPORT_PATH, "w") as fh:
        fh.write(report + "\n")
    print(f"\n[report → {REPORT_PATH}]", file=sys.stderr)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
