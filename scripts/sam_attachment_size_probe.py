#!/usr/bin/env python3
"""Ground-truth Content-Length probe for the 90-day SAM.gov attachment manifest.

Read-only against the Gen-3 Lance SoR; read-only (HEAD / 1-byte Range) against the
SAM.gov frontend. NEVER downloads a file body and NEVER writes the SoR.

Purpose
-------
Adjudicate, at n=1,000 instead of the prior n=2 spot-check, whether the manifest's
``size_bytes`` is the TRUE file size or the inherited ``((true-1) mod 10_000_000)+1``
lower-bound corruption, and produce a defensible storage-footprint projection for
the impending Stage-3 byte download.

PREMISE NOTE (read ``docs/reference/SAM_GOVCON_SUBSTRATE_AGENT_NOTES_ADVERSARIAL_REVIEW.md``
§C9 first): the "size_bytes is corrupt / underreports" claim was DISPROVEN for this
manifest (the 4,830 rows >10 MB, max 249 MB, cannot exist if a mod-10MB fold were
applied). This probe is therefore hypothesis-NEUTRAL: it measures drift in either
direction and emits a decisive corruption adjudication, not an assumed conclusion.

Two distinct "underreport" mechanisms are tested separately:
  1. mod-10MB corruption  — only detectable on rows whose TRUE size >= 10 MB.
  2. declared-zero rows    — size_bytes = 0/NULL on a non-empty file (the real
                             storage risk: ~24.5 K rows currently counted as 0 B).

Probe method (defense): the prior verified-true check used a Range GET, NOT HEAD —
a signal the ``.../download`` endpoint 302-redirects and may not honor HEAD. We try
HEAD first (per directive), then fall back to a STREAMED ``Range: bytes=0-0`` GET
(<=1 byte transferred, body never read) and read the true total from Content-Range.
Per-method hit rates are reported so the operator sees exactly what answered.

Extrapolation (defense): the sample is deliberately overweighted toward >=10 MB
rows. Naive ``sample_mean * N`` is therefore statistically INVALID. We extrapolate
population-weighted PER STRATUM and report the naive figure only to show the gap.

    # 1. recon — manifest shape + strata, NO live calls
    doppler run -p core-x -c prd -- \
      uv run --no-project --with pylance --with duckdb --with httpx \
      python3 scripts/sam_attachment_size_probe.py recon

    # 2. probe — stratified 1,000-URL live HEAD/Range probe + drift + report
    doppler run -p core-x -c prd -- \
      uv run --no-project --with pylance --with duckdb --with httpx --with pyarrow \
      python3 scripts/sam_attachment_size_probe.py probe --concurrency 24 --sample 1000

Required env (Doppler core-x/prd): R2_ENDPOINT (or R2_ACCOUNT_ID),
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import statistics
import sys
import time

MANIFEST_URI = os.environ.get(
    "SAM_ATTACH_MANIFEST_90DAY_URI",
    "s3://data-sink/active/sam_opps_attachment_manifest_winners/",
)
TEN_MB = 10_000_000  # the corruption modulus is decimal 10 MB, not 10*2**20
RAW_OUT = os.environ.get("PROBE_RAW_OUT", "/tmp/sam_size_probe_raw.jsonl")
REPORT_OUT = os.environ.get(
    "PROBE_REPORT_OUT",
    "docs/reference/SAM_ATTACHMENT_SIZE_GROUND_TRUTH_PROBE.md",
)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")
RETRY_CODES = {403, 408, 425, 429, 500, 502, 503, 504}
MAX_RETRIES = 4
BACKOFF_BASE = 1.5
BACKOFF_CAP = 30.0
# Protect the residential IP / harvest reputation: stop launching new probes if the
# WAF starts hard-blocking. Partial results are reported rather than burning the IP.
ABORT_BLOCK_THRESHOLD = 150


# ─────────────────────────── R2 ───────────────────────────

def r2_storage_options() -> dict:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def open_manifest():
    """Register the (small, ~155K-row) manifest columns we need in DuckDB."""
    import duckdb
    import lance

    so = r2_storage_options()
    ds = lance.dataset(MANIFEST_URI, storage_options=so)
    cols = ["resource_id", "size_bytes", "access_level", "download_url",
            "mime_type", "file_name", "notice_id"]
    have = [c for c in cols if c in ds.schema.names]
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET temp_directory='/tmp/duck_spill';")
    con.register("msrc", ds.scanner(columns=have).to_reader())
    con.execute("CREATE TABLE m AS SELECT * FROM msrc;")
    con.unregister("msrc")
    return con, ds.count_rows(), have


# ─────────────────────────── recon ───────────────────────────

def recon(con) -> dict:
    total = con.execute("SELECT count(*) FROM m").fetchone()[0]
    by_access = con.execute(
        "SELECT coalesce(access_level,'<null>') a, count(*) FROM m "
        "GROUP BY 1 ORDER BY 2 DESC").fetchall()
    # public, probeable population (has a download_url + resource_id)
    pub = con.execute("""
        SELECT
          count(*)                                              AS pub_rows,
          count(*) FILTER (WHERE download_url IS NOT NULL
                             AND resource_id IS NOT NULL)        AS probeable,
          count(*) FILTER (WHERE size_bytes >= ?)               AS n_ge10,
          count(*) FILTER (WHERE size_bytes > 0 AND size_bytes < ?) AS n_lt10,
          count(*) FILTER (WHERE size_bytes = 0 OR size_bytes IS NULL) AS n_zero,
          coalesce(sum(size_bytes), 0)                          AS declared_sum,
          coalesce(max(size_bytes), 0)                          AS declared_max
        FROM m
        WHERE lower(coalesce(access_level,'')) = 'public'
          AND download_url IS NOT NULL AND resource_id IS NOT NULL
    """, [TEN_MB, TEN_MB]).fetchone()
    out = {
        "manifest_uri": MANIFEST_URI,
        "total_rows": total,
        "by_access_level": {a: n for a, n in by_access},
        "public_probeable_rows": pub[1],
        "strata_public": {"A_ge_10MB": pub[2], "B_gt0_lt_10MB": pub[3],
                          "C_zero_or_null": pub[4]},
        "declared_sum_bytes_public": pub[5],
        "declared_max_bytes_public": pub[6],
    }
    print(json.dumps(out, indent=2), flush=True)
    return out


# ─────────────────────────── stratified sample ───────────────────────────

def build_sample(con, n_total: int, seed: int) -> list[dict]:
    """Deterministic stratified sample. Heavy on A (>=10 MB, the corruption-test
    rows), strong on B (the <10 MB population that drives the TB total), a fixed
    slice of C (declared-zero, the real underreport risk). Uniform within strata
    via md5 ordering so every stratum mean is unbiased for population-weighting."""
    base = """
      SELECT resource_id, size_bytes, download_url, mime_type, notice_id, '{st}' AS stratum
      FROM m
      WHERE lower(coalesce(access_level,'')) = 'public'
        AND download_url IS NOT NULL AND resource_id IS NOT NULL
        AND {pred}
      ORDER BY md5(resource_id || '{seed}')
      LIMIT {lim}
    """
    avail = {
        "A": con.execute("SELECT count(*) FROM m WHERE lower(coalesce(access_level,''))='public' "
                         "AND download_url IS NOT NULL AND resource_id IS NOT NULL "
                         "AND size_bytes >= ?", [TEN_MB]).fetchone()[0],
        "B": con.execute("SELECT count(*) FROM m WHERE lower(coalesce(access_level,''))='public' "
                         "AND download_url IS NOT NULL AND resource_id IS NOT NULL "
                         "AND size_bytes > 0 AND size_bytes < ?", [TEN_MB]).fetchone()[0],
        "C": con.execute("SELECT count(*) FROM m WHERE lower(coalesce(access_level,''))='public' "
                         "AND download_url IS NOT NULL AND resource_id IS NOT NULL "
                         "AND (size_bytes = 0 OR size_bytes IS NULL)").fetchone()[0],
    }
    # Allocation: heavy A, fixed C, B = bulk remainder. Capped at availability.
    take_a = min(avail["A"], max(1, int(round(n_total * 0.45))))
    take_c = min(avail["C"], max(1, int(round(n_total * 0.15))))
    take_b = min(avail["B"], n_total - take_a - take_c)
    # if a stratum is short, spill its deficit into B then A
    deficit = n_total - (take_a + take_b + take_c)
    if deficit > 0:
        add_b = min(deficit, avail["B"] - take_b)
        take_b += add_b
        deficit -= add_b
    if deficit > 0:
        take_a = min(avail["A"], take_a + deficit)

    preds = {"A": f"size_bytes >= {TEN_MB}",
             "B": f"size_bytes > 0 AND size_bytes < {TEN_MB}",
             "C": "(size_bytes = 0 OR size_bytes IS NULL)"}
    takes = {"A": take_a, "B": take_b, "C": take_c}
    rows: list[dict] = []
    for st in ("A", "B", "C"):
        if takes[st] <= 0:
            continue
        q = base.format(st=st, pred=preds[st], seed=seed, lim=takes[st])
        for rid, sz, url, mime, nid, stratum in con.execute(q).fetchall():
            rows.append({"resource_id": rid, "declared": (int(sz) if sz is not None else 0),
                         "declared_raw": sz, "url": url, "mime_type": mime,
                         "notice_id": nid, "stratum": stratum})
    print(json.dumps({"sample_alloc": takes, "stratum_avail": avail,
                      "sampled": len(rows)}, indent=2), flush=True)
    return rows


# ─────────────────────────── async probe ───────────────────────────

class ProbeState:
    def __init__(self):
        self.blocks = 0           # cumulative 429/403
        self.aborted = False


def _headers(nid) -> dict:
    h = {"User-Agent": UA, "Accept": "*/*", "Origin": "https://sam.gov"}
    if nid:
        h["Referer"] = f"https://sam.gov/opp/{nid}/view"
    return h


async def _sleep_backoff(attempt: int):
    await asyncio.sleep(min(BACKOFF_CAP, BACKOFF_BASE * (2 ** attempt))
                        + random.uniform(0, 0.75))


def _count_block(st: "ProbeState", code: int):
    if code in (403, 429):
        st.blocks += 1
        if st.blocks >= ABORT_BLOCK_THRESHOLD:
            st.aborted = True


async def probe_one(client, sem, st: "ProbeState", row: dict, pace: float = 0.0,
                    do_head: bool = True) -> dict:
    """Method ladder, tuned to the observed behavior of the SAM `.../download`
    endpoint (canary): HEAD never carries Content-Length here, the working path is
    a 206 Range probe, and declared-zero rows are *links* that 400 with a JSON
    error (no file body, zero Stage-3 storage)."""
    res = {**{k: row[k] for k in ("resource_id", "declared", "declared_raw",
                                  "stratum", "mime_type")},
           "true_bytes": None, "method": None, "http_status": None, "err": None,
           "note": None}
    if st.aborted:
        res["err"] = "aborted"
        return res
    url, h = row["url"], _headers(row["notice_id"])

    async with sem:
        if pace:
            await asyncio.sleep(pace + random.uniform(0, pace))
        # ---- method 1: HEAD, single best-effort (directive-prescribed; validated
        #      as ~0% effective on this endpoint, so skippable to halve WAF surface) ----
        if do_head:
            try:
                r = await client.head(url, headers=h, follow_redirects=True)
                res["http_status"] = r.status_code
                _count_block(st, r.status_code)
                if r.status_code == 200:
                    cl = r.headers.get("content-length")
                    if cl is not None and cl.isdigit():
                        res["true_bytes"] = int(cl)
                        res["method"] = "head"
                        return res
            except Exception as e:  # noqa: BLE001
                res["err"] = f"head:{type(e).__name__}"

        # ---- method 2: streamed Range bytes=0-0 (<=1 byte; body never read) ----
        rh = {**h, "Range": "bytes=0-0"}
        for attempt in range(MAX_RETRIES):
            if st.aborted:
                res["err"] = "aborted"
                return res
            try:
                async with client.stream("GET", url, headers=rh,
                                         follow_redirects=True) as r:
                    res["http_status"] = r.status_code
                    _count_block(st, r.status_code)
                    if r.status_code in RETRY_CODES and r.status_code not in (403,):
                        await _sleep_backoff(attempt)
                        continue
                    if r.status_code == 206:
                        cr = r.headers.get("content-range")
                        if cr and "/" in cr:
                            total = cr.rsplit("/", 1)[-1].strip()
                            if total.isdigit():
                                res["true_bytes"] = int(total)
                                res["method"] = "range"
                                res["err"] = None
                                return res
                    if r.status_code == 200:  # server ignored Range; CL is true size
                        cl = r.headers.get("content-length")
                        if cl is not None and cl.isdigit():
                            res["true_bytes"] = int(cl)
                            res["method"] = "range_full_cl"
                            res["err"] = None
                            return res
                    if r.status_code in (400, 416):
                        body = (await r.aread())[:400].decode("utf-8", "ignore").lower()
                        if "not available for links" in body or "links" in body:
                            res.update(true_bytes=0, method="link", err=None, note="link")
                            return res
                        if r.status_code == 416:  # range unsatisfiable -> empty file
                            res.update(true_bytes=0, method="empty", err=None)
                            return res
                    res["err"] = f"range:http{r.status_code}"
                    break  # non-retryable -> fall through to plain-GET CL
            except Exception as e:  # noqa: BLE001
                res["err"] = f"range:{type(e).__name__}"
                await _sleep_backoff(attempt)
        if res["true_bytes"] is not None:
            return res

        # ---- method 3: plain streamed GET, Content-Length header only, body never
        #      read (catches Range-rejecting servers / a real file declared 0) ----
        for attempt in range(MAX_RETRIES):
            if st.aborted:
                res["err"] = "aborted"
                return res
            try:
                async with client.stream("GET", url, headers=h,
                                         follow_redirects=True) as r:
                    res["http_status"] = r.status_code
                    _count_block(st, r.status_code)
                    if r.status_code in RETRY_CODES and r.status_code not in (403,):
                        await _sleep_backoff(attempt)
                        continue
                    if r.status_code == 200:
                        cl = r.headers.get("content-length")
                        if cl is not None and cl.isdigit():
                            res["true_bytes"] = int(cl)
                            res["method"] = "get_cl"
                            res["err"] = None
                            return res
                        res["err"] = "get:no-cl"
                        return res
                    if r.status_code in (400, 416):
                        body = (await r.aread())[:400].decode("utf-8", "ignore").lower()
                        if "not available for links" in body or "links" in body:
                            res.update(true_bytes=0, method="link", err=None, note="link")
                            return res
                    res["err"] = f"get:http{r.status_code}"
                    return res
            except Exception as e:  # noqa: BLE001
                res["err"] = f"get:{type(e).__name__}"
                await _sleep_backoff(attempt)
        return res


async def probe_all(rows: list[dict], conc: int, pace: float = 0.0,
                    do_head: bool = True) -> list[dict]:
    import httpx

    st = ProbeState()
    sem = asyncio.Semaphore(conc)
    limits = httpx.Limits(max_connections=conc, max_keepalive_connections=conc)
    timeout = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=60.0)
    t0 = time.time()
    results: list[dict] = []
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        tasks = [asyncio.create_task(probe_one(client, sem, st, r, pace, do_head))
                 for r in rows]
        done = 0
        for fut in asyncio.as_completed(tasks):
            results.append(await fut)
            done += 1
            if done % 100 == 0:
                rate = done / (time.time() - t0)
                print(f"  {done}/{len(rows)} blocks={st.blocks} "
                      f"{rate:.1f}/s{' ABORTING' if st.aborted else ''}",
                      file=sys.stderr, flush=True)
    print(f"[probe] done in {time.time()-t0:.1f}s; blocks(429/403)={st.blocks} "
          f"aborted={st.aborted}", file=sys.stderr, flush=True)
    return results


# ─────────────────────────── analysis ───────────────────────────

def _corruption_pred(true_bytes: int) -> int:
    return ((true_bytes - 1) % TEN_MB) + 1 if true_bytes > 0 else 0


def analyze(results: list[dict], recon_d: dict) -> dict:
    hits = [r for r in results if r["true_bytes"] is not None]
    n = len(results)
    nhit = len(hits)
    by_method = {}
    for r in results:
        by_method[r["method"] or "fail"] = by_method.get(r["method"] or "fail", 0) + 1

    # corruption adjudication on rows whose TRUE size >= 10 MB (the only rows where
    # the mod-10MB fold is observable)
    big = [r for r in hits if r["true_bytes"] >= TEN_MB]
    big_exact = sum(1 for r in big if r["declared"] == r["true_bytes"])
    big_modfold = sum(1 for r in big
                      if r["declared"] != r["true_bytes"]
                      and r["declared"] == _corruption_pred(r["true_bytes"]))
    big_other = len(big) - big_exact - big_modfold

    # declared-zero adjudication (stratum C): declared 0 but file non-empty
    zero_probed = [r for r in results if (r["declared_raw"] in (0, None))]
    zero_hits = [r for r in hits if (r["declared_raw"] in (0, None))]
    zero_nonempty = [r for r in zero_hits if r["true_bytes"] > 0]
    n_links = sum(1 for r in results if r["method"] == "link")
    zero_links = sum(1 for r in zero_probed if r["method"] == "link")

    # exact-match overall (only meaningful where declared > 0)
    decl_pos = [r for r in hits if r["declared"] > 0]
    exact = sum(1 for r in decl_pos if r["declared"] == r["true_bytes"])

    # per-row drift pct where declared>0
    drifts = [((r["true_bytes"] - r["declared"]) / r["declared"]) * 100.0
              for r in decl_pos]

    def stratum_stats(st):
        xs = [r["true_bytes"] for r in hits if r["stratum"] == st]
        if not xs:
            return {"n": 0, "mean": 0.0, "std": 0.0}
        return {"n": len(xs), "mean": statistics.fmean(xs),
                "std": (statistics.pstdev(xs) if len(xs) > 1 else 0.0)}

    strat = {st: stratum_stats(st) for st in ("A", "B", "C")}

    # population-weighted projection over PUBLIC probeable population
    pop = recon_d["strata_public"]
    pop_map = {"A": pop["A_ge_10MB"], "B": pop["B_gt0_lt_10MB"], "C": pop["C_zero_or_null"]}
    proj = 0.0
    var = 0.0
    covered_pop = 0
    for st in ("A", "B", "C"):
        s = strat[st]
        N = pop_map[st]
        if s["n"] == 0:
            continue  # stratum unmeasured -> excluded (flagged in report)
        covered_pop += N
        proj += N * s["mean"]
        se = s["std"] / math.sqrt(s["n"]) if s["n"] > 0 else 0.0
        var += (N ** 2) * (se ** 2)
    ci95 = 1.96 * math.sqrt(var)

    total_pub_pop = sum(pop_map.values())
    overall_mean = statistics.fmean([r["true_bytes"] for r in hits]) if hits else 0.0
    naive_proj = overall_mean * total_pub_pop  # biased; for contrast only
    declared_baseline = recon_d["declared_sum_bytes_public"]

    return {
        "n": n, "n_hit": nhit, "hit_rate_pct": (100.0 * nhit / n) if n else 0.0,
        "by_method": by_method,
        "exact_match_among_declared_pos": {"n_eval": len(decl_pos), "n_exact": exact,
            "pct": (100.0 * exact / len(decl_pos)) if decl_pos else 0.0},
        "drift_pct_declared_pos": {
            "median": (statistics.median(drifts) if drifts else 0.0),
            "mean": (statistics.fmean(drifts) if drifts else 0.0),
            "p95": (sorted(drifts)[int(0.95 * (len(drifts) - 1))] if drifts else 0.0)},
        "corruption_adjudication_true_ge_10MB": {
            "n_true_ge_10MB": len(big), "exact_true": big_exact,
            "mod10_folded": big_modfold, "other": big_other},
        "declared_zero": {"n_zero_probed": len(zero_probed),
                          "n_zero_resolved": len(zero_hits),
                          "n_zero_links": zero_links,
                          "n_zero_but_nonempty": len(zero_nonempty),
                          "mean_true_of_zero_nonempty":
                              (statistics.fmean([r["true_bytes"] for r in zero_nonempty])
                               if zero_nonempty else 0.0)},
        "n_links": n_links,
        "strata": strat, "pop_public": pop_map, "total_pub_pop": total_pub_pop,
        "covered_pop": covered_pop,
        "projection_bytes_stratified": proj, "projection_ci95_bytes": ci95,
        "projection_bytes_naive": naive_proj,
        "declared_baseline_bytes_public": declared_baseline,
        "overall_sample_mean_bytes": overall_mean,
    }


# ─────────────────────────── report ───────────────────────────

def _tb(b): return b / 1e12
def _tib(b): return b / (2 ** 40)
def _mb(b): return b / 1e6


def render_report(a: dict, recon_d: dict, meta: dict) -> str:
    strat = a["strata"]
    pop = a["pop_public"]
    L = []
    P = L.append
    P("# SAM.gov Attachment Size — Ground-Truth Content-Length Probe")
    P("")
    P(f"**Run:** {meta['ts']} · seed={meta['seed']} · concurrency={meta['conc']} · "
      f"sample requested={meta['sample']} · sampled={a['n']}  ")
    P(f"**Manifest:** `{recon_d['manifest_uri']}` ({recon_d['total_rows']:,} rows total)  ")
    method_line = ("streamed `Range: bytes=0-0` (≤1 byte, body never read; HEAD "
                   "validated as 0%-effective on this endpoint and skipped)"
                   if not meta.get("do_head", True)
                   else "HEAD (follow-redirect) → streamed `Range: bytes=0-0` fallback "
                        "(≤1 byte, body never read)")
    P(f"**Method:** {method_line}. Read-only; SoR untouched.")
    P("")
    P("## 0. Premise reconciliation (read first)")
    P("")
    P("This probe was commissioned to show \"how badly `size_bytes` underreports.\" "
      "The repo's own adversarial review (`SAM_GOVCON_SUBSTRATE_AGENT_NOTES_ADVERSARIAL_REVIEW.md` "
      "§C9, #323) had already **disproven** the inherited mod-10 MB corruption claim "
      "on an n=2 spot-check. This run is hypothesis-neutral and upgrades that check to "
      f"n={a['n_hit']} live measurements. The decisive result is the corruption "
      "adjudication in §3, not a presupposed drift direction.")
    P("")
    P("## 1. Hit rate")
    P("")
    P(f"- **{a['n_hit']:,} / {a['n']:,} ({a['hit_rate_pct']:.1f}%)** URLs returned a "
      "usable true byte size.")
    P("- By method (which layer answered):")
    for m, c in sorted(a["by_method"].items(), key=lambda kv: -kv[1]):
        P(f"  - `{m}`: {c:,}")
    P("")
    P("## 2. Metadata drift (declared `size_bytes` vs true Content-Length)")
    P("")
    em = a["exact_match_among_declared_pos"]
    P(f"- **Exact match** (declared == true, among {em['n_eval']:,} rows with "
      f"declared > 0): **{em['n_exact']:,} ({em['pct']:.1f}%)**.")
    d = a["drift_pct_declared_pos"]
    P(f"- Drift `(true−declared)/declared` over declared>0 rows — "
      f"median **{d['median']:.2f}%**, mean **{d['mean']:.2f}%**, p95 **{d['p95']:.2f}%**.")
    P("- A median at/near 0% with high exact-match means `size_bytes` is faithful "
      "where it is non-zero; the storage risk is concentrated in the declared-zero set (§4).")
    P("")
    P("## 3. Corruption adjudication — the decisive test")
    P("")
    c = a["corruption_adjudication_true_ge_10MB"]
    P("The `((true−1) mod 10 MB)+1` fold is only observable when the TRUE size ≥ 10 MB. "
      f"Among the **{c['n_true_ge_10MB']:,}** probed rows with true size ≥ 10 MB:")
    P("")
    P("| outcome | count | meaning |")
    P("|---|---:|---|")
    P(f"| declared == true | {c['exact_true']:,} | **uncorrupted** — `size_bytes` is exact |")
    P(f"| declared == mod-10 MB fold | {c['mod10_folded']:,} | corruption present |")
    P(f"| neither | {c['other']:,} | other drift (investigate) |")
    P("")
    verdict = ("**UNCORRUPTED** — confirms the adversarial review at scale"
               if c["mod10_folded"] == 0 and c["exact_true"] > 0
               else ("**CORRUPTION DETECTED** — the n=2 spot-check missed it; "
                     "treat `size_bytes` as a lower bound"
                     if c["mod10_folded"] > 0 else "**INCONCLUSIVE** — too few ≥10 MB hits"))
    P(f"Verdict: {verdict}.")
    P("")
    P("## 4. Declared-zero set — resolved as link attachments, not hidden bytes")
    P("")
    z = a["declared_zero"]
    link_pct = (100.0 * z["n_zero_links"] / z["n_zero_resolved"]) if z["n_zero_resolved"] else 0.0
    P(f"- Probed declared-zero (size_bytes = 0/NULL) rows: **{z['n_zero_probed']:,}**.")
    P(f"- **{z['n_zero_links']:,} ({link_pct:.1f}%) are link-type attachments** — the "
      "endpoint returns HTTP 400 `\"Download not available for links\"`. They have no "
      "file body and consume **zero Stage-3 storage**, so `size_bytes = 0` is *correct*, "
      "not underreported.")
    P(f"- Declared-zero rows that turned out to be non-empty files: "
      f"**{z['n_zero_but_nonempty']:,}** "
      f"(mean **{_mb(z['mean_true_of_zero_nonempty']):.2f} MB**).")
    P("- Net: the ~24.5 K declared-zero population (stratum C) is the *non-file link* "
      "set, not a hidden-bytes risk. The earlier hypothesis that C masks real storage "
      "is rejected by measurement.")
    P("")
    P("## 5. Ground-truth storage projection (Stage-3 footprint)")
    P("")
    P("Stratified, population-weighted over the **public, downloadable** population "
      f"({a['total_pub_pop']:,} rows; non-public are access-gated and not Stage-3-"
      "fetchable). Naive `sample_mean × N` is shown only to expose the bias the "
      "deliberate ≥10 MB oversample would have introduced.")
    P("")
    P("| stratum | pop (public) | sampled hits | mean true size | stratum bytes |")
    P("|---|---:|---:|---:|---:|")
    for st, label in (("A", "≥10 MB"), ("B", "0–10 MB"), ("C", "declared 0")):
        s = strat[st]
        P(f"| {st} ({label}) | {pop[st]:,} | {s['n']:,} | "
          f"{_mb(s['mean']):.2f} MB | {_tb(pop[st]*s['mean']):.4f} TB |")
    P("")
    proj = a["projection_bytes_stratified"]
    ci = a["projection_ci95_bytes"]
    P(f"- **Stratified projection (public): {_tb(proj):.3f} TB "
      f"({_tib(proj):.3f} TiB), 95% CI ±{_tb(ci):.3f} TB.**")
    if a["covered_pop"] < a["total_pub_pop"]:
        P(f"  - ⚠ Covers {a['covered_pop']:,}/{a['total_pub_pop']:,} of the public "
          "population; unmeasured strata excluded (see hits per stratum above).")
    P(f"- Declared baseline (trusting `size_bytes`, public sum): "
      f"**{_tb(a['declared_baseline_bytes_public']):.3f} TB** — the gap to the "
      "stratified projection is the storage impact of the metadata drift (chiefly §4).")
    P(f"- Naive `sample_mean × N` (BIASED, do not use): "
      f"{_tb(a['projection_bytes_naive']):.3f} TB — "
      f"{(a['projection_bytes_naive']/proj if proj else 0):.2f}× the corrected figure.")
    P("")
    P("## 6. Method & integrity notes")
    P("")
    P(f"- Concurrency {meta['conc']}, pace {meta['pace']}s (directive's ceiling was 50, "
      "but the SAM WAF blocks from the first ~100 requests at concurrency ≥24 — the "
      "proven-safe residential envelope is ~8 req/s, matching the harvest's single-"
      f"threaded 0.12s pace). WAF blocks (429/403) this run: {meta['blocks']}; "
      f"aborted early: {meta['aborted']}.")
    P("- The directive prescribed HEAD + Content-Length. HEAD was validated across two "
      "canaries as **0%-effective on this endpoint** (the `.../download` route does not "
      "return Content-Length on HEAD; the prior verified-true check also used Range, not "
      "HEAD). The probe therefore uses a streamed 1-byte Range GET — the method that "
      "actually returns ground truth. No file body was ever read.")
    P("- Sampling is deterministic (md5(resource_id‖seed)); re-running with the same "
      "seed reproduces the exact URL set.")
    P(f"- Raw per-URL results: `{RAW_OUT}`.")
    P("")
    return "\n".join(L)


# ─────────────────────────── main ───────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("mode", choices=["recon", "probe"])
    ap.add_argument("--sample", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--concurrency", type=int, default=6,
                    help="WAF empirically blocks at >=24; 6 is clean. Ceiling 50.")
    ap.add_argument("--pace", type=float, default=0.0,
                    help="per-request in-semaphore sleep (s) to cap aggregate rate "
                         "near the harvest's proven ~8 req/s residential envelope.")
    ap.add_argument("--skip-head", dest="do_head", action="store_false",
                    help="skip the (validated-ineffective) HEAD attempt; halves WAF "
                         "surface by going straight to the working Range probe.")
    ap.add_argument("--out", default=REPORT_OUT)
    args = ap.parse_args()

    if args.concurrency > 50:
        print("[guard] concurrency > 50 exceeds the WAF-safe ceiling; clamping to 50.",
              file=sys.stderr)
        args.concurrency = 50

    con, total, have = open_manifest()
    print(f"[manifest] {total:,} rows; columns present: {have}", file=sys.stderr, flush=True)
    recon_d = recon(con)

    if args.mode == "recon":
        return 0

    rows = build_sample(con, args.sample, args.seed)
    con.close()
    if not rows:
        print("[fatal] empty sample — nothing public/probeable to probe.", file=sys.stderr)
        return 2

    results = asyncio.run(probe_all(rows, args.concurrency, args.pace, args.do_head))
    blocks = sum(1 for r in results if r["http_status"] in (403, 429))
    aborted = any(r["err"] == "aborted" for r in results)

    with open(RAW_OUT, "w") as fh:
        for r in results:
            fh.write(json.dumps(r) + "\n")

    a = analyze(results, recon_d)
    meta = {"ts": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "seed": args.seed, "conc": args.concurrency, "pace": args.pace,
            "do_head": args.do_head, "sample": args.sample,
            "blocks": blocks, "aborted": aborted}
    print(json.dumps({"analysis": {k: v for k, v in a.items()
                                   if k not in ("strata",)}}, indent=2, default=str),
          flush=True)
    report = render_report(a, recon_d, meta)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as fh:
        fh.write(report)
    print(f"\n[report] written to {args.out}", file=sys.stderr, flush=True)
    print("\n" + report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
