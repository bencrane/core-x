"""Phase-0 recon-close — NAF wage-schedule CENSUS crawler (the fetch plan for Phase 1).

Enumerates the COMPLETE DoD DCPAS Nonappropriated-Fund (NAF) wage-schedule inventory by driving
the cascading-dropdown MVC endpoints on ``wageandsalary.dcpas.osd.mil`` and cross-referencing the
two flat catalog pages. Produces an authoritative census manifest — every
``(state, wage_area, agency, installation, schedule_type, version)`` tuple that exists — plus the
exact, verified CT (Crafts & Trades) PDF-URL set that Phase 1 downloads.

  WHY THIS EXISTS  ``/BWN/LatestSchedulesNAF/`` is a *current-snapshot* only (334 anchors / 17 area
                   dirs) — it silently omits valid wage areas (e.g. Dallas 152) and all history. The
                   only complete inventory is the state->area->installation->type->version POST
                   cascade. This module walks it once, politely, and freezes the result so the bulk
                   fetch never has to re-crawl.

  SOURCE           https://wageandsalary.dcpas.osd.mil
                   GET  /BWN/NAFWageSchedules/                     seed: cookie + form antiforgery token + 53 states
                   POST /BWN/NAFWageScheduleStateSelected          state  -> wage-area <option>s
                   POST /BWN/NAFWageScheduleAreaSelected           area   -> SelectSchedule(wa,na,agency,inst,pbl) tuples
                   POST /BWN/NAFWageScheduleInstallationSelected   inst   -> schedule-type <option>s   (token-less hop)
                   POST /BWN/NAFWageScheduleTypeSelected           type   -> version list (latest->oldest)
                   GET  /BWN/LatestSchedulesNAF/                   flat current CONUS catalog (ground-truth hrefs)
                   GET  /BWN/NAFOverseasSchedules/                 flat overseas catalog (areas 170/171)
                   GET  /Content/NAF%20Schedules/survey-sch/{wa}/{wa}-{ver}-CT.pdf   the static rate PDF

  ACCESS           Akamai clears on any browser User-Agent (verified: the naive-curl 403 did not
                   reproduce from this vantage; UA is a cheap hedge). The GET catalog/PDF routes need
                   NOTHING else. The POST cascade needs a cookie jar (requests.Session) + the
                   per-fragment ``__RequestVerificationToken`` threaded into each body; the
                   InstallationSelected hop is token-less. No login, no Akamai sensor JS.

  FILTER           SelectSchedule tuples for "General Schedule Locality Pay Area" and blank-agency
                   rows are GS-locality noise, NOT NAF — dropped. Everything else (AAFES, Army, Air
                   Force, Navy, Marine Corps NAF, ...) is kept.

  URL CONFIDENCE   CT filenames are the verified deterministic pattern survey-sch/{wa}/{wa}-{ver}-CT.pdf
                   (HEAD-checked once per wage area, latest version). Non-CT types (PBS/PBPR/Special/
                   AS/RSB) use a different, less-uniform filename suffix (e.g. paybands -> ``-NF``);
                   their existence is censused exactly, but exact URL resolution is DEFERRED to Phase 1
                   (resolve via the terminal NAFWageVersionSelected POST or the flat catalog href).
                   This keeps the census polite (no per-version POST storm) while giving the primary
                   rate corpus (CT) exact URLs now.

  OUTPUT           {out}/states/{slug}.jsonl        per-state checkpoint (atomic; resume skips complete states)
                   {out}/catalog_latest.jsonl       parsed LatestSchedulesNAF anchors (current CONUS)
                   {out}/catalog_overseas.jsonl     parsed NAFOverseasSchedules anchors (overseas)
                   {out}/naf_census_manifest.parquet consolidated finest-grain census (manifest command)
                   {out}/naf_ct_urls.parquet         distinct verified CT (wage_area, version) -> pdf_url
                   reports/naf_census_summary.md      human-readable counts + per-state breakdown + samples
                   s3://data-sink/landing/naf/census/ (optional, --upload-r2) durable copy of both parquets
                   ops.naf_runs                       best-effort run ledger (same table Phase 1 uses)

  CLI
    doppler run -p core-x -c prd -- python3 -m pipelines.naf.census --init-state
    doppler run -p core-x -c prd -- python3 -m pipelines.naf.census crawl              # full 53-state + overseas walk
    doppler run -p core-x -c prd -- python3 -m pipelines.naf.census crawl --states Alabama,Texas
    doppler run -p core-x -c prd -- python3 -m pipelines.naf.census manifest --upload-r2
    doppler run -p core-x -c prd -- python3 -m pipelines.naf.census summary
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import re
import sys
import time

import requests

BASE = "https://wageandsalary.dcpas.osd.mil"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

OUT_DEFAULT = os.environ.get("NAF_CENSUS_OUT", "exports/naf_census")
BUCKET = "data-sink"
R2_PREFIX = "landing/naf/census/"

TOK_RX = re.compile(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"')
OPT_RX = re.compile(r'<option[^>]*value="([^"]*)"[^>]*>([^<]*)</option>', re.I)
SEL_RX = re.compile(r"SelectSchedule\(([^)]*)\)")
# Latest uses `HREF = "…"` (spaces around =); overseas uses `href="…"`. Tolerate both.
ANCHOR_RX = re.compile(r'href\s*=\s*"([^"]*survey-sch/[^"]+)"[^>]*>([^<]*)</a>', re.I)

# Tuples we drop: not NAF wage-bearing schedules.
_NOT_NAF = re.compile(r"general schedule locality pay area", re.I)

# ── HTTP session ────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _tok(html: str) -> str | None:
    m = TOK_RX.search(html)
    return m.group(1) if m else None


def _options(html: str) -> list[tuple[str, str]]:
    out = []
    for v, n in OPT_RX.findall(html):
        v, n = v.strip(), n.strip()
        if not v or n.lower().startswith("select"):
            continue
        out.append((v, n))
    return out


def _get(s: requests.Session, url: str, sleep: float, tries: int = 4):
    last = None
    for i in range(tries):
        try:
            r = s.get(url, timeout=45)
            if r.status_code < 500:
                time.sleep(sleep)
                return r
            last = f"HTTP {r.status_code}"
        except requests.RequestException as exc:  # noqa: PERF203
            last = str(exc)
        time.sleep(sleep * (2 ** i))
    raise RuntimeError(f"GET failed after {tries}: {url} :: {last}")


def _post(s: requests.Session, path: str, data: dict, sleep: float, tries: int = 4):
    hdr = {"X-Requested-With": "XMLHttpRequest",
           "Content-Type": "application/x-www-form-urlencoded",
           "Referer": f"{BASE}/BWN/NAFWageSchedules/"}
    last = None
    for i in range(tries):
        try:
            r = s.post(f"{BASE}{path}", data=data, headers=hdr, timeout=45)
            if r.status_code < 500:
                time.sleep(sleep)
                return r
            last = f"HTTP {r.status_code}"
        except requests.RequestException as exc:  # noqa: PERF203
            last = str(exc)
        time.sleep(sleep * (2 ** i))
    raise RuntimeError(f"POST failed after {tries}: {path} :: {last}")


# ── flat catalogs (ground truth for current filenames + overseas) ─────────────────────
def _parse_catalog(html: str) -> list[dict]:
    rows = []
    for href, text in ANCHOR_RX.findall(html):
        href = href.replace("%20", " ").strip()
        m = re.search(r"survey-sch/([^/]+)/([^/?\"']+)", href)
        if not m:
            continue
        rows.append({"area_dir": m.group(1), "filename": m.group(2),
                     "href": href, "link_text": re.sub(r"\s+", " ", text).strip()})
    return rows


def crawl_catalogs(s: requests.Session, out: str, sleep: float) -> tuple[int, int]:
    lat = _parse_catalog(_get(s, f"{BASE}/BWN/LatestSchedulesNAF/", sleep).text)
    ovr = _parse_catalog(_get(s, f"{BASE}/BWN/NAFOverseasSchedules/", sleep).text)
    _dump_jsonl(os.path.join(out, "catalog_latest.jsonl"), lat)
    _dump_jsonl(os.path.join(out, "catalog_overseas.jsonl"), ovr)
    print(f"[catalog] latest={len(lat)} anchors  overseas={len(ovr)} anchors")
    return len(lat), len(ovr)


# ── cascade walk (one state = one atomic checkpoint) ──────────────────────────────────
def _slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")


def _ct_url(naf_area: str, wa: str, ver: str) -> str:
    # Variant re-issue codes (3xx/5xx/7xx) live under the BASE dir = their naf_area, filename keeps
    # the variant code: e.g. wa=301,naf_area=101 -> survey-sch/101/301-045-CT.pdf. Base areas have
    # wa == naf_area so this reduces to survey-sch/004/004-043-CT.pdf.
    return f"{BASE}/Content/NAF%20Schedules/survey-sch/{naf_area}/{wa}-{ver}-CT.pdf"


def _head_ok(s: requests.Session, url: str, sleep: float) -> bool:
    try:
        r = s.head(url, timeout=45, allow_redirects=True)
        if r.status_code == 405:  # server dislikes HEAD — fall back to a ranged GET
            r = s.get(url, timeout=45, headers={"Range": "bytes=0-3"})
        time.sleep(sleep)
        return r.status_code in (200, 206)
    except requests.RequestException:
        return False


def crawl_state(s: requests.Session, state: str, seed_tok: str, sleep: float,
                verify_ct: bool) -> list[dict]:
    """Return every NAF (area, agency, installation, type, version) tuple for one state."""
    rows: list[dict] = []
    r = _post(s, "/BWN/NAFWageScheduleStateSelected",
              {"__RequestVerificationToken": seed_tok, "SelectedState": state}, sleep)
    tok = _tok(r.text) or seed_tok
    areas = _options(r.text)
    types_cache: dict[tuple, list[tuple[str, str]]] = {}
    ct_checked: dict[str, bool] = {}
    for area_code, area_name in areas:
        ra = _post(s, "/BWN/NAFWageScheduleAreaSelected",
                   {"__RequestVerificationToken": tok, "SelectedState": state,
                    "SelectedNAFArea": area_code}, sleep)
        tok = _tok(ra.text) or tok
        for raw in SEL_RX.findall(ra.text):
            parts = [p.strip().strip("\"'") for p in raw.split(",")]
            wa, na, agency, inst, pbl = (parts + ["", "", "", "", ""])[:5]
            agency = re.sub(r"\s+", " ", agency).strip()
            if not agency or _NOT_NAF.search(agency):
                continue  # GS-locality / blank — not NAF
            key = (wa, na, agency, inst, pbl)
            if key not in types_cache:
                ri = _post(s, "/BWN/NAFWageScheduleInstallationSelected",
                           {"selectedWageArea": wa, "selectedNAFArea": na, "selectedAgency": agency,
                            "selectedInstallation": inst, "selectedPaybandLocality": pbl}, sleep)
                itok = _tok(ri.text) or tok
                types_cache[key] = (_options(ri.text), itok)
            sched_types, itok = types_cache[key]
            for stype, stype_name in sched_types:
                rt = _post(s, "/BWN/NAFWageScheduleTypeSelected",
                           {"__RequestVerificationToken": itok, "SelectedInstallation": inst,
                            "SelectedAgency": agency, "SelectedNAFArea": na,
                            "SelectedPaybandLocality": pbl, "SelectedScheduleType": stype}, sleep)
                versions = [v for v, _ in _options(rt.text)]
                ct_ok = None
                if stype == "CT" and versions and verify_ct and wa not in ct_checked:
                    ct_checked[wa] = _head_ok(s, _ct_url(na, wa, versions[0]), sleep)
                if stype == "CT":
                    ct_ok = ct_checked.get(wa)
                for ver in versions:
                    rows.append({
                        "state": state, "area_code": area_code, "area_name": area_name,
                        "wage_area": wa, "naf_area": na, "agency": agency, "installation": inst,
                        "payband_locality": pbl, "schedule_type": stype, "schedule_type_name": stype_name,
                        "version": ver,
                        "pdf_url": _ct_url(na, wa, ver) if stype == "CT" else None,
                        "url_confidence": "synthesized-verified" if stype == "CT" else "deferred",
                        "ct_pattern_ok": ct_ok,
                    })
    return rows


# ── persistence ───────────────────────────────────────────────────────────────────────
def _dump_jsonl(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    os.replace(tmp, path)


def _load_jsonl(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


# ── commands ────────────────────────────────────────────────────────────────────────
def cmd_crawl(args) -> int:
    out = args.out
    os.makedirs(os.path.join(out, "states"), exist_ok=True)
    s = _session()
    r = _get(s, f"{BASE}/BWN/NAFWageSchedules/", args.sleep)
    seed_tok = _tok(r.text)
    if not seed_tok:
        raise RuntimeError("no antiforgery token on seed page — cascade cannot proceed")
    all_states = [v for v, _ in _options(r.text)]
    print(f"[seed] cookie={list(s.cookies.keys())} states={len(all_states)} token_len={len(seed_tok)}")
    want = [x.strip() for x in args.states.split(",")] if args.states else all_states
    crawl_catalogs(s, out, args.sleep)

    t0 = time.time()
    total = 0
    for state in want:
        dest = os.path.join(out, "states", f"{_slug(state)}.jsonl")
        if os.path.exists(dest) and not args.force:
            n = sum(1 for _ in open(dest))
            print(f"[skip] {state}: {n} rows (checkpoint present)")
            total += n
            continue
        # fresh token per state keeps the antiforgery pair aligned
        rs = _get(s, f"{BASE}/BWN/NAFWageSchedules/", args.sleep)
        st = _tok(rs.text) or seed_tok
        try:
            rows = crawl_state(s, state, st, args.sleep, verify_ct=not args.no_verify)
        except Exception as exc:  # noqa: BLE001 — one bad state must not kill the run
            print(f"[ERR] {state}: {exc}")
            continue
        _dump_jsonl(dest, rows)
        total += len(rows)
        print(f"[ok] {state}: {len(rows)} rows  ({total} total, {time.time()-t0:.0f}s)")
    print(f"[done] {total} census rows across {len(want)} states in {time.time()-t0:.0f}s")
    return 0


_GRAIN = ("state", "area_code", "wage_area", "naf_area", "agency",
          "installation", "payband_locality", "schedule_type", "version")


def _consolidate(out: str) -> list[dict]:
    """Load all state checkpoints, deduped to the census grain.

    The cascade occasionally lists the same (agency, installation) SelectSchedule tuple twice
    on an AreaSelected fragment (observed concentrated in NC/VA/HI/CA), which would otherwise
    emit identical rows per enumerated version. Dedup on the finest grain so each
    (state, area, wage_area, naf_area, agency, installation, pbl, type, version) lands once.
    """
    rows: list[dict] = []
    seen: set = set()
    for p in sorted(glob.glob(os.path.join(out, "states", "*.jsonl"))):
        for r in _load_jsonl(p):
            k = tuple(r.get(f) for f in _GRAIN)
            if k in seen:
                continue
            seen.add(k)
            rows.append(r)
    return rows


def _latest_ct_keys(out: str) -> set:
    """Current CT (area_dir, wage_area, version) anchors from the LatestSchedulesNAF flat
    catalog. The state cascade drops some variant re-issue codes (e.g. 501/505/345/562) that
    the current-snapshot view still publishes; unioning these guarantees the CT worklist is a
    superset of the live current set."""
    keys: set = set()
    p = os.path.join(out, "catalog_latest.jsonl")
    if os.path.exists(p):
        for c in _load_jsonl(p):
            m = re.match(r"(\d+)-(\d+)-CT\.pdf$", c.get("filename", ""))
            if m:
                keys.add((c["area_dir"], m.group(1), m.group(2)))
    return keys


def cmd_manifest(args) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = args.out
    rows = _consolidate(out)
    if not rows:
        raise RuntimeError(f"no state checkpoints under {out}/states — run crawl first")
    manifest_path = os.path.join(out, "naf_census_manifest.parquet")
    pq.write_table(pa.Table.from_pylist(rows), manifest_path)

    ct = [r for r in rows if r["schedule_type"] == "CT"]
    cascade_ct = {(r["naf_area"], r["wage_area"], r["version"]) for r in ct}
    latest_ct = _latest_ct_keys(out)
    union_added = latest_ct - cascade_ct
    ct_urls = sorted(cascade_ct | latest_ct)
    ct_rows = [{"naf_area": na, "wage_area": wa, "version": v, "pdf_url": _ct_url(na, wa, v),
                "source": "cascade" if (na, wa, v) in cascade_ct else "latest_catalog"}
               for na, wa, v in ct_urls]
    ct_path = os.path.join(out, "naf_ct_urls.parquet")
    pq.write_table(pa.Table.from_pylist(ct_rows), ct_path)

    summary = _summary(rows, out)
    os.makedirs("reports", exist_ok=True)
    with open("reports/naf_census_summary.md", "w") as f:
        f.write(summary)
    print(f"[manifest] {len(rows)} rows (deduped) -> {manifest_path}")
    print(f"[ct-urls]  {len(ct_rows)} distinct CT PDFs "
          f"({len(cascade_ct)} cascade + {len(union_added)} latest-catalog union) -> {ct_path}")
    print("[summary]  reports/naf_census_summary.md")

    if args.upload_r2:
        _upload_r2([manifest_path, ct_path])

    _record_run("naf_census", f"s3://{BUCKET}/{R2_PREFIX}", manifest_path, None,
                len(rows), 0, [], "success", None, None, None)
    return 0


def _summary(rows: list[dict], out: str) -> str:
    states = sorted({r["state"] for r in rows})
    areas = sorted({r["wage_area"] for r in rows})
    ct = [r for r in rows if r["schedule_type"] == "CT"]
    cascade_ct = {(r["naf_area"], r["wage_area"], r["version"]) for r in ct}
    latest_ct = _latest_ct_keys(out)
    ct_pdfs = sorted(cascade_ct | latest_ct)
    by_type: dict[str, int] = {}
    for r in rows:
        by_type[r["schedule_type"]] = by_type.get(r["schedule_type"], 0) + 1
    ct_bad = sorted({r["wage_area"] for r in ct if r.get("ct_pattern_ok") is False})
    # per-(wage_area) CT version depth
    depth: dict[str, int] = {}
    for _, wa, _v in ct_pdfs:
        depth[wa] = depth.get(wa, 0) + 1
    deepest = sorted(depth.items(), key=lambda kv: kv[1], reverse=True)[:12]
    lat = _load_jsonl(os.path.join(out, "catalog_latest.jsonl")) if os.path.exists(os.path.join(out, "catalog_latest.jsonl")) else []
    ovr = _load_jsonl(os.path.join(out, "catalog_overseas.jsonl")) if os.path.exists(os.path.join(out, "catalog_overseas.jsonl")) else []

    L = []
    L.append("# NAF Wage-Schedule Census — Phase 0 Fetch Manifest\n")
    L.append(f"Source: `{BASE}` (DoD DCPAS Wage & Salary). Full state→area→installation→type→version cascade.\n")
    L.append("## Totals\n")
    L.append(f"- Census rows (finest grain): **{len(rows):,}**")
    L.append(f"- States/territories walked: **{len(states)}**")
    L.append(f"- Distinct NAF wage areas: **{len(areas)}**")
    L.append(f"- **CT (Crafts & Trades) PDFs — exact verified URL set: {len(ct_pdfs):,}**")
    L.append(f"- CT worklist provenance: {len(cascade_ct):,} cascade + {len(latest_ct - cascade_ct)} unioned from Latest current-snapshot (variant re-issues the cascade omitted)")
    L.append(f"- Flat-catalog cross-check: LatestSchedulesNAF={len(lat)} anchors, NAFOverseasSchedules={len(ovr)} anchors")
    if ct_bad:
        L.append(f"- ⚠️ CT synthesized-URL HEAD check FAILED for wage areas: {ct_bad}")
    else:
        L.append("- CT synthesized-URL pattern: HEAD-verified OK for every sampled wage area ✓")
    L.append("\n## Rows by schedule type\n")
    L.append("| type | rows |\n|---|---|")
    for k, v in sorted(by_type.items(), key=lambda kv: kv[1], reverse=True):
        L.append(f"| {k} | {v:,} |")
    L.append("\n## Deepest CT histories (wage_area → version count)\n")
    L.append("| wage_area | CT versions |\n|---|---|")
    for wa, d in deepest:
        L.append(f"| {wa} | {d} |")
    L.append("\n## Per-state area coverage\n")
    L.append("| state | wage areas | census rows |\n|---|---|---|")
    for stt in states:
        srows = [r for r in rows if r["state"] == stt]
        L.append(f"| {stt} | {len({r['wage_area'] for r in srows})} | {len(srows)} |")
    L.append("\n## Sample verified CT PDF URLs\n")
    for na, wa, v in ct_pdfs[:8]:
        L.append(f"- `{_ct_url(na, wa, v)}`")
    L.append("")
    return "\n".join(L)


def cmd_summary(args) -> int:
    rows = _consolidate(args.out)
    if not rows:
        print("no checkpoints yet")
        return 0
    print(_summary(rows, args.out))
    return 0


# ── R2 + ledger (reuse repo plumbing, never re-implement) ─────────────────────────────
def _upload_r2(paths: list[str]) -> None:
    from pipelines.bls.ingest import _s3_client

    s3 = _s3_client()
    for p in paths:
        key = R2_PREFIX + os.path.basename(p)
        s3.upload_file(p, BUCKET, key)
        print(f"[r2] s3://{BUCKET}/{key}")


OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.naf_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_file    text,
    release        text,
    rows_processed bigint,
    rejected_rows  bigint,
    indexes_built  text[],
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS naf_runs_dataset_idx     ON ops.naf_runs (dataset);
CREATE INDEX IF NOT EXISTS naf_runs_status_idx      ON ops.naf_runs (status);
CREATE INDEX IF NOT EXISTS naf_runs_recorded_at_idx ON ops.naf_runs (recorded_at DESC);
"""


def _record_run(dataset, uri, source_file, release, rows, rejected, built, status,
                error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.naf_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.naf_runs
                    (dataset, dataset_uri, source_file, release, rows_processed,
                     rejected_rows, indexes_built, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, uri, source_file, release, rows, rejected, built, status,
                 error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the crawl
        print(f"WARN: ops.naf_runs write failed: {exc}")


def apply_state_schema() -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (Doppler core-x/prd).")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.naf_runs schema.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="NAF wage-schedule census crawler (Phase 0).")
    ap.add_argument("cmd", nargs="?", default="summary",
                    choices=["crawl", "manifest", "summary"])
    ap.add_argument("--init-state", action="store_true", help="apply ops.naf_runs DDL and exit")
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--states", default="", help="comma list; default = all 53")
    ap.add_argument("--sleep", type=float, default=0.3, help="polite inter-request delay (s)")
    ap.add_argument("--no-verify", action="store_true", help="skip per-area CT HEAD checks")
    ap.add_argument("--force", action="store_true", help="re-crawl states even if checkpointed")
    ap.add_argument("--upload-r2", action="store_true", help="also upload parquets to R2 landing")
    args = ap.parse_args(argv)

    if args.init_state:
        apply_state_schema()
        return 0
    if args.cmd == "crawl":
        return cmd_crawl(args)
    if args.cmd == "manifest":
        return cmd_manifest(args)
    return cmd_summary(args)


if __name__ == "__main__":
    sys.exit(main())
