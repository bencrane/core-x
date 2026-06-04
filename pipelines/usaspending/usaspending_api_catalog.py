"""Builder — USAspending API CATALOG (canonical endpoint surface → Lance).

Clones the API CONTRACTS from the upstream repo
(github.com/fedspendingtransparency/usaspending-api, usaspending_api/api_contracts/),
parses each per-endpoint API-Blueprint markdown contract, and lands ONE row per
endpoint to:

    s3://data-sink/active/usaspending_api_catalog/   (Lance, BTREE on endpoint_path)

This is the authoritative source — the contracts the API is built from — not a scrape
of the docs site. Each row carries the endpoint path (what to hit), HTTP method(s),
the request + response examples we could extract, the FULL raw contract markdown
(fidelity), and a GitHub permalink to the contract for the full spec.

Self-contained; runs locally with R2 creds in the env (no Modal — it's a tiny
reference build):

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'requests>=2.32' \
      python3 pipelines/usaspending/usaspending_api_catalog.py
"""

from __future__ import annotations

import datetime as dt
import io
import os
import json
import re
import sys
import tarfile
import time
import urllib.parse

REPO = "fedspendingtransparency/usaspending-api"
TARBALL = f"https://github.com/{REPO}/archive/refs/heads/master.tar.gz"
CONTRACTS_MARKER = "usaspending_api/api_contracts/contracts/"
CATALOG_URI = os.environ.get(
    "USASPENDING_API_CATALOG_URI", "s3://data-sink/active/usaspending_api_catalog/")
DATA_STORAGE_VERSION = "2.1"

# Live response-sampling (--live-sample): fills endpoints whose contract defines the
# response via MSON attribute references, so no literal JSON example exists upstream to
# extract. We call the real endpoint and capture the actual JSON.
API_HOST = "https://api.usaspending.gov"
SAMPLE_VALUES = {
    "fiscal_year": "2025", "fy": "2025", "fips": "06", "code": "541512",
    "naics": "541512", "duns_or_uei": "TP7EK8DZV6N5", "group": "all",
    "limit": "10", "page": "1",
}
SAMPLE_DELAY_SECONDS = 1.2   # polite spacing for USAspending's per-IP throttle

# API-Blueprint extraction patterns.
_TEMPLATE_RE = re.compile(r"\[(/api/v\d+/[^\]\s]*)\]")          # full URI template inside [ ]
_PATH_FALLBACK_RE = re.compile(r"(/api/v\d+/[A-Za-z0-9_/{}.-]+)")
_METHOD_HEAD_RE = re.compile(r"(?m)^\s{0,3}#{1,6}\s*(GET|POST|PUT|DELETE|PATCH)\b")  # '## POST'
_METHOD_BRKT_RE = re.compile(r"\[(GET|POST|PUT|DELETE|PATCH)\b")                     # '[POST /api/…]'
_VER_RE = re.compile(r"/contracts/(v\d+)/")


def _r2_storage_options() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _fetch_contracts() -> tuple[list[tuple[str, str]], str]:
    """Download the repo tarball; return [(repo_rel_path, text)] for every contract
    .md under api_contracts/contracts/, plus the resolved top-level dir (provenance)."""
    import requests

    print(f"downloading {TARBALL} …", flush=True)
    r = requests.get(TARBALL, timeout=(30, 600))
    r.raise_for_status()
    out: list[tuple[str, str]] = []
    top = ""
    with tarfile.open(fileobj=io.BytesIO(r.content), mode="r:gz") as tf:
        for m in tf.getmembers():
            if not top:
                top = m.name.split("/", 1)[0]
            if not (m.isfile() and m.name.endswith(".md") and CONTRACTS_MARKER in m.name):
                continue
            rel = m.name.split("/", 1)[1]            # strip the "<repo>-master/" prefix
            text = tf.extractfile(m).read().decode("utf-8", errors="replace")
            out.append((rel, text))
    print(f"extracted {len(out)} contract .md files (source dir {top})", flush=True)
    return out, top


def _indented_blocks(md: str, marker_prefix: str, want_json: bool) -> list[tuple[int, str]]:
    """(char_pos, text) for each indented block under a line starting with marker_prefix
    (e.g. '+ Body', '+ Parameters', '+ Schema'). API Blueprint uses 4+-space-indented
    blocks, not ``` fences. want_json keeps only blocks that begin with { or [."""
    lines = md.splitlines(keepends=True)
    offs, c = [], 0
    for ln in lines:
        offs.append(c)
        c += len(ln)
    out, i = [], 0
    while i < len(lines):
        if lines[i].strip().startswith(marker_prefix):
            base = len(lines[i]) - len(lines[i].lstrip())
            j, block = i + 1, []
            while j < len(lines):
                ln = lines[j]
                if ln.strip() == "":
                    block.append("")
                    j += 1
                    continue
                if (len(ln) - len(ln.lstrip())) > base:
                    block.append(ln)
                    j += 1
                else:
                    break
            text = "".join(block).strip()
            if text and (not want_json or text[:1] in ("{", "[")):
                out.append((offs[i], text[:20000]))
            i = j
        else:
            i += 1
    return out


def _parse(rel_path: str, md: str) -> dict:
    """Best-effort parse of one API-Blueprint contract → catalog row. Full fidelity is
    preserved in contract_md; the extracted fields are query conveniences."""
    tmpl_m = _TEMPLATE_RE.search(md)
    if tmpl_m:
        uri_template = tmpl_m.group(1)
    else:
        fb = _PATH_FALLBACK_RE.search(md)
        uri_template = fb.group(1) if fb else None
    # callable base path = template minus the {?query}/{&query} suffix and any stray '{'
    endpoint_path = None
    if uri_template:
        endpoint_path = re.split(r"\{[?&]", uri_template)[0].rstrip()
        if endpoint_path.endswith("{"):
            endpoint_path = endpoint_path[:-1]
    methods = sorted(set(_METHOD_HEAD_RE.findall(md)) | set(_METHOD_BRKT_RE.findall(md)))
    ver_m = _VER_RE.search(rel_path)
    after = rel_path.split("/contracts/", 1)[1] if "/contracts/" in rel_path else rel_path
    parts = after.split("/")
    group = parts[1] if len(parts) > 2 else (parts[0] if parts else None)

    # title = first markdown heading that carries a [/path]
    title = None
    for line in md.splitlines():
        s = line.strip()
        if s.startswith("#") and "[" in s:
            title = re.sub(r"\s*\[.*$", "", s.lstrip("# ")).strip() or None
            break

    resp_pos = md.find("+ Response")
    if resp_pos < 0:
        resp_pos = len(md)
    bodies = _indented_blocks(md, "+ Body", want_json=True)
    params = _indented_blocks(md, "+ Parameters", want_json=False)
    schemas = _indented_blocks(md, "+ Schema", want_json=True)
    # Request side = before the first + Response; response side = at/after it.
    req_example = next((t for p, t in bodies if p < resp_pos), None)
    resp_example = next((t for p, t in bodies if p >= resp_pos), None)
    # GETs carry no body — their "payload" is query/path params; this fills that gap.
    request_parameters = next((t for p, t in params if p < resp_pos), None)
    # When a response has no literal JSON body, its JSON Schema (if any) is the shape.
    response_schema = next((t for p, t in schemas if p >= resp_pos), None)

    return {
        "endpoint_path": endpoint_path,
        "uri_template": uri_template,
        "methods": ",".join(methods) or None,
        "api_version": ver_m.group(1) if ver_m else None,
        "group_name": group,
        "title": title,
        "request_example": req_example,
        "request_parameters": request_parameters,
        "response_example": resp_example,
        "response_schema": response_schema,
        "contract_md": md,
        "contract_path": rel_path,
        "github_url": f"https://github.com/{REPO}/blob/master/{rel_path}",
    }


def _bootstrap_ids() -> dict:
    """Best-effort: fetch a real agency id + federal_account_id to fill the path/query
    params some GET endpoints require. Failures are non-fatal — those endpoints just stay
    unsampled (their full spec remains in contract_md)."""
    import requests

    out: dict = {}
    try:
        r = requests.get(API_HOST + "/api/v2/references/toptier_agencies/", timeout=(15, 60))
        if r.ok:
            res = r.json().get("results") or []
            if res:
                aid = res[0].get("agency_id") or res[0].get("toptier_agency_id")
                if aid:
                    out["awarding_agency_id"] = out["funding_agency_id"] = out["agency"] = str(aid)
    except Exception as exc:  # noqa: BLE001
        print(f"  bootstrap agencies failed: {exc}", flush=True)
    try:
        r = requests.post(API_HOST + "/api/v2/federal_accounts/",
                          json={"limit": 1, "page": 1, "filters": {"fy": "2025"}}, timeout=(15, 60))
        if r.ok:
            res = r.json().get("results") or []
            if res:
                faid = res[0].get("account_id") or res[0].get("id") or res[0].get("federal_account_id")
                if faid:
                    out["federal_account_id"] = str(faid)
    except Exception as exc:  # noqa: BLE001
        print(f"  bootstrap federal_accounts failed: {exc}", flush=True)
    return out


def _sample_one(row: dict, samples: dict):
    """One best-effort live call for a gap endpoint. Returns (status:int|None, text|None).
    POST → replay the documented request body. GET → substitute path params + fill query
    params from the sample values. status=-1 marks a transport error; None marks skipped."""
    import requests

    path = row.get("endpoint_path")
    methods = row.get("methods") or ""
    if not path:
        return None, None
    try:
        if "POST" in methods:
            if not row.get("request_example"):
                return None, None
            body = json.loads(row["request_example"])
            r = requests.post(API_HOST + path, json=body, timeout=(15, 90))
            return r.status_code, r.text
        if "GET" in methods:
            url_path = path
            for p in re.findall(r"\{([^}?&]+)\}", url_path):       # required path params
                if samples.get(p):
                    url_path = url_path.replace("{%s}" % p, urllib.parse.quote(str(samples[p]), safe=""))
                else:
                    return None, None                               # no sample → skip
            q = {}
            mq = re.search(r"\{[?&]([^}]*)\}", row.get("uri_template") or "")
            if mq:
                for name in (n.strip() for n in mq.group(1).split(",")):
                    if samples.get(name):
                        q[name] = samples[name]
            url = API_HOST + url_path + (("?" + urllib.parse.urlencode(q)) if q else "")
            r = requests.get(url, timeout=(15, 90))
            return r.status_code, r.text
    except Exception as exc:  # noqa: BLE001
        return -1, f"__error__: {exc}"
    return None, None


def _live_sample(rows: list[dict], now: str) -> int:
    """Fill response_example for gap rows (no contract example) by calling the real
    endpoint. Polite per-IP spacing; each call isolated so one failure never aborts the
    pass. Records response_source='live', the HTTP status, and the sample timestamp."""
    samples = dict(SAMPLE_VALUES)
    samples.update(_bootstrap_ids())
    n_ok = 0
    for row in rows:
        if row.get("response_example") or row.get("response_schema") or not row.get("endpoint_path"):
            continue
        status, text = _sample_one(row, samples)
        if status is None:
            continue
        row["response_sampled_at"] = now
        row["response_http_status"] = status if (isinstance(status, int) and status > 0) else None
        ok = (isinstance(status, int) and 200 <= status < 300
              and bool(text) and text.lstrip()[:1] in ("{", "["))
        if ok:
            row["response_example"] = text[:40000]
            row["response_source"] = "live"
            n_ok += 1
            print(f"  ✓ {status} {row.get('methods'):<5} {row['endpoint_path']}", flush=True)
        else:
            snippet = (text or "").replace("\n", " ")[:80]
            print(f"  · {status} {row.get('methods'):<5} {row['endpoint_path']}  {snippet!r}", flush=True)
        time.sleep(SAMPLE_DELAY_SECONDS)
    print(f"live-sampled {n_ok} real 2xx responses", flush=True)
    return n_ok


def main() -> int:
    import lance
    import pyarrow as pa

    live = "--live-sample" in sys.argv
    contracts, _top = _fetch_contracts()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    for rel, md in sorted(contracts):
        row = _parse(rel, md)
        row["source_repo"] = REPO
        row["fetched_at"] = now
        row["response_source"] = "contract" if row.get("response_example") else None
        row["response_http_status"] = None
        row["response_sampled_at"] = None
        rows.append(row)

    with_path = sum(1 for r in rows if r["endpoint_path"])
    print(f"parsed {len(rows)} contracts; {with_path} with an /api endpoint path", flush=True)

    if live:
        print("live-sampling gap endpoints (no contract response example)…", flush=True)
        _live_sample(rows, now)

    table = pa.Table.from_pylist(rows)
    so = _r2_storage_options()
    lance.write_dataset(table, CATALOG_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(CATALOG_URI, storage_options=so)
    ds.create_scalar_index("endpoint_path", index_type="BTREE")
    print(f"landed {ds.count_rows()} rows → {CATALOG_URI} (BTREE on endpoint_path)", flush=True)

    # read-back: response coverage by provenance
    import duckdb
    con = duckdb.connect(":memory:")
    con.register("src", ds.scanner(
        columns=["response_example", "response_schema", "response_source"]).to_reader())
    con.execute("CREATE TABLE c AS SELECT * FROM src")
    cov = con.execute("""SELECT count(*),
        count(*) FILTER (WHERE response_example IS NOT NULL OR response_schema IS NOT NULL),
        count(*) FILTER (WHERE response_source='contract'),
        count(*) FILTER (WHERE response_source='live') FROM c""").fetchone()
    print(f"response coverage: {cov[1]}/{cov[0]}  (contract {cov[2]}, live {cov[3]})", flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
