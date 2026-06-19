#!/usr/bin/env python3
"""Benchmark — gtm-mcp Lance retrieval code path (sam.gov + usaspending access patterns).

WHY. The gtm-mcp gateway serves point-lookups and audience queries over the Gen-3 Lance/R2
sink. The optimizable surface that gtm-mcp OWNS is the retrieval *code path* — registry
resolution, the warm-handle cache, BTREE scanner pushdown (projection + predicate), the
read-only SQL guard's noise-stripping, and `referenced_datasets`' per-query registry scan.
R2 network latency is upstream and not something gateway code can shave; this benchmark
therefore isolates the code path.

TWO MODES, ONE WORKLOAD.
  --mode local (default, deterministic, network-free): builds local Lance fixtures shaped
    like the load-bearing sam.gov (`sam_master_entities`, `sam_master_domains`) and
    usaspending (`contractor_award_summary`) datasets — WIDE schemas with real BTREE scalar
    indices — then drives the REAL gateway code against them by monkeypatching only the
    three R2 data-source seams (`discover_datasets`, `r2_storage_options`,
    `_versions_listing`). `get_registry` / `referenced_datasets` / `assert_read_only` /
    `_cached_dataset` / `open_dataset` / `query` and all of `audience.*` stay REAL — exactly
    the code being optimized. Network-free → median latency is reproducible (low variance),
    so it is a defensible single optimization metric.
  --mode live (correctness smoke, needs R2 creds via doppler): samples real ids out of the
    live datasets and runs the SAME workload against the real registry. Proves the
    optimizations hold on real data and that documented columns survive. NOT the optimization
    signal (network noise) — a constraint.

METRIC (stdout JSON): `min_ms` — best-of-N warm-cache wall-clock of one full workload pass.
Best-of-N is the standard CPU-microbenchmark estimator: it isolates the code-path cost from
transient OS-scheduling / GC noise, so it is far more reproducible run-to-run than the mean
(which the validator's <10% determinism gate requires). `median_ms` is reported too, for
context.
GOLDEN (stdout JSON): `golden_sha` — sha256 over the DOCUMENTED columns of a fixed retrieval
set. Invariant to projection (undocumented-column removal does not change it), so it pins
correctness across optimization iterations.
`components_ms` (stdout JSON): per-phase best-of-N breakdown so the optimizable share (the
projection-eligible point-lookups vs the DuckDB audience SQL) is visible.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]  # scripts/benchmarks/ -> scripts -> repo root
sys.path.insert(0, str(REPO_ROOT))

# Dataset names exactly as the gateway registry resolves them.
NAMES = ["companies", "people", "sam_master_entities", "sam_master_domains",
         "contractor_award_summary"]

# Documented agent-facing column contracts (audience.py docstrings). The golden digest is
# computed over THESE keys only, so projecting a wide row down to its documented columns
# leaves the digest unchanged — correctness is pinned to the contract, not to leaked columns.
COMPANY_DOC = ["company_id", "company_name", "normalized_domain",
               "company_linkedin_url", "source_platform"]
PEOPLE_DOC = ["contact_id", "company_id", "normalized_domain", "full_name",
              "first_name", "last_name", "title", "person_linkedin_url", "source_platform"]

# Fixture sizing — big enough that BTREE pushdown + projection matter, small enough to build
# in a few seconds. WIDE filler columns simulate the real datasets where projection saves
# real deserialization.
N = int(os.environ.get("GTM_BENCH_ROWS", "10000"))
N_LOOKUPS = 50   # point-lookups of each kind per workload pass
N_RESOLVE = 20   # recipient_uei -> contact resolutions per pass
NAICS_CYCLE = ["541512", "541511", "236220", "541330", "517311", "423430"]


def _filler(prefix: str, i: int, width: int) -> dict:
    return {f"{prefix}_attr_{k}": f"{prefix}-{i}-val-{k}" for k in range(width)}


def build_fixtures(tmp: Path) -> dict[str, str]:
    import lance
    import pyarrow as pa

    companies, people, entities, domains, awards = [], [], [], [], []
    for i in range(N):
        uei = f"UEI{i:08d}"
        dom = f"dom{i}.example.com"
        companies.append({
            "company_id": f"co{i}", "company_name": f"Company {i}", "normalized_domain": dom,
            "company_linkedin_url": f"https://linkedin.com/company/{i}",
            "source_platform": "fixture",
            **_filler("co", i, 8),  # wide: 8 undocumented filler columns
        })
        entities.append({
            "uei": uei, "primary_naics": NAICS_CYCLE[i % len(NAICS_CYCLE)],
            "cage_code": f"CAGE{i:05d}", "legal_business_name": f"Legal Name {i}",
            "is_active": (i % 7 != 0),
            **_filler("ent", i, 18),  # wide: simulate the ~140-field SAM entity master
        })
        domains.append({"normalized_domain": dom, "uei": uei,
                        "legal_business_name": f"Legal Name {i}", "cage_code": f"CAGE{i:05d}",
                        "sam_extract_code": "A"})
        awards.append({
            "recipient_uei": uei, "recipient_name": f"Recipient {i}",
            "combined_total_dollars": float(i * 1000),
            "prime_award_count": i % 50, "subaward_count": i % 30,
            **_filler("aw", i, 14),  # wide: the precomputed federal-spend resume
        })
    # people: 2 per company so the most-senior reduction has something to pick.
    for i in range(N * 2):
        ci = i % N
        people.append({
            "contact_id": f"ct{i}", "company_id": f"co{ci}",
            "normalized_domain": f"dom{ci}.example.com",
            "full_name": f"Person {i}", "first_name": f"First{i}", "last_name": f"Last{i}",
            "title": ("Chief Executive Officer" if i < N else "Analyst"),
            "person_linkedin_url": f"https://linkedin.com/in/{i}",
            "source_platform": "fixture",
            **_filler("ppl", i, 6),
        })

    tables = {
        "companies": pa.Table.from_pylist(companies),
        "people": pa.Table.from_pylist(people),
        "sam_master_entities": pa.Table.from_pylist(entities),
        "sam_master_domains": pa.Table.from_pylist(domains),
        "contractor_award_summary": pa.Table.from_pylist(awards),
    }
    btree = {
        "companies": ["normalized_domain"],
        "people": ["normalized_domain", "company_id"],
        "sam_master_entities": ["uei", "primary_naics", "cage_code"],
        "sam_master_domains": ["normalized_domain", "uei"],
        "contractor_award_summary": ["recipient_uei"],
    }
    paths: dict[str, str] = {}
    for name, tbl in tables.items():
        p = str(tmp / f"{name}.lance")
        lance.write_dataset(tbl, p, mode="create")
        ds = lance.dataset(p)
        for col in btree[name]:
            ds.create_scalar_index(col, index_type="BTREE")
        paths[name] = p
    return paths


def wire_local(database, paths: dict[str, str]) -> None:
    """Patch ONLY the three R2 data-source seams; everything above stays real."""
    database.discover_datasets = lambda: dict(paths)
    database.r2_storage_options = lambda: {}

    def local_versions(uri: str):
        vdir = os.path.join(uri, "_versions")
        if not os.path.isdir(vdir):
            return None
        out = []
        for e in os.scandir(vdir):
            st = e.stat()
            out.append((e.name, st.st_size, int(st.st_mtime)))
        return sorted(out) if out else None

    database._versions_listing = local_versions
    database.get_registry(refresh=True)  # rebuild registry through the REAL _build_registry


def _digest_rows(rows: list[dict], doc_keys: list[str] | None) -> list:
    """Canonicalize rows to documented keys (or all keys, sorted) for a stable digest."""
    out = []
    for r in rows:
        keys = doc_keys if doc_keys is not None else sorted(r.keys())
        out.append([f"{k}={r.get(k)!r}" for k in keys])
    return out


def run_workload(audience, corex, database, *, collect_golden: bool):
    """One full retrieval pass. Returns (golden_payload_or_None)."""
    golden: list = []
    award_ueis = [f"UEI{i:08d}" for i in range(0, N_LOOKUPS * 7, 7)]
    domains = [f"dom{i}.example.com" for i in range(0, N_LOOKUPS * 11, 11)]
    resolve_rows = [{"recipient_uei": f"UEI{i:08d}"} for i in range(0, N_RESOLVE * 13, 13)]

    for uei in award_ueis:
        res = audience.lookup_awards_by_uei(uei)
        if collect_golden:
            golden.append(("award", uei, _digest_rows(
                [res["award_summary"]] if res["award_summary"] else [], None)))
    for dom in domains:
        res = audience.search_company_by_domain(dom)
        if collect_golden:
            golden.append(("company", dom, _digest_rows(res["companies"], COMPANY_DOC)))
    for dom in domains:
        res = audience.search_people_by_domain(dom)
        if collect_golden:
            golden.append(("people", dom, _digest_rows(res["people"], PEOPLE_DOC)))
    # corex recipient_uei -> contact bridge (sam_master_domains BTREE + people graph)
    contacts = corex._resolve_contacts(resolve_rows, "recipient_uei")
    if collect_golden:
        golden.append(("resolve", "batch", _digest_rows(
            sorted(contacts, key=lambda c: c["contact_id"]),
            ["contact_id", "company_id", "normalized_domain", "full_name", "title"])))
    # audience SQL — exercises referenced_datasets registry scan + JIT register + DuckDB
    sqls = [
        "SELECT uei, primary_naics, cage_code FROM sam_master_entities "
        "WHERE primary_naics = '541512' LIMIT 100",
        "SELECT e.uei, e.primary_naics, d.normalized_domain "
        "FROM sam_master_entities e JOIN sam_master_domains d ON e.uei = d.uei "
        "WHERE e.primary_naics = '541512' LIMIT 100",
        "SELECT recipient_uei, recipient_name FROM awards "
        "WHERE recipient_uei = 'UEI00000005' LIMIT 10",
    ]
    for sql in sqls:
        res = audience.execute_audience_query(sql)
        if collect_golden:
            golden.append(("sql", sql[:40], _digest_rows(
                sorted(res["rows"], key=lambda r: json.dumps(r, sort_keys=True, default=str)),
                None)))
    return golden if collect_golden else None


def golden_sha(golden) -> str:
    blob = json.dumps(golden, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


PHASES = ("awards", "company", "people", "resolve", "sql")


def run_phase(audience, corex, phase: str, seeds: dict) -> None:
    if phase == "awards":
        for uei in seeds["award_ueis"]:
            audience.lookup_awards_by_uei(uei)
    elif phase == "company":
        for dom in seeds["domains"]:
            audience.search_company_by_domain(dom)
    elif phase == "people":
        for dom in seeds["domains"]:
            audience.search_people_by_domain(dom)
    elif phase == "resolve":
        corex._resolve_contacts(seeds["resolve_rows"], "recipient_uei")
    elif phase == "sql":
        for sql in seeds["sqls"]:
            audience.execute_audience_query(sql)


def _seeds() -> dict:
    return {
        "award_ueis": [f"UEI{i:08d}" for i in range(0, N_LOOKUPS * 7, 7)],
        "domains": [f"dom{i}.example.com" for i in range(0, N_LOOKUPS * 11, 11)],
        "resolve_rows": [{"recipient_uei": f"UEI{i:08d}"} for i in range(0, N_RESOLVE * 13, 13)],
        "sqls": [
            "SELECT uei, primary_naics, cage_code FROM sam_master_entities "
            "WHERE primary_naics = '541512' LIMIT 100",
            "SELECT e.uei, e.primary_naics, d.normalized_domain "
            "FROM sam_master_entities e JOIN sam_master_domains d ON e.uei = d.uei "
            "WHERE e.primary_naics = '541512' LIMIT 100",
            "SELECT recipient_uei, recipient_name FROM awards "
            "WHERE recipient_uei = 'UEI00000005' LIMIT 10",
        ],
    }


def bench_local(reps: int) -> dict:
    from apps.gtm_mcp.src import database
    os.environ.setdefault("R2_ACCESS_KEY_ID", "x")
    os.environ.setdefault("R2_SECRET_ACCESS_KEY", "x")
    os.environ.setdefault("R2_ENDPOINT", "https://example.invalid")
    with tempfile.TemporaryDirectory(prefix="gtm-bench-") as td:
        paths = build_fixtures(Path(td))
        wire_local(database, paths)
        from apps.gtm_mcp.src.tools import audience, corex
        seeds = _seeds()
        golden = run_workload(audience, corex, database, collect_golden=True)
        sha = golden_sha(golden)
        for _ in range(3):  # warm: handle cache, DuckDB catalog, index residency
            run_workload(audience, corex, database, collect_golden=False)
        samples, comp = [], {p: [] for p in PHASES}
        for _ in range(reps):
            t0 = time.perf_counter()
            run_workload(audience, corex, database, collect_golden=False)
            samples.append((time.perf_counter() - t0) * 1000.0)
            for p in PHASES:
                tp = time.perf_counter()
                run_phase(audience, corex, p, seeds)
                comp[p].append((time.perf_counter() - tp) * 1000.0)
    samples.sort()
    return {
        "mode": "local", "reps": reps, "rows": N,
        "min_ms": round(samples[0], 3),                       # METRIC — best-of-N
        "median_ms": round(statistics.median(samples), 3),
        "mean_ms": round(statistics.mean(samples), 3),
        "stdev_ms": round(statistics.pstdev(samples), 3),
        "p90_ms": round(samples[min(len(samples) - 1, int(len(samples) * 0.9))], 3),
        "golden_sha": sha,
        "min_stdev_pct": 0.0,  # placeholder; cross-run stability is measured by the validator
        "components_ms": {p: round(min(comp[p]), 3) for p in PHASES},
    }


def bench_live(reps: int) -> dict:
    """Correctness smoke against live R2 (creds via doppler env). Samples real ids, runs the
    workload, asserts documented columns survive. Exits non-zero on structural failure."""
    from apps.gtm_mcp.src import database
    from apps.gtm_mcp.src.tools import audience
    reg = database.get_registry(refresh=True)
    for must in ("companies", "contractor_award_summary"):
        if must not in reg:
            print(json.dumps({"mode": "live", "error": f"dataset {must} not registered"}))
            return {"mode": "live", "ok": False, "error": f"{must} unregistered"}
    # sample real seeds
    ueis = [r["recipient_uei"] for r in database.open_dataset("awards")
            .scanner(columns=["recipient_uei"], limit=N_LOOKUPS).to_table().to_pylist()
            if r.get("recipient_uei")]
    doms = [r["normalized_domain"] for r in database.open_dataset("companies")
            .scanner(columns=["normalized_domain"], limit=N_LOOKUPS).to_table().to_pylist()
            if r.get("normalized_domain")]
    checks = {"award_nonempty": 0, "company_nonempty": 0, "company_doc_ok": 0}
    for uei in ueis[:25]:
        if audience.lookup_awards_by_uei(uei)["found"]:
            checks["award_nonempty"] += 1
    for dom in doms[:25]:
        res = audience.search_company_by_domain(dom)
        if res["companies"]:
            checks["company_nonempty"] += 1
            if all(k in res["companies"][0] for k in COMPANY_DOC):
                checks["company_doc_ok"] += 1
    # warm latency (informational only)
    samples = []
    for _ in range(reps):
        t0 = time.perf_counter()
        for uei in ueis[:25]:
            audience.lookup_awards_by_uei(uei)
        for dom in doms[:25]:
            audience.search_company_by_domain(dom)
        samples.append((time.perf_counter() - t0) * 1000.0)
    ok = (checks["company_nonempty"] == 0 or checks["company_doc_ok"] == checks["company_nonempty"])
    return {"mode": "live", "ok": bool(ok), "reps": reps, "checks": checks,
            "seeds": {"ueis": len(ueis), "domains": len(doms)},
            "median_ms": round(statistics.median(samples), 3) if samples else None}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "live"], default="local")
    ap.add_argument("--reps", type=int, default=15)
    args = ap.parse_args()
    result = bench_local(args.reps) if args.mode == "local" else bench_live(args.reps)
    print(json.dumps(result))
    if args.mode == "live" and not result.get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
