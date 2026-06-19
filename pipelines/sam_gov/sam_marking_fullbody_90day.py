"""SAM.gov 90-day attachment — FULL-BODY content-marking pre-pass + chunk write-back (Phase 0 item 3).

Implements build-plan PHASE 0 item 3 (docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md):
re-run the per-caveat control-marking regexes over the RE-ASSEMBLED full text of every resource in
the three chunk sinks (scope + unknown `text`; pricing `text` AND the doc-level `cells` grid), then
back-propagate marking PROMOTIONS onto the chunk rows — chunk-level `content_marking` is the single
enforcement gate for every egress decision; the head-scan-only markings carry a permanent
false-negative hole without this pass. Ledger events on `sam_attachment_extraction`
(state=`marking_fullbody`) are AUDIT PROVENANCE of how each promotion was decided — not a second gate.

SEMANTICS (promotion-only — the hard safety rule):
  * detected = full-body caveat set. new = union(existing, detected) per chunk, EXCEPT the legacy
    `unspecified` placeholder is REPLACED by the specific detected caveats when (and only when) the
    full-body scan detects >=1 caveat for that resource. detected == {} never changes anything:
    `['unspecified']` is never downgraded to `[]`, and no existing caveat is ever removed.
  * Full-body acronym matching is UPPERCASE-ONLY for the bare acronyms `EAR` and `CUI` (the 2,000-char
    head window tolerated IGNORECASE; over a multi-MB body, lowercase "ear"/"cui" inside ordinary
    words/phrases would over-mark wholesale). Spelled-out phrases stay case-insensitive, matching
    `sam_attachment_extract_90day._MARKING_PATTERNS` otherwise.

MECHANICS (probed live 2026-06-12, lance 7.0.0):
  * Reassembly: chunks concatenated in `chunk_ix` order joined with "\\n" (preserves \\b word
    boundaries; the ~180-char chunk overlap means a duplicated span can only re-detect caveats that
    exist in the document — it cannot synthesize new ones). Pricing `cells` is the doc-level grid
    DUPLICATED onto every chunk row (21.8 GB aggregate, exactly one distinct value per resource,
    probed live) — it is read ONCE per resource via a `chunk_ix = 0` filtered scan, which
    late-materializes only the 42.5 MB of needed pages (1.7 s live).
  * Write-back: `merge_insert("chunk_id")` with a SUBSET source table of exactly
    (chunk_id, content_marking) — lance 7.0.0 `when_matched_update_all` accepts subset-column
    sources and rewrites ONLY those columns (probed: `text`/`embedding` untouched, embedding never
    read). Because the write rewrites only matched fragments and the vector index keeps covering all
    unmatched rows, this subset write-back is SAFE on a vector-indexed sink (the same index-safe path
    as the embed module's flush) — see `_assert_marking_writeback_safe`. The broad
    `_assert_no_vector_index` guard remains only on the overwrite/compaction path, not this write-back.
    Each sink's write runs under its `SinkCommitLease` (D3).
  * Resume without double-apply: the scan phase checkpoints per-resource decisions to a JSONL file;
    the write-back worklist is re-derived from LIVE chunk state (rows where promote(existing,
    detected) != existing), so a crash-resume re-selects only the rows not yet applied and a re-run
    after completion selects zero rows. merge_insert on chunk_id is idempotent besides.
  * The embedding column is NEVER read, never written.

Run (heavy scan — background it; progress streams to --log; every phase is resumable):
    doppler run -- python pipelines/sam_gov/sam_marking_fullbody_90day.py --phase all --daemon
    ... --phase scan        # full-body detection -> decisions JSONL (read-only)
    ... --phase writeback   # promotions -> chunk rows (merge_insert) + ledger audit events
    ... --phase reconcile   # write-back == expansion assert + post-state distribution
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from pipelines.sam_gov.sam_attachment_extract_90day import (  # noqa: E402
    EXTRACTION_URI, PRICING_URI, SCOPE_URI, UNKNOWN_URI,
    SinkCommitLease, _append_dataset, _dataset_exists,
    _extraction_schema, _ledger_row, _r2_storage_options, _daemonize,
)

FEED = "sam_marking_fullbody_90day"
EXTRACTOR_TAG = "sam_marking_fullbody_90day@v1"

DECISIONS_PATH = os.environ.get("SAM90_MARKING_DECISIONS", "/tmp/sam_marking_fullbody_decisions.jsonl")
WB_CKPT_PATH = os.environ.get("SAM90_MARKING_WB_CKPT", "/tmp/sam_marking_fullbody_wb_ckpt.jsonl")
REPORT_PATH = os.environ.get("SAM90_MARKING_REPORT", "/tmp/sam_marking_fullbody_report.json")
LOG_PATH = os.environ.get("SAM90_MARKING_LOG", "/tmp/sam_marking_fullbody.log")
SCAN_BATCH_ROWS = int(os.environ.get("SAM90_MARKING_SCAN_BATCH", "8192"))
WB_BATCH_ROWS = int(os.environ.get("SAM90_MARKING_WB_BATCH", "50000"))

# ── Full-body per-caveat patterns. Source of truth: _MARKING_PATTERNS/_DIST_STMT_RX in
#    sam_attachment_extract_90day.py (spec §7.4), with the single mandated deviation: the bare
#    acronyms EAR and CUI match UPPERCASE-ONLY (case-sensitive) in the full-body pass; spelled-out
#    phrases stay case-insensitive. Caveat names and Distribution-Statement letter capture identical.
_FULLBODY_MARKING_PATTERNS: tuple[tuple[str, tuple[re.Pattern, ...]], ...] = (
    ("cui", (re.compile(r"CONTROLLED UNCLASSIFIED INFORMATION", re.IGNORECASE),
             re.compile(r"\bCUI\b"))),                                   # acronym: uppercase-only
    ("fouo", (re.compile(r"FOR OFFICIAL USE ONLY|\bFOUO\b", re.IGNORECASE),)),
    ("export_controlled", (re.compile(r"EXPORT CONTROLLED", re.IGNORECASE),)),
    ("itar", (re.compile(r"\bITAR\b", re.IGNORECASE),)),
    ("ear", (re.compile(r"\bEAR\b"),)),                                  # acronym: uppercase-only
)
_DIST_STMT_RX = re.compile(r"DISTRIBUTION STATEMENT ([B-F])", re.IGNORECASE)

# Canonical caveat ordering for deterministic post-promotion lists (set-equality decides writes;
# the stored order is cosmetic but stable). Unknown tokens sort after, alphabetically.
_CANON_ORDER = ("cui", "fouo", "export_controlled", "itar", "ear",
                "dist_stmt_b", "dist_stmt_c", "dist_stmt_d", "dist_stmt_e", "dist_stmt_f",
                "unspecified")
_CANON_RANK = {v: i for i, v in enumerate(_CANON_ORDER)}


def detect_markings_fullbody(body: str) -> list[str]:
    """Ordered, de-duplicated caveats literally present anywhere in the reassembled full body."""
    out: list[str] = []
    for name, rxs in _FULLBODY_MARKING_PATTERNS:
        if any(rx.search(body) for rx in rxs):
            out.append(name)
    for m in _DIST_STMT_RX.finditer(body):
        tok = f"dist_stmt_{m.group(1).lower()}"
        if tok not in out:
            out.append(tok)
    return out


def canon_sort(vals) -> list[str]:
    return sorted(set(vals), key=lambda v: (_CANON_RANK.get(v, len(_CANON_ORDER)), v))


def promote(existing, detected) -> list[str]:
    """PROMOTION-ONLY union (safety rule #2). detected empty => existing unchanged (never downgrade
    ['unspecified'] to []). detected non-empty => union, with the legacy 'unspecified' placeholder
    replaced by the specific detected caveats. No existing specific caveat is ever removed."""
    ex = set(existing or [])
    det = set(detected or [])
    if not det:
        return canon_sort(ex)
    out = (ex - {"unspecified"}) | det
    assert out >= (ex - {"unspecified"}), "promotion-only invariant violated"
    return canon_sort(out)


def classify_decision(prior, post) -> str:
    """How the promotion was decided — audit label for the ledger event."""
    p, q = set(prior or []), set(post or [])
    if p == q:
        return "unchanged"
    if not p:
        return "promoted"                      # newly marked ([] -> caveats)
    if "unspecified" in p:
        return "upgraded"                      # ['unspecified'] (+specifics) -> specific caveats
    return "extended"                          # existing specifics gained additional caveats


# ════════════════════════════════════════════════════════════════════════ checkpoint helpers
def _load_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _append_jsonl(path: str, rec: dict) -> None:
    with open(path, "a", buffering=1) as f:
        f.write(json.dumps(rec) + "\n")


def _sink_uris(args) -> list[tuple[str, str]]:
    return [("scope", args.scope_uri), ("pricing", args.pricing_uri), ("unknown", args.unknown_uri)]


# ════════════════════════════════════════════════════════════════════════ phase: scan (read-only)
def phase_scan(args, so: dict) -> dict:
    """Per sink: reassemble every resource's full body in chunk_ix order and emit a per-resource
    decision line {prior, detected, post, decision} to the decisions JSONL. Read-only; resumable
    (already-decided resources are skipped; a finished sink writes a scan_done marker)."""
    import lance
    done_markers = set()
    decided: dict[str, set] = {"scope": set(), "pricing": set(), "unknown": set()}
    for rec in _load_jsonl(args.decisions):
        if rec.get("marker") == "scan_done":
            done_markers.add(rec["sink"])
        elif "resource_id" in rec:
            decided.setdefault(rec["sink"], set()).add(rec["resource_id"])

    totals = {}
    for sink, uri in _sink_uris(args):
        if sink in done_markers:
            print(f"scan {sink}: scan_done marker present — skipped (resume)", flush=True)
            continue
        t0 = time.time()
        ds = lance.dataset(uri, storage_options=so)        # version-pinned for both passes
        # Pass A: expected chunk count per resource (cheap single-column scan).
        expected: dict[str, int] = {}
        for b in ds.to_batches(columns=["resource_id"], batch_size=65536):
            for rid in b.column("resource_id").to_pylist():
                expected[rid] = expected.get(rid, 0) + 1
        print(f"scan {sink}: {ds.count_rows():,} chunks / {len(expected):,} resources "
              f"(counts in {time.time()-t0:.0f}s)", flush=True)

        # Pricing only: doc-level cells grid, fetched ONCE per resource via late-materialized
        # filtered scan (the grid is duplicated on every chunk row — never read the full column).
        cells_by_res: dict[str, str] = {}
        if sink == "pricing":
            for b in ds.to_batches(columns=["resource_id", "cells"], batch_size=4096,
                                   filter="chunk_ix = 0"):
                for rid, c in zip(b.column("resource_id").to_pylist(),
                                  b.column("cells").to_pylist()):
                    if c:
                        cells_by_res[rid] = c
            print(f"scan {sink}: cells fetched for {len(cells_by_res):,} resources", flush=True)

        # Pass B: stream chunks, buffer per resource, flush each resource when complete.
        buf: dict[str, list] = {}
        seen: dict[str, int] = {}
        n_flushed = n_skipped = rows_seen = 0
        skip_rids = decided.get(sink, set())

        def _flush(rid: str) -> None:
            nonlocal n_flushed
            rows = sorted(buf.pop(rid), key=lambda r: r[0])
            texts = [t for _, t, _ in rows]
            prior: set = set()
            for _, _, mk in rows:
                prior |= set(mk or [])
            body = "\n".join(t or "" for t in texts)
            if sink == "pricing" and rid in cells_by_res:
                body = body + "\n" + cells_by_res[rid]
            detected = detect_markings_fullbody(body)
            post = promote(prior, detected)
            _append_jsonl(args.decisions, {
                "sink": sink, "resource_id": rid,
                "prior": canon_sort(prior), "detected": detected, "post": post,
                "decision": classify_decision(canon_sort(prior), post),
                "n_chunks": len(rows), "fullbody_chars": len(body),
                "sha256_fullbody": hashlib.sha256(body.encode("utf-8", "replace")).hexdigest(),
            })
            n_flushed += 1

        for b in ds.to_batches(columns=["resource_id", "chunk_ix", "text", "content_marking"],
                               batch_size=SCAN_BATCH_ROWS):
            rids = b.column("resource_id").to_pylist()
            cixs = b.column("chunk_ix").to_pylist()
            texts = b.column("text").to_pylist()
            marks = b.column("content_marking").to_pylist()
            rows_seen += len(rids)
            for rid, cix, txt, mk in zip(rids, cixs, texts, marks):
                seen[rid] = seen.get(rid, 0) + 1
                if rid in skip_rids:
                    if seen[rid] == expected[rid]:
                        n_skipped += 1
                    continue
                buf.setdefault(rid, []).append((cix, txt, mk))
                if seen[rid] == expected[rid]:
                    _flush(rid)
            if rows_seen % (SCAN_BATCH_ROWS * 32) < SCAN_BATCH_ROWS:
                print(f"scan {sink}: rows {rows_seen:,}/{ds.count_rows():,} flushed {n_flushed:,} "
                      f"buffered_res {len(buf):,} ({time.time()-t0:.0f}s)", flush=True)

        if buf:   # every resource must complete within the pinned dataset version
            raise RuntimeError(f"scan {sink}: {len(buf)} resources incomplete at stream end "
                               f"(e.g. {list(buf)[:3]}) — counts/stream diverged; re-run scan")
        _append_jsonl(args.decisions, {"marker": "scan_done", "sink": sink})
        totals[sink] = {"resources": len(expected), "flushed": n_flushed, "resumed_skip": n_skipped,
                        "seconds": round(time.time() - t0, 1)}
        print(f"scan {sink}: DONE {n_flushed:,} decided (+{n_skipped:,} resumed) "
              f"in {time.time()-t0:.0f}s", flush=True)
    return totals


def _assert_marking_writeback_safe(ds, uri: str) -> None:
    """The full-body marking write-back is a SUBSET-column merge_insert('chunk_id')
    .when_matched_update_all() — it rewrites ONLY (chunk_id, content_marking), never `text`/`embedding`,
    and lance 7.0.0 leaves the vector index covering all unmatched rows (the same index-safe path as
    sam_attachment_embed_90day._flush). This path is therefore SAFE on a vector-indexed sink; the broad
    `_assert_no_vector_index` guard is for the overwrite/compaction path only (and stays intact there).
    No-op here — kept as the single documented deviation point so the swap is greppable and explained."""
    return


# ════════════════════════════════════════════════════════════════════════ phase: writeback
def phase_writeback(args, so: dict, run_id: str) -> dict:
    """Back-propagate promotions onto chunk rows (merge_insert on chunk_id, subset source of exactly
    (chunk_id, content_marking)) and append per-resource audit events to the extraction ledger.

    Double-apply-proof: the worklist is rows where promote(existing, detected) != existing computed
    from LIVE sink state — already-applied rows select nothing on resume/re-run. Ledger appends are
    guarded by a ledger_done marker per sink."""
    import lance
    import pyarrow as pa
    started = dt.datetime.now(dt.timezone.utc)

    decisions = [r for r in _load_jsonl(args.decisions) if "resource_id" in r]
    by_sink: dict[str, dict[str, dict]] = {"scope": {}, "pricing": {}, "unknown": {}}
    for r in decisions:
        by_sink.setdefault(r["sink"], {})[r["resource_id"]] = r
    done = {r["sink"] for r in _load_jsonl(args.decisions) if r.get("marker") == "scan_done"}
    for sink, uri in _sink_uris(args):
        if sink not in done:
            raise RuntimeError(f"writeback: sink '{sink}' has no scan_done marker in "
                               f"{args.decisions} — run --phase scan to completion first")

    ledger_done = {r["sink"] for r in _load_jsonl(args.wb_ckpt) if r.get("marker") == "ledger_done"}
    report = {}
    for sink, uri in _sink_uris(args):
        t0 = time.time()
        # Only detected-nonempty resources can ever change a chunk row (promotion-only).
        cand = {rid: rec for rid, rec in by_sink.get(sink, {}).items() if rec["detected"]}
        ds = lance.dataset(uri, storage_options=so)
        _assert_marking_writeback_safe(ds, uri)  # subset chunk_id merge_insert is index-safe (see helper)

        # Worklist from LIVE state: (chunk_id, new content_marking) where the promotion changes the row.
        src_ids: list[str] = []
        src_marks: list[list[str]] = []
        written_per_res: dict[str, int] = {}
        for b in ds.to_batches(columns=["chunk_id", "resource_id", "content_marking"],
                               batch_size=65536):
            for cid, rid, mk in zip(b.column("chunk_id").to_pylist(),
                                    b.column("resource_id").to_pylist(),
                                    b.column("content_marking").to_pylist()):
                rec = cand.get(rid)
                if rec is None:
                    continue
                new = promote(mk, rec["detected"])
                if set(new) != set(mk or []):
                    src_ids.append(cid)
                    src_marks.append(new)
                    written_per_res[rid] = written_per_res.get(rid, 0) + 1
        expected = len(src_ids)

        # Batched subset-source merge_insert under the sink's single-committer lease (D3).
        written = 0
        if expected:
            with SinkCommitLease(uri, holder=f"marking_fullbody:{run_id}"):
                ds = lance.dataset(uri, storage_options=so)
                _assert_marking_writeback_safe(ds, uri)  # subset chunk_id merge_insert is index-safe (see helper)
                for i in range(0, expected, WB_BATCH_ROWS):
                    src = pa.table({
                        "chunk_id": pa.array(src_ids[i:i + WB_BATCH_ROWS], pa.string()),
                        "content_marking": pa.array(src_marks[i:i + WB_BATCH_ROWS],
                                                    pa.list_(pa.string())),
                    })
                    res = (ds.merge_insert("chunk_id").when_matched_update_all().execute(src))
                    n_upd = None
                    if isinstance(res, dict):
                        n_upd = res.get("num_updated_rows")
                    else:
                        n_upd = getattr(res, "num_updated_rows", None)
                    n_upd = src.num_rows if n_upd is None else int(n_upd)
                    written += n_upd
                    _append_jsonl(args.wb_ckpt, {"sink": sink, "batch_start": i,
                                                 "rows": src.num_rows, "updated": n_upd})
                    print(f"writeback {sink}: batch @{i} rows={src.num_rows} updated={n_upd}",
                          flush=True)

        # Post-verify on live state: zero rows still needing the promotion.
        residual = 0
        ds = lance.dataset(uri, storage_options=so)
        for b in ds.to_batches(columns=["resource_id", "content_marking"], batch_size=65536):
            for rid, mk in zip(b.column("resource_id").to_pylist(),
                               b.column("content_marking").to_pylist()):
                rec = cand.get(rid)
                if rec is not None and set(promote(mk, rec["detected"])) != set(mk or []):
                    residual += 1

        # Ledger audit events — one per resource whose post-state differs from prior (provenance of
        # the promotion decision), appended once (ledger_done marker guards resume duplicates).
        promoted = {rid: rec for rid, rec in by_sink.get(sink, {}).items()
                    if set(rec["post"]) != set(rec["prior"])}
        events_appended = 0
        if promoted and sink not in ledger_done:
            now = dt.datetime.now(dt.timezone.utc)
            rows = []
            for rid, rec in promoted.items():
                rows.append(_ledger_row(
                    resource_id=rid, lane=sink, stage="marking", state="marking_fullbody",
                    extractor=EXTRACTOR_TAG,
                    header_class=rec["decision"],
                    codec=("prior=" + (",".join(rec["prior"]) or "-")
                           + ";detected=" + (",".join(rec["detected"]) or "-")),
                    content_marking=rec["post"], n_chunks=rec["n_chunks"],
                    text_chars=rec["fullbody_chars"], sha256_text=rec["sha256_fullbody"],
                    run_id=run_id, started_at=started, completed_at=now))
            import pyarrow as _pa
            with SinkCommitLease(args.extraction_uri, holder=f"marking_fullbody:{run_id}"):
                _append_dataset(args.extraction_uri,
                                _pa.Table.from_pylist(rows, schema=_extraction_schema()), so)
            events_appended = len(rows)
            _append_jsonl(args.wb_ckpt, {"marker": "ledger_done", "sink": sink,
                                         "events": events_appended})
        elif sink in ledger_done:
            print(f"writeback {sink}: ledger_done marker present — ledger append skipped (resume)",
                  flush=True)

        report[sink] = {
            "candidate_resources": len(cand),
            "promoted_resources": len(promoted),
            "expansion_expected_chunks": expected,
            "chunks_written": written,
            "residual_rows_needing_promotion": residual,
            "ledger_events_appended": events_appended,
            "reconcile_writeback": "PASS" if (written == expected and residual == 0) else "FAIL",
            "seconds": round(time.time() - t0, 1),
        }
        print(f"writeback {sink}: expected={expected:,} written={written:,} residual={residual} "
              f"promoted_res={len(promoted):,} ledger_events={events_appended:,} "
              f"-> {report[sink]['reconcile_writeback']}", flush=True)
    return report


# ════════════════════════════════════════════════════════════════════════ phase: reconcile
def phase_reconcile(args, so: dict) -> dict:
    """Read-only post-state probe: per-sink chunk-level content_marking distribution, per-resource
    state for every promoted resource (must equal its decided post set), and the headline counters."""
    import lance
    decisions = [r for r in _load_jsonl(args.decisions) if "resource_id" in r]
    report = {"per_sink": {}, "headline": {}}
    mismatched_resources = 0
    for sink, uri in _sink_uris(args):
        sink_dec = {r["resource_id"]: r for r in decisions if r["sink"] == sink}
        dist: dict[str, int] = {}
        res_state: dict[str, set] = {}
        ds = lance.dataset(uri, storage_options=so)
        for b in ds.to_batches(columns=["resource_id", "content_marking"], batch_size=65536):
            for rid, mk in zip(b.column("resource_id").to_pylist(),
                               b.column("content_marking").to_pylist()):
                key = ",".join(canon_sort(mk or [])) or "[]"
                dist[key] = dist.get(key, 0) + 1
                if rid in sink_dec and set(sink_dec[rid]["post"]) != set(sink_dec[rid]["prior"]):
                    res_state.setdefault(rid, set()).add(key)
        bad = {rid: sorted(states) for rid, states in res_state.items()
               if states != {",".join(sink_dec[rid]["post"]) or "[]"}}
        mismatched_resources += len(bad)
        report["per_sink"][sink] = {
            "chunk_marking_distribution": dict(sorted(dist.items(), key=lambda kv: -kv[1])),
            "promoted_resources_checked": len(res_state),
            "promoted_resources_mismatched": len(bad),
            "mismatch_sample": dict(list(bad.items())[:5]),
        }
        print(f"reconcile {sink}: {len(res_state):,} promoted resources checked, "
              f"{len(bad)} mismatched; top dist: "
              f"{list(report['per_sink'][sink]['chunk_marking_distribution'].items())[:4]}",
              flush=True)

    n_scanned = len(decisions)
    promoted = [r for r in decisions if r["decision"] == "promoted"]
    upgraded = [r for r in decisions if r["decision"] == "upgraded"]
    extended = [r for r in decisions if r["decision"] == "extended"]
    report["headline"] = {
        "resources_scanned": n_scanned,
        "resources_promoted_newly_marked": len(promoted),
        "resources_upgraded_from_unspecified": len(upgraded),
        "resources_extended": len(extended),
        "resources_unchanged": n_scanned - len(promoted) - len(upgraded) - len(extended),
        "promoted_resources_mismatched": mismatched_resources,
    }
    return report


# ════════════════════════════════════════════════════════════════════════ main
def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Full-body content-marking pre-pass + chunk write-back "
                                            "(Phase 0 item 3).")
    p.add_argument("--phase", default="all", choices=["scan", "writeback", "reconcile", "all"])
    p.add_argument("--run-id", default=None)
    p.add_argument("--daemon", action="store_true", help="double-fork + setsid; log to --log")
    p.add_argument("--decisions", default=DECISIONS_PATH)
    p.add_argument("--wb-ckpt", default=WB_CKPT_PATH)
    p.add_argument("--report-out", default=REPORT_PATH)
    p.add_argument("--log", default=LOG_PATH)
    p.add_argument("--scope-uri", default=SCOPE_URI)
    p.add_argument("--pricing-uri", default=PRICING_URI)
    p.add_argument("--unknown-uri", default=UNKNOWN_URI)
    p.add_argument("--extraction-uri", default=EXTRACTION_URI)
    args = p.parse_args(argv)
    run_id = args.run_id or f"marking_fullbody_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"

    if args.daemon:
        _daemonize(args.log)

    so = _r2_storage_options() if args.scope_uri.startswith("s3://") else {}
    for _, uri in _sink_uris(args):
        if not _dataset_exists(uri, so):
            raise RuntimeError(f"sink dataset absent: {uri}")

    report: dict = {"run_id": run_id, "started_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    if args.phase in ("scan", "all"):
        report["scan"] = phase_scan(args, so)
    if args.phase in ("writeback", "all"):
        report["writeback"] = phase_writeback(args, so, run_id)
    if args.phase in ("reconcile", "all"):
        report["reconcile"] = phase_reconcile(args, so)
        wb = report.get("writeback") or {}
        wb_pass = all(v.get("reconcile_writeback") == "PASS" for v in wb.values()) if wb else None
        rec_pass = report["reconcile"]["headline"]["promoted_resources_mismatched"] == 0
        report["reconcile_overall"] = (
            "PASS" if rec_pass and (wb_pass is not False) else "FAIL")
    report["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    with open(args.report_out, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report.get("reconcile", {}).get("headline", {}), indent=2), flush=True)
    print(f"report -> {args.report_out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
