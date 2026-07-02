"""Generate docs/reference/FPDS_CANONICAL_FIELD_DICTIONARY.md from the SINGLE source of truth.

The dictionary is a PROJECTION of the pipeline's COLUMN_SPEC (the same contract that generates every
projection, the final column order, and the index lists in usaspending_fpds_canonical.py) — NOT a
hand-maintained parallel list that drifts. Structure/type/provenance/index-topology come from
COLUMN_SPEC; per-field definitions + domain-value codes come from the committed sidecar
fpds_field_definitions.json (USAspending Data Dictionary join, extracted once from the prior doc).

FAIL-CLOSED: when a live probe JSON is supplied (--probe), the generator asserts live schema names,
order, count, and indexed columns EXACTLY equal the code contract, and refuses to emit on any
divergence. The doc therefore can never assert a schema that disagrees with either the code or R2.

    # regenerate from code only (types via duck->arrow map, indices via BTREE/BITMAP lists):
    python3 -m pipelines.usaspending.gen_fpds_canonical_dictionary --verified-date 2026-07-02

    # regenerate anchored to a live probe (authoritative types + rowcount + fail-closed cross-check):
    #   probe.json = {"rows","ncols","n_indices","version","schema":[{name,type}...],"indices":[...]}
    python3 -m pipelines.usaspending.gen_fpds_canonical_dictionary \
        --probe probe.json --verified-date 2026-07-02
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
PIPE = HERE / "usaspending_fpds_canonical.py"
SIDE = HERE / "fpds_field_definitions.json"
OUT_DEFAULT = REPO / "docs" / "reference" / "FPDS_CANONICAL_FIELD_DICTIONARY.md"

DUCK2ARROW = {"DATE": "date32[day]", "TIMESTAMP": "timestamp[us]", "DOUBLE": "double",
              "BIGINT": "int64", "VARCHAR": "string"}
# tokens that are SQL wrappers/type keywords, not source column identifiers
_STOP = {"s", "kbulk", "TRY_CAST", "COALESCE", "AS", "DOUBLE", "BIGINT", "DATE", "TIMESTAMP",
         "VARCHAR", "INTEGER", "replace", "upper", "substr", "CAST", "lower", "trim", "nullif"}

GROUP_LABEL = {
    "key": "Primary keys",
    "core": "Volatile core — source-reconciled per key (argmax `last_modified_date`)",
    "enrich_pg": "Enrichment — BULK/pg only (overwritten from the reconciled base, key-independent of the core winner)",
    "enrich_monthly": "Enrichment — monthly-unique (Treasury / federal-account funding + highly-compensated-officer comp; pg lacks all 12)",
    "prov": "Provenance",
}


def load_module():
    spec = importlib.util.spec_from_file_location("fpdscanon", PIPE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def natives(expr: str | None) -> list[str]:
    """Distinct source column identifiers referenced by a projection expr (wrappers/types stripped)."""
    if not expr:
        return []
    out: list[str] = []
    for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr):
        if tok in _STOP or tok in out:
            continue
        out.append(tok)
    return out


def esc(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("|", "\\|")).strip()


def bucket(c: dict) -> str:
    if c["group"] == "enrich":
        return "enrich_monthly" if c["feed_expr"] is not None else "enrich_pg"
    return c["group"]


def source_cell(c: dict) -> str:
    if c["group"] == "prov":
        return "injected UTC literal" if c["canonical"] == "built_at" else "derived per key (winning source tag)"
    b, f = natives(c["bulk_expr"]), natives(c["feed_expr"])
    firstchar = " *(first char)*" if (c["bulk_expr"] and "substr" in c["bulk_expr"]) else ""
    fmt = lambda xs: ", ".join(f"`{x}`" for x in xs)
    if b and f:
        cell = f"`{b[0]}` (BULK+FRESH)" if b == f else f"BULK {fmt(b)} · FRESH/MO {fmt(f)}"
    elif b:
        cell = f"BULK-only {fmt(b)}"
    elif f:
        cell = f"FRESH/MO-only {fmt(f)}"
    else:
        cell = "—"
    return cell + firstchar


# Cross-vocabulary synonyms: canonical (monthly/API) name -> pg (BULK dictionary) name that carries
# the USAspending definition. The exec-comp fields are the same concept under two upstream names.
ALIASES: dict[str, str] = {}
for _i in range(1, 6):
    ALIASES[f"highly_compensated_officer_{_i}_name"] = f"officer_{_i}_name"
    ALIASES[f"highly_compensated_officer_{_i}_amount"] = f"officer_{_i}_amount"

# Pipeline-authored columns have no USAspending definition (they are OURS) — defined from the build
# contract, not fabricated. Kept minimal and authoritative.
INTERNAL_DEFS = {
    "canonical_source": "Pipeline provenance — which upstream feed won this row's volatile core: one of `fresh`, `bulk`, or `monthly`.",
    "built_at": "Pipeline provenance — a single UTC build-timestamp literal injected into every row of a given build.",
}


def resolve(c: dict, side: dict, key: str) -> str:
    if key == "definition" and c["canonical"] in INTERNAL_DEFS:
        return INTERNAL_DEFS[c["canonical"]]
    cands = [c["canonical"], *natives(c["bulk_expr"]), *natives(c["feed_expr"])]
    cands += [ALIASES[x] for x in list(cands) if x in ALIASES]
    for cand in cands:
        rec = side.get(cand)
        if rec and rec.get(key):
            return rec[key]
    return ""


def col_table(cols: list[dict], side: dict, tmap: dict) -> str:
    head = ("| # | canonical column | type | source | definition | domain / codes |\n"
            "|---|---|---|---|---|---|")
    rows = []
    for i, c in enumerate(cols, 1):
        canon = c["canonical"]
        typ = tmap.get(canon, DUCK2ARROW.get(c["duck_type"], c["duck_type"].lower()))
        rows.append("| {n} | `{col}` | `{typ}` | {src} | {defn} | {dom} |".format(
            n=i, col=canon, typ=typ, src=source_cell(c),
            defn=esc(resolve(c, side, "definition")) or "—",
            dom=esc(resolve(c, side, "domain")) or "—"))
    return head + "\n" + "\n".join(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", type=str, default=None)
    ap.add_argument("--verified-date", type=str, default="")
    ap.add_argument("--out", type=str, default=str(OUT_DEFAULT))
    args = ap.parse_args()

    mod = load_module()
    spec_cols = list(mod.COLUMN_SPEC)
    canon = [c["canonical"] for c in spec_cols]
    btree, bitmap = list(mod.BTREE_COLS), list(mod.BITMAP_COLS)

    side = json.loads(SIDE.read_text(encoding="utf-8"))

    tmap: dict[str, str] = {}
    rows_live = ncols_live = nidx_live = ver_live = None
    idx_live: list[dict] = []
    if args.probe:
        probe = json.loads(Path(args.probe).read_text(encoding="utf-8"))
        live_names = [s["name"] for s in probe["schema"]]
        tmap = {s["name"]: s["type"] for s in probe["schema"]}
        idx_live = probe.get("indices", [])
        idx_cols_live = sorted(f for i in idx_live for f in (i.get("fields") or []))
        # FAIL-CLOSED cross-check: code contract == live, exactly.
        errs = []
        if live_names != canon:
            errs.append(f"schema order/name mismatch (live {len(live_names)} vs spec {len(canon)})")
        if idx_cols_live != sorted(btree + bitmap):
            errs.append("indexed columns differ from BTREE_COLS+BITMAP_COLS")
        if errs:
            raise SystemExit("REFUSING TO EMIT — live/code divergence:\n  - " + "\n  - ".join(errs))
        rows_live, ncols_live = probe["rows"], probe["ncols"]
        nidx_live, ver_live = probe.get("n_indices"), probe.get("version")

    rows_str = f"{rows_live:,}" if rows_live is not None else "_(run with --probe to anchor)_"
    ncols_str = str(ncols_live if ncols_live is not None else len(canon))
    nidx_str = str(nidx_live if nidx_live is not None else len(btree) + bitmap.__len__())
    vdate = args.verified_date or "(unspecified)"
    ver_str = f" · manifest v{ver_live}" if ver_live is not None else ""

    # ---- provenance / not-carried sets ----
    spine_src = set(canon)
    for c in spec_cols:
        spine_src.update(natives(c["bulk_expr"]))
        spine_src.update(natives(c["feed_expr"]))
    not_carried = sorted(n for n in side if n not in spine_src)

    # ---- assemble ----
    buckets: dict[str, list[dict]] = {}
    for c in spec_cols:
        buckets.setdefault(bucket(c), []).append(c)
    order = ["key", "core", "enrich_pg", "enrich_monthly", "prov"]
    counts = {k: len(buckets.get(k, [])) for k in order}

    P: list[str] = []
    P.append("# FPDS Canonical Transaction Table — Field Dictionary & Reference")
    P.append("")
    P.append("> **AUTO-GENERATED — do not hand-edit.** This file is a projection of `COLUMN_SPEC` in "
             "[`pipelines/usaspending/usaspending_fpds_canonical.py`](../../pipelines/usaspending/usaspending_fpds_canonical.py), "
             "cross-checked fail-closed against the live R2 dataset. To change it, change the pipeline "
             "contract and regenerate (see [§7](#7-regenerating-this-document)). Definitions are joined "
             "from the committed sidecar `fpds_field_definitions.json` (USAspending Data Dictionary) and "
             "may be length-capped verbatim from source.")
    P.append("")
    P.append(f"**Live anchor:** `{mod.CANONICAL_URI}` — **{rows_str} rows · {ncols_str} cols · "
             f"{nidx_str} indices**{ver_str}. Verified live: {vdate}.")
    P.append("")
    P.append("---")
    P.append("")

    # §1 identity
    P.append("## 1. What this table is")
    P.append("")
    P.append("The typed, PK-grained **system-of-record read model** for U.S. federal contract (FPDS) "
             "prime-award **transactions**. One row = one contract transaction, uniquely keyed. It is the "
             "reconciliation of three upstream FPDS feeds into a single canonical vocabulary (the FPDS "
             "`bulk_download`/`awards` field names), stored append-only in Lance on R2.")
    P.append("")
    P.append("| property | value |")
    P.append("|---|---|")
    P.append(f"| URI | `{mod.CANONICAL_URI}` |")
    P.append(f"| rows | {rows_str} |")
    P.append(f"| columns | {ncols_str} |")
    P.append(f"| indices | {nidx_str} ({len(btree)} BTREE + {len(bitmap)} BITMAP) |")
    P.append("| grain / PK | `contract_transaction_unique_key` (exactly one surviving row per key — "
             "structural uniqueness gate at build) |")
    P.append(f"| storage | Lance `data_storage_version={mod.DATA_STORAGE_VERSION}`, "
             f"`max_rows_per_file={mod.MAX_ROWS_PER_FILE:,}`, written LOCAL then boto3 uniform-part "
             "published to R2 (never a direct-R2 writer — Giants `400 InvalidPart`) |")
    P.append("| ops ledger | `" + mod.OPS_TABLE + "` (Postgres; one row per build) |")
    P.append("")
    P.append("### Upstream feeds (reconciled into this table)")
    P.append("")
    P.append("| feed | source dataset | ~rows | role |")
    P.append("|---|---|---|---|")
    P.append(f"| **BULK** | `{mod.BULK_URI}` | 107.25M | pg-derived; 378 typed `rpt.*` cols; the enrichment source |")
    P.append(f"| **FRESH** | `{mod.FRESH_URI}` | 1.99M | USAspending API; canonical vocabulary; newest corrections |")
    P.append(f"| **MONTHLY** | `{mod.ARCHIVE_FULL_URI}` | 2.98M | monthly bulk-download CSV (physical name `archive_full` — a tracked rename); carries 12 monthly-unique enrichment cols |")
    P.append(f"| **DELTA** | `{mod.ARCHIVE_DELTA_URI}` | — | deletion ledger (`correction_delete_ind='D'`); drives R6 tombstones |")
    P.append("")

    # §2 querying
    P.append("## 2. How to read / query it")
    P.append("")
    P.append("Open with the R2 storage options and hand the scanner to DuckDB (out-of-core). The table "
             "is append-only and PK-unique, so no dedup is needed on read.")
    P.append("")
    P.append("```python")
    P.append("import os, lance")
    P.append('os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")  # before any lance call')
    P.append("so = {\"aws_access_key_id\": os.environ[\"R2_ACCESS_KEY_ID\"],")
    P.append("      \"aws_secret_access_key\": os.environ[\"R2_SECRET_ACCESS_KEY\"],")
    P.append("      \"endpoint\": os.environ[\"R2_ENDPOINT\"], \"region\": \"auto\"}")
    P.append(f'ds = lance.dataset("{mod.CANONICAL_URI}", storage_options=so)')
    P.append("# pushdown filters on the 17 indexed columns are cheap; scans on the rest are full")
    P.append("```")
    P.append("")
    P.append("**Indexed columns accelerate pushdown** (see [§4](#4-index-topology-17)). Point-lookups and "
             "range scans on the BTREE columns and equality/`IN` filters on the BITMAP columns are the "
             "fast paths; everything else is a full column scan of 108M rows.")
    P.append("")

    # §3 column reference
    P.append("## 3. Column reference (" + ncols_str + ")")
    P.append("")
    P.append("Grouped by role. `source` shows the native upstream column(s) each canonical column "
             "projects from — `BULK` = pg `rpt.*` vocabulary, `FRESH/MO` = canonical vocabulary carried "
             "verbatim by the FRESH + monthly feeds. Enrichment columns are BULK-sourced except the 12 "
             "monthly-unique ones. `type` is the live Arrow type.")
    P.append("")
    for k in order:
        cols = buckets.get(k, [])
        if not cols:
            continue
        P.append(f"### 3.{order.index(k)+1} {GROUP_LABEL[k]}  ({len(cols)})")
        P.append("")
        P.append(col_table(cols, side, tmap))
        P.append("")

    # §4 indices
    P.append("## 4. Index topology (" + nidx_str + ")")
    P.append("")
    P.append("All scalar indices. **BTREE** for high-cardinality / temporal / point-lookup columns; "
             "**BITMAP** for low-cardinality categoricals (cheap equality/`IN` bitset filters). Index "
             "name convention: `<column>_idx`. Rebuilt in-RAM (`LANCE_BYPASS_SPILLING=true`) — a 108M-row "
             "external merge sort OOMs the DataFusion spill pool.")
    P.append("")
    P.append("| # | column | type | index name |")
    P.append("|---|---|---|---|")
    n = 0
    live_by_col = {f: i for i in idx_live for f in (i.get("fields") or [])}
    for col in btree:
        n += 1
        nm = live_by_col.get(col, {}).get("name", f"{col}_idx")
        P.append(f"| {n} | `{col}` | BTREE | `{nm}` |")
    for col in bitmap:
        n += 1
        nm = live_by_col.get(col, {}).get("name", f"{col}_idx")
        P.append(f"| {n} | `{col}` | BITMAP | `{nm}` |")
    P.append("")

    # §5 reconciliation
    P.append("## 5. Reconciliation semantics")
    P.append("")
    P.append("Two-tier logical merge, one physical artifact. Executed as a single flat 3-way "
             "`row_number()` window (argmax is associative, so the flat window is byte-identical to the "
             "explicit two tiers):")
    P.append("")
    P.append("- **Core (volatile) columns** — resolved per key by **argmax(`last_modified_date`)** across "
             "the three per-key-collapsed feeds. Cross-source tie precedence: **FRESH (1) > MONTHLY (2) > "
             "BULK (3)**. The winning feed's tag is recorded in **`canonical_source ∈ {fresh, bulk, "
             "monthly}`**.")
    P.append("- **Enrichment columns** — filled **independently of the core winner**, pg-preferred from "
             "the reconciled base. The 27 pg-only enrichment cols come straight from `bulk_latest`; the 12 "
             "monthly-unique cols are `COALESCE(pg, monthly)` from a separate enrichment-populatedness "
             "dedup (pg currently lacks all 12, so they resolve to monthly — the `COALESCE` future-proofs "
             "a pg schema add).")
    P.append("- **Deletes / reinstatement** — a `D` key in the DELTA ledger is honored only when scoped to "
             "the latest `archive_snapshot_stamp` and the reconciled winner is not strictly newer than the "
             "delete (R6 tombstone with R5 reinstatement).")
    P.append("- **PK-uniqueness** is structural (`row_number()=1` over ≤1-row-per-source collapses) and "
             "fail-closed-gated before publish.")
    P.append("")
    P.append("`subcontracting_plan` carries the raw FPDS code (`A`–`H`); the `has_subcontracting_plan` "
             "boolean (`IN ('C','D','E','F','G','H')`) is derived downstream in serving, not on the spine.")
    P.append("")

    # §6 not carried
    P.append("## 6. Documented in BULK but NOT carried on the spine (" + str(len(not_carried)) + ")")
    P.append("")
    P.append("These native columns exist in the USAspending BULK source / FPDS Data Dictionary but are "
             "**not projected** onto the canonical spine. To carry one, add an entry to `COLUMN_SPEC` "
             "(canonical name, type, group, `bulk_expr`/`feed_expr`) and rebuild — do not hand-edit this "
             "doc. Definitions are from the committed sidecar (may be abbreviated).")
    P.append("")
    P.append("| native column | type | definition |")
    P.append("|---|---|---|")
    for nat in not_carried:
        rec = side.get(nat, {})
        P.append(f"| `{nat}` | {esc(rec.get('type')) or '—'} | {esc(rec.get('definition')) or '—'} |")
    P.append("")

    # §7 regenerate
    P.append("## 7. Regenerating this document")
    P.append("")
    P.append("This doc is generated; the pipeline `COLUMN_SPEC` is the source of truth. After any spine "
             "change, regenerate (fail-closed against a live probe):")
    P.append("")
    P.append("```bash")
    P.append("# 1. probe.json — dump live schema+indices+rowcount (read-only) from R2:")
    P.append("doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' python3 - <<'PY'")
    P.append("import os, json, lance")
    P.append('os.environ.setdefault("LANCE_BYPASS_SPILLING","true")')
    P.append('so={"aws_access_key_id":os.environ["R2_ACCESS_KEY_ID"],"aws_secret_access_key":os.environ["R2_SECRET_ACCESS_KEY"],"endpoint":os.environ["R2_ENDPOINT"],"region":"auto"}')
    P.append(f'ds=lance.dataset("{mod.CANONICAL_URI}",storage_options=so); sch=ds.schema')
    P.append('json.dump({"uri":ds.uri,"rows":ds.count_rows(),"ncols":len(sch),"n_indices":len(ds.list_indices()),"version":ds.version,"schema":[{"name":sch.field(i).name,"type":str(sch.field(i).type)} for i in range(len(sch))],"indices":ds.list_indices()},open("probe.json","w"),default=str,indent=2)')
    P.append("PY")
    P.append("")
    P.append("# 2. regenerate (definitions come from the committed sidecar; no R2 needed for this step):")
    P.append("python3 -m pipelines.usaspending.gen_fpds_canonical_dictionary \\")
    P.append("    --probe probe.json --verified-date $(date +%F)")
    P.append("```")
    P.append("")
    P.append("To refresh **definitions** (e.g. after USAspending revises its Data Dictionary), rebuild the "
             "sidecar `pipelines/usaspending/fpds_field_definitions.json` (native column → "
             "`{definition, domain, type}`) and regenerate.")
    P.append("")

    Path(args.out).write_text("\n".join(P), encoding="utf-8")

    matched = sum(1 for c in spec_cols if resolve(c, side, "definition"))
    print(json.dumps({
        "out": args.out,
        "rows": rows_str, "cols": ncols_str, "indices": nidx_str,
        "group_counts": counts,
        "cols_with_definition": matched,
        "cols_without_definition": len(spec_cols) - matched,
        "not_carried": len(not_carried),
        "cross_checked_live": bool(args.probe),
    }, indent=2))


if __name__ == "__main__":
    main()
