"""Local end-to-end proof for credit_spine_normalize_index (no Modal, no R2).

Exercises the EXACT data-plane mechanism the Modal worker runs against R2 — but on
a synthetic PPP-shaped Lance dataset on the local filesystem, so it can run in CI /
in-session with no credentials. It imports the SHIPPED pure functions from
``credit_spine_normalize_index`` (``_name_norm`` / ``_zip5`` / ``_norm_block_projection``
/ the column constants), so a drift in the worker's normalization or projection is
caught here, not in production.

Steps (identical API surface to patch_dataset + verify_dataset; only the storage
backend differs — local FS vs R2):
  1. synthesize messy borrower names/zips (punctuation, &, accents, ZIP+4, leading
     zeros, empties) → write a local Lance dataset (borrower_name / borrower_zip);
  2. scan (with_row_id) → DuckDB applies the canonical macros via the worker's
     projection builder → add_columns zips the two keys on positionally (_rowid order);
  3. create_scalar_index BTREE on normalized_legal_name + zip_code;
  4. integrity gate: RECOMPUTE both keys from source and assert 0 mismatches + row
     count preserved + both indices committed;
  5. golden-case assertions on known tricky inputs;
  6. DELIVERABLE: explain_plan(verbose=True) contains ``ScalarIndexQuery`` AND the
     warm median point-query latency is < 50 ms.

    /tmp/credit_spine_venv/bin/python pipelines/resolution/credit_spine_local_verify.py
Exit code 0 ⇔ every assertion passed.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import time

import duckdb
import lance
import pyarrow as pa

# ── import the SHIPPED worker module by file path (no `pipelines` package needed,
#    and we never call its Modal entrypoints — only its pure helpers/constants). ──
_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "credit_spine_normalize_index", os.path.join(_HERE, "credit_spine_normalize_index.py"))
W = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(W)  # requires `modal` importable (top-level import in the worker)

N = 200_000  # enough rows that an unindexed equality scan is non-trivial; builds in seconds


def _synthesize() -> pa.Table:
    """PPP-shaped rows. `tag` marks the golden cases; the rest are high-cardinality
    realistic borrower names so a point query seeks a small match set."""
    golden = [
        # (tag,           borrower_name,                 borrower_zip,    expect_name,            expect_zip)
        ("g_amp",         "Smith & Co., LLC",            "94105",         "SMITH CO LLC",         "94105"),
        ("g_the_accent",  "  The   Café   Corporation ", "10001-4567",    "THE CAF CORPORATION",  "10001"),
        ("g_punct",       "José's Tacos #1",             "0123",          "JOSS TACOS 1",         "0123"),
        ("g_leadzero",    "Zero Lead Inc",               "01234-5678",    "ZERO LEAD INC",        "01234"),
        ("g_null_name",   "   ",                         "  ",            None,                   None),
        ("g_null_zip",    "Valid Name LLC",              "no-digits!",    "VALID NAME LLC",       None),
        # Canonical ordering: strip [^A-Z0-9 ] (literal-space class) FIRST, then collapse
        # \s+. Tabs are NOT in the class, so they are DELETED (joining neighbors), not
        # turned into a space — "A\t\tB   C" → "AB C". Literal-space runs DO collapse.
        ("g_tab_strip",   "A\t\tB   C",                  "902100000",     "AB C",                 "90210"),
    ]
    tags = [g[0] for g in golden]
    names = [g[1] for g in golden]
    zips = [g[2] for g in golden]
    loan = [f"GOLD{i:04d}" for i in range(len(golden))]
    # bulk high-cardinality rows
    for i in range(len(golden), N):
        tags.append("bulk")
        names.append(f"Acme Holdings {i} & Partners, L.P.")
        zips.append(f"{(10000 + (i % 89999)):05d}-{(i % 9999):04d}")
        loan.append(f"L{i:08d}")
    return pa.table({
        "loan_number": pa.array(loan, pa.string()),
        "borrower_name": pa.array(names, pa.string()),
        "borrower_zip": pa.array(zips, pa.string()),
        "tag": pa.array(tags, pa.string()),
    }), golden


def _duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    return con


def main() -> int:
    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        print(("  PASS " if cond else "  FAIL ") + msg)
        if not cond:
            failures.append(msg)

    name_col, zip_col = "borrower_name", "borrower_zip"
    NORM_NAME, NORM_ZIP = W.NORM_NAME_COL, W.NORM_ZIP_COL

    tmp = tempfile.mkdtemp(prefix="credit_spine_")
    uri = os.path.join(tmp, "ppp.lance")
    table, golden = _synthesize()

    print(f"[1] write synthetic dataset: {N:,} rows → {uri}")
    lance.write_dataset(table, uri, mode="create")
    ds = lance.dataset(uri)
    v_before, n0 = ds.version, ds.count_rows()
    check(n0 == N, f"row count after write = {n0:,} (expected {N:,})")
    check(NORM_NAME not in set(ds.schema.names) and NORM_ZIP not in set(ds.schema.names),
          "normalized keys absent before patch")

    print("[2] DuckDB transform (canonical macros) → add_columns (positional, _rowid order)")
    to_add = tuple(c for c in W.NEW_COLS if c not in set(ds.schema.names))
    proj = W._norm_block_projection(name_col, zip_col, to_add)
    print(f"    projection SQL:\n      " + proj.replace("\n", "\n      "))
    con = _duck()
    con.register("rdr", ds.scanner(columns=[name_col, zip_col], with_row_id=True).to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM rdr")
    con.unregister("rdr")
    arrow = con.execute(f"SELECT\n    {proj}\nFROM t ORDER BY _rowid").to_arrow_table().combine_chunks()
    con.close()
    ds.add_columns(arrow, batch_size=65536)
    ds = lance.dataset(uri)
    check(NORM_NAME in set(ds.schema.names) and NORM_ZIP in set(ds.schema.names),
          "both normalized keys present after add_columns")

    print("[3] create_scalar_index BTREE on both keys")
    for col in W.NEW_COLS:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
    ds = lance.dataset(uri)
    idx = W._index_names(ds)
    check(f"{NORM_NAME}_idx" in idx, f"{NORM_NAME}_idx committed")
    check(f"{NORM_ZIP}_idx" in idx, f"{NORM_ZIP}_idx committed")

    print("[4] integrity gate — recompute both keys from source, assert equality")
    con = _duck()
    con.register("rdr", ds.scanner(columns=[name_col, zip_col, NORM_NAME, NORM_ZIP]).to_reader())
    con.execute("CREATE TABLE v AS SELECT * FROM rdr")
    con.unregister("rdr")
    bad_name = con.execute(
        f"SELECT count(*) FROM v WHERE {NORM_NAME} IS DISTINCT FROM {W._name_norm(W._q(name_col))}"
    ).fetchone()[0]
    bad_zip = con.execute(
        f"SELECT count(*) FROM v WHERE {NORM_ZIP} IS DISTINCT FROM {W._zip5(W._q(zip_col))}"
    ).fetchone()[0]
    n1 = con.execute("SELECT count(*) FROM v").fetchone()[0]
    con.close()
    check(bad_name == 0, f"name recompute mismatches = {bad_name}")
    check(bad_zip == 0, f"zip recompute mismatches = {bad_zip}")
    check(n1 == n0, f"row count preserved = {n1:,}/{n0:,}")

    print("[5] golden-case assertions (known tricky inputs)")
    for tag, src_name, src_zip, exp_name, exp_zip in golden:
        row = ds.scanner(columns=[NORM_NAME, NORM_ZIP], filter=f"tag = '{tag}'",
                         limit=1).to_table().to_pylist()[0]
        gn, gz = row[NORM_NAME], row[NORM_ZIP]
        check(gn == exp_name, f"{tag}: name {src_name!r} → {gn!r} (expected {exp_name!r})")
        check(gz == exp_zip, f"{tag}: zip  {src_zip!r} → {gz!r} (expected {exp_zip!r})")

    print("[6] DELIVERABLE — explain_plan ScalarIndexQuery + warm median < 50 ms")
    # sample a real non-null normalized_legal_name and run the point query the
    # resolution join would issue (select a non-indexed payload col → index → take).
    val = str(ds.scanner(columns=[NORM_NAME], filter=f"{NORM_NAME} IS NOT NULL",
                         limit=1).to_table().column(NORM_NAME)[0].as_py()).replace("'", "''")
    cols = [NORM_NAME, name_col]
    flt = f"{NORM_NAME} = '{val}'"
    plan = ds.scanner(columns=cols, filter=flt).explain_plan(True)
    print("    physical plan:\n      " + plan.replace("\n", "\n      "))
    check("ScalarIndexQuery" in plan, "physical plan contains ScalarIndexQuery")
    ds.scanner(columns=cols, filter=flt).to_table()  # warm-up
    ts = []
    for _ in range(7):
        t0 = time.perf_counter()
        ds.scanner(columns=cols, filter=flt).to_table()
        ts.append((time.perf_counter() - t0) * 1000)
    ts.sort()
    median = ts[len(ts) // 2]
    print(f"    point-query latency: median={median:.3f}ms min={ts[0]:.3f}ms max={ts[-1]:.3f}ms (value={val!r})")
    check(median < 50.0, f"warm median point query {median:.3f}ms < 50ms")

    # contrast: same equality predicate on an UNINDEXED column (full scan) for context.
    plain_val = ds.scanner(columns=["loan_number"], limit=1).to_table().column("loan_number")[0].as_py()
    ds.scanner(columns=["loan_number"], filter=f"tag = 'bulk'").to_table()  # warm
    t0 = time.perf_counter()
    ds.scanner(columns=["loan_number"], filter=f"loan_number = '{plain_val}'").to_table()
    full_ms = (time.perf_counter() - t0) * 1000
    print(f"    (context) unindexed equality full-scan on loan_number: {full_ms:.3f}ms")

    print()
    if failures:
        print(f"RESULT: FAIL ({len(failures)} assertion(s) failed)")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("RESULT: PASS — all assertions passed (mechanism, gate, golden cases, deliverable)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
