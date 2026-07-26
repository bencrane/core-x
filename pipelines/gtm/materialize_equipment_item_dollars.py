"""Dollar-weight the combo equipment mentions → equipment-item grain Lance mart.

Predecessor: ``materialize_combo_equipment_needs.py`` (the clean mention grain).
Directive:   ~/Desktop/hq/directives/2026-07-26-equipment-dollar-weight.md

Purely mechanical allocation — every rule is stated, none is inferred. NO categorization,
NO taxonomy, NO judgment about which equipment matters. Bucketing is a later LLM cycle.

INPUTS
    s3://data-sink/active/combo_equipment_mentions/   one row per (naics, psc, item).
        DE-DUPED to (naics_code, psc_code, equipment_item) here — the source grain also
        carries equipment_item_raw, and 12 (combo, item) pairs have two raw spellings.
        Without the DISTINCT those combos would double-count their dollars.
    sidecar combo_award_active_state    active_obligated per combo (the active book).
    sidecar txn_events_combo            sum(obligation) FY23-25, i.e. action_date
                                        BETWEEN 2022-10-01 AND 2025-09-30.
    s3://data-sink/active/reference/equipment_flowdown_factors/  (v1)
                                        production_code -> equipment_related_share.
    NAICS6 -> production_code            scripts/demo_bakes/_shared.klems_mapping()
                                        — imported, never reimplemented.

ALLOCATION (crude by design — do not improve)
    share(c)                 = equipment_related_share(klems(naics(c))), else 0.039
                               (the weighted national default for unmapped NAICS)
    equip_envelope_active(c) = active_obligated(c)      x share(c)
    equip_envelope_fy2325(c) = obligations_fy23_25(c)   x share(c)
    each of the n_c items mentioned by combo c receives envelope(c) / n_c  (equal split)

    Combos with dollars but no mentions contribute nothing. Combos with mentions but no
    dollars contribute zero-dollar rows — kept as a coverage signal.

OUTPUT  s3://data-sink/active/equipment_item_dollars/  — one row per canonical item.
    *_mentioning columns are UN-SPLIT sums over every combo mentioning the item (the
    dollar-weighted-mention view: a $1B combo counts $1B toward each of its items).
    *_alloc columns are the equal-split envelope view and conserve exactly to the summed
    envelope of covered combos. The two answer different questions; both are carried.
    BTREE on equipment_item. Full rebuild each run, no appends.

Run:
    doppler run -p core-x -c prd -- \
        python3 pipelines/gtm/materialize_equipment_item_dollars.py
"""
from __future__ import annotations

import os
import sys

import duckdb

_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "scripts", "demo_bakes"))

from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402
from _shared import flowdown_factors, klems_mapping, q  # noqa: E402

MENTIONS_URI = "s3://data-sink/active/combo_equipment_mentions/"
OUT_URI = "s3://data-sink/active/equipment_item_dollars/"
OUT_INDEXES = [("equipment_item", "BTREE")]

DEFAULT_SHARE = 0.039  # weighted national default for NAICS with no KLEMS production_code
FY_START, FY_END = "2022-10-01", "2025-09-30"

METHOD = (
    "equal-split allocation of equip_envelope over the combo's mentioned items; "
    "envelope = combo obligations x equipment_related_share from "
    "s3://data-sink/active/reference/equipment_flowdown_factors (v1), "
    "NAICS6->production_code via scripts/demo_bakes/_shared.klems_mapping(), "
    f"unmapped NAICS -> flat {DEFAULT_SHARE}; active book from sidecar "
    "combo_award_active_state.active_obligated; FY23-25 from sidecar txn_events_combo "
    f"sum(obligation) action_date {FY_START}..{FY_END}; "
    "*_mentioning = un-split sums, *_alloc = equal-split"
)

PAGE = 50000


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _paged(sql_template: str, expected: int) -> list[list]:
    """Sidecar reads cap at 50k rows/call — page deterministically until drained."""
    rows: list[list] = []
    while True:
        page = q(f"{sql_template} LIMIT {PAGE} OFFSET {len(rows)}", limit=PAGE)
        rows.extend(page)
        print(f"    …{len(rows):,}/{expected:,}")
        if len(page) < PAGE:
            break
    assert len(rows) == expected, f"paged read drift: {len(rows)} != {expected}"
    return rows


def main() -> None:
    import pyarrow as pa

    so = _r2_storage_options()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC';")

    # ── 1. mentions (Lance → DuckDB), de-duped to the (combo, item) grain ──────
    import lance
    mentions_tbl = lance.dataset(MENTIONS_URI, storage_options=so).to_table(
        columns=["naics_code", "psc_code", "equipment_item"])
    con.register("mentions_raw", mentions_tbl)
    con.execute("""CREATE TABLE mentions AS
        SELECT DISTINCT naics_code, psc_code, equipment_item FROM mentions_raw""")
    n_ment, n_combo, n_item = con.execute(
        "SELECT count(*), count(DISTINCT (naics_code,psc_code)), "
        "count(DISTINCT equipment_item) FROM mentions").fetchone()
    print(f"mentions: {mentions_tbl.num_rows:,} source rows → {n_ment:,} distinct "
          f"(combo,item) · {n_combo:,} combos · {n_item:,} items")

    # ── 2. sidecar: the active book, all combos (needed for coverage denominator) ──
    print("sidecar combo_award_active_state…")
    n_state = q("SELECT count(*) FROM combo_award_active_state")[0][0]
    state = _paged(
        "SELECT naics_code, psc_code, active_obligated FROM combo_award_active_state "
        "ORDER BY naics_code, psc_code", n_state)
    con.register("state_arrow", pa.table({
        "naics_code": pa.array([r[0] for r in state], pa.string()),
        "psc_code": pa.array([r[1] for r in state], pa.string()),
        "active_obligated": pa.array([r[2] for r in state], pa.float64()),
    }))
    con.execute("CREATE TABLE combo_active AS SELECT * FROM state_arrow")

    # ── 3. sidecar: FY23-25 obligations per combo ──────────────────────────────
    print("sidecar txn_events_combo FY23-25…")
    fy_sql = (f"SELECT naics_code, psc_code, sum(obligation) AS obligated_fy23_25 "
              f"FROM txn_events_combo "
              f"WHERE action_date BETWEEN DATE '{FY_START}' AND DATE '{FY_END}' "
              f"GROUP BY naics_code, psc_code")
    n_fy = q(f"SELECT count(*) FROM ({fy_sql})")[0][0]
    fy = _paged(f"{fy_sql} ORDER BY naics_code, psc_code", n_fy)
    con.register("fy_arrow", pa.table({
        "naics_code": pa.array([r[0] for r in fy], pa.string()),
        "psc_code": pa.array([r[1] for r in fy], pa.string()),
        "obligated_fy23_25": pa.array([r[2] for r in fy], pa.float64()),
    }))
    con.execute("CREATE TABLE combo_fy AS SELECT * FROM fy_arrow")

    # ── 4. flow-down share per NAICS (mapping imported, never reimplemented) ───
    print("KLEMS mapping + flow-down factors…")
    to_pc = klems_mapping()
    factors = flowdown_factors()
    naics = [r[0] for r in con.execute(
        "SELECT DISTINCT naics_code FROM combo_active "
        "UNION SELECT DISTINCT naics_code FROM mentions "
        "UNION SELECT DISTINCT naics_code FROM combo_fy").fetchall()]
    shares, mapped = [], 0
    for n in naics:
        pc = to_pc(n)
        s = factors.get(pc) if pc is not None else None
        if s is None:
            shares.append(DEFAULT_SHARE)
        else:
            shares.append(float(s))
            mapped += 1
    con.register("share_arrow", pa.table({
        "naics_code": pa.array(naics, pa.string()),
        "equipment_related_share": pa.array(shares, pa.float64()),
    }))
    con.execute("CREATE TABLE naics_share AS SELECT * FROM share_arrow")
    print(f"  {len(naics):,} distinct NAICS · {mapped:,} mapped to a KLEMS factor · "
          f"{len(naics) - mapped:,} on the {DEFAULT_SHARE} default")

    # ── 5. envelopes per combo, then the equal split ──────────────────────────
    con.execute("""CREATE TABLE combo_env AS
        SELECT m.naics_code, m.psc_code,
               count(*)                                    AS n_items,
               coalesce(a.active_obligated, 0.0)            AS active_obligated,
               coalesce(f.obligated_fy23_25, 0.0)           AS obligated_fy23_25,
               s.equipment_related_share                    AS share,
               coalesce(a.active_obligated, 0.0)  * s.equipment_related_share AS env_active,
               coalesce(f.obligated_fy23_25, 0.0) * s.equipment_related_share AS env_fy2325
        FROM mentions m
        JOIN naics_share s USING (naics_code)
        LEFT JOIN combo_active a USING (naics_code, psc_code)
        LEFT JOIN combo_fy     f USING (naics_code, psc_code)
        GROUP BY m.naics_code, m.psc_code, a.active_obligated, f.obligated_fy23_25,
                 s.equipment_related_share""")

    con.execute(f"""CREATE TABLE mart AS
        SELECT
            m.equipment_item,
            count(*)                                   AS n_combos,
            sum(e.active_obligated)                    AS active_obligated_mentioning,
            sum(e.obligated_fy23_25)                   AS obligated_fy23_25_mentioning,
            sum(e.env_active  / e.n_items)             AS equip_dollars_active_alloc,
            sum(e.env_fy2325  / e.n_items)             AS equip_dollars_fy23_25_alloc,
            '{METHOD.replace("'", "''")}'              AS method,
            now()                                      AS materialized_at
        FROM mentions m
        JOIN combo_env e USING (naics_code, psc_code)
        GROUP BY m.equipment_item
        ORDER BY equip_dollars_active_alloc DESC, m.equipment_item""")

    # ── 6. gates ──────────────────────────────────────────────────────────────
    rows, items = con.execute(
        "SELECT count(*), count(DISTINCT equipment_item) FROM mart").fetchone()
    assert rows == items == n_item, (
        f"item coverage gate FAILED: mart rows={rows:,} distinct={items:,} "
        f"vs mentions items={n_item:,}")

    alloc_a, alloc_f = con.execute(
        "SELECT sum(equip_dollars_active_alloc), sum(equip_dollars_fy23_25_alloc) "
        "FROM mart").fetchone()
    env_a, env_f = con.execute(
        "SELECT sum(env_active), sum(env_fy2325) FROM combo_env").fetchone()
    d_a, d_f = abs(alloc_a - env_a), abs(alloc_f - env_f)
    print(f"conservation: active alloc {alloc_a:,.6f} vs envelope {env_a:,.6f} "
          f"(Δ ${d_a:.6f})")
    print(f"conservation: fy23-25 alloc {alloc_f:,.6f} vs envelope {env_f:,.6f} "
          f"(Δ ${d_f:.6f})")
    assert d_a < 0.01 and d_f < 0.01, "equal-split conservation gate FAILED (>1 cent)"

    # ── 7. publish ────────────────────────────────────────────────────────────
    tbl = con.execute("SELECT * FROM mart").to_arrow_table()
    ds = write_indexed_dataset(tbl, OUT_URI, OUT_INDEXES, so)
    print(f"published {OUT_URI} rows={ds.count_rows():,} version={ds.version}")

    # ── 8. coverage ───────────────────────────────────────────────────────────
    book = q("SELECT sum(active_obligated) FROM combo_award_active_state")[0][0]
    cov_a, cov_combos = con.execute(
        "SELECT sum(active_obligated), count(*) FROM combo_env").fetchone()
    print(f"coverage: {cov_combos:,} covered combos hold ${cov_a:,.0f} of the "
          f"${book:,.0f} active book ({cov_a / book:.1%})")
    con.close()


if __name__ == "__main__":
    main()
