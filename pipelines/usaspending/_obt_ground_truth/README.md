# FPDS Canonical OBT — committed ground-truth artifacts

R2 live-probed 2026-07-03. These are the authoritative, committed inputs for the FPDS L1 canonical
spine → 392-column OBT expansion. The execution plan that consumes them is
`docs/reference/FPDS_CANONICAL_OBT_EXECUTION_PLAN.md`.

They were originally generated in a worktree that was deleted mid-session and are committed here so a
fresh agent can execute the build with **zero reliance on any ephemeral scratchpad or conversation**.

| File | Contents |
|---|---|
| `spec_current.json` | the current 131 `COLUMN_SPEC` entries (verbatim) from `usaspending_fpds_canonical.py` @ main `5ddd960` |
| `spec_meta.json` | `BTREE_COLS` (11), `BITMAP_COLS` (7), `BULK_URI`, `FRESH_URI`, `CANONICAL_URI`, `n_spec=131` |
| `bulk_live_schema.json` | live BULK schema, 378 cols `{name: arrow_type}` (== committed sidecar `fpds_field_definitions.json`, 0 drift) |
| `fresh_live_schema.json` | live FRESH schema, 297 cols `{name: arrow_type}` (`contract_prime_txn`, all VARCHAR) |
| `not_carried_enum.json` | the 261 "documented but not carried" BULK cols `{bulk_col, sidecar_type, duck_type, in_live_bulk, in_live_fresh}` |
| `proposed_additions.json` | the 261 generated draft `COLUMN_SPEC` entries `{canonical, duck_type, group, bulk_expr, feed_expr, ...}` |
| `proposed_additions_snippet.py` | paste-ready Python `COLUMN_SPEC` snippet of all 261 entries |

## Invariants (verified)

- 131 current + 261 adds = **392** final OBT width.
- 261-add histogram: **143 VARCHAR, 84 BOOLEAN, 13 BIGINT, 11 DOUBLE, 6 TIMESTAMP, 4 DATE**.
- Sole tz col: `ingested_at` → `bulk_expr = CAST(ingested_at AS TIMESTAMP)`.
- 0 name collisions between the 261 adds and the existing 131 canonicals.
- All 261 adds: `group="enrich"`, `feed_expr=None` (BULK-native, pg-only enrichment).

`proposed_additions.json` / `_snippet.py`, the plan's Appendix §12, and the plan's Phase A §A.1
regenerator are three independent representations of the same set and agree exactly. Regenerate via
§A.1 as a fail-closed cross-check before pasting.
