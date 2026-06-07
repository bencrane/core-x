# RUNBOOK — Form 5500 (2025) Orphan Materialization to SoR

**Scope:** Publish the four patched Form 5500 datasets (carrier identity + Schedule C counterparty fees) from the R2 landing zone into the Gen-3 system of record and the local lake, **re-publish `sch_a_broker`** so the patch's backfilled `FORM_ID` index lands, then verify. Pipeline patch merged in PR #300 (commit `0c69ce2`); this runbook executes the materialization it enables.

> **Revision (post adversarial review):** §5 verification is now a committed script (`pipelines/form5500/verify_carrier_recon.py`), not a `/tmp` heredoc — the heredoc shipped a `.df()`/pandas crash. Phases A/B re-publish `sch_a_broker` (the FORM_ID index is created at ingest time, so the in-code patch alone does not land it). The pass gate is the report's `✅ PASS` token + exit `0` + the pipeline's internal `row_match`, **not** a vintage-specific row literal.

**Idempotency model:** No PostgreSQL ledger guards this pipeline. Safety is `mode=overwrite` + clean-publish (delete-prefix → upload-ordered → manifest-last → size-census verify → read-back). **Every phase is safe to re-run** — a retry reproduces an **equivalent** dataset (identical rows / types / indices; the `ingested_at` provenance column is per-run wall-clock and is the only intended difference) and self-heals a partial failure. Blast radius is per-dataset (independent prefixes, independent publish).

---

## 0. Definition of Done (verifiable)

- [ ] R2 SoR carries **11** `form5500_*` prefixes under `s3://data-sink/active/` (7 prior + 4 net-new; `sch_a_broker` overwritten in place, not added).
- [ ] Local lake `~/core-x-lake/active/` carries **11** `form5500_*.lance` dirs.
- [ ] Ingest exit `0` and report `✅ PASS` for all 5 published datasets (row-count match, `ACK_ID IS NULL` = 0, leading-zero proof).
- [ ] `form5500_sch_a_carrier` carries 4 BTREE (`ACK_ID, FORM_ID, SCH_A_EIN, SCH_A_PLAN_NUM`); `form5500_sch_a_broker` carries `BTREE(ACK_ID, FORM_ID)` — the composite carrier↔broker join is BTREE on **both** sides; the three Sch C tables carry `BTREE(ACK_ID)`.
- [ ] `pipelines/form5500/verify_carrier_recon.py` → `RESULT: ✅ PASS` against **both** the R2 SoR and the local lake (carrier orphan ~0%, `INS_CARRIER_EIN` leading-zero > 0, broker→carrier composite resolution > 95%, index census satisfied).

---

## 1. Context an executing agent needs

| Fact | Value |
|---|---|
| Repo (operator checkout) | `/Users/benjamincrane/core-x` (must be at commit `0c69ce2` or later) |
| Pipeline entry | `pipelines/form5500/ingest_form5500.py` (uv inline-deps; `uv` auto-resolves duckdb/pylance/pyarrow/requests/boto3) |
| Verifier | `pipelines/form5500/verify_carrier_recon.py` (read-only; `--root` selects SoR or local) |
| Secrets | Doppler `core-x` / `prd` → `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` (`R2_ACCOUNT_ID` empty — pipeline uses `R2_ENDPOINT`) |
| Source (landing) | `s3://data-sink/landing/form-5500/` — 41 staged zips + `form5500_2025_files.csv` manifest |
| Target (SoR) | `s3://data-sink/active/` (default `--target`) |
| Target (local lake) | `~/core-x-lake/active/` (explicit `--target`) |

**Datasets published by this runbook (4 net-new + 1 re-indexed) — reference figures for the 2026-06-06 "Latest" vintage:**

| `--only` name | Source stem | Lance dataset | Rows | BTREE | Note |
|---|---|---|--:|---|---|
| `sch_a_carrier` | `F_SCH_A` | `form5500_sch_a_carrier` | 23,648 | `ACK_ID, FORM_ID, SCH_A_EIN, SCH_A_PLAN_NUM` | net-new (carrier identity) |
| `sch_a_broker` | `F_SCH_A_PART1` | `form5500_sch_a_broker` | 34,358 | `ACK_ID, FORM_ID` | **re-publish** — backfills FORM_ID index |
| `sch_c_indirect` | `F_SCH_C_PART1_ITEM3` | `form5500_sch_c_indirect` | 2,656 | `ACK_ID` | net-new (indirect comp by payor) |
| `sch_c_eligible` | `F_SCH_C_PART1_ITEM1` | `form5500_sch_c_eligible` | 1,611 | `ACK_ID` | net-new (eligible-indirect providers) |
| `sch_c_terminated` | `F_SCH_C_PART3` | `form5500_sch_c_terminated` | 81 | `ACK_ID` | net-new (terminated accountants) |

> Row counts are the **reference** for this vintage. The binding gate is the pipeline's internal `row_match` (landed == parsed-from-CSV), not the absolute number — a re-staged weekly file shifts counts but `row_match` must still hold.

---

## 2. Preflight gates (run before any mutation)

```bash
cd /Users/benjamincrane/core-x

# 2.1 Patch + verifier present on disk
git log -1 --oneline                                         # expect 0c69ce2 or later
grep -c "sch_a_carrier" pipelines/form5500/ingest_form5500.py     # expect >= 1
test -f pipelines/form5500/verify_carrier_recon.py && echo "verifier present"

# 2.2 Doppler authenticated + R2 creds resolvable
doppler run --project core-x --config prd -- sh -c \
  'echo EP=${R2_ENDPOINT:+set} AK=${R2_ACCESS_KEY_ID:+set} SK=${R2_SECRET_ACCESS_KEY:+set}'
#   expect: EP=set AK=set SK=set

# 2.3 Landing zone reachable + the 5 source zips physically staged
doppler run --project core-x --config prd -- sh -c '
  export AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY AWS_EC2_METADATA_DISABLED=true
  aws s3 ls s3://data-sink/landing/form-5500/ --endpoint-url "$R2_ENDPOINT" --region auto \
    | grep -E "F_SCH_A_2025|F_SCH_A_PART1_2025|F_SCH_C_PART1_ITEM1_2025|F_SCH_C_PART1_ITEM3_2025|F_SCH_C_PART3_2025"'
#   expect 5 lines
```

**Gate:** all three pass. If 2.1 fails → checkout stale: `git -C /Users/benjamincrane/core-x fetch && git -C /Users/benjamincrane/core-x pull --ff-only origin main`. If 2.3 returns fewer than 5 → landing zone incomplete; stop and re-stage (do not invent data).

---

## 3. Phase A — Publish to R2 SoR (canonical)

```bash
cd /Users/benjamincrane/core-x
doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py \
  --only sch_a_carrier,sch_a_broker,sch_c_indirect,sch_c_eligible,sch_c_terminated
```

Per dataset: EBSA layout fetch → R2 landing zip read → all-string parse → DuckDB typed projection → local Lance stage → BTREE build → **publish to `s3://data-sink/active/form5500_<name>/`** (delete-prefix → upload data/index/txn → `_versions/` manifest LAST → size-census verify → read-back).

**Expected tail (reference — 2026-06-06 vintage):**
```
**Result:** ✅ PASS — 5 datasets · 62,354 rows · 9 BTREE indexes · …
```

**Gate:** process exit `0` **and** the report header begins `**Result:** ✅ PASS`. The `62,354` / `9 BTREE` figures are the reference vintage; the binding condition is the pipeline's per-dataset `row_match`, which holds across re-stages even when absolute counts move. If exit `1` → §6.

**Confirm SoR state:**
```bash
doppler run --project core-x --config prd -- sh -c '
  export AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY AWS_EC2_METADATA_DISABLED=true
  aws s3 ls s3://data-sink/active/ --endpoint-url "$R2_ENDPOINT" --region auto | grep -c form5500'
#   expect: 11   (sch_a_broker overwrites in place — re-publish does not add a prefix)
```

---

## 4. Phase B — Refresh local lake (operator working plane)

```bash
cd /Users/benjamincrane/core-x
doppler run --project core-x --config prd -- uv run pipelines/form5500/ingest_form5500.py \
  --target ~/core-x-lake/active \
  --only sch_a_carrier,sch_a_broker,sch_c_indirect,sch_c_eligible,sch_c_terminated
```

**Gate:** exit `0`, report `✅ PASS`.

**Confirm:** `ls -d ~/core-x-lake/active/form5500_*.lance | wc -l` → expect **11**.

---

## 5. Phase C — Relational + index verification (committed, read-only)

The ingest proves intra-table integrity. The committed verifier proves the **cross-table joins** the patch unlocks AND the index census on both sides of the composite join. Run against both planes:

```bash
cd /Users/benjamincrane/core-x
# R2 SoR:
doppler run --project core-x --config prd -- uv run pipelines/form5500/verify_carrier_recon.py
# Local lake:
doppler run --project core-x --config prd -- uv run pipelines/form5500/verify_carrier_recon.py --root ~/core-x-lake/active
```

**Expected (reference):** carrier 23,648 · orphan 0 (0.00%) · `INS_CARRIER_EIN` leading-zero ≈ 2,823 · broker→carrier ≈ 100% · carrier BTREE ⊇ {ACK_ID, FORM_ID, SCH_A_EIN, SCH_A_PLAN_NUM} · broker BTREE ⊇ {ACK_ID, FORM_ID}.

**Gate:** `RESULT: ✅ PASS` and exit `0` on **both** roots. The orphan ceiling is `<1%` (empirically ~0 — Schedule A binds to F_5500, not 5500-SF; the `∪ sf` term is a harmless superset guard against a torn `main` slice).

---

## 6. Failure modes & recovery

| Symptom | Cause | Action |
|---|---|---|
| `targeted stems absent from manifest` | landing manifest re-staged without a stem | Re-confirm §2.3; manifest must list all 5 `data_file`s. Do not edit `STEMS` to drop one. |
| Layout fetch error / non-200 | EBSA transient or URL drift | Re-run (idempotent). Confirm the `secondary_url` resolves with a redirect-following GET. |
| `publish verify failed … mismatched` | R2 multipart/size-census mismatch mid-upload | Re-run the **same** `--only`; clean-publish deletes the partial prefix and re-uploads. |
| Report `❌ FAIL` on `row_match` | parsed != landed | Do **not** publish over it; inspect the source CSV for embedded newlines/encoding. Re-run; if reproducible, capture the stem and stop. |
| `verify_carrier_recon.py` FAIL on `broker_index` | `sch_a_broker` not re-published (omitted from `--only`) | Re-run Phase A/B with `sch_a_broker` in the list — the FORM_ID index is created at ingest time. |
| `verify_carrier_recon.py` FAIL on `orphan<max` | carrier published against a different vintage than `main`/`sf` | Re-stage `main`/`sf`/carrier from one snapshot; orphan must be ~0. |
| Exit `1` overall | any per-dataset FAIL | Fix the offending stem, re-run only that `--only` name — others are already published and untouched. |

**Re-run granularity:** every command is restartable at the `--only` level.

---

## 7. Rollback

Net-new datasets are removed by delete-prefix. `sch_a_broker` is **not** net-new — to revert it, re-publish from the pre-patch registry (it returns to `BTREE(ACK_ID)` only); a bare prefix delete would strand the broker dataset, so do **not** delete the broker prefix.

```bash
# Remove a NET-NEW dataset from R2 SoR (carrier / indirect / eligible / terminated only):
doppler run --project core-x --config prd -- sh -c '
  export AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY AWS_EC2_METADATA_DISABLED=true
  aws s3 rm s3://data-sink/active/form5500_sch_a_carrier/ --recursive --endpoint-url "$R2_ENDPOINT" --region auto'
# Local lake: remove ~/core-x-lake/active/form5500_sch_a_carrier.lance (operator runs the delete).
```

---

## 8. Post-ingest structural diagnostic (covers all 11)

`pipelines/form5500/diagnose_post_ingest.py` now declares all 11 datasets (the 4 new + the broker `FORM_ID` index expectation). Running it yields a full structural / type / index / storage-health census of the Form 5500 plane:

```bash
doppler run --project core-x --config prd -- uv run pipelines/form5500/diagnose_post_ingest.py
```

> It also re-runs the NPPES/CMS cross-graph probe (Phase 3 — heavy: scans ~1.9M NPPES org rows). Run it for a full census; the binding verification gate for **this** materialization is §3 (ingest PASS) + §5 (`verify_carrier_recon.py`), which do not require the cross-graph scan.

---

## 9. Out of scope (backlog)

- **Tier B orphans** (`F_SCH_R_PART1` contributing employers, `F_SCH_MEP_PART2` participating employers, the H/I/SF transfer tables) — next cycle, same `STEMS` + `FORCE_STRING` pattern.
- **Tier C** actuarial long tables + `F_SCH_DCG` (no layout in manifest) — deferred, low commercial signal.
- **`ops.form5500_runs` ledger** — the pipeline is ledgerless (correctness is `mode=overwrite`; a ledger would add observability only: run_id, finished-at, manifest vintage, row counts). The fleet (`ops.*_runs`) gates ingests on a ledger; wiring one here closes the "did this run, and against which vintage?" gap. Strategic, not required for correctness.
- **Downstream TiC triage join** (`INS_CARRIER_EIN` / `INS_CARRIER_NAIC_CODE` → carrier identity) — consumes this output; separate work item.
