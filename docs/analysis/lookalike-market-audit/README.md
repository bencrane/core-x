# lookalike-market-audit — runbook

Reproducible harness for the 2026-07-21 audit of `sub_dossier_v1.sql_market()` (the
"lookalike buyer market"). Each script prints the exact numbers the verdict cites. The
written verdict is delivered separately (agent transcript); this directory is the machine
that regenerates its evidence.

## Prereqs
- Doppler access to `core-x` / config `prd` (provides `QUERY_SIDECAR_TOKEN`).
- Python 3 (stdlib only; scripts shell out to `curl`).
- Run from the repo root `/Users/benjamincrane/core-x`.

## Pins (determinism)
- **Artifact:** `lib_sidecar.PIN_ARTIFACT = query-sidecar/query_sidecar_20260721T020734Z.duckdb`.
  Raw-SQL scripts pass `require_artifact`; if it has rolled they raise `ArtifactRolled` — check
  `curl -s https://query-sidecar-api.onrender.com/healthz`, re-pin, and expect legitimately
  different numbers.
- **Sample:** `lib_sidecar.SAMPLE` (8 hard-coded UEIs; no run-time re-sampling). Regenerate the
  pool with `sql/00_sample_selection.sql` if you want a different set, then edit `SAMPLE`.
- **`current_date`:** the DEFAULT (gate-OFF) market count, Tier 1, and Tier 2 have no
  `current_date` term — date-stable per artifact. Only `require_subout=ON` totals drift with the
  run date (`sql_market` line 372). Scripts `02`/`03` call the live engine (reads current
  `/healthz`); run them while the pin is current for exact reproduction.

## Run
```bash
cd /Users/benjamincrane/core-x
doppler run -p core-x -c prd -- python3 docs/analysis/lookalike-market-audit/01_universe_and_caps.py
doppler run -p core-x -c prd -- python3 docs/analysis/lookalike-market-audit/02_reconstruct_total.py
doppler run -p core-x -c prd -- python3 docs/analysis/lookalike-market-audit/03_monotonicity_dialed_caps.py
doppler run -p core-x -c prd -- python3 docs/analysis/lookalike-market-audit/04_tiers.py
doppler run -p core-x -c prd -- python3 docs/analysis/lookalike-market-audit/05_lookalike_validity.py
```

## Expected output (against the pinned artifact)

**01** — universe & caps
```
firms with prime_obl_60mo >= $10M : 14482      # < 25000 => cap cannot bite at default dials
distinct primes in signature      : 194043     # floor=0 ceiling >> 25000
limit 25000->25000, 50000->50000, 60000->50000 (silent cap only above the 50k max)
25k-element IN-list executes OK
```

**02** — `market.total` reconstruction (engine == my_total for all 8)
```
CARAHSOFT   c_raw 9112  engine 7260  my_total 7260  OK   drops 23/578/1251
GLENAIR     c_raw 6881  engine 5481  my_total 5481  OK   drops 0/304/1096
... 8/8 OK
```

**03** — dialed caps + monotonicity (Carahsoft)
```
floor 1e7  mh2 -> c_raw 9112
floor 1e6  mh2 -> c_raw 19258
floor 0    mh2 -> c_raw 54827   (>25k, engine total truncates)
floor 0    mh1 -> c_raw 101477  (>25k)
engine floor0/mh1: OFF total=22449 total_capped=True ; ON total=1392 ; monotonicity HOLDS on a CAPPED number
```

**04** — tiers (rep-level; naive nesting False, unified nesting True)
```
CARAHSOFT  Tier0 7260  T1_pairs5y 1203  T1_cube 1612  T2_cube 1421  T2<=T1_pairs? False  T2<=T1_cube<=T0? True
GLENAIR    Tier0 5481  T1_pairs5y 1051  T1_cube 1372  T2_cube 1206  ...            False  ...              True
```

**05** — lookalike validity
```
A. Carahsoft & Glenair top-10 IDENTICAL (10/10 shared), all top_naics 541330, all wt-tied
B. min_lane_hits 2->3 : count -2703, top50_kept 0/50 (jaccard 0.00) ; sig_rank/share : jaccard 1.00
C. wt-top50 vs cosine-top50 overlap : 0/50
D. 299 candidates with obl60>=$1B all rank 3890-7293 / 9112 in wt
```

## How another agent diffs a re-run
1. Confirm `/healthz` artifact == `PIN_ARTIFACT`. If not, re-pin and note drift.
2. Run each script; compare the printed integers to the block above.
3. Any mismatch while the pin is current is a real change in the plane (or a code change in
   `sub_dossier_v1.py`) — bisect against `git log -- apps/catalyst_api/src/routers/sub_dossier_v1.py`.

## File map
- `lib_sidecar.py` — pins, sample, `q()` executor, the gate-OFF candidate query builder.
- `01`–`05_*.py` — one investigation each (see docstrings).
- `sql/*.sql` — verbatim load-bearing queries (`00` sample, `10` ceiling, `20` candidate
  universe, `40` tiers).
