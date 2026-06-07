# cigna_tic — Transparency-in-Coverage ToC stream-parse

Memory-bounded extraction of Cigna's Transparency-in-Coverage **Table-of-Contents (index)**
files, intersected against the local Form 5500 sponsor-EIN universe to isolate the
in-network-rates files attributable to a target set of employers.

## What a Cigna ToC index is

A single JSON object whose only large member is `reporting_structure[]`. Each element binds
one or more `reporting_plans` (which carry `plan_id` + `plan_id_type`) to a set of
`in_network_files[].location` URLs — the absolute, signed links to that structure's
`in-network-rates.json.gz`. The provider **NPIs live inside those rate files, not in the
index**; the index is purely the routing table.

```
reporting_structure[]
├─ reporting_plans[]      { plan_name, plan_id_type: "ein"|"hios", plan_id, plan_market_type }
├─ in_network_files[]     { description, location, file_size? }   ← may be null
└─ allowed_amount_file    { description, location }
```

The intersect key is `plan_id` **when `plan_id_type == "ein"`** (group market): that is the
plan **sponsor's** EIN, the same identifier space as Form 5500 `SPONS_DFE_EIN`.

## Two load-bearing facts (both verified against the real bytes)

1. **EIN format is inconsistent — within a single file.** The national index stores some
   EINs **hyphenated** (the issuer EIN `59-1031071`, ×7,400) and others **raw**
   (employer EINs like `911325671`); the Colorado file is **entirely hyphenated**. Form 5500
   stores 9 raw digits with EFAST2 leading zeros (`591031071`). Both sides are normalized to
   digits-only / 9-wide. Skip this and any hyphenated entry silently misses while the run
   looks complete, and the issuer EIN double-counts.
2. **Issuer vs. employer keying is per-file, not universal.** A state/fully-insured index
   (e.g. the Colorado file) stamps Cigna's **own issuer EIN** on every group structure → it
   is *not* employer-identifiable. The national CHLIC index is dominated by **distinct
   employer-sponsor EINs** (the self-funded/ASO book) → it *is* intersectable. Always
   measure `distinct_sponsor_eins` before trusting an intersect.

## Scripts

| Script | Role |
|---|---|
| `load_employer_eins.py` | Top-N high-volume employer EINs from local `form5500_main` (ranked by `TOT_ACTIVE_PARTCP_CNT`), normalized 9-wide. |
| `stream_index.py` | Streaming ToC parser (ijson, never `json.load`). Emits structure/EIN/rate-file distribution, the EIN intersect, a JSONL queue of high-conviction links, and RSS/throughput telemetry. Non-zero exit if the RSS cap is blown. |

## Run

```bash
# 1. employer EIN universe (local Form 5500, no R2 round-trip)
uv run pipelines/cigna_tic/load_employer_eins.py --out /tmp/employer_eins.txt -n 50

# 2. stream the index, intersect, emit queue + telemetry
uv run pipelines/cigna_tic/stream_index.py \
  --index <toc_index.json> \
  --eins  /tmp/employer_eins.txt \
  --queue /tmp/high_conviction.jsonl \
  --telemetry /tmp/telemetry.json
```

Memory is bounded by the per-structure object plus the accumulator sets, **independent of
file size** — the 72 MB national index parses in ~0.2 s at ~24 MB peak RSS (cap: 256 MB).

## Source assets

Inputs are operator drops in `~/Downloads` for this run; the canonical landing namespace is
`s3://data-sink/landing/cigna/tic` (R2, not a local mount). Form 5500 is the local lake at
`/Users/benjamincrane/core-x-lake/active/form5500_main.lance`.
