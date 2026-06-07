# Cigna TiC ToC — Local Stream-Parse Analysis

**Scope:** memory-bounded streaming parse of Cigna's Transparency-in-Coverage Table-of-Contents
(index) files, intersected against the local Form 5500 sponsor-EIN universe to isolate
employer-attributable in-network-rates files. All numbers below are **measured**, not
estimated, against the real local assets.

**Run plane:** local. Cigna assets in `~/Downloads`; Form 5500 = local lake
`/Users/benjamincrane/core-x-lake/active/form5500_main.lance`. ijson backend: `yajl2_c`.

---

## 0. Directive ⇄ plane reconciliation

| Directive premise | Reality | Disposition |
|---|---|---|
| Source dir `/data-sink/landing/cigna/tic` | No such local path — `data-sink` is the **R2** SoR namespace, not a mount. Assets were operator drops in `~/Downloads`. | Read from `~/Downloads`; canonical R2 landing noted in README. |
| Implement in `gtm-mcp`, **TypeScript** + `JSONStream`/`oboe.js` | `apps/gtm_mcp` is a **Python** query service. The data plane is Python/DuckDB/Lance. | Implemented as a Python `uv`-run streaming pipeline under `pipelines/cigna_tic/` (extraction is a pipeline concern, not an MCP query tool). |
| "Query the local Form 5500 database table" | Correct — local Lance lake `form5500_main` (19,114 large-plan filings). | Ranked top-50 by `TOT_ACTIVE_PARTCP_CNT`. |
| Index `plan_id` EIN = employer identity | **True only for the national/self-funded book.** State/fully-insured indexes stamp Cigna's own issuer EIN. | Quantified below (§3.2). |
| 72.1 MB "national" file | 72,077,559 B (68.7 MiB) — one legal entity (**CHLIC**), not all of Cigna. | Treated as the CHLIC entity index. |

---

## 1. Inputs

**Employer EIN universe** — top-50 sponsors by active-participant count from `form5500_main`,
normalized to 9 digits. Head:

| EIN | Active participants | Sponsor |
|---|--:|---|
| 911325671 | 258,086 | STARBUCKS CORPORATION |
| 942404110 | 101,080 | APPLE INC. |
| 340963169 | 71,178 | THE PROGRESSIVE CORPORATION |
| 430724835 | 56,587 | ENTERPRISE HOLDINGS, INC. |
| 232259884 | 55,318 | VERIZON COMMUNICATIONS INC. |
| 542185193 | 54,686 | ORACLE CORPORATION |

**Cigna assets:** `2026-06-01_…CHLIC…_index.json` (national, 68.7 MiB) and
`2026-01-01_CO_…CHLIC…_index.json` (Colorado sandbox, 56.8 KiB).

---

## 2. The Cigna node schema — where an EIN links to a rate file

Verified against the real bytes. The sponsor EIN and the rate-file URL are **siblings**
inside a single `reporting_structure` element:

```jsonc
{
  "reporting_structure": [
    {
      "reporting_plans": [
        {
          "plan_name": "OAP",
          "plan_id_type": "ein",          // ← "ein" (group) | "hios" (individual)
          "plan_id": "911325671",         // ← sponsor EIN — Starbucks, stored RAW here…
          "plan_market_type": "group"     //    …but the issuer EIN is stored "59-1031071"
        }                                 //    HYPHENATED in the same file (mixed format).
      ],
      "in_network_files": [               // ← may be null (724 such structures nationally)
        {
          "description": "in-network file",
          "location": "https://d25kgz5rikkq4n.cloudfront.net/.../national-oap_in-network-rates.json.gz?Expires=…&Signature=…",
          "file_size": null               // ← omitted in the national index (declared only in the state file)
        }
      ],
      "allowed_amount_file": { "description": "allowed amount file", "location": "https://…" }
    }
  ]
}
```

Extraction path: `reporting_structure[] ▸ reporting_plans[] ▸ {plan_id_type=="ein"} ▸ plan_id`
→ normalize (strip non-digits, 9-wide) → intersect with Form 5500 `SPONS_DFE_EIN`; on a hit,
emit sibling `reporting_structure[] ▸ in_network_files[] ▸ location`.

**Normalization is load-bearing — the field is mixed-format _within one file_.** Verified
counts in the national index: issuer `59-1031071` appears **hyphenated 7,400×** (raw 1×),
while employer EINs (Starbucks `911325671`, …) are stored **raw**. The Colorado file is
**entirely hyphenated** (`59-1031071` ×24, raw 0×). Consequences: a raw-vs-raw match on the
national employer book happens to coincide, but any file with hyphenated entries (the CO
file, every issuer-stamped national structure) returns **0 without digits-only
normalization**, and the issuer EIN would be miscounted as two distinct EINs. Normalize both
sides — never assume a format.

---

## 3. Telemetry

### 3.1 Parser performance — national index (68.7 MiB)

| Metric | Value |
|---|--:|
| Wall-clock | **211.2 ms** |
| Throughput | **325.5 MiB/s** |
| Peak RSS (OS, `ru_maxrss`) | **24.3 MB** |
| RSS cap | 256 MB — **within (9.5 % of budget)** |
| Reporting structures | 29,216 |
| Group (`ein`) plans / Individual (`hios`) | 28,978 / 238 |
| Structures with `in_network_files == null` | 724 |

Peak RSS (24.3 MB) is **one-third of the file size** — direct proof the file is never
materialized; memory tracks the per-structure object + accumulator sets, not the input.

### 3.2 EIN identity — issuer vs. employer

| Metric | National (CHLIC) | CO sandbox |
|---|--:|--:|
| Distinct sponsor EINs | **18,362** | **1** |
| Top EIN | `59-1031071` (Cigna issuer) | `59-1031071` (Cigna issuer) |
| Top-EIN share of references | 25.5 % (7,401 structures) | 100 % |
| Sentinels | `000000000`×189, `999999999`×19 | — |

**Interpretation.** The national index is a three-way mix: ~25.5 % issuer-stamped
(fully-insured, *not* employer-identifiable), ~0.7 % sentinel/placeholder, and the remaining
~74 % spread across 18,358 distinct **employer-sponsor** EINs (self-funded/ASO) — the
intersectable surface. The Colorado file is **entirely** issuer-stamped; using a state file
as the intersect sandbox would have falsely shown the technique yielding zero employers.

### 3.3 Intersect — 50 employers × national index

| Metric | Value |
|---|--:|
| Employer EINs loaded | 50 |
| Distinct employer EINs matched | **8** |
| High-conviction links emitted | 103 |
| Distinct rate files behind the hits | 54 |

Matched employers (Form 5500 sponsor ⇄ Cigna network product):

| EIN | Employer | Cigna plan |
|---|---|---|
| 23-2259884 | Verizon Communications | GPPO |
| 25-0542520 | Kraft Heinz Foods | OAP |
| 34-0963169 | The Progressive Corp. | OAP |
| 36-3051915 | AON Corporation | Local Plus |
| 47-2124505 | Berkshire Hathaway Automotive | OAP |
| 56-0751071 | Boddie-Noell Enterprises | OAP |
| 76-0509980 | Charles River Laboratories | OAP |
| 91-1325671 | Starbucks Corporation | OAP |

---

## 4. Scale forecast — downstream files to capture every NPI

NPIs live in the `in-network-rates` files, not the index. The forecast is therefore the
count of **distinct physical rate files**, not structures.

| Quantity (CHLIC entity) | Value |
|---|--:|
| In-network references (raw) | 141,865 |
| **Distinct rate files** | **132** |
| Employer→file fan-in | **≈ 1,075 : 1** |

**The dominant architectural fact:** Cigna does not publish per-employer rate files. 29,216
employer structures fan into **132 shared files** organized by **network product × geography**
— `national-oap`, `localplus`, `{city}-gppo` (chicago/detroit/cleveland/metro-ny…),
`{state}-hmo` (alabama/arizona/florida…). To capture **every NPI in CHLIC's commercial
network** for `practice_group_360`, stream **132 files**, not 29,216 — a 221× reduction
versus a naïve per-structure crawl.

**Caveats (all measured):**
- **No declared sizes.** The national index omits `file_size` on all 132 files (the state
  file declares them: 5.94–17.17 MiB per state slice). A firm byte total requires 132 HEAD
  requests against the signed URLs — out of scope for a local-only run; not fabricated here.
- **Cross-host.** 127 of the 132 are on `d25kgz5rikkq4n.cloudfront.net`; the other **5 are
  affiliate/rental-network MRFs on 4 external host domains** — Priority Health
  (`…s3.amazonaws.com`), MVP via `mrf.healthsparq.com`, Sagamore via
  `myportal.cignapayersolutions.com`, and Upper-Midwest-Affiliation-Partner (×2). NPI capture
  crosses host boundaries.
- **Per-entity.** 132 is for the **CHLIC** legal entity only. Total Cigna footprint = Σ over
  all Cigna reporting-entity indexes; this run covers one.

---

## 5. Artifacts

- `pipelines/cigna_tic/stream_index.py` — streaming parser (telemetry + JSONL queue).
- `pipelines/cigna_tic/load_employer_eins.py` — Form 5500 employer-EIN loader.
- `pipelines/cigna_tic/README.md` — schema + run docs.
- Queue (this run): 103 high-conviction links → 54 distinct rate files for 8 employers.

## 6. Verification

Every headline number was re-derived by an **independent tool**, not the parser:
`jq` (structure counts 29,216 / 28,978 / 238 / 724; distinct EINs 18,362; issuer share
25.54 %; sentinels 189/19; 141,865 → 132 distinct files; 5 external-host files on 4
domains), `grep` (the
8/50 employer matches, plus the per-file EIN-format census above), and `/usr/bin/time -l`
(peak RSS 24.4 MB vs the parser's psutil 24.3 MB — corroborating the streaming claim
independently). All four verdicts matched.
