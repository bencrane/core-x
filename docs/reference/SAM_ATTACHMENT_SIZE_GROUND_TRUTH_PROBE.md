# SAM.gov Attachment Size — Ground-Truth Content-Length Probe

**Run:** 2026-06-08 00:42:14 UTC · seed=42 · concurrency=6 · sample requested=1000 · sampled=1000  
**Manifest:** `s3://data-sink/active/sam_opps_attachment_manifest_90day_winners/` (155,183 rows total)  
**Method:** streamed `Range: bytes=0-0` (≤1 byte, body never read; HEAD validated as 0%-effective on this endpoint and skipped). Read-only; SoR untouched.

## 0. Premise reconciliation (read first)

This probe was commissioned to show "how badly `size_bytes` underreports." The repo's own adversarial review (`SAM_GOVCON_SUBSTRATE_AGENT_NOTES_ADVERSARIAL_REVIEW.md` §C9, #323) had already **disproven** the inherited mod-10 MB corruption claim on an n=2 spot-check. This run is hypothesis-neutral and upgrades that check to n=998 live measurements. The decisive result is the corruption adjudication in §3, not a presupposed drift direction.

## 1. Hit rate

- **998 / 1,000 (99.8%)** URLs returned a usable true byte size.
- By method (which layer answered):
  - `range`: 848
  - `link`: 150
  - `fail`: 2

## 2. Metadata drift (declared `size_bytes` vs true Content-Length)

- **Exact match** (declared == true, among 848 rows with declared > 0): **848 (100.0%)**.
- Drift `(true−declared)/declared` over declared>0 rows — median **0.00%**, mean **0.00%**, p95 **0.00%**.
- A median at/near 0% with high exact-match means `size_bytes` is faithful where it is non-zero; the storage risk is concentrated in the declared-zero set (§4).

## 3. Corruption adjudication — the decisive test

The `((true−1) mod 10 MB)+1` fold is only observable when the TRUE size ≥ 10 MB. Among the **450** probed rows with true size ≥ 10 MB:

| outcome | count | meaning |
|---|---:|---|
| declared == true | 450 | **uncorrupted** — `size_bytes` is exact |
| declared == mod-10 MB fold | 0 | corruption present |
| neither | 0 | other drift (investigate) |

Verdict: **UNCORRUPTED** — confirms the adversarial review at scale.

## 4. Declared-zero set — resolved as link attachments, not hidden bytes

- Probed declared-zero (size_bytes = 0/NULL) rows: **150**.
- **150 (100.0%) are link-type attachments** — the endpoint returns HTTP 400 `"Download not available for links"`. They have no file body and consume **zero Stage-3 storage**, so `size_bytes = 0` is *correct*, not underreported.
- Declared-zero rows that turned out to be non-empty files: **0** (mean **0.00 MB**).
- Net: the ~24.5 K declared-zero population (stratum C) is the *non-file link* set, not a hidden-bytes risk. The earlier hypothesis that C masks real storage is rejected by measurement.

## 5. Ground-truth storage projection (Stage-3 footprint)

Stratified, population-weighted over the **public, downloadable** population (152,146 rows; non-public are access-gated and not Stage-3-fetchable). Naive `sample_mean × N` is shown only to expose the bias the deliberate ≥10 MB oversample would have introduced.

| stratum | pop (public) | sampled hits | mean true size | stratum bytes |
|---|---:|---:|---:|---:|
| A (≥10 MB) | 4,391 | 450 | 30.06 MB | 0.1320 TB |
| B (0–10 MB) | 123,185 | 398 | 0.66 MB | 0.0809 TB |
| C (declared 0) | 24,570 | 150 | 0.00 MB | 0.0000 TB |

- **Stratified projection (public): 0.213 TB (0.194 TiB), 95% CI ±0.020 TB.**
- Declared baseline (trusting `size_bytes`, public sum): **0.212 TB** — the gap to the stratified projection is the storage impact of the metadata drift (chiefly §4).
- Naive `sample_mean × N` (BIASED, do not use): 2.102 TB — 9.87× the corrected figure.

## 6. Method & integrity notes

- Concurrency 6, pace 0.1s (directive's ceiling was 50, but the SAM WAF blocks from the first ~100 requests at concurrency ≥24 — the proven-safe residential envelope is ~8 req/s, matching the harvest's single-threaded 0.12s pace). WAF blocks (429/403) this run: 0; aborted early: False.
- The directive prescribed HEAD + Content-Length. HEAD was validated across two canaries as **0%-effective on this endpoint** (the `.../download` route does not return Content-Length on HEAD; the prior verified-true check also used Range, not HEAD). The probe therefore uses a streamed 1-byte Range GET — the method that actually returns ground truth. No file body was ever read.
- An earlier full pass at the directive's concurrency was **WAF-blocked from the first ~100 requests (848 × 429/403)** and the circuit breaker aborted it to protect the residential IP; this clean pass at concurrency 6 + 0.1s pace took 0 blocks. The directive's "max 50 concurrent" is unsafe for this WAF.
- 2 / 1,000 rows failed (stratum B, HTTP 400) — 0.2%, immaterial to every figure above.
- Sampling is deterministic (md5(resource_id‖seed)); re-running with the same seed reproduces the exact URL set. Raw per-URL results (ephemeral): `/tmp/sam_size_probe_raw.jsonl` — regenerable via `scripts/archive/sam_attachment_size_probe.py probe --seed 42`.
