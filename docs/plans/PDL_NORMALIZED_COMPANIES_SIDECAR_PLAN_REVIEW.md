# `pdl_normalized_companies` — Sidecar Build Plan, Adversarial Design Review

Reviewer stance: hostile. Goal — break the plan before a data-plane write lands: correctness bugs,
OOM, idempotency holes, false engine-parity assumptions, edge cases. Plan under review:
[`docs/plans/PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md`](PDL_NORMALIZED_COMPANIES_SIDECAR_PLAN.md).

Every load-bearing number below is a **live read-only probe** of `s3://data-sink/active/pdl_companies/`
(Lance **v11**, **35,446,771 rows**) on 2026-06-06 via Doppler `core-x/prd` (pylance 7 / pyarrow 24 /
duckdb 1.5 / canonical `core.name_norm` imported by path). **Zero mutation** — no `write_dataset`, no
`create_*index`, no DDL, no R2 write. The `core.web_norm` builders were run **verbatim as written in
§5.2** against synthetic edge cases and live PDL `linkedin_url`/`domain`/`company_name` samples.

---

## Verdict

**SHIP-WITH-AMENDMENTS.** The architecture is right (sidecar-not-inline survives re-litigation — §A),
the SoR-immutability reasoning is correct, the `name_norm` engine-parity fear is empirically a
non-issue (DuckDB-write vs DataFusion-read produce byte-identical output — §W1), and the memory
envelope holds with real headroom. But three defects must be fixed before the build: **(1)** the
`linkedin_slug` builder lower()s the *output* not the *input*, so any uppercase/mixed-case host yields
NULL — a latent footgun for a fleet-wide substrate even though it is empirically 0-impact on the
*current* PDL snapshot; **(2)** the idempotency/parity gate hardcodes `== 35,446,771`, which is a
constant, not a parity check, and silently passes on the wrong snapshot; **(3)** shipping
`normalized_domain` with the webmail/social/platform blocklist omitted and *no* `is_generic_domain`
flag pushes a quantified ~1.46%-of-domains many-to-many false-join bomb onto every consumer.

---

## Blocking defects (must-fix before build)

### B1 — `linkedin_slug` lowercases the OUTPUT, not the INPUT → uppercase/mixed-case host returns NULL — **Blocking (latent)**

**What breaks.** §5.2 builder:
```python
"nullif(lower(regexp_extract(CAST(" + expr + " AS VARCHAR),"
" 'linkedin\\.com/(?:company|school)/([^/?#]+)', 1)), '')"
```
`lower()` wraps the *extracted slug*. The regex `linkedin\.com/(?:company|school)/…` is matched against
the **raw, un-lowered** input. So `LINKEDIN.COM` / `LinkedIn.com` / a `/COMPANY/` path never matches →
`regexp_extract` returns `''` → `nullif → NULL`. The whole highest-precision blocking key silently
drops for any row whose URL isn't already lowercase.

**Empirical evidence (synthetic, builder run verbatim):**
```
https://www.linkedin.com/company/Foo-Bar/    -> 'foo-bar'     (OK)
https://WWW.LINKEDIN.COM/COMPANY/Foo-Bar/    -> None          <-- BUG (should be 'foo-bar')
https://www.LinkedIn.com/company/Acme        -> None          <-- BUG (should be 'acme')
LINKEDIN.COM/company/Bar                      -> None          <-- BUG (should be 'bar')
```

**Blast radius on the current PDL snapshot: ZERO.** Two live samples (500k fragment-0 +
98k mid-stream offset) show PDL's `linkedin_url` is **100% canonical lowercase** `linkedin.com/company|school/…`:
`any_uppercase = 0`, `ci_hit_cs_miss = 0` in both. The bug does not bite PDL today.

**Why it is still blocking, not cosmetic.** §5.2 declares `core.web_norm` "THE canonical web-identity
rule … the substrate owns this rule; existing copies reconcile to it (§10)." It is a **fleet-wide
shared builder**. The SAM and GLEIF spines named in §10 will feed it raw, vendor-dirty
`LinkedIn.com`/`/Company/` URLs. The defect is dormant only because PDL happens to be pre-cleaned; the
*first* consumer that isn't gets silent NULL keys and a silent recall hole — the exact "rule silently
breaks joins" failure mode `core.name_norm`'s docstring exists to prevent. A canonical substrate must
be correct independent of its first caller's data hygiene.

**Fix.** Lowercase the input before matching (and the slug output is then already lower):
```python
def linkedin_slug(expr: str) -> str:
    """Bare LinkedIn company/school slug, lowercased; NULL if absent. Lowercase the
    INPUT so scheme/host/path case all fold before the anchor match."""
    return (
        "nullif(regexp_extract(lower(CAST(" + expr + " AS VARCHAR)),"
        " 'linkedin\\.com/(?:company|school)/([^/?#]+)', 1), '')"
    )
```
`lower()` now wraps the CAST input; the pattern matches case-folded text; the captured group is already
lowercase so the outer `lower()` is redundant and removed. Add an uppercase-host fixture to Gate 1:
`https://WWW.LINKEDIN.COM/COMPANY/Foo-Bar/` → `foo-bar`.

---

### B2 — The 1:1 parity gate hardcodes `== 35,446,771` — a constant, not a parity check — **Blocking**

**What breaks.** §9 + Gate 3 assert the committed sidecar `count_rows()` and `COUNT(DISTINCT
pdl_company_id)` both equal the literal `35,446,771`. This is the row count of **snapshot v11**, frozen
into the gate. The transform's `WHERE nullif(trim(pdl_company_id),'') IS NOT NULL` (§6) silently drops
null/empty-PK rows. So the gate conflates two independent quantities and tests against a constant:

- On the **next** `pdl_companies` snapshot with a different row count (the base is manual-overwrite —
  every refresh is a new full file of arbitrary size), the gate **false-fails** a perfectly correct
  build.
- If a future snapshot carries any null/empty `pdl_company_id`, the `WHERE` drops them; the committed
  count is then `< source rows`, and a gate that compared committed-vs-source-constant would fail —
  but the *correct* check (committed == source rows that survive the same filter) would pass. The
  hardcoded gate cannot distinguish "I correctly dropped 3 empty-PK rows" from "I lost 3 rows to a
  bug."

**Empirical evidence.** On v11 the filter is a **no-op** today: `is_null = 0`, `null_after_trim = 0`,
`distinct = total = 35,446,771`. So the build is correct *now* — but the gate is right only by
coincidence of this snapshot, and is structurally a constant-equality, not a parity test.

**Fix.** Capture the source row count *under the same null/empty filter* at scan time and gate against
it — never a literal:
```python
# during the read (the scan that captures source_version):
src_rows_total = ds.count_rows()                          # full source
# after projection, BEFORE write, on the Arrow table:
con.register("proj", table)
committed = con.sql("SELECT count(*) FROM proj").fetchone()[0]
distinct  = con.sql("SELECT count(DISTINCT pdl_company_id) FROM proj").fetchone()[0]
src_valid = con.sql(  # source rows that SHOULD survive the sidecar WHERE
    "SELECT count(*) FROM src WHERE nullif(trim(pdl_company_id),'') IS NOT NULL"
).fetchone()[0]
assert committed == distinct, f"PK not 1:1: rows={committed} distinct={distinct}"
assert committed == src_valid, f"parity drift: committed={committed} src_valid={src_valid} (dropped {src_rows_total-src_valid} null-PK)"
```
Log `src_rows_total`, `src_valid`, `committed` to the ledger (the §9 schema already has
`rows_processed`/`distinct_ids` — add `source_rows`). Gate 3's prose becomes: *"committed `count_rows()`
== `COUNT(DISTINCT pdl_company_id)` == source rows surviving the null/empty-PK filter (parity against
live source, not a constant)."* Drop the literal `35,446,771` everywhere except as an *informational*
expected-value note. Gate 2's `projected rows == 35,446,771` gets the same treatment.

---

### B3 — `normalized_domain` ships with the blocklist omitted AND no `is_generic_domain` flag → a quantified many-to-many false-join bomb on every consumer — **Blocking (design)**

**What breaks.** §5.2 deliberately omits the webmail/junk blocklist that
`sam_fmcsa_domain_spine.py::_is_domain_or_null_sql` applies (`sam_fmcsa_domain_spine.py:227-235`,
`CONSUMER_BLOCK` at `:92-125`), with the rationale "normalization ≠ policy … blocklisting is a CONSUMER
concern." The *principle* is defensible — a substrate should store the true normalized host, not a
policy-censored one. **But the plan stores the bare host and provides the consumer nothing to filter
on.** Every consumer must independently re-derive and maintain a blocklist, and the *first one that
forgets* gets a cartesian explosion.

**Empirical evidence (live, ~130k normalized domains from a 12-batch mid-stream sample):** the top
shared hosts and their row counts —
```
instagram.com 819 · facebook.com 684 · indiamart.com 571 · linktr.ee 251 · linkedin.com 155 ·
youtube.com 120 · yelp.com 109 · sites.google.com 62 · twitter.com 24 · etsy.com 20 · wa.me 19 · …
```
**1.46% of all normalized domains land on a webmail/social/platform host** (1,903 of 130,033 in
sample, counting only a conservative ~22-host set). Extrapolated to the ~23M non-null-domain PDL
population, that is **~340,000 rows** whose `normalized_domain` is a shared host. A consumer joining
`spine.normalized_domain = pdl_normalized_companies.normalized_domain` without a blocklist `WHERE`
fan-outs every one of its `instagram.com` rows against all ~819-per-130k PDL `instagram.com` rows — a
silent N×M cartesian. Note `indiamart.com` (571!) is a B2B marketplace **not on standard mailbox
blocklists** — a naive consumer's hand-rolled `NOT IN ('gmail.com',…)` won't even catch it. The brief's
worry ("catastrophic many-to-many the first time a consumer forgets") is real and measured.

**Also surfaced (related, same root):** PDL's `domain` field holds social-profile URLs like
`youtube.com/@handle`, `medium.com/@cryptoinvestmentgroup` (27 `@`-domains in the 98k sample). `_bare_host`'s
`[/:?#].*$` cut correctly reduces these to `youtube.com`/`medium.com` — which then **pass**
`normalized_domain` as valid-looking domains and pile onto the shared-host buckets above. So the junk
isn't only webmail; it's every company that listed a social profile as its "website."

**Fix — store the truth AND a cheap filter the consumer cannot forget.** Do **not** null the host (the
substrate principle is right). Add a materialized boolean flag column, indexed, so the policy travels
*with* the substrate and a consumer applies it as one `WHERE` they can see in the schema:
```python
# core/web_norm.py — generic/shared-host classifier (the policy lives next to the rule it qualifies)
_GENERIC_DOMAINS = (  # webmail + social + link-in-bio + marketplace; superset of FMCSA CONSUMER_BLOCK
    "gmail.com","yahoo.com","hotmail.com","outlook.com","aol.com","icloud.com","msn.com","live.com",
    "facebook.com","web.facebook.com","instagram.com","twitter.com","x.com","youtube.com","youtu.be",
    "linkedin.com","linktr.ee","medium.com","wordpress.com","blogspot.com","sites.google.com",
    "indiamart.com","yelp.com","etsy.com","behance.net","wa.me","t.me","calendly.com","g.page",
    # … reconcile to the union of FMCSA CONSUMER_BLOCK + the live top-shared-host tail
)
def is_generic_domain(host_expr: str) -> str:
    """1 when the normalized host is a shared webmail/social/marketplace host that must
    not be a 1:1 join key; 0 otherwise. NULL-safe. Stored, BTREE/BITMAP-indexed, so every
    consumer filters `WHERE NOT is_generic_domain` instead of re-deriving a blocklist."""
    items = "(" + ",".join("'" + s.replace("'","''") + "'" for s in _GENERIC_DOMAINS) + ")"
    return f"CASE WHEN {host_expr} IS NULL THEN NULL WHEN {host_expr} IN {items} THEN true ELSE false END"
```
- Add column `is_generic_domain boolean` to §4 (BITMAP index — it is exactly the low-card seek key the
  plan claims it doesn't have; "No BITMAP" in §4 becomes wrong once this lands).
- The §10 consumer contract gains one mandatory clause: *"join `normalized_domain` only under
  `AND NOT pdl_normalized_companies.is_generic_domain` — the substrate flags shared hosts; the consumer
  MUST exclude them from 1:1 domain blocking."*
- This keeps the true host (for audit / many-to-many-aware consumers) **and** makes the safe path the
  default a consumer sees in the schema, instead of tribal knowledge. Strictly better than nulling
  (which destroys the audit value §5.2 rightly wants to preserve) and strictly better than
  consumer-only (which the evidence shows will be forgotten).

---

## Non-blocking improvements (prioritized by blast radius)

### N1 — `_bare_host` does not strip userinfo; a colon in userinfo truncates the host to garbage — **High**
`_bare_host` strips scheme/www/path but **not** `user@` userinfo, and its `[/:?#].*$` cut fires at the
first `:` — so a credentialed URL is destroyed. Evidence (synthetic):
```
user@example.com               -> host='user@example.com'  (userinfo NOT stripped)
https://bob:pw@host.com:8443/x  -> host='bob'              (truncated at the ':' in 'bob:pw')
```
The `user@example.com` case currently survives as a bogus host and *passes* `normalized_domain`
(it has a dot, alpha TLD) → pollutes the key. The `bob:pw@…` case collapses to `bob` (nulled, since no
dot — but `bob.x:pw@host.com` would survive as `bob.x`, a false domain). PDL's live `@`-domains are
social handles (handled by N3's path-cut), not userinfo, so impact on PDL is low — but the shared
substrate will see real `user@host` email-as-website values from other spines. **Fix:** strip userinfo
*before* the path/port cut:
```python
def _bare_host(expr: str) -> str:
    return (
        "trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "lower(trim(CAST(" + expr + " AS VARCHAR))),"
        " '^https?://', ''), '^[^/@]*@', ''),"          # NEW: strip leading userinfo up to '@'
        " '^www\\.', ''), '[/:?#].*$', ''), '.')"
    )
```
Order matters: strip scheme → strip userinfo → strip www → cut path/port. (`'^[^/@]*@'` only removes a
userinfo that precedes any `/`, so it cannot eat a path segment like `medium.com/@handle`.)

### N2 — Denormalized tiebreak set omits `industry`; a sector-scoped resolver is forced into an un-pushable join — **High**
§4 inlines `locality/region/country` but the §10 contract resolves SAM/GLEIF entities that carry a
NAICS/sector. A resolver that blocks on `company_name_norm` and tiebreaks on **industry** (the obvious
disambiguator for the Sherwin-Williams-style multi-UEI/multi-location fan-out the precedent SAM review
quantified at max 2,184) must hydrate `industry` from `pdl_companies` mid-resolution — the exact
index-defeating join the geo inline-rationale forbids. `industry` is 152-distinct, ~9 bytes,
present/indexed on the source; inlining it costs ~0.4 GB. `employee_size_range` (8 buckets) and
`year_founded` (int32, range-filter for as-of matching) are similarly cheap and resolution-relevant.
**Recommendation:** add `industry` (definitely) and `employee_size_range` + `year_founded` (cheap,
likely-needed) to the inline projection and the scanned-columns list. This is +0.7 GB Arrow (envelope
still fits — see N5) and removes a guaranteed downstream hydration join. Do **not** index them on the
sidecar (tiebreak-over-candidate-set, like geo) — except `year_founded` if range-scans are expected.

### N3 — Social-profile `domain` values collapse to platform hosts (covered by B3's flag, noted for completeness) — **Medium**
`youtube.com/@x`, `medium.com/@x`, `linktr.ee/x` reduce to the bare platform host and inflate the
shared-host buckets. B3's `is_generic_domain` flag resolves this — but ensure the `_GENERIC_DOMAINS`
set explicitly includes the link-in-bio / video / social platforms seen live (`linktr.ee`, `youtube.com`,
`medium.com`, `behance.net`, `t.me`, `wa.me`, `g.page`, `sites.google.com`), not just webmail.

### N4 — IDN/unicode hosts are stored un-punycoded and inconsistently survive the TLD gate — **Medium**
Evidence: `münchen.de` → `münchen.de` (survives: ASCII `.de` TLD passes `[a-z]{2,}$`); `сбербанк.рф` →
NULL (cyrillic `.рф` TLD fails `[a-z]{2,}`). So a unicode SLD with an ASCII TLD is stored raw
(un-punycoded), while a full-unicode host is dropped. Two issues: (a) inconsistency — some IDNs survive,
some don't; (b) a consumer that punycodes (`xn--…`) will not byte-match the raw-unicode stored value.
PDL impact is small (most domains are ASCII) but it is a silent cross-spine mismatch vector. **Options
(pick one, document it):** (a) accept raw-unicode storage and require all spines to store raw-unicode
too (cheapest, document the contract); (b) NULL all non-ASCII hosts for now (`regexp_matches(h,
'^[a-z0-9.-]+$')` added to the gate) and revisit when an IDN-aware consumer exists. Given the substrate
ambition, (a) with an explicit "hosts are stored as-is, not punycoded" contract line is the right call —
just make it explicit so a punycoding consumer doesn't silently miss.

### N5 — Arrow-table size estimate is ~40% low, and the table is not freed before indexing — **Medium (no OOM, but tighten)**
§8 estimates the Arrow table at "~4-5 GB for 11 cols." Live per-column measurement (200k sample × 177×)
of the **actual sidecar schema** (note: the sidecar carries `linkedin_slug` ~0.4 GB, *not* the 1.6 GB
`linkedin_url`):
```
pdl_company_id 1.14 + company_name_norm 0.85 + company_name 0.91 + company_legal_base ~0.80 +
normalized_domain ~0.50 + linkedin_slug ~0.40 + locality 0.42 + region 0.43 + country 0.44 +
source_version(int64) 0.28 + built_at(ts) 0.28  ≈  ~6.9 GB
```
Real ~7 GB, not 4-5. **Still fits 32 GiB** — no OOM. The BTREE build re-reads columns from the local
Lance file, so peak co-residency is Arrow table (~7 GB, still alive — the plan never frees it) + the
in-memory sort working set for `company_name_norm` (~30M distinct, ~0.85 GB values + offsets +
sort scratch, ~2 GB) + overhead ≈ **~10-12 GB peak**, comfortably under 24 GB DuckDB limit and 32 GiB
container. `LANCE_BYPASS_SPILLING=true` + 512 GB ephemeral NVMe for the index external-sort is correctly
sized. **Two tightenings:** (1) correct the §8 estimate to ~7 GB; (2) explicitly `del table; con.close()`
*after* `write_dataset` and *before* `_create_indexes` so the ~7 GB Arrow buffer is released before the
5 sort builds run — the precedent (`free_company_dataset.py:431-478`) leaves it alive, a latent
inefficiency worth not cloning. The streaming `to_arrow_reader(1048576)` fallback on
`MemoryError`/`OutOfMemoryException` is the correct guard and should be kept verbatim.

### N6 — Staleness trigger deferred to "one-line follow-on" — ship the wiring, not just the stamp — **Medium**
§9 stamps `source_version` (correct) but defers the base→sidecar refresh fan-out as "a one-line
follow-on (noted §13), not a blocker." For a **foundational substrate that bridges will treat as
authoritative**, the silent-staleness window is the precise hazard: `pdl_companies` gets a new
manual-overwrite snapshot (v12), the sidecar still reflects v11, and every bridge silently resolves
against stale firmographics with **no error** — `source_version` is observable only if someone *looks*.
The plan's own §2 sells "a stale sidecar is detectable and non-corrupting" — detectable ≠ detected. The
trigger is genuinely one line given the precedent: `free_company_dataset.py:489` already POSTs a
terminal callback; `sam_fmcsa_domain_spine.py:359-376` shows the `_post_callback` fan-out idiom. **Wire
it in this PR:** in `ingest_pdl_companies`'s success path, after the callback, fire
`pdl_normalized_companies::run` (or register it on the same Trigger waitpoint). The compute is proven by
the first manual `modal run`; the fan-out is additive and removes the staleness window before any
consumer depends on the substrate. If truly deferred, add a hard gate to the consumer contract:
*"a bridge MUST assert `pdl_normalized_companies.source_version == lance.dataset(pdl_companies).version`
before resolving, and fail closed on mismatch."* One or the other — not neither.

### N7 — Read-path concurrency: `ds.version` captured before scan, but no overwrite-during-scan guard — **Low**
§8 step 1 captures `ds.version` then streams `ds.scanner(...).to_batches()`. If `pdl_companies` is
overwritten mid-scan (manual drop races the sidecar build), Lance's immutable-version MVCC means the
already-opened `ds` handle keeps reading the v11 fragments (they aren't deleted until a later
`cleanup`), so the scan is consistent — **but** `source_version` is stamped from the handle opened
*before* the race, which is correct (it records what was actually read). This is fine as-is; the only
fragile case is if the build opens `lance.dataset(SRC_URI)` *twice* (once for version, once for scan)
and the overwrite lands between — then the two see different versions. **Fix:** open the dataset handle
**once**, read `.version` off that handle, and scan off the *same* handle (the plan implies this; make
it explicit in code — `ds = lance.dataset(SRC_URI, so); v = ds.version; reader = ds.scanner(...)`). Do
not re-open. DuckDB consumes the Lance reader fine via `con.register(name, ds.scanner(...).to_reader())`
(proven in `sam_fmcsa_domain_spine.py:295`, `:387`); no fragility there.

### N8 — IP-literal hosts pass `_bare_host` but are nulled by the TLD gate — affirm, don't change — **Nit**
`192.168.1.1` → host `192.168.1.1` → `normalized_domain` NULL (the final octet `.1` fails
`\.[a-z]{2,}$`, no alpha TLD). Correct by accident but correct — IP literals should not be domain join
keys. No change; note it as intended in a Gate-1 fixture (`192.168.1.1` → NULL).

---

## Architecture check — did sidecar-vs-inline survive re-litigation? **Yes.** {#A}

I tried to steelman inline (and a third option) and the sidecar wins on this specific dataset:

- **Inline (columns on `pdl_companies`)** is genuinely worse *here* because the SoR's only write paths
  (`overwrite`, `reindex`) both publish via `_replace_r2_prefix` — a full **wipe + ~7 GB re-upload**
  (`free_company_dataset.py:274-295`) that destroys version history. `lance.add_columns` collapses into
  the same full rewrite under this boto3-publish constraint (R2's multipart part-size rule forbids the
  native Lance writer at index scale — `free_company_dataset.py:24-33`). So "just add a column" is
  *not* cheap here; it is a 35.4M-row SoR rewrite that other consumers (`hmda_bulk.py`,
  `cms_open_payments/ingest.py`, `overture_maps/places.py` all reference pdl) read. The plan's §2.1 is
  correct.
- **The norm rule is mutable policy** (`core.name_norm`/`web_norm` exist *because* it changes and breaks
  joins). Coupling it to the immutable SoR forces a base republish on every rule revision; decoupled, a
  rule change rebuilds only the thin sidecar. Correct.
- **Is the join hop ever un-pushable/costly?** The one workload where a sidecar join hop *could* hurt is
  a resolver that needs a tiebreak column living *only* on `pdl_companies` (not inlined) in the *same*
  ranking query — which is exactly N2 (industry/size/year). The plan already pre-empts this for geo by
  inlining; **N2 extends the same fix to industry/size/year and the objection dissolves.** With N2
  applied, every resolution tiebreak is on the sidecar row and the only hop back to `pdl_companies` is
  the final hydration-by-PK (BTREE point lookup), which is cheap and unavoidable in any design.
- **Is `source_version` enough, or is content-hash/fragment-id lineage needed?** `source_version` (a
  Lance version int) is sufficient lineage here because `pdl_companies` is **whole-snapshot
  manual-overwrite** — one version == one complete content state, no partial appends, no per-fragment
  drift (the diagnostic confirms v11 = 1 data write + 10 index commits, no append churn). A content hash
  would add nothing a monotonic version doesn't already give for an overwrite-only source. **The caveat
  is N6:** the version stamp is only lineage if something *enforces* it — ship the trigger or the
  fail-closed consumer assert.

Verdict on architecture: **sidecar is correct.** It survives. The blocking defects are
implementation/contract bugs, not an approach error — same shape as the SAM precedent (the SAM review
also landed SHIP-WITH-AMENDMENTS with the build sound and the *contract* needing fixes).

---

## What's already right (load-bearing affirmations only)

- **W1 — Engine-parity (the headline fear) is empirically a non-issue.** {#W1} The concern: the sidecar
  stores DuckDB-computed `name_norm`, but a consumer might evaluate `name_norm()` via DataFusion at read
  time (the diagnostic shows DataFusion lowers `trim`→`btrim`, `VARCHAR`→`Utf8`). **Tested:** `btrim`
  doesn't even *exist* in DuckDB (write side uses `trim`) — but it doesn't matter, because by the time
  the outer `trim`/`btrim` runs, name_norm's `[^A-Z0-9 ]+ → ''` step has already stripped every
  non-`[A-Z0-9 space]` char (including NBSP/em-space, which DuckDB's `\s` does *not* match — verified:
  `'\xa0ACME CORP\xa0'` → `'ACME CORP'` via the char-class strip, not the `\s` collapse). The string
  reaching `trim`/`btrim` is pure `[A-Z0-9 ]` with at most ASCII leading/trailing spaces, which both
  `trim` and `btrim` strip identically. **DuckDB-stored and DataFusion-read `name_norm` are
  byte-identical.** The diagnostic's `btrim` finding is about *index pushdown* (the function isn't bound
  to the BTREE → full scan), which the sidecar correctly solves by materializing — **not** about byte
  divergence. The join contract holds. The *only* residual hazard is a consumer issuing `WHERE
  company_name_norm = name_norm(col)` where `name_norm(col)` is evaluated by the Lance scanner against
  the *sidecar* (re-deriving at read time) — §10's "never re-inline; seek with a DuckDB-computed
  literal" rule closes it. Make that rule explicit in §10: *"seek values are computed by `core.name_norm`
  in DuckDB and bound as constants; never wrap a column in `name_norm()` inside a Lance/DataFusion
  predicate."*
- **W2 — SoR-immutability and `overwrite`-safety reasoning (§2).** The derived-from-immutable argument
  ("overwrite is idempotent and safe for the sidecar precisely because it is reconstructable") is
  correct and is the right justification for needing no append-dedup guard.
- **W3 — Two-stage CTE computing each heavy regex once (§6).** `_cnn`/`_host`/`_lslug` materialized in
  the CTE, then `legal_name_base(_cnn)` / `normalized_domain(_host)` referencing the computed columns,
  avoids re-running the regex chains per output column. Matches the proven `legal_name_base(normalized_legal_name)`
  alias idiom. Correct.
- **W4 — Build-local-then-boto3-publish to a NEW prefix.** Cloning `_replace_r2_prefix` to
  `active/pdl_normalized_companies/` (never `active/pdl_companies/`) correctly dodges R2's multipart
  escalation on the high-card index `page_data.lance` files and structurally cannot touch the SoR. The
  "SoR untouched" Gate 8 (`version == 11`, no object written under the source prefix) is the right
  proof.
- **W5 — Memory/disk envelope.** `memory=32768` / DuckDB `memory_limit='24GB'` / `LANCE_BYPASS_SPILLING=true`
  / 512 GB ephemeral NVMe is correctly sized for the ~7 GB Arrow table + in-memory high-card string BTREE
  sorts + disk-bound index external-sort. No OOM (N5 quantifies ~10-12 GB peak).

---

## Empirical appendix — what was run live vs. reasoned

**Run live against `pdl_companies` v11 (2026-06-06, zero mutation):**
- `linkedin_url` case distribution: 500k + 98k samples, **100% canonical lowercase**, `any_uppercase=0`,
  `ci_hit_cs_miss=0` → B1 is latent (0 impact on PDL, real for the shared substrate).
- `normalized_domain` shared-host collapse: **1.46%** of normalized domains on a webmail/social/platform
  host; top hosts `instagram.com`/`facebook.com`/`indiamart.com`/`linktr.ee`/`linkedin.com`/… → B3 blast
  radius ~340k rows extrapolated.
- `domain` field shapes: 27/98k carry `@` (all social handles `youtube.com/@x`, not userinfo) → N3.
- PK null/empty drop: `is_null=0`, `null_after_trim=0`, `distinct=total=35,446,771` → B2 filter is a
  no-op *today* but the gate is a constant, not parity.
- `name_norm` byte-parity: char-class strip removes NBSP/em-space (DuckDB `\s` does not); `trim`/`btrim`
  strip identical ASCII residue → W1 parity holds.
- Per-column Arrow sizing (200k × 177×): real sidecar table ~7 GB (plan said 4-5) → N5.
- `core.web_norm` §5.2 builders run verbatim on synthetic edge battery (uppercase scheme/host, userinfo,
  ports, IP literals, trailing dots, IDN, empty slug, `/in/` vs `/company/`) → B1, N1, N4, N8.

**Reasoned, not independently built:**
- The SAM/GLEIF→PDL bridges (§10 consumers) do not yet exist; B3's `is_generic_domain` clause and W1's
  seek-literal rule are prescriptions for *their* contracts, validated against data but not a built
  bridge.
- DataFusion's `btrim` Unicode-whitespace set was reasoned (both `trim`/`btrim` default to the same strip
  for the ASCII residue name_norm leaves) but not executed inside a live Lance read (DuckDB lacks
  `btrim`); the conclusion rests on name_norm's char-class strip making the trim-flavor moot, which *was*
  verified in DuckDB.
- Index-build wall time / external-sort behavior under `LANCE_BYPASS_SPILLING` at 35.4M was not run
  (read-only constraint); sizing is reasoned from the precedent's proven completion on the same machine
  shape.
