# Directive 21 — Multi-Provider Email Waterfall & Verification

> **Status:** ⚙️ **BUILT (Directive 21-Final)** — decoupled pipeline implemented & merged. See the amendment below; §§1–9 are the original blueprint, retained for design history.
> **Verified against live systems:** 2026-06-02 (blueprint) · **2026-06-03** (Icypeas + LeadMagic contracts locked via official-docs recon; Doppler/Modal re-checked).
> **Authoritative for:** vendor cascade order, verification rubric, Modal concurrency model, payload I/O, error handling.

---

## ⚠️ Amendment — Directive 21-Final (BUILD): Blitz decoupled

**Structural pivot (supersedes §§0–8 wherever they mention Blitz or a pattern tier):** Blitz is **removed entirely** from this workflow. The waterfall is now strictly **Icypeas (Tier 1) → LeadMagic (Tier 2)**, with **MillionVerifier as the sole arbiter** after every hit. The Tier-4 pattern-permutation step is dropped. The MillionVerifier rubric (§3) is unchanged and remains authoritative.

### A. Vendor contracts — LOCKED (autonomous official-docs recon, 2026-06-03)

| | **Icypeas** (Tier 1) | **LeadMagic** (Tier 2) |
|---|---|---|
| Submit | `POST https://app.icypeas.com/api/email-search` | `POST https://api.leadmagic.io/email-finder` |
| Auth | `Authorization: <API_KEY>` — **raw key, no HMAC** (the Icypeas HMAC-SHA1 scheme is webhook-verification-only) | `X-API-Key: <key>` (case-insensitive) |
| Request | `{firstname, lastname, domainOrCompany, custom:{externalId}}` | `{first_name, last_name, domain` \| `company_name}` (names required) |
| Model | **async** — submit → `item._id` (status `NONE`); poll until terminal | synchronous |
| Fetch results | `POST https://app.icypeas.com/api/bulk-single-searchs/read` `{"id": <_id>}` → email at `items[0].results.emails[0].email`, confidence at `…[0].certainty` | n/a (in the response) |
| Hit/miss | terminal status ∈ {`FOUND`,`DEBITED`} with an email = hit; {`NOT_FOUND`,`DEBITED_NOT_FOUND`} = miss | `status ∈ {valid, valid_catch_all}` + email = hit; `not_found`/null = miss |
| **Rate limits** | **two separate buckets**: submit **10/sec**, read **30/min** ← *the binding constraint* | **300/min**; charged only on a found email |
| Errors | validation = HTTP 200 + `success:false`; 401; 429 | 400/401/402/404/429/500; RFC 9457 problem+json |

> **Verification mandate (unchanged):** vendor-native status (`valid`, `FOUND`, certainty) is used **only** to detect hit-vs-miss. **Every** address — including LeadMagic `valid` — is re-verified by MillionVerifier, which alone decides `verified`/`risky`/discard. No vendor "valid" flag is trusted.

### B. As-built architecture

| Artifact | Path | Role |
|---|---|---|
| Icypeas rate gateway | `core/icypeas_gateway.py` (app `icypeas-gateway`) | **single-container** (`max_containers=1` + `@modal.concurrent`) global egress; submit 10/s + read 30/min token buckets; submit+poll → final envelope. Holds `ICYPEAS_API_KEY` only (secret blast radius). Mirrors `core/blitz_gateway.py`. |
| Cascade worker | `pipelines/enrichment_email_cascade/enrich_email_cascade.py` (app `enrichment-email-cascade`) | `run_cascade(contacts[])`: Icypeas (via gateway) → LeadMagic (inline, elastic) → MillionVerifier (inline, elastic) per the §3 rubric. Holds `LEADMAGIC_API_KEY` + `MILLIONVERIFIER_API_KEY` (`email-cascade` secret) + `hqx-postgres`. |
| Sink DDL | `pipelines/enrichment_email_cascade/ops_email_cascade_runs.sql` | `ops.email_resolutions` (latest-wins per `contact_id`) + `ops.email_cascade_runs` (run-state). |
| Coordinator | `src/trigger/enrichment_email_cascade.ts` (task `enrichment-email-cascade-resolve`) | chunks contacts, dispatches per-chunk via the Universal Dispatcher, suspends on `wait.forToken`, aggregates terminal counts. |

**Concurrency:** Icypeas is the *only* gated vendor (single-container buckets). LeadMagic (300/min) + MillionVerifier run **elastically** inline — safe by construction because they only see Tier-1 misses, a population already throttled below Icypeas's 30/min read ceiling. Matches Directive 21-Final mandate §3.

**Sink decision (aligned to Directive 23 §5 "event-log / no new Lance write path"):** the worker writes the work-email system-of-record to **`ops.email_resolutions`** (Postgres, latest-wins upsert keyed on `contact_id`), **not** a direct Lance `merge_insert` as the original §8 proposed. A downstream materializer can roll it into a Lance dataset on its own cadence, exactly as `firmographics-blitz` does. Idempotency: already-`verified` contacts are skipped unless `force=true`.

### C. Provisioning & deploy status

- **🔴 Human-gated blocker — vendor keys absent from Doppler `core-x/prd`.** As of 2026-06-03 the config holds only `BLITZAPI_API_KEY`. **`ICYPEAS_API_KEY`, `LEADMAGIC_API_KEY`, `MILLIONVERIFIER_API_KEY` must be added**, then synced to two Modal secrets:
  ```sh
  modal secret create icypeas-api  ICYPEAS_API_KEY="$(doppler secrets get ICYPEAS_API_KEY  -p core-x -c prd --plain)" --force
  modal secret create email-cascade \
      LEADMAGIC_API_KEY="$(doppler secrets get LEADMAGIC_API_KEY  -p core-x -c prd --plain)" \
      MILLIONVERIFIER_API_KEY="$(doppler secrets get MILLIONVERIFIER_API_KEY -p core-x -c prd --plain)" --force
  ```
  The gateway and worker **fail closed** when a key is absent (return a structured "key absent" envelope; never crash), so deploy + dispatcher-resolution work today and live resolution lights up the moment the keys land.

---

## 0. Executive summary

A resilient, asynchronous **Work-Email Waterfall** that takes a located contact profile and resolves a deliverable corporate email by cascading through a prioritized vendor chain — **Blitz → Icypeas → LeadMagic → pattern-permutation** — running **every** vendor hit through **MillionVerifier** before it is allowed to touch the sink. **MillionVerifier is the single arbiter of deliverability**; vendor-native validation is used only to detect hit-vs-miss.

The system is a **Trigger.dev coordinator** driving a **multi-worker Modal** data plane, reusing the fleet's existing control/data-plane split (Universal Dispatcher → `spawn()` → flat-JSON waitpoint callback → `ops.*_runs` state + Lance/R2 sink). The only genuinely new infrastructure is four vendor adapters, one MillionVerifier gate, two single-container rate chokepoints, and one Lance sink dataset.

### 0.1 Recon verdict — what already exists vs. what is net-new

| Component | State | Evidence |
|---|---|---|
| Control plane (Trigger v4 `createToken`→dispatch→`forToken`) | ✅ exists, reuse verbatim | `src/trigger/gtm_companies_people.ts` |
| Universal Dispatcher (single proxy-authed Modal endpoint) | ✅ exists, reuse verbatim | `core/modal_dispatcher.py` |
| Gen-3 worker pattern (ops.*_runs + flat callback + R2/Lance) | ✅ exists, clone shape | `pipelines/firmographics_blitz/materialize_blitz.py` |
| **Blitz** API integration + key | ✅ key present (`BLITZAPI_API_KEY`) | Doppler `core-x/prd`; `docs/reference/BLITZ_API_CANONICAL_REFERENCE.md` |
| Blitz **email** endpoint (`/v2/enrichment/email`) | ⚠️ documented, **never called in-repo** (firmographics uses company endpoints only) | Blitz canonical ref §Enrichment |
| **MillionVerifier** key | ❌ **MISSING from Doppler `core-x/prd`** | `secrets_names(core-x/prd)` — no `MILLIONVERIFIER_*` |
| **Icypeas** integration + key | ❌ net-new (no key, no code) | grep: zero hits repo-wide |
| **LeadMagic** integration + key | ❌ net-new (no key, no code) | grep: zero hits repo-wide |
| Email sink (Lance dataset + ops table) | ❌ net-new | — |

> **⚠️ Directive correction (do not skip):** Mandate §2 states *"Locate the MillionVerifier API keys within our core-x Doppler secrets manager."* **No such secret exists today.** `core-x/prd` holds `BLITZAPI_API_KEY` but **none** of `MILLIONVERIFIER_API_KEY`, `ICYPEAS_API_KEY`, `ICYPEAS_API_SECRET`, `LEADMAGIC_API_KEY`. Provisioning these (§9) is a **hard prerequisite** of the build phase. The directive's note that DB vars are `HQX_`-prefixed is correct and unchanged (`HQX_DB_URL_POOLED`, `HQX_DB_URL_DIRECT` present).

---

## 1. Ground-truth vendor contracts

All four external calls + the verifier, as verified on 2026-06-02. Fields marked **⚠ verify-at-build** are where vendor docs render as JS SPAs / fragment across pages and could not be byte-confirmed; they do not affect the architecture, only adapter wiring.

### 1.1 Tier 1 — Blitz `POST /v2/enrichment/email`  (Find Work Email)

| Property | Value |
|---|---|
| Base URL | `https://api.blitz-api.ai` |
| Auth | header `x-api-key: $BLITZAPI_API_KEY` |
| Input | `{ "person_linkedin_url": "<url>" }` — **requires a LinkedIn profile URL** |
| Output | `{ "found": bool, "email": "...", "all_emails": [{ email, job_order_in_profile, company_linkedin_url, email_domain }] }` |
| **Rate limit** | **5 req/s** (`max_requests_per_seconds`, all plans). Exceed → **429** `{"success":false,"message":"Rate limit exceeded..."}` |
| Plan gating | Email plan ($499/mo)+ unlocks `/enrichment/email`. **Leads plan returns this endpoint as 402/forbidden.** Confirm the active plan via `GET /v2/account/key-info → allowed_apis`. |
| Errors | 401 (bad key), 402 (trial credits / plan-gated), 500 |
| Billing | flat-rate unlimited on paid plans (no per-call credit burn) |

**Footprint note:** Blitz email lookup is keyed on `person_linkedin_url` **only** — it cannot resolve from name+domain. Contacts lacking a LinkedIn URL **skip Tier 1** and enter at Tier 2. A Tier-0 re-resolution (`/v2/enrichment/domain-to-linkedin` + `/v2/search/waterfall-icp-keyword`) can mint a LinkedIn URL when missing; specced as optional pre-step in §2.4.

### 1.2 Tier 2 — Icypeas `POST /api/email-search`  (Email Discovery)

| Property | Value |
|---|---|
| Base URL | `https://app.icypeas.com/api/` |
| Auth | header `Authorization: $ICYPEAS_API_KEY` (raw key; the docs' "compute the signature" HMAC variant exists for some accounts — **⚠ verify which scheme our key uses at build**, provision `ICYPEAS_API_SECRET` defensively) |
| Input | `{ "firstname", "lastname", "domainOrCompany", "custom": { "webhookUrl", "externalId" } }` (firstname **or** lastname may be empty) |
| **Model** | **ASYNCHRONOUS.** Submit returns immediately with a scan id + `status: SCHEDULED`. Result arrives via (a) **webhook** to `custom.webhookUrl`, or (b) **polling** a fetch-results read endpoint by id/`externalId`. ⚠ exact read endpoint + terminal status enum (`FOUND`/`NOT_FOUND`/`DEBITED`) + result field/certainty are split across the Fetch-results / Push-notifications doc sections — confirm verbatim at build. |
| **Rate limit** | **30 req/min** (tightest of all vendors — the binding Tier-2 throughput constraint) |
| Errors | 200 (incl. validation errors in-body), 401, 429 |

### 1.3 Tier 3 — LeadMagic `POST /v1/people/email-finder`  (Find Work Email)

| Property | Value |
|---|---|
| Base URL | `https://api.leadmagic.io` (endpoint `…/v1/people/email-finder`; legacy `…/email-finder` also seen — **⚠ confirm path at build**) |
| Auth | header `X-API-Key: $LEADMAGIC_API_KEY` (case-insensitive) |
| Input | `{ "first_name", "last_name", "domain" }` (domain or `company_name`) |
| Output | `{ "email", "status", "credits_consumed", ... }` |
| **`status` enum** | `valid` · `valid_catch_all` · `catch_all` · `not_found` (LeadMagic runs its own 5-layer validation and tries to resolve catch-all to `valid`/`not_found`) |
| **Rate limit** | elastic / high-concurrency (no published hard per-second cap). Treat as the **elastic tier**. |
| Billing | **charged only on success**; `not_found` is **free and cached** server-side (cheap to call last) |
| Errors | 401, 402, 429 |

### 1.4 Verifier — MillionVerifier `GET /api/v3/`  (single, real-time)

| Property | Value |
|---|---|
| Endpoint | `GET https://api.millionverifier.com/api/v3/?api=$KEY&email=<urlenc>&timeout=<2..60>` (default timeout 20s) |
| Credits | `GET https://api.millionverifier.com/api/v3/credits?api=$KEY` |
| Output | `{ email, quality, result, resultcode, subresult, free, role, didyoumean, credits, executiontime, error, livemode }` |

**The governing table** (`resultcode` → `result` → `quality`), authoritative:

| `resultcode` | `result` | `quality` | Deliverability |
|:--:|---|---|---|
| **1** | `ok` | **good** | SAFE — exists |
| **2** | `catch_all` | **risky** | domain accepts-all; address *may* exist |
| **3** | `unknown` | **risky** | transient verify failure; indeterminate |
| **4** | `error` | bad | input/system error |
| **5** | `disposable` | **bad** | temp-mailbox provider; do not send |
| **6** | `invalid` | **bad** | does not exist |

---

## 2. Vendor Cascade Protocol

### 2.1 Cascade order & per-tier entry condition

```
                 ┌─────────────────────────────────────────────────────────┐
  ContactInput ──┤  TIER 1  Blitz /enrichment/email   (needs LinkedIn URL)  │
                 │     │ hit→VERIFY    miss→↓                                 │
                 │  TIER 2  Icypeas /email-search      (needs name+domain)   │
                 │     │ hit→VERIFY    miss→↓                                 │
                 │  TIER 3  LeadMagic /email-finder    (needs name+domain)   │
                 │     │ hit→VERIFY    miss→↓                                 │
                 │  TIER 4  pattern permutation        (MV-gated, last resort)│
                 │     │ ok→done       none→unresolved                        │
                 └─────────────────────────────────────────────────────────┘
   every "hit" → MillionVerifier → governs STOP / HOLD / DISCARD (see §3)
```

A tier is attempted **only if** the prior tier produced no `verified` (`ok`) result **and** the contact carries the inputs that tier needs:

| Tier | Vendor | Required contact fields | Skip when |
|:--:|---|---|---|
| 1 | Blitz | `person_linkedin_url` | LinkedIn URL absent → enter at Tier 2 |
| 2 | Icypeas | (`first_name` \| `last_name`) + (`company_domain` \| `company_name`) | name or company absent |
| 3 | LeadMagic | (`first_name` + `last_name`) + `company_domain` | name or domain absent |
| 4 | pattern | `company_domain` + name | domain absent → terminal `unresolved` |

### 2.2 "Miss" definition (when to cascade) — per vendor

A tier **misses** (→ next tier) on any of:

- **Blitz:** `found == false` · `email` null/blank · HTTP **402** (plan-gated / no credits) · HTTP **404**.
- **Icypeas:** terminal status `NOT_FOUND` · no email in result · HTTP **4xx** (non-429).
- **LeadMagic:** `status == "not_found"` · `email` null.
- **Any vendor:** a hit whose email returns **MV bad** (`resultcode` 4/5/6) is reclassified as a **miss** and cascades.

A tier **back-pressures** (retry **within** the gate, **not** a miss) on: HTTP **429**, connect/read **timeout**, HTTP **5xx**. See §7.

> **Timeout ≠ miss.** A vendor timeout is *indeterminate*, not *not-found*. It is retried with bounded backoff inside the gate; only after retries exhaust is it recorded as a **soft-miss** (`outcome: "soft_miss_timeout"`) and cascaded — never silently dropped.

### 2.3 Universal verification mandate

> Per Mandate §2: **every** address any vendor returns is intercepted and sent to MillionVerifier **before** it can stop the waterfall or reach the sink. Vendor-native status (`valid`, `found:true`, …) is used **only** to detect hit-vs-miss — it is **never** trusted as deliverability. This normalizes confidence across heterogeneous vendors behind one rubric.
> *Cost note:* a LeadMagic `valid` could in principle skip re-verification to save a MillionVerifier credit; the directive forbids this, and it is the correct call (single arbiter, no per-vendor trust calibration). Optional cost optimization, **disabled by default**, recorded in §9.

### 2.4 Tier-0 re-resolution & Tier-4 fallback

- **Tier-0 (optional pre-step, off by default):** when `person_linkedin_url` is absent but `company_domain` is present, mint one via Blitz `domain-to-linkedin` → `waterfall-icp-keyword`, then run Tier 1. Adds Blitz-budget cost; enable per-batch.
- **Tier-4 (last resort):** generate canonical local-part permutations against `company_domain` (`{first}.{last}`, `{first}`, `{f}{last}`, `{first}{l}`, `{first}_{last}`) and run **each through MillionVerifier**, accepting the **first `ok` (resultcode 1) only**. Hard cap: ≤ 6 permutations ⇒ ≤ 6 MV calls/contact. Because Tier 4 emits **only** MV-`ok` addresses, it can never pollute the sink. No `ok` ⇒ terminal **`unresolved`**.

---

## 3. MillionVerifier validation gate — the loop governor

### 3.1 Evaluation rubric

| MV `resultcode` | Bucket | Waterfall action |
|:--:|---|---|
| **1** `ok` | good | **STOP.** Persist email, `verification_status = verified`. |
| **2** `catch_all` | risky | **HOLD & CONTINUE.** Stash as candidate; keep cascading to beat it with an `ok`. |
| **3** `unknown` | risky | **RETRY once** at `timeout=60` (transient). Still unknown → HOLD & CONTINUE (rank below catch_all). |
| **4** `error` | bad | **DISCARD**, cascade. (If `error` reflects malformed input, log loudly — not a vendor miss.) |
| **5** `disposable` | bad | **DISCARD**, cascade. |
| **6** `invalid` | bad | **DISCARD**, cascade. |

### 3.2 Catch-all / risky strategy (the directive's "document your strategy" ask)

**Decision: never let `catch_all` stop the waterfall, and never write it as `verified`.**

1. On `catch_all`/`unknown`, the address is **held** as a ranked candidate (`ok > catch_all > unknown`), and the cascade **continues** — a later vendor may return an `ok` that strictly dominates.
2. If a later tier yields `ok`, it wins immediately (STOP).
3. If the waterfall **exhausts** with only risky candidates, persist the **highest-ranked** one with `verification_status = risky` (accept-degraded) — **flagged, never `verified`**.
4. The downstream send layer (EmailBison) already carries a native `risky` verification status and gates sends on it. The waterfall's contract is: emit `{verified, risky, unresolved}` truthfully; **the campaign layer decides whether to send `risky`.** Clean separation of concerns — the waterfall does not silently launder catch-all into "good."

### 3.3 The exact loop (illustrative — lives in the Modal worker)

```python
# MillionVerifier resultcode → action. Single arbiter of deliverability.
MV_GOOD  = {1}            # ok          → STOP, verified
MV_RISKY = {2, 3}         # catch_all, unknown → HOLD, keep cascading
MV_BAD   = {4, 5, 6}      # error, disposable, invalid → DISCARD, cascade
_RISKY_RANK = {2: 0, 3: 1}  # catch_all outranks unknown

CASCADE = [("blitz", 1), ("icypeas", 2), ("leadmagic", 3), ("pattern", 4)]

async def resolve_contact(c: ContactInput) -> ResolvedEmail:
    attempts: list[dict] = []
    best_risky: dict | None = None     # highest-ranked risky candidate, held across tiers

    for vendor, tier in CASCADE:
        if not _tier_eligible(vendor, c):          # §2.1 entry condition
            attempts.append({"vendor": vendor, "tier": tier, "outcome": "skipped"})
            continue

        email = await VENDOR[vendor](c)            # None on miss / soft-miss / timeout-exhausted
        if not email:
            attempts.append({"vendor": vendor, "tier": tier, "outcome": "miss"})
            continue

        mv = await millionverifier(email)          # {resultcode, result, quality, subresult}
        rc = mv["resultcode"]
        attempts.append({"vendor": vendor, "tier": tier, "email": email,
                         "mv_result": mv["result"], "mv_resultcode": rc})

        if rc in MV_GOOD:                          # OK / Deliverable → STOP, save, verified
            return _resolved(c, email, "verified", vendor, tier, mv, attempts)

        if rc in MV_RISKY:                         # Catch-all / Unknown → HOLD, keep cascading
            cand = {"email": email, "vendor": vendor, "tier": tier,
                    "mv": mv, "rank": _RISKY_RANK[rc]}
            if best_risky is None or cand["rank"] < best_risky["rank"]:
                best_risky = cand
            continue

        # rc in MV_BAD → Bad / Undeliverable → throw away, drop to next vendor
        continue

    if best_risky is not None:                     # exhausted: accept-degraded, flagged risky
        b = best_risky
        return _resolved(c, b["email"], "risky", b["vendor"], b["tier"], b["mv"], attempts)
    return _resolved(c, None, "unresolved", None, None, None, attempts)


async def millionverifier(email: str) -> dict:
    mv = await _mv_call(email, timeout=20)
    if mv["resultcode"] == 3:                       # unknown = transient → one slow retry
        mv = await _mv_call(email, timeout=60)
    return mv
```

---

## 4. Modal task state & concurrency control

### 4.1 The core problem

Modal scales containers **horizontally**. A naïve `resolve_contact.map(50_000_contacts)` fans out to dozens of containers, each calling Blitz independently → the **global** 5 req/s ceiling is blown N-fold → 429 storms. The constraint is **global**, so it must be enforced at a **single chokepoint**, not per-container.

**Mandate assumption honored:** this job owns the entire Blitz 5/s budget during its window (no competing processes), so a single in-process token bucket is authoritative — no cross-job distributed coordination needed.

### 4.2 Topology — one chokepoint per hard-limited vendor

```
 Trigger coordinator ──(dispatch N chunks)──▶ resolve_chunk         [elastic: max_containers≈8]
                                                  │  async fan-out of resolve_contact()
                          ┌───────────────────────┼───────────────────────┐
                          ▼                        ▼                        ▼
                   blitz_gate              icypeas_gate            (inline httpx)
              [max_containers=1]      [max_containers=1]        LeadMagic + MillionVerifier
               5 req/s bucket          30 req/min bucket          [elastic, semaphore-capped]
              GLOBAL chokepoint        GLOBAL chokepoint
```

- **Two single-container gates** (`blitz_gate`, `icypeas_gate`) — exactly the two vendors with hard global caps. `max_containers=1` makes the module-level token bucket the **authoritative global limiter**, shared across every chunk/run. `@modal.concurrent(max_inputs=K)` lets the one container hold K in-flight async inputs (pure I/O wait), so the bucket — not the container count — paces outbound calls.
- **Elastic vendors inline:** LeadMagic and MillionVerifier have no hard per-second cap, so they are called as plain async `httpx` inside `resolve_chunk`, bounded by a generous module-level `asyncio.Semaphore` (e.g. 50). No chokepoint needed → lowest latency for the highest-volume tier (verification runs on *every* hit).
- **Backpressure is automatic:** when many `resolve_contact` coroutines await `blitz_gate.remote.aio()`, Modal queues the inputs and the single Blitz container drains them at 5/s. Callers simply suspend.

### 4.3 Container-pool configuration

| Function | `max_containers` | `@modal.concurrent(max_inputs)` | Limiter | Rationale |
|---|:--:|:--:|---|---|
| `blitz_gate` | **1** | 24 | `AsyncRateLimiter(5, 1.0)` | global 5 req/s |
| `icypeas_gate` | **1** | 8 | `AsyncRateLimiter(30, 60.0)` | global 30 req/min; async submit+poll/webhook |
| `resolve_chunk` | 8 | 4 | local `Semaphore` over contacts | orchestration; I/O-bound, cheap |
| (inline) LeadMagic | — | — | `Semaphore(50)` in-process | elastic |
| (inline) MillionVerifier | — | — | `Semaphore(50)` in-process | elastic; runs on every hit |

> **Small-batch simplification:** for batches ≲ a few thousand, collapse the whole plane into **one** `resolve_batch` function at `max_containers=1` with all four module-level buckets and asyncio fan-out — the workload is pure I/O (sockets, not CPU), so a single async container saturates every vendor's ceiling with the least moving parts. Scale out to §4.2 only when orchestration CPU or chunk durability demands it.

### 4.4 Global token bucket (illustrative)

```python
import asyncio, time

class AsyncRateLimiter:
    """Token bucket. One instance per max_containers=1 gate ⇒ authoritative global limiter."""
    def __init__(self, rate: int, per: float):
        self._rate, self._per = rate, per
        self._allow = float(rate)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._allow = min(self._rate,
                                  self._allow + (now - self._last) * (self._rate / self._per))
                self._last = now
                if self._allow >= 1.0:
                    self._allow -= 1.0
                    return
                wait = (1.0 - self._allow) * (self._per / self._rate)
            await asyncio.sleep(wait)            # lock released while sleeping
```

### 4.5 Throughput envelope

- Tier-1 Blitz is the binding constraint: **5/s = 300/min = 18,000/hr**. A 50k batch where every contact reaches Tier 1 ≈ **2.8 h** of Blitz wall-clock. Only Tier-1 *misses* descend, so Icypeas (30/min) and below see a shrinking population — Icypeas's 30/min caps the *fallback* throughput, not the whole batch.
- Implication: batch sizing and `maxDuration` (§5) must assume worst-case all-Tier-1. The durable waitpoint consumes **zero** compute while suspended, so long wall-clock is free on the control plane.

### 4.6 Modal app skeleton (illustrative)

```python
app = modal.App("email-waterfall", image=image, secrets=[
    modal.Secret.from_name("email-waterfall"),    # vendor + MV keys (Doppler core-x/prd → synced)
    modal.Secret.from_name("r2-credentials"),     # Lance sink
    modal.Secret.from_name("hqx-postgres"),       # ops.email_waterfall_runs (HQX_DB_URL_POOLED)
])

_blitz_bucket = AsyncRateLimiter(5, 1.0)
_icy_bucket   = AsyncRateLimiter(30, 60.0)

@app.function(max_containers=1, timeout=3600)
@modal.concurrent(max_inputs=24)
async def blitz_gate(person_linkedin_url: str) -> dict:
    await _blitz_bucket.acquire()
    return await _blitz_email(person_linkedin_url)

@app.function(max_containers=1, timeout=3600)
@modal.concurrent(max_inputs=8)
async def icypeas_gate(payload: dict) -> dict:
    await _icy_bucket.acquire()
    return await _icypeas_search_and_await(payload)   # submit + bounded poll/webhook

@app.function(max_containers=8, timeout=3600)
@modal.concurrent(max_inputs=4)
async def resolve_chunk(contacts: list[dict], trigger_callback_url: str | None = None) -> dict:
    sem = asyncio.Semaphore(64)
    async def _one(c):
        async with sem:
            return await resolve_contact(ContactInput(**c))
    results = await asyncio.gather(*(_one(c) for c in contacts))
    _sink_upsert(results)                              # Lance merge_insert on contact_id
    _record_run(results)                               # ops.email_waterfall_runs (HQX)
    _post_callback(trigger_callback_url, _summary(results))   # flat JSON → waitpoint
    return _summary(results)
```

---

## 5. Trigger.dev coordinator

Mirrors `gtm_companies_people.ts` verbatim in shape: mint waitpoint → POST Universal Dispatcher → suspend on `forToken` → resolve from flat callback. One waitpoint **per chunk** so a single chunk failure can't time out the whole batch; the coordinator fans out chunks and `Promise.all`s the tokens.

```typescript
export const emailWaterfall = task({
  id: "email-waterfall-resolve",
  maxDuration: 14400,                                   // 4h ceiling; suspended waits are free
  run: async (payload: { contacts: ContactInput[]; chunkSize?: number; options?: WaterfallOptions }) => {
    const size = payload.chunkSize ?? 1000;
    const chunks = chunk(payload.contacts, size);

    const settled = await Promise.all(chunks.map(async (slice, i) => {
      const token = await wait.createToken({ timeout: "4h", tags: ["email-waterfall", `chunk-${i}`] });
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "email-waterfall",
          function_name: "resolve_chunk",
          kwargs: { contacts: slice, options: payload.options ?? {} },
          trigger_callback_url: token.url,
        }),
      });
      if (!res.ok) throw new Error(`dispatcher ${res.status}: ${(await res.text()).slice(0, 500)}`);
      const out = await wait.forToken<ChunkSummary>(token.id);
      if (!out.ok) throw new Error(`chunk ${i} timed out before Modal callback`);
      return out.output;                                 // {verified, risky, unresolved, errors}
    }));

    return aggregate(settled);
  },
});
```

**Idempotency:** dispatch carries an `idempotencyKey = sha256(contact_id + job_id)` per contact; the sink upsert is a Lance `merge_insert` on `contact_id`, so chunk retries and at-least-once callbacks converge. Re-running the whole job re-resolves and upserts — safe.

---

## 6. Payload I/O model

### 6.1 Input — `ContactInput` (what the person-finder emits)

```python
class ContactInput(BaseModel):
    contact_id: str                       # stable PK into the sink (required)
    person_linkedin_url: str | None = None   # Tier 1
    first_name: str | None = None            # Tier 2/3/4
    last_name:  str | None = None
    company_domain: str | None = None        # Tier 2/3/4 (normalized, no scheme/www)
    company_name:   str | None = None        # Tier 2 fallback when domain absent
    company_linkedin_url: str | None = None  # Tier-0 re-resolution
```

### 6.2 Output — `ResolvedEmail` (one row per contact)

```python
class Attempt(BaseModel):
    vendor: str; tier: int
    outcome: str                          # skipped | miss | soft_miss_timeout | hit
    email: str | None = None
    mv_result: str | None = None          # ok | catch_all | unknown | error | disposable | invalid
    mv_resultcode: int | None = None
    latency_ms: int | None = None

class ResolvedEmail(BaseModel):
    contact_id: str
    email: str | None
    verification_status: str              # verified | risky | unresolved
    source_vendor: str | None             # blitz | icypeas | leadmagic | pattern
    source_tier: int | None
    mv_resultcode: int | None
    mv_result: str | None
    mv_quality: str | None                # good | risky | bad
    mv_subresult: str | None
    attempts: list[Attempt]               # full audit trail of the cascade
    resolved_at: datetime
```

### 6.3 Chunk callback (flat JSON → waitpoint, no `{"data":…}` envelope)

```json
{ "status": "success", "chunk": 3, "counts": { "verified": 612, "risky": 88,
  "unresolved": 271, "errors": 29 }, "rows_total": 1000 }
```

---

## 7. Error handling

### 7.1 Vendor-timeout & transient events

| Event | Handling | Becomes a miss? |
|---|---|---|
| Connect/read **timeout** | retry ≤3 w/ jittered exp backoff (0.5·2ⁿ ± jitter) **inside the gate** | only after retries exhaust → `soft_miss_timeout`, cascade |
| HTTP **429** | honor `Retry-After` if present; else backoff in-gate. Should not occur if buckets correct (defensive) | no — re-attempt same tier |
| HTTP **5xx** | retry ≤3 w/ backoff | after exhaust → soft-miss, cascade |
| HTTP **401** | **fail loud, halt** — misconfigured key; do not cascade-mask an auth error | n/a (abort) |
| Blitz **402** (plan-gated) | treat as Tier-1 unavailable → cascade; surface once per run | yes |
| Icypeas async never reaches terminal | bounded poll (max wait 90s, backoff) → `soft_miss_timeout` | yes |
| MillionVerifier `unknown` | one retry at `timeout=60` (§3) | held as risky, not a miss |
| MillionVerifier itself times out/5xx | retry ≤2; if still dead, **do not** write the unverified email — treat hit as `soft_miss_timeout` and cascade (fail-closed: an unverified address never reaches the sink) | yes |

### 7.2 Structural guarantees

- **Fail-closed verification:** no address reaches the sink without a terminal MillionVerifier verdict. MV outage degrades to `unresolved`, never to unverified writes.
- **Per-vendor circuit breaker:** if a vendor returns ≥ N consecutive hard errors (401/5xx) within a run, trip it open — skip that tier for the remainder, record `circuit_open`, continue the cascade. Prevents one dead vendor from burning the whole batch's latency.
- **Modal:** worker `retries=2` on infra crash (not on business miss); `timeout` per §4.3. Trigger task `maxAttempts` from `trigger.config.ts` (3) covers dispatch-side failures.
- **At-least-once callbacks:** waitpoint may receive a duplicate callback; the Lance `merge_insert` on `contact_id` makes re-application idempotent.

---

## 8. Data sink

System of record is **Lance on R2** (architecture invariant — Lance is canonical, Parquet is transport only).

- **Dataset:** `s3://data-sink/active/work_email_waterfall/`
- **Write mode:** Lance `merge_insert(on="contact_id").when_matched_update_all().when_not_matched_insert_all()` per chunk → idempotent upsert.
- **Schema (one row/contact):** `contact_id` *(PK)*, `email`, `email_domain_norm`, `verification_status`, `source_vendor`, `source_tier`, `mv_resultcode`, `mv_result`, `mv_quality`, `mv_subresult`, `attempts` *(list<struct>)*, `resolved_at`, `job_id`.
- **Indexes (hard deliverable per architecture):** **BTREE** `contact_id` (PK), `email`, `email_domain_norm`; **BITMAP** `verification_status`, `source_vendor`.
- **Control table (HQX Postgres):** `ops.email_waterfall_runs` — `{ id, job_id, feed, counts(jsonb: verified/risky/unresolved/errors), rows_total, status, error, started_at, completed_at, recorded_at }`, mirroring `ops.firmographics_blitz_runs`.
- **Downstream handoff:** `verified` rows are send-ready for the EmailBison campaign layer; `risky` rows map to EmailBison's native `risky` verification status (campaign layer gates the send); `unresolved` rows are retained for re-resolution on the next vendor-coverage expansion. The waterfall does **not** write to EmailBison directly — its contract terminates at the Lance sink + ops state.

---

## 9. Provisioning prerequisites & build sequence (post-sign-off)

**Hard prerequisites (blockers):**
1. **Add secrets to Doppler `core-x/prd`:** `MILLIONVERIFIER_API_KEY`, `ICYPEAS_API_KEY` (+ `ICYPEAS_API_SECRET` if the signed scheme applies), `LEADMAGIC_API_KEY`. (`BLITZAPI_API_KEY` already present.)
2. **Confirm Blitz plan unlocks `/v2/enrichment/email`** via `key-info.allowed_apis` (needs Email plan $499+). If on Leads-only, Tier 1 is dark until upgraded.
3. **Create Modal secret `email-waterfall`** synced from the Doppler keys above (mirrors the `modal secret create … "$(doppler secrets get … --project core-x --config prd --plain)"` pattern in `.env.example`).
4. **Verify-at-build adapter details:** Icypeas auth scheme (raw key vs HMAC) + fetch-results endpoint/enum; LeadMagic exact path (`/v1/people/email-finder`).

**Build order (each independently verifiable):**
1. Vendor adapters + normalizers (`VENDOR[...]` → common hit/miss) with recorded fixtures per vendor.
2. MillionVerifier gate + `resolve_contact` loop (§3.3) — unit-tested against the resultcode matrix.
3. Modal app: `blitz_gate`/`icypeas_gate` chokepoints + `resolve_chunk` + buckets (§4) — load-test the 5/s and 30/min ceilings.
4. Lance sink + `ops.email_waterfall_runs` (§8).
5. Trigger `email-waterfall` coordinator (§5) end-to-end on a small live batch.
6. Backfill batches at scale.

**Open decisions for sign-off:**
- **(A)** Enable Tier-0 re-resolution (mint LinkedIn URL via Blitz) when `person_linkedin_url` is missing? *Recommend: per-batch flag, default off.*
- **(B)** Accept `risky` (catch-all) into the sink flagged, or drop to `unresolved`? *Recommend: persist flagged `risky` — let the send layer decide (§3.2).*
- **(C)** Trust LeadMagic `valid` to skip MV re-verify for cost? *Recommend: no — honor universal-verification mandate; revisit only if MV credit spend is material.*
- **(D)** Tier-4 pattern permutation on/off and permutation set. *Recommend: on, ≤6 permutations, MV-`ok`-only.*
```
