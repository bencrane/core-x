import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Standalone Blitz Phone Finder.
 *
 * A single-purpose, decoupled mobile-phone resolver: take known identities
 * (person_linkedin_url) and resolve a direct mobile number via BlitzAPI's
 * /v2/enrichment/phone. The structural twin of the Blitz Email Finder, writing into
 * ops.phone_resolutions (this pipeline's mobile-phone system-of-record).
 *
 * Task surface (the only id callers trigger):
 *   blitzPhoneFinderResolve   contacts[] → resolved mobile phones (found|unresolved)
 *
 * No MillionVerifier analog. Blitz phone enrichment returns a direct mobile number and
 * is itself terminal — there is no second-vendor gate the way MV arbitrates email.
 * Outcome is binary: found (Blitz returned a number) vs unresolved (Blitz miss, a
 * non-US skip, or a contact with no person_linkedin_url).
 *
 * US-only coverage. Blitz phone is United States only. A contact carrying a
 * country_code other than "US" is recorded unresolved by the worker WITHOUT a Blitz
 * call (credit-free skip); a contact with no country_code is attempted.
 *
 * Fan-out durable-callback pattern. The resolver chunks the batch and fans out one
 * CHILD run per chunk via batchTriggerAndWait — a SINGLE batch waitpoint in the
 * parent. Each child (blitzPhoneFinderChunk) mints its own waitpoint token, POSTs
 * the Universal Dispatcher with the blitz-phone-finder worker + callback url, and
 * suspends on exactly one wait.forToken (zero compute while suspended), resuming
 * from the worker's RAW terminal-counts callback. The parent aggregates child
 * outputs once the whole batch completes.
 *
 * Why a child, not Promise.all over wait.forToken in one run: Trigger.dev forbids
 * concurrent waitpoints within a single run — `Promise.all(chunks.map(... wait
 * .forToken ...))` trips TASK_DID_CONCURRENT_WAIT ("Parallel waits are not
 * supported") and system-fails any payload with >1 chunk in ~1s. batchTriggerAndWait
 * is the Trigger-sanctioned parallel primitive: one batch waitpoint in the parent,
 * N independent child runs each holding their own single waitpoint.
 *
 * DECOUPLED + GATEWAY-ROUTED (Directive 23/28 §1). Every Blitz phone call is routed by
 * the worker through the single-container core/blitz_gateway.py — the global ≤5-RPS
 * priority egress. These bulk calls ride the LOW (or NORMAL) lane so interactive GTM
 * tasks (HIGH) are never starved. The worker holds no Blitz key; the gateway governs.
 * Trigger carries only signals — the callback body is terminal COUNTS, never phones.
 */

interface Contact {
  contact_id: string;
  person_linkedin_url?: string; // REQUIRED for a Blitz phone hit; absent → unresolved
  company_domain?: string;
  country_code?: string; // US-only gate; non-"US" is skipped (unresolved) without a Blitz call
  first_name?: string;
  last_name?: string;
  company_name?: string;
}

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface BlitzPhoneCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  found: number;
  unresolved: number;
  failed: number;
  gateway_calls: number;
  error?: string | null;
}

interface BlitzPhonePayload {
  contacts: Contact[];
  batchLabel?: string;
  priority?: "low" | "normal"; // gateway lane (bulk); never HIGH. default "low"
  force?: boolean; // re-resolve contacts already marked found (default: skip them)
  chunkSize?: number; // contacts per Modal invocation (default 250)
}

// One Modal chunk-worker per child run. parentRunId + index reconstruct the stable
// worker-facing run_id (`${parentRunId}:${index}`) — identical to the pre-fan-out
// scheme, so the worker's dedupe/idempotency on run_id is unchanged.
interface BlitzPhoneChunkPayload {
  contacts: Contact[];
  batchLabel: string | null;
  priority: "low" | "normal";
  force: boolean;
  parentRunId: string;
  index: number;
}

const RESOLVE_CONCURRENCY = 6; // admitted resolver orchestrations; each is checkpointed (zero compute) while its chunks fan out
const CHUNK_CONCURRENCY = 6; // coarse Modal-container budget on chunk-workers; the gateway is the hard 5-RPS governor
const DEFAULT_CHUNK_SIZE = 250; // bounds per-worker wall-clock under maxDuration

const ZERO: Omit<BlitzPhoneCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  found: 0,
  unresolved: 0,
  failed: 0,
  gateway_calls: 0,
};

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

// CHILD — resolve one chunk. Exactly one wait.forToken per run, so N chunks fan out
// as N independent runs with no concurrent waitpoint in any single run. The
// queue.concurrencyLimit here is the real container-budget cap across all parents.
export const blitzPhoneFinderChunk = task({
  id: "blitz-phone-finder-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: BlitzPhoneChunkPayload): Promise<BlitzPhoneCallback> => {
    const { contacts, batchLabel, priority, force, parentRunId, index } = payload;

    // 1) Mint the durable callback token (callbackHash in the url authenticates).
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["blitz-phone-finder", `chunk-${index}`],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202).
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "blitz-phone-finder",
        function_name: "run_phone_finder",
        kwargs: {
          contacts,
          batch_label: batchLabel,
          run_id: `${parentRunId}:${index}`,
          priority,
          force,
        },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("blitz-phone-finder chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: contacts.length,
      priority,
      tokenId: token.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<BlitzPhoneCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `blitz-phone-finder chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `blitz-phone-finder chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const blitzPhoneFinderResolve = task({
  id: "blitz-phone-finder-resolve",
  queue: { concurrencyLimit: RESOLVE_CONCURRENCY },
  maxDuration: 14400, // 4h ceiling; suspended at the batch waitpoint consumes no compute
  run: async (payload: BlitzPhonePayload, { ctx }) => {
    const contacts = (payload.contacts ?? []).filter((c) => c && c.contact_id);
    if (contacts.length === 0) {
      return { status: "success", feed: "blitz_phone_finder", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = payload.chunkSize ?? DEFAULT_CHUNK_SIZE;
    const priority = payload.priority === "normal" ? "normal" : "low";
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(contacts, size);

    logger.info("blitz-phone-finder resolve starting", {
      contacts: contacts.length,
      chunks: chunks.length,
      chunkSize: size,
      priority,
    });

    // Fan out one child run per chunk under a SINGLE batch waitpoint. This is the
    // Trigger-sanctioned parallel primitive — a Promise.all over wait.forToken would
    // trip TASK_DID_CONCURRENT_WAIT. CHUNK_CONCURRENCY (child queue) bounds how many
    // workers are alive; the gateway is the hard 5-RPS governor.
    const { runs } = await blitzPhoneFinderChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { contacts: slice, batchLabel, priority, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    // Aggregate terminal counts across chunks; fail the whole resolve if any chunk
    // run failed (mirrors the prior Promise.all fail-fast semantics).
    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(
          `blitz-phone-finder chunk run ${run.id} failed: ${JSON.stringify(run.error)}`,
        );
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.found += c.found;
      totals.unresolved += c.unresolved;
      totals.failed += c.failed;
      totals.gateway_calls += c.gateway_calls;
    }

    logger.info("blitz-phone-finder resolve complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "blitz_phone_finder", batch_label: batchLabel, ...totals };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
