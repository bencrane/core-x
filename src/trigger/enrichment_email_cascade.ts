import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Work-Email Waterfall (Directive 21-Final).
 *
 * Decoupled email resolution: Blitz is REMOVED. The waterfall is strictly
 * Icypeas → LeadMagic, with MillionVerifier as the sole deliverability arbiter
 * after every vendor hit (the rubric lives entirely in the Modal worker).
 *
 * Task surface (the only id callers trigger):
 *   emailCascadeResolve   contacts[] → resolved work emails (verified|risky|unresolved)
 *
 * Fan-out durable-callback pattern (mirror src/trigger/blitz_email_finder.ts). The
 * resolver chunks the batch and fans out one CHILD run per chunk via
 * batchTriggerAndWait — a SINGLE batch waitpoint in the parent. Each child
 * (emailCascadeChunk) mints its own waitpoint token, POSTs the Universal Dispatcher
 * with the enrichment-email-cascade worker + callback url, and suspends on exactly
 * one wait.forToken (zero compute while suspended), resuming from the worker's RAW
 * terminal-counts callback. The parent aggregates child outputs once the whole batch
 * completes.
 *
 * Why a child, not Promise.all over wait.forToken in one run: Trigger.dev forbids
 * concurrent waitpoints within a single run — `Promise.all(chunks.map(... wait
 * .forToken ...))` trips TASK_DID_CONCURRENT_WAIT ("Parallel waits are not
 * supported") and system-fails any payload with >1 chunk in ~1s. batchTriggerAndWait
 * is the Trigger-sanctioned parallel primitive: one batch waitpoint in the parent,
 * N independent child runs each holding their own single waitpoint.
 *
 * Two-tier throttle:
 *   - FINE / hard: the single-container core/icypeas_gateway.py token buckets cap
 *     global Icypeas egress (submit ≤10/s, read ≤30/min) regardless of how many
 *     chunk-workers are in flight. LeadMagic + MillionVerifier run elastically —
 *     their volume is bounded below Icypeas's 30/min read ceiling by construction.
 *   - COARSE: the chunk task's queue.concurrencyLimit bounds how many chunk-workers
 *     are alive at once (Modal container budget). It is NOT the rate governor — the
 *     gateway is.
 *
 * Trigger carries only signals: the callback body is terminal COUNTS, never email
 * rows. Results land in hq-x ops.email_resolutions (the work-email system-of-record).
 */

interface Contact {
  contact_id: string;
  first_name?: string;
  last_name?: string;
  company_domain?: string;
  company_name?: string;
  person_linkedin_url?: string;
}

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface EmailCascadeCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  verified: number;
  risky: number;
  unresolved: number;
  failed: number;
  error?: string | null;
}

interface EmailCascadePayload {
  contacts: Contact[];
  batchLabel?: string;
  force?: boolean; // re-resolve contacts already marked verified (default: skip them)
  chunkSize?: number; // contacts per Modal invocation (default 250)
}

// One Modal chunk-worker per child run. parentRunId + index reconstruct the stable
// worker-facing run_id (`${parentRunId}:${index}`) — identical to the pre-fan-out
// scheme, so the worker's dedupe/idempotency on run_id is unchanged.
interface EmailCascadeChunkPayload {
  contacts: Contact[];
  batchLabel: string | null;
  force: boolean;
  parentRunId: string;
  index: number;
}

const RESOLVE_CONCURRENCY = 6; // admitted resolver orchestrations; each is checkpointed (zero compute) while its chunks fan out
const CHUNK_CONCURRENCY = 6; // coarse Modal-container budget on chunk-workers; the gateway is the hard rate governor
const DEFAULT_CHUNK_SIZE = 250; // bounds per-worker wall-clock under maxDuration (serial Icypeas poll)

const ZERO: Omit<EmailCascadeCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  verified: 0,
  risky: 0,
  unresolved: 0,
  failed: 0,
};

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

// CHILD — resolve one chunk. Exactly one wait.forToken per run, so N chunks fan out
// as N independent runs with no concurrent waitpoint in any single run. The
// queue.concurrencyLimit here is the real container-budget cap across all parents.
export const emailCascadeChunk = task({
  id: "enrichment-email-cascade-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: EmailCascadeChunkPayload): Promise<EmailCascadeCallback> => {
    const { contacts, batchLabel, force, parentRunId, index } = payload;

    // 1) Mint the durable callback token (callbackHash in the url authenticates).
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["email-cascade", `chunk-${index}`],
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
        app_name: "enrichment-email-cascade",
        function_name: "run_cascade",
        kwargs: {
          contacts,
          batch_label: batchLabel,
          run_id: `${parentRunId}:${index}`,
          force,
        },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("email-cascade chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: contacts.length,
      tokenId: token.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<EmailCascadeCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `email-cascade chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `email-cascade chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const emailCascadeResolve = task({
  id: "enrichment-email-cascade-resolve",
  queue: { concurrencyLimit: RESOLVE_CONCURRENCY },
  maxDuration: 14400, // 4h ceiling; suspended at the batch waitpoint consumes no compute
  run: async (payload: EmailCascadePayload, { ctx }) => {
    const contacts = (payload.contacts ?? []).filter((c) => c && c.contact_id);
    if (contacts.length === 0) {
      return { status: "success", feed: "email_cascade", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = payload.chunkSize ?? DEFAULT_CHUNK_SIZE;
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(contacts, size);

    logger.info("email-cascade resolve starting", {
      contacts: contacts.length,
      chunks: chunks.length,
      chunkSize: size,
    });

    // Fan out one child run per chunk under a SINGLE batch waitpoint. This is the
    // Trigger-sanctioned parallel primitive — a Promise.all over wait.forToken would
    // trip TASK_DID_CONCURRENT_WAIT. CHUNK_CONCURRENCY (child queue) bounds how many
    // workers are alive; the gateway is the hard rate governor.
    const { runs } = await emailCascadeChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { contacts: slice, batchLabel, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    // Aggregate terminal counts across chunks; fail the whole resolve if any chunk
    // run failed (mirrors the prior Promise.all fail-fast semantics).
    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(
          `email-cascade chunk run ${run.id} failed: ${JSON.stringify(run.error)}`,
        );
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.verified += c.verified;
      totals.risky += c.risky;
      totals.unresolved += c.unresolved;
      totals.failed += c.failed;
    }

    logger.info("email-cascade resolve complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "email_cascade", batch_label: batchLabel, ...totals };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
