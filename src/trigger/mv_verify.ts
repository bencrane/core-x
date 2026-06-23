import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Standalone MillionVerifier Email Verifier.
 *
 * A single-purpose resolver: take a KNOWN work email (e.g. gtm.contacts.work_email) and
 * resolve its deliverability via MillionVerifier — the sole arbiter. The structural twin
 * of the Blitz Email Finder, MINUS the email-FINDING step: the email is supplied by the
 * caller, so there is no Blitz/Icypeas/LeadMagic call and no rate-governed gateway — MV
 * is the only egress. Writes into ops.email_verifications (this pipeline's SoR), which a
 * downstream materializer mirrors to native Lance.
 *
 * Task surface (the only id callers trigger):
 *   mvVerifyResolve   contacts[] → verified work emails (verified|risky|unresolved)
 *
 * House rubric on MV resultcode: 1 ok → verified | 2 catch_all / 3 unknown → risky |
 * 4/5/6 → unresolved | no terminal verdict → unresolved (fail-closed).
 *
 * Fan-out durable-callback pattern. The resolver chunks the batch and fans out one CHILD
 * run per chunk via batchTriggerAndWait — a SINGLE batch waitpoint in the parent. Each
 * child (mvVerifyChunk) mints its own waitpoint token, POSTs the Universal Dispatcher with
 * the mv-verify worker + callback url, and suspends on exactly one wait.forToken (zero
 * compute while suspended), resuming from the worker's RAW terminal-counts callback. The
 * parent aggregates child outputs once the whole batch completes.
 *
 * Why a child, not Promise.all over wait.forToken in one run: Trigger.dev forbids
 * concurrent waitpoints within a single run — Promise.all over wait.forToken trips
 * TASK_DID_CONCURRENT_WAIT. batchTriggerAndWait is the Trigger-sanctioned parallel
 * primitive: one batch waitpoint in the parent, N independent child runs.
 *
 * Trigger carries only signals — the callback body is terminal COUNTS, never emails.
 */

interface Contact {
  contact_id: string;
  email: string; // REQUIRED — the work email to verify; a contact without one is unresolved
  company_domain?: string;
  source?: string; // provenance of the input email (e.g. "gtm.contacts")
}

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface MvVerifyCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  verified: number;
  risky: number;
  unresolved: number;
  failed: number;
  mv_calls: number;
  error?: string | null;
}

interface MvVerifyPayload {
  contacts: Contact[];
  batchLabel?: string;
  force?: boolean; // re-verify contacts already marked verified (default: skip them)
  chunkSize?: number; // contacts per Modal invocation (default 250)
}

// One Modal chunk-worker per child run. parentRunId + index reconstruct the stable
// worker-facing run_id (`${parentRunId}:${index}`).
interface MvVerifyChunkPayload {
  contacts: Contact[];
  batchLabel: string | null;
  force: boolean;
  parentRunId: string;
  index: number;
}

const RESOLVE_CONCURRENCY = 6; // admitted resolver orchestrations; each is checkpointed (zero compute) while its chunks fan out
const CHUNK_CONCURRENCY = 6; // coarse Modal-container budget on chunk-workers
const DEFAULT_CHUNK_SIZE = 250; // bounds per-worker wall-clock under maxDuration

const ZERO: Omit<MvVerifyCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  verified: 0,
  risky: 0,
  unresolved: 0,
  failed: 0,
  mv_calls: 0,
};

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

// CHILD — verify one chunk. Exactly one wait.forToken per run, so N chunks fan out as N
// independent runs with no concurrent waitpoint in any single run.
export const mvVerifyChunk = task({
  id: "mv-verify-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: MvVerifyChunkPayload): Promise<MvVerifyCallback> => {
    const { contacts, batchLabel, force, parentRunId, index } = payload;

    // 1) Mint the durable callback token (callbackHash in the url authenticates).
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["mv-verify", `chunk-${index}`],
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
        app_name: "mv-verify",
        function_name: "run_mv_verify",
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

    logger.info("mv-verify chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: contacts.length,
      tokenId: token.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<MvVerifyCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `mv-verify chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `mv-verify chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const mvVerifyResolve = task({
  id: "mv-verify-resolve",
  queue: { concurrencyLimit: RESOLVE_CONCURRENCY },
  maxDuration: 14400, // 4h ceiling; suspended at the batch waitpoint consumes no compute
  run: async (payload: MvVerifyPayload, { ctx }) => {
    const contacts = (payload.contacts ?? []).filter((c) => c && c.contact_id);
    if (contacts.length === 0) {
      return { status: "success", feed: "mv_verify", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = payload.chunkSize ?? DEFAULT_CHUNK_SIZE;
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(contacts, size);

    logger.info("mv-verify resolve starting", {
      contacts: contacts.length,
      chunks: chunks.length,
      chunkSize: size,
    });

    // Fan out one child run per chunk under a SINGLE batch waitpoint.
    const { runs } = await mvVerifyChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { contacts: slice, batchLabel, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    // Aggregate terminal counts across chunks; fail the whole resolve if any chunk failed.
    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(
          `mv-verify chunk run ${run.id} failed: ${JSON.stringify(run.error)}`,
        );
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.verified += c.verified;
      totals.risky += c.risky;
      totals.unresolved += c.unresolved;
      totals.failed += c.failed;
      totals.mv_calls += c.mv_calls;
    }

    logger.info("mv-verify resolve complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "mv_verify", batch_label: batchLabel, ...totals };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
