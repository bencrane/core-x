import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Icypeas+MillionVerifier BULK Work-Email rail (icypeas + millionverifier only).
 *
 * A single-finder batch pipeline: Icypeas bulk email-search → MillionVerifier (the sole
 * deliverability arbiter). No LeadMagic, no Blitz. This is the go-forward BULK path — one
 * Icypeas /bulk-search launch resolves up to 5000 contacts that the per-contact submit+poll
 * cascade would need 5000 submits + ≥5000 reads to cover (~100× the read-bound throughput).
 *
 * Task surface (the only id callers trigger):
 *   icypeasMvResolve   contacts[] → resolved work emails (verified|risky|unresolved)
 *
 * Fan-out durable-callback pattern (mirror src/trigger/enrichment_email_cascade.ts). The
 * resolver chunks the batch at the Icypeas 5000-row bulk ceiling and fans out one CHILD run
 * per chunk via batchTriggerAndWait — a SINGLE batch waitpoint in the parent. Each child
 * (icypeasMvChunk) mints its own waitpoint token, POSTs the Universal Dispatcher with the
 * enrichment-icypeas-mv worker + callback url, and suspends on exactly one wait.forToken
 * (zero compute while the Modal worker launches the bulk search, polls it to done, drains the
 * results, and MV-verifies). The parent aggregates child terminal COUNTS once the batch done.
 *
 * Why a child, not Promise.all over wait.forToken in one run: Trigger.dev forbids concurrent
 * waitpoints within a single run (TASK_DID_CONCURRENT_WAIT). batchTriggerAndWait is the
 * sanctioned parallel primitive — one batch waitpoint in the parent, N independent children.
 *
 * Rate governance lives entirely in the Modal core/icypeas_gateway.py bulk functions
 * (launch 1/s, file-status 15/min, drain 30/min — single-container global buckets).
 * CHUNK_CONCURRENCY is only the coarse Modal-container budget on chunk-workers; the gateway
 * is the hard rate governor. Trigger carries only signals: the callback body is terminal
 * COUNTS, never email rows. Results land in ops.email_resolutions (the work-email SoR).
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
interface IcypeasMvCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  ineligible: number;
  verified: number;
  risky: number;
  unresolved: number;
  failed: number;
  error?: string | null;
}

interface IcypeasMvPayload {
  contacts: Contact[];
  batchLabel?: string;
  force?: boolean; // re-resolve contacts already marked verified (default: skip them)
  chunkSize?: number; // contacts per bulk launch (default 5000 = Icypeas bulk ceiling)
}

// One Modal chunk-worker per child run. parentRunId + index reconstruct the stable
// worker-facing run_id (`${parentRunId}:${index}`) for the worker's ops idempotency.
interface IcypeasMvChunkPayload {
  contacts: Contact[];
  batchLabel: string | null;
  force: boolean;
  parentRunId: string;
  index: number;
}

const RESOLVE_CONCURRENCY = 6; // admitted resolver orchestrations; each checkpointed while its chunks fan out
const CHUNK_CONCURRENCY = 4; // coarse Modal-container budget on chunk-workers; the gateway is the rate governor
const DEFAULT_CHUNK_SIZE = 5000; // Icypeas /bulk-search hard ceiling — one launch per chunk

const ZERO: Omit<IcypeasMvCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  ineligible: 0,
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

// CHILD — resolve one ≤5000-contact chunk as ONE Icypeas bulk search. Exactly one
// wait.forToken per run. The worker (enrichment-icypeas-mv::run_bulk) launches the bulk
// search, polls to done, drains results, MV-verifies, upserts, and POSTs the terminal
// counts back to this token.
export const icypeasMvChunk = task({
  id: "enrichment-icypeas-mv-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 7500, // ~2h5m; exceeds the 2h token timeout with margin (suspended waits are free)
  run: async (payload: IcypeasMvChunkPayload): Promise<IcypeasMvCallback> => {
    const { contacts, batchLabel, force, parentRunId, index } = payload;

    // 1) Mint the durable callback token. Bulk processing (launch → poll-done → drain → MV)
    //    can run many minutes; 2h covers the worker's poll ceiling + drain + verify with margin.
    const token = await wait.createToken({
      timeout: "2h",
      tags: ["icypeas-mv", `chunk-${index}`],
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
        app_name: "enrichment-icypeas-mv",
        function_name: "run_bulk",
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

    logger.info("icypeas-mv chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: contacts.length,
      tokenId: token.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<IcypeasMvCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `icypeas-mv chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `icypeas-mv chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const icypeasMvResolve = task({
  id: "enrichment-icypeas-mv-resolve",
  queue: { concurrencyLimit: RESOLVE_CONCURRENCY },
  maxDuration: 14400, // 4h ceiling; suspended at the batch waitpoint consumes no compute
  run: async (payload: IcypeasMvPayload, { ctx }) => {
    const contacts = (payload.contacts ?? []).filter((c) => c && c.contact_id);
    if (contacts.length === 0) {
      return { status: "success", feed: "icypeas_mv", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = Math.min(payload.chunkSize ?? DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(contacts, size);

    logger.info("icypeas-mv resolve starting", {
      contacts: contacts.length,
      chunks: chunks.length,
      chunkSize: size,
    });

    // Fan out one child run per chunk under a SINGLE batch waitpoint (the Trigger-sanctioned
    // parallel primitive; a Promise.all over wait.forToken would trip TASK_DID_CONCURRENT_WAIT).
    const { runs } = await icypeasMvChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { contacts: slice, batchLabel, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    // Aggregate terminal counts; fail the whole resolve if any chunk run failed.
    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(
          `icypeas-mv chunk run ${run.id} failed: ${JSON.stringify(run.error)}`,
        );
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.ineligible += c.ineligible;
      totals.verified += c.verified;
      totals.risky += c.risky;
      totals.unresolved += c.unresolved;
      totals.failed += c.failed;
    }

    logger.info("icypeas-mv resolve complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "icypeas_mv", batch_label: batchLabel, ...totals };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
