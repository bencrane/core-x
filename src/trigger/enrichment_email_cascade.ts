import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Work-Email Waterfall (Directive 21-Final).
 *
 * Decoupled email resolution: Blitz is REMOVED. The waterfall is strictly
 * Icypeas → LeadMagic, with MillionVerifier as the sole deliverability arbiter
 * after every vendor hit (the rubric lives entirely in the Modal worker).
 *
 * One task surface:
 *   emailCascadeResolve   contacts[] → resolved work emails (verified|risky|unresolved)
 *
 * Canonical durable-callback pattern (mirror src/trigger/enrichment_blitz.ts):
 * chunk the batch, mint one waitpoint token per chunk, POST the Universal
 * Dispatcher with the enrichment-email-cascade worker + callback url, suspend on
 * wait.forToken (zero compute while suspended), resume from the worker's RAW
 * terminal-counts callback, aggregate across chunks.
 *
 * Two-tier throttle:
 *   - FINE / hard: the single-container core/icypeas_gateway.py token buckets cap
 *     global Icypeas egress (submit ≤10/s, read ≤30/min) regardless of how many
 *     chunk-workers are in flight. LeadMagic + MillionVerifier run elastically —
 *     their volume is bounded below Icypeas's 30/min read ceiling by construction.
 *   - COARSE: queue.concurrencyLimit bounds how many chunk-workers are alive at
 *     once (Modal container budget). It is NOT the rate governor — the gateway is.
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

const QUEUE_CONCURRENCY = 6; // coarse container-budget cap; the gateway is the hard rate governor
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

async function dispatchChunkAndWait(
  contacts: Contact[],
  batchLabel: string | null,
  force: boolean,
  runId: string,
  index: number,
): Promise<EmailCascadeCallback> {
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
        run_id: `${runId}:${index}`,
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
}

export const emailCascadeResolve = task({
  id: "enrichment-email-cascade-resolve",
  queue: { concurrencyLimit: QUEUE_CONCURRENCY },
  maxDuration: 14400, // 4h ceiling; suspended waitpoints consume no compute
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

    const settled = await Promise.all(
      chunks.map((slice, i) => dispatchChunkAndWait(slice, batchLabel, force, ctx.run.id, i)),
    );

    // Aggregate terminal counts across chunks.
    const totals = settled.reduce(
      (acc, c) => ({
        requested: acc.requested + c.requested,
        skipped: acc.skipped + c.skipped,
        verified: acc.verified + c.verified,
        risky: acc.risky + c.risky,
        unresolved: acc.unresolved + c.unresolved,
        failed: acc.failed + c.failed,
      }),
      { ...ZERO },
    );

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
