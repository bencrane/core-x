import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Standalone LeadMagic Phone Finder.
 *
 * The LeadMagic twin of src/trigger/blitz_phone_finder.ts: take known identities
 * (person_linkedin_url and/or work_email) and resolve a direct mobile number via
 * LeadMagic's POST /mobile-finder, writing into ops.phone_resolutions (the SAME mobile
 * system-of-record the Blitz finder uses; source_vendor distinguishes). Stood up as a
 * decoupled vendor path because Blitz phone is gateway-routed + US-only + metered, whereas
 * LeadMagic is a separate key, has international coverage, and charges only on a HIT.
 *
 * Task surface (the only id callers trigger):
 *   leadmagicPhoneFinderResolve   contacts[] → resolved mobile phones (found|unresolved)
 *
 * No MillionVerifier analog (mobile is terminal). No US gate (LeadMagic has intl coverage
 * and misses cost 0 credits, so there is nothing to pre-skip).
 *
 * Fan-out durable-callback pattern (identical to the Blitz phone finder). The resolver
 * chunks the batch and fans out one CHILD run per chunk via batchTriggerAndWait — a single
 * batch waitpoint in the parent. Each child (leadmagicPhoneFinderChunk) mints its own
 * waitpoint token, POSTs the Universal Dispatcher with the leadmagic-phone-finder worker +
 * callback url, and suspends on exactly one wait.forToken (zero compute while suspended),
 * resuming from the worker's RAW terminal-counts callback.
 *
 * DECOUPLED (no gateway). Unlike Blitz, the Modal worker (app "leadmagic-phone-finder", fn
 * "run_leadmagic_phone") holds the LeadMagic key (Modal secret "leadmagic-api") and calls
 * LeadMagic directly with per-call 429/5xx retry. Trigger carries only signals — the
 * callback body is terminal COUNTS (incl. credits_consumed), never phones.
 */

interface Contact {
  contact_id: string;
  person_linkedin_url?: string; // profile_url; one of these three is REQUIRED for a hit
  work_email?: string;
  personal_email?: string;
  company_domain?: string;
  country_code?: string; // carried for the SoR; LeadMagic does NOT gate on it (intl coverage)
  first_name?: string;
  last_name?: string;
  company_name?: string;
}

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface LeadMagicPhoneCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  found: number;
  unresolved: number;
  failed: number;
  api_calls: number;
  credits_consumed: number;
  error?: string | null;
}

interface LeadMagicPhonePayload {
  contacts: Contact[];
  batchLabel?: string;
  priority?: "low" | "normal"; // ledger parity only; LeadMagic has no lane. default "low"
  force?: boolean; // re-resolve contacts already marked found by ANY vendor (default: skip)
  chunkSize?: number; // contacts per Modal invocation (default 250)
}

interface LeadMagicPhoneChunkPayload {
  contacts: Contact[];
  batchLabel: string | null;
  priority: "low" | "normal";
  force: boolean;
  parentRunId: string;
  index: number;
}

const RESOLVE_CONCURRENCY = 6;
const CHUNK_CONCURRENCY = 6; // coarse Modal-container budget; LeadMagic 429s are retried in-worker
const DEFAULT_CHUNK_SIZE = 250;

const ZERO: Omit<LeadMagicPhoneCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  found: 0,
  unresolved: 0,
  failed: 0,
  api_calls: 0,
  credits_consumed: 0,
};

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

// CHILD — resolve one chunk. Exactly one wait.forToken per run.
export const leadmagicPhoneFinderChunk = task({
  id: "leadmagic-phone-finder-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 3900,
  run: async (payload: LeadMagicPhoneChunkPayload): Promise<LeadMagicPhoneCallback> => {
    const { contacts, batchLabel, priority, force, parentRunId, index } = payload;

    const token = await wait.createToken({
      timeout: "1h",
      tags: ["leadmagic-phone-finder", `chunk-${index}`],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "leadmagic-phone-finder",
        function_name: "run_leadmagic_phone",
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

    logger.info("leadmagic-phone-finder chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: contacts.length,
      tokenId: token.id,
    });

    const result = await wait.forToken<LeadMagicPhoneCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `leadmagic-phone-finder chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `leadmagic-phone-finder chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const leadmagicPhoneFinderResolve = task({
  id: "leadmagic-phone-finder-resolve",
  queue: { concurrencyLimit: RESOLVE_CONCURRENCY },
  maxDuration: 14400,
  run: async (payload: LeadMagicPhonePayload, { ctx }) => {
    const contacts = (payload.contacts ?? []).filter((c) => c && c.contact_id);
    if (contacts.length === 0) {
      return { status: "success", feed: "leadmagic_phone_finder", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = payload.chunkSize ?? DEFAULT_CHUNK_SIZE;
    const priority = payload.priority === "normal" ? "normal" : "low";
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(contacts, size);

    logger.info("leadmagic-phone-finder resolve starting", {
      contacts: contacts.length,
      chunks: chunks.length,
      chunkSize: size,
    });

    const { runs } = await leadmagicPhoneFinderChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { contacts: slice, batchLabel, priority, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(
          `leadmagic-phone-finder chunk run ${run.id} failed: ${JSON.stringify(run.error)}`,
        );
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.found += c.found;
      totals.unresolved += c.unresolved;
      totals.failed += c.failed;
      totals.api_calls += c.api_calls;
      totals.credits_consumed += c.credits_consumed;
    }

    logger.info("leadmagic-phone-finder resolve complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "leadmagic_phone_finder", batch_label: batchLabel, ...totals };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
