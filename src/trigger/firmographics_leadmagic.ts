import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Standalone LeadMagic Company Enrichment (firmographics).
 *
 * The firmographic twin of src/trigger/leadmagic_phone_finder.ts: take a company identity
 * (company_domain and/or company_linkedin_url and/or company_name) and enrich it to
 * firmographic data via LeadMagic's POST /company-search (the wire endpoint LeadMagic markets
 * as "Company Enrichment"; operationId searchCompany). Stood up as a DECOUPLED vendor source,
 * NOT folded into the Blitz firmographic path: LeadMagic is a separate key, has its own limits,
 * and charges only on a HIT.
 *
 * Two-stage, two task surfaces:
 *   firmographicsLeadmagicEnrich      entities[] → captured firmographics (found|not_found)
 *                                     STAGE 1: fan-out capture into ops.firmographics_leadmagic_capture
 *   firmographicsLeadmagicMaterialize (no args)  → project + dedup capture → Lance SoR
 *                                     STAGE 2: s3://data-sink/active/firmographics_leadmagic/
 *
 * DECOUPLED (no gateway). Unlike Blitz, the capture Modal worker (app
 * "firmographics-leadmagic-capture", fn "run_leadmagic_company") holds the LeadMagic key (Modal
 * secret "leadmagic-api") and calls LeadMagic directly with per-call 429/5xx retry. Trigger
 * carries only signals — the callback body is terminal COUNTS (incl. credits_consumed), never
 * firmo rows. Capture and materialize are split so vendor-spend blast radius never touches the
 * Lance write path.
 *
 * Fan-out durable-callback pattern (identical to the LeadMagic phone finder). The enrich resolver
 * chunks the batch and fans out one CHILD run per chunk via batchTriggerAndWait; each child mints
 * its own waitpoint token, POSTs the Universal Dispatcher with the worker + callback url, and
 * suspends on exactly one wait.forToken (zero compute while suspended), resuming from the worker's
 * RAW terminal-counts callback.
 */

interface Entity {
  entity_id: string;
  company_domain?: string; // one of these three is REQUIRED for a hit
  company_linkedin_url?: string; // → LeadMagic profile_url
  company_name?: string;
}

// ── STAGE 1: capture ────────────────────────────────────────────────────────────────────────
// The RAW body the capture Modal worker POSTs to the waitpoint url becomes result.output.
interface LeadMagicCompanyCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  found: number;
  not_found: number;
  failed: number;
  api_calls: number;
  credits_consumed: number;
  error?: string | null;
}

interface LeadMagicCompanyPayload {
  entities: Entity[];
  batchLabel?: string;
  priority?: "low" | "normal"; // ledger parity only; LeadMagic has no lane. default "low"
  force?: boolean; // re-enrich entities already marked found (default: skip)
  chunkSize?: number; // entities per Modal invocation (default 250)
}

interface LeadMagicCompanyChunkPayload {
  entities: Entity[];
  batchLabel: string | null;
  priority: "low" | "normal";
  force: boolean;
  parentRunId: string;
  index: number;
}

const RESOLVE_CONCURRENCY = 6;
const CHUNK_CONCURRENCY = 6; // coarse Modal-container budget; LeadMagic 429s are retried in-worker
const DEFAULT_CHUNK_SIZE = 250;

const ZERO: Omit<LeadMagicCompanyCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  found: 0,
  not_found: 0,
  failed: 0,
  api_calls: 0,
  credits_consumed: 0,
};

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

// CHILD — capture one chunk. Exactly one wait.forToken per run.
export const firmographicsLeadmagicEnrichChunk = task({
  id: "firmographics-leadmagic-enrich-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 3900,
  run: async (payload: LeadMagicCompanyChunkPayload): Promise<LeadMagicCompanyCallback> => {
    const { entities, batchLabel, priority, force, parentRunId, index } = payload;

    const token = await wait.createToken({
      timeout: "1h",
      tags: ["firmographics-leadmagic", `chunk-${index}`],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "firmographics-leadmagic-capture",
        function_name: "run_leadmagic_company",
        kwargs: {
          entities,
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

    logger.info("firmographics-leadmagic chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: entities.length,
      tokenId: token.id,
    });

    const result = await wait.forToken<LeadMagicCompanyCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `firmographics-leadmagic chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `firmographics-leadmagic chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const firmographicsLeadmagicEnrich = task({
  id: "firmographics-leadmagic-enrich",
  queue: { concurrencyLimit: RESOLVE_CONCURRENCY },
  maxDuration: 14400,
  run: async (payload: LeadMagicCompanyPayload, { ctx }) => {
    const entities = (payload.entities ?? []).filter((e) => e && e.entity_id);
    if (entities.length === 0) {
      return { status: "success", feed: "firmographics_leadmagic", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = payload.chunkSize ?? DEFAULT_CHUNK_SIZE;
    const priority = payload.priority === "normal" ? "normal" : "low";
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(entities, size);

    logger.info("firmographics-leadmagic enrich starting", {
      entities: entities.length,
      chunks: chunks.length,
      chunkSize: size,
    });

    const { runs } = await firmographicsLeadmagicEnrichChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { entities: slice, batchLabel, priority, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(
          `firmographics-leadmagic chunk run ${run.id} failed: ${JSON.stringify(run.error)}`,
        );
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.found += c.found;
      totals.not_found += c.not_found;
      totals.failed += c.failed;
      totals.api_calls += c.api_calls;
      totals.credits_consumed += c.credits_consumed;
    }

    logger.info("firmographics-leadmagic enrich complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "firmographics_leadmagic", batch_label: batchLabel, ...totals };
  },
});

// ── STAGE 2: materialize ──────────────────────────────────────────────────────────────────────
interface LeadMagicMaterializeCallback {
  status: "success" | "error";
  feed: string;
  source_db: string;
  rows_total: number;
  rows_source: number;
  datasets: Record<string, number>;
  error?: string | null;
}

// Project + dedup ops.firmographics_leadmagic_capture → Lance SoR. No fan-out — one Modal run.
export const firmographicsLeadmagicMaterialize = task({
  id: "firmographics-leadmagic-materialize",
  queue: { concurrencyLimit: 1 }, // single-container materializer; one run at a time
  maxDuration: 2100,
  run: async (_payload: Record<string, never>, { ctx }): Promise<LeadMagicMaterializeCallback> => {
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["firmographics-leadmagic", "materialize"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "firmographics-leadmagic",
        function_name: "run",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("firmographics-leadmagic materialize dispatched; suspending on waitpoint", {
      tokenId: token.id,
      runId: ctx.run.id,
    });

    const result = await wait.forToken<LeadMagicMaterializeCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `firmographics-leadmagic materialize timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `firmographics-leadmagic materialize failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    logger.info("firmographics-leadmagic materialize complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
