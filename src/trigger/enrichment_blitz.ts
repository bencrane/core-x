import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Atomic Enrichment (Blitz) Workflows A/B/C (Directive 23).
 *
 * Three on-demand task surfaces the interactive GTM agent triggers to patch a
 * single missing firmographic hop for a company cohort:
 *
 *   enrichmentResolveDomains  Workflow A  domain → company_linkedin_url
 *   enrichmentEnrichLinkedin  Workflow B  company_linkedin_url → firmographics
 *   enrichmentCascade         Workflow C  domain → linkedin → firmographics
 *
 * Each follows the canonical durable-callback pattern (mirror
 * src/trigger/sam_opps_bulk.ts): mint a waitpoint token, POST the Universal
 * Dispatcher with the target enrichment-blitz worker + the callback url, suspend
 * on wait.forToken (zero compute while suspended), resume from the worker's RAW
 * terminal-counts callback.
 *
 * Two-tier throttle (Directive 23 §3.3):
 *   - FINE / hard: the single-container core/blitz_gateway.py token bucket caps
 *     global Blitz egress at ≤5 RPS regardless of how many runs are in flight.
 *   - COARSE: the per-workflow `queue.concurrencyLimit` below bounds how many
 *     chunk-workers are alive at once (Modal container budget). It is NOT the
 *     RPS governor — the gateway is.
 *
 * The Modal worker streams the whole cohort through the gateway in one
 * invocation; for typical GTM cohorts that is equally fast as fan-out (the
 * gateway is the bottleneck) and far simpler. Very large cohorts (≳ a few
 * thousand) should be chunked by the caller — batch.trigger fan-out over a
 * chunk-runner is the documented scaling path when worker wall-clock approaches
 * maxDuration.
 *
 * Trigger carries only signals: the callback body is terminal COUNTS, never
 * firmo rows. Results land in hq-x ops.task_runs → the firmographics-blitz
 * materializer → the firmographics_blitz Lance system-of-record.
 */

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface EnrichmentCallback {
  status: "success" | "error";
  workflow: "A" | "B" | "C";
  feed: string;
  requested: number;
  skipped: number;
  api_calls: number;
  succeeded: number;
  not_found: number;
  failed: number;
  error?: string | null;
}

// A cohort is an inline list (domains for A/C, company LinkedIn URLs for B) or a
// reference to a transport Parquet drop in the data-sink bucket.
type Cohort = string[] | { r2_key: string; column?: string };

interface EnrichmentPayload {
  cohort: Cohort;
  priority?: "high" | "normal" | "low";
  batchLabel?: string;
  firmoTtlDays?: number; // freshness gate for the JIT skip (default 180)
  negTtlDays?: number; // negative-cache window (default 30)
}

const QUEUE_CONCURRENCY = 8; // coarse container-budget cap; the gateway is the hard 5-RPS governor

async function dispatchAndWait(opts: {
  functionName: string;
  workflow: "A" | "B" | "C";
  defaultPriority: "high" | "normal" | "low";
  payload: EnrichmentPayload;
  runId: string;
}): Promise<EnrichmentCallback> {
  // 1) Mint the durable callback token (callbackHash in the url authenticates).
  const token = await wait.createToken({
    timeout: "1h",
    tags: ["enrichment-blitz", `wf-${opts.workflow}`],
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
      app_name: "enrichment-blitz",
      function_name: opts.functionName,
      kwargs: {
        cohort: opts.payload.cohort,
        priority: opts.payload.priority ?? opts.defaultPriority,
        batch_label: opts.payload.batchLabel ?? null,
        run_id: opts.runId,
        firmo_ttl_days: opts.payload.firmoTtlDays ?? 180,
        neg_ttl_days: opts.payload.negTtlDays ?? 30,
      },
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
  }

  logger.info("enrichment-blitz dispatched; suspending on waitpoint", {
    workflow: opts.workflow,
    tokenId: token.id,
    runId: opts.runId,
  });

  // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
  const result = await wait.forToken<EnrichmentCallback>(token.id);
  if (!result.ok) {
    throw new Error(
      `enrichment-blitz ${opts.workflow} timed out before Modal callback (token ${token.id})`,
    );
  }
  if (result.output.status !== "success") {
    throw new Error(
      `enrichment-blitz ${opts.workflow} failed in Modal: ${JSON.stringify(result.output)}`,
    );
  }
  logger.info("enrichment-blitz complete", { ...result.output });
  return result.output;
}

// Workflow A — bulk domain resolution. Default LOW priority (yields to B/C).
export const enrichmentResolveDomains = task({
  id: "enrichment-blitz-resolve-domains",
  queue: { concurrencyLimit: QUEUE_CONCURRENCY },
  maxDuration: 3900,
  run: async (payload: EnrichmentPayload, { ctx }) =>
    dispatchAndWait({
      functionName: "run_resolve_domains",
      workflow: "A",
      defaultPriority: "low",
      payload,
      runId: ctx.run.id,
    }),
});

// Workflow B — LinkedIn-URL firmographic enrichment. Default HIGH priority.
export const enrichmentEnrichLinkedin = task({
  id: "enrichment-blitz-enrich-linkedin",
  queue: { concurrencyLimit: QUEUE_CONCURRENCY },
  maxDuration: 3900,
  run: async (payload: EnrichmentPayload, { ctx }) =>
    dispatchAndWait({
      functionName: "run_enrich_linkedin",
      workflow: "B",
      defaultPriority: "high",
      payload,
      runId: ctx.run.id,
    }),
});

// Workflow C — full cascade. Default NORMAL priority (between B and bulk A).
export const enrichmentCascade = task({
  id: "enrichment-blitz-cascade",
  queue: { concurrencyLimit: QUEUE_CONCURRENCY },
  maxDuration: 3900,
  run: async (payload: EnrichmentPayload, { ctx }) =>
    dispatchAndWait({
      functionName: "run_cascade",
      workflow: "C",
      defaultPriority: "normal",
      payload,
      runId: ctx.run.id,
    }),
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
