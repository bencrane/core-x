import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Company Scraper (Icypeas /api/scrape bulk rail).
 *
 * Feeds LinkedIn company URLs into Icypeas company scraping. This is the SUBMIT
 * orchestrator; results are delivered by WEBHOOK to edge_api (/webhooks/icypeas/*
 * → business.icypeas_webhook_events, the raw SoR) — NOT returned through here.
 *
 * Task surface (the only id callers trigger):
 *   companyScraperEnroll   companyUrls[] → governed /api/scrape submits (+ ops ledger)
 *
 * Fan-out durable-callback pattern (mirror src/trigger/enrichment_email_cascade.ts).
 * enroll chunks the batch and fans out one CHILD run per chunk via batchTriggerAndWait
 * — a SINGLE batch waitpoint in the parent. Each child (companyScraperChunk) mints its
 * own waitpoint token, POSTs the Universal Dispatcher (enrichment-company-scrape worker +
 * callback url), and suspends on exactly one wait.forToken (zero compute while suspended),
 * resuming from the worker's RAW terminal-counts callback. Trigger.dev forbids concurrent
 * waitpoints in a single run, so batchTriggerAndWait (one batch waitpoint) is the sanctioned
 * parallel primitive; a Promise.all over wait.forToken would trip TASK_DID_CONCURRENT_WAIT.
 *
 * Throttle: the single-container core/icypeas_gateway.py scrape_submit bucket is the HARD
 * global governor on /api/scrape submits regardless of how many chunk-workers are in flight.
 * The chunk queue.concurrencyLimit is only a COARSE Modal-container budget, NOT the rate governor.
 *
 * The worker is submit-only and ZERO-read: it never polls Icypeas, so this rail never touches
 * the global 30/min read ceiling the email cascade + bulk drain contend for. A run completes at
 * "submitted", not "scraped" — scraped rows land asynchronously at edge_api over the following
 * seconds/minutes; reconcile via ops.company_scrape_submissions ⋈ business.icypeas_webhook_events.
 *
 * Trigger carries only signals: the callback body is terminal COUNTS, never scraped rows.
 */

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface CompanyScrapeCallback {
  status: "success" | "error";
  feed: string;
  batch_label?: string | null;
  requested: number;
  skipped: number;
  submitted: number;
  batches: number;
  failed: number;
  error?: string | null;
}

interface CompanyScraperPayload {
  companyUrls: string[];
  batchLabel?: string;
  force?: boolean; // re-submit urls already marked 'submitted' (default: skip them)
  chunkSize?: number; // urls per Modal worker invocation (default 500; worker sub-batches into 50s)
}

// One Modal chunk-worker per child run. parentRunId + index reconstruct the stable worker-facing
// run_id (`${parentRunId}:${index}`), so the worker's ledger/idempotency on run_root is stable.
interface CompanyScraperChunkPayload {
  companyUrls: string[];
  batchLabel: string | null;
  force: boolean;
  parentRunId: string;
  index: number;
}

const ENROLL_CONCURRENCY = 6; // admitted enroll orchestrations; checkpointed (zero compute) while chunks fan out
const CHUNK_CONCURRENCY = 6; // coarse Modal-container budget on chunk-workers; the gateway is the hard governor
const DEFAULT_CHUNK_SIZE = 500; // urls per worker; the worker slices into ≤50-URL /api/scrape submits

const ZERO: Omit<CompanyScrapeCallback, "status" | "feed"> = {
  requested: 0,
  skipped: 0,
  submitted: 0,
  batches: 0,
  failed: 0,
};

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

// CHILD — submit one chunk. Exactly one wait.forToken per run, so N chunks fan out as N independent
// runs with no concurrent waitpoint in any single run.
export const companyScraperChunk = task({
  id: "enrichment-company-scrape-chunk",
  queue: { concurrencyLimit: CHUNK_CONCURRENCY },
  maxDuration: 2400, // ~40m; the worker is submit-only (fast) but tolerates gateway back-pressure
  run: async (payload: CompanyScraperChunkPayload): Promise<CompanyScrapeCallback> => {
    const { companyUrls, batchLabel, force, parentRunId, index } = payload;

    // 1) Mint the durable callback token (callbackHash in the url authenticates).
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["company-scrape", `chunk-${index}`],
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
        app_name: "enrichment-company-scrape",
        function_name: "run_company_scrape",
        kwargs: {
          company_urls: companyUrls,
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

    logger.info("company-scrape chunk dispatched; suspending on waitpoint", {
      chunk: index,
      size: companyUrls.length,
      tokenId: token.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<CompanyScrapeCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `company-scrape chunk ${index} timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `company-scrape chunk ${index} failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }
    return result.output;
  },
});

export const companyScraperEnroll = task({
  id: "enrichment-company-scrape-enroll",
  queue: { concurrencyLimit: ENROLL_CONCURRENCY },
  maxDuration: 14400, // 4h ceiling; suspended at the batch waitpoint consumes no compute
  run: async (payload: CompanyScraperPayload, { ctx }) => {
    const urls = Array.from(
      new Set((payload.companyUrls ?? []).map((u) => (u ?? "").trim()).filter(Boolean)),
    );
    if (urls.length === 0) {
      return { status: "success", feed: "company_scrape", batch_label: payload.batchLabel ?? null, ...ZERO };
    }

    const size = payload.chunkSize ?? DEFAULT_CHUNK_SIZE;
    const force = payload.force ?? false;
    const batchLabel = payload.batchLabel ?? null;
    const chunks = chunk(urls, size);

    logger.info("company-scrape enroll starting", {
      urls: urls.length,
      chunks: chunks.length,
      chunkSize: size,
    });

    // Fan out one child run per chunk under a SINGLE batch waitpoint (the Trigger-sanctioned parallel
    // primitive). CHUNK_CONCURRENCY bounds how many workers are alive; the gateway is the hard governor.
    const { runs } = await companyScraperChunk.batchTriggerAndWait(
      chunks.map((slice, i) => ({
        payload: { companyUrls: slice, batchLabel, force, parentRunId: ctx.run.id, index: i },
      })),
    );

    // Aggregate terminal counts; fail the whole enroll if any chunk run failed.
    const totals = { ...ZERO };
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(`company-scrape chunk run ${run.id} failed: ${JSON.stringify(run.error)}`);
      }
      const c = run.output;
      totals.requested += c.requested;
      totals.skipped += c.skipped;
      totals.submitted += c.submitted;
      totals.batches += c.batches;
      totals.failed += c.failed;
    }

    logger.info("company-scrape enroll complete", { ...totals, chunks: chunks.length });
    return { status: "success", feed: "company_scrape", batch_label: batchLabel, ...totals };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
