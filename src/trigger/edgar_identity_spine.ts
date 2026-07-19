import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SEC EDGAR CIK Identity Spine (company_tickers.json).
 *
 * Trigger.dev v4 durable callback. WEEKLY full snapshot of the universal CIK↔ticker
 * mapping — the join key for every downstream EDGAR dataset. The worker fetches
 * company_tickers.json (+ company_tickers_exchange.json when available), DuckDB-transforms
 * the object-of-objects into one row per (cik, ticker), and writes Lance DIRECT to R2 at
 * s3://data-sink/active/edgar_cik_map/ with BTREE indexes on cik_str, cik10, and ticker.
 *
 * Mints a waitpoint token (its `url` is a pre-signed HTTP callback — no API key; the
 * callbackHash in the URL authenticates), POSTs the Universal Dispatcher (the ONLY Modal
 * endpoint) with the target worker + that callback url, then suspends on `wait.forToken` —
 * checkpointed, zero compute — and resumes from the worker's flat-JSON terminal callback.
 */

const APP_NAME = "edgar-pipelines";

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset?: string;
  run_mode?: string;
  dataset_uri?: string;
}

export const edgarIdentitySpine = schedules.task({
  id: "edgar-identity-spine",
  // Mondays 06:00 ET. The mapping changes slowly; a weekly refresh keeps the spine current.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 6 * * 1", timezone: "America/New_York" },
  maxDuration: 1800,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({ timeout: "1h", tags: ["edgar", "cik-map"] });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: APP_NAME,
        function_name: "ingest_cik_map",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status} for edgar cik_map: ${body.slice(0, 300)}`);
    }

    logger.info("Dispatched edgar cik_map; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<IngestCallback>(token.id);
    if (!result.ok) throw new Error(`edgar cik_map timed out before Modal callback (token ${token.id})`);
    if (result.output.status !== "success") {
      throw new Error(`edgar cik_map failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("edgar cik_map ingest complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
