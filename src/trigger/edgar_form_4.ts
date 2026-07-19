import { schedules, task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SEC EDGAR Form 4 / Insider Transactions (Forms 3/4/5 bundle).
 *
 * Trigger.dev v4 durable callback. The SEC publishes the insider-transactions data sets
 * QUARTERLY (file {YYYY}q{Q}_form345.zip). Two surfaces, both via the Universal Dispatcher
 * (the ONLY Modal endpoint) + a waitpoint token (mint url → POST dispatcher → suspend on
 * wait.forToken → resume from the worker's flat-JSON callback):
 *   • form4Quarterly — QUARTERLY schedule. Ingests the most-recent published quarter via
 *                      merge_insert(accession_number) (idempotent). The worker auto-resolves
 *                      the latest available quarter, so the schedule needs no date math.
 *   • form4Backfill  — on-demand. Streams the last N published quarters (overwrite→append in
 *                      one container) then BTREE-indexes accession_number / issuer_cik.
 *
 * issuer_cik is held VARCHAR (10-digit zero-padded by the source) → joins exactly to the
 * Identity Spine's cik10. Dataset: s3://data-sink/active/edgar_form_4/.
 */

const APP_NAME = "edgar-pipelines";
const DATASET = "form_4";

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset?: string;
  run_mode?: string;
  dataset_uri?: string;
  quarter?: string | null;
}

export const form4Quarterly = schedules.task({
  id: "edgar-form-4-quarterly",
  // 09:00 ET on the 10th of Jan/Apr/Jul/Oct — SEC posts the prior quarter within days of
  // quarter-end; the 10th leaves comfortable margin. Worker resolves the latest available file.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 9 10 1,4,7,10 *", timezone: "America/New_York" },
  maxDuration: 3600,
  run: async (_payload) => {
    logger.info("edgar form_4 quarterly ingest starting");
    return dispatch("ingest_form_quarter", { dataset: DATASET }, "1h", ["edgar", "form_4", "quarter"]);
  },
});

export const form4Backfill = task({
  id: "edgar-form-4-backfill",
  maxDuration: 10800,
  run: async (payload: { quarters?: number }) => {
    const quarters = payload?.quarters ?? 4;
    logger.info("edgar form_4 backfill starting", { quarters });
    return dispatch("backfill_form", { dataset: DATASET, quarters }, "3h", ["edgar", "form_4", "backfill"]);
  },
});

async function dispatch(
  functionName: "ingest_form_quarter" | "backfill_form",
  kwargs: Record<string, unknown>,
  timeout: string,
  tags: string[],
): Promise<IngestCallback> {
  const token = await wait.createToken({ timeout, tags });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: APP_NAME,
      function_name: functionName,
      kwargs,
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for ${DATASET}/${functionName}: ${body.slice(0, 300)}`);
  }

  const out = await wait.forToken<IngestCallback>(token.id);
  if (!out.ok) throw new Error(`timed out before Modal callback for ${DATASET}/${functionName}`);
  if (out.output.status !== "success") {
    throw new Error(`Modal failed for ${DATASET}/${functionName}: ${JSON.stringify(out.output)}`);
  }
  logger.info("edgar form_4 ingest complete", { ...out.output });
  return out.output;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
