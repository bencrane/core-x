import { schedules, task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USPTO Trademark APPLICATIONS (apc / case-files), the master entity spine.
 *
 * Two surfaces, both via the Universal Dispatcher (the ONLY Modal endpoint) + a Trigger v4
 * durable waitpoint token (mint url → POST dispatcher → suspend on wait.forToken →
 * resume from the worker's flat-JSON callback):
 *   • applicationsDelta   — DAILY schedule. UPSERTs the latest apcyymmdd.zip into Lance via
 *                           merge_insert(serial_number); folds pending→registered→dead in place.
 *   • applicationsBackfill — on-demand. Sequentially streams every apc backfile part
 *                           (overwrite→append in one container) then BTREE-indexes serial_number.
 *
 * USPTO posts the daily file MON–SUN at 24:00 ET; the schedule fires at 08:00 ET so the file
 * is settled, and the worker resolves the most-recent delta in the R2 landing zone.
 */

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset?: string;
  run_mode?: string;
  dataset_uri?: string;
  as_of?: string | null;
}

const APP_NAME = "uspto-trademarks";
const DATASET = "applications";

export const applicationsDelta = schedules.task({
  id: "uspto-tm-applications-delta",
  // 08:00 ET daily — comfortably after the 24:00 ET daily-file posting.
  cron: { pattern: "0 8 * * *", timezone: "America/New_York" },
  maxDuration: 3600,
  run: async (payload) => {
    logger.info("apc delta starting", { scheduledAt: payload.timestamp });
    return dispatch("ingest_delta", {}, "1h", ["uspto-tm", "applications", "delta"]);
  },
});

export const applicationsBackfill = task({
  id: "uspto-tm-applications-backfill",
  // The ~30-40 GB corpus streams part-by-part in one Modal container; the durable wait
  // consumes no compute while suspended.
  maxDuration: 7200,
  run: async (payload: { key?: string }) => {
    logger.info("apc backfill starting");
    return dispatch("ingest_backfile", {}, "6h", ["uspto-tm", "applications", "backfill"]);
  },
});

async function dispatch(
  fn: "ingest_delta" | "ingest_backfile",
  extraKwargs: Record<string, unknown>,
  timeout: string,
  tags: string[],
) {
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
      function_name: fn,
      kwargs: { dataset: DATASET, ...extraKwargs },
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for ${DATASET}/${fn}: ${body.slice(0, 300)}`);
  }

  const out = await wait.forToken<IngestCallback>(token.id);
  if (!out.ok) throw new Error(`timed out before Modal callback for ${DATASET}/${fn}`);
  if (out.output.status !== "success") {
    throw new Error(`Modal failed for ${DATASET}/${fn}: ${JSON.stringify(out.output)}`);
  }
  logger.info("apc ingest complete", { ...out.output });
  return out.output;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
