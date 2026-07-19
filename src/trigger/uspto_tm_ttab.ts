import { schedules, task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USPTO Trademark TTAB proceedings (tt), a child dataset that resolves back
 * to the Applications spine via party/property serial numbers (flattened
 * property_serial_numbers list; merge key is proceeding_number).
 *
 * Same waitpoint-token pattern: a DAILY merge_insert delta and an on-demand sequential
 * backfile, both dispatched through the Universal Dispatcher.
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
const DATASET = "ttab";

export const ttabDelta = schedules.task({
  id: "uspto-tm-ttab-delta",
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 8 * * *", timezone: "America/New_York" },
  maxDuration: 3600,
  run: async (payload) => {
    logger.info("tt delta starting", { scheduledAt: payload.timestamp });
    return dispatch("ingest_delta", {}, "1h", ["uspto-tm", "ttab", "delta"]);
  },
});

export const ttabBackfill = task({
  id: "uspto-tm-ttab-backfill",
  maxDuration: 7200,
  run: async () => {
    logger.info("tt backfill starting");
    return dispatch("ingest_backfile", {}, "4h", ["uspto-tm", "ttab", "backfill"]);
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
  logger.info("tt ingest complete", { ...out.output });
  return out.output;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
