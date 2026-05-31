import { schedules, task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USPTO Trademark ASSIGNMENTS (asb), a child dataset that resolves back to
 * the Applications spine via property serial numbers (each row carries a flattened
 * property_serial_numbers list; merge key is the reel-frame assignment_id).
 *
 * Same waitpoint-token pattern as the Applications task: a DAILY merge_insert delta and an
 * on-demand sequential backfile, both dispatched through the Universal Dispatcher.
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
const DATASET = "assignments";

export const assignmentsDelta = schedules.task({
  id: "uspto-tm-assignments-delta",
  cron: { pattern: "0 8 * * *", timezone: "America/New_York" },
  maxDuration: 3600,
  run: async (payload) => {
    logger.info("asb delta starting", { scheduledAt: payload.timestamp });
    return dispatch("ingest_delta", {}, "1h", ["uspto-tm", "assignments", "delta"]);
  },
});

export const assignmentsBackfill = task({
  id: "uspto-tm-assignments-backfill",
  maxDuration: 7200,
  run: async () => {
    logger.info("asb backfill starting");
    return dispatch("ingest_backfile", {}, "4h", ["uspto-tm", "assignments", "backfill"]);
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
  logger.info("asb ingest complete", { ...out.output });
  return out.output;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
