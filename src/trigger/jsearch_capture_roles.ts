import { schedules, task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — JSearch capture-roles harvester.
 *
 * A company posting a capture-manager role is actively investing in winning US federal contracts —
 * a high-intent govcon account signal. This feed harvests those postings from JSearch (OpenWeb
 * Ninja's Google-for-Jobs aggregate) and lands them, so downstream resolution can map
 * employer_website -> federal entity (UEI/CAGE). Mirrors src/trigger/sam_opps_bulk.ts and the
 * leadmagic pattern: mint a waitpoint token, POST the Universal Dispatcher, suspend on
 * wait.forToken (zero compute while suspended), resume from the worker's RAW terminal callback.
 *
 * Two-stage, land-then-hydrate. Each stage is one dispatch + one waitpoint:
 *   1. harvest_capture_roles   -> ops.jsearch_capture_postings (Postgres landing SoR)
 *   2. materialize_capture_roles -> s3://data-sink/active/jsearch_capture_roles/ (Lance SoR)
 * Materialize runs only after harvest succeeds — the Lance projection reflects fresh PG truth.
 *
 * Tasks (the ids callers trigger):
 *   jsearch-capture-roles-daily      scheduled: incremental harvest (date_posted=3days) + hydrate
 *   jsearch-capture-roles-backfill   manual:    deep harvest (date_posted=all) + hydrate
 *
 * Zero new endpoints, zero new secrets: routed through the same dispatcher by name; syncEnvVars in
 * trigger.config.ts already forwards MODAL_DISPATCHER_URL / MODAL_KEY / MODAL_SECRET.
 */

const MODAL_APP = "jsearch-capture-roles";

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface WorkerCallback {
  status: "success" | "error";
  feed: string;
  mode: "backfill" | "incremental" | "materialize";
  error?: string | null;
  queries_run: number;
  pages_fetched: number;
  credits_spent: number;
  jobs_seen: number;
  jobs_new: number;
  jobs_updated: number;
  employers_distinct: number;
  rows_materialized?: number;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

/**
 * One dispatch + one durable waitpoint. Mints the callback token, fires the Universal Dispatcher
 * (fire-and-forget 202), suspends until the worker POSTs its RAW terminal callback, then asserts
 * both the transport (result.ok) and the business status (result.output.status).
 */
async function dispatchAndWait(
  functionName: string,
  kwargs: Record<string, unknown>,
  tags: string[],
): Promise<WorkerCallback> {
  const token = await wait.createToken({ timeout: "1h", tags });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: MODAL_APP,
      function_name: functionName,
      kwargs,
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
  }

  logger.info("Dispatched to Modal; suspending on waitpoint", { functionName, tokenId: token.id });

  const result = await wait.forToken<WorkerCallback>(token.id);
  // result.ok === false ONLY on token timeout (no callback arrived).
  if (!result.ok) {
    throw new Error(`${functionName} timed out before Modal callback (token ${token.id})`);
  }
  // ok:true still carries the worker's payload — inspect business status.
  if (result.output.status !== "success") {
    throw new Error(`${functionName} failed in Modal: ${JSON.stringify(result.output)}`);
  }
  return result.output;
}

async function harvestThenMaterialize(
  mode: "incremental" | "backfill",
  datePosted: string,
): Promise<{ harvest: WorkerCallback; materialize: WorkerCallback }> {
  const harvest = await dispatchAndWait(
    "harvest_capture_roles",
    { mode, date_posted: datePosted },
    ["jsearch-capture-roles", `harvest-${mode}`],
  );
  logger.info("harvest complete; hydrating Lance", { ...harvest });

  const materialize = await dispatchAndWait(
    "materialize_capture_roles",
    {},
    ["jsearch-capture-roles", "materialize"],
  );
  logger.info("materialize complete", { ...materialize });

  return { harvest, materialize };
}

// Scheduled incremental — daily 13:00 UTC. The durable waits consume no compute while suspended.
export const jsearchCaptureRolesDaily = schedules.task({
  id: "jsearch-capture-roles-daily",
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 13 * * *", timezone: "UTC" },
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    logger.info("jsearch capture-roles daily incremental starting", { triggerRunId: ctx.run.id });
    return harvestThenMaterialize("incremental", "3days");
  },
});

// Manual deep backfill — date_posted=all across the full title-variant fan-out, then hydrate.
export const jsearchCaptureRolesBackfill = task({
  id: "jsearch-capture-roles-backfill",
  maxDuration: 7200,
  run: async (payload: { datePosted?: string }) => {
    const datePosted = payload?.datePosted ?? "all";
    logger.info("jsearch capture-roles backfill starting", { datePosted });
    return harvestThenMaterialize("backfill", datePosted);
  },
});
