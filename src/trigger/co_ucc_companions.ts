import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Colorado UCC companion datasets (debtors / secured parties / collateral)
 * Gen-2 → Gen-3 migration.
 *
 * Trigger.dev v4 durable callback. Manual cadence (no cron): the Gen-2 UCC streams are
 * re-snapshotted periodically; trigger this task with an optional { only, snapshot } when
 * a fresh snapshot lands. Each run is a FULL-SNAPSHOT overwrite of the companion tables
 * (mode="overwrite" in the worker), so re-running is safe.
 *
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target worker +
 *      that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune to
 *      HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the flat callback with terminal metadata.
 *
 * Flip to `schedules.task` with a cron if/when an automated refresh cadence is wanted.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface CompanionsCallback {
  status: "success" | "error";
  feed: string;
  source_bucket?: string;
  snapshot_date?: string;
  rows_total?: number;
  datasets?: Record<string, number>;
}

export const coUccCompanionsIngest = task({
  id: "co-ucc-companions-ingest",
  // The durable wait consumes no compute while suspended; generous cap.
  maxDuration: 3900,
  run: async (payload: { only?: string; snapshot?: string }) => {
    logger.info("CO UCC companions migration starting", {
      only: payload?.only,
      snapshot: payload?.snapshot,
    });

    // 1) Durable callback token. token.url's callbackHash is the auth — no API key.
    const token = await wait.createToken({ timeout: "1h", tags: ["co-ucc", "companions", "modal-dispatch"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    //    Pass only/snapshot through when provided; otherwise the worker migrates all
    //    three streams at the latest snapshot.
    const kwargs: Record<string, string> = {};
    if (payload?.only) kwargs.only = payload.only;
    if (payload?.snapshot) kwargs.snapshot = payload.snapshot;

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "co-ucc-companions",
        function_name: "ingest_co_ucc_companions",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    //    result.ok === false ONLY on token timeout (no callback arrived).
    const result = await wait.forToken<CompanionsCallback>(token.id);
    if (!result.ok) {
      throw new Error(`CO UCC companions migration timed out before Modal callback (token ${token.id})`);
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(`CO UCC companions migration failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("CO UCC companions migration complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
