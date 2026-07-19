import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SAM.gov × USAspending canonical crosswalk daily refresh.
 *
 * Trigger.dev v4 durable callback. Daily at 16:00 UTC this task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback —
 *      no API key; the callbackHash in the URL authenticates),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts while the rebuild runs in Modal (~3-5 min),
 *   4. resumes when the worker POSTs the flat callback with its terminal metadata.
 *
 * CADENCE / CHAINING NOTE. core-x has no completion-chained "daily ingest sequence";
 * every feed owns an independent Trigger cron, and dependent feeds fire on a
 * time-offset after their upstream's publish window (gleif/fmcsa/uspto pattern).
 * This crosswalk reads three published Lance datasets:
 *   - entity_registrations  (SAM entity registry — MONTHLY extract, manual backfill)
 *   - recipient_lookup       (USAspending — periodic full-DB dump)
 *   - transaction_search_fpds (USAspending — periodic full-DB dump, cage bridge)
 * None of those upstreams is on a daily cron today, so there is no completion event
 * to chain off. 16:00 UTC sits comfortably after the SAM daily window (12:00 UTC)
 * with margin. The rebuild is an idempotent full overwrite, so a no-change day
 * simply re-publishes the same crosswalk — it can never go stale relative to
 * whatever the upstreams currently hold. If/when the SAM + USAspending raw syncs
 * become daily completion-emitting tasks, convert this to a triggered chain.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface CrosswalkCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  sam_label?: string;
  matched_by_uei?: number;
  matched_by_cage?: number;
  matched_any?: number;
}

export const crosswalkSamUsaspending = schedules.task({
  id: "crosswalk-sam-usaspending",
  // 16:00 UTC daily — after the SAM upstream window; the build is overwrite-idempotent.
  // PARKED (operator ruling 2026-07-19: no scheduled cadence for now; restore to reinstate).
  // cron: { pattern: "0 16 * * *", timezone: "UTC" },
  // Generous cap; the durable wait itself consumes no compute while suspended.
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token.
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["crosswalk-sam-usaspending", "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202). The worker
    //    runs in Modal; this task does not hold the connection.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "resolution-crosswalk-pipelines",
        function_name: "build_crosswalk",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<CrosswalkCallback>(token.id);
    if (!result.ok) {
      throw new Error(`crosswalk build timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`crosswalk build failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("Crosswalk refresh complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
