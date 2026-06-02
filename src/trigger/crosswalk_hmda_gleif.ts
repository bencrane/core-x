import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — HMDA × GLEIF corporate-identity crosswalk daily refresh.
 *
 * Trigger.dev v4 durable callback. Daily at 08:00 UTC this task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback —
 *      no API key; the callbackHash in the URL authenticates),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts while the rebuild runs in Modal (~1-2 min),
 *   4. resumes when the worker POSTs the flat callback with its terminal metadata.
 *
 * CADENCE / TIME-OFFSET NOTE. core-x has no completion-chained "daily sequence"; every
 * feed owns an independent Trigger cron, and a dependent feed fires on a TIME OFFSET after
 * its upstreams' publish windows (the gleif/fmcsa/uspto pattern). This crosswalk reads two
 * published Lance datasets:
 *   - gleif_l1_entities  (GLEIF — DAILY golden copy; published ~00:00 UTC, ingested by the
 *                         gleif-daily task at 06:00 UTC, L1 finishing well before 08:00)
 *   - hmda_panels        (HMDA — ANNUAL release, manual/irregular backfill; no daily cron)
 * GLEIF is the only daily-moving upstream, so 08:00 UTC sits a safe 2h after gleif-daily
 * kicks off (06:00) — the crosswalk always reflects the morning's fresh GLEIF snapshot.
 * HMDA moves at most yearly, so any daily run absorbs an HMDA refresh the day it lands. The
 * rebuild is an idempotent full overwrite, so a no-change day simply re-publishes the same
 * crosswalk — it can never go stale relative to whatever the upstreams currently hold. If
 * GLEIF ingest ever emits a completion event, convert this to a triggered chain.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface CrosswalkCallback {
  status: "success" | "error";
  feed: string;
  dataset_uri?: string;
  rows: number;
  gleif_publish_date?: string | null;
  hmda_panel_leis?: number;
  matched_leis?: number;
  match_rate?: number;
}

export const crosswalkHmdaGleif = schedules.task({
  id: "crosswalk-hmda-gleif",
  // 08:00 UTC daily — a 2h offset after gleif-daily (06:00 UTC); the build is
  // overwrite-idempotent so it can never go stale relative to the published sources.
  cron: { pattern: "0 8 * * *", timezone: "UTC" },
  // Generous cap; the durable wait itself consumes no compute while suspended.
  maxDuration: 1800,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token.
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["crosswalk-hmda-gleif", "modal-dispatch"],
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
        app_name: "resolution-hmda-gleif-pipelines",
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

    logger.info("HMDA × GLEIF crosswalk refresh complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
