import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — OSHA "Sniper" daily severe-violation triggers.
 *
 * Trigger.dev v4 owns cadence (modal.Cron is forbidden in core-x). Daily at
 * 13:00 UTC this task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback — no API
 *      key; the callbackHash in the URL authenticates),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) targeting the
 *      `osha-pipelines` worker `osha_sniper` with that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, zero compute, timeout-immune,
 *   4. resumes when the worker POSTs its terminal metadata, and resolves/fails.
 *
 * The worker is quota-governed (hard MAX_API_CALLS circuit breaker); once-daily
 * dispatch keeps the whole feed well under the DOL ~20-calls/day plan (a normal day is
 * ~7: a frontier probe, a server-side windowed pull, and a few inspection batches).
 * `truncated` in the callback means the day's window was clipped — surfaced as a WARN
 * below so a backfill spike can't silently shed citations.
 */

// The body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  api_calls_used?: number;
  truncated?: boolean;
  dropped?: number;
  page_full?: boolean;
  in_window?: number;
}

export const oshaSniperDispatcher = schedules.task({
  id: "osha-sniper-dispatcher",
  // Daily 13:00 UTC. Object form pins the timezone explicitly (no ambiguity).
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 13 * * *", timezone: "UTC" },
  // Generous cap; the durable wait itself consumes no compute while suspended.
  maxDuration: 1800,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token.
    const token = await wait.createToken({
      timeout: "30m",
      tags: ["osha-daily-triggers", "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202). Empty kwargs
    //    → the worker's defaults (lookback 7d, severe-only, merge, its own call budget).
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "osha-pipelines",
        function_name: "osha_sniper",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched OSHA sniper to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until Modal POSTs the callback url. 4) Resolve from it.
    const result = await wait.forToken<IngestCallback>(token.id);

    // result.ok === false ONLY on token timeout (no callback arrived).
    if (!result.ok) {
      throw new Error(`OSHA sniper timed out before Modal callback (token ${token.id})`);
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(`OSHA sniper failed in Modal: ${JSON.stringify(result.output)}`);
    }

    // Data landed, but a clipped window is a real signal — emit a WARN (alertable in
    // the Trigger dashboard) rather than failing a run that successfully materialized.
    if (result.output.truncated) {
      logger.warn("OSHA sniper TRUNCATED — in-window citations clipped this run", {
        dropped: result.output.dropped ?? null,
        pageFull: result.output.page_full ?? null,
        inWindow: result.output.in_window ?? null,
        rows: result.output.rows,
        apiCallsUsed: result.output.api_calls_used ?? null,
      });
    }

    logger.info("OSHA sniper complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
