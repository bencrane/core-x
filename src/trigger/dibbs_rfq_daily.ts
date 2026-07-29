import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — DLA DIBBS daily RFQ/Awards raw artifact capture (Phase 1).
 *
 * Daily at 16:00 UTC (11:00/12:00 ET — after DIBBS posts the prior day's
 * finalized files) this task:
 *   1. mints a waitpoint token (5h — runs move gigabytes; the worker's Modal
 *      timeout is 4h),
 *   2. POSTs the Universal Dispatcher targeting
 *      dibbs-pipelines/capture_rfq_daily,
 *   3. suspends on wait.forToken (checkpointed, zero compute),
 *   4. resumes on the worker's terminal callback and resolves/fails from it.
 *
 * The worker is idempotent (ledger diff on ops.dibbs_rfq_daily_r2_ingest_runs),
 * so the default retry policy is safe.
 */

// The body Modal POSTs to the waitpoint url becomes this run's output.
interface CaptureCallback {
  status: "success" | "error";
  feed: string;
  files: Record<string, number>;
  bytes_landed: number;
  error?: string | null;
}

export const dibbsRfqDailyDispatcher = schedules.task({
  id: "dibbs-rfq-daily-capture",
  cron: { pattern: "0 16 * * *", timezone: "UTC" },
  // Generous cap; the durable wait itself consumes no compute while suspended.
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token. 5h: the daily ca zip is 250-400 MB
    //    and a catch-up run can move several GB on a polite .mil rate limit.
    const token = await wait.createToken({
      timeout: "5h",
      tags: ["dibbs-rfq-daily", "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202).
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "dibbs-pipelines",
        function_name: "capture_rfq_daily",
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

    // 3) Suspend until Modal POSTs the callback url. 4) Resolve from it.
    const result = await wait.forToken<CaptureCallback>(token.id);

    if (!result.ok) {
      throw new Error(`DIBBS capture timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`DIBBS capture failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("DIBBS daily capture complete", { ...result.output });
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
