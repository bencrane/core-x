import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — DLA DIBBS daily RFP/IFB raw & structured capture.
 *
 * Daily at 16:15 UTC (15 min after the RFQ capture and 15 min before the Awards
 * capture, staggering the three polite single-client .mil crawls) this task:
 *   1. mints a waitpoint token (5h),
 *   2. POSTs the Universal Dispatcher targeting
 *      dibbs-rfp-pipelines/capture_rfp_daily,
 *   3. suspends on wait.forToken (checkpointed, zero compute),
 *   4. resumes on the worker's terminal callback and resolves/fails from it.
 *
 * Idempotent (ledger diff on ops.dibbs_rfp_daily_r2_ingest_runs), so the
 * default retry policy is safe.
 */

interface CaptureCallback {
  status: "success" | "error";
  feed: string;
  files: Record<string, number>;
  bytes_landed: number;
  error?: string | null;
}

export const dibbsRfpDailyDispatcher = schedules.task({
  id: "dibbs-rfp-daily-capture",
  cron: { pattern: "15 16 * * *", timezone: "UTC" },
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "5h",
      tags: ["dibbs-rfp-daily", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "dibbs-rfp-pipelines",
        function_name: "capture_rfp_daily",
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

    const result = await wait.forToken<CaptureCallback>(token.id);

    if (!result.ok) {
      throw new Error(`DIBBS RFP capture timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`DIBBS RFP capture failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("DIBBS RFP daily capture complete", { ...result.output });
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
