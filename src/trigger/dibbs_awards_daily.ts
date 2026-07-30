import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — DLA DIBBS daily Awards raw capture (Phase 1).
 *
 * Daily at 16:30 UTC (30 min after the RFQ capture, to stagger the two polite
 * single-client .mil crawls) this task:
 *   1. mints a waitpoint token (5h — award PDFs can be tens of MB each and a
 *      catch-up run sweeps a backlog on a 1s-spaced crawl),
 *   2. POSTs the Universal Dispatcher targeting
 *      dibbs-awards-pipelines/capture_awards_daily,
 *   3. suspends on wait.forToken (checkpointed, zero compute),
 *   4. resumes on the worker's terminal callback and resolves/fails from it.
 *
 * Idempotent (ledger diff on ops.dibbs_awards_daily_r2_ingest_runs), so the
 * default retry policy is safe.
 */

interface CaptureCallback {
  status: "success" | "error";
  feed: string;
  files: Record<string, number>;
  bytes_landed: number;
  error?: string | null;
}

export const dibbsAwardsDailyDispatcher = schedules.task({
  id: "dibbs-awards-daily-capture",
  cron: { pattern: "30 16 * * *", timezone: "UTC" },
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "5h",
      tags: ["dibbs-awards-daily", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "dibbs-awards-pipelines",
        function_name: "capture_awards_daily",
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
      throw new Error(`DIBBS awards capture timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`DIBBS awards capture failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("DIBBS awards daily capture complete", { ...result.output });
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
