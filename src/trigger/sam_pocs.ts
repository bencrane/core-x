import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SAM.gov POC human-layer daily refresh.
 *
 * Trigger.dev v4 durable callback. Daily at 16:30 UTC this task mints a waitpoint
 * token, POSTs the Universal Dispatcher (the only Modal endpoint) to spawn the
 * `build_sam_pocs` worker with that callback url, suspends on `wait.forToken`
 * (checkpointed, zero compute, HTTP-timeout-immune), and resumes when the worker
 * POSTs its flat terminal metadata.
 *
 * CADENCE. Mirrors the crosswalk pattern: this reshape reads one published Lance
 * dataset (entity_registrations — SAM monthly extract, manual backfill), which is
 * not a daily completion-emitting upstream, so there is nothing to chain off.
 * 16:30 UTC sits after the SAM daily window (12:00 UTC) and just after the
 * crosswalk's 16:00 slot — staggered to spread Modal load. The rebuild is an
 * idempotent full overwrite, so a no-change day simply re-publishes; it can never
 * go stale relative to whatever entity_registrations currently holds.
 */

interface SamPocsCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  sam_label?: string;
  distinct_uei?: number;
  distinct_cage?: number;
}

export const samPocs = schedules.task({
  id: "sam-pocs",
  cron: { pattern: "30 16 * * *", timezone: "UTC" },
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["sam-pocs", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "sam-gov-pocs-pipelines",
        function_name: "build_sam_pocs",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched sam_pocs to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<SamPocsCallback>(token.id);
    if (!result.ok) {
      throw new Error(`sam_pocs build timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`sam_pocs build failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("sam_pocs refresh complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
