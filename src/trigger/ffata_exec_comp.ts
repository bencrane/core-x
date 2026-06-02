import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending FFATA Executive Compensation human-layer refresh.
 *
 * Trigger.dev v4 durable callback. Daily at 17:00 UTC this task mints a waitpoint
 * token, POSTs the Universal Dispatcher to spawn the `build_ffata_exec_comp`
 * worker with that callback url, suspends on `wait.forToken` (checkpointed, zero
 * compute), and resumes when the worker POSTs its flat terminal metadata.
 *
 * CADENCE. Mirrors the crosswalk pattern. This reshape reads three published Lance
 * datasets (award_search / transaction_search_fpds / transaction_search_fabs —
 * USAspending periodic full-DB dumps), none of which is a daily completion-emitting
 * upstream, so there is nothing to chain off. 17:00 UTC is staggered after the
 * crosswalk (16:00) and sam_pocs (16:30) to spread Modal load. The rebuild is an
 * idempotent full overwrite — it can never go stale relative to current upstream
 * state. NOTE: the build scans all three carriers (~314M rows) to extract the
 * sparse officer disclosures; if daily scan cost matters, narrow CARRIERS in the
 * worker to award_search only (one-line change, ~91% of UEI reach).
 */

interface FfataCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  distinct_uei?: number;
}

export const ffataExecComp = schedules.task({
  id: "ffata-exec-comp",
  cron: { pattern: "0 17 * * *", timezone: "UTC" },
  maxDuration: 3900,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["ffata-exec-comp", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-ffata-pipelines",
        function_name: "build_ffata_exec_comp",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched ffata_exec_comp to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<FfataCallback>(token.id);
    if (!result.ok) {
      throw new Error(`ffata_exec_comp build timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`ffata_exec_comp build failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("ffata_exec_comp refresh complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
