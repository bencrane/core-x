import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending Contractor Award Summary refresh (the precomputed resume).
 *
 * Trigger.dev v4 durable callback. Daily at 18:00 UTC this task mints a waitpoint
 * token, POSTs the Universal Dispatcher to spawn the `build_contractor_award_summary`
 * worker with that callback url, suspends on `wait.forToken` (checkpointed, zero
 * compute), and resumes when the worker POSTs its flat terminal metadata.
 *
 * CADENCE. Mirrors the ffata pattern. This reshape reads two published Lance datasets
 * (award_search 78.4M + subaward_search 9.8M — USAspending periodic full-DB dumps),
 * neither a daily completion-emitting upstream, so there is nothing to chain off.
 * 18:00 UTC is staggered after ffata_exec_comp (17:00) to spread Modal load — both
 * scan award_search. The rebuild is an idempotent full overwrite, so it can never go
 * stale relative to current upstream state; it only needs to re-run after a fresh
 * USAspending bulk snapshot lands. Token timeout 2h covers the full prime+subaward
 * scan + aggregation + index build with headroom.
 */

interface AwardSummaryCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  distinct_combined_uei?: number;
  total_combined_obligated?: number;
}

export const contractorAwardSummary = schedules.task({
  id: "contractor-award-summary",
  cron: { pattern: "0 18 * * *", timezone: "UTC" },
  maxDuration: 7800,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "2h",
      tags: ["contractor-award-summary", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-award-summary-pipelines",
        function_name: "build_contractor_award_summary",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched contractor_award_summary to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<AwardSummaryCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `contractor_award_summary build timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `contractor_award_summary build failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("contractor_award_summary refresh complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
