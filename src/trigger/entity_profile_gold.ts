import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Gold Mirror reconciliation (entity_profile_gold, the unified profile).
 *
 * Trigger.dev v4 durable callback. Daily at 19:00 UTC this task mints a waitpoint token,
 * POSTs the Universal Dispatcher to spawn the `build_entity_profile_gold` worker with that
 * callback url, suspends on `wait.forToken` (checkpointed, zero compute), and resumes when
 * the worker POSTs its flat terminal metadata.
 *
 * CADENCE. This is a write-time reconciliation that reshapes five already-published Lance
 * datasets — the SAM identity spine (monthly) and the USAspending prime ledger + 90-day
 * daily API override. It is staggered to 19:00 UTC, AFTER contractor_award_summary (18:00)
 * and ffata_exec_comp (17:00), so the day's usaspending_api_fresh pull and the upstream
 * summaries have landed before the merge runs. The rebuild is an idempotent full overwrite
 * (index-guarded, with rollback), so it can never go stale relative to current upstream
 * state and is safe to re-run. The heaviest leg is the award_search (78.6M) FULL OUTER JOIN
 * against the fresh override; a 2.5h token timeout covers the scan + per-award resolve +
 * uei aggregation + SAM join + index hydration with headroom.
 */

interface EntityProfileGoldCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  distinct_uei?: number;
  entities_with_awards?: number;
  sum_total_active_obligations?: number;
}

export const entityProfileGold = schedules.task({
  id: "entity-profile-gold",
  // PARKED (operator ruling 2026-07-19: no scheduled cadence for now; restore to reinstate).
  // cron: { pattern: "0 19 * * *", timezone: "UTC" },
  maxDuration: 9000,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "2.5h",
      tags: ["entity-profile-gold", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "entity-profile-gold-pipelines",
        function_name: "build_entity_profile_gold",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched entity_profile_gold to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<EntityProfileGoldCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `entity_profile_gold build timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `entity_profile_gold build failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("entity_profile_gold reconciliation complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
