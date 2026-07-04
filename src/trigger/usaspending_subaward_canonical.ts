import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending SUBAWARD CANONICAL refresh (typed v2 SoR reconciliation).
 *
 * ⚠️ SCHEDULE DISABLED 2026-07-04 — the declarative `cron` has been REMOVED so that NO
 * trigger.dev deploy re-activates this task. Its `refresh_fn` worker runs a FULL OVERWRITE
 * (build → mode="overwrite") of the ~1.3M-row subaward canonical SoR; auto-firing it would
 * wipe and rebuild the entire dataset from whatever the BULK/FRESH sources currently hold.
 * The task stays exported and manually/imperatively triggerable, but will never fire on a
 * cron. RE-ENABLE ONLY by deliberately restoring a cron AND after replacing the overwrite
 * path with an append/merge worker — never point a schedule at the destructive rebuild again.
 *
 * Trigger.dev v4 durable callback. When run, this task mints a waitpoint
 * token, POSTs the Universal Dispatcher to spawn the `refresh_fn` worker (which runs
 * build → index → verify as one terminal unit) with that callback url, suspends on
 * `wait.forToken` (checkpointed, zero compute), and resumes when the worker POSTs its
 * flat terminal metadata.
 *
 * CADENCE. The canonical reconciles two published Lance datasets — BULK
 * `usaspending/subaward_search` (periodic full-DB dump) + FRESH
 * `usaspending_api_fresh/contract_subaward` (accumulating daily FSRS append). Neither
 * emits a completion event to chain off. 20:00 UTC is staggered after
 * contractor_award_summary (18:00) to spread Modal load. The rebuild is an idempotent
 * full overwrite (contract-only, ~1.3M rows) so it can never go stale relative to
 * current upstream state; it only needs to re-run after the FRESH daily append and any
 * fresh BULK subaward snapshot. Token timeout 2h covers build + index + verify with
 * headroom (the on-Modal build+index is ~6-8 min at this scale).
 */

interface SubawardCanonicalCallback {
  status: "success" | "error";
  feed: string;
  dataset_uri?: string;
  rows_out?: number;
  fresh_corrections_applied?: number;
  indices_built?: number;
  verify_pass?: boolean;
  verify_failures?: string[];
  error?: string;
}

export const usaspendingSubawardCanonical = schedules.task({
  id: "usaspending-subaward-canonical",
  // ⚠️ DISABLED 2026-07-04 — declarative cron removed so NO deploy re-activates the daily
  // full-overwrite rebuild of the ~1.3M-row subaward canonical SoR. Do NOT restore a cron
  // until refresh_fn is switched off mode="overwrite" (append/merge). Was:
  //   cron: { pattern: "0 20 * * *", timezone: "UTC" },
  maxDuration: 7800,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "2h",
      tags: ["usaspending-subaward-canonical", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-subaward-canonical",
        function_name: "refresh_fn",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_subaward_canonical to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<SubawardCanonicalCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `usaspending_subaward_canonical refresh timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `usaspending_subaward_canonical refresh failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("usaspending_subaward_canonical refresh complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
