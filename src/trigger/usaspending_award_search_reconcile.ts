import { task, schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending award_search reconciliation (bulk + delta merge-to-spine).
 *
 * Two tasks: backfill (one-time, all landings ≥ snapshot date) and daily (recurring, 7d trailing).
 * Both mint a Trigger.dev v4 waitpoint, POST the Universal Dispatcher to spawn the
 * `usaspending-pipelines` Modal worker, and suspend on `wait.forToken`.
 *
 * The reconciliation pattern is:
 * - Read bulk historical (s3://data-sink/active/usaspending/award_search/, snapshot 2026-05-06)
 * - Read delta landings (s3://data-sink/usaspending_api_landings/award_search/pull_date=*/)
 * - Merge on contract_award_unique_key, deduplicate taking argmax(last_modified_date)
 * - Filter out deletion records (correction_delete_ind='D')
 * - Write merged serving spine (s3://data-sink/active/usaspending/award_search_merged/)
 * - Build BTREE indices on award key and GTM filters
 * - Record ops.* audit row and POST Trigger callback
 */
interface ReconcileDailyCallback {
  status: "success" | "error";
  rows_merged: number;
  columns: number;
  table_rows_after: number;
  max_last_modified?: string;
  dataset_uri?: string;
  run_mode?: string;
  error?: string;
}

/**
 * Backfill: one-time merge of all API landings ≥ snapshot date (2026-05-06).
 * Manually triggerable with `{ }` (no payload required).
 */
export const usaspendingAwardSearchReconcileBackfill = task({
  id: "usaspending-award-search-reconcile-backfill",
  maxDuration: 7200, // 2 hours; merge operation is I/O + CPU intensive
  run: async (payload: {}, { ctx }) => {
    const token = await wait.createToken({
      timeout: "2h",
      tags: ["usaspending-award-search-reconcile", "backfill", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-pipelines",
        function_name: "run_backfill",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_award_search_reconcile backfill → Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<ReconcileDailyCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `usaspending_award_search_reconcile backfill timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `usaspending_award_search_reconcile backfill failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("usaspending_award_search_reconcile backfill complete", { ...result.output });
    return result.output;
  },
});

/**
 * Daily: recurring merge of trailing landings (default 7d window).
 * Scheduled daily at 04:00 UTC.
 */
export const usaspendingAwardSearchReconcileDaily = schedules.task({
  id: "usaspending-award-search-reconcile-daily",
  cron: "0 4 * * *", // 04:00 UTC daily
  maxDuration: 3600, // 1 hour; daily incremental is faster than backfill
  run: async (payload: {}, { ctx }) => {
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["usaspending-award-search-reconcile", "daily", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-pipelines",
        function_name: "run_daily",
        kwargs: { days: 7 },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_award_search_reconcile daily → Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<ReconcileDailyCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `usaspending_award_search_reconcile daily timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `usaspending_award_search_reconcile daily failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("usaspending_award_search_reconcile daily merge complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
