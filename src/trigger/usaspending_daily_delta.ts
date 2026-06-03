import { schedules, runs, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending award_search DAILY DELTA (merge_insert freshness).
 *
 * Trigger.dev v4 durable callback. Daily at 11:00 UTC this task:
 *   0. GUARD — refuses to run if a `usaspending-bulk` ingest is in flight (that task
 *      rewrites award_search with mode="overwrite"; a concurrent merge_insert would
 *      race the commit). See the guard note below.
 *   1. mints a waitpoint token (its `url` is a pre-signed callback — no API key),
 *   2. POSTs the Universal Dispatcher to spawn the `ingest_award_search_delta` worker
 *      with that callback url,
 *   3. suspends on `wait.forToken` (checkpointed, zero compute) while the worker runs
 *      in Modal — cold-start (bulk_download/awards) can take many minutes,
 *   4. resumes when the worker POSTs its flat terminal metadata.
 *
 * The worker auto-selects cold-start vs steady-state from the ops watermark
 * (max(feed_date) WHERE status='success' in ops.usaspending_award_search_delta_runs):
 * empty ledger ⇒ wide cold-start [2026-05-06 → yesterday]; otherwise the single-day
 * steady-state window. No payload is needed — the watermark IS the state.
 *
 * CADENCE (all artifact-derived). 11:00 UTC sits after USAspending's ~05:00 UTC
 * nightly ETL (Gen-2 precedent ran 06:00/08:00) and BEFORE the existing award_search
 * consumers — crosswalk_sam_usaspending (16:00) and contractor_award_summary (18:00) —
 * so they read a freshened award_search the same day. Token timeout 2h covers a
 * worst-case cold-start (async bulk job + 78M-row merge) with headroom.
 *
 * GUARD note. core-x has no single-dataset commit-lock helper; for a SHARED dataset
 * concurrency is managed by scheduling discipline. award_search is shared by exactly
 * two writers — the monthly `usaspending-bulk` (overwrite) and this daily delta
 * (merge_insert). They must never co-run. The bulk is rare + manual; this guard makes
 * the daily side defensive. A transient management-API error in the guard is logged
 * and we PROCEED (a list hiccup must not silently skip the daily delta; a real merge
 * conflict would surface as a failed run, not corruption).
 */

const BULK_TASK_ID = "usaspending-bulk";
// Non-terminal run states (the @trigger.dev/sdk v4 RunStatus enum). CRITICAL: the
// bulk orchestrator spends most of its wall-clock SUSPENDED on durable waitpoints,
// which is the WAITING state — omit it and an in-flight bulk would look idle and the
// guard would wave a conflicting merge through. Excludes every terminal state
// (COMPLETED/CANCELED/FAILED/CRASHED/SYSTEM_FAILURE/EXPIRED/TIMED_OUT).
const ACTIVE_STATUSES = [
  "QUEUED",
  "DEQUEUED",
  "EXECUTING",
  "WAITING",
  "PENDING_VERSION",
  "DELAYED",
] as const;

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface DeltaCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  run_mode?: "cold_start" | "steady_state";
  window_start?: string;
  window_end?: string;
  api_calls?: number;
}

async function bulkIngestInFlight(): Promise<boolean> {
  try {
    for await (const r of runs.list({
      taskIdentifier: [BULK_TASK_ID],
      status: [...ACTIVE_STATUSES],
      limit: 1,
    })) {
      logger.warn("usaspending-bulk run is active; aborting daily delta", {
        bulkRunId: r.id,
        bulkStatus: r.status,
      });
      return true;
    }
    return false;
  } catch (err) {
    // Management-API hiccup — do not let it silently skip the daily delta.
    logger.warn("bulk-in-flight guard check failed; proceeding (defensive)", {
      error: String(err).slice(0, 200),
    });
    return false;
  }
}

export const usaspendingDailyDelta = schedules.task({
  id: "usaspending-daily-delta",
  // 11:00 UTC daily — after USAspending's nightly ETL, before the award_search consumers.
  cron: { pattern: "0 11 * * *", timezone: "UTC" },
  // Generous cap; the durable wait consumes no compute while suspended.
  maxDuration: 7800,
  run: async (_payload, { ctx }) => {
    // 0) Commit-conflict guard.
    if (await bulkIngestInFlight()) {
      throw new Error(
        "Aborting: a usaspending-bulk ingest is concurrently active on award_search " +
          "(overwrite vs merge_insert commit conflict). The next scheduled run will " +
          "self-heal via the ops watermark.",
      );
    }

    // 1) Mint the durable callback token.
    const token = await wait.createToken({
      timeout: "2h",
      tags: ["usaspending-daily-delta", "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher; the worker runs in Modal (this task does not
    //    hold the connection). The worker resolves its window from the ops watermark.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-daily-delta",
        function_name: "ingest_award_search_delta",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_daily_delta to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<DeltaCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `usaspending_daily_delta timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `usaspending_daily_delta failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("usaspending_daily_delta complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
