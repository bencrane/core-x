import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SAM resolution-spine daily refresh (WS-C of the spine cycle).
 *
 * Freshness-gated, self-healing orchestrator. Daily at 18:30 UTC it dispatches the two
 * spine workers through the Universal Dispatcher (waitpoint-token durable callbacks):
 *
 *   1. build_sam_master            (skip_if_current=true) — rebuilds sam_master_entities +
 *      satellites only when entity_registrations has advanced; self-skips otherwise.
 *   2. build_sam_normalized_entities (skip_if_current=true) — ALWAYS dispatched, regardless of
 *      whether the master rebuilt. Its own skip_if_current no-ops when the sidecar already
 *      matches the master snapshot, and rebuilds when it lags. Running it unconditionally is
 *      what makes the chain SELF-HEALING: a current-master + stale-sidecar state (e.g. a prior
 *      normalized failure, or an orchestrator crash between the two steps) is caught and fixed
 *      on the next daily fire instead of being frozen forever.
 *
 * CADENCE / no-overlap. Both workers are idempotent (overwrite) and self-skipping, so a
 * no-change day is two cheap label-checks. 18:30 UTC sits after the SAM drop window (12:00),
 * sam_pocs (16:30), and the 18:00 slot (contractor_award_summary) — de-collided.
 *
 * Concurrency. queue.concurrencyLimit=1 is defense-in-depth, but note Trigger releases the
 * slot when a run checkpoints at a waitpoint (docs: queue-concurrency#waits-and-concurrency),
 * so a suspended orchestrator does NOT hold the slot. Overlap is instead precluded by the
 * timeout math: the waitpoint timeouts below (3h + 1.5h) cap this task's wall-time well under
 * the 24h cron interval, so run N always terminates before run N+1 fires. (The Modal workers
 * also carry their own 2h/1h function timeouts.) Even in a pathological overlap, the workers'
 * skip_if_current + deterministic overwrite make a redundant rebuild a no-op, not corruption.
 */

interface SpineCallback {
  status: "success" | "skipped" | "error";
  feed?: string;
  label?: string | null;
  sam_label?: string | null;
  dataset_uri?: string;
  entities_rows?: number;
  rows?: number;
  distinct_uei?: number;
}

async function dispatch(
  appName: string,
  functionName: string,
  tokenTimeout: string,
): Promise<SpineCallback> {
  const token = await wait.createToken({
    timeout: tokenTimeout,
    tags: ["sam-spine-refresh", "modal-dispatch", functionName],
  });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: appName,
      function_name: functionName,
      kwargs: { skip_if_current: true },
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for ${functionName}: ${body.slice(0, 500)}`);
  }

  const result = await wait.forToken<SpineCallback>(token.id);
  if (!result.ok) {
    throw new Error(`${functionName} timed out before Modal callback (token ${token.id})`);
  }
  if (result.output.status !== "success" && result.output.status !== "skipped") {
    throw new Error(`${functionName} failed in Modal: ${JSON.stringify(result.output)}`);
  }
  return result.output;
}

export const samSpineRefresh = schedules.task({
  id: "sam-spine-refresh",
  cron: { pattern: "30 18 * * *", timezone: "UTC" },
  queue: { concurrencyLimit: 1 },
  maxDuration: 18000, // 5h — comfortably covers both waitpoint timeouts (3h + 1.5h) + slack
  run: async (_payload, { ctx }) => {
    logger.info("sam-spine-refresh: dispatching master", { triggerRunId: ctx.run.id });

    // Step 1 — master (self-skips when entity_registrations hasn't advanced).
    const master = await dispatch("sam-gov-master-pipelines", "build_sam_master", "3h");
    logger.info("master step done", { ...master });

    // Step 2 — normalized, UNCONDITIONAL (self-heals a current-master + stale-sidecar state).
    const normalized = await dispatch(
      "sam-gov-normalized-entities-pipelines",
      "build_sam_normalized_entities",
      "90m",
    );
    logger.info("normalized step done", { ...normalized });

    return { master, normalized };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
