import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — query-sidecar daily artifact rebuild.
 *
 * Trigger.dev v4 owns cadence (modal.Cron is forbidden in core-x). Daily at
 * 11:00 UTC — after the overnight usaspending/gtm mart cycles land — this task:
 *   1. mints a waitpoint token (pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher targeting the `query-sidecar` app's
 *      `build` worker (tiers A,B,C,D → sorted .duckdb → blue-green publish to
 *      s3://data-sink/query-sidecar/ with LATEST swap → best-effort
 *      POST /api/v1/refresh to query-sidecar-api, which hot-swaps serving),
 *   3. suspends on `wait.forToken`,
 *   4. resolves from the worker's terminal callback (one ops.query_sidecar_runs
 *      row is written either way).
 *
 * The whole ~708M-row rebuild runs ~10 minutes on one 128GiB/8cpu container;
 * per-mart row-count parity against pinned Lance versions gates the publish —
 * a parity failure publishes NOTHING and the serving endpoint keeps the prior
 * artifact (measured failure path: docs/plans/QUERY_SIDECAR_PHASE1_RUN_RECORD.md).
 * Manual rebuild remains: `modal run pipelines/query_sidecar/build_query_sidecar.py::run`.
 */

interface BuildCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env: ${name}`);
  return v;
}

export const querySidecarRebuild = schedules.task({
  id: "query-sidecar-rebuild",
  cron: { pattern: "0 11 * * *", timezone: "UTC" },
  maxDuration: 3600,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "90m",
      tags: ["query-sidecar", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "query-sidecar",
        function_name: "build",
        kwargs: { tiers: "A,B,C,D", publish: true, smoke: false },
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched query-sidecar rebuild to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<BuildCallback>(token.id);
    if (!result.ok) {
      throw new Error(`query-sidecar rebuild timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status !== "success") {
      throw new Error(`query-sidecar rebuild failed: ${JSON.stringify(result.output)}`);
    }
    logger.info("query-sidecar rebuild complete", { rows: result.output.rows });
    return result.output;
  },
});
