import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — hourly ops watchdog dispatch.
 *
 * The fleet's only scheduled failure detector (2026-07-23 modal-durability-closure
 * directive §2): hung sidecar builds, stale serving artifact, error terminal rows in
 * the in-scope ops.*_runs ledgers, cadence misses. The worker
 * (pipelines/ops_watchdog/watchdog.py, app `ops-watchdog`) is read-only; alerts land
 * via OPS_ALERT_WEBHOOK (Telegram). A quiet run alerts nothing.
 *
 * This is deliberately the ONE active schedule in src/trigger (every feed schedule
 * was parked 2026-07-19 under the free-plan 10-schedule cap — the watchdog takes one
 * slot because nothing else notices failures at all). Hourly at :17, off the
 * top-of-hour herd. Checks are sized to the cadence (error window 2h).
 */

interface WatchdogCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env: ${name}`);
  return v;
}

export const opsWatchdog = schedules.task({
  id: "ops-watchdog",
  cron: { pattern: "17 * * * *", timezone: "UTC" },
  maxDuration: 900,
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "10m",
      tags: ["ops-watchdog", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "ops-watchdog",
        function_name: "check",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched ops-watchdog check; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<WatchdogCallback>(token.id);
    if (!result.ok) {
      // The watchdog watching the watchdog: a missed callback is itself a failure —
      // Trigger marks the run failed and its dashboard shows the miss.
      throw new Error(`ops-watchdog check timed out before Modal callback (token ${token.id})`);
    }
    logger.info("ops-watchdog check complete", { findings: result.output.rows });
    return result.output;
  },
});
