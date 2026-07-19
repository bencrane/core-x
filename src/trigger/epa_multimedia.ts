import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — EPA Multi-Media Compliance materialization (Directive 30).
 *
 * Trigger.dev v4 durable callback. Monthly this task mints a waitpoint token, POSTs the
 * Universal Dispatcher (the ONLY Modal endpoint) targeting the `epa-pipelines` orchestrator
 * `run_epa_ingest`, suspends on `wait.forToken` (checkpointed, zero compute), and resolves
 * when the Modal worker POSTs the callback url with its terminal metadata.
 *
 * The EPA ECHO/FRS national bulk archives refresh on a multi-week cadence; a monthly full
 * re-materialization keeps the spine + 3-year DMR window + enforcement ledgers + SoS bridge
 * current. Trigger owns true end-to-end success/failure state — no polling, no heartbeat.
 */

interface IngestCallback {
  status: "success" | "partial" | "error";
  rows: number;
  feed: string;
}

export const epaMultimediaDispatcher = schedules.task({
  id: "epa-multimedia-dispatcher",
  // 06:00 UTC on the 2nd of each month (after EPA's monthly bulk refresh settles).
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 6 2 * *", timezone: "UTC" },
  maxDuration: 10800, // durable wait consumes no compute while suspended
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "3h",
      tags: ["epa-multimedia", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "epa-pipelines",
        function_name: "run_epa_ingest",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched EPA ingest to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<IngestCallback>(token.id);
    if (!result.ok) {
      throw new Error(`EPA ingest timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status === "error") {
      throw new Error(`EPA ingest failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("EPA ingest complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
