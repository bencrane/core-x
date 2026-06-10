import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — EPA Unified Facility Spine (identifiers-only).
 *
 * Trigger.dev v4 durable callback. The spine is a pure function of the landed EPA datasets
 * (epa_facilities / epa_echo_exporter / epa_program_links / the detail giants), so it must
 * re-derive AFTER the monthly EPA multi-media refresh (`epa-multimedia-dispatcher`, 06:00 UTC
 * on the 2nd) settles — never on an independent clock, or it would read a temporally-skewed mix
 * of new and stale source tables (plan D3). This task fires a few hours later, mints a waitpoint,
 * POSTs the Universal Dispatcher targeting `epa-spine-pipelines::run_epa_spine`, suspends on
 * `wait.forToken` (checkpointed, zero compute), and resolves on the Modal terminal callback.
 *
 * `run_epa_spine` itself runs preflight → crosswalks → spine → rollups (NPDES heavy, separate
 * container) → capstone → `verify_epa_spine` published re-gate — the authoritative success check,
 * independent of any build's return value (the _verify_published doctrine).
 */

interface SpineCallback {
  status: "success" | "partial" | "error";
  feed: string;
}

export const epaSpineDispatcher = schedules.task({
  id: "epa-spine-dispatcher",
  // 12:00 UTC on the 2nd — ~6h after the EPA multi-media refresh dispatch, so the source
  // datasets the spine reads are the freshly-materialized ones.
  cron: { pattern: "0 12 2 * *", timezone: "UTC" },
  maxDuration: 28800, // durable wait consumes no compute while suspended (heavy NPDES DMR scan)
  run: async (_payload, { ctx }) => {
    const token = await wait.createToken({
      timeout: "8h",
      tags: ["epa-spine", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "epa-spine-pipelines",
        function_name: "run_epa_spine",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched EPA spine build to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<SpineCallback>(token.id);
    if (!result.ok) {
      throw new Error(`EPA spine timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status === "error") {
      throw new Error(`EPA spine build failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("EPA spine build complete", { ...result.output });
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
