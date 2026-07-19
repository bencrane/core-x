import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — CMS Open Payments bulk ingest (all program years the catalog advertises).
 *
 * Trigger.dev v4 durable callback. The Modal worker (cms-open-payments-pipelines) resolves
 * the CKAN metastore catalog and ingests three DETAIL families — General / Research /
 * Ownership payments — into three DISTINCT Lance datasets, accumulating every program year,
 * each keyed on the NPI resolution key. One sequential orchestrator (`refresh_all`) on a
 * single heavy container processes one year at a time (download → DuckDB → Lance append →
 * rm) so ephemeral disk never holds two years' files at once, then BTREE/BITMAP-indexes
 * each family once its years land.
 *
 * QUARTERLY cadence (06:00 UTC, 1st of Jan/Apr/Jul/Oct): CMS publishes annually (≈June) and
 * issues rolling corrections + late submissions throughout the year. A quarterly full
 * refresh re-pulls every advertised year; the worker's idempotent per-year delete+append
 * means re-ingesting an unchanged year is a clean no-op, and a corrected/new year is picked
 * up automatically (the worker reads whatever the catalog returns — no hardcoded year list).
 *
 * Mechanics: mint a waitpoint token (its `url` is a pre-signed HTTP callback — the
 * callbackHash in the URL authenticates, no API key), POST the Universal Dispatcher (the
 * ONLY Modal endpoint) with the target worker + that callback url, then suspend on
 * `wait.forToken` — checkpointed, zero compute, immune to HTTP timeouts across a multi-hour
 * backfill — and resume when the worker POSTs its flat-JSON terminal summary. Trigger owns
 * true end-to-end success/failure state; no polling, no heartbeat.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface RefreshCallback {
  status: "success" | "partial" | "error";
  feed: string;
  phase: "refresh_all";
  units_total: number;
  units_succeeded: number;
  units_failed: number;
  rows_processed: number;
  by_family: Record<string, { rows: number; years: number[]; indices: string[] }>;
  failures: Array<{ family: string; year: number; error: string }>;
}

export const cmsOpenPaymentsRefresh = schedules.task({
  id: "cms-open-payments-refresh",
  // 06:00 UTC on the 1st of Jan/Apr/Jul/Oct — quarterly, to catch CMS's annual publish
  // plus rolling late submissions / corrections.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 6 1 1,4,7,10 *", timezone: "UTC" },
  // Generous ceiling: the durable wait consumes no compute while suspended; the actual
  // work (a full historical backfill — General alone is ~8 GB/year × all years) runs on
  // Modal and is bounded by the worker's own 10 h function timeout + the 12 h token below.
  maxDuration: 46800,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token. 12 h window spans a full cold backfill.
    const token = await wait.createToken({
      timeout: "12h",
      tags: ["cms-open-payments", "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202). The orchestrator runs
    //    on Modal; this task does not hold the connection.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "cms-open-payments-pipelines",
        function_name: "refresh_all",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched CMS Open Payments refresh to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until Modal POSTs the callback url. 4) Resolve from it.
    const result = await wait.forToken<RefreshCallback>(token.id);

    if (!result.ok) {
      throw new Error(
        `CMS Open Payments refresh timed out before Modal callback (token ${token.id})`,
      );
    }
    // The worker re-raises only when EVERY unit fails (status 'error'); a 'partial' run
    // (some years failed, the rest landed + indexed) still wakes us with a usable result.
    if (result.output.status === "error") {
      throw new Error(`CMS Open Payments refresh failed in Modal: ${JSON.stringify(result.output)}`);
    }
    if (result.output.status === "partial") {
      logger.warn("CMS Open Payments refresh completed with per-year failures", {
        units_failed: result.output.units_failed,
        failures: result.output.failures,
      });
    }

    logger.info("CMS Open Payments refresh complete", {
      status: result.output.status,
      units_succeeded: result.output.units_succeeded,
      units_total: result.output.units_total,
      rows_processed: result.output.rows_processed,
      by_family: result.output.by_family,
    });
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
