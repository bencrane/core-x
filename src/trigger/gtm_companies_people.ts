import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — GTM minimal materialization (companies + people), Directive 8.
 *
 * Trigger.dev v4 durable callback. Manual cadence (no cron): trigger this task to re-snapshot
 * the DEX gtm.companies / gtm.people grains into the Gen-3 active sink. Each run is a
 * FULL-SNAPSHOT overwrite of both Lance datasets (mode="overwrite" in the worker), so
 * re-running is safe. Pass { only: "companies" | "people" } to materialize just one grain.
 *
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target worker +
 *      that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune to
 *      HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the flat callback with terminal metadata.
 *
 * Flip to `schedules.task` with a cron if/when an automated refresh cadence is wanted.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface MigrationCallback {
  status: "success" | "error";
  feed: string;
  source_db?: string;
  rows_total?: number;
  datasets?: Record<string, number>;
}

export const gtmCompaniesPeopleMaterialize = task({
  id: "gtm-companies-people-materialize",
  // The durable wait consumes no compute while suspended; generous cap.
  maxDuration: 2100,
  run: async (payload: { only?: string }) => {
    logger.info("GTM companies/people materialization starting", { only: payload?.only });

    // 1) Durable callback token. token.url's callbackHash is the auth — no API key.
    const token = await wait.createToken({ timeout: "1h", tags: ["gtm", "materialize", "modal-dispatch"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    //    Pass `only` through when provided; otherwise the worker materializes both grains.
    const kwargs: Record<string, string> = {};
    if (payload?.only) kwargs.only = payload.only;

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "gtm-company-people",
        function_name: "ingest_gtm_company_people",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    //    result.ok === false ONLY on token timeout (no callback arrived).
    const result = await wait.forToken<MigrationCallback>(token.id);
    if (!result.ok) {
      throw new Error(`GTM materialization timed out before Modal callback (token ${token.id})`);
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(`GTM materialization failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("GTM companies/people materialization complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
