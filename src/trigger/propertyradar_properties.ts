import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — PropertyRadar quota-governed property/owner harvest.
 *
 * Trigger.dev v4 durable callback. MANUAL / ON-DEMAND by design (no cron): this is a
 * BILLABLE feed — PropertyRadar charges one credit per purchased property — so every
 * run is a deliberate operator act with an explicit `max_allowed_spend` ceiling and a
 * `criteria` query. Cadence belongs to a human, never a schedule.
 *
 * One Modal worker invocation runs the full two-stage Quota Governor (free preview →
 * threshold check → only-then billable pagination) and produces both Lance datasets, so
 * this is ONE dispatch, not a fan-out. This task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback — no API key),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target worker +
 *      that callback url + the governor parameters,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute, immune to HTTP
 *      timeouts,
 *   4. resumes when the Modal worker POSTs the flat-JSON callback with terminal metadata.
 *
 * The governor's HARD STOP (matched set exceeds max_allowed_spend) is a CLEAN terminal
 * state, not a failure: status="aborted_over_budget" returns the required credit cost so
 * the operator can tighten the criteria. Only status="error" is thrown.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "aborted_over_budget" | "error";
  feed: string;
  governor_decision: "preview_only" | "aborted_over_budget" | "authorized";
  property_uri: string;
  person_uri: string;
  max_allowed_spend: number;
  matches_found: number; // totalResultCount from the free preview
  credits_consumed: number; // exact spend = rows retrieved under Purchase=1
  pages: number;
  property_rows: number;
  person_rows: number;
  property_indexes: string[];
  person_indexes: string[];
  error?: string;
}

export const propertyRadarIngest = task({
  id: "propertyradar-ingest",
  // The durable wait itself consumes no compute while suspended.
  maxDuration: 3900,
  run: async (payload: {
    criteria?: Array<Record<string, unknown>>;
    max_allowed_spend?: number;
    page_limit?: number;
  }) => {
    // max_allowed_spend DEFAULTS TO 0 → preview-only (spends nothing) unless the operator
    // consciously authorizes a credit ceiling. The governor enforces this in the worker.
    const maxAllowedSpend = payload?.max_allowed_spend ?? 0;
    const criteria = payload?.criteria ?? [];
    logger.info("PropertyRadar ingest starting", {
      max_allowed_spend: maxAllowedSpend,
      criteria_count: criteria.length,
    });

    // 1) Durable callback token.
    const token = await wait.createToken({ timeout: "1h", tags: ["propertyradar"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "propertyradar-pipelines",
        function_name: "ingest_propertyradar",
        kwargs: {
          criteria,
          max_allowed_spend: maxAllowedSpend,
          ...(payload?.page_limit ? { page_limit: payload.page_limit } : {}),
        },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 300)}`);
    }

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const out = await wait.forToken<IngestCallback>(token.id);
    if (!out.ok) throw new Error("timed out before Modal callback for propertyradar");

    // The governor's hard stop is a clean, expected outcome — surface it, do not throw.
    if (out.output.status === "aborted_over_budget") {
      logger.warn("PropertyRadar quota governor HARD STOP — over budget, nothing purchased", {
        matches_found: out.output.matches_found,
        max_allowed_spend: out.output.max_allowed_spend,
      });
      return out.output;
    }
    if (out.output.status !== "success") {
      throw new Error(`Modal failed for propertyradar: ${JSON.stringify(out.output)}`);
    }

    logger.info("PropertyRadar ingest complete", {
      governor_decision: out.output.governor_decision,
      credits_consumed: out.output.credits_consumed,
      property_rows: out.output.property_rows,
      person_rows: out.output.person_rows,
    });
    return out.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
