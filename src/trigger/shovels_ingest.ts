import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Shovels.ai parameterized ingest (permits / contractors / tags).
 *
 * Trigger.dev v4 durable callback. UNLIKE the bulk feeds, Shovels is billable —
 * every returned record costs 1 credit (size == spend) — so this is an ON-DEMAND,
 * PAYLOAD-DRIVEN task (not a blind cron). The operator triggers it with an explicit
 * geo seed + date window + page size, keeping spend under direct control. A recurring
 * pull for a fixed market is a thin wrapper (see the commented schedules.task at the
 * bottom) that calls this same task with a pinned payload.
 *
 * Flow (identical durable-callback contract to the rest of the fleet):
 *   1. validate the payload (refuse an unbounded pull — geo_id + window required for
 *      the billable search entities),
 *   2. mint a waitpoint token (its `url` is a pre-signed callback — the callbackHash
 *      in the URL authenticates; no API key),
 *   3. POST the Universal Dispatcher (the ONLY Modal endpoint) targeting the entity's
 *      worker function with the parameter kwargs + that callback url,
 *   4. suspend on `wait.forToken` — checkpointed, zero compute, timeout-immune,
 *   5. resume when the Modal worker POSTs its terminal metadata, resolve from it.
 *
 * Trigger owns true end-to-end success/failure state. No polling, no heartbeat.
 */

// The flat body the Modal worker POSTs to the waitpoint url becomes this run's output.
interface ShovelsIngestCallback {
  status: "success" | "no_data" | "error";
  feed: string;
  entity: string;
  dataset_uri: string;
  rows_fetched: number;
  rows: number;
  credits_spent: number;
  run_id: string;
}

type ShovelsEntity = "permits" | "contractors" | "tags";

export interface ShovelsIngestPayload {
  /** Which table to pull. Billable search: permits | contractors. FREE catalog: tags. */
  entity: ShovelsEntity;
  /** Required for permits/contractors: a 2-letter state code (CA), a ZIP (90210), or a
   *  Shovels geo_id. Ignored for tags. */
  geoId?: string;
  /** Required for permits/contractors. YYYY-MM-DD, inclusive, on permit start_date. */
  permitFrom?: string;
  /** Required for permits/contractors. YYYY-MM-DD, inclusive. */
  permitTo?: string;
  /** Page size == credits spent per page. Clamped to [1, 100]. Default 50. */
  size?: number;
  /** Credit guardrail: max pages fetched. <= 0 means exhaust the cursor (uncapped —
   *  explicit opt-in to a full-market backfill). Default 20. */
  maxPages?: number;
  /** Optional §7 filter surface, e.g. { permit_tags: ["solar"], property_type: "residential" }. */
  filters?: Record<string, unknown>;
}

const FUNCTION_BY_ENTITY: Record<ShovelsEntity, string> = {
  permits: "ingest_permits",
  contractors: "ingest_contractors",
  tags: "ingest_tags",
};

export const shovelsIngest = task({
  id: "shovels-ingest",
  maxDuration: 2400, // generous cap; the durable wait itself consumes no compute
  run: async (payload: ShovelsIngestPayload, { ctx }) => {
    const entity = payload.entity;
    if (!FUNCTION_BY_ENTITY[entity]) {
      throw new Error(`shovels-ingest: entity must be permits|contractors|tags, got ${entity}`);
    }

    // Build the worker kwargs. Refuse an unbounded billable pull up front (credit guard).
    let kwargs: Record<string, unknown> = {};
    if (entity === "permits" || entity === "contractors") {
      const { geoId, permitFrom, permitTo } = payload;
      if (!geoId || !permitFrom || !permitTo) {
        throw new Error(
          `shovels-ingest ${entity}: geoId + permitFrom + permitTo are required ` +
            `(refusing an unbounded, credit-spending pull).`,
        );
      }
      const size = Math.min(Math.max(payload.size ?? 50, 1), 100); // clamp [1,100]
      const maxPages = payload.maxPages ?? 20;
      kwargs = {
        geo_id: geoId,
        permit_from: permitFrom,
        permit_to: permitTo,
        size,
        max_pages: maxPages,
        ...(payload.filters ? { filters_json: JSON.stringify(payload.filters) } : {}),
      };
    }

    // 1) Mint the durable callback token.
    const token = await wait.createToken({
      timeout: "1h",
      tags: [`shovels-${entity}`, "modal-dispatch"],
    });

    // 2) Fire the Universal Dispatcher and return immediately (202).
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "shovels-pipelines",
        function_name: FUNCTION_BY_ENTITY[entity],
        kwargs,
        trigger_callback_url: token.url,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched Shovels ingest to Modal; suspending on waitpoint", {
      entity,
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until Modal POSTs the callback url. 4) Resolve from it.
    const result = await wait.forToken<ShovelsIngestCallback>(token.id);

    if (!result.ok) {
      throw new Error(`shovels ${entity} ingest timed out before Modal callback (token ${token.id})`);
    }
    if (result.output.status === "error") {
      throw new Error(`shovels ${entity} ingest failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("Shovels ingest complete", { ...result.output });
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

/*
 * Recurring pull for a fixed market — uncomment + pin the payload to schedule it.
 * Cadence belongs to Trigger v4 (modal.Cron is forbidden, ARCHITECTURE.md §1).
 *
 * import { schedules } from "@trigger.dev/sdk";
 * export const shovelsBayAreaSolarWeekly = schedules.task({
 *   id: "shovels-bay-area-solar-weekly",
 *   cron: { pattern: "0 13 * * 1", timezone: "UTC" }, // Mondays 13:00 UTC
 *   run: async () => {
 *     await shovelsIngest.triggerAndWait({
 *       entity: "permits",
 *       geoId: "94110",
 *       permitFrom: "2025-01-01",
 *       permitTo: "2025-12-31",
 *       size: 50,
 *       maxPages: 20,
 *       filters: { permit_tags: ["solar"] },
 *     });
 *   },
 * });
 */
