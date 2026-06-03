import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Blitz Contact Hydration, Waterfall ICP (Directive 20-Final).
 *
 * Event-triggered (NO cron): the interactive GTM agent fires this task via the
 * gtm-mcp `launch_contact_hydration` tool (Trigger REST trigger) to hydrate a
 * saved campaign audience with executive contacts. Canonical durable-callback
 * pattern (mirror src/trigger/enrichment_blitz.ts): mint a waitpoint token, POST
 * the Universal Dispatcher with the target worker + the callback url, suspend on
 * wait.forToken (zero compute while suspended), resume from the worker's RAW
 * terminal-counts callback.
 *
 * DECOUPLED + GATEWAY-ROUTED (Directive 20-Final). Trigger carries only signals —
 * `{ campaignId, audienceName?, cascade, ... }`, NEVER company lists or contact
 * rows. The Modal worker (`blitz-hydration-waterfall`) reads ops.campaign_audiences,
 * resolves the audience over the Lance plane, and routes EVERY Blitz call (bridge,
 * Waterfall, email/phone) through the single-container core/blitz_gateway.py — the
 * authoritative global ≤5-RPS priority egress. `priority` (default "high") rides
 * the gateway lane so interactive GTM hydration preempts background bulk sweeps.
 *
 * Dynamic input: `cascade` is the NATIVE Waterfall cascade array, populated
 * dynamically by the upstream Anthropic GTM agent (ordered include_title /
 * exclude_title / location tiers).
 *
 * Scale: the worker streams the whole audience through the gateway in one
 * invocation (the gateway is the bottleneck). Very large audiences (≳ a few
 * thousand companies) should be chunked by the caller — batch.trigger fan-out is
 * the documented scaling path when worker wall-clock approaches maxDuration.
 */

// One native Waterfall cascade tier (passed straight through to Blitz by the worker).
interface CascadeTier {
  include_title: string[];
  exclude_title?: string[];
  location?: string[]; // ISO-3166 alpha-2, or ["WORLD"]
  include_headline_search?: boolean;
}

interface HydrationPayload {
  campaignId: string;
  audienceName?: string; // omit → the campaign's most-recently-updated audience
  cascade: CascadeTier[]; // ordered tiers (≤8); the dynamic ICP persona
  maxResultsPerCompany?: number; // 1–25 (Waterfall cap); default 5
  enrichEmail?: boolean; // default true
  enrichPhone?: boolean; // default false (US-only at the API)
  priority?: "high" | "normal" | "low"; // gateway lane; default "high" (interactive)
}

// The RAW body the Modal worker POSTs to the waitpoint url becomes result.output.
interface HydrationCallback {
  status: "success" | "error";
  feed: string;
  campaign_id: string;
  audience_name: string | null;
  companies_in: number;
  contacts_found: number;
  emails_found: number;
  phones_found: number;
  gateway_calls: number;
  landing_uri: string | null;
  error?: string | null;
}

export const blitzContactHydrationWaterfall = task({
  id: "blitz-contact-hydration-waterfall",
  // The durable wait consumes no compute while suspended; generous compute cap.
  maxDuration: 3900,
  run: async (payload: HydrationPayload, { ctx }) => {
    if (!payload?.campaignId) throw new Error("campaignId is required");
    if (!Array.isArray(payload?.cascade) || payload.cascade.length === 0) {
      throw new Error("cascade must be a non-empty array of Waterfall tiers");
    }

    // 1) Durable callback token. Generous timeout — a large audience streamed
    //    through the ≤5-RPS gateway can run long; suspended wait is free.
    const token = await wait.createToken({
      timeout: "2h",
      tags: ["blitz-hydration", "waterfall", `campaign-${payload.campaignId}`],
    });

    // 2) Fire the Universal Dispatcher (202). The worker runs out of band; this
    //    task holds no connection. Trigger carries only signals.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "blitz-hydration-waterfall",
        function_name: "hydrate_campaign_waterfall",
        kwargs: {
          campaign_id: payload.campaignId,
          audience_name: payload.audienceName ?? null,
          cascade: payload.cascade,
          max_results_per_company: payload.maxResultsPerCompany ?? 5,
          enrich_email: payload.enrichEmail ?? true,
          enrich_phone: payload.enrichPhone ?? false,
          priority: payload.priority ?? "high",
          run_id: ctx.run.id,
        },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("blitz-hydration dispatched; suspending on waitpoint", {
      campaignId: payload.campaignId,
      tokenId: token.id,
      runId: ctx.run.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    //    result.ok === false ONLY on token timeout (no callback arrived).
    const result = await wait.forToken<HydrationCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `blitz-hydration timed out before Modal callback (token ${token.id})`,
      );
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(
        `blitz-hydration failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("blitz-hydration complete", { ...result.output });
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
