import { logger, task } from "@trigger.dev/sdk";

import { callHqx } from "./lib/hqx-client";

/**
 * Control plane — AO engagement-document render (PARALLEL pathway).
 *
 * Fired by the rare-structure-hq Applications "lock the price + term" action (via the platform-api
 * BFF → edge_api generate-mandate). The TS task owns ZERO state: it calls edge_api's
 * `/internal/engagement-doc/render` (callHqx → TRIGGER_SHARED_SECRET), which binds the opportunity's
 * values + the locked package into the repo-resident static AO term-only HTML, renders a plain PDF
 * via DocRaptor, stores it in R2, and records the mandate.
 *
 * Distinct from the proposal/markdown pathway. Idempotent end-to-end (the mandate upserts on
 * opportunity_id), so the default retry policy is safe.
 */

interface EngagementDocRenderPayload {
  /** The opportunity to render the mandate for (required). */
  opportunityId: string;
  /** The locked price+term preset key (required; the dollar amount is resolved server-side). */
  packageKey: string;
}

interface RenderResult {
  action: string; // rendered | failed | skipped_no_opportunity
  status?: string;
  opportunity_id?: string;
  mandate_id?: string;
  pdf_url?: string;
  pdf_bytes?: number;
}

export const engagementDocRender = task({
  id: "engagement-doc-render",
  maxDuration: 180,
  run: async (payload: EngagementDocRenderPayload): Promise<RenderResult> => {
    const opportunityId = (payload?.opportunityId ?? "").trim();
    const packageKey = (payload?.packageKey ?? "").trim();
    if (!opportunityId) throw new Error("opportunityId is required");
    if (!packageKey) throw new Error("packageKey is required");

    logger.info("engagement-doc-render starting", { opportunityId, packageKey });

    const result = await callHqx<RenderResult>("/internal/engagement-doc/render", {
      opportunityId,
      packageKey,
    });

    logger.info("engagement-doc-render complete", { opportunityId, ...result });
    return result;
  },
});
