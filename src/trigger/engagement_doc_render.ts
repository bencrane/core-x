import { logger, task } from "@trigger.dev/sdk";

import { callHqx } from "./lib/hqx-client";

/**
 * Control plane — AO engagement-document render (PARALLEL pathway).
 *
 * Fired by the rare-structure-hq Applications "Stage" action (via the platform-api BFF → edge_api
 * generate-mandate, which INSERTs a deal row first). The TS task owns ZERO state: it calls edge_api's
 * `/internal/engagement-doc/render` (callHqx → TRIGGER_SHARED_SECRET) with the deal's id, which binds
 * the opportunity's values + the deal's locked package into the static AO term-only HTML, renders a
 * plain PDF via DocRaptor, stores it in R2, and creates a DRAFT Documenso document.
 *
 * One deal = one document; an opportunity may have many. Idempotency key is the deal id, so a
 * double-click of the same deal collapses to one run.
 */

interface EngagementDocRenderPayload {
  /** The deal (mandate) row to render — its locked terms already live on the row. */
  mandateId: string;
}

interface RenderResult {
  action: string; // rendered | failed
  status?: string;
  mandate_id?: string;
  opportunity_id?: string;
  pdf_bytes?: number;
  documenso_envelope_id?: string;
}

export const engagementDocRender = task({
  id: "engagement-doc-render",
  maxDuration: 180,
  run: async (payload: EngagementDocRenderPayload): Promise<RenderResult> => {
    const mandateId = (payload?.mandateId ?? "").trim();
    if (!mandateId) throw new Error("mandateId is required");

    logger.info("engagement-doc-render starting", { mandateId });

    const result = await callHqx<RenderResult>("/internal/engagement-doc/render", { mandateId });

    logger.info("engagement-doc-render complete", { mandateId, ...result });
    return result;
  },
});
