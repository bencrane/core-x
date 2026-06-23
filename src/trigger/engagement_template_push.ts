import { logger, task } from "@trigger.dev/sdk";

import { callHqx } from "./lib/hqx-client";

/**
 * Control plane — Engagement-template render + PUSH to Documenso.
 *
 * Renders a repo-resident engagement-content source to a PDF (DocRaptor) and creates a
 * Documenso TEMPLATE from it. The TS task owns ZERO state: it calls edge_api's
 * `/internal/engagement-templates/render-push` (callHqx → TRIGGER_SHARED_SECRET), which
 * resolves the content source, renders, pushes to Documenso, and records a terminal row in
 * `ops.engagement_template_push_runs`.
 *
 * CHOOSE-WHERE-TO-PULL: pass `registryPath` (a `business.global_input_content` row, e.g.
 * `docraptor-to-documenso-template/capital-origination/v1`) — the row carries the brand +
 * source_kind — OR pass an explicit `brand`/`path`/`archetype`/`version`.
 *
 * retry maxAttempts 1: a push creates a Documenso template (a real, billable artifact), so a
 * blind retry risks duplicate templates. Re-fire deliberately if a run fails.
 */

interface EngagementTemplatePushPayload {
  /** A business.global_input_content row path, e.g. "docraptor-to-documenso-template/capital-origination/v1". */
  registryPath?: string;
  /** Or a registry row uuid. */
  registryId?: string;
  /** Or an explicit selector (used when no registry key is given). */
  brand?: string;
  path?: string;
  archetype?: string;
  version?: string;
  /** "plain" (default) | "branded". */
  style?: string;
  /** Override the Documenso template title (defaults to the manifest name). */
  title?: string;
}

interface PushResult {
  status: string;
  brand: string;
  path: string;
  archetype: string;
  version: string;
  style: string;
  source_kind: string;
  documenso_template_id: string;
  documenso_numeric_id?: number | null;
  pdf_bytes: number;
  pdf_url?: string | null;
}

export const engagementTemplatePush = task({
  id: "engagement-template-push",
  maxDuration: 300,
  retry: { maxAttempts: 1 },
  run: async (
    payload: EngagementTemplatePushPayload,
    { ctx },
  ): Promise<PushResult> => {
    const hasRegistry = Boolean(payload?.registryPath || payload?.registryId);
    const hasExplicit = Boolean(payload?.path && payload?.archetype && payload?.version);
    if (!hasRegistry && !hasExplicit) {
      throw new Error(
        "provide registryPath/registryId, or all of brand/path/archetype/version",
      );
    }

    logger.info("engagement-template-push starting", {
      registryPath: payload.registryPath,
      registryId: payload.registryId,
      brand: payload.brand,
      path: payload.path,
      archetype: payload.archetype,
      version: payload.version,
      style: payload.style,
    });

    const result = await callHqx<PushResult>(
      "/internal/engagement-templates/render-push",
      { ...payload, triggerRunId: ctx.run.id },
      // DocRaptor render + Documenso template create are sequential HTTP round-trips.
      { timeoutMs: 120_000 },
    );

    logger.info("engagement-template-push complete", {
      documenso_template_id: result.documenso_template_id,
      brand: result.brand,
      path: result.path,
    });
    return result;
  },
});
