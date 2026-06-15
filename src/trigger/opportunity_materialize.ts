import { logger, task } from "@trigger.dev/sdk";

import { callHqx } from "./lib/hqx-client";

/**
 * Control plane — Booking → CRM opportunity materialization.
 *
 * Fired by the cal.com webhook on a NEW booking (sibling of booking-enrich /
 * parallel-deep-research). The TS task owns ZERO state: it calls edge_api's
 * `/internal/opportunities/materialize` (callHqx → TRIGGER_SHARED_SECRET), which
 * projects the normalized `corex.bookings` row into `business.accounts` +
 * `business.contacts` + `business.opportunities` in one transaction.
 *
 * Idempotent end-to-end (the edge_api upsert collapses on the booking + domain/email
 * keys), so the default retry policy is safe. This task is the seam the DocRaptor
 * engagement-document render layers onto in phase 2.
 */

interface OpportunityMaterializePayload {
  /** The booking's stable iCalUID — the materialization anchor (required). */
  icalUid: string;
}

interface MaterializeResult {
  action: string; // created | updated | skipped_no_booking
  ical_uid?: string;
  booking_id?: string;
  account_id?: string;
  contact_id?: string | null;
  opportunity_id?: string;
  status?: string;
}

export const opportunityMaterialize = task({
  id: "opportunity-materialize",
  maxDuration: 120,
  run: async (payload: OpportunityMaterializePayload): Promise<MaterializeResult> => {
    const icalUid = (payload?.icalUid ?? "").trim();
    if (!icalUid) {
      throw new Error("icalUid is required (the booking anchor for materialization)");
    }

    logger.info("opportunity-materialize starting", { icalUid });

    const result = await callHqx<MaterializeResult>(
      "/internal/opportunities/materialize",
      { icalUid },
    );

    logger.info("opportunity-materialize complete", { icalUid, ...result });
    return result;
  },
});
