import { logger, task } from "@trigger.dev/sdk";

import { callHqx } from "./lib/hqx-client";

/**
 * Control plane — Booking → CRM deal materialization.
 *
 * Fired by the cal.com webhook on a NEW booking (sibling of booking-enrich /
 * parallel-deep-research). The TS task owns ZERO state: it calls edge_api's
 * `/internal/deals/materialize` (callHqx → TRIGGER_SHARED_SECRET), which projects the
 * normalized `corex.bookings` row into `business.accounts` + `business.contacts` +
 * `business.deals` (one per account, via `uq_deals_account`) + the
 * `business.deal_contacts` signatory link, in one transaction.
 *
 * Replaces the retired `opportunity-materialize` task: the going-forward model is one deal
 * per ACCOUNT (advancing `last_booking_id`) rather than one opportunity per booking.
 * Idempotent end-to-end (the account dedupes on domain; the deal collapses on `account_id`),
 * so the default retry policy is safe. This is the seam the DocRaptor engagement-document
 * render layers onto in phase 2.
 */

interface DealMaterializePayload {
  /** The booking's stable iCalUID — the materialization anchor (required). */
  icalUid: string;
}

interface MaterializeResult {
  action: string; // created | updated | skipped_no_booking
  ical_uid?: string;
  booking_id?: string;
  account_id?: string;
  contact_id?: string | null;
  deal_id?: string;
  deal_handle?: string;
  status?: string;
}

export const dealMaterialize = task({
  id: "deal-materialize",
  maxDuration: 120,
  run: async (payload: DealMaterializePayload): Promise<MaterializeResult> => {
    const icalUid = (payload?.icalUid ?? "").trim();
    if (!icalUid) {
      throw new Error("icalUid is required (the booking anchor for materialization)");
    }

    logger.info("deal-materialize starting", { icalUid });

    const result = await callHqx<MaterializeResult>(
      "/internal/deals/materialize",
      { icalUid },
    );

    logger.info("deal-materialize complete", { icalUid, ...result });
    return result;
  },
});
