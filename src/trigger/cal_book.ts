import { logger, task } from "@trigger.dev/sdk";

import { callHqx } from "./lib/hqx-client";

/**
 * Control plane — Close custom activity → cal.com booking (OUTBOUND create).
 *
 * Fired by the Close webhook (`/webhooks/close`) when a `activity.custom_activity` / `created`
 * event matches the configured booking type. The TS task owns ZERO state: it calls edge_api's
 * `/internal/cal/book` (callHqx → TRIGGER_SHARED_SECRET), which reads the activity + contact from
 * the Close API and creates the cal.com booking, idempotent on `closeActivityId` via the
 * `ops.cal_booking_runs` ledger. The created booking re-enters via cal.com's own BOOKING_CREATED
 * webhook (`/webhooks/cal`), so the normalize → `corex.bookings` → materialize pipeline runs
 * unchanged — one closed loop.
 *
 * Billing/side-effecting task (a booking is a real calendar event) — `maxAttempts: 1`, no blind
 * retry. The Trigger-run idempotency key (`cal-book:{closeActivityId}`, set by the Python kickoff)
 * collapses Close's duplicate deliveries to a SINGLE run.
 */

interface CalBookPayload {
  /** The Close custom-activity id — the booking anchor + idempotency key (required). */
  closeActivityId: string;
  /** The Close lead the activity hangs off (optional). */
  closeLeadId?: string;
  /** The Close contact resolved as the booking attendee (optional). */
  closeContactId?: string;
  /** The matched custom_activity_type_id (optional, for trace). */
  customActivityTypeId?: string;
}

interface BookResult {
  status: string; // success | already_booked | skipped | error
  cal_booking_uid?: string | null;
  cal_booking_id?: string | null;
  ical_uid?: string | null;
  event_type_slug?: string | null;
  reason?: string | null;
}

export const calBook = task({
  id: "cal-book",
  maxDuration: 120,
  // Side-effecting booking — no blind retry (a retry could double-book).
  retry: { maxAttempts: 1 },
  run: async (payload: CalBookPayload): Promise<BookResult> => {
    const closeActivityId = (payload?.closeActivityId ?? "").trim();
    if (!closeActivityId) {
      throw new Error("closeActivityId is required (the booking anchor)");
    }

    logger.info("cal-book starting", { closeActivityId });

    const result = await callHqx<BookResult>("/internal/cal/book", {
      closeActivityId,
      closeLeadId: payload?.closeLeadId,
      closeContactId: payload?.closeContactId,
      customActivityTypeId: payload?.customActivityTypeId,
    });

    logger.info("cal-book complete", { closeActivityId, ...result });
    return result;
  },
});
