import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — per-BOOKING company enrichment (Parallel.ai Task API).
 *
 * A STANDALONE workflow (NOT parallel-enrich's audience/spec batch path). On a new cal.com
 * booking, enrich the prospect's company into a few typed firmographic columns. Same durable
 * shape as the other Parallel tasks:
 *   1. validate the payload (one company: icalUid + domain),
 *   2. mint a waitpoint token (run-unique key — never a shared constant),
 *   3. POST the Universal Dispatcher targeting `booking-enrich` / `enrich_booking`,
 *   4. suspend on `wait.forToken`,
 *   5. resume on the worker's flat callback.
 *
 * The enrichment OUTPUT SCHEMA is fixed worker-side (pipelines/parallel/booking_enrich.py,
 * `_OUTPUT_SCHEMA`) — edit it there to change the columns. This task carries no schema.
 *
 * Tier: lite|base|core (default lite). `ultra` is rejected worker-side (per-run webhook path).
 * Billing task → no blind retry.
 */

interface BookingEnrichPayload {
  /** The booking's stable iCalUID — the merge key for the enrichment row (required). */
  icalUid: string;
  /** Prospect company domain (required; the primary enrichment anchor). */
  domain: string;
  /** Prospect company name (optional but recommended — steers the extraction). */
  companyName?: string;
  /** lite | base | core. Default lite. ultra rejected worker-side. */
  processor?: "lite" | "base" | "core";
  /** Advisory USD cap recorded on the ledger. */
  maxUsd?: number;
  /** Idempotency key override. Defaults to a per-booking key. */
  idempotencyKey?: string;
}

// The flat body Modal POSTs to the waitpoint url becomes this run's output.
interface BookingEnrichCallback {
  status: "success" | "rejected" | "failed";
  run_id?: string;
  workflow?: string;
  ical_uid?: string | null;
  parallel_run_id?: string | null;
  dataset_uri?: string;
  completed?: number;
  review_fields?: number;
  error?: string | null;
}

const ALLOWED_PROCESSORS = ["lite", "base", "core"];

export const bookingEnrich = task({
  id: "booking-enrich",
  // One Parallel Task run; the durable wait consumes no compute while suspended.
  maxDuration: 1800,
  // Billing task — no blind retry (idempotencyKey reuse would risk re-billing).
  retry: { maxAttempts: 1 },
  run: async (payload: BookingEnrichPayload, { ctx }) => {
    // ── 1) Validate ────────────────────────────────────────────────────────────────────
    const icalUid = (payload?.icalUid ?? "").trim();
    if (!icalUid) {
      throw new Error("icalUid is required (the booking anchor + enrichment merge key)");
    }
    const domain = (payload?.domain ?? "").trim();
    const companyName = (payload?.companyName ?? "").trim();
    if (!domain && !companyName) {
      throw new Error("domain or companyName is required");
    }
    const processor = String(payload?.processor ?? "lite");
    if (!ALLOWED_PROCESSORS.includes(processor)) {
      throw new Error(
        `processor ${JSON.stringify(processor)} not allowed — booking-enrich caps at 'core' ` +
          `(ultra needs the per-run webhook path, deferred).`,
      );
    }
    // Per-booking waitpoint key. The waitpoint key is PROJECT-scoped (30d TTL); a per-booking
    // string keeps every booking distinct (and collapses cal's duplicate deliveries to one run).
    const idempotencyKey = payload?.idempotencyKey ?? `booking-enrich:${icalUid}`;

    const kwargs: Record<string, unknown> = {
      run_id: ctx.run.id,
      ical_uid: icalUid,
      company_name: companyName,
      domain,
      processor,
      idempotency_key: idempotencyKey,
      cost_cap: payload?.maxUsd ?? null,
    };

    logger.info("Booking enrich starting", { icalUid, domain, processor });

    // ── 2) Durable callback token ──────────────────────────────────────────────────────
    const token = await wait.createToken({
      timeout: "30m",
      idempotencyKey,
      tags: ["booking-enrich", processor, "modal-dispatch"],
    });

    // ── 3) Fire the Universal Dispatcher (202) ─────────────────────────────────────────
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "booking-enrich",
        function_name: "enrich_booking",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // ── 4) Suspend. 5) Resolve. ────────────────────────────────────────────────────────
    const result = await wait.forToken<BookingEnrichCallback>(token.id);
    if (!result.ok) {
      throw new Error(`booking enrich timed out before Modal callback (token ${token.id})`);
    }
    const out = result.output;
    if (out.status === "failed") {
      throw new Error(`booking enrich failed in Modal: ${JSON.stringify(out)}`);
    }
    if (out.status === "rejected") {
      logger.warn("Booking enrich rejected by guardrail", { error: out.error, ...out });
    } else {
      logger.info("Booking enrich complete", { ...out });
    }
    return out;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
