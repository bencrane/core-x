import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Florida Federal Lien Registrations (FLR / federal tax liens).
 *
 * Trigger.dev v4 durable callback. ONE Modal worker invocation parses the three
 * fixed-width members (FLRF/FLRD/FLRS), builds the debtor-grain unified view, and
 * writes + BTREE-indexes one Lance dataset — so this is ONE dispatch, not a fan-out.
 * This task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the flat-JSON callback with terminal metadata.
 *
 * Cadence: the FLR bulk export is QUARTERLY. The cron fires on the 12th of Jan/Apr/Jul/Oct
 * (a buffer for the operator to land the new quarter's ZIPs to
 * s3://data-sink/landing/fl_federal_tax_liens/). The worker re-reads whatever is currently
 * landed and overwrites the dataset, so a manual re-trigger after an out-of-band drop is safe.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  feed: string;
  dataset_uri: string;
  as_of: string;
  rows_processed: number;
  filing_rows: number;
  secured_rows: number;
  dropped_sentinel: number;
  indexes: string[];
  index_mode: string;
}

export const flFederalTaxLiens = schedules.task({
  id: "fl-federal-tax-liens-ingest",
  // 16:00 UTC on the 12th of each quarter's first month — after the quarterly FLR drop lands.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 16 12 1,4,7,10 *", timezone: "UTC" },
  // One Modal invocation builds + indexes the dataset; the durable wait consumes no compute.
  maxDuration: 3600,
  run: async (payload) => {
    const asOf = payload.timestamp.toISOString().slice(0, 10);
    logger.info("FL FLR ingest starting", { as_of: asOf });

    // 1) Durable callback token.
    const token = await wait.createToken({ timeout: "1h", tags: ["fl-federal-tax-liens"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "fl-federal-tax-liens",
        function_name: "ingest_fl_flr",
        kwargs: { as_of: asOf },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 300)}`);
    }

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const out = await wait.forToken<IngestCallback>(token.id);
    if (!out.ok) throw new Error("timed out before Modal callback for fl_federal_tax_liens");
    if (out.output.status !== "success") {
      throw new Error(`Modal failed for fl_federal_tax_liens: ${JSON.stringify(out.output)}`);
    }

    logger.info("FL FLR ingest complete", {
      rows_processed: out.output.rows_processed,
      indexes: out.output.indexes,
      index_mode: out.output.index_mode,
    });
    return out.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
