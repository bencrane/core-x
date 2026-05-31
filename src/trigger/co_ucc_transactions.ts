import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Colorado UCC filing-transaction ledger bulk ingest.
 *
 * Trigger.dev v4 durable callback. Manual cadence (no cron): the CO SOS bulk file
 * is republished periodically; trigger this task with an optional { source_key }
 * when a new drop lands. Each run is a FULL-SNAPSHOT overwrite of the transaction
 * ledger (mode="overwrite" in the worker), so re-running is safe.
 *
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the flat callback with terminal metadata.
 *
 * Flip to `schedules.task` with a cron if/when an automated refresh cadence is wanted.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  snapshot_date?: string;
}

export const coUccTransactionsIngest = task({
  id: "co-ucc-transactions-ingest",
  // The durable wait consumes no compute while suspended; generous cap.
  maxDuration: 3900,
  run: async (payload: { source_key?: string }) => {
    logger.info("CO UCC transactions ingest starting", { source_key: payload?.source_key });

    // 1) Durable callback token. token.url's callbackHash is the auth — no API key.
    const token = await wait.createToken({ timeout: "1h", tags: ["co-ucc", "modal-dispatch"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    //    Pass source_key through only when provided; otherwise the worker defaults
    //    to the landed file.
    const kwargs = payload?.source_key ? { source_key: payload.source_key } : {};
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "co-ucc-filings",
        function_name: "ingest_co_ucc_transactions",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    //    result.ok === false ONLY on token timeout (no callback arrived).
    const result = await wait.forToken<IngestCallback>(token.id);
    if (!result.ok) {
      throw new Error(`CO UCC ingest timed out before Modal callback (token ${token.id})`);
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(`CO UCC ingest failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("CO UCC transactions ingest complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
