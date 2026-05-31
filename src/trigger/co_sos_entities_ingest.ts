import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Colorado SoS bulk business-entity registry ingest.
 *
 * Trigger.dev v4 durable callback. Manual / on-demand by design (no cron): the
 * operator drops a new date-stamped snapshot into the R2 landing zone and
 * triggers this task with an optional { key, snapshot_date }. There is no Socrata
 * fetch phase yet — the CSV is landed out of band. This task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts (the overwrite + 7 index builds over ~3M rows run long),
 *   4. resumes when the Modal worker POSTs the callback with its terminal metadata.
 */

const DEFAULT_KEY = "landing/co_sos/Business_Entities_in_Colorado_20260531.csv";

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  snapshot_date?: string;
  dataset_uri?: string;
}

export const coSosEntitiesIngest = task({
  id: "co-sos-entities-ingest",
  // The overwrite + 4 BTREE / 3 BITMAP builds run long; the durable wait itself
  // consumes no compute while suspended.
  maxDuration: 7200,
  run: async (payload: { key?: string; snapshot_date?: string }) => {
    const key = payload?.key ?? DEFAULT_KEY;
    const snapshotDate = payload?.snapshot_date;
    logger.info("CO SoS entities ingest starting", { key, snapshotDate });

    // 1) Durable callback token (generous — a ~16 GiB ingest + index of ~3M rows).
    const token = await wait.createToken({ timeout: "2h", tags: ["co-sos-entities"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "co-sos-entities",
        function_name: "ingest_co_sos",
        kwargs: { key, ...(snapshotDate ? { snapshot_date: snapshotDate } : {}) },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status} for ${key}: ${body.slice(0, 300)}`);
    }

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const out = await wait.forToken<IngestCallback>(token.id);
    if (!out.ok) throw new Error(`timed out before Modal callback for ${key}`);
    if (out.output.status !== "success") {
      throw new Error(`Modal failed for ${key}: ${JSON.stringify(out.output)}`);
    }

    logger.info("CO SoS entities ingest complete", { ...out.output });
    return out.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
