import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Overture Maps Foundation "Places" spatial bulk ingest.
 *
 * Trigger.dev v4 durable callback, MONTHLY cadence. The worker reads the public
 * Overture GeoParquet anonymously (DuckDB httpfs + spatial), flattens the
 * geometry to longitude/latitude floats, filters addresses[1].country='US', and
 * overwrites the Lance dataset at s3://data-sink/active/overture_places/ (built
 * on local disk, published via boto3 to dodge R2's multipart part-size rule).
 *
 * Cadence lives ONLY here (no modal.Cron). The 5th-of-month run lets the latest
 * Overture release settle; the worker resolves the newest release automatically,
 * so no date is pinned in code. `wait.forToken` suspends the run while Modal
 * computes — checkpointed, zero compute, immune to HTTP timeouts — and resumes
 * when the worker POSTs its flat-JSON terminal callback.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  release_tag?: string;
  snapshot_date?: string;
  distinct_ids?: number | null;
  published_files?: number;
  published_bytes?: number;
  write_path?: string;
}

export const overturePlaces = schedules.task({
  id: "overture-places-ingest",
  // Monthly, 5th @ 06:00 UTC — the latest Overture release has settled by then.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 6 5 * *", timezone: "UTC" },
  // Generous compute cap; the durable wait itself consumes no compute while
  // suspended (the token timeout, not maxDuration, bounds the wait window).
  maxDuration: 5400,
  run: async (_payload, { ctx }) => {
    // 1) Mint the durable callback token (generous — full S3 read + 25–30M-row
    //    index sorts + boto3 publish). token.url is a pre-signed HTTP callback;
    //    the callbackHash in the URL authenticates — no API key.
    const token = await wait.createToken({ timeout: "2h", tags: ["overture-places"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "overture-maps-pipelines",
        function_name: "ingest_overture_places",
        kwargs: {}, // empty → worker resolves the latest release itself
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched Overture Places to Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const result = await wait.forToken<IngestCallback>(token.id);

    // result.ok === false ONLY on token timeout (no callback arrived).
    if (!result.ok) {
      throw new Error(`Overture Places ingest timed out before Modal callback (token ${token.id})`);
    }
    // ok:true still carries the worker's payload — inspect business status.
    if (result.output.status !== "success") {
      throw new Error(`Overture Places ingest failed in Modal: ${JSON.stringify(result.output)}`);
    }

    logger.info("Overture Places ingest complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}
