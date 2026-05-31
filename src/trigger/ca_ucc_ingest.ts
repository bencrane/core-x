import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — California SOS UCC bulk filing ingest.
 *
 * Trigger.dev v4 durable callback. A single Modal worker invocation produces all
 * five Lance datasets (four normalized Tier-1 + the Tier-2 firmographic index), so
 * this is ONE dispatch, not a fan-out. This task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the flat-JSON callback with terminal metadata.
 *
 * Manual/on-demand by design (no cron): the CA SOS Data Request is a point-in-time
 * bulk export. Re-trigger this task (optionally with { zip_key, as_of }) when a new
 * Data Request is landed to s3://data-sink/landing/ca_ucc/.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  feed: string;
  source_zip: string;
  as_of: string;
  data_through: string | null;
  rows_total: number;
  datasets: Record<string, number>;
}

export const caUccIngest = task({
  id: "ca-ucc-ingest",
  // One Modal invocation builds all five datasets; the durable wait consumes no compute.
  maxDuration: 7200,
  run: async (payload: { zip_key?: string; as_of?: string }) => {
    const asOf = payload?.as_of ?? "2026-05-31";
    const zipKey = payload?.zip_key ?? "";
    logger.info("CA UCC ingest starting", { as_of: asOf, zip_key: zipKey || "(discover)" });

    // 1) Durable callback token.
    const token = await wait.createToken({ timeout: "2h", tags: ["ca-ucc"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "ca-ucc-filings",
        function_name: "ingest_ca_ucc",
        kwargs: { zip_key: zipKey, as_of: asOf },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 300)}`);
    }

    // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
    const out = await wait.forToken<IngestCallback>(token.id);
    if (!out.ok) throw new Error("timed out before Modal callback for ca_ucc");
    if (out.output.status !== "success") {
      throw new Error(`Modal failed for ca_ucc: ${JSON.stringify(out.output)}`);
    }

    logger.info("CA UCC ingest complete", {
      rows_total: out.output.rows_total,
      datasets: out.output.datasets,
      data_through: out.output.data_through,
    });
    return out.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
