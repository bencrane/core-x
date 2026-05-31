import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — People Data Labs (PDL) Free Company Dataset bulk ingest.
 *
 * Trigger.dev v4 durable callback. A single overwrite-snapshot ingest of ~35.4M
 * companies (no fan-out): mint a waitpoint token (its `url` is a pre-signed HTTP
 * callback), POST the Universal Dispatcher (the ONLY Modal endpoint) targeting
 * `pdl-companies` / `ingest_pdl_companies` with that callback url, suspend on
 * `wait.forToken` — checkpointed, consuming no compute and immune to HTTP
 * timeouts — and resume when the Modal worker POSTs its flat terminal metadata.
 *
 * Manual/on-demand by design (no cron): refresh is a manual R2 drop. Trigger
 * with an empty payload to ingest the default landed key, or { key } to override.
 */

const DEFAULT_LANDING_KEY =
  "landing/pdl_companies/free_company_dataset.pipe.zip";

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  snapshot_date?: string;
  distinct_ids?: number | null;
  write_path?: string;
}

export const pdlCompanies = task({
  id: "pdl-companies-ingest",
  // One overwrite + ten scalar-index builds over 35.4M rows; the durable wait
  // consumes no compute while suspended.
  maxDuration: 10800,
  run: async (payload: { key?: string }) => {
    const key = payload?.key ?? DEFAULT_LANDING_KEY;
    logger.info("PDL companies ingest starting", { key });

    // 1) Durable callback token (generous — overwrite + 35.4M-row index sorts).
    const token = await wait.createToken({ timeout: "3h", tags: ["pdl-companies"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "pdl-companies",
        function_name: "ingest_pdl_companies",
        kwargs: { key },
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

    logger.info("PDL companies ingest complete", {
      rows: out.output.rows,
      distinct_ids: out.output.distinct_ids,
      write_path: out.output.write_path,
    });
    return {
      key,
      rows: out.output.rows,
      distinct_ids: out.output.distinct_ids,
      dataset_uri: out.output.dataset_uri,
      snapshot_date: out.output.snapshot_date,
    };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
