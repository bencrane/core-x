import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — NY Department of State active corporations bulk ingest.
 *
 * Trigger.dev v4 durable callback. A single overwrite-snapshot ingest (no
 * fan-out): mint a waitpoint token (its `url` is a pre-signed HTTP callback),
 * POST the Universal Dispatcher (the ONLY Modal endpoint) targeting
 * `ny-sos-corporations` / `ingest_ny_sos` with that callback url, suspend on
 * `wait.forToken` — checkpointed, consuming no compute and immune to HTTP
 * timeouts — and resume when the Modal worker POSTs its flat terminal metadata.
 *
 * Manual/on-demand by design (no cron): refresh is a manual R2 drop. Trigger
 * with an empty payload to ingest the default landed key, or { key } to override.
 */

const DEFAULT_LANDING_KEY =
  "landing/ny_sos/NY_Active_Corporations___Beginning_1800.csv";

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  snapshot_date?: string;
}

export const nySosCorporations = task({
  id: "ny-sos-corporations-ingest",
  // One overwrite + nine scalar-index builds over 4.2M rows; the durable wait
  // consumes no compute while suspended.
  maxDuration: 7200,
  run: async (payload: { key?: string }) => {
    const key = payload?.key ?? DEFAULT_LANDING_KEY;
    logger.info("NY SoS ingest starting", { key });

    // 1) Durable callback token (generous — overwrite + multi-million-row index sorts).
    const token = await wait.createToken({ timeout: "2h", tags: ["ny-sos-corporations"] });

    // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "ny-sos-corporations",
        function_name: "ingest_ny_sos",
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

    logger.info("NY SoS ingest complete", { rows: out.output.rows });
    return {
      key,
      rows: out.output.rows,
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
