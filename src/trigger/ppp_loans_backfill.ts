import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SBA PPP (FOIA) bulk ingest, Phase 2 (Lance ingestion).
 *
 * A BOUNDED backfill of a single point-in-time FOIA release (snapshot 2024-09-30)
 * — there is no cron. Phase 1 (landing the raw CSVs to R2) is run out of band via
 * the Modal `fetch` entrypoint; this task sequences the 13 landed keys one-by-one
 * through the Universal Dispatcher into `ingest_ppp_extract`. Sequential on
 * purpose: concurrent writers to one Lance dataset can hit commit conflicts. Each
 * file suspends on a durable waitpoint token, so the run consumes no compute while
 * Modal works and is immune to HTTP timeouts.
 *
 * Trigger with an empty payload to ingest all 13 default landing keys, or pass
 * { keys: [...] } to ingest a subset.
 */

const DEFAULT_LANDING_KEYS: string[] = [
  "landing/ppp/public_150k_plus_240930.csv",
  "landing/ppp/public_up_to_150k_1_240930.csv",
  "landing/ppp/public_up_to_150k_2_240930.csv",
  "landing/ppp/public_up_to_150k_3_240930.csv",
  "landing/ppp/public_up_to_150k_4_240930.csv",
  "landing/ppp/public_up_to_150k_5_240930.csv",
  "landing/ppp/public_up_to_150k_6_240930.csv",
  "landing/ppp/public_up_to_150k_7_240930.csv",
  "landing/ppp/public_up_to_150k_8_240930.csv",
  "landing/ppp/public_up_to_150k_9_240930.csv",
  "landing/ppp/public_up_to_150k_10_240930.csv",
  "landing/ppp/public_up_to_150k_11_240930.csv",
  "landing/ppp/public_up_to_150k_12_240930.csv",
];

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  source_file?: string;
}

export const pppLoansBackfill = task({
  id: "ppp-loans-backfill",
  // Sequential over 13 files; the durable waits consume no compute while suspended.
  maxDuration: 14400,
  run: async (payload: { keys?: string[] }) => {
    const keys = payload?.keys?.length ? payload.keys : DEFAULT_LANDING_KEYS;

    logger.info("PPP backfill (Phase 2) starting", { files: keys.length });
    const results: Array<{ source_file: string; rows: number }> = [];

    for (const key of keys) {
      // 1) Durable callback token (generous — a ~16 GiB ingest of ~1M rows).
      const token = await wait.createToken({ timeout: "1h", tags: ["ppp-loans-backfill"] });

      // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "sba-ppp-pipelines",
          function_name: "ingest_ppp_extract",
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

      logger.info("file ingested", { key, rows: out.output.rows });
      results.push({ source_file: key, rows: out.output.rows });
    }

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("PPP backfill complete", { files: results.length, rows });
    return { files: results.length, rows, results };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
