import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — GLEIF Golden Copy daily ingest (Level 1 LEI records + Level 2 relationships).
 *
 * GLEIF republishes a COMPLETE golden-copy snapshot once per day (publish files stamped
 * 00:00 UTC). We pull once daily, well after the publish window, and the Modal worker
 * resolves the latest publish from the discovery API itself — so this task carries no
 * URLs, only the level selector.
 *
 * Cadence belongs to Trigger v4 (no `modal.Cron`). The two levels write DISTINCT Lance
 * datasets, so there is no single-dataset commit-conflict constraint — both are dispatched
 * up front and run in PARALLEL on Modal, then we collect the durable waitpoint callbacks.
 * Each surface goes through the Universal Dispatcher (the ONLY Modal endpoint) + a Trigger
 * v4 durable waitpoint token: mint url → POST dispatcher → suspend on wait.forToken →
 * resume from the worker's flat-JSON callback. Suspended ⇒ zero compute, HTTP-timeout-immune.
 *
 * A single level failing fails the run loudly without silently dropping the other from
 * the report.
 */

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  level: string;
  run_mode?: string;
  dataset_uri?: string;
  publish_date?: string | null;
}

const APP_NAME = "gleif-pipelines";
const FUNCTION_NAME = "ingest_gleif";

// Both levels of the daily golden copy. `level` is the only kwarg — the worker resolves
// the latest publish + download URL from the discovery API.
const LEVELS: Array<{ level: string; feed: string }> = [
  { level: "l1", feed: "gleif_l1_entities" },
  { level: "l2", feed: "gleif_l2_relationships" },
];

export const gleifDaily = schedules.task({
  id: "gleif-daily",
  // 06:00 UTC — comfortably after GLEIF's 00:00 UTC daily golden-copy publish.
  // PARKED (operator ruling 2026-07-19: no scheduled cadence for now; restore to reinstate).
  // cron: { pattern: "0 6 * * *", timezone: "UTC" },
  maxDuration: 7200,
  run: async (payload) => {
    logger.info("GLEIF daily starting", {
      levels: LEVELS.length,
      scheduledAt: payload.timestamp,
    });

    // 1) Dispatch both levels up front so Modal runs them in parallel.
    const inflight: Array<{ level: string; feed: string; tokenId: string }> = [];
    for (const { level, feed } of LEVELS) {
      const token = await wait.createToken({ timeout: "2h", tags: ["gleif-daily", level] });
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: APP_NAME,
          function_name: FUNCTION_NAME,
          kwargs: { level },
          trigger_callback_url: token.url,
        }),
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`dispatcher ${res.status} for ${level}: ${body.slice(0, 300)}`);
      }
      inflight.push({ level, feed, tokenId: token.id });
    }

    // 2) Collect the durable callbacks. Suspended → zero compute, HTTP-timeout-immune.
    const results: Array<{ level: string; feed: string; rows: number; publishDate?: string | null }> = [];
    const failures: string[] = [];
    for (const { level, feed, tokenId } of inflight) {
      const out = await wait.forToken<IngestCallback>(tokenId);
      if (!out.ok) {
        failures.push(`${level}: timed out before Modal callback`);
        continue;
      }
      if (out.output.status !== "success") {
        failures.push(`${level}: ${JSON.stringify(out.output)}`);
        continue;
      }
      logger.info("level ingested", { level, feed, rows: out.output.rows });
      results.push({ level, feed, rows: out.output.rows, publishDate: out.output.publish_date });
    }

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("GLEIF daily complete", { ok: results.length, failed: failures.length, rows });
    if (failures.length) {
      throw new Error(`GLEIF daily: ${failures.length}/${LEVELS.length} levels failed → ${failures.join(" | ")}`);
    }
    return { levels: results.length, rows, results };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
