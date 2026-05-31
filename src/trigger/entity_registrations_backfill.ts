import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SAM.gov Entity Registrations historical backfill.
 *
 * A BOUNDED backfill, not a schedule — there is no cron. Trigger it manually with
 * the deduped landing keys. It sequences them one-by-one through the Universal
 * Dispatcher (sequential on purpose: concurrent writers to one Lance dataset can
 * hit commit conflicts), suspending on a durable waitpoint token per file so the
 * run consumes no compute while Modal works and is immune to HTTP timeouts.
 *
 * Get the deduped key list from the Modal planner:
 *   modal run pipelines/sam_gov/entity_registrations_bulk.py --dry-run
 * then trigger this task with { keys: [...] } (or just run the Modal `backfill`
 * entrypoint directly to skip Trigger entirely).
 */

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  source_file?: string;
}

export const entityRegistrationsBackfill = task({
  id: "entity-registrations-backfill",
  // Sequential over ~26 files; the durable waits consume no compute while suspended.
  maxDuration: 10800,
  run: async (payload: { keys: string[] }) => {
    const keys = dedupKeys(payload?.keys ?? []);
    if (keys.length === 0) {
      throw new Error("No keys. Trigger with { keys: string[] } of data-sink/landing/*.zip objects.");
    }

    logger.info("Entity-registration backfill starting", { files: keys.length });
    const results: Array<{ source_file: string; rows: number }> = [];

    for (const key of keys) {
      // 1) Durable callback token.
      const token = await wait.createToken({ timeout: "1h", tags: ["entity-reg-backfill"] });

      // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "sam-gov-entity-pipelines",
          function_name: "ingest_entity_registration_extract",
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
    logger.info("Entity-registration backfill complete", { files: results.length, rows });
    return { files: results.length, rows, results };
  },
});

/** Drop cp1252 V2 encoding-twins when a native UTF-8 sibling for the same date exists. */
function dedupKeys(keys: string[]): string[] {
  const utf8Dates = new Set<string>();
  for (const k of keys) {
    const m = k.toUpperCase().match(/UTF-8_MONTHLY_V2_(\d{8})/);
    if (m) utf8Dates.add(m[1]);
  }
  return keys
    .filter((k) => {
      const name = k.toUpperCase();
      if (name.includes("_V2_") && !name.includes("UTF-8")) {
        const d = name.match(/_V2_(\d{8})/);
        if (d && utf8Dates.has(d[1])) return false;
      }
      return true;
    })
    .sort();
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
