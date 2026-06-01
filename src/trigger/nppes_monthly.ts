import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — CMS NPPES monthly full-replacement snapshot ingest.
 *
 * Trigger.dev v4 durable callback. CMS publishes the NPPES registry as a single monthly
 * FULL REPLACEMENT (no deltas, no history). This task fires on the 15th of every month —
 * late enough that the new month's full-replacement ZIP is reliably published — and
 * dispatches the Modal worker to:
 *   scrape https://download.cms.gov/nppes/NPI_Files.html for the current monthly ZIP
 *   (weekly + deactivated files strictly excluded) → download → extract the core
 *   npidata_pfile CSV → DuckDB transform (100%) → Lance, BTREE/BITMAP-indexed, published
 *   to its own month partition s3://data-sink/active/nppes/snapshot=YYYY-MM/.
 *
 * Because CMS gives no history, each month is a DISTINCT immutable Lance dataset — the
 * partition ledger we build ourselves. `snapshot_month` is the execution month (this
 * task's `payload.timestamp`); on the 15th that equals the published file's month.
 *
 * One dispatch, one durable waitpoint: mint a token (its `url` is a pre-signed HTTP
 * callback — the embedded callbackHash is the auth, no API key), POST the Universal
 * Dispatcher (the ONLY Modal endpoint) with the worker + that callback url, then suspend
 * on `wait.forToken` — checkpointed, zero compute, immune to HTTP timeouts — and resume
 * when the Modal worker POSTs its flat-JSON terminal callback. `modal.Cron` is forbidden;
 * cadence lives here.
 */

// The flat JSON body the Modal worker POSTs to the waitpoint url becomes the run output.
interface IngestCallback {
  status: "success" | "error";
  feed: string;
  snapshot_month: string;
  rows: number;
  rejected_rows: number;
  dataset_uri?: string;
  source_file?: string;
}

export const nppesMonthly = schedules.task({
  id: "nppes-monthly",
  // 12:00 UTC on the 15th — CMS has published the new monthly full replacement by then.
  cron: { pattern: "0 12 15 * *", timezone: "UTC" },
  // Download (~1 GB) + ~10 GB CSV transform + index + ~6 GB publish; the durable wait
  // consumes no compute while suspended.
  maxDuration: 14400,
  run: async (payload) => {
    const snapshotMonth = payload.timestamp.toISOString().slice(0, 7); // YYYY-MM
    logger.info("NPPES monthly ingest starting", { snapshot_month: snapshotMonth });

    const token = await wait.createToken({ timeout: "4h", tags: ["nppes", snapshotMonth] });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "nppes-pipelines",
        function_name: "ingest_nppes",
        kwargs: { snapshot_month: snapshotMonth },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status} for nppes(${snapshotMonth}): ${body.slice(0, 300)}`);
    }

    const out = await wait.forToken<IngestCallback>(token.id);
    if (!out.ok) throw new Error(`timed out before Modal callback for nppes(${snapshotMonth})`);
    if (out.output.status !== "success") {
      throw new Error(`Modal failed for nppes(${snapshotMonth}): ${JSON.stringify(out.output)}`);
    }

    logger.info("NPPES monthly ingest complete", {
      snapshot_month: snapshotMonth,
      rows: out.output.rows,
      rejected_rows: out.output.rejected_rows,
      dataset_uri: out.output.dataset_uri,
      source_file: out.output.source_file,
    });
    return {
      snapshot_month: snapshotMonth,
      rows: out.output.rows,
      rejected_rows: out.output.rejected_rows,
      dataset_uri: out.output.dataset_uri,
      source_file: out.output.source_file,
    };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
