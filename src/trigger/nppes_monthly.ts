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

// The flat JSON the `nppes-analytical` materialize worker POSTs to its waitpoint url. Unlike
// the raw ingest, `rows` is a per-table map (provider / taxonomy / identifier) and `status`
// carries a third `partial` value (a publish that landed <3 of the derived prefixes — rejected
// by gate G10 on the next verify).
interface MaterializeCallback {
  status: "success" | "partial" | "error";
  feed: string;
  snapshot_month: string;
  rows: Record<string, number>;
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

    // Decoupled downstream refresh (directive §10). The raw capture is COMMITTED and this run's
    // success is already sealed above; rebuilding the derived analytical serving layer for the
    // snapshot is best-effort and BLAST-RADIUS ISOLATED. The `nppes-analytical` materialize
    // worker reads the raw SoR READ-ONLY and writes only its own derived prefixes, so a failure
    // here can never touch the raw month partition. Mirror the token+dispatcher+forToken pattern,
    // but wrap it: any failure — dispatcher reject, waitpoint timeout, or a non-success
    // materialize callback — is logged loudly (the page signal) and SWALLOWED, never rethrown,
    // so it cannot roll back or fail the raw run. The worker's own ops.nppes_analytical_runs
    // ledger row is the authoritative terminal record of the build either way.
    let analytical: { status: "success" | "failed"; rows?: Record<string, number> } = {
      status: "failed",
    };
    try {
      // The worker's Modal compute ceiling is 4h; give the waitpoint headroom over it (queue +
      // cold-start + R2 publish + read-back gate) so a still-running build is never spuriously
      // timed out. A waitpoint timeout here is non-fatal regardless — the ledger row is truth.
      const mToken = await wait.createToken({
        timeout: "5h",
        tags: ["nppes-analytical", snapshotMonth],
      });
      const mRes = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "nppes-analytical",
          function_name: "materialize",
          kwargs: { snapshot_month: snapshotMonth },
          trigger_callback_url: mToken.url,
        }),
      });
      if (!mRes.ok) {
        throw new Error(`dispatcher ${mRes.status}: ${(await mRes.text()).slice(0, 300)}`);
      }

      const mOut = await wait.forToken<MaterializeCallback>(mToken.id);
      if (!mOut.ok) throw new Error("timed out before materialize callback");
      if (mOut.output.status !== "success") {
        throw new Error(`materialize ${mOut.output.status}: ${JSON.stringify(mOut.output)}`);
      }
      analytical = { status: "success", rows: mOut.output.rows };
      logger.info("NPPES analytical layer refreshed", {
        snapshot_month: snapshotMonth,
        rows: mOut.output.rows,
      });
    } catch (err) {
      // Page: the derived layer is stale for this month, but the raw SoR is intact and this run
      // stays green. Operator follow-up = re-run `materialize --snapshot-month <m>` out of band.
      logger.error("NPPES analytical refresh FAILED — raw capture intact, derived layer stale", {
        snapshot_month: snapshotMonth,
        error: err instanceof Error ? err.message : String(err),
      });
    }

    return {
      snapshot_month: snapshotMonth,
      rows: out.output.rows,
      rejected_rows: out.output.rejected_rows,
      dataset_uri: out.output.dataset_uri,
      source_file: out.output.source_file,
      analytical_refresh: analytical,
    };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
