import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — FMCSA daily feeds (Phase 1).
 *
 * Replaces the legacy every-15-minutes heartbeat (cron "0,15,30,45 * * * *",
 * 96 dispatch-probes/day against an `ops.fmcsa_feed_schedule_config` SPOF) with a
 * single Trigger.dev v4 daily cron. The
 * FMCSA "daily difference" feeds are full snapshots refreshed overnight; we pull
 * once per day, well after the upstream publish window.
 *
 * Cadence belongs to Trigger v4 (no `modal.Cron`). Each feed writes its OWN Lance
 * dataset, so there is no single-dataset commit-conflict constraint — every feed
 * is dispatched up front and runs in PARALLEL on Modal, then we collect the
 * durable waitpoint callbacks. A single feed failing fails the run loudly without
 * silently dropping the rest from the report.
 */

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  snapshot_date?: string;
}

// Phase-1 daily feeds. `fn` is the Modal function (size-routed per D-6: >5M rows
// → the 32 GiB streaming `ingest_fmcsa_feed_xl`). All Phase-1 feeds are ≤5M.
const DAILY_FEEDS: Array<{ feed: string; fn: string }> = [
  { feed: "carrier", fn: "ingest_fmcsa_feed" },
  { feed: "census", fn: "ingest_fmcsa_feed_xl" }, // heavy: 4.4M rows × 147 cols → 32 GiB streaming
  { feed: "auth_hist", fn: "ingest_fmcsa_feed" },
  { feed: "revocation", fn: "ingest_fmcsa_feed" },
  { feed: "insurance", fn: "ingest_fmcsa_feed" },
  { feed: "boc3", fn: "ingest_fmcsa_feed" },
  { feed: "oos", fn: "ingest_fmcsa_feed" },
];

export const fmcsaDaily = schedules.task({
  id: "fmcsa-daily",
  // 15:00 UTC ≈ 11:00 ET — after FMCSA's overnight snapshot publish.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 15 * * *", timezone: "UTC" },
  maxDuration: 7200,
  run: async (payload) => {
    const snapshotDate = payload.timestamp.toISOString().slice(0, 10);
    logger.info("FMCSA daily starting", { feeds: DAILY_FEEDS.length, snapshotDate });

    // 1) Dispatch every feed up front so Modal runs them in parallel.
    const inflight: Array<{ feed: string; tokenId: string }> = [];
    for (const { feed, fn } of DAILY_FEEDS) {
      const token = await wait.createToken({ timeout: "2h", tags: ["fmcsa-daily", feed] });
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "fmcsa-pipelines",
          function_name: fn,
          kwargs: { feed, snapshot_date: snapshotDate },
          trigger_callback_url: token.url,
        }),
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`dispatcher ${res.status} for ${feed}: ${body.slice(0, 300)}`);
      }
      inflight.push({ feed, tokenId: token.id });
    }

    // 2) Collect the durable callbacks. Suspended → zero compute, HTTP-timeout-immune.
    const results: Array<{ feed: string; rows: number }> = [];
    const failures: string[] = [];
    for (const { feed, tokenId } of inflight) {
      const out = await wait.forToken<IngestCallback>(tokenId);
      if (!out.ok) {
        failures.push(`${feed}: timed out before Modal callback`);
        continue;
      }
      if (out.output.status !== "success") {
        failures.push(`${feed}: ${JSON.stringify(out.output)}`);
        continue;
      }
      logger.info("feed ingested", { feed, rows: out.output.rows });
      results.push({ feed, rows: out.output.rows });
    }

    // 3) Chain the direct-mail serving projection off census. It is a pure
    //    read-only projection of the census SoR (app `fmcsa-derived`), so it runs
    //    only after census ingested cleanly and never races the raw write.
    if (results.some((r) => r.feed === "census")) {
      const dtoken = await wait.createToken({ timeout: "1h", tags: ["fmcsa-daily", "census_mail_ready"] });
      const dres = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "fmcsa-derived",
          function_name: "build_mail_ready",
          kwargs: {},
          trigger_callback_url: dtoken.url,
        }),
      });
      if (!dres.ok) {
        failures.push(`census_mail_ready: dispatcher ${dres.status}: ${(await dres.text()).slice(0, 200)}`);
      } else {
        const dout = await wait.forToken<IngestCallback>(dtoken.id);
        if (!dout.ok) failures.push("census_mail_ready: timed out before Modal callback");
        else if (dout.output.status !== "success") failures.push(`census_mail_ready: ${JSON.stringify(dout.output)}`);
        else logger.info("derived built", { feed: "census_mail_ready", rows: dout.output.rows });
      }
    }

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("FMCSA daily complete", { ok: results.length, failed: failures.length, rows });
    if (failures.length) {
      throw new Error(`FMCSA daily: ${failures.length}/${DAILY_FEEDS.length} feeds failed → ${failures.join(" | ")}`);
    }
    return { snapshotDate, feeds: results.length, rows, results };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
