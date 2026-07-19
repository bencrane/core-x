import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — FMCSA monthly feeds (SMS family).
 *
 * The SMS family (Safety Measurement System: violations, inspections, percentiles)
 * publishes MONTHLY upstream, so it gets its own cron rather than riding the daily
 * task. Structure is identical to fmcsa-daily: dispatch all feeds, run in parallel
 * on Modal, collect durable waitpoints.
 *
 * PHASE 2 — not yet activated. The SMS feeds are `file`/blob assets with headerless
 * positional layouts that still need a byte-peek + per-feed projection before they
 * are added to the worker registry. Until MONTHLY_FEEDS is populated this task is a
 * safe no-op (it never dispatches unvalidated feeds). Wiring it now satisfies the
 * 2-task cadence model (D-1) without violating Phase-1-only scope (D-5).
 */

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  snapshot_date?: string;
}

// Phase-2 activation point. Add SMS feeds here once their projections are validated
// and registered in pipelines/fmcsa/fmcsa_bulk.py (FEEDS). `fn` is size-routed
// (>5M rows → ingest_fmcsa_feed_xl; SMS Input-Violation/Inspection are >5M).
const MONTHLY_FEEDS: Array<{ feed: string; fn: string }> = [
  // { feed: "sms_input_violation", fn: "ingest_fmcsa_feed_xl" },
  // { feed: "sms_input_inspection", fn: "ingest_fmcsa_feed_xl" },
  // { feed: "sms_input_census",     fn: "ingest_fmcsa_feed" },
  // ...
];

export const fmcsaMonthly = schedules.task({
  id: "fmcsa-monthly",
  // 16:00 UTC on the 6th — after FMCSA's monthly SMS publish settles.
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate.
  // cron: { pattern: "0 16 6 * *", timezone: "UTC" },
  maxDuration: 10800,
  run: async (payload) => {
    const snapshotDate = payload.timestamp.toISOString().slice(0, 10);
    if (MONTHLY_FEEDS.length === 0) {
      logger.info("FMCSA monthly: no feeds activated (Phase 2 pending) — no-op", { snapshotDate });
      return { snapshotDate, feeds: 0, rows: 0, results: [] as Array<{ feed: string; rows: number }> };
    }
    logger.info("FMCSA monthly starting", { feeds: MONTHLY_FEEDS.length, snapshotDate });

    const inflight: Array<{ feed: string; tokenId: string }> = [];
    for (const { feed, fn } of MONTHLY_FEEDS) {
      const token = await wait.createToken({ timeout: "3h", tags: ["fmcsa-monthly", feed] });
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

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("FMCSA monthly complete", { ok: results.length, failed: failures.length, rows });
    if (failures.length) {
      throw new Error(`FMCSA monthly: ${failures.length}/${MONTHLY_FEEDS.length} feeds failed → ${failures.join(" | ")}`);
    }
    return { snapshotDate, feeds: results.length, rows, results };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
