import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — HMDA Nationwide Loan-Level (LAR) + Reporter Panel historical sweep.
 *
 * Trigger.dev v4 durable callback. Drives the `hmda-pipelines` Modal worker through the
 * Universal Dispatcher (the ONLY Modal endpoint). Each step mints a waitpoint token (its
 * `url` is a pre-signed HTTP callback — no API key), POSTs the dispatcher with the target
 * worker + that callback url, suspends on `wait.forToken` (checkpointed, zero compute,
 * immune to HTTP timeouts), and resumes from the worker's flat-JSON terminal callback.
 *
 * The sweep is SEQUENTIAL per dataset by design: every year delete-then-appends into ONE
 * unified Lance dataset (hmda_lar / hmda_panels), so concurrent writers would collide on the
 * manifest. LAR 2016–2025 land in order, then panels 2016–2025, then the BTREE indexes are
 * built once. Durable waits consume no compute, so a multi-hour sweep is free while suspended.
 *
 * 2018–2024 LAR = snapshot CSV; 2025 LAR = combined MLAR (interim); 2016–2017 LAR = CFPB
 * historic codes. 2024/2025 panels fall back to the 2024 Transmittal Sheet.
 *
 * Manual/on-demand (no cron) — a backfill is pulled by request. `init_state` (ops.hmda_runs)
 * must be applied once first: `modal run pipelines/hmda/hmda_bulk.py::init_state`.
 *
 *   await tasks.trigger("hmda-backfill", {});                       // full sweep
 *   await tasks.trigger("hmda-backfill", { larYears: [2025] });    // targeted re-run
 */

const ALL_YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025];
const APP = "hmda-pipelines";

interface YearCallback {
  status: "success" | "error";
  dataset: "lar" | "panels";
  year: number;
  rows: number;
  rejected_rows?: number;
}

interface ReindexCallback {
  status: "success" | "error";
  dataset: string;
  indexes: string[];
}

interface StageCallback {
  status: "success" | "error";
  phase: "stage";
  staged: number;
  remaining: string[];
}

export const hmdaBackfill = task({
  id: "hmda-backfill",
  // Sequential 20-year-file sweep (10 LAR + 10 panel) + 2 index builds; durable waits are free.
  maxDuration: 21600,
  run: async (payload: {
    larYears?: number[];
    panelYears?: number[];
    reindex?: boolean;
    skipStage?: boolean;
  }) => {
    const larYears = payload?.larYears ?? ALL_YEARS;
    const panelYears = payload?.panelYears ?? ALL_YEARS;
    const doReindex = payload?.reindex ?? true;
    const skipStage = payload?.skipStage ?? false;
    logger.info("HMDA backfill starting", { larYears, panelYears, doReindex, skipStage });

    // ── Phase 1 — stage every raw source zip → R2 landing. stage_all fans stage_one out in
    //    parallel (containers spread across hosts → ~60% reach the WAF'd origin) and re-fans
    //    stragglers; ingest then reads R2, which Modal can always reach. ──
    if (!skipStage) {
      const s = await dispatch<StageCallback>("stage_all", {}, "stage");
      logger.info("staging complete", { staged: s.staged, remaining: s.remaining });
    }

    const lar: Record<number, number> = {};
    for (const year of larYears) {
      const r = await dispatch<YearCallback>("ingest_lar_year", { year }, `lar-${year}`);
      lar[year] = r.rows ?? 0;
      logger.info(`LAR ${year} landed`, { rows: lar[year], rejected: r.rejected_rows });
    }

    const panels: Record<number, number> = {};
    for (const year of panelYears) {
      const r = await dispatch<YearCallback>("ingest_panel_year", { year }, `panel-${year}`);
      panels[year] = r.rows ?? 0;
      logger.info(`Panel ${year} landed`, { rows: panels[year] });
    }

    const indexes: Record<string, string[]> = {};
    if (doReindex) {
      for (const dataset of ["lar", "panels"]) {
        const r = await dispatch<ReindexCallback>("reindex_dataset", { dataset }, `reindex-${dataset}`);
        indexes[dataset] = r.indexes ?? [];
      }
    }

    const larTotal = Object.values(lar).reduce((a, b) => a + b, 0);
    const panelTotal = Object.values(panels).reduce((a, b) => a + b, 0);
    logger.info("HMDA backfill complete", { larTotal, panelTotal, lar, panels, indexes });
    return { larTotal, panelTotal, lar, panels, indexes };
  },
});

/**
 * Mint a durable waitpoint, fire the Universal Dispatcher (202), suspend until the Modal
 * worker POSTs its flat-JSON terminal callback. Returns that callback body.
 */
async function dispatch<T extends { status: "success" | "error" }>(
  functionName: string,
  kwargs: Record<string, unknown>,
  tag: string,
): Promise<T> {
  // Single durable dispatch. The WAF/egress-IP block is handled entirely inside Phase-1
  // staging (stage_all's parallel fan-out + re-fan); ingest/reindex read R2, which Modal
  // always reaches, so no container rotation is needed here.
  const token = await wait.createToken({ timeout: "2h", tags: ["hmda", tag] });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: APP,
      function_name: functionName,
      kwargs,
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for ${functionName}(${tag}): ${body.slice(0, 300)}`);
  }

  const out = await wait.forToken<T>(token.id);
  if (!out.ok) throw new Error(`timed out before Modal callback for ${functionName}(${tag})`);
  if (out.output.status !== "success") {
    throw new Error(`Modal failed for ${functionName}(${tag}): ${JSON.stringify(out.output)}`);
  }
  return out.output;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
