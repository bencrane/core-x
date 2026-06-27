import { schedules, task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SEC Form ADV Part 1 + ADV-W (Withdrawals) monthly bulk ingest.
 *
 * Trigger.dev v4 durable callback. Two firm-level Lance datasets, both keyed on the firm's
 * CRD number, refreshed monthly from the SEC Form ADV Data FOIA page:
 *   part1 → IA_ADV_Base_A + ERA_ADV_Base  → sec_adv_part1 (one row per CRD, current state)
 *   advw  → ADVW_<dates>.csv               → sec_adv_w     (one row per ADV-W filing)
 *
 * The worker self-acquires: it scrapes the FOIA page (with the compliant SEC User-Agent),
 * resolves each dataset's ZIP URL dynamically, downloads from sec.gov, transcodes cp1252→utf8,
 * and writes Lance to R2. This task is the cadence + durable-state layer only.
 *
 * Single-phase fan-out: one CHILD run per dataset (part1 + advw) via batchTriggerAndWait — a
 * SINGLE batch waitpoint in the parent. They write to distinct Lance datasets, so there is no
 * shared-writer manifest conflict. Each child (secAdvDataset) mints a waitpoint token (its `url`
 * is a pre-signed HTTP callback — no API key; the callbackHash in the URL authenticates), POSTs
 * the Universal Dispatcher (the ONLY Modal endpoint) with the target worker + that callback url,
 * then suspends on exactly one `wait.forToken` — checkpointed, zero compute, immune to HTTP
 * timeouts — and resumes when the Modal worker POSTs its flat-JSON terminal callback. A
 * Promise.all over wait.forToken in one run would trip TASK_DID_CONCURRENT_WAIT ("Parallel waits
 * are not supported"); batchTriggerAndWait is the Trigger-sanctioned parallel primitive. Trigger
 * owns true end-to-end success/failure state; no polling.
 *
 * Monthly cadence (09:00 ET on the 1st): the SEC posts the Form ADV historical data files
 * periodically; a monthly full-snapshot overwrite keeps both datasets current.
 */

const APP_NAME = "sec-adv-pipelines";
const DATASETS = ["part1", "advw"] as const;
const DATASET_CONCURRENCY = 2; // both datasets fan out at once; suspended waits consume no compute

// The flat JSON body Modal POSTs to each waitpoint url becomes that dispatch's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset: string;
  dataset_uri?: string;
  as_of?: string;
  source_url?: string;
}

// CHILD — ingest one dataset. Exactly one wait.forToken per run, so the datasets fan
// out as independent runs with no concurrent waitpoint in any single run.
export const secAdvDataset = task({
  id: "sec-adv-dataset",
  queue: { concurrencyLimit: DATASET_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: { dataset: string }): Promise<IngestCallback> =>
    dispatch<IngestCallback>("ingest_dataset", { dataset: payload.dataset }),
});

export const secAdvMonthly = schedules.task({
  id: "sec-adv-monthly",
  // 09:00 America/New_York on the 1st of every month.
  cron: { pattern: "0 9 1 * *", timezone: "America/New_York" },
  // The parent suspends at a single batch waitpoint (zero compute) while the dataset
  // child runs fan out in Modal.
  maxDuration: 5400,
  run: async (payload) => {
    logger.info("SEC ADV monthly ingest starting", { scheduledAt: payload.timestamp, datasets: DATASETS });

    // Fan out one child run per dataset under a SINGLE batch waitpoint (a Promise.all
    // over wait.forToken would trip TASK_DID_CONCURRENT_WAIT).
    const { runs } = await secAdvDataset.batchTriggerAndWait(
      DATASETS.map((dataset) => ({ payload: { dataset } })),
    );

    // Collect outcomes WITHOUT throwing yet — the firm-profile MV is a pure function of part1
    // alone, so an advw-only failure must not suppress the rebuild. Surface failures after the MV.
    const ok: IngestCallback[] = [];
    const failed: string[] = [];
    for (const run of runs) {
      if (run.ok) ok.push(run.output);
      else failed.push(`run ${run.id}: ${JSON.stringify(run.error)}`);
    }
    const part1Ok = ok.find((r) => r.dataset === "part1");

    const totalRows = ok.reduce((acc, r) => acc + (r.rows ?? 0), 0);
    const byDataset = Object.fromEntries(
      ok.map((r) => [r.dataset, { rows: r.rows, dataset_uri: r.dataset_uri, as_of: r.as_of }]),
    );
    logger.info("SEC ADV monthly ingest complete", { total_rows: totalRows, byDataset, failed });

    // Chain the derived firm-profile serving MV — fires on part1 success ONLY. FAILURE-ISOLATED:
    // an MV-build failure never fails the monthly run; the part1/advw SoR writes are already
    // committed and untouched. Record degraded + continue, never roll back.
    let firmProfile: { status: string; rows?: number } = { status: "skipped" };
    if (part1Ok) {
      try {
        firmProfile = await dispatch<{ status: "success" | "error"; rows?: number }>(
          "build_remote", {}, "sec-adv-firm-profile",
        );
        logger.info("sec_adv_firm_profile MV rebuilt", { firmProfile });
      } catch (err) {
        logger.error("sec_adv_firm_profile MV build failed (non-fatal; SoR unaffected)", {
          error: String(err),
        });
        firmProfile = { status: "error" };
      }
    }

    // Surface any child failure AFTER the MV has had its chance off the good part1 snapshot, so the
    // monthly run still signals failure for alerting without suppressing the (valid) MV rebuild.
    if (failed.length) {
      throw new Error(`sec-adv dataset run(s) failed: ${failed.join("; ")}`);
    }

    return { total_rows: totalRows, datasets: byDataset, firm_profile: firmProfile };
  },
});

/**
 * Mint a durable waitpoint, fire the Universal Dispatcher (202), and suspend until the Modal
 * worker POSTs its flat-JSON terminal callback. Returns that callback body.
 */
async function dispatch<T extends { status: "success" | "error" }>(
  functionName: string,
  kwargs: Record<string, unknown>,
  appName: string = APP_NAME,
): Promise<T> {
  const tag = String(kwargs.dataset ?? functionName);
  const token = await wait.createToken({ timeout: "1h", tags: ["sec-adv", tag] });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: appName,
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
