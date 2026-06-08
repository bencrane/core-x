import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — NMLS (Nationwide Multistate Licensing System) public Business Reports ingest.
 *
 * Trigger.dev v4 durable callback, two phases against the `nmls-pipelines` Modal app:
 *   1. acquire_rosters — Playwright enumerates the public NMLS Business Reports page and lands
 *      the MCR/Licensing zip + the latest Mortgage Industry Report xlsx (RAW) into
 *      s3://data-sink/landing/nmls/<as_of>/. Its callback returns the logical `targets` list.
 *   2. ingest_target  — fanned out per target as independent CHILD runs (nmlsTarget) via
 *      batchTriggerAndWait — a SINGLE batch waitpoint, sequential to Phase 1's wait
 *      (distinct Lance datasets → no shared-writer conflict): DuckDB read_csv/read_xlsx →
 *      Lance on R2 + scalar indexes. A Promise.all over wait.forToken in one run trips
 *      TASK_DID_CONCURRENT_WAIT ("Parallel waits are not supported").
 *
 * Each dispatch mints a waitpoint token (its `url` is a pre-signed HTTP callback), POSTs the
 * Universal Dispatcher (the ONLY Modal endpoint) with the target worker + that callback url, then
 * suspends on `wait.forToken` — checkpointed, zero compute, immune to HTTP timeouts — and resumes
 * when the Modal worker POSTs its flat-JSON terminal callback.
 *
 * PUBLIC-ONLY surface: every dataset is a STATE × PERIOD AGGREGATE (no nmls_id/LEI in public data).
 * Manual/on-demand by design (NMLS refresh is irregular — quarterly). `as_of` is the harvest date.
 */

const FALLBACK_TARGETS = [
  "nmls_mcr_license_activity",
  "nmls_mcr_forward_by_purpose",
  "nmls_mcr_forward_by_type",
  "nmls_mcr_forward_by_business_line",
  "nmls_mcr_reverse_by_business_line",
  "nmls_mcr_applications_received",
  "nmls_state_entity_counts",
] as const;
const AS_OF_DEFAULT = "2026-06-01";
const TARGET_CONCURRENCY = 8; // covers the 7 fallback targets fanning out at once; suspended waits consume no compute

interface AcquireCallback {
  status: "success" | "error";
  phase: "acquire";
  as_of: string;
  files_landed: number;
  landed: string[];
  targets: string[];
}

interface IngestCallback {
  status: "success" | "error";
  phase: "ingest";
  target: string;
  rows: number;
  rejected_rows: number;
  dataset_uri?: string;
  as_of?: string;
}

// CHILD — ingest one target. Exactly one wait.forToken per run, so the targets fan out as
// independent runs with no concurrent waitpoint in any single run.
export const nmlsTarget = task({
  id: "nmls-target",
  queue: { concurrencyLimit: TARGET_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: { target: string; asOf: string }): Promise<IngestCallback> =>
    dispatch<IngestCallback>("ingest_target", { target: payload.target, as_of: payload.asOf }),
});

export const nmlsRostersIngest = task({
  id: "nmls-rosters-ingest",
  // Phase 1 acquire (single waitpoint) then a batch waitpoint over the target child runs;
  // both suspend with zero compute. maxDuration covers Phase 1's up-to-1h acquire wait.
  maxDuration: 5400,
  run: async (payload: { as_of?: string }) => {
    const asOf = payload?.as_of ?? AS_OF_DEFAULT;
    logger.info("NMLS ingest starting", { as_of: asOf });

    // Phase 1 — acquire (single dispatch). Blocks ingest if the scrape failed.
    const acq = await dispatch<AcquireCallback>("acquire_rosters", { as_of: asOf });
    logger.info("NMLS acquire complete", { ...acq });

    const targets = acq.targets?.length ? acq.targets : [...FALLBACK_TARGETS];

    // Phase 2 — fan out one child run per target under a SINGLE batch waitpoint (sequential
    // to Phase 1's acquire wait; a Promise.all over wait.forToken would trip
    // TASK_DID_CONCURRENT_WAIT). Distinct Lance datasets — no conflict.
    const { runs } = await nmlsTarget.batchTriggerAndWait(
      targets.map((target) => ({ payload: { target, asOf } })),
    );

    const results: IngestCallback[] = [];
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(`nmls target run ${run.id} failed: ${JSON.stringify(run.error)}`);
      }
      results.push(run.output);
    }

    const totalRows = results.reduce((acc, r) => acc + (r.rows ?? 0), 0);
    const byTarget = Object.fromEntries(
      results.map((r) => [r.target, { rows: r.rows, rejected_rows: r.rejected_rows, dataset_uri: r.dataset_uri }]),
    );
    logger.info("NMLS ingest complete", { as_of: asOf, files_landed: acq.files_landed, total_rows: totalRows, byTarget });
    return { as_of: asOf, files_landed: acq.files_landed, total_rows: totalRows, targets: byTarget };
  },
});

/**
 * Mint a durable waitpoint, fire the Universal Dispatcher (202), and suspend until the Modal
 * worker POSTs its flat-JSON terminal callback. Returns that callback body.
 */
async function dispatch<T extends { status: "success" | "error" }>(
  functionName: string,
  kwargs: Record<string, unknown>,
): Promise<T> {
  const tag = String(kwargs.target ?? functionName);
  const token = await wait.createToken({ timeout: "1h", tags: ["nmls", tag] });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "nmls-pipelines",
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
