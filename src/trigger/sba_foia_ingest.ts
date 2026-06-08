import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SBA 7(a) & 504 FOIA loan-level bulk ingest.
 *
 * Trigger.dev v4 durable callback. Fans the two programs out as independent CHILD
 * runs via batchTriggerAndWait — a SINGLE batch waitpoint in the parent. They write
 * to distinct Lance datasets (sba_504 / sba_7a) so there is no shared-writer
 * conflict. Each child (sbaFoiaProgram):
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the callback with its terminal metadata.
 *
 * Why a child, not Promise.all over wait.forToken in one run: Trigger.dev forbids
 * concurrent waitpoints within a single run — `Promise.all(PROGRAMS.map(... wait
 * .forToken ...))` trips TASK_DID_CONCURRENT_WAIT ("Parallel waits are not
 * supported") and system-fails. batchTriggerAndWait is the Trigger-sanctioned
 * parallel primitive: one batch waitpoint in the parent, N independent child runs.
 *
 * Manual/back-on-demand by design (no cron): the FOIA files refresh quarterly, so
 * trigger this task with an optional { as_of } when SBA republishes. Flip to
 * `schedules.task` with a quarterly cron if/when an automated cadence is wanted.
 */

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  program?: string;
  dataset_uri?: string;
  as_of?: string;
}

const PROGRAMS = ["504", "7a"] as const;
const PROGRAM_CONCURRENCY = 2; // both programs fan out at once; suspended waits consume no compute

interface ProgramResult {
  program: string;
  rows: number;
  dataset_uri?: string;
}

// CHILD — ingest one SBA program. Exactly one wait.forToken per run, so the two
// programs fan out as two independent runs with no concurrent waitpoint in any run.
export const sbaFoiaProgram = task({
  id: "sba-foia-program",
  queue: { concurrencyLimit: PROGRAM_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: { program: string; asOf: string }): Promise<ProgramResult> =>
    dispatchProgram(payload.program, payload.asOf),
});

export const sbaFoiaIngest = task({
  id: "sba-foia-ingest",
  // The parent suspends at a single batch waitpoint (zero compute) while the two
  // child runs fan out in Modal.
  maxDuration: 7200,
  run: async (payload: { as_of?: string }) => {
    const asOf = payload?.as_of ?? "260331";
    logger.info("SBA FOIA ingest starting", { as_of: asOf, programs: PROGRAMS });

    // Fan out one child run per program under a SINGLE batch waitpoint (a Promise.all
    // over wait.forToken would trip TASK_DID_CONCURRENT_WAIT).
    const { runs } = await sbaFoiaProgram.batchTriggerAndWait(
      PROGRAMS.map((program) => ({ payload: { program, asOf } })),
    );

    // Fail-fast aggregation (mirrors the prior Promise.all semantics).
    const results: ProgramResult[] = [];
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(`sba-foia program run ${run.id} failed: ${JSON.stringify(run.error)}`);
      }
      results.push(run.output);
    }

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("SBA FOIA ingest complete", { rows, results });
    return { as_of: asOf, rows, results };
  },
});

async function dispatchProgram(program: string, asOf: string) {
  // 1) Durable callback token.
  const token = await wait.createToken({ timeout: "1h", tags: ["sba-foia", program] });

  // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "sba-7a-504",
      function_name: "ingest_sba_program",
      kwargs: { program, as_of: asOf },
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for ${program}: ${body.slice(0, 300)}`);
  }

  // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
  const out = await wait.forToken<IngestCallback>(token.id);
  if (!out.ok) throw new Error(`timed out before Modal callback for ${program}`);
  if (out.output.status !== "success") {
    throw new Error(`Modal failed for ${program}: ${JSON.stringify(out.output)}`);
  }

  logger.info("program ingested", { program, rows: out.output.rows });
  return { program, rows: out.output.rows, dataset_uri: out.output.dataset_uri };
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
