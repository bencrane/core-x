import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SBA 7(a) & 504 FOIA loan-level bulk ingest.
 *
 * Trigger.dev v4 durable callback. Fans the two programs out in PARALLEL — they
 * write to distinct Lance datasets (sba_504 / sba_7a) so there is no shared-writer
 * conflict. For each program this task:
 *   1. mints a waitpoint token (its `url` is a pre-signed HTTP callback),
 *   2. POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the target
 *      worker + that callback url,
 *   3. suspends on `wait.forToken` — checkpointed, consuming no compute and immune
 *      to HTTP timeouts,
 *   4. resumes when the Modal worker POSTs the callback with its terminal metadata.
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

export const sbaFoiaIngest = task({
  id: "sba-foia-ingest",
  // Both programs run concurrently in Modal; the durable waits consume no compute.
  maxDuration: 7200,
  run: async (payload: { as_of?: string }) => {
    const asOf = payload?.as_of ?? "260331";
    logger.info("SBA FOIA ingest starting", { as_of: asOf, programs: PROGRAMS });

    const results = await Promise.all(
      PROGRAMS.map((program) => dispatchProgram(program, asOf)),
    );

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
