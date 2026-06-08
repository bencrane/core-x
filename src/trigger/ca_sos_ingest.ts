import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — California Secretary of State bulk business-records ingest.
 *
 * Trigger.dev v4 durable callback, two-phase. The FOIA payload is a single ZIP in
 * R2 landing holding three relational members (Filings/Agents/Principals) keyed on
 * ENTITY_NUM; this task:
 *   Phase 1 — dispatches `explode_zip` (extract each member → per-member .csv.zst
 *             landing artifacts) and suspends on its single waitpoint until done.
 *   Phase 2 — fans the three members out as independent CHILD runs (caSosMember) via
 *             batchTriggerAndWait — a SINGLE batch waitpoint, sequential to Phase 1's
 *             wait. They write to distinct Lance datasets (ca_sos_entities /
 *             ca_sos_agents / ca_sos_principals) so there is no shared-writer conflict.
 *             A Promise.all over wait.forToken in one run trips TASK_DID_CONCURRENT_WAIT.
 *
 * Each step mints a waitpoint token (its `url` is a pre-signed HTTP callback), POSTs
 * the Universal Dispatcher (the ONLY Modal endpoint) with the target worker + that
 * callback url, then suspends on `wait.forToken` — checkpointed, zero compute, immune
 * to HTTP timeouts — and resumes when the Modal worker POSTs the flat-JSON callback.
 *
 * Manual/on-demand by design (no cron): CA SoS bulk extracts are pulled by request.
 * `as_of` is the explicit export date (the date the state generated the export);
 * it is REQUIRED (defaults to "2026-05-31"), never derived from R2 LastModified.
 */

const MEMBERS = ["entities", "agents", "principals"] as const;
const AS_OF_DEFAULT = "2026-05-31";
const MEMBER_CONCURRENCY = 3; // all three members fan out at once; suspended waits consume no compute

// The flat JSON body Modal POSTs to each waitpoint url becomes that step's output.
interface ExplodeCallback {
  status: "success" | "error";
  phase: "explode";
  as_of: string;
  source_zip?: string;
  members?: Record<string, string>;
}

interface IngestCallback {
  status: "success" | "error";
  phase: "ingest";
  member: string;
  rows: number;
  rejected_rows: number;
  dataset_uri?: string;
  as_of?: string;
}

// CHILD — ingest one member. Exactly one wait.forToken per run, so the three members fan
// out as independent runs with no concurrent waitpoint in any single run.
export const caSosMember = task({
  id: "ca-sos-member",
  queue: { concurrencyLimit: MEMBER_CONCURRENCY },
  maxDuration: 3900, // ~65m; exceeds the 1h token timeout with margin
  run: async (payload: { member: string; asOf: string }): Promise<IngestCallback> =>
    dispatch<IngestCallback>("ingest_member", { member: payload.member, as_of: payload.asOf }),
});

export const caSosIngest = task({
  id: "ca-sos-ingest",
  // Phase 1 then 3-way parallel Phase 2; durable waits consume no compute while suspended.
  maxDuration: 10800,
  run: async (payload: { as_of?: string; zip_key?: string }) => {
    const asOf = payload?.as_of ?? AS_OF_DEFAULT;
    logger.info("CA SoS ingest starting", { as_of: asOf, members: MEMBERS });

    // ── Phase 1 — explode the ZIP into per-member landing artifacts ──
    const explode = await dispatch<ExplodeCallback>("explode_zip", {
      as_of: asOf,
      ...(payload?.zip_key ? { zip_key: payload.zip_key } : {}),
    });
    logger.info("explode complete", { source_zip: explode.source_zip, members: explode.members });

    // ── Phase 2 — fan the three members out as independent child runs under a SINGLE
    // batch waitpoint (sequential to Phase 1's wait; a Promise.all over wait.forToken
    // would trip TASK_DID_CONCURRENT_WAIT). Distinct Lance datasets — no conflict. ──
    const { runs } = await caSosMember.batchTriggerAndWait(
      MEMBERS.map((member) => ({ payload: { member, asOf } })),
    );

    const results: IngestCallback[] = [];
    for (const run of runs) {
      if (!run.ok) {
        throw new Error(`ca-sos member run ${run.id} failed: ${JSON.stringify(run.error)}`);
      }
      results.push(run.output);
    }

    const rows = results.reduce((acc, r) => acc + (r.rows ?? 0), 0);
    const byMember = Object.fromEntries(
      results.map((r) => [r.member, { rows: r.rows, rejected_rows: r.rejected_rows, dataset_uri: r.dataset_uri }]),
    );
    logger.info("CA SoS ingest complete", { as_of: asOf, total_rows: rows, byMember });
    return { as_of: asOf, source_zip: explode.source_zip, total_rows: rows, members: byMember };
  },
});

/**
 * Mint a durable waitpoint, fire the Universal Dispatcher (202), and suspend until the
 * Modal worker POSTs its flat-JSON terminal callback. Returns that callback body.
 */
async function dispatch<T extends { status: "success" | "error" }>(
  functionName: string,
  kwargs: Record<string, unknown>,
): Promise<T> {
  const tag = String(kwargs.member ?? functionName);
  const token = await wait.createToken({ timeout: "1h", tags: ["ca-sos", tag] });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "ca-sos-businesses",
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
