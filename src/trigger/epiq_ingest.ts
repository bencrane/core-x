import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Epiq corporate bankruptcy harvest (dm.epiq11.com getcards API).
 *
 * Modal app "epiq-pipelines". The fleet's first API harvest — three grains, two phases:
 *   P1  harvest_cases   → GET /api/search/getcases (master universe), lands raw + a
 *                         project_codes manifest, writes active/epiq_cases (overwrite),
 *                         and returns the manifest's R2 key + the project_code count.
 *   P2  harvest_claims ‖ harvest_dockets  (Promise.all — distinct Lance datasets → no
 *                         shared-writer manifest conflict). Each reads the P1 manifest
 *                         from R2 and fans out ONE Modal container PER project_code via
 *                         fetch_grain_for_case.map(), capped at max_containers=8 — the
 *                         single global politeness ceiling against Epiq. The wide dynamic
 *                         fan-out lives Modal-side; Trigger holds exactly 3 waitpoint
 *                         tokens, not 2N.
 *
 * Retry/rate-limit: 429/403/5xx from dm.epiq11.com are handled INSIDE the Modal worker
 * (_request backoff + Retry-After + session rotation); the per-case fetcher also carries
 * Modal retries=2. This task only sees the dispatcher's status; a non-2xx there throws
 * into the global trigger.config retry envelope (maxAttempts 3, 1s→10s ×2, jittered).
 *
 * Manual/parameterized (run_date defaults to today, UTC). The PDFs are excluded by
 * design — document references are captured, binaries are never fetched. Cadence: promote
 * cases to a daily schedule once proven; keep claims/dockets on-demand until the
 * incremental high-water-mark path lands (full daily re-harvest of every register is heavy).
 */

// The flat JSON body Modal POSTs to each waitpoint url becomes that step's output.
interface CasesCallback {
  status: "success" | "error";
  feed: "cases";
  run_date: string;
  rows: number;
  project_codes: number;
  manifest_key?: string; // R2 key of project_codes.json — the P2 fan-out seed
  dataset_uri?: string;
}

interface GrainCallback {
  status: "success" | "error";
  feed: "claims" | "dockets";
  run_date: string;
  rows: number;
  cases_attempted: number;
  cases_failed: number; // per-case fetch failures (partial durability: harvest continues)
  dataset_uri?: string;
}

export const epiqIngest = task({
  id: "epiq-ingest",
  // P2 fan-out + two full DuckDB→Lance transforms; the durable waits consume no compute
  // while suspended (the token timeouts below bound the wait, not maxDuration).
  maxDuration: 10800,
  run: async (payload: { run_date?: string }) => {
    const runDate = payload?.run_date ?? todayUtc();
    logger.info("Epiq ingest starting", { run_date: runDate });

    // P1 — cases (also yields the project_codes manifest the grains fan out over).
    const cases = await dispatch<CasesCallback>(
      "harvest_cases",
      { run_date: runDate },
      "1h",
      ["epiq", "cases"],
    );
    if (!cases.manifest_key) {
      throw new Error("harvest_cases returned no manifest_key");
    }

    // P2 — claims ‖ dockets in parallel (distinct Lance datasets → no writer conflict).
    const [claims, dockets] = await Promise.all([
      dispatch<GrainCallback>(
        "harvest_claims",
        { manifest_key: cases.manifest_key, run_date: runDate },
        "3h",
        ["epiq", "claims"],
      ),
      dispatch<GrainCallback>(
        "harvest_dockets",
        { manifest_key: cases.manifest_key, run_date: runDate },
        "3h",
        ["epiq", "dockets"],
      ),
    ]);

    const result = {
      run_date: runDate,
      project_codes: cases.project_codes,
      cases: cases.rows,
      claims: claims.rows,
      dockets: dockets.rows,
      cases_failed: { claims: claims.cases_failed, dockets: dockets.cases_failed },
    };
    logger.info("Epiq ingest complete", result);
    return result;
  },
});

/**
 * Mint a durable waitpoint, fire the Universal Dispatcher (202), and suspend until the
 * Modal worker POSTs its flat-JSON terminal callback. Returns that callback body.
 */
async function dispatch<T extends { status: "success" | "error" }>(
  functionName: string,
  kwargs: Record<string, unknown>,
  timeout: string,
  tags: string[],
): Promise<T> {
  const token = await wait.createToken({ timeout, tags });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "epiq-pipelines",
      function_name: functionName,
      kwargs,
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for ${functionName}: ${body.slice(0, 300)}`);
  }

  const out = await wait.forToken<T>(token.id);
  if (!out.ok) throw new Error(`timed out before Modal callback for ${functionName}`);
  if (out.output.status !== "success") {
    throw new Error(`Modal failed for ${functionName}: ${JSON.stringify(out.output)}`);
  }
  return out.output;
}

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
