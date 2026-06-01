import { schedules, wait, logger } from "@trigger.dev/sdk";

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
 * Single-phase fan-out: dispatch `ingest_dataset` for part1 + advw in PARALLEL. They write to
 * distinct Lance datasets, so there is no shared-writer manifest conflict. Each dispatch mints
 * a waitpoint token (its `url` is a pre-signed HTTP callback — no API key; the callbackHash in
 * the URL authenticates), POSTs the Universal Dispatcher (the ONLY Modal endpoint) with the
 * target worker + that callback url, then suspends on `wait.forToken` — checkpointed, zero
 * compute, immune to HTTP timeouts — and resumes when the Modal worker POSTs its flat-JSON
 * terminal callback. Trigger owns true end-to-end success/failure state; no polling.
 *
 * Monthly cadence (09:00 ET on the 1st): the SEC posts the Form ADV historical data files
 * periodically; a monthly full-snapshot overwrite keeps both datasets current.
 */

const APP_NAME = "sec-adv-pipelines";
const DATASETS = ["part1", "advw"] as const;

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

export const secAdvMonthly = schedules.task({
  id: "sec-adv-monthly",
  // 09:00 America/New_York on the 1st of every month.
  cron: { pattern: "0 9 1 * *", timezone: "America/New_York" },
  // Two parallel ingests (the part1 download + ~600k-row transform + index is the long pole);
  // durable waits consume no compute while suspended.
  maxDuration: 5400,
  run: async (payload) => {
    logger.info("SEC ADV monthly ingest starting", { scheduledAt: payload.timestamp, datasets: DATASETS });

    // Fan out both datasets in parallel (distinct Lance datasets — no writer conflict).
    const results = await Promise.all(
      DATASETS.map((dataset) => dispatch<IngestCallback>("ingest_dataset", { dataset })),
    );

    const totalRows = results.reduce((acc, r) => acc + (r.rows ?? 0), 0);
    const byDataset = Object.fromEntries(
      results.map((r) => [r.dataset, { rows: r.rows, dataset_uri: r.dataset_uri, as_of: r.as_of }]),
    );
    logger.info("SEC ADV monthly ingest complete", { total_rows: totalRows, byDataset });
    return { total_rows: totalRows, datasets: byDataset };
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
      app_name: APP_NAME,
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
