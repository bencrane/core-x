import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — California CSLB (Contractors State License Board) registry bulk ingest.
 *
 * Trigger.dev v4 durable callback. Three uncompressed UTF-8 CSV payloads in R2 landing,
 * written to three DISTINCT Lance datasets keyed on `license_number`:
 *   MasterLicenseData.csv -> cslb_licenses       (entity spine, PK license_number)
 *   PersonnelData.csv     -> cslb_personnel       (1:N child, 100% RI to master)
 *   WorkerCompData.csv    -> cslb_workers_comp    (coverage peer; soft key, 49% RI)
 *
 * Single-phase (no explode — the payloads are plain CSV): fan `ingest_target` out for the
 * three targets in PARALLEL. They write to distinct Lance datasets, so there is no
 * shared-writer manifest conflict. Each dispatch mints a waitpoint token (its `url` is a
 * pre-signed HTTP callback), POSTs the Universal Dispatcher (the ONLY Modal endpoint) with
 * the target worker + that callback url, then suspends on `wait.forToken` — checkpointed,
 * zero compute, immune to HTTP timeouts — and resumes when the Modal worker POSTs its
 * flat-JSON terminal callback.
 *
 * Manual/on-demand by design (NO cron): refresh follows a manual R2 payload drop into
 * landing/ca_cslb/. `as_of` is the export date; operator-overridable (defaults "2026-05-31").
 */

const TARGETS = ["licenses", "personnel", "workers_comp"] as const;
const AS_OF_DEFAULT = "2026-05-31";

// The flat JSON body Modal POSTs to each waitpoint url becomes that step's output.
interface IngestCallback {
  status: "success" | "error";
  phase: "ingest";
  target: string;
  rows: number;
  rejected_rows: number;
  dataset_uri?: string;
  as_of?: string;
}

export const caCslbIngest = task({
  id: "ca-cslb-ingest",
  // Three parallel overwrites + scalar-index builds over ≤406k rows; the durable waits
  // consume no compute while suspended.
  maxDuration: 3600,
  run: async (payload: { as_of?: string }) => {
    const asOf = payload?.as_of ?? AS_OF_DEFAULT;
    logger.info("CA CSLB ingest starting", { as_of: asOf, targets: TARGETS });

    // Fan out all three targets in parallel (distinct Lance datasets — no conflict).
    const results = await Promise.all(
      TARGETS.map((target) => dispatch<IngestCallback>("ingest_target", { target, as_of: asOf })),
    );

    const totalRows = results.reduce((acc, r) => acc + (r.rows ?? 0), 0);
    const byTarget = Object.fromEntries(
      results.map((r) => [r.target, { rows: r.rows, rejected_rows: r.rejected_rows, dataset_uri: r.dataset_uri }]),
    );
    logger.info("CA CSLB ingest complete", { as_of: asOf, total_rows: totalRows, byTarget });
    return { as_of: asOf, total_rows: totalRows, targets: byTarget };
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
  const tag = String(kwargs.target ?? functionName);
  const token = await wait.createToken({ timeout: "1h", tags: ["ca-cslb", tag] });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "ca-cslb-pipelines",
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
