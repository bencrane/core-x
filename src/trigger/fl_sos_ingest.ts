import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Florida SoS (Sunbiz) corporate registry bulk ingest.
 *
 * Trigger.dev v4 durable callback, two-phase. Two fixed-width payloads in R2 landing
 * (cordata.zip → master entity spine, corevent.zip → child event history), keyed on
 * `document_number`:
 *   Phase 1 — dispatches `explode_zip` (7z-extract each member — cordata.zip is
 *             Deflate64, which stdlib zip cannot read — and re-land as .zst) and
 *             suspends on its waitpoint until done.
 *   Phase 2 — fans `ingest_target` out for master + events in PARALLEL. They write to
 *             distinct Lance datasets (fl_sos_corporations / fl_sos_events) so there is
 *             no shared-writer manifest conflict.
 *
 * Each step mints a waitpoint token (its `url` is a pre-signed HTTP callback), POSTs the
 * Universal Dispatcher (the ONLY Modal endpoint) with the target worker + that callback
 * url, then suspends on `wait.forToken` — checkpointed, zero compute, immune to HTTP
 * timeouts — and resumes when the Modal worker POSTs the flat-JSON callback.
 *
 * Manual/on-demand by design (no cron): Sunbiz bulk extracts are pulled by request.
 * `as_of` is the explicit export date; REQUIRED (defaults to "2026-05-31").
 */

const TARGETS = ["master", "events"] as const;
const AS_OF_DEFAULT = "2026-05-31";

// The flat JSON body Modal POSTs to each waitpoint url becomes that step's output.
interface ExplodeCallback {
  status: "success" | "error";
  phase: "explode";
  as_of: string;
  members?: Record<string, string>;
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

export const flSosIngest = task({
  id: "fl-sos-ingest",
  // Phase 1 then 2-way parallel Phase 2 (14.4M-row events + index sorts); durable waits
  // consume no compute while suspended.
  maxDuration: 10800,
  run: async (payload: { as_of?: string }) => {
    const asOf = payload?.as_of ?? AS_OF_DEFAULT;
    logger.info("FL SoS ingest starting", { as_of: asOf, targets: TARGETS });

    // ── Phase 1 — explode both zips into per-member .zst landing artifacts ──
    const explode = await dispatch<ExplodeCallback>("explode_zip", { as_of: asOf });
    logger.info("explode complete", { members: explode.members });

    // ── Phase 2 — ingest master + events in parallel (distinct Lance datasets) ──
    const results = await Promise.all(
      TARGETS.map((target) => dispatch<IngestCallback>("ingest_target", { target, as_of: asOf })),
    );

    const rows = results.reduce((acc, r) => acc + (r.rows ?? 0), 0);
    const byTarget = Object.fromEntries(
      results.map((r) => [r.target, { rows: r.rows, rejected_rows: r.rejected_rows, dataset_uri: r.dataset_uri }]),
    );
    logger.info("FL SoS ingest complete", { as_of: asOf, total_rows: rows, byTarget });
    return { as_of: asOf, total_rows: rows, targets: byTarget };
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
  const token = await wait.createToken({ timeout: "2h", tags: ["fl-sos", tag] });

  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "fl-sos-corporations",
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
