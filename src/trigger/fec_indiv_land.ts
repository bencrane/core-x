import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — FEC Individual Contributions, Phase 1 (PARALLEL land).
 *
 * Fans the 24 even-year cycles (1980-2026) out in PARALLEL: each cycle mints its
 * own waitpoint token and dispatches `fec-contributions` / `fetch_indiv_to_landing`
 * through the Universal Dispatcher (the ONLY Modal endpoint). The land step writes
 * independent R2 landing keys (no Lance writes) so concurrency is safe; the Modal
 * worker caps real fan-out at max_containers=8 to stay polite to FEC's GovCloud S3.
 * Each run suspends on its token (checkpointed, zero compute, immune to HTTP
 * timeouts) and resumes on the worker's flat terminal callback.
 *
 * Bounded backfill (no cron): trigger with an empty payload to land all 24 cycles,
 * or { years: [2024, 2026] } to land a subset (e.g. refreshing the live cycle).
 */

const CYCLE_YEARS: number[] = Array.from({ length: 24 }, (_, i) => 1980 + i * 2);

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface LandCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  year?: number;
  landing_key?: string;
}

export const fecIndivLand = task({
  id: "fec-indiv-land",
  // 24 parallel durable waits; the suspended waits consume no compute.
  maxDuration: 7200,
  run: async (payload: { years?: number[] }) => {
    const years = payload?.years?.length ? payload.years : CYCLE_YEARS;
    logger.info("FEC indiv land (Phase 1) starting", { cycles: years.length });

    const results = await Promise.all(years.map((year) => landCycle(year)));

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("FEC indiv land complete", { cycles: results.length, rows });
    return { cycles: results.length, rows, results };
  },
});

async function landCycle(year: number) {
  // 1) Durable callback token (generous — a ~4 GiB download for recent cycles).
  const token = await wait.createToken({ timeout: "1h", tags: ["fec-indiv-land", String(year)] });

  // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
  const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": requireEnv("MODAL_KEY"),
      "Modal-Secret": requireEnv("MODAL_SECRET"),
    },
    body: JSON.stringify({
      app_name: "fec-contributions",
      function_name: "fetch_indiv_to_landing",
      kwargs: { year },
      trigger_callback_url: token.url,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`dispatcher ${res.status} for cycle ${year}: ${body.slice(0, 300)}`);
  }

  // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
  const out = await wait.forToken<LandCallback>(token.id);
  if (!out.ok) throw new Error(`timed out before Modal callback for cycle ${year}`);
  if (out.output.status !== "success") {
    throw new Error(`Modal land failed for cycle ${year}: ${JSON.stringify(out.output)}`);
  }

  logger.info("cycle landed", { year, rows: out.output.rows, key: out.output.landing_key });
  return { year, rows: out.output.rows, landing_key: out.output.landing_key };
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
