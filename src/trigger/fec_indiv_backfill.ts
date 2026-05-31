import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — FEC Individual Contributions, Phase 2 (SEQUENTIAL append).
 *
 * Iterates the 24 landed cycles ONE AT A TIME through the Universal Dispatcher into
 * `fec-contributions` / `ingest_indiv_cycle`. Sequential on purpose: all cycles
 * append to the SINGLE unified Lance dataset, and concurrent writers to one dataset
 * can hit OCC commit conflicts. Each cycle suspends on a durable waitpoint token, so
 * the run consumes no compute while Modal works and is immune to HTTP timeouts. Each
 * append is idempotent (the worker deletes WHERE cycle_year=N before appending).
 *
 * Run Phase 1 (fec-indiv-land) first. Trigger this with an empty payload to ingest
 * all 24 cycles, or { years: [2024, 2026] } for a subset. Build indexes ONCE after,
 * via `modal run pipelines/fec/indiv_contributions.py::index`.
 */

const CYCLE_YEARS: number[] = Array.from({ length: 24 }, (_, i) => 1980 + i * 2);

const yy = (year: number) => String(year % 100).padStart(2, "0");
const landingKey = (year: number) => `landing/fec_indiv/indiv${yy(year)}.txt.zst`;

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  cycle_year?: number;
  source_file?: string;
}

export const fecIndivBackfill = task({
  id: "fec-indiv-backfill",
  // Sequential over 24 cycles; the durable waits consume no compute while suspended.
  maxDuration: 21600,
  run: async (payload: { years?: number[] }) => {
    const years = payload?.years?.length ? payload.years : CYCLE_YEARS;
    logger.info("FEC indiv backfill (Phase 2) starting", { cycles: years.length });

    const results: Array<{ cycle_year: number; rows: number }> = [];
    for (const year of years) {
      // 1) Durable callback token (generous — a streaming ingest of up to ~58M rows).
      const token = await wait.createToken({ timeout: "2h", tags: ["fec-indiv-backfill", String(year)] });

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
          function_name: "ingest_indiv_cycle",
          kwargs: { key: landingKey(year), cycle_year: year },
          trigger_callback_url: token.url,
        }),
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`dispatcher ${res.status} for cycle ${year}: ${body.slice(0, 300)}`);
      }

      // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
      const out = await wait.forToken<IngestCallback>(token.id);
      if (!out.ok) throw new Error(`timed out before Modal callback for cycle ${year}`);
      if (out.output.status !== "success") {
        throw new Error(`Modal ingest failed for cycle ${year}: ${JSON.stringify(out.output)}`);
      }

      logger.info("cycle ingested", { year, rows: out.output.rows });
      results.push({ cycle_year: year, rows: out.output.rows });
    }

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("FEC indiv backfill complete", { cycles: results.length, rows });
    return { cycles: results.length, rows, results };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
