import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — sub diversification serving rebuild (weekly).
 *
 * Rebuilds s3://data-sink/active/govcon_sub_diversification/ — the neutral, full-universe
 * sub→NEW-prime diversification substrate: every subawardee scored (BGE ANN over the
 * prime-solicitation scope corpus) against fresh awards won by primes it does NOT already work
 * under, that it is a domain-aligned capability match for. Generalizes the retired captive build:
 * the "captive" segment is now a query-time predicate (n_incumbent_primes = 1), not a baked
 * substrate.
 *
 * Dispatches the Modal `run_build` worker (app "sub-diversification") through the Universal
 * Dispatcher and waits on a durable token for its terminal callback. Snapshot-overwrite + idempotent,
 * so the default retry policy is safe. Mon 11:00 UTC — off-peak, after the weekend prime-feed appends.
 * Cadence/window are tunable: the worker reads SUBDIV_WINDOW_DAYS (default 365) for the rolling
 * award action_date window; the serving query filters tighter (e.g. last 90 days).
 */
interface DiversificationCallback {
  status: "success" | "error";
  run_mode: "build";
  feed: string;
  rows: number;
  naics2_aligned_rows: number;
  subs_with_naics2_match: number;
  single_prime_subs: number;
  multi_prime_subs: number;
  window_days: number;
  error?: string | null;
}

export const subDiversification = schedules.task({
  id: "sub-diversification",
  // PARKED (Trigger free-plan 10-schedule cap, 2026-07-19): cron removed; restore to reinstate. // Mon 11:00 UTC — off-peak, post-weekend appends
  // cron: { pattern: "0 11 * * 1", timezone: "UTC" },
  maxDuration: 9000, // 2.5h — full-universe ANN is ~8x the captive build
  run: async () => {
    const token = await wait.createToken({
      timeout: "3h",
      tags: ["sub-diversification", "serving"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "sub-diversification",
        function_name: "run_build",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      throw new Error(`dispatcher ${res.status}: ${(await res.text()).slice(0, 300)}`);
    }

    const r = await wait.forToken<DiversificationCallback>(token.id);
    if (!r.ok) throw new Error(`sub-diversification timed out (token ${token.id})`);
    if (r.output.status !== "success") {
      throw new Error(`sub-diversification build failed: ${JSON.stringify(r.output)}`);
    }
    logger.info("sub-diversification rebuilt", { ...r.output });
    return r.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
