import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — captive-sub diversification serving rebuild (weekly).
 *
 * Rebuilds s3://data-sink/active/captive_sub_diversification/ — the "diversify off your
 * one prime" GTM target list: every single-prime captive subawardee scored (BGE ANN over the
 * prime-solicitation scope corpus) against fresh awards won by OTHER primes it is a domain-aligned
 * capability match for. The ANN, full-universe upgrade of govcon_sub_targeting's deterministic
 * capability_match leg.
 *
 * Dispatches the Modal `run_build` worker (app "captive-diversification") through the Universal
 * Dispatcher and waits on a durable token for its terminal callback. Snapshot-overwrite + idempotent,
 * so the default retry policy is safe. Mon 11:00 UTC — off-peak, after the weekend prime-feed appends.
 * Cadence/window are tunable: the worker reads CAPTIVE_WINDOW_DAYS (default 365) for the rolling
 * award action_date window; the serving query filters tighter (e.g. last 90 days).
 */
interface DiversificationCallback {
  status: "success" | "error";
  run_mode: "build";
  feed: string;
  rows: number;
  naics2_aligned_rows: number;
  subs_with_naics2_match: number;
  window_days: number;
  error?: string | null;
}

export const captiveDiversification = schedules.task({
  id: "captive-diversification",
  cron: { pattern: "0 11 * * 1", timezone: "UTC" }, // Mon 11:00 UTC — off-peak, post-weekend appends
  maxDuration: 5400,
  run: async () => {
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["captive-diversification", "serving"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "captive-diversification",
        function_name: "run_build",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      throw new Error(`dispatcher ${res.status}: ${(await res.text()).slice(0, 300)}`);
    }

    const r = await wait.forToken<DiversificationCallback>(token.id);
    if (!r.ok) throw new Error(`captive-diversification timed out (token ${token.id})`);
    if (r.output.status !== "success") {
      throw new Error(`captive-diversification build failed: ${JSON.stringify(r.output)}`);
    }
    logger.info("captive-diversification rebuilt", { ...r.output });
    return r.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
