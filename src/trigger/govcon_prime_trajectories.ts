import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — GovCon Prime Trajectories full rebuild.
 *
 * Mints a Trigger.dev v4 waitpoint, POSTs the Universal Dispatcher to spawn the
 * `run_build` Modal worker (idempotent full OVERWRITE of govcon_prime_trajectories),
 * suspends on `wait.forToken` (checkpointed, zero compute), and resumes on the
 * worker's flat terminal callback.
 *
 * Manually triggerable (no payload). Wrap in `schedules.task({ cron })` if a
 * recurring rebuild cadence is wanted — the materialization is a safe idempotent
 * overwrite, so re-runs never corrupt the table. No new endpoint or secret: the
 * worker is resolved by name through the single Universal Dispatcher
 * (MODAL_DISPATCHER_URL + MODAL_KEY/MODAL_SECRET).
 */
interface TrajectoriesCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  as_of_date?: string;
  bonded_uei?: number;
}

export const govconPrimeTrajectoriesBuild = task({
  id: "govcon-prime-trajectories-build",
  maxDuration: 7800, // suspended wait consumes no compute; build itself ~10-20 min on Modal
  run: async (_payload: Record<string, never>, { ctx }) => {
    const token = await wait.createToken({
      timeout: "2h", // 109M-row dedup + per-UEI rollup; generous ceiling
      tags: ["govcon-prime-trajectories", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-govcon-trajectories-pipelines",
        function_name: "run_build",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched govcon_prime_trajectories build → Modal; suspending on waitpoint", {
      tokenId: token.id,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<TrajectoriesCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `govcon_prime_trajectories build timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `govcon_prime_trajectories build failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("govcon_prime_trajectories build complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
