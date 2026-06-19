import { defineConfig } from "@trigger.dev/sdk/v3";
import { syncEnvVars } from "@trigger.dev/build/extensions/core";

export default defineConfig({
  project: "proj_pakdcffjbeiwcixcoepb",
  runtime: "node",
  logLevel: "log",
  // The max compute seconds a task is allowed to run. If the task run exceeds this duration, it will be stopped.
  // You can override this on an individual task.
  // See https://trigger.dev/docs/runs/max-duration
  maxDuration: 3600,
  retries: {
    enabledInDev: true,
    default: {
      maxAttempts: 3,
      minTimeoutInMs: 1000,
      maxTimeoutInMs: 10000,
      factor: 2,
      randomize: true,
    },
  },
  dirs: ["./src/trigger"],
  build: {
    // Forward the Universal Dispatcher's proxy-auth credentials into this
    // project's deployed environment at deploy time. Values are read from the
    // deploy-time process env (supply them via `doppler run -- npx trigger.dev
    // deploy`); they are never committed to the repo.
    extensions: [
      syncEnvVars(() =>
        [
          // Universal Dispatcher proxy-auth (data-plane tasks)
          "MODAL_DISPATCHER_URL",
          "MODAL_KEY",
          "MODAL_SECRET",
          // gtm post-payment pipeline driver tasks -> edge-api /run-step
          "EDGE_API_BASE_URL",
          "TRIGGER_SHARED_SECRET",
        ]
          .filter((k) => process.env[k])
          .map((k) => ({ name: k, value: process.env[k] as string })),
      ),
    ],
  },
});
