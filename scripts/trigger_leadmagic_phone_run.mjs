#!/usr/bin/env node
/**
 * Trigger the deployed `leadmagic-phone-finder-resolve` task (Trigger.dev prod) with a contacts
 * payload — the canonical control-plane path: resolve → chunk (250) → batchTriggerAndWait →
 * chunk children → Universal Dispatcher → Modal worker `run_leadmagic_phone` → ops.phone_resolutions
 * (source_vendor='leadmagic') + ops.leadmagic_phone_finder_runs ledger. No Modal-direct shortcut.
 *
 * Usage: doppler run -p core-x -c prd -- node scripts/trigger_leadmagic_phone_run.mjs <payload.json>
 * payload.json = {"contacts":[{contact_id, person_linkedin_url?, work_email?, ...}], "batchLabel": "..."}
 * TRIGGER_SECRET_KEY (env-scoped: tr_prod_ → prod) selects the environment.
 */
import { tasks } from "@trigger.dev/sdk";
import { readFileSync } from "node:fs";

const path = process.argv[2];
if (!path) {
  console.error("usage: node scripts/trigger_leadmagic_phone_run.mjs <payload.json>");
  process.exit(1);
}
if (!process.env.TRIGGER_SECRET_KEY) {
  console.error("TRIGGER_SECRET_KEY not set (source via doppler)");
  process.exit(1);
}

const { contacts, batchLabel } = JSON.parse(readFileSync(path, "utf8"));
const withEmail = contacts.filter((c) => c.work_email).length;
const withLinkedin = contacts.filter((c) => c.person_linkedin_url).length;
console.log(
  `env=${process.env.TRIGGER_SECRET_KEY.slice(0, 8)} contacts=${contacts.length} ` +
  `linkedin=${withLinkedin} work_email=${withEmail} batchLabel=${batchLabel}`,
);

const handle = await tasks.trigger(
  "leadmagic-phone-finder-resolve",
  { contacts, batchLabel, priority: "low", force: false, chunkSize: 250 },
  { tags: ["leadmagic-phone", "dexarchive-staffing-dm"] },
);

console.log("RUN_ID " + handle.id);
console.log(
  "DASHBOARD https://cloud.trigger.dev/projects/v3/proj_pakdcffjbeiwcixcoepb/runs/" + handle.id,
);
