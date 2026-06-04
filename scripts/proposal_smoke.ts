/**
 * Local smoke runner for the proposal-send flow — exercises the FULL path
 * (render → Anvil createEtchPacket → generateSignUrl → optional Resend email)
 * against the dev key in test mode. No Trigger.dev server required.
 *
 *   doppler run -- npx tsx scripts/proposal_smoke.ts                  # key-check + create packet + sign URL + download test PDF
 *   doppler run -- npx tsx scripts/proposal_smoke.ts --to you@x.com   # also email it via Resend to you@x.com
 *   doppler run -- npx tsx scripts/proposal_smoke.ts --keycheck       # auth probe only
 *
 * Flags:
 *   --to <email>   recipient override + actually send the email (Resend)
 *   --keycheck     run only the Anvil auth/health gate
 *   --no-download  skip writing the test PDF to /tmp
 */

import { writeFileSync } from "node:fs";

import { anvilKeyCheck, downloadProposalZip, resolveAnvilConfig } from "../src/proposals/anvil";
import { sendProposal } from "../src/proposals/send";
import type { ProposalPayload } from "../src/proposals/types";

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}
function has(name: string): boolean {
  return process.argv.includes(name);
}

function samplePayload(toOverride?: string): ProposalPayload {
  return {
    client: {
      name: "Dana Whitfield",
      email: toOverride ?? "dana@meridian-realty.example",
      company: "Meridian Realty Group",
      title: "VP, Marketing",
    },
    provider: {
      name: "Ben Crane",
      company: "Substrate",
      email: process.env.PROPOSAL_PROVIDER_EMAIL ?? "tools@substrate.build",
      website: "substrate.build",
    },
    proposal: {
      title: "Direct Mail Campaign — Proposal & Agreement",
      number: "PRO-2026-0042",
      validUntilISO: "2026-07-15",
      intro:
        "Thank you for the opportunity to partner on Meridian's Q3 direct mail outreach. This proposal covers creative, print, and managed mailing for a targeted 25,000-piece campaign across your three priority markets.\n\nOur goal: a measurable lift in qualified inbound within 60 days of the first drop.",
      scopeItems: [
        "Audience build & list hygiene (dedupe, NCOA, suppression)",
        "Creative concepting + 2 rounds of revisions on a 6\" x 11\" oversized postcard",
        "Variable-data personalization (recipient name, neighborhood, QR-coded landing page)",
        "Print production on 14pt C2S gloss, full bleed",
        "Managed mailing, postage, and drop scheduling (2 waves)",
        "Response tracking dashboard (scans, calls, landing-page conversions)",
      ],
      lineItems: [
        { description: "Campaign strategy & audience build", quantity: 1, unitPrice: 2500 },
        { description: "Creative design (oversized postcard, 2 revisions)", quantity: 1, unitPrice: 1800 },
        { description: "Print production — 6\"x11\" 14pt gloss", quantity: 25000, unitPrice: 0.22 },
        { description: "Variable-data personalization", quantity: 25000, unitPrice: 0.04 },
        { description: "Postage & managed mailing (2 waves)", quantity: 25000, unitPrice: 0.39 },
        { description: "Response tracking dashboard (3 mo.)", quantity: 1, unitPrice: 1200 },
      ],
      currency: "USD",
      notes: "Pricing assumes a single shared artwork across markets. Additional creative variants billed at $600 each.",
      terms:
        "50% deposit due on signature; balance due net-15 from first drop. Postage is pass-through at USPS rates current at mail date. Either party may cancel unstarted waves with 10 business days' notice. This agreement is governed by the laws of the State of Delaware.",
    },
    options: {
      skipEmail: !toOverride,
      emailToOverride: toOverride,
    },
  };
}

async function main() {
  const cfg = resolveAnvilConfig();
  console.log(`\n[anvil] mode=${cfg.mode} isTest=${cfg.isTest} key=${cfg.apiKey.slice(0, 5)}…(${cfg.apiKey.length})\n`);

  console.log("→ key-check…");
  const kc = await anvilKeyCheck(cfg);
  console.log(JSON.stringify(kc, null, 2));
  if (!kc.valid) {
    console.error("\n✗ key-check failed — aborting.");
    process.exit(1);
  }
  if (has("--keycheck")) return;

  const to = arg("--to");
  const payload = samplePayload(to);

  console.log(`\n→ sendProposal (skipEmail=${payload.options?.skipEmail})…`);
  const result = await sendProposal(payload, cfg);
  console.log(JSON.stringify(result, null, 2));

  if (result.documentGroupEid && !has("--no-download")) {
    try {
      console.log("\n→ downloading test PDF…");
      const zip = await downloadProposalZip(result.documentGroupEid, cfg);
      const out = `/tmp/proposal-${result.packetEid}.zip`;
      writeFileSync(out, zip);
      console.log(`  saved ${zip.length} bytes → ${out}  (unzip to view the watermarked PDF)`);
    } catch (err) {
      console.log(`  (download skipped: ${String(err)})`);
    }
  }

  console.log("\n✓ done.");
  if (result.signUrl) console.log(`\n  Sign URL (≈2h TTL):\n  ${result.signUrl}\n`);
}

main().catch((err) => {
  console.error("\n✗ smoke failed:", err);
  process.exit(1);
});
