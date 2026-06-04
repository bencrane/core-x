import { logger, task } from "@trigger.dev/sdk";

import { sendProposal } from "../proposals/send";
import type { ProposalPayload, SendResult } from "../proposals/types";

/**
 * Control plane — Proposal send (hq-zone form → Anvil e-sign → Resend).
 *
 * On-demand task (NOT scheduled). hq-zone triggers it per potential client:
 *
 *   import { tasks } from "@trigger.dev/sdk";
 *   await tasks.trigger<typeof proposalSend>("proposal-send", payload);
 *
 * The run: renders the proposal to PDF (HTML generation — no Anvil template
 * needed, works on the dev key in test mode), creates an Etch e-sign packet with
 * an embedded signer (Anvil's own emails suppressed), mints a signing URL, and
 * delivers it via Resend. Anvil + Resend keys live ONLY in this control plane;
 * hq-zone holds just a Trigger key.
 *
 * Phase 2 (decoupled): set a per-packet webhookURL so Anvil's etchPacketComplete
 * fires a `proposal-signed` task for downstream actions. Not wired here.
 */
export const proposalSend = task({
  id: "proposal-send",
  maxDuration: 300,
  run: async (payload: ProposalPayload, { ctx }): Promise<SendResult> => {
    logger.info("proposal-send starting", {
      client: payload?.client?.email,
      title: payload?.proposal?.title,
      runId: ctx.run.id,
    });

    const result = await sendProposal(payload);

    logger.info("proposal-send complete", {
      packetEid: result.packetEid,
      isTest: result.isTest,
      mode: result.mode,
      emailSent: result.email?.sent ?? false,
      emailError: result.email?.error,
    });
    return result;
  },
});
