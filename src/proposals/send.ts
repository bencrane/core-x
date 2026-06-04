/**
 * Proposal send orchestration — the pure, framework-agnostic core.
 *
 * render → createEtchPacket → generateSignUrl → (Resend) email. Shared by the
 * Trigger.dev task (src/trigger/proposal_send.ts) and the local smoke runner
 * (scripts/proposal_smoke.ts) so both exercise the identical path.
 */

import {
  createProposalPacket,
  generateSignUrl,
  resolveAnvilConfig,
  type AnvilConfig,
} from "./anvil";
import { sendProposalEmail } from "./email";
import { renderProposal } from "./render";
import type { ProposalPayload, SendResult } from "./types";

export async function sendProposal(
  payload: ProposalPayload,
  cfg: AnvilConfig = resolveAnvilConfig(),
): Promise<SendResult> {
  validatePayload(payload);

  const rendered = renderProposal(payload);
  const packet = await createProposalPacket(payload, rendered, cfg);

  const clientUserId = payload.options?.clientUserId ?? payload.client.email;
  const signUrl = await generateSignUrl(packet.signerEid, clientUserId, cfg);

  const result: SendResult = {
    ok: true,
    mode: cfg.mode,
    isTest: cfg.isTest,
    packetEid: packet.packetEid,
    documentGroupEid: packet.documentGroupEid,
    signerEid: packet.signerEid,
    signUrl,
    detailsURL: packet.detailsURL,
  };

  if (payload.options?.skipEmail) {
    result.email = { sent: false, error: "skipped (options.skipEmail)" };
    return result;
  }

  result.email = await sendProposalEmail(payload, signUrl);
  return result;
}

function validatePayload(payload: ProposalPayload): void {
  const errs: string[] = [];
  if (!payload?.client?.name) errs.push("client.name is required");
  if (!payload?.client?.email) errs.push("client.email is required");
  if (!payload?.proposal?.title) errs.push("proposal.title is required");
  if (!payload?.proposal?.lineItems?.length) errs.push("proposal.lineItems must have at least one row");
  if (errs.length) throw new Error(`Invalid proposal payload: ${errs.join("; ")}`);
}

export type { ProposalPayload, SendResult };
