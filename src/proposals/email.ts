/**
 * Resend mailer — delivers the proposal + signing link to the client.
 *
 * Anvil's own signer emails are disabled (signerType embedded, enableEmails:false),
 * so this is the sole delivery path — full control of from-domain, branding, copy.
 *
 * ⚠️ Embedded sign URLs expire ≈2h after minting. Emailing the raw URL is fine for
 * immediate/test sends (the smoke runner). For production, email a link to an
 * hq-zone redirect route that calls generateEtchSignUrl fresh on click — see
 * src/proposals/README.md. `signUrl` here is whatever the caller hands in.
 */

import { Resend } from "resend";

import { resolveProvider } from "./render";
import type { ProposalPayload } from "./types";

export interface EmailResult {
  sent: boolean;
  id?: string;
  to?: string;
  error?: string;
}

function buildHtml(payload: ProposalPayload, signUrl: string): string {
  const provider = resolveProvider(payload.provider);
  const { client, proposal } = payload;
  const from = provider.name ? `${provider.name} at ${provider.company}` : provider.company;
  return `<!doctype html><html><body style="margin:0;background:#f5f5f5;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#1a1a1a">
  <div style="max-width:520px;margin:0 auto;padding:32px 24px">
    <div style="background:#fff;border-radius:12px;padding:32px;border:1px solid #ececec">
      <p style="margin:0 0 16px;font-size:15px">Hi ${escapeText(client.name.split(" ")[0] || client.name)},</p>
      <p style="margin:0 0 16px;font-size:15px;line-height:1.55">
        ${escapeText(from)} has prepared a proposal for your review:
        <strong>${escapeText(proposal.title)}</strong>.
      </p>
      <p style="margin:0 0 24px;font-size:15px;line-height:1.55">
        Review the full document and add your signature using the secure link below.
      </p>
      <a href="${escapeAttr(signUrl)}" style="display:inline-block;background:#111;color:#fff;text-decoration:none;padding:13px 22px;border-radius:8px;font-size:15px;font-weight:600">
        Review &amp; sign proposal
      </a>
      <p style="margin:24px 0 0;font-size:12px;color:#888;line-height:1.5">
        This signing link is unique to you. If you have questions, just reply to this email.
      </p>
    </div>
    <p style="text-align:center;margin:16px 0 0;font-size:11px;color:#aaa">
      ${escapeText(provider.company)}${provider.website ? " &middot; " + escapeText(provider.website) : ""}
    </p>
  </div>
  </body></html>`;
}

function escapeText(s: unknown): string {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeAttr(s: unknown): string {
  return escapeText(s).replace(/"/g, "&quot;");
}

export async function sendProposalEmail(
  payload: ProposalPayload,
  signUrl: string,
): Promise<EmailResult> {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) return { sent: false, error: "RESEND_API_KEY unset" };

  const to = payload.options?.emailToOverride ?? payload.client.email;
  const from = process.env.PROPOSAL_FROM_EMAIL ?? "onboarding@resend.dev";
  const provider = resolveProvider(payload.provider);
  const subject = `${provider.company}: ${payload.proposal.title}`;

  try {
    const resend = new Resend(apiKey);
    const { data, error } = await resend.emails.send({
      from,
      to,
      subject,
      html: buildHtml(payload, signUrl),
      ...(provider.email ? { replyTo: provider.email } : {}),
    });
    if (error) return { sent: false, to, error: `${error.name}: ${error.message}` };
    return { sent: true, to, id: data?.id };
  } catch (err) {
    return { sent: false, to, error: String(err) };
  }
}
