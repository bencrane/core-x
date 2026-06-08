"""Strategic Origination Mandate — merge data → clean legal HTML for DocRaptor.

This renders the LEGAL artifact (a normal, professional, white legal document), NOT the
dark proposal page — that gestalt lives in the consumer app's React renderer. The merge
tokens (``<<...>>`` in the source docx) are bound here from the Proposal row.

Signature placement: Rare Structure's execution block is PRE-RENDERED (RS issues a pre-signed
mandate). The CLIENT signature + date are left as a visible blank block; the Documenso v2
SIGNATURE + DATE fields are overlaid there by coordinate placement (see ``documenso_client``),
so nothing in the PDF shows a literal placeholder token if auto-detection is unavailable.

Tokens are substituted by ``str.replace`` against a ``«TOKEN»`` sentinel (NOT ``str.format``)
so the print-CSS braces are never touched.
"""
from __future__ import annotations

import datetime as _dt
import html

from .models import Proposal, format_usd

# Documenso v2 auto-detected text anchors for the CLIENT recipient (r1). Rendered INVISIBLY
# (white, tiny) in the PDF; v2 envelope/create scans the uploaded document and creates the
# signature + date fields at these tokens — robust to variable page count (no coordinate guess).
# CALIBRATE the exact tag syntax against the live v2 instance before Platform go-live.
CLIENT_SIGNATURE_TOKEN = "{{signature:r1}}"
CLIENT_DATE_TOKEN = "{{date:r1}}"


def _long_date(d: _dt.date) -> str:
    return f"{d:%B} {d.day}, {d:%Y}"


def render_agreement_html(p: Proposal) -> str:
    """Return the full agreement as a self-contained print-CSS HTML document."""
    sub = {
        "«EFFECTIVE_DATE»": html.escape(_long_date(p.effective_date)),
        "«CLIENT_NAME»": html.escape(p.client_name),
        "«MONTHLY_FEE»": html.escape(format_usd(p.monthly_fee_cents)),
        "«QUARTERLY_TOTAL»": html.escape(format_usd(p.quarterly_total_cents)),
        "«RS_NAME»": html.escape(p.rs_signer_name),
        "«RS_DATE»": html.escape(_long_date(p.effective_date)),
        "«CLIENT_SIGNER_NAME»": html.escape(p.client_signer_name),
        "«CLIENT_TITLE»": html.escape(p.client_title or ""),
        # Raw (un-escaped) — these must reach the PDF as literal Documenso anchor tags.
        "«CLIENT_SIG_TOKEN»": CLIENT_SIGNATURE_TOKEN,
        "«CLIENT_DATE_TOKEN»": CLIENT_DATE_TOKEN,
    }
    out = _TEMPLATE
    for token, value in sub.items():
        out = out.replace(token, value)
    return out


_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Strategic Origination Mandate</title>
<style>
  @page { size: Letter; margin: 1in 1in 1.1in 1in;
    @bottom-center {
      content: "RARE STRUCTURE LLC  \2022  STRATEGIC ORIGINATION MANDATE  \2022  Page " counter(page);
      font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 7.5pt; letter-spacing: 0.12em;
      color: #9aa0aa; text-transform: uppercase;
    }
  }
  * { box-sizing: border-box; }
  body { font-family: 'Georgia', 'Times New Roman', serif; font-size: 10.5pt; line-height: 1.5;
    color: #14161a; margin: 0; }
  .wordmark { font-family: 'Helvetica Neue', Arial, sans-serif; font-weight: 700;
    letter-spacing: 0.34em; font-size: 12pt; color: #14161a; }
  h1 { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 15pt; letter-spacing: 0.06em;
    margin: 6pt 0 2pt; }
  .rule { border: 0; border-top: 1.5pt solid #14161a; margin: 10pt 0 16pt; }
  h2 { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 10.5pt; letter-spacing: 0.08em;
    text-transform: uppercase; margin: 18pt 0 4pt; }
  p { margin: 0 0 8pt; text-align: justify; }
  .lead { font-weight: 600; }
  table.fees { width: 100%; border-collapse: collapse; margin: 8pt 0 4pt; font-size: 10pt; }
  table.fees th { text-align: left; font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt;
    letter-spacing: 0.1em; text-transform: uppercase; color: #4a4f57; border-bottom: 1pt solid #14161a;
    padding: 4pt 6pt; }
  table.fees td { padding: 5pt 6pt; border-bottom: 0.5pt solid #cfd3d9; }
  table.fees td.rate { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  /* Force the execution block onto its own final page so the Documenso signature/date fields
     (percent-positioned on the last page) land deterministically regardless of body length. */
  .sig-wrap { margin-top: 26pt; page-break-inside: avoid; page-break-before: always; }
  .sig-grid { width: 100%; border-collapse: separate; border-spacing: 0; }
  .sig-grid td { width: 50%; vertical-align: top; padding-right: 24pt; }
  .sig-party { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8pt; letter-spacing: 0.12em;
    text-transform: uppercase; color: #4a4f57; margin-bottom: 14pt; }
  .sig-line { border-bottom: 1pt solid #14161a; height: 30pt; margin-bottom: 3pt; }
  /* Typed-name signature for the pre-signing party. Italic serif — NOT a cursive system font:
     DocRaptor/Prince blocks filesystem font access, so named script fonts trigger a 422. */
  .sig-rs { font-family: Georgia, 'Times New Roman', serif; font-style: italic; font-weight: 600;
    font-size: 18pt; line-height: 30pt; padding-left: 4pt; color: #1b3a8a; }
  .sig-field { font-size: 8pt; color: #4a4f57; font-family: 'Helvetica Neue', Arial, sans-serif; }
  .sig-val { font-size: 10pt; }
  .anchor { color: #ffffff; font-size: 7pt; }   /* Documenso text anchors — invisible to the reader */
</style>
</head>
<body>
  <div class="wordmark">RARE STRUCTURE</div>
  <h1>STRATEGIC ORIGINATION MANDATE</h1>
  <hr class="rule" />

  <p class="lead">This Strategic Origination Mandate (the &ldquo;Agreement&rdquo;) is entered into as
  of «EFFECTIVE_DATE», by and between Rare Structure LLC (&ldquo;Rare Structure&rdquo;) and
  «CLIENT_NAME», the undersigned institutional fund partner (the &ldquo;Client&rdquo;).</p>

  <h2>1. Scope of Mandate &amp; Routing Protocol</h2>
  <p>Rare Structure deploys specialized data-engineering infrastructure and a proprietary sourcing
  engine to identify and originate off-market deal flow. Client engages Rare Structure on a buy-side
  basis to route proprietary acquisition and investment opportunities within Client&rsquo;s designated
  investment verticals.</p>
  <p><strong>Execution Discretion:</strong> Rare Structure retains absolute, sole discretion over the
  methods, data architecture, protocols, and front-facing enterprise networks, brands, or affiliate
  channels utilized to execute this mandate.</p>
  <p><strong>Thesis-Qualified Targets:</strong> An asset or operating corporate entity shall be formally
  classified as a &ldquo;Thesis-Qualified Target&rdquo; upon satisfying either of the following
  conditions: (i)&nbsp;inclusion within a Client-submitted target list or database export
  pre-authorized by the Client, or (ii)&nbsp;explicit affirmative selection by the Client of an
  anonymized proprietary profile presented by Rare Structure.</p>
  <p><strong>Exclusivity Window:</strong> Upon the formal introduction of a Thesis-Qualified Target to
  the Client (whether by corporate name, direct data transmission, or operational communication), the
  Client shall possess an exclusive window of sixty (60) calendar days (the &ldquo;Initial Evaluation
  Period&rdquo;) to engage with the target.</p>

  <h2>2. Milestone-Driven Exclusivity Extensions</h2>
  <p>To prevent the stranding of proprietary target relationships and ensure programmatic deal velocity,
  extensions of the Initial Evaluation Period are strictly driven by operational milestones:</p>
  <p><strong>Milestone 1 (Discovery Extension):</strong> If, within the initial sixty (60) day window,
  the Client conducts an initial management meeting and formally requests baseline financial or
  operational discovery data, the exclusivity window shall automatically extend by an additional sixty
  (60) days (totaling 120 days from initial introduction) to allow for complete underwriting.</p>
  <p><strong>Milestone 2 (Letter of Intent Lock):</strong> Upon the formal execution of a signed Letter
  of Intent (LOI) or binding Indicated Term Sheet between the Client and the Thesis-Qualified Target, the
  exclusivity window shall lock down and automatically extend by an additional ninety (90) days from the
  date of LOI execution to carry the transaction through financial closing.</p>
  <p>If the Client fails to clear a milestone within the specified timeframes, the Client&rsquo;s
  exclusivity regarding that target automatically expires. Rare Structure shall immediately retain the
  unrestricted right to clear and route that target&rsquo;s intent profile and relationship access to
  alternate capital allocators, specialty lenders, or competing investment funds.</p>

  <h2>3. Fees and Sourcing Capacity Allocation</h2>
  <p>As consideration for the infrastructure deployment and strategic routing resources provided under
  this Mandate, the Client shall remit payments according to the following structures:</p>
  <p><strong>Infrastructure Fee:</strong> Client shall pay a non-refundable fee of «MONTHLY_FEE» per
  month, invoiced every three (3) months in advance («QUARTERLY_TOTAL» per three-month billing period),
  commencing immediately upon execution of this Agreement.</p>
  <p><strong>Nature of Fee:</strong> This fee is fully earned by Rare Structure upon receipt and is
  completely non-refundable. The infrastructure fee represents a dedicated sourcing capacity allocation
  within Rare Structure&rsquo;s broader network architecture. This capital underwrites continuous
  data-engineering overhead, infrastructure costs, and the strategic calibration required to map
  corporate intent to the Client&rsquo;s investment mandate. Client acknowledges that Rare Structure
  executes outbound tracking via its own independently operated market brands and networks entirely at
  Rare Structure&rsquo;s sole cost, risk, and discretion. This fee is paid strictly to maintain capacity
  allocation and is entirely independent of specific outreach volumes or final transaction closure.</p>

  <h2>4. Transaction Success Fees</h2>
  <p>Upon the successful completion, closing, or legal execution of any Transaction involving the Client
  (or its affiliates, co-investors, or platform portfolio companies) and a Thesis-Qualified Target
  introduced by Rare Structure, the Client shall pay Rare Structure a Transaction Success Fee.</p>
  <p><strong>Definition of Transaction:</strong> &ldquo;Transaction&rdquo; shall mean any merger,
  acquisition, asset purchase, joint venture, minority equity investment, corporate recapitalization, or
  other combination resulting in the acquisition of control, equity interest, or operational assets of
  the Thesis-Qualified Target.</p>
  <p><strong>Fee Schedule:</strong> The Success Fee shall be calculated based on the aggregate Enterprise
  Value of the Transaction at close, according to the standard institutional scaled formula:</p>
  <table class="fees">
    <thead><tr><th>Transaction Enterprise Value Tier</th><th class="rate">Success Fee Percentage</th></tr></thead>
    <tbody>
      <tr><td>First $1,000,000 of Enterprise Value</td><td class="rate">Five percent (5.0%)</td></tr>
      <tr><td>Second $1,000,000 of Enterprise Value</td><td class="rate">Four percent (4.0%)</td></tr>
      <tr><td>Third $1,000,000 of Enterprise Value</td><td class="rate">Three percent (3.0%)</td></tr>
      <tr><td>Fourth $1,000,000 of Enterprise Value</td><td class="rate">Two percent (2.0%)</td></tr>
      <tr><td>All Enterprise Value Exceeding $4,000,000</td><td class="rate">One and one-half percent (1.5%)</td></tr>
    </tbody>
  </table>
  <p><strong>Payment Terms:</strong> The Success Fee shall be paid entirely in cash via wire transfer
  immediately upon the legal closing of the Transaction, as a non-negotiable condition of closing.</p>

  <h2>5. Client Data Ingestion &amp; IP Protection</h2>
  <p><strong>Permissive Ingestion:</strong> The Client may, at its own option and discretion, provide
  Rare Structure with target lists, internal pipeline logs, or third-party database exports (e.g.,
  CapitalIQ, Grata) to prioritize within Rare Structure&rsquo;s active operational loops. Rare Structure
  is under no contractually binding obligation to pursue, contact, or guarantee connection with any
  client-provided entity.</p>
  <p><strong>Intellectual Property Lock:</strong> Any lookalike models, clustering parameters, derivative
  enrichment data, behavioral intent signals, or analytical taxonomies generated by Rare Structure&rsquo;s
  infrastructure&mdash;regardless of whether they utilize, reference, or ingest Client-provided
  data&mdash;remain the sole, exclusive, and unencumbered intellectual property of Rare Structure.</p>

  <h2>6. Term, Termination, and Tail Protection</h2>
  <p><strong>Term:</strong> This Agreement shall have an initial committed term of six (6) months from
  the date of execution, automatically renewing on a month-to-month basis thereafter. Either party may
  terminate this Agreement upon thirty (30) days&rsquo; written notice following the expiration of the
  initial committed term.</p>
  <p><strong>Tail Protection Clause:</strong> Notwithstanding the expiration of any individual
  target&rsquo;s Evaluation Period, the expiration of this Mandate, or the formal termination of this
  Agreement for any reason, the Client remains legally bound to pay Rare Structure the full Transaction
  Success Fee for any Transaction closed, structured, or executed with any introduced Thesis-Qualified
  Target within twenty-four (24) months following the initial date of introduction (the &ldquo;Tail
  Period&rdquo;).</p>

  <h2>7. Governing Law and Confidentiality</h2>
  <p>This Agreement and all performance hereunder shall be governed by, and construed in accordance with,
  the laws of the State of Delaware, without regard to its conflict of laws principles. Both parties
  explicitly covenant that all strategic mandates, target identities, proprietary operational
  capabilities, and structural transaction discussions remain strictly confidential and protected from
  public disclosure.</p>

  <div class="sig-wrap">
    <table class="sig-grid"><tr>
      <td>
        <div class="sig-party">Rare Structure LLC</div>
        <div class="sig-line"><span class="sig-rs">«RS_NAME»</span></div>
        <div class="sig-field">By: <span class="sig-val">«RS_NAME»</span></div>
        <div class="sig-field">Title: <span class="sig-val">Managing Director</span></div>
        <div class="sig-field">Date: <span class="sig-val">«RS_DATE»</span></div>
      </td>
      <td>
        <div class="sig-party">Client / Institutional Partner</div>
        <!-- Documenso auto-creates the SIGNATURE field at the invisible anchor on this line. -->
        <div class="sig-line"><span class="anchor">«CLIENT_SIG_TOKEN»</span></div>
        <div class="sig-field">By: <span class="sig-val">&nbsp;</span></div>
        <div class="sig-field">Name: <span class="sig-val">«CLIENT_SIGNER_NAME»</span></div>
        <div class="sig-field">Title: <span class="sig-val">«CLIENT_TITLE»</span></div>
        <!-- Documenso auto-creates the DATE field at the invisible anchor after "Date:". -->
        <div class="sig-field">Date: <span class="sig-val"><span class="anchor">«CLIENT_DATE_TOKEN»</span></span></div>
      </td>
    </tr></table>
  </div>
</body>
</html>
"""
