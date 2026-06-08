# Proposal Experience — Native React Presentation + Documenso for Signature

**Implementation spec / handoff.** Self-contained. Target: a web app (Next.js App Router
assumed; adapt as needed) where a prospect opens a link, reads a beautiful, web-native
proposal rendered in React, and signs it — with the legally-binding signature handled by
Documenso, **not** rebuilt in-house.

Facts marked **[VERIFIED]** were confirmed against Documenso Cloud (`app.documenso.com`)
at time of writing. Facts marked **[PER-DOCS]** follow Documenso's API contract and should
be re-confirmed against your instance/plan before relying on them.

---

## 1. Core principle

**Separate presentation from signature, and drive both from a single structured data
source.**

- The proposal **content** is structured data (`{ contact, lineItems[], total, notes }`) —
  NOT a PDF. You render it as React for the web experience.
- The **legal signature** is owned by Documenso. You never build a signature pad, never
  store a signature image, never generate an audit trail. Documenso captures the signature,
  seals the document, and issues the signing certificate.
- The document Documenso signs is a **PDF generated from the same structured data**, so what
  the prospect reads (React) matches what they legally sign (PDF). One source of truth → two
  renderings.

Why this split: the signature image is trivial, but the **legal weight** (ESIGN/UETA:
identity, intent, consent, timestamp, IP, tamper-evident seal + certificate) is the entire
value of an e-sign provider. Capturing a signature in your own UI and storing a PNG gives
you a picture with no legal standing. Documenso's signing only happens on its own signing
surface (hosted `/sign/<token>` page or the embedded iframe) — there is intentionally **no**
public API to "submit a signature image and mark it signed," because that would let anyone
forge a completion.

---

## 2. Components & responsibilities

| Layer | You build | Documenso owns |
|---|---|---|
| Proposal content | Structured data model + React renderer (the web-native proposal) | — |
| PDF | Generate a PDF from the same data (the legal document) | Stores/serves it |
| Signature capture | Trigger it (embed or redirect) + listen for completion | Signature pad, capture, **sealing**, **certificate**, audit trail |
| State | Track proposal status (draft→sent→opened→signed→completed) from webhooks | Emits webhook events |
| Signed artifact | Pull + store the final signed PDF | Generates the sealed PDF + cert |

---

## 3. Canonical data model

```ts
// One source of truth. Renders to React (web) AND to PDF (legal doc).
interface Proposal {
  id: string;                       // your deal/proposal id (the URL slug)
  contact: {
    name: string;
    company: string;
    email: string;                  // becomes the Documenso recipient
    date: string;                   // ISO or display string
  };
  brand?: string;                   // wordmark / theme label
  lineItems: Array<{
    title: string;
    description: string;
    price: number;                  // minor logic in cents recommended; display in USD
  }>;
  total: number;
  notes?: string;

  // Documenso linkage (populated when the doc is created):
  documensoDocumentId?: number;     // numeric document id
  signingToken?: string;            // recipient token — drives the embed
  status: ProposalStatus;           // see §8
  generatedAt?: string;
  signedPdfUrl?: string;            // your stored copy of the sealed PDF
}

type ProposalStatus =
  | 'draft' | 'sent' | 'opened' | 'signed' | 'completed' | 'rejected' | 'voided';
```

The frontend page only strictly needs `contact`, `lineItems`, `total`, `notes`,
`signingToken`, and `status`. Everything else is backend bookkeeping.

---

## 4. End-to-end flow

1. **Create proposal** (internal/admin action) → persist structured `Proposal` (status `draft`).
2. **Generate the legal PDF** from the proposal data (HTML→PDF; see §5.2).
3. **Create the Documenso document** from that PDF, with one recipient (the prospect) as a
   `SIGNER`, a signature field placed, and **email delivery suppressed** (you're embedding,
   not letting Documenso email them). Capture `documentId` + recipient `token`. Status → `sent`.
4. **Send the prospect your link**: `https://yourapp.com/proposal/<id>` (your link, not
   Documenso's).
5. **Prospect opens the React page** → backend returns the structured proposal + `signingToken`.
   Page renders the proposal natively (dark, branded, web — no white PDF).
6. **Prospect clicks "Review & sign"** → the page mounts Documenso's `EmbedSignDocument`
   with the token (or full-screen redirects to `/sign/<token>`). Documenso shows the PDF +
   captures the signature.
7. **Documenso fires webhooks** (`DOCUMENT_OPENED`, `DOCUMENT_SIGNED`, `DOCUMENT_COMPLETED`).
   Your webhook handler verifies + advances status.
8. **On completion** → pull the sealed signed PDF from Documenso, store its URL, mark
   `completed`, trigger downstream (Stripe checkout, CRM update, etc.).

---

## 5. Backend spec

### 5.1 Proposal data API
`GET /api/proposal/:id` → returns the structured `Proposal` (the frontend's data source).
This is the contract the React page consumes:

```jsonc
{
  "id": "deal_123",
  "brand": "Revenue Activation",
  "contact": { "name": "Sarah Johnson", "company": "Acme Corporation",
               "email": "sarah@acme.io", "date": "2026-02-09" },
  "lineItems": [
    { "title": "Sales Pipeline Optimization", "price": 750000,
      "description": "Comprehensive audit and optimization..." },
    { "title": "Outbound Campaign Setup", "price": 500000,
      "description": "Design and launch of targeted outbound..." }
  ],
  "total": 1250000,
  "notes": "Q1 2026 consulting engagement.",
  "signingToken": "phn-xxxxxxxxxxxxx",
  "status": "sent"
}
```
Return `signingToken` only to the intended recipient context. The token is a bearer
credential for signing — treat the proposal URL as sensitive (unguessable id, optionally an
auth gate).

### 5.2 PDF generation (the legal document)
Render the **same** proposal data to HTML, then to PDF. Options: DocRaptor (Prince), Puppeteer/
Playwright, or `@react-pdf/renderer`. Keep the PDF visually faithful to the legal terms; it
does not have to match the React styling pixel-for-pixel, but its **content** must match
(line items, prices, total, terms). This PDF is the artifact Documenso signs.

### 5.3 Create the Documenso document
Two viable paths — pick based on how dynamic your layout is:

**(A) Generate-PDF-then-upload** — best when line items / lengths vary per deal.
**[PER-DOCS]** Documenso v1:
1. `POST /api/v1/documents` with `{ title, recipients: [{ name, email, role: "SIGNER" }] }`
   → response includes the document id and a presigned **upload URL**.
2. `PUT <uploadUrl>` the PDF bytes (`application/pdf`).
3. `POST /api/v1/documents/{id}/fields` → place a `SIGNATURE` field for the recipient
   (`{ type: "SIGNATURE", recipientId, pageNumber, pageX, pageY, pageWidth, pageHeight }`).
4. `POST /api/v1/documents/{id}/send` with `{ sendEmail: false }` (you're embedding).
5. Read the recipient `token` from the document (see §5.6 / §7).

**(B) Documenso Template + prefill** — best when the proposal layout is fixed and only data
varies. Author a template once in Documenso's UI (place the signature field on the role),
then per-deal instantiate from the template with the recipient + prefilled fields. **[PER-DOCS]**
Cleanest on a Platform plan / self-host where the v2 `template/use` endpoint is available.

> **[VERIFIED] Plan caveat:** On the test Cloud account, the **v1 API works** but **v2 endpoints
> return `404 NOT_FOUND`**. So on a non-Platform Cloud plan, use the **v1** create flow (A).
> v2 (`/api/v2/documents`, `/api/v2/template/use`) and the cleaner template flow may require a
> Platform plan or self-hosting. Confirm your plan before choosing (B).

### 5.4 Token storage
Persist `documensoDocumentId` + `signingToken` on the proposal row. The token is stable for a
recipient until the document is completed/voided.

### 5.5 Webhook handler
`POST /api/webhooks/documenso` — Documenso calls this on signing events.

**[VERIFIED-via-reference-impl] Events:** `DOCUMENT_SENT`, `DOCUMENT_OPENED`, `DOCUMENT_SIGNED`,
`DOCUMENT_COMPLETED`, `DOCUMENT_REJECTED`, `DOCUMENT_CANCELLED`. Payload shape:
`{ event, payload: { id, externalId, status, recipients: [{ email, role, token, signingStatus, signedAt }], ... }, createdAt }`.

**Verification:** Documenso sends the configured secret **verbatim** in the
`X-Documenso-Secret` header (it is a shared secret, **not** an HMAC). Compare it against your
`DOCUMENSO_WEBHOOK_SECRET` with a constant-time comparison. Reject with 401 on mismatch.

**Idempotency:** key off the document id + event; signing events can be delivered more than
once. Make status transitions monotonic (don't regress `completed` → `signed`).

**On `DOCUMENT_COMPLETED`:** mark `completed`, then pull the signed PDF (§5.6) and fire
downstream effects.

### 5.6 Read document / retrieve signed PDF
- `GET /api/v1/documents/{id}` **[VERIFIED]** → `{ title, status, recipients: [{ id, email,
  role, token, signingStatus, signedAt }] }`. Use this to (re)read the recipient `token` and
  status.
- `GET /api/v1/documents/{id}/download` **[PER-DOCS]** → the sealed, signed PDF (after
  completion). Store your own copy.

---

## 6. Frontend spec

### 6.1 Native React proposal renderer
A data-driven component that renders the `Proposal` as web-native UI (your design system —
dark theme, branded). No iframe, no PDF for the reading experience. Sections: brand wordmark,
title + "Prepared for", contact meta, scope-of-work line-item cards (title / description /
price), total banner, notes, and a "Review & sign" CTA.

> Tip: if your CSS pipeline is fragile, inline styles guarantee the look renders regardless of
> build config. Otherwise use your design system.

### 6.2 Signature step — `@documenso/embed-react`
Install `@documenso/embed-react`. The signature step renders Documenso's signing surface; you
capture nothing. **[VERIFIED]** `EmbedSignDocument` loads an iframe to
`{host}/embed/sign/{token}` and works for a valid recipient token **without a paid plan**
(the recipient signing surface is public by design).

**[VERIFIED] Exact component API (`@documenso/embed-react@0.4.0`):**
```ts
type EmbedSignDocumentProps = {
  token: string;                 // recipient signing token (required)
  host?: string;                 // e.g. "https://app.documenso.com" or your self-host URL
  className?: string;
  css?: string;                  // raw CSS injected into the signing surface
  cssVars?: CssVars;             // themed variables — see below (this is how you match your UI)
  darkModeDisabled?: boolean;
  name?: string;                 // prefill signer name
  lockName?: boolean;
  allowDocumentRejection?: boolean;
  additionalProps?: Record<string, string | number | boolean>;
  onDocumentReady?: () => void;
  onDocumentCompleted?: (d: { token: string; documentId: number; recipientId: number }) => void;
  onDocumentError?: (error: string) => void;
  onDocumentRejected?: (d: { token: string; documentId: number; recipientId: number; reason: string }) => void;
};
```

**Theming (`cssVars`)** — **this is the answer to "make the signature step match my dark UI."**
Available variables: `background`, `foreground`, `muted`, `mutedForeground`, `popover`,
`popoverForeground`, `card`, `cardBorder`, `cardBorderTint`, `cardForeground`, `fieldCard`,
`fieldCardBorder`, `fieldCardForeground`, `widget`, `widgetForeground`, `border`, `input`,
`primary`, `primaryForeground`, `secondary`, `secondaryForeground`, `accent`,
`accentForeground`, `destructive`, `destructiveForeground`, `ring`, `radius`, `warning`.
You can theme the signing **chrome/widgets** to match a dark, branded look. **Caveat:** the
**document pages themselves remain the rendered PDF** (white sheet) — `cssVars` styles the UI
around it, not the PDF content. To avoid the white-PDF feel entirely, keep the *reading*
experience in your React renderer and treat the embed purely as the signature action.

```tsx
'use client';
import { EmbedSignDocument } from '@documenso/embed-react';

function SignatureStep({ token, onDone }: { token: string; onDone: () => void }) {
  return (
    <EmbedSignDocument
      token={token}
      host="https://app.documenso.com"
      darkModeDisabled={false}
      cssVars={{
        background: '#0a0a0f', foreground: '#e7e9ee',
        card: '#0d0d12', cardForeground: '#e7e9ee', cardBorder: 'rgba(255,255,255,0.1)',
        primary: '#5b8cff', primaryForeground: '#ffffff',
        accent: '#5b8cff', radius: '0.75rem',
      }}
      onDocumentCompleted={() => onDone()}
      onDocumentError={(e) => console.error('sign error', e)}
    />
  );
}
```

### 6.3 Completion handling
- `onDocumentCompleted` → optimistic UI ("Signed — thank you"), then `router.refresh()` to
  re-pull status. **Do not** treat the client callback as authoritative for business logic —
  it can be spoofed; the **webhook** (§5.5) is the source of truth.
- Three ways to surface the signature step, in increasing nativeness:
  1. **Inline embed** behind a "Review & sign" button (simplest; iframe in your page).
  2. **Themed inline embed** via `cssVars` (chrome matches your UI; PDF still PDF).
  3. **Full-screen redirect** to `{host}/sign/{token}` (cleanest hand-off, no nested iframe).

### 6.4 Other embed components (optional)
`@documenso/embed-react` also exports `EmbedDirectTemplate` (recipient self-serves from a
direct-link template — they enter email/name and sign in one shot; props include `email`,
`lockEmail`, `onFieldSigned`) and `unstable_*` create/update builders. For pre-created,
per-recipient proposals, `EmbedSignDocument` is the right one; `EmbedDirectTemplate` fits a
"public link, fill-then-sign" model.

### 6.5 Embed approach vs. signing-token (hosted) approach — concrete

This is the most important integration decision and the easiest to get confused, because
**both approaches use the exact same recipient `token`.** The "embed vs token" naming does
NOT refer to two different credentials — it refers to **how you deliver Documenso's signing
surface to the prospect, and how you learn it completed.** In fact the embed's iframe URL
*is* a token URL: `/embed/sign/{token}`. So precisely:

- **Embed approach** = render Documenso's signing surface **inside your page** as an iframe,
  via the `@documenso/embed-react` `EmbedSignDocument` component, pointed at
  `{host}/embed/sign/{token}`.
- **Signing-token (hosted) approach** = take the same `token` and send the prospect to
  Documenso's **hosted full-page** signing experience at `{host}/sign/{token}` (a redirect or
  a link). No embed package, no iframe.

#### Side-by-side

| | **Embed** (`EmbedSignDocument`) | **Hosted token** (`/sign/{token}`) |
|---|---|---|
| URL it hits | `{host}/embed/sign/{token}` (in an iframe) | `{host}/sign/{token}` (full page) |
| Client dependency | `@documenso/embed-react` (required) | **none** — it's just a URL |
| Where signing happens | **Inside your app** (your domain, your layout) | Documenso's hosted page (their domain / a new tab) |
| Theming control | **Full** — `cssVars`, `css`, `darkModeDisabled` on the chrome | **None** — Documenso's stock UI/branding |
| Completion signal | Real-time JS callbacks (`onDocumentCompleted` / `onDocumentError` / `onDocumentRejected`) via postMessage **and** webhook | **Webhook only** (+ optional post-sign redirect URL) |
| Prefill / lock signer name | `name` / `lockName` props | Not via the URL (set on the document) |
| Prospect leaves your app | No | Yes (full-page) or new tab |
| Iframe constraints | Subject to third-party-storage/cookie rules, CSP `frame-ancestors`, mobile iframe quirks | **None** — maximally robust everywhere |
| Plan requirement | Embedding can be **Platform-gated** on some Cloud tiers — **[VERIFIED]** working on the tested account for `/embed/sign/<token>` | Works on **any** plan (standard recipient page); **[VERIFIED]** `/sign/<token>` → 200 |
| Best for | Keeping the prospect in a branded, in-app flow ("read the React proposal, sign right below") | Maximum reliability, mobile, zero-dependency, simplest hand-off |

#### Concrete: embed approach
```tsx
'use client';
import { EmbedSignDocument } from '@documenso/embed-react';

<EmbedSignDocument
  token={token}                          // recipient token, e.g. "phn-xxxx"
  host="https://app.documenso.com"       // MUST match the instance the doc was created on
  cssVars={{ background: '#0a0a0f', card: '#0d0d12', primary: '#5b8cff', radius: '0.75rem' }}
  onDocumentCompleted={() => router.refresh()}   // client signal (NOT authoritative)
  onDocumentError={(e) => console.error(e)}
/>
```

#### Concrete: signing-token (hosted) approach
```tsx
// No package. The token is the whole integration.
const signUrl = `${process.env.NEXT_PUBLIC_DOCUMENSO_APP_URL}/sign/${token}`;

// full-page hand-off:
//   window.location.href = signUrl;
// or a link / new tab:
<a href={signUrl} target="_blank" rel="noopener noreferrer">Review &amp; sign →</a>
```
With hosted mode you get **no client callback** — your **webhook** (§5.5) is the *only* way to
learn the document was signed/completed. **[PER-DOCS]** Documenso supports a post-signing
**redirect URL** configured on the document/recipient; that's how you bring the signer back to
your app after a hosted signing.

#### They compose — recommended production pattern
Use the embed, but fall back to the hosted token URL if the iframe fails to load (blocked
third-party storage, CSP, mobile). This is exactly the pattern in the reference
`DocumentSignPanel`:
```tsx
const [embedError, setEmbedError] = useState(false);
return embedError
  ? <a href={`${host}/sign/${token}`} target="_blank" rel="noopener noreferrer">Open signing in a new tab →</a>
  : <EmbedSignDocument token={token} host={host}
      onDocumentError={() => setEmbedError(true)}
      onDocumentCompleted={onDone} />;
```

#### Three precise clarifications
1. **Don't hand-roll a raw iframe** to `/embed/sign/{token}` to avoid the package. You *can*,
   but you lose two things the package provides via a postMessage handshake: (a) `cssVars`/`css`
   theming injection, and (b) the completion/error/rejected callbacks. If you want embedding,
   use `@documenso/embed-react`. If you want zero dependencies, use the **hosted** redirect —
   not a bare iframe.
2. **Webhook is authoritative in both modes.** The embed's `onDocumentCompleted` is a
   convenience (it's client-side and can be spoofed or missed if the tab closes mid-flight);
   the hosted mode has no callback at all. Business logic (mark `completed`, charge, provision)
   keys off the **webhook**, never the client signal.
3. **Neither mode changes the document into HTML.** In both, the thing being signed is the PDF.
   Embed lets you theme the *surrounding* chrome; hosted shows Documenso's full branding.
   Neither makes the document page non-white. The "web-native" reading experience is your React
   renderer (§6.1) — the signing surface, in either mode, is Documenso rendering the PDF.

---

## 7. Documenso integration reference (verified facts)

- **Instance:** Documenso Cloud `https://app.documenso.com` (or your self-host base URL).
  No custom API URL was set in the reference deployment → Cloud.
- **[VERIFIED] API version:** v1 works on the Cloud account tested; **v2 returns
  `404 NOT_FOUND`** there. Use v1 unless on a Platform plan / self-host.
- **[VERIFIED] Auth:** `Authorization: <API_KEY>` (the raw key as the header value) — both
  raw and `Bearer <KEY>` were accepted on reads. Key format: `api_…`. Store as
  `DOCUMENSO_API_KEY`. **Server-side only** — never ship it to the browser.
- **[VERIFIED] Endpoints exercised:** `GET /api/v1/documents` (list, paginated, `totalPages`);
  `GET /api/v1/documents/{id}` (→ `recipients[].token`, `signingStatus`, `status`).
- **[VERIFIED] Recipient signing URLs:** public `GET {host}/sign/{token}` → 200; embeddable
  `GET {host}/embed/sign/{token}` → 200 with real signing content, **no paywall**.
- **[VERIFIED] Embed component URL pattern:** `@documenso/embed-react` builds
  `{host}/embed/sign/{token}` (and `/embed/direct/…` for direct templates).
- **[PER-DOCS] Create / fields / send / download:** v1 `POST /api/v1/documents`,
  `POST /api/v1/documents/{id}/fields`, `POST /api/v1/documents/{id}/send` (`sendEmail:false`),
  `GET /api/v1/documents/{id}/download`. Confirm exact request/response against your version.
- **Webhook auth:** `X-Documenso-Secret` header = your configured secret verbatim (shared
  secret, not HMAC); constant-time compare.

Required env: `DOCUMENSO_API_KEY`, `DOCUMENSO_API_URL` (default `https://app.documenso.com`),
`DOCUMENSO_WEBHOOK_SECRET`. For the browser embed you may also expose
`NEXT_PUBLIC_DOCUMENSO_APP_URL` (the host) — that's a public URL, not the key.

> **Host consistency:** the `host` you pass to `EmbedSignDocument` MUST be the same Documenso
> instance the document was created on. A doc created on a self-host but embedded against
> `app.documenso.com` (or vice-versa) will not resolve.

---

## 8. Status state machine

```
draft ──create+send──▶ sent ──opened webhook──▶ opened
  │                      │                          │
  │                      └──────────────┬───────────┘
  │                                     ▼
  │                          DOCUMENT_SIGNED ──▶ signed
  │                                     │
  │                          DOCUMENT_COMPLETED ──▶ completed ──▶ (pull signed PDF, downstream)
  │
  ├── DOCUMENT_REJECTED ──▶ rejected
  └── (admin void)       ──▶ voided
```
Single-signer proposals: `signed` and `completed` often coincide. Transitions are driven by
webhooks (authoritative), never by the client embed callback alone.

---

## 9. Gotchas & constraints

- **Don't capture the signature yourself.** No custom signature pad. Documenso captures,
  seals, and certifies. Building your own = a legally worthless PNG. (If you ever truly need a
  native signature pad, you must also rebuild the entire ESIGN/UETA sealing + audit — i.e.
  replace Documenso. Not recommended.)
- **The PDF stays a PDF.** `cssVars` themes the embed chrome, not the document page. The
  "web-native" feel comes from your React renderer being the thing the prospect *reads*; the
  embed is only the *sign* action.
- **Content parity:** generate the signed PDF from the **same** structured data as the React
  view so the legal artifact matches what they read.
- **v2 is plan-gated on Cloud** (404 on the tested account). Default to v1.
- **API key is server-only.** All Documenso REST calls happen on your backend. The browser
  only ever sees the recipient `token` and the public `host`.
- **Webhook is the source of truth**, not `onDocumentCompleted` (client callback is spoofable).
- **Email suppression:** create/send with `sendEmail: false` so Documenso doesn't email the
  prospect a competing signing link — you control delivery via your own proposal URL.
- **Token sensitivity:** the proposal URL exposes a signing token to whoever has the link;
  use unguessable ids and consider an auth/identity gate for higher-value deals.

---

## 10. Open decisions for the implementing team

1. **PDF generation engine** (DocRaptor vs Puppeteer vs react-pdf) and where it runs.
2. **Create path:** generate-and-upload (A) vs Documenso template + prefill (B) — depends on
   plan (v2 availability) and how dynamic the layout is.
3. **Signature surface:** inline embed vs themed embed vs full-screen redirect.
4. **Auth on the proposal URL:** open unguessable link vs identity-gated.
5. **Downstream on completion:** Stripe checkout? CRM/deal-stage update? counter-signature?
6. **Self-host vs Cloud Platform** if you need v2 / templates / advanced embedding.

---

### Reference implementations (patterns, if accessible)
- A clean provider abstraction + webhook (`X-Documenso-Secret` verify, event normalization,
  embed `EmbedSignDocument` with completed/error/fallback states) exists in a sibling repo's
  `lib/signature/` + `components/.../DocumentSignPanel.tsx` — good model for the provider
  interface and webhook handling.
- A backend HTML→PDF→Documenso pipeline exists in a sibling repo's `forms_api.py` — good
  model for the generation step (note: its create call used a non-standard v2 shape; prefer
  the v1 flow in §5.3).
