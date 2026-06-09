# catalyst_api

Gen-3 read API — a **core-x shared gateway** over the committed R2 system-of-record
sink. Resolves a web **domain** to its **US federal award profile**, and a SAM.gov
**UEI** to its entity surfaces (SAM profile, active/past contracts, overview), via
native Lance `BTREE` point-lookups. Read-only — it never writes a dataset (see
`ARCHITECTURE.md`: a gateway reads the committed plane; pipelines materialize it).

This is the Gen-3 replacement for the deprecated data-engine-x (DEX) REST layer.
It is **core-x-owned infrastructure** — deployed as its own `catalyst-api` Railway
project, **not** nested in any product. Multiple product BFFs consume it
cross-project (government-contracted `platform-app`, rare-structure `platform-api`,
…), so it is reachable on a public Railway domain and every `/api/v1` route is
gated by the `CATALYST_API_TOKEN` bearer (the token, enforced fail-closed at boot,
is the auth boundary — not network isolation).

## Lookup chain

```
domain ──BTREE(domain_norm)──► firmographics_blitz ──uei──► contractor_award_summary  (BTREE recipient_uei)
                                                          └─► usaspending/award_search (BTREE recipient_uei, opt-in)
```

1. `firmographics_blitz.domain_norm` (BTREE) → company + SAM.gov `uei`.
2. `contractor_award_summary.recipient_uei` (BTREE, 1 row/recipient) → the federal
   award rollup. The load-bearing sub-100 ms anchor.
3. `usaspending/award_search.recipient_uei` (BTREE, 78M rows) → prime award line
   items. A separate opt-in detail call (`?awards=N`), bounded + projected.

Datasets are opened per request (never cached) so the gateway always reflects the
latest committed Lance version — the gtm_mcp freshness convention. A request is a
manifest GET + a handful of BTREE index/data GETs; **sub-100 ms holds in-region**
(the Railway region is co-located near R2). The figure is network-bound, not algorithmic: from
a cross-region dev laptop the same lookup is ~2 s of round-trips. The 78M-row
`award_search` detail path (`?awards=N`) is materially heavier and is why it is
opt-in rather than on the default profile response.

## Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/healthz` | open | Liveness + R2 reachability (`503` if the sink is unreachable) |
| `GET` | `/` | open | Service info |
| `GET` | `/api/v1/award-profile/{domain}` | Bearer | Company + federal award profile |
| `GET` | `/api/v1/award-profile/{domain}?awards=N` | Bearer | …plus up to N (≤100) prime award line items |

**Semantics**
- `404` — domain resolves to no known company.
- `200` with `awardProfile: null`, `isFederalContractor: false` — company known,
  no federal contracting footprint (valid, common).
- `400` — input is not a well-formed domain.

**Response** (`{ data: … }` envelope, camelCase):

```jsonc
{
  "data": {
    "domain": "rotochopper.com",
    "matched": true,
    "isFederalContractor": true,
    "company": {
      "name": "Rotochopper, Inc", "uei": "WBBSNBA9GBL7",
      "website": "…", "industry": "Paper and Forest Product Manufacturing",
      "employeeSizeBand": "…", "foundedYear": 1992,
      "hqCity": "…", "hqState": "Minnesota", "hqRegion": "…"
    },
    "awardProfile": {
      "totalCombinedObligated": 2929171.0, "lifetimePrimeObligated": 2929171.0,
      "primeTotalAwards": 4, "primeActiveAwards": 1, "primeClosedAwards": 3,
      "contractDollars": 2929171.0, "grantDollars": 0.0, "otherDollars": 0.0,
      "primaryNaics": "333120", "primaryPsc": "3040",
      "firstAwardDate": "…", "mostRecentActionDate": "2025-04-30",
      "topAgencies": [{ "name": "Department of Agriculture", "dollars": 2929171.0 }],
      "asOfDate": "2026-06-03"
    },
    "recentAwards": null
  }
}
```

## Security boundary

Every `/api/v1` route requires `Authorization: Bearer <CATALYST_API_TOKEN>`,
compared in constant time (`hmac.compare_digest`). Each consuming BFF presents the
same secret on every proxied request. `/healthz` and `/` stay open for platform
liveness probes. The gate is **fail-closed**: when `CATALYST_API_TOKEN` is unset in
a deployed environment (`RAILWAY_ENVIRONMENT` set, or `CATALYST_REQUIRE_AUTH`) the
service refuses to start; only a bare local dev box warns-and-allows.

## Env vars

Injected via Doppler (`core-x/prd`) locally and on the `catalyst-api` Railway
service (via the `DOPPLER_TOKEN` it holds).

| Key | Description |
|-----|-------------|
| `CATALYST_API_TOKEN` | Operator bearer token the BFF must present |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | R2 credentials for the Lance reader |
| `R2_ENDPOINT` *(or `R2_ACCOUNT_ID`)* | R2 endpoint, or account id to derive it |
| `FIRMOGRAPHICS_LANCE_URI` | override (default `s3://data-sink/active/firmographics_blitz/`) |
| `CONTRACTOR_AWARD_SUMMARY_LANCE_URI` | override (default `s3://data-sink/active/contractor_award_summary/`) |
| `AWARD_SEARCH_LANCE_URI` | override (default `s3://data-sink/active/usaspending/award_search/`) |
| `PORT` / `HOST` | Railway service pins `8080` / `::`; default `8080` / `::` locally |

## Local dev

```bash
# from the core-x repo root
pip install -r apps/catalyst_api/requirements.txt

# boot against the live sink (Doppler supplies R2_* + CATALYST_API_TOKEN)
doppler run --project core-x --config prd -- python -m apps.catalyst_api.main

# query
curl -s -H "Authorization: Bearer $CATALYST_API_TOKEN" \
  http://localhost:8080/api/v1/award-profile/rotochopper.com | jq .
```

## Tests

```bash
pytest apps/catalyst_api/tests          # pure composition + normalization, no network
```

## Deployment

Its own **`catalyst-api` Railway project** (core-x-owned, env `production`) with a
**public domain**, gated by the bearer token. A shared gateway consumed by product
BFFs in *other* Railway projects cannot use private networking
(`*.railway.internal` resolves per-project), so consumers reach it over HTTPS with
`CATALYST_API_TOKEN`.

Build/run is the `apps/catalyst_api/Dockerfile` (Doppler CLI + `doppler run --
python -m apps.catalyst_api.main`), context = core-x repo root.

Create the service (GitHub-connected, auto-redeploys on `core-x` main):

```bash
# from a dir linked to the catalyst-api Railway project (env: production)
railway add --service catalyst-api --repo bencrane/core-x \
  --variables "RAILWAY_DOCKERFILE_PATH=apps/catalyst_api/Dockerfile" \
  --variables "HOST=0.0.0.0" \
  --variables "PORT=8080" \
  --variables "DOPPLER_TOKEN=<core-x/prd service token>"
railway domain --port 8080   # generate the public *.up.railway.app domain
```

`DOPPLER_TOKEN` is a Doppler service token scoped to `core-x/prd`
(`doppler configs tokens create catalyst-railway --project core-x --config prd --plain`);
`doppler run` then injects `R2_*` + `CATALYST_API_TOKEN` at startup.

Consumers point at the public URL:

| Consumer | Env |
|----------|-----|
| government-contracted `platform-app` | `CATALYST_API_URL=https://<domain>` + `CATALYST_API_TOKEN` |
| rare-structure `platform-api` | its catalyst base-URL var → `https://<domain>` (+ token) |

> **Bind:** the public service sets `HOST=0.0.0.0` — Railway's public edge reaches
> the container over IPv4, so an IPv6-only `::` bind returns `502 Application
> failed to respond`. (`HOST=::` is only correct for a *private*-network deploy,
> Railway's private net being IPv6-only — the legacy rare-structure variant.)
