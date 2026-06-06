# edge_api

Public Anthropic **Managed-Agents edge** service for core-x — the strangler-fig
extraction of the agent-facing surface out of the hq-x monolith. Built in phases;
each surface is copied, verified on edge_api, then swapped one lever at a time with
hq-x left running until the swap is proven.

## What it hosts (by phase)

| Phase | Surface | Source (hq-x) | Auth boundary |
|------|---------|---------------|---------------|
| 0 | skeleton — `/`, `/healthz`, `/v1/_authcheck` | — | — |
| 1 | `/mcp/trigger/` | `app/mcp/trigger.py` + `app/services/trigger_dev_client.py` | MCP bearer (vault) |
| 2 | `/mcp/lob/` | `app/mcp/lob.py` + `app/providers/lob/client.py` | MCP bearer (vault) |
| 3 | `/api/v1/agent-runs/*` (SSE) | `app/routers/agent_runs_v1.py` + `app/services/managed_agents.py` | service token |
| 4 | `/internal/gtm/.../run-step` | `app/routers/internal/gtm_pipeline.py` (+ the gtm_pipeline subsystem) | service token / trigger secret |

`dmaas` stays on hq-x — it is welded to the DMaaS Postgres subsystem, not a thin mount.

## Two auth boundaries

- **MCP mounts ← Anthropic's platform:** `DMAAS_MCP_BEARER_TOKEN`, injected by the
  agent's Anthropic vault (scoped by `mcp_server_url`). Gate: [`src/mcp_bearer.py`](src/mcp_bearer.py).
- **agent-runs + pipeline ← platform-api BFF / Trigger.dev:** `EDGE_API_SERVICE_TOKEN`,
  constant-time compared. Gate: [`src/service_token.py`](src/service_token.py).

## Secrets (Doppler `core-x/prd`)

Reads the **same** config hq-x does — incl. `HQX_DB_URL_POOLED` (identical Postgres),
`ANTHROPIC_MANAGED_AGENTS_API_KEY`, `MANAGED_AGENT_ID_GTM`, `MANAGED_ENVIRONMENT_ID_GTM`,
`MANAGED_VAULT_ID_GTM_MCP`, `DMAAS_MCP_BEARER_TOKEN`, `LOB_API_KEY_TEST`. Phase 0 adds
`EDGE_API_SERVICE_TOKEN`; Phase 1 adds `TRIGGER_SECRET_KEY` (Trigger project
`proj_pakdcffjbeiwcixcoepb`).

## Run

```bash
# local (from the core-x repo root)
doppler run -p core-x -c prd -- python -m apps.edge_api.main
curl -fsS localhost:8080/healthz
```

## Deploy

Public **Railway** service in its own new project (not `rare-structure-hq`):
build context = repo root, `RAILWAY_DOCKERFILE_PATH=apps/edge_api/Dockerfile`,
`DOPPLER_TOKEN` scoped `core-x/prd`. Public domain → `<edge-host>`. Render (Python
buildpack) is an equivalent alternative — see `requirements.txt`.
