"""Application layer for core-x.

Distinct from ``pipelines/`` (Modal compute workers that ingest/materialize the
Gen-3 data plane): ``apps/`` holds long-running services that *read* the
system-of-record and expose it. First tenant: ``gtm_mcp`` — the unified GTM MCP
gateway (Render Web Service)."""
