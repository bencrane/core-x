"""gtm-mcp — unified GTM MCP gateway.

A global data gateway + action engine for autonomous GTM agents, served as a
Render Web Service (Ohio) over the SSE transport. Reads the Gen-3 R2 sink
(Lance system-of-record) two ways: Lance ``BTREE`` index pushdown for sub-100 ms
point-lookups, and raw DuckDB ANSI SQL for cross-layer audience queries. Built
for multi-dataset extensibility — registering a new dataset is one entry in
``src/database.py:DATASETS`` plus a tool.

Run: ``python -m apps.gtm_mcp.main`` (from the repo root). The package name is
``gtm_mcp`` (underscore) so it is a valid importable module; the Render service
is named ``gtm-mcp``."""
