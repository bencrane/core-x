"""Serving-tier precompute pipelines — materialize warm artifacts the BFFs serve
in-memory, so the request path never opens Lance or runs DuckDB."""
