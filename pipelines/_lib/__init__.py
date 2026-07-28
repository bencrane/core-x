"""Shared pipeline libraries (cross-directive helpers).

Home of infrastructure that more than one ingest imports verbatim. First tenant:
``rate_governor`` — the token-bucket + warm-up + circuit-breaker + path-checkpoint
governor every polite-crawl directive routes its network calls through.
"""
