"""In-session classification harness for naics_psc_labor_profile.

Captures the durable, parameterized version of the ephemeral /tmp scripts that drive the
zero-API-spend, in-session Opus 4.8 / xhigh classification. See RUNBOOK.md for the end-to-end
sequence. Every script imports the materializer module by absolute path via `_util.load_module`
to avoid sys.path shadowing.
"""
