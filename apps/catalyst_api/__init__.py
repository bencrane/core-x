"""catalyst_api — Gen-3 read API for the rare-structure client surface.

A lightweight FastAPI gateway that resolves a web domain to its US federal
contracting profile via native Lance ``BTREE`` point-lookups against the R2
system-of-record sink. Read-only: it never writes a dataset (see ARCHITECTURE.md
— a gateway reads the committed plane; pipelines materialize it).
"""
