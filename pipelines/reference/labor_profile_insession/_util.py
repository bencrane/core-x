"""Shared plumbing for the in-session labor-profile harness.

The single job here is a ROBUST, UNAMBIGUOUS import of the materializer module. A stale copy of
`materialize_naics_psc_labor_profile.py` sitting on an old worktree's sys.path once silently
shadowed the intended one — so we load the sibling file BY ABSOLUTE PATH, not by bare
`import materialize_naics_psc_labor_profile`.

Override with NPLP_MODULE_PATH=/abs/path/to/materialize_naics_psc_labor_profile.py if you ever
need to point at a different checkout.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from types import ModuleType

_MODULE_NAME = "materialize_naics_psc_labor_profile"


def module_path() -> str:
    override = os.environ.get("NPLP_MODULE_PATH")
    if override:
        return os.path.abspath(override)
    # sibling of this package dir: pipelines/reference/materialize_naics_psc_labor_profile.py
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(here), f"{_MODULE_NAME}.py")


def load_module() -> ModuleType:
    """Import the materializer module from its exact file path (no sys.path shadowing)."""
    path = module_path()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"materializer module not found at {path}; set NPLP_MODULE_PATH to override")
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = mod  # so dataclasses/pickling by name resolve to THIS instance
    spec.loader.exec_module(mod)
    return mod
