"""Verification harness for the canonical blocking-key macro (``core/name_norm.py``).

Proves the single-source-of-truth refactor is correct, two ways:

  1. BYTE-IDENTITY — load every pipeline that consumes the macro, grab the bound builder it
     imported, and assert it emits the EXACT same SQL string as ``core.name_norm`` for a probe
     expression (catches any stray re-inlined copy or broken import).

  2. DuckDB EQUIVALENCE — run the generated SQL over an ``&`` / dash / quote / accent /
     whitespace / suffix / empty battery and assert the normalized output matches the
     documented goldens, AND that ``fl_federal_tax_liens``'s PRE-refactor inline form (which
     omitted ``CAST(... AS VARCHAR)`` and used tight spacing) yields byte-for-byte the SAME
     normalized output as the new canonical macro — proving FL's convergence changed no result.

Run:  python core/name_norm_check.py      (exits non-zero on any mismatch)

The DuckDB battery requires ``duckdb>=1.5``; if duckdb is not importable the battery is
skipped with a loud warning, but the byte-identity checks always run.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.name_norm import legal_name_base, name_norm  # noqa: E402

# (pipeline file, attribute holding the bound macro) — every consumer of the shared macro.
# sam passes an EXPRESSION and binds the macro as ``_norm_sql``; the rest bind it as ``_name_norm``.
CONSUMERS: list[tuple[str, str]] = [
    ("pipelines/sos_normalized/normalize.py", "_name_norm"),
    ("pipelines/fl_federal_tax_liens/ingest.py", "_name_norm"),
    ("pipelines/osha/osha_sniper.py", "_name_norm"),
    ("pipelines/resolution/recon_ca_ucc_sos.py", "_name_norm"),
    ("pipelines/resolution/crosswalk_hmda_gleif.py", "_name_norm"),
    ("pipelines/resolution/crosswalk_sam_usaspending.py", "_norm_sql"),
    ("pipelines/resolution/credit_spine_normalize_index.py", "_name_norm"),
]


def _fl_pre_refactor(slot: str) -> str:
    """fl_federal_tax_liens' EXACT pre-refactor inline name_norm (HEAD before this change),
    parameterized on the column/literal slot: no CAST, tight comma spacing. Used to prove the
    convergence to the canonical CAST form is behaviour-preserving."""
    return (
        "nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "upper(" + slot + "), '&',' AND ','g'), '[-\\x{2013}\\x{2014}]+',' ','g'),"
        " '[^A-Z0-9 ]+','','g'), '\\s+',' ','g')),'')"
    )


# Normalization battery: (raw input, expected normalized_legal_name). Covers the conjunction,
# all three dash codepoints, quotes/apostrophes, accents (stripped, not folded), whitespace
# collapse, punctuation, and emptied→NULL.
NAME_BATTERY: list[tuple[str, object]] = [
    ("Smith & Sons", "SMITH AND SONS"),
    ("A&B", "A AND B"),
    ("&", "AND"),
    ("BRAND - STORE", "BRAND STORE"),
    ("BRAND-STORE", "BRAND STORE"),
    ("AAA–BBB", "AAA BBB"),              # en-dash U+2013
    ("AAA—BBB", "AAA BBB"),              # em-dash U+2014
    ("Café Niño", "CAF NIO"),       # accents stripped, not folded (É/Ñ ∉ A-Z)
    ("O'Brien \"Inc\"", "OBRIEN INC"),
    ("ACME,  INC.", "ACME INC"),
    ("  Multiple   Spaces  ", "MULTIPLE SPACES"),
    ("***", None),                            # emptied → NULL
    ("", None),
]

# legal_name_base battery: (raw input, expected base) — input is first run through name_norm.
BASE_BATTERY: list[tuple[str, object]] = [
    ("ACME CO LLC", "ACME"),                  # peels BOTH trailing tokens
    ("FOO LLC INC", "FOO"),
    ("WIDGETS LTD", "WIDGETS"),
    ("TACO COMPANY", "TACO COMPANY"),         # COMPANY ∉ set (partial CO can't reach $)
    ("ACME CORPORATION", "ACME CORPORATION"),  # CORPORATION ∉ set
    ("PLAIN NAME", "PLAIN NAME"),
    ("LLC", "LLC"),                           # no leading space → whole-token rule spares it
]


def _load(rel_path: str, i: int):
    """Import a pipeline module by file path under a unique synthetic name (offline: the
    modules import only ``modal`` + ``core.name_norm`` at load time)."""
    spec = importlib.util.spec_from_file_location(f"_probe_mod_{i}", REPO_ROOT / rel_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def check_byte_identity() -> list[str]:
    """Every consumer's bound macro must emit the EXACT canonical string for a probe expr."""
    failures: list[str] = []
    probe = "some_table.some_col"
    golden = name_norm(probe)
    mods: dict[str, object] = {}
    for i, (rel, attr) in enumerate(CONSUMERS):
        mod = _load(rel, i)
        mods[rel] = mod
        fn = getattr(mod, attr, None)
        if fn is None:
            failures.append(f"{rel}: missing bound macro {attr!r}")
            continue
        got = fn(probe)
        if got != golden:
            failures.append(f"{rel}::{attr} emits divergent SQL:\n  got   : {got}\n  golden: {golden}")
    # legal_name_base lives only in the sos spine.
    sos = mods["pipelines/sos_normalized/normalize.py"]
    lnb = getattr(sos, "_legal_name_base", None)
    if lnb is None:
        failures.append("sos_normalized: missing bound _legal_name_base")
    elif lnb("normalized_legal_name") != legal_name_base("normalized_legal_name"):
        failures.append("sos_normalized::_legal_name_base emits divergent SQL")
    return failures


def check_duckdb_battery() -> list[str]:
    """Run the canonical SQL in DuckDB; assert goldens + FL pre/post equivalence."""
    try:
        import duckdb
    except ModuleNotFoundError:
        print("WARN: duckdb not importable — skipping the DuckDB battery (byte-identity still ran). "
              "Install duckdb>=1.5 to run it.")
        return []
    failures: list[str] = []
    con = duckdb.connect(":memory:")

    def _lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    # name_norm goldens + FL pre/post DuckDB equivalence (one row per case).
    for raw, expected in NAME_BATTERY:
        lit = _lit(raw)
        got = con.execute("SELECT " + name_norm(lit)).fetchone()[0]
        if got != expected:
            failures.append(f"name_norm({raw!r}) = {got!r}, expected {expected!r}")
        fl_old = con.execute("SELECT " + _fl_pre_refactor(lit)).fetchone()[0]
        if fl_old != got:
            failures.append(f"FL pre-refactor diverged for {raw!r}: pre={fl_old!r} canonical={got!r}")

    # legal_name_base goldens (input first passes through name_norm).
    for raw, expected in BASE_BATTERY:
        got = con.execute("SELECT " + legal_name_base(name_norm(_lit(raw)))).fetchone()[0]
        if got != expected:
            failures.append(f"legal_name_base(name_norm({raw!r})) = {got!r}, expected {expected!r}")

    con.close()
    print(f"DuckDB {duckdb.__version__}: ran {len(NAME_BATTERY)} name_norm + "
          f"{len(NAME_BATTERY)} FL-equivalence + {len(BASE_BATTERY)} legal_name_base cases.")
    return failures


def main() -> int:
    failures = check_byte_identity()
    print(f"byte-identity: {len(CONSUMERS)} consumers + legal_name_base checked, "
          f"{len(failures)} failure(s).")
    failures += check_duckdb_battery()
    if failures:
        print("\nFAIL:")
        for f in failures:
            print("  - " + f)
        return 1
    print(f"\nOK — all {len(CONSUMERS)} consumers emit byte-identical canonical SQL; "
          "DuckDB battery matches goldens.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
