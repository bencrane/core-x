"""Unit and regression tests for pipelines.dibbs.parse_rfq_pdfs."""

from __future__ import annotations

import datetime as dt
import sys
import types


def _ensure_modal_stub():
    if "modal" not in sys.modules:
        m = types.ModuleType("modal")
        m.Image = types.SimpleNamespace(
            debian_slim=lambda **kw: types.SimpleNamespace(pip_install=lambda *a: None)
        )
        m.Secret = types.SimpleNamespace(from_name=lambda *a: None)
        m.App = lambda *a, **kw: types.SimpleNamespace(
            function=lambda **fkw: (lambda fn: fn)
        )
        sys.modules["modal"] = m


_ensure_modal_stub()

from pipelines.dibbs.parse_rfq_pdfs import (  # noqa: E402
    RE_CAGE_PN,
    RE_HIST_ROW,
    _parse_index_text,
)


def test_parse_index_text_fixed_width():
    sample_line = (
        "SPE1C126T07568415010688341                                    "
        "701589111009/03/26SPE1C126T0756.pdf  0000050EAHOOD,DISPOSABLE      DSM01Z1N000"
    )
    by_pdf = _parse_index_text(sample_line)
    assert "SPE1C126T0756.PDF" in by_pdf
    rec = by_pdf["SPE1C126T0756.PDF"][0]
    assert rec["sol"] == "SPE1C126T0756"
    assert rec["nsn"] == "8415010688341"
    assert rec["fsc"] == "8415"
    assert rec["pr"] == "7015891110"
    assert rec["ret_date"] == dt.date(2026, 9, 3)
    assert rec["qty"] == 50
    assert rec["uoi"] == "EA"
    assert rec["nom"] == "HOOD,DISPOSABLE"
    assert rec["buyer"] == "DSM01"
    assert rec["amsc"] == "Z"
    assert rec["amc"] == "1"
    assert rec["set_aside"] == "N"


def test_hist_row_regex():
    line = "97537 SPE7L126V085S 2.000 15800.00000 20260630 N"
    m = RE_HIST_ROW.match(line)
    assert m is not None
    assert m.groups() == ("97537", "SPE7L126V085S", "2.000", "15800.00000", "20260630", "N")


def test_cage_pn_regex():
    line = "JUNIPER ELBOW CO., INC. 97537 P/N JE10641-001"
    m = RE_CAGE_PN.match(line)
    assert m is not None
    assert m.groups() == ("JUNIPER ELBOW CO., INC.", "97537", "JE10641-001")
