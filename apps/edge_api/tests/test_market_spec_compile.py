"""compile_spec — the market-spec predicate compiler (pure, no I/O)."""
import pytest
from fastapi import HTTPException

from apps.edge_api.src.routers.market_spec_v1 import compile_spec


def test_empty_spec_is_all() -> None:
    where, echo = compile_spec({})
    assert where == ""
    assert echo == {}


def test_geo_defaults_to_hq_basis() -> None:
    where, echo = compile_spec({"geo": {"states": ["tx", "OK"]}})
    assert where == " WHERE physical_state IN ('OK','TX')"
    assert echo["geo"] == {"basis": "hq", "states": ["OK", "TX"]}


def test_geo_pop_basis() -> None:
    where, _ = compile_spec({"geo": {"basis": "pop", "states": ["CA"]}})
    assert "primary_pop_state IN ('CA')" in where


def test_geo_unknown_state_refuses() -> None:
    with pytest.raises(HTTPException) as e:
        compile_spec({"geo": {"states": ["ZZ"]}})
    assert "ZZ" in e.value.detail


def test_dollars_default_total_24mo() -> None:
    where, echo = compile_spec({"dollars": {"min": 1_000_000}})
    assert where == " WHERE COALESCE(total_amt_24mo, 0) >= 1000000.0"
    assert echo["dollars"]["side"] == "total"
    assert echo["dollars"]["window"] == "24mo"


def test_dollars_side_window_and_max() -> None:
    where, _ = compile_spec(
        {"dollars": {"side": "prime", "window": "lifetime", "min": 5e6, "max": 1e8}}
    )
    assert "COALESCE(prime_obl_lifetime, 0) >= 5000000.0" in where
    assert "COALESCE(prime_obl_lifetime, 0) <= 100000000.0" in where


def test_dollars_36mo_refuses() -> None:
    with pytest.raises(HTTPException):
        compile_spec({"dollars": {"window": "36mo", "min": 1}})


def test_dollars_without_bound_refuses() -> None:
    with pytest.raises(HTTPException):
        compile_spec({"dollars": {"window": "24mo"}})


def test_dollars_bool_refuses() -> None:
    with pytest.raises(HTTPException):
        compile_spec({"dollars": {"min": True}})


def test_designations_and_together() -> None:
    where, _ = compile_spec({"designations": ["dsbs_8a", "fsrs_any_designation"]})
    assert where == " WHERE dsbs_8a = TRUE AND fsrs_any_designation = TRUE"


def test_unknown_designation_refuses() -> None:
    with pytest.raises(HTTPException) as e:
        compile_spec({"designations": ["small_business"]})
    assert "small_business" in e.value.detail


def test_employee_bands() -> None:
    where, _ = compile_spec({"firmographics": {"employee_bands": ["11-50", "1-10"]}})
    assert where == " WHERE employee_size_band IN ('1-10','11-50')"


def test_unknown_band_refuses() -> None:
    with pytest.raises(HTTPException):
        compile_spec({"firmographics": {"employee_bands": ["1001+"]}})


def test_full_spec_composes() -> None:
    where, echo = compile_spec(
        {
            "geo": {"states": ["TX"]},
            "dollars": {"min": 250_000},
            "designations": ["in_dsbs"],
            "firmographics": {"employee_bands": ["1-10"]},
        }
    )
    assert where.startswith(" WHERE ")
    assert where.count(" AND ") == 3
    assert set(echo) == {"geo", "dollars", "designations", "firmographics"}
