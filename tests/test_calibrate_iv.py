"""Tests for the pure logic in scripts/calibrate_iv.py."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from calibrate_iv import compute_factor, extract_atm_iv


def test_compute_factor():
    assert compute_factor(0.36, 0.30) == 1.2
    assert compute_factor(None, 0.3) is None
    assert compute_factor(0.36, 0.0) is None
    assert compute_factor(0.36, None) is None


def test_extract_atm_iv_nearest_strike():
    chain = {
        "calls": [
            {"strike": 95, "impliedVolatility": 0.40},
            {"strike": 100, "impliedVolatility": 0.35},
            {"strike": 105, "impliedVolatility": 0.38},
        ],
        "puts": [{"strike": 100, "impliedVolatility": 0.34}],
    }
    assert abs(extract_atm_iv(chain, spot=101.0) - 0.35) < 1e-9


def test_extract_atm_iv_nested_greeks_and_aliases():
    chain = {"options": [{"strikePrice": 50, "greeks": {"iv": 0.55}}]}
    assert abs(extract_atm_iv(chain, spot=49.0) - 0.55) < 1e-9


def test_extract_atm_iv_empty():
    assert extract_atm_iv({}, spot=100.0) is None
    assert extract_atm_iv({"calls": [{"strike": 100}]}, spot=100.0) is None  # no IV field
