"""Tests for the pure logic in scripts/calibrate_iv.py."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from calibrate_iv import compute_factor


def test_compute_factor():
    assert compute_factor(0.36, 0.30) == 1.2
    assert compute_factor(None, 0.3) is None
    assert compute_factor(0.36, 0.0) is None
    assert compute_factor(0.36, None) is None
