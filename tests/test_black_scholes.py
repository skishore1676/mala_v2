"""Tests for the Black-Scholes-Merton pricer (S4 option translation)."""

import math

from src.oracle import black_scholes as bs


def test_atm_call_put_known_value():
    # S=K=100, T=1, r=q=0, sigma=0.2 -> call == put ~= 7.966
    c = bs.price(100, 100, 1.0, 0.2, "call")
    p = bs.price(100, 100, 1.0, 0.2, "put")
    assert abs(c - 7.9656) < 1e-2
    assert abs(p - 7.9656) < 1e-2
    assert abs(c - p) < 1e-6  # parity with r=q=0, S=K


def test_atm_delta():
    assert abs(bs.delta(100, 100, 1.0, 0.2, "call") - 0.5398) < 1e-3
    assert abs(bs.delta(100, 100, 1.0, 0.2, "put") + 0.4602) < 1e-3


def test_put_call_parity():
    S, K, T, sig, r, q = 100, 95, 0.5, 0.25, 0.04, 0.01
    c = bs.price(S, K, T, sig, "call", r=r, q=q)
    p = bs.price(S, K, T, sig, "put", r=r, q=q)
    lhs = c - p
    rhs = S * math.exp(-q * T) - K * math.exp(-r * T)
    assert abs(lhs - rhs) < 1e-6


def test_intrinsic_at_expiry():
    assert bs.price(110, 100, 0.0, 0.2, "call") == 10.0
    assert bs.price(90, 100, 0.0, 0.2, "put") == 10.0
    assert bs.price(90, 100, 0.0, 0.2, "call") == 0.0


def test_strike_for_delta_round_trip():
    for kind, target in [("call", 0.30), ("call", 0.55), ("put", 0.30), ("put", 0.45)]:
        K = bs.strike_for_delta(100, target, 0.1, 0.4, kind)
        got = abs(bs.delta(100, K, 0.1, 0.4, kind))
        assert abs(got - target) < 1e-3, f"{kind} {target}: got {got}"
