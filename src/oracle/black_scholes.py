"""Black-Scholes-Merton option pricing for the S4 option-translation scorer.

Pure functions used to reconstruct an option-premium path from an underlying
path when real option-chain history is unavailable (or too short). The IV that
feeds these functions is *modeled* (see src/research/option_translation.py);
this module only does the deterministic pricing/greeks math.

European BSM. For the short-DTE long-premium use case the American
early-exercise error is negligible; upgrade to a binomial model later if needed.
"""

from __future__ import annotations

import math
from statistics import NormalDist

_N = NormalDist()  # standard normal


def _d1_d2(S: float, K: float, T: float, r: float, sigma: float, q: float) -> tuple[float, float]:
    vol = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / vol
    return d1, d1 - vol


def price(S: float, K: float, T: float, sigma: float, kind: str,
          r: float = 0.0, q: float = 0.0) -> float:
    """Option price. ``kind`` is 'call' or 'put'. Degenerates to intrinsic at expiry."""
    if S <= 0 or K <= 0:
        return 0.0
    call = kind.lower() == "call"
    if T <= 0 or sigma <= 0:
        intrinsic = (S - K) if call else (K - S)
        return max(0.0, intrinsic)
    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    if call:
        return S * math.exp(-q * T) * _N.cdf(d1) - K * math.exp(-r * T) * _N.cdf(d2)
    return K * math.exp(-r * T) * _N.cdf(-d2) - S * math.exp(-q * T) * _N.cdf(-d1)


def delta(S: float, K: float, T: float, sigma: float, kind: str,
          r: float = 0.0, q: float = 0.0) -> float:
    """Option delta (signed: calls positive, puts negative)."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    d1, _ = _d1_d2(S, K, T, r, sigma, q)
    disc = math.exp(-q * T)
    if kind.lower() == "call":
        return disc * _N.cdf(d1)
    return disc * (_N.cdf(d1) - 1.0)


def strike_for_delta(S: float, target_delta: float, T: float, sigma: float, kind: str,
                     r: float = 0.0, q: float = 0.0) -> float:
    """Strike whose option has approximately ``target_delta`` (absolute, 0<δ<1).

    Inverts the BSM delta closed-form (no root finding needed).
    """
    if not 0.0 < target_delta < 1.0 or T <= 0 or sigma <= 0 or S <= 0:
        return S
    disc = math.exp(-q * T)
    call = kind.lower() == "call"
    # delta_call = disc * N(d1); |delta_put| = disc * N(-d1)
    n_arg = target_delta / disc
    n_arg = min(max(n_arg, 1e-6), 1.0 - 1e-6)
    d1 = _N.inv_cdf(n_arg) if call else -_N.inv_cdf(n_arg)
    vol = sigma * math.sqrt(T)
    # d1 = (ln(S/K) + (r-q+sigma^2/2)T) / vol  ->  K = S * exp((r-q+sigma^2/2)T - d1*vol)
    return S * math.exp((r - q + 0.5 * sigma * sigma) * T - d1 * vol)
