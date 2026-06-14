"""S4 option translation: score an exit profile on the reconstructed option path.

The underlying exit optimizer can't value option convexity (Wave 1 validation,
2026-06-13): partials/giveback that pay on the premium just cut the move on the
linear underlying. This module reconstructs the option-premium path from the
underlying path via Black-Scholes with a *modeled* IV, then runs the SAME
ProfileExitPolicy on the premium — where the profile's R-targets, partials, and
giveback are designed to live.

IV is modeled (no real historical chain exists yet) and made DIRECTION-AWARE via
the equity spot-vol leverage effect: IV rises when spot falls (negative spot-vol
correlation). Because the IV response keys off the SIGNED underlying return,
puts bought into a selloff correctly get IV *expansion* (vega tailwind) and calls
bought into a melt-up get IV bleed — fixing the earlier direction-agnostic crush.

  sigma_t = entry_iv * (1 + vol_beta * (-return_since_entry)),  clamped [0.3x, 3x]
  entry_iv = iv_premium_factor * annualized realized vol of the underlying

Run as a BAND over the two real unknowns — how rich options were at entry
(iv_premium_factor) and how strong the leverage effect is (vol_beta) — so the
expectancy spread names the fragility. Real IV (Public get_option_greeks) is
LIVE-only today; it can calibrate iv_premium_factor / vol_beta going forward but
cannot fill a past holdout, so the historical path stays modeled.

Single-session (exits at EOD), matching the underlying simulator. See
docs/EXIT_PROFILE_PLAYBOOKS.md.
"""

from __future__ import annotations

import datetime as dt
import math
import statistics

import polars as pl

from src.oracle import black_scholes as bs
from src.oracle.trade_simulator import BarSnapshot, OpenTrade, ProfileExitPolicy
from src.time_utils import et_date_expr, et_time_expr

_YEAR_MINUTES = 365 * 24 * 60
_GIVEBACK = {"STRICT": (0.5, 0.5), "MODERATE": (0.75, 0.6), "LOOSE": (1.0, 0.75), "OFF": (None, 0.5)}

# IV-band scenarios: (label, iv_premium_factor, vol_beta). "flat" turns the
# leverage effect off as a baseline; the rest keep it on and vary entry richness.
IV_BAND = (
    ("flat", 1.2, 0.0),
    ("leverage", 1.2, 2.5),
    ("cheap_iv", 1.0, 2.5),
    ("rich_iv", 1.4, 2.5),
)

# Option-premium-side profile params (the live values from
# public_api_trading_v3 profiles.py): stop is premium %, R-multiples/partials/
# giveback/time carry over, plus vehicle selection (dte, delta).
OPTION_PROFILES: dict[str, dict] = {
    "FLASH_REVERSAL":      dict(stop_loss_pct=0.30, target_1_r=1.0, target_2_r=2.0, target_1_quantity=0.75,
                                giveback="STRICT",   no_progress_bars=15,  max_hold_bars=90,  dte=2,  delta=0.52),
    "EXHAUSTION_REVERSAL": dict(stop_loss_pct=0.45, target_1_r=1.0, target_2_r=2.5, target_1_quantity=0.50,
                                giveback="MODERATE", no_progress_bars=75,  max_hold_bars=240, dte=8,  delta=0.35),
    "TREND_CONTINUATION":  dict(stop_loss_pct=0.35, target_1_r=1.0, target_2_r=2.0, target_1_quantity=0.60,
                                giveback="MODERATE", no_progress_bars=45,  max_hold_bars=180, dte=7,  delta=0.33),
    "RANGE_EXPANSION":     dict(stop_loss_pct=0.40, target_1_r=1.0, target_2_r=2.0, target_1_quantity=0.40,
                                giveback="LOOSE",    no_progress_bars=120, max_hold_bars=None, dte=16, delta=0.30),
}


def annualized_realized_vol(frame: pl.DataFrame) -> float:
    """Annualized close-to-close vol from daily closes (fallback 0.30)."""
    daily = (
        frame.group_by(et_date_expr("timestamp").alias("d"))
        .agg(pl.col("close").last().alias("c")).sort("d")
    )
    closes = [c for c in daily["c"].to_list() if c and c > 0]
    if len(closes) < 6:
        return 0.30
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    return max(0.05, statistics.pstdev(rets) * math.sqrt(252))


def _iv(entry_iv: float, underlying_return: float, vol_beta: float) -> float:
    """Direction-aware IV via the spot-vol leverage effect (negative spot-vol
    correlation): IV rises as spot falls. ``underlying_return`` is signed since
    entry, so puts-in-selloffs get IV up and calls-in-melt-ups get IV bleed.
    Clamped to [0.3x, 3x] entry IV."""
    sigma = entry_iv * (1.0 + vol_beta * (-underlying_return))
    return min(max(sigma, 0.3 * entry_iv), 3.0 * entry_iv)


def _build_policy(profile_name: str) -> ProfileExitPolicy:
    p = OPTION_PROFILES[profile_name]
    arm, retrace = _GIVEBACK[p["giveback"]]
    return ProfileExitPolicy(
        profile_name=profile_name, stop_loss_pct=p["stop_loss_pct"],
        target_1_r=p["target_1_r"], target_2_r=p["target_2_r"], target_1_quantity=p["target_1_quantity"],
        giveback_arm_r=arm, giveback_retrace_frac=retrace,
        no_progress_bars=p["no_progress_bars"], no_progress_floor_r=0.25,
        max_hold_bars=p["max_hold_bars"], breakeven_after_t1=True,
    )


def score_profile_on_options(
    frame: pl.DataFrame, direction: str, profile_name: str, *,
    iv_premium_factor: float = 1.2, vol_beta: float = 2.5, r: float = 0.04,
    market_close: dt.time = dt.time(15, 59),
    symbol: str | None = None, use_real_iv: bool = False,
) -> dict:
    """Premium-path expectancy (% per trade) of a profile on a holdout frame.

    ``frame`` must carry timestamp/close/high/low/signal/signal_direction. Each
    matching signal buys one option (put for short, call for long) at the
    profile's delta/DTE, then the profile manages the premium to EOD.
    """
    p = OPTION_PROFILES[profile_name]
    kind = "call" if direction.lower() == "long" else "put"
    anchor = annualized_realized_vol(frame)
    if use_real_iv and symbol:
        from src.research.kamandal_iv import real_iv_factor  # lazy: avoid import cycle
        real_factor = real_iv_factor(symbol)
        if real_factor:
            iv_premium_factor = real_factor
    entry_iv = max(0.05, iv_premium_factor * anchor)
    T0 = p["dte"] / 365.0

    ts = frame["timestamp"].to_list()
    close = frame["close"].to_numpy(); high = frame["high"].to_numpy(); low = frame["low"].to_numpy()
    sig = frame["signal"].to_list(); sdir = frame["signal_direction"].to_list()
    times = frame.select(et_time_expr("timestamp").alias("t"))["t"].to_list()
    dates = frame.select(et_date_expr("timestamp").alias("d"))["d"].to_list()
    n = len(frame)
    pol = _build_policy(profile_name)

    pnls: list[float] = []
    i = 0
    while i < n:
        if not sig[i] or sdir[i] is None or str(sdir[i]).lower() != direction.lower():
            i += 1
            continue
        S0 = float(close[i])
        K = bs.strike_for_delta(S0, p["delta"], T0, entry_iv, kind, r=r)
        entry_prem = bs.price(S0, K, T0, entry_iv, kind, r=r)
        if entry_prem <= 0.01:
            i += 1
            continue
        pol.reset()
        otrade = OpenTrade(entry_idx=0, entry_time=ts[i], direction="long",
                           entry_price=entry_prem, entry_date=dates[i], entry_values={})
        exit_prem = None
        j, k = i + 1, 1
        while j < n and dates[j] == dates[i] and times[j] < market_close:
            T = max((T0 * _YEAR_MINUTES - k) / _YEAR_MINUTES, 1e-6)
            sigma = _iv(entry_iv, (float(close[j]) - S0) / S0, vol_beta)
            pc = bs.price(float(close[j]), K, T, sigma, kind, r=r)
            if kind == "call":
                ph = bs.price(float(high[j]), K, T, sigma, kind, r=r)
                pl_ = bs.price(float(low[j]), K, T, sigma, kind, r=r)
            else:
                ph = bs.price(float(low[j]), K, T, sigma, kind, r=r)
                pl_ = bs.price(float(high[j]), K, T, sigma, kind, r=r)
            bar = BarSnapshot(idx=k, timestamp=ts[j], close=pc, high=ph, low=pl_,
                              bar_time=times[j], trade_date=dates[j], values={})
            decision = pol.should_exit(otrade, bar)
            if decision is not None:
                exit_prem = decision.exit_price if decision.exit_price is not None else pc
                break
            j += 1; k += 1
        if exit_prem is None:
            jj = min(j, n - 1)
            T = max((T0 * _YEAR_MINUTES - k) / _YEAR_MINUTES, 1e-6)
            sigma = _iv(entry_iv, (float(close[jj]) - S0) / S0, vol_beta)
            exit_prem = bs.price(float(close[jj]), K, T, sigma, kind, r=r)
        pnls.append((exit_prem - entry_prem) / entry_prem * 100.0)
        i = j + 1

    if not pnls:
        return {"profile": profile_name, "iv_premium_factor": iv_premium_factor,
                "vol_beta": vol_beta, "n": 0,
                "expectancy_pct": 0.0, "win_rate": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0}
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    return {
        "profile": profile_name, "iv_premium_factor": iv_premium_factor,
        "vol_beta": vol_beta, "n": len(pnls),
        "expectancy_pct": sum(pnls) / len(pnls),
        "win_rate": len(wins) / len(pnls),
        "avg_win_pct": sum(wins) / len(wins) if wins else 0.0,
        "avg_loss_pct": sum(losses) / len(losses) if losses else 0.0,
    }


def score_profile_band(frame: pl.DataFrame, direction: str, profile_name: str, **kw) -> list[dict]:
    """Run the IV-band scenarios for a profile; expectancy spread names the fragility."""
    out = []
    for label, factor, beta in IV_BAND:
        res = score_profile_on_options(frame, direction, profile_name,
                                       iv_premium_factor=factor, vol_beta=beta, **kw)
        out.append({"scenario": label, **res})
    return out
