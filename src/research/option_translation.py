"""S4 option translation: score an exit profile on the reconstructed option path.

The underlying exit optimizer can't value option convexity (Wave 1 validation,
2026-06-13): partials/giveback that pay on the premium just cut the move on the
linear underlying. This module reconstructs the option-premium path from the
underlying path via Black-Scholes with a *modeled* IV, then runs the SAME
ProfileExitPolicy on the premium — where the profile's R-targets, partials, and
giveback are designed to live.

IV is modeled (no real chain needed) and run as a BAND so fragility is named:
  flat        — IV held at entry
  mean_revert — IV drifts toward a realized-vol anchor over the hold
  crush       — IV deflates as the trade goes favorable (vega drag)

Entry IV anchor = iv_premium_factor x annualized realized vol of the underlying.
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

IV_MODELS = ("flat", "mean_revert", "crush")
_YEAR_MINUTES = 365 * 24 * 60
_GIVEBACK = {"STRICT": (0.5, 0.5), "MODERATE": (0.75, 0.6), "LOOSE": (1.0, 0.75), "OFF": (None, 0.5)}

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


def _iv(model: str, entry_iv: float, anchor: float, elapsed_frac: float, fav_frac: float) -> float:
    if model == "crush":
        return max(0.05, entry_iv * (1.0 - 0.5 * min(1.0, max(0.0, fav_frac))))
    if model == "mean_revert":
        return entry_iv + (anchor - entry_iv) * min(1.0, max(0.0, elapsed_frac))
    return entry_iv


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
    iv_model: str = "flat", iv_premium_factor: float = 1.2, r: float = 0.04,
    market_close: dt.time = dt.time(15, 59), crush_move_scale: float = 0.02,
) -> dict:
    """Premium-path expectancy (% per trade) of a profile on a holdout frame.

    ``frame`` must carry timestamp/close/high/low/signal/signal_direction. Each
    matching signal buys one option (put for short, call for long) at the
    profile's delta/DTE, then the profile manages the premium to EOD.
    """
    p = OPTION_PROFILES[profile_name]
    kind = "call" if direction.lower() == "long" else "put"
    anchor = annualized_realized_vol(frame)
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
            fav = (S0 - float(low[j])) / S0 if kind == "put" else (float(high[j]) - S0) / S0
            sigma = _iv(iv_model, entry_iv, anchor, k / max(1.0, T0 * _YEAR_MINUTES),
                        max(0.0, fav) / crush_move_scale)
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
            sigma = _iv(iv_model, entry_iv, anchor, 1.0, 0.0)
            exit_prem = bs.price(float(close[jj]), K, T, sigma, kind, r=r)
        pnls.append((exit_prem - entry_prem) / entry_prem * 100.0)
        i = j + 1

    if not pnls:
        return {"profile": profile_name, "iv_model": iv_model, "n": 0,
                "expectancy_pct": 0.0, "win_rate": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0}
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    return {
        "profile": profile_name, "iv_model": iv_model, "n": len(pnls),
        "expectancy_pct": sum(pnls) / len(pnls),
        "win_rate": len(wins) / len(pnls),
        "avg_win_pct": sum(wins) / len(wins) if wins else 0.0,
        "avg_loss_pct": sum(losses) / len(losses) if losses else 0.0,
    }


def score_profile_band(frame: pl.DataFrame, direction: str, profile_name: str, **kw) -> list[dict]:
    """Run all IV-band models for a profile; expectancy spread names the fragility."""
    return [score_profile_on_options(frame, direction, profile_name, iv_model=m, **kw) for m in IV_MODELS]
