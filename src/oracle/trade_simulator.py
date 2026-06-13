"""
Trade Simulator for Market Impulse Strategy

Walks bar-by-bar after each signal entry and delegates exit decisions
to a pluggable exit policy.

Produces trade-level P&L with win rate, profit factor, and expectancy.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import time as dt_time
from typing import List

import numpy as np
import polars as pl
from loguru import logger
from src.time_utils import et_time_expr, et_date_expr


@dataclass
class Trade:
    """Record of a single simulated trade."""
    entry_time: object  # datetime
    exit_time: object   # datetime
    direction: str      # "long" or "short"
    entry_price: float
    exit_price: float
    exit_reason: str    # e.g. "vma_stop", "take_profit", "stop_loss", "eod"
    pnl: float = 0.0
    bars_held: int = 0
    vma_5m_at_entry: float = 0.0

    @property
    def is_winner(self) -> bool:
        return self.pnl > 0


@dataclass(frozen=True, slots=True)
class BarSnapshot:
    """Minimal bar view passed into exit-policy checks."""

    idx: int
    timestamp: object
    close: float
    high: float
    low: float
    bar_time: dt_time
    trade_date: object
    values: dict[str, float | None]


@dataclass(frozen=True, slots=True)
class OpenTrade:
    """State captured at entry for exit-policy evaluation."""

    entry_idx: int
    entry_time: object
    direction: str
    entry_price: float
    entry_date: object
    entry_values: dict[str, float | None]


@dataclass(frozen=True, slots=True)
class ExitDecision:
    """Exit decision returned by a policy when a bar triggers an exit."""

    reason: str
    exit_price: float | None = None


class ExitPolicy(ABC):
    """Interface for bar-by-bar trade exits."""

    policy_name: str

    @property
    def required_columns(self) -> set[str]:
        return set()

    def entry_is_valid(self, entry_bar: BarSnapshot, direction: str) -> bool:
        return True

    def reset(self) -> None:
        return None

    @abstractmethod
    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        ...


@dataclass(frozen=True, slots=True)
class VmaTrailingExitPolicy(ExitPolicy):
    """Replicates the legacy VMA trailing stop behavior."""

    vma_col: str = "vma_10_5m"
    policy_name: str = "vma_trailing"

    @property
    def required_columns(self) -> set[str]:
        return {self.vma_col}

    def entry_is_valid(self, entry_bar: BarSnapshot, direction: str) -> bool:
        value = entry_bar.values.get(self.vma_col)
        return value is not None and not np.isnan(value)

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        vma_value = bar.values.get(self.vma_col)
        if vma_value is None or np.isnan(vma_value):
            return None

        if trade.direction == "long" and bar.high < vma_value:
            return ExitDecision(reason="vma_stop")
        if trade.direction == "short" and bar.low > vma_value:
            return ExitDecision(reason="vma_stop")
        return None


@dataclass(frozen=True, slots=True)
class FixedRewardRiskExitPolicy(ExitPolicy):
    """Exit at a fixed stop distance or fixed reward multiple from entry."""

    stop_loss: float
    reward_multiple: float = 2.0
    policy_name: str = "fixed_rr"

    def __post_init__(self) -> None:
        if self.stop_loss <= 0:
            raise ValueError("stop_loss must be positive for fixed_rr exits.")
        if self.reward_multiple <= 0:
            raise ValueError("reward_multiple must be positive for fixed_rr exits.")

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        reward_distance = self.stop_loss * self.reward_multiple

        if trade.direction == "long":
            stop_price = trade.entry_price - self.stop_loss
            target_price = trade.entry_price + reward_distance
            # Conservative ordering when both thresholds are touched intra-bar.
            if bar.low <= stop_price:
                return ExitDecision(reason="stop_loss", exit_price=stop_price)
            if bar.high >= target_price:
                return ExitDecision(reason="take_profit", exit_price=target_price)
            return None

        stop_price = trade.entry_price + self.stop_loss
        target_price = trade.entry_price - reward_distance
        if bar.high >= stop_price:
            return ExitDecision(reason="stop_loss", exit_price=stop_price)
        if bar.low <= target_price:
            return ExitDecision(reason="take_profit", exit_price=target_price)
        return None


@dataclass(frozen=True, slots=True)
class FixedPercentRewardRiskExitPolicy(ExitPolicy):
    """Exit at a fixed percent stop and reward multiple from entry."""

    stop_loss_pct: float
    reward_multiple: float = 2.0
    policy_name: str = "fixed_rr_underlying"

    def __post_init__(self) -> None:
        if self.stop_loss_pct <= 0:
            raise ValueError("stop_loss_pct must be positive for fixed_rr_underlying exits.")
        if self.reward_multiple <= 0:
            raise ValueError("reward_multiple must be positive for fixed_rr_underlying exits.")

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        risk_distance = trade.entry_price * self.stop_loss_pct
        reward_distance = risk_distance * self.reward_multiple

        if trade.direction == "long":
            stop_price = trade.entry_price - risk_distance
            target_price = trade.entry_price + reward_distance
            if bar.low <= stop_price:
                return ExitDecision(reason="stop_loss_underlying", exit_price=stop_price)
            if bar.high >= target_price:
                return ExitDecision(reason="take_profit_underlying", exit_price=target_price)
            return None

        stop_price = trade.entry_price + risk_distance
        target_price = trade.entry_price - reward_distance
        if bar.high >= stop_price:
            return ExitDecision(reason="stop_loss_underlying", exit_price=stop_price)
        if bar.low <= target_price:
            return ExitDecision(reason="take_profit_underlying", exit_price=target_price)
        return None


@dataclass(slots=True)
class AtrTrailingExitPolicy(ExitPolicy):
    """ATR-based trailing stop anchored to the best favorable price seen so far."""

    atr_col: str = "atr_14"
    atr_multiple: float = 2.0
    policy_name: str = "atr_trailing_underlying"
    _extreme_by_entry_idx: dict[int, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.atr_multiple <= 0:
            raise ValueError("atr_multiple must be positive for atr_trailing exits.")

    @property
    def required_columns(self) -> set[str]:
        return {self.atr_col}

    def entry_is_valid(self, entry_bar: BarSnapshot, direction: str) -> bool:
        value = entry_bar.values.get(self.atr_col)
        return value is not None and not np.isnan(value) and value > 0

    def reset(self) -> None:
        self._extreme_by_entry_idx.clear()

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        atr_value = bar.values.get(self.atr_col)
        if atr_value is None or np.isnan(atr_value) or atr_value <= 0:
            return None

        if trade.direction == "long":
            best = self._extreme_by_entry_idx.get(trade.entry_idx, trade.entry_price)
            stop_price = best - self.atr_multiple * atr_value
            if bar.low <= stop_price:
                return ExitDecision(reason="atr_trailing_stop", exit_price=stop_price)
            self._extreme_by_entry_idx[trade.entry_idx] = max(best, bar.high)
            return None

        best = self._extreme_by_entry_idx.get(trade.entry_idx, trade.entry_price)
        stop_price = best + self.atr_multiple * atr_value
        if bar.high >= stop_price:
            return ExitDecision(reason="atr_trailing_stop", exit_price=stop_price)
        self._extreme_by_entry_idx[trade.entry_idx] = min(best, bar.low)
        return None


@dataclass(frozen=True, slots=True)
class MovingAverageTrailingExitPolicy(ExitPolicy):
    """Exit when close crosses the wrong side of a moving average."""

    ma_col: str = "ema_20_exit"
    policy_name: str = "ma_trailing_underlying"

    @property
    def required_columns(self) -> set[str]:
        return {self.ma_col}

    def entry_is_valid(self, entry_bar: BarSnapshot, direction: str) -> bool:
        value = entry_bar.values.get(self.ma_col)
        return value is not None and not np.isnan(value)

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        ma_value = bar.values.get(self.ma_col)
        if ma_value is None or np.isnan(ma_value):
            return None
        if trade.direction == "long" and bar.close < ma_value:
            return ExitDecision(reason="ma_trailing_stop", exit_price=bar.close)
        if trade.direction == "short" and bar.close > ma_value:
            return ExitDecision(reason="ma_trailing_stop", exit_price=bar.close)
        return None


@dataclass(frozen=True, slots=True)
class MovingAverageCrossoverExitPolicy(ExitPolicy):
    """Exit when the fast moving average crosses against the trade direction."""

    fast_ma_col: str = "ema_8_exit"
    slow_ma_col: str = "ema_20_exit"
    policy_name: str = "ma_crossover_underlying"

    @property
    def required_columns(self) -> set[str]:
        return {self.fast_ma_col, self.slow_ma_col}

    def entry_is_valid(self, entry_bar: BarSnapshot, direction: str) -> bool:
        fast = entry_bar.values.get(self.fast_ma_col)
        slow = entry_bar.values.get(self.slow_ma_col)
        return (
            fast is not None
            and slow is not None
            and not np.isnan(fast)
            and not np.isnan(slow)
        )

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        fast = bar.values.get(self.fast_ma_col)
        slow = bar.values.get(self.slow_ma_col)
        if fast is None or slow is None or np.isnan(fast) or np.isnan(slow):
            return None
        if trade.direction == "long" and fast < slow:
            return ExitDecision(reason="ma_crossover_exit", exit_price=bar.close)
        if trade.direction == "short" and fast > slow:
            return ExitDecision(reason="ma_crossover_exit", exit_price=bar.close)
        return None


@dataclass(frozen=True, slots=True)
class TimeStopExitPolicy(ExitPolicy):
    """Hold until a configured intraday time unless the simulator reaches EOD first."""

    exit_time: dt_time
    policy_name: str = "time_stop_underlying"

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        if bar.bar_time >= self.exit_time:
            return ExitDecision(
                reason=f"time_stop_{self.exit_time.strftime('%H%M')}",
                exit_price=bar.close,
            )
        return None


@dataclass(frozen=True, slots=True)
class HoldToEodExitPolicy(ExitPolicy):
    """No thesis exit; the simulator exits at the same-day EOD boundary."""

    policy_name: str = "hold_to_eod_underlying"

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        return None


@dataclass(slots=True)
class HighWaterGivebackExitPolicy(ExitPolicy):
    """Exit when an armed trade gives back a fraction of its peak favorable move.

    Arms once the best favorable excursion since entry reaches ``arm_pct``
    (fraction of entry price), then exits on close once price retraces
    ``retrace_frac`` of that peak. Underlying-path proxy for the live option
    profile's high-water giveback rule.
    """

    arm_pct: float
    retrace_frac: float = 0.5
    policy_name: str = "high_water_giveback"
    _best_by_entry_idx: dict[int, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.arm_pct <= 0:
            raise ValueError("arm_pct must be positive for high_water_giveback exits.")
        if not 0.0 < self.retrace_frac < 1.0:
            raise ValueError("retrace_frac must be between 0 and 1 for high_water_giveback exits.")

    def reset(self) -> None:
        self._best_by_entry_idx.clear()

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        entry = trade.entry_price
        if entry <= 0:
            return None
        long = trade.direction == "long"
        best = self._best_by_entry_idx.get(trade.entry_idx, entry)
        best_move = (best - entry) if long else (entry - best)
        if best_move > 0 and (best_move / entry) >= self.arm_pct:
            floor_move = best_move * (1.0 - self.retrace_frac)
            floor_price = entry + floor_move if long else entry - floor_move
            if (long and bar.close <= floor_price) or (not long and bar.close >= floor_price):
                return ExitDecision(reason="high_water_giveback", exit_price=bar.close)
        # Update best favorable extreme AFTER the check (avoid same-bar arm+exit).
        self._best_by_entry_idx[trade.entry_idx] = (
            max(best, bar.high) if long else min(best, bar.low)
        )
        return None


@dataclass(slots=True)
class NoProgressTimeStopExitPolicy(ExitPolicy):
    """Exit a trade that "never got going" after a configured number of bars.

    If, by ``no_progress_bars`` after entry, the best favorable excursion is
    still below ``min_favorable_pct`` (fraction of entry price), close at the
    current bar. Distinct from a plain wall-clock time stop: it only fires when
    the thesis has failed to make progress.
    """

    no_progress_bars: int
    min_favorable_pct: float = 0.0
    policy_name: str = "no_progress"
    _best_by_entry_idx: dict[int, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.no_progress_bars <= 0:
            raise ValueError("no_progress_bars must be positive for no_progress exits.")

    def reset(self) -> None:
        self._best_by_entry_idx.clear()

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        entry = trade.entry_price
        if entry <= 0:
            return None
        long = trade.direction == "long"
        best = self._best_by_entry_idx.get(trade.entry_idx, entry)
        best = max(best, bar.high) if long else min(best, bar.low)
        self._best_by_entry_idx[trade.entry_idx] = best
        best_move = (best - entry) if long else (entry - best)
        if bar.idx - trade.entry_idx >= self.no_progress_bars:
            if (best_move / entry) < self.min_favorable_pct:
                return ExitDecision(reason="no_progress", exit_price=bar.close)
        return None


@dataclass(slots=True)
class ProfileExitPolicy(ExitPolicy):
    """Named operator exit profile evaluated on the underlying path.

    A deterministic priority ladder approximating a live option exit profile
    (``public_api_trading_v3`` ``profiles.py``) on underlying bars:
    initial stop -> partial at target_1 (then move the stop to breakeven) ->
    target_2 -> high-water giveback -> no-progress / max-hold time stop.

    R is defined by ``stop_loss_pct`` (underlying-scaled). The partial
    scale-out is modeled as a blended single-trade exit price, so the
    simulator's per-unit P&L reflects banking ``target_1_quantity`` at T1 and
    riding the remainder. Time bounds are in bars (1-minute bars ~ minutes).
    """

    profile_name: str
    stop_loss_pct: float
    target_1_r: float = 1.0
    target_2_r: float = 2.0
    target_1_quantity: float = 0.5
    giveback_arm_r: float | None = None
    giveback_retrace_frac: float = 0.5
    no_progress_bars: int | None = None
    no_progress_floor_r: float = 0.25
    max_hold_bars: int | None = None
    breakeven_after_t1: bool = True
    policy_name: str = "profile_exit"
    _state_by_entry_idx: dict[int, dict] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.stop_loss_pct <= 0:
            raise ValueError("stop_loss_pct must be positive for profile exits.")
        if not 0.0 <= self.target_1_quantity <= 1.0:
            raise ValueError("target_1_quantity must be between 0 and 1 for profile exits.")

    def reset(self) -> None:
        self._state_by_entry_idx.clear()

    def should_exit(self, trade: OpenTrade, bar: BarSnapshot) -> ExitDecision | None:
        entry = trade.entry_price
        risk = entry * self.stop_loss_pct
        if risk <= 0:
            return None
        long = trade.direction == "long"
        st = self._state_by_entry_idx.setdefault(
            trade.entry_idx, {"best": entry, "t1": False, "t1_price": 0.0}
        )

        best_move = (st["best"] - entry) if long else (entry - st["best"])
        best_r = best_move / risk  # from prior bars only

        def signed_move(price: float) -> float:
            return (price - entry) if long else (entry - price)

        def blended(runner_price: float) -> float:
            if st["t1"]:
                return (
                    self.target_1_quantity * st["t1_price"]
                    + (1.0 - self.target_1_quantity) * runner_price
                )
            return runner_price

        # 1. Stop — initial protective stop, or breakeven after the T1 partial.
        if st["t1"] and self.breakeven_after_t1:
            stop_price = entry
            stop_reason = "breakeven_stop"
        else:
            stop_price = entry - risk if long else entry + risk
            stop_reason = "stop_loss_underlying"
        if (long and bar.low <= stop_price) or (not long and bar.high >= stop_price):
            return ExitDecision(reason=stop_reason, exit_price=blended(stop_price))

        # 2. Target 1 — bank the partial and move the stop to breakeven.
        if not st["t1"] and self.target_1_quantity > 0:
            t1_price = entry + self.target_1_r * risk if long else entry - self.target_1_r * risk
            if (long and bar.high >= t1_price) or (not long and bar.low <= t1_price):
                st["t1"] = True
                st["t1_price"] = t1_price
                if self.target_1_quantity >= 1.0:
                    return ExitDecision(reason="take_profit_underlying", exit_price=t1_price)

        # 3. Target 2 — full exit of the runner (or single target when no partial).
        if st["t1"] or self.target_1_quantity == 0:
            t2_price = entry + self.target_2_r * risk if long else entry - self.target_2_r * risk
            if (long and bar.high >= t2_price) or (not long and bar.low <= t2_price):
                return ExitDecision(reason="take_profit_underlying", exit_price=blended(t2_price))

        # 4. High-water giveback (armed by peak R from prior bars).
        if self.giveback_arm_r is not None and best_r >= self.giveback_arm_r:
            floor_r = best_r * (1.0 - self.giveback_retrace_frac)
            if signed_move(bar.close) / risk <= floor_r:
                return ExitDecision(reason="high_water_giveback", exit_price=blended(bar.close))

        # 5. Time stops — max hold, then no-progress.
        bars_elapsed = bar.idx - trade.entry_idx
        if self.max_hold_bars is not None and bars_elapsed >= self.max_hold_bars:
            return ExitDecision(reason="max_hold", exit_price=blended(bar.close))
        if (
            self.no_progress_bars is not None
            and bars_elapsed >= self.no_progress_bars
            and best_r < self.no_progress_floor_r
        ):
            return ExitDecision(reason="no_progress", exit_price=blended(bar.close))

        # Update best favorable extreme after all checks.
        st["best"] = max(st["best"], bar.high) if long else min(st["best"], bar.low)
        return None


@dataclass
class SimulationResult:
    """Aggregate results from the trade simulation."""
    trades: List[Trade] = field(default_factory=list)

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def winners(self) -> List[Trade]:
        return [t for t in self.trades if t.is_winner]

    @property
    def losers(self) -> List[Trade]:
        return [t for t in self.trades if not t.is_winner]

    @property
    def win_rate(self) -> float:
        return len(self.winners) / self.total_trades if self.total_trades else 0.0

    @property
    def avg_winner(self) -> float:
        wins = [t.pnl for t in self.winners]
        return sum(wins) / len(wins) if wins else 0.0

    @property
    def avg_loser(self) -> float:
        losses = [t.pnl for t in self.losers]
        return sum(losses) / len(losses) if losses else 0.0

    @property
    def profit_factor(self) -> float:
        gross_wins = sum(t.pnl for t in self.winners)
        gross_losses = abs(sum(t.pnl for t in self.losers))
        return gross_wins / gross_losses if gross_losses > 0 else float("inf")

    @property
    def expectancy(self) -> float:
        """Average P&L per trade."""
        return sum(t.pnl for t in self.trades) / self.total_trades if self.total_trades else 0.0

    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)

    def long_trades(self) -> "SimulationResult":
        return SimulationResult(trades=[t for t in self.trades if t.direction == "long"])

    def short_trades(self) -> "SimulationResult":
        return SimulationResult(trades=[t for t in self.trades if t.direction == "short"])

    def to_dataframe(self) -> pl.DataFrame:
        """Convert trades list to a Polars DataFrame."""
        if not self.trades:
            return pl.DataFrame()
        return pl.DataFrame([
            {
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "direction": t.direction,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "exit_reason": t.exit_reason,
                "pnl": round(t.pnl, 4),
                "bars_held": t.bars_held,
                "vma_5m_at_entry": round(t.vma_5m_at_entry, 4),
            }
            for t in self.trades
        ])


class TradeSimulator:
    """
    Bar-by-bar trade simulator with pluggable exit policies.

    Defaults to the legacy VMA trailing-stop behavior, but can also run
    fixed reward/risk or other explicit exit policies.
    """

    def __init__(
        self,
        vma_5m_col: str = "vma_10_5m",
        market_close: dt_time = dt_time(15, 59),
        exit_policy: ExitPolicy | None = None,
        entry_delay_bars: int = 0,
        min_hold_bars: int = 0,
        cooldown_bars_after_signal: int = 0,
    ) -> None:
        self.market_close = market_close
        self.exit_policy = exit_policy or VmaTrailingExitPolicy(vma_col=vma_5m_col)
        self.entry_delay_bars = max(0, int(entry_delay_bars))
        self.min_hold_bars = max(0, int(min_hold_bars))
        self.cooldown_bars_after_signal = max(0, int(cooldown_bars_after_signal))
        self.vma_5m_col = (
            self.exit_policy.vma_col
            if isinstance(self.exit_policy, VmaTrailingExitPolicy)
            else vma_5m_col
        )

    def simulate(self, df: pl.DataFrame) -> SimulationResult:
        """
        Run the simulation on a DataFrame that has 'signal',
        'signal_direction', and the 5-min VMA column.

        Returns a SimulationResult with all trades.
        """
        required = {
            "timestamp",
            "close",
            "high",
            "low",
            "signal",
            "signal_direction",
            *self.exit_policy.required_columns,
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"TradeSimulator requires columns: {missing}")
        self.exit_policy.reset()

        # Convert to numpy for fast iteration
        timestamps = df["timestamp"].to_list()
        close = df["close"].to_numpy()
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()
        signal = df["signal"].to_list()
        direction = df["signal_direction"].to_list()
        # Pre-compute bar times for EOD check
        bar_times = df.select(et_time_expr("timestamp").alias("t"))["t"].to_list()

        # Pre-compute dates for session boundary tracking
        dates = df.select(et_date_expr("timestamp").alias("d"))["d"].to_list()
        policy_arrays = {
            column: df[column].to_numpy()
            for column in self.exit_policy.required_columns
        }

        n = len(df)
        trades: List[Trade] = []
        i = 0

        def bar_snapshot(idx: int) -> BarSnapshot:
            return BarSnapshot(
                idx=idx,
                timestamp=timestamps[idx],
                close=float(close[idx]),
                high=float(high[idx]),
                low=float(low[idx]),
                bar_time=bar_times[idx],
                trade_date=dates[idx],
                values={
                    column: float(policy_arrays[column][idx])
                    if policy_arrays[column][idx] is not None
                    else None
                    for column in self.exit_policy.required_columns
                },
            )

        while i < n:
            # Look for signal entry
            if not signal[i] or direction[i] is None:
                i += 1
                continue

            entry_idx = i + self.entry_delay_bars
            if entry_idx >= n or dates[entry_idx] != dates[i]:
                i += 1
                continue
            entry_bar = bar_snapshot(entry_idx)
            if not self.exit_policy.entry_is_valid(entry_bar, str(direction[i])):
                i += 1
                continue

            entry_time = timestamps[entry_idx]
            entry_price = close[entry_idx]
            entry_direction = direction[i]
            entry_date = dates[entry_idx]
            open_trade = OpenTrade(
                entry_idx=entry_idx,
                entry_time=entry_time,
                direction=str(entry_direction),
                entry_price=float(entry_price),
                entry_date=entry_date,
                entry_values=entry_bar.values,
            )

            # Walk forward to find exit
            j = entry_idx + max(1, self.min_hold_bars)
            exit_reason = "eod"
            exit_price_override: float | None = None

            while j < n:
                # Session boundary — if we crossed into a new day, exit at last bar of entry day
                if dates[j] != entry_date:
                    j = j - 1  # back to last bar of entry day
                    exit_reason = "eod"
                    break

                # EOD time stop
                if bar_times[j] >= self.market_close:
                    exit_reason = "eod"
                    break

                decision = self.exit_policy.should_exit(open_trade, bar_snapshot(j))
                if decision is not None:
                    exit_reason = decision.reason
                    exit_price_override = decision.exit_price
                    break

                j += 1

            # Clamp to valid index
            exit_idx = min(j, n - 1)
            exit_price = exit_price_override if exit_price_override is not None else close[exit_idx]
            exit_time = timestamps[exit_idx]
            bars_held = exit_idx - entry_idx

            # Calculate P&L
            if entry_direction == "long":
                pnl = exit_price - entry_price
            else:
                pnl = entry_price - exit_price

            trades.append(Trade(
                entry_time=entry_time,
                exit_time=exit_time,
                direction=entry_direction,
                entry_price=round(entry_price, 4),
                exit_price=round(exit_price, 4),
                exit_reason=exit_reason,
                pnl=round(pnl, 4),
                bars_held=bars_held,
                vma_5m_at_entry=round(open_trade.entry_values.get(self.vma_5m_col) or 0.0, 4),
            ))

            # Move past the exit bar to avoid overlapping trades
            i = exit_idx + 1 + self.cooldown_bars_after_signal

        result = SimulationResult(trades=trades)
        logger.info(
            "Simulation complete: {} trades, {:.1%} win rate, "
            "${:.4f} expectancy, {:.2f} profit factor",
            result.total_trades,
            result.win_rate,
            result.expectancy,
            result.profit_factor,
        )
        return result
