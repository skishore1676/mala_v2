"""
Oracle Metrics Calculator

Computes forward-looking metrics for every signal bar:
  - MFE  (Maximum Favorable Excursion)   – highest high in next N bars
  - MAE  (Maximum Adverse Excursion)     – lowest low in next N bars
  - Win  flag                             – MFE > 2× MAE
  - Confidence Score                      – Wins / Total Signals

Also produces a summary report DataFrame.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import polars as pl
from loguru import logger

from src.config import settings
from src.oracle.policies import RewardRiskWinCondition
from src.time_utils import et_date_expr


class MetricsCalculator:
    """Compute MFE / MAE and build probabilistic output surfaces."""

    def __init__(
        self,
        forward_window: int = settings.forward_window_bars,
        win_condition: RewardRiskWinCondition | None = None,
        entry_delay_bars: int = 0,
        min_hold_bars: int = 0,
        exit_evaluation_start_bar: int = 1,
        cooldown_bars_after_signal: int = 0,
    ) -> None:
        self.forward_window = forward_window
        self.win_condition = win_condition or RewardRiskWinCondition()
        self.entry_delay_bars = max(0, int(entry_delay_bars))
        self.min_hold_bars = max(0, int(min_hold_bars))
        self.exit_evaluation_start_bar = max(1, int(exit_evaluation_start_bar))
        self.cooldown_bars_after_signal = max(0, int(cooldown_bars_after_signal))

    @property
    def directional_win_column(self) -> str:
        return self.win_condition.directional_column()

    @property
    def directional_confidence_column(self) -> str:
        return f"confidence_{self.win_condition.label_suffix}"

    @staticmethod
    def _available_directional_snapshot_windows(df: pl.DataFrame) -> tuple[int, ...]:
        mfe_windows = {
            int(column.removeprefix("forward_mfe_"))
            for column in df.columns
            if column.startswith("forward_mfe_") and column.removeprefix("forward_mfe_").isdigit()
        }
        mae_windows = {
            int(column.removeprefix("forward_mae_"))
            for column in df.columns
            if column.startswith("forward_mae_") and column.removeprefix("forward_mae_").isdigit()
        }
        return tuple(sorted(mfe_windows & mae_windows))

    # ── Public ───────────────────────────────────────────────────────────

    def add_forward_metrics(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Append forward-looking MFE, MAE, and win flag columns.

        Requires 'high', 'low', 'close' columns in the DataFrame.
        Must be called BEFORE filtering to signals-only so that we
        have the full bar window for look-ahead.
        """
        n = len(df)
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()
        close = df["close"].to_numpy()

        mfe = np.full(n, np.nan)
        mae = np.full(n, np.nan)

        exit_offset = self._earliest_exit_offset()
        for i in range(n):
            entry_i = i + self.entry_delay_bars
            end_i = entry_i + self.forward_window
            favorable_start_i = entry_i + exit_offset
            adverse_start_i = entry_i + 1
            if entry_i >= n or end_i >= n or favorable_start_i > end_i or adverse_start_i > end_i:
                continue

            future_high = high[favorable_start_i : end_i + 1]
            future_low = low[adverse_start_i : end_i + 1]
            entry_price = close[entry_i]

            if len(future_high) == 0 or len(future_low) == 0:
                continue

            mfe[i] = float(np.max(future_high) - entry_price)
            mae[i] = float(entry_price - np.min(future_low))

        df = df.with_columns([
            pl.Series(f"forward_mfe_{self.forward_window}", mfe),
            pl.Series(f"forward_mae_{self.forward_window}", mae),
        ])

        # Win = MFE > 2× MAE (reward/risk > 2)
        mfe_col = f"forward_mfe_{self.forward_window}"
        mae_col = f"forward_mae_{self.forward_window}"
        df = df.with_columns(self.win_condition.expr(mfe_col, mae_col).alias("win"))

        logger.info("Forward metrics added (window = {} bars)", self.forward_window)
        return df

    def summarise_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Given a DataFrame with 'signal' and forward metrics, produce a
        summary row per ticker.
        """
        if "signal" not in df.columns:
            raise ValueError("DataFrame must contain a 'signal' column.")

        mfe_col = f"forward_mfe_{self.forward_window}"
        mae_col = f"forward_mae_{self.forward_window}"

        signals_df = df.filter(pl.col("signal")).drop_nulls(subset=[mfe_col, mae_col])

        if signals_df.is_empty():
            logger.warning("No valid signals to summarise.")
            return pl.DataFrame()

        total = signals_df.height
        wins = signals_df.filter(pl.col("win")).height
        confidence = wins / total if total > 0 else 0.0

        summary = pl.DataFrame({
            "total_signals": [total],
            "wins": [wins],
            "losses": [total - wins],
            "confidence_score": [round(confidence, 4)],
            "avg_mfe": [round(float(signals_df[mfe_col].mean()), 4)],  # type: ignore[arg-type]
            "avg_mae": [round(float(signals_df[mae_col].mean()), 4)],  # type: ignore[arg-type]
            "median_mfe": [round(float(signals_df[mfe_col].median()), 4)],  # type: ignore[arg-type]
            "median_mae": [round(float(signals_df[mae_col].median()), 4)],  # type: ignore[arg-type]
            "max_mfe": [round(float(signals_df[mfe_col].max()), 4)],  # type: ignore[arg-type]
            "max_mae": [round(float(signals_df[mae_col].max()), 4)],  # type: ignore[arg-type]
        })

        logger.info(
            "Summary: {} signals, {} wins, confidence {:.2%}",
            total,
            wins,
            confidence,
        )
        return summary

    def trade_log(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Extract a detailed trade log for every signal occurrence.
        """
        mfe_col = f"forward_mfe_{self.forward_window}"
        mae_col = f"forward_mae_{self.forward_window}"

        cols_to_keep = [
            "timestamp", "ticker", "close",
            "velocity_1m", "accel_1m",
            "vpoc_4h",
            mfe_col, mae_col, "win",
        ]
        present = [c for c in cols_to_keep if c in df.columns]

        log = (
            df.filter(pl.col("signal"))
            .select(present)
            .drop_nulls(subset=[mfe_col, mae_col])
            .sort("timestamp")
        )
        logger.info("Trade log contains {} entries", len(log))
        return log

    # ── Directional Forward Metrics (for Market Impulse) ─────────────────

    def add_directional_forward_metrics(
        self,
        df: pl.DataFrame,
        snapshot_windows: tuple[int, ...] = (30, 60),
    ) -> pl.DataFrame:
        """
        Compute forward MFE/MAE that respect signal direction with
        end-of-day measurement window.

        For long signals:  MFE = max(future highs) − entry
                           MAE = entry − min(future lows)
        For short signals: MFE = entry − min(future lows)
                           MAE = max(future highs) − entry

        Also provides snapshot windows (e.g. 30-min, 60-min).

        Requires: 'close', 'high', 'low', 'timestamp', 'signal',
                  'signal_direction' columns.
        """
        n = len(df)
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()
        close = df["close"].to_numpy()
        timestamps = df["timestamp"].to_list()

        # Pre-compute date for each bar to find end-of-day boundaries
        dates = df.select(
            et_date_expr("timestamp").alias("trade_date")
        )["trade_date"].to_list()

        signal = [bool(value) for value in df["signal"].to_list()]
        direction = df["signal_direction"].to_list()
        accepted_signal = self._apply_signal_cooldown(signal, dates)
        if accepted_signal != signal:
            accepted_direction = [
                value if accepted_signal[idx] else None
                for idx, value in enumerate(direction)
            ]
            df = df.with_columns([
                pl.Series("signal", accepted_signal),
                pl.Series("signal_direction", accepted_direction),
            ])
            direction = df["signal_direction"].to_list()

        # Build day-end index lookup: for each bar, the last bar of that day
        day_end_idx = {}
        for i in range(n - 1, -1, -1):
            d = dates[i]
            if d not in day_end_idx:
                day_end_idx[d] = i

        # ── End-of-day metrics ──────────────────────────────────────────
        mfe_eod = np.full(n, np.nan)
        mae_eod = np.full(n, np.nan)

        exit_offset = self._earliest_exit_offset()
        for i in range(n):
            if not direction[i]:
                continue

            d = dates[i]
            end_i = day_end_idx.get(d, i)
            entry_i = i + self.entry_delay_bars
            if entry_i >= n or dates[entry_i] != d:
                continue
            favorable_start_i = entry_i + exit_offset
            adverse_start_i = entry_i + 1
            if favorable_start_i > end_i or adverse_start_i > end_i:
                continue

            favorable_high = high[favorable_start_i : end_i + 1]
            favorable_low = low[favorable_start_i : end_i + 1]
            adverse_high = high[adverse_start_i : end_i + 1]
            adverse_low = low[adverse_start_i : end_i + 1]
            entry_price = close[entry_i]

            if len(favorable_high) == 0 or len(adverse_high) == 0:
                continue

            if direction[i] == "long":
                mfe_eod[i] = float(np.max(favorable_high) - entry_price)
                mae_eod[i] = float(entry_price - np.min(adverse_low))
            elif direction[i] == "short":
                mfe_eod[i] = float(entry_price - np.min(favorable_low))
                mae_eod[i] = float(np.max(adverse_high) - entry_price)

        df = df.with_columns([
            pl.Series("forward_mfe_eod", mfe_eod),
            pl.Series("forward_mae_eod", mae_eod),
        ])

        # ── Snapshot windows (e.g., 30-min, 60-min) ─────────────────────
        for window in snapshot_windows:
            mfe_w = np.full(n, np.nan)
            mae_w = np.full(n, np.nan)

            for i in range(n):
                if not direction[i]:
                    continue

                entry_i = i + self.entry_delay_bars
                if entry_i >= n:
                    continue
                end_i = min(entry_i + window, n - 1)
                if dates[entry_i] != dates[i]:
                    continue
                favorable_start_i = entry_i + exit_offset
                adverse_start_i = entry_i + 1
                if favorable_start_i > end_i or adverse_start_i > end_i:
                    continue

                favorable_high = high[favorable_start_i : end_i + 1]
                favorable_low = low[favorable_start_i : end_i + 1]
                adverse_high = high[adverse_start_i : end_i + 1]
                adverse_low = low[adverse_start_i : end_i + 1]
                entry_price = close[entry_i]

                if len(favorable_high) == 0 or len(adverse_high) == 0:
                    continue

                if direction[i] == "long":
                    mfe_w[i] = float(np.max(favorable_high) - entry_price)
                    mae_w[i] = float(entry_price - np.min(adverse_low))
                elif direction[i] == "short":
                    mfe_w[i] = float(entry_price - np.min(favorable_low))
                    mae_w[i] = float(np.max(adverse_high) - entry_price)

            df = df.with_columns([
                pl.Series(f"forward_mfe_{window}", mfe_w),
                pl.Series(f"forward_mae_{window}", mae_w),
            ])

        # ── Win flag: reward:risk threshold on end-of-day excursion ────
        df = df.with_columns(
            self.win_condition.expr("forward_mfe_eod", "forward_mae_eod")
            .alias(self.directional_win_column)
        )

        logger.info(
            "Directional forward metrics added (EOD + snapshots: {})",
            snapshot_windows,
        )
        return df

    def _earliest_exit_offset(self) -> int:
        return max(1, self.exit_evaluation_start_bar, self.min_hold_bars)

    def _apply_signal_cooldown(
        self,
        signal: list[bool],
        dates: list[object],
    ) -> list[bool]:
        if self.cooldown_bars_after_signal <= 0:
            return signal
        accepted = [False] * len(signal)
        last_signal_idx_by_date: dict[object, int] = {}
        for idx, is_signal in enumerate(signal):
            if not is_signal:
                continue
            trade_date = dates[idx]
            last_idx = last_signal_idx_by_date.get(trade_date)
            if last_idx is not None and idx - last_idx <= self.cooldown_bars_after_signal:
                continue
            accepted[idx] = True
            last_signal_idx_by_date[trade_date] = idx
        return accepted

    def summarise_directional_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Produce a summary report for directional signals,
        broken out by long / short / combined.
        """
        signals_df = (
            df.filter(pl.col("signal"))
            .drop_nulls(subset=["forward_mfe_eod", "forward_mae_eod"])
        )

        if signals_df.is_empty():
            logger.warning("No valid directional signals to summarise.")
            return pl.DataFrame()

        snapshot_windows = self._available_directional_snapshot_windows(signals_df)
        rows = []
        for direction_filter, label in [
            (None, "Combined"),
            ("long", "Long"),
            ("short", "Short"),
        ]:
            if direction_filter:
                subset = signals_df.filter(
                    pl.col("signal_direction") == direction_filter
                )
            else:
                subset = signals_df

            if subset.is_empty():
                continue

            total = subset.height
            wins = subset.filter(pl.col(self.directional_win_column)).height
            confidence = wins / total if total > 0 else 0.0
            mean_mfe = float(subset["forward_mfe_eod"].mean())
            mean_mae = float(subset["forward_mae_eod"].mean())
            if np.isnan(mean_mfe) or np.isnan(mean_mae) or mean_mae <= 0:
                ratio = None
            else:
                ratio = round(mean_mfe / mean_mae, 2)

            row = {
                "direction": label,
                "total_signals": total,
                "wins": wins,
                "losses": total - wins,
                self.directional_confidence_column: round(confidence, 4),
                "avg_mfe_eod": round(mean_mfe, 4) if not np.isnan(mean_mfe) else None,
                "avg_mae_eod": round(mean_mae, 4) if not np.isnan(mean_mae) else None,
                "median_mfe_eod": round(float(subset["forward_mfe_eod"].median()), 4),
                "median_mae_eod": round(float(subset["forward_mae_eod"].median()), 4),
                "avg_mfe_mae_ratio": ratio,
            }

            # Add snapshot window metrics if available
            for w in snapshot_windows:
                mfe_col = f"forward_mfe_{w}"
                mae_col = f"forward_mae_{w}"
                valid = subset.drop_nulls(subset=[mfe_col, mae_col])
                if not valid.is_empty():
                    row[f"avg_mfe_{w}m"] = round(float(valid[mfe_col].mean()), 4)
                    row[f"avg_mae_{w}m"] = round(float(valid[mae_col].mean()), 4)

            rows.append(row)

        summary = pl.DataFrame(rows)
        for _, row_data in enumerate(rows):
            ratio_display = (
                f"{row_data['avg_mfe_mae_ratio']:.2f}x"
                if row_data["avg_mfe_mae_ratio"] is not None
                else "n/a"
            )
            logger.info(
                "Summary [{}]: {} signals, {} wins, confidence {:.2%}, "
                "MFE/MAE ratio {}",
                row_data["direction"],
                row_data["total_signals"],
                row_data["wins"],
                row_data[self.directional_confidence_column],
                ratio_display,
            )

        return summary

    def directional_trade_log(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Extract a detailed trade log for directional signal entries.
        """
        cols_to_keep = [
            "timestamp", "ticker", "close", "signal_direction",
            "impulse_regime_5m", "impulse_stage",
            "vma_10",
            "forward_mfe_eod", "forward_mae_eod",
            "forward_mfe_30", "forward_mae_30",
            "forward_mfe_60", "forward_mae_60",
            self.directional_win_column,
        ]
        present = [c for c in cols_to_keep if c in df.columns]

        log = (
            df.filter(pl.col("signal"))
            .select(present)
            .drop_nulls(subset=["forward_mfe_eod", "forward_mae_eod"])
            .sort("timestamp")
        )
        logger.info("Directional trade log contains {} entries", len(log))
        return log
