"""Lag-safe forecast feature diagnostics for Mala research.

This module intentionally lives outside the strategy registry.  Forecasts are
treated as candidate research features first; only a diagnostic edge should
graduate into a strategy gate or Newton feature.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from src.chronos.storage import LocalStorage


DEFAULT_MODELS = ("lag_return", "momentum_30", "mean_reversion_30")
DEFAULT_HORIZONS = (5, 15, 30, 60)
DEFAULT_OUTPUT_ROOT = Path("data/results/forecast_diagnostics")


@dataclass(frozen=True, slots=True)
class ForecastMetric:
    ticker: str
    model: str
    horizon_bars: int
    rows: int
    directional_rows: int
    directional_coverage: float
    direction_hit_rate: float | None
    avg_signed_return_bps: float | None
    top_quartile_rows: int
    top_quartile_hit_rate: float | None
    top_quartile_avg_signed_return_bps: float | None
    pearson_corr: float | None
    mae_bps: float | None
    rmse_bps: float | None
    verdict: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "model": self.model,
            "horizon_bars": self.horizon_bars,
            "rows": self.rows,
            "directional_rows": self.directional_rows,
            "directional_coverage": self.directional_coverage,
            "direction_hit_rate": self.direction_hit_rate,
            "avg_signed_return_bps": self.avg_signed_return_bps,
            "top_quartile_rows": self.top_quartile_rows,
            "top_quartile_hit_rate": self.top_quartile_hit_rate,
            "top_quartile_avg_signed_return_bps": self.top_quartile_avg_signed_return_bps,
            "pearson_corr": self.pearson_corr,
            "mae_bps": self.mae_bps,
            "rmse_bps": self.rmse_bps,
            "verdict": self.verdict,
        }


def build_forecast_feature_frame(
    df: pl.DataFrame,
    *,
    ticker: str,
    horizons: list[int],
    models: list[str],
    sample_every: int = 1,
) -> pl.DataFrame:
    """Return lag-safe forecast rows for one ticker.

    Every forecast column uses only current or prior closes.  The realized return
    column is the only forward-looking value and must remain diagnostic-only.
    """
    if df.is_empty():
        return pl.DataFrame()
    required = {"timestamp", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Forecast diagnostic requires columns: {sorted(missing)}")

    sample_every = max(1, sample_every)
    base = (
        df.sort("timestamp")
        .with_row_index("_row_idx")
        .filter((pl.col("_row_idx") % sample_every) == 0)
        .select(["timestamp", "ticker", "close"])
    )
    if "ticker" not in base.columns:
        base = base.with_columns(pl.lit(ticker.upper()).alias("ticker"))

    frames: list[pl.DataFrame] = []
    for horizon in horizons:
        horizon = int(horizon)
        if horizon <= 0:
            raise ValueError("Forecast horizons must be positive integers")
        horizon_frame = base.with_columns(
            [
                pl.lit(horizon).alias("horizon_bars"),
                ((pl.col("close").shift(-horizon) / pl.col("close")) - 1.0).alias(
                    "actual_return"
                ),
            ]
        )
        for model in models:
            horizon_frame = horizon_frame.with_columns(
                _forecast_expr(model, horizon).alias(f"forecast_return__{model}")
            )
        frames.append(horizon_frame)

    wide = pl.concat(frames, how="vertical")
    value_columns = [f"forecast_return__{model}" for model in models]
    return wide.unpivot(
        index=["timestamp", "ticker", "close", "horizon_bars", "actual_return"],
        on=value_columns,
        variable_name="model",
        value_name="forecast_return",
    ).with_columns(
        pl.col("model").str.replace("^forecast_return__", "").alias("model")
    )


def summarize_forecast_edges(
    feature_frame: pl.DataFrame,
    *,
    min_rows: int = 500,
    min_directional_coverage: float = 0.20,
    min_hit_rate: float = 0.52,
    min_avg_signed_return_bps: float = 0.0,
    min_top_quartile_signed_return_bps: float = 1.0,
) -> pl.DataFrame:
    """Score forecast rows by ticker/model/horizon."""
    if feature_frame.is_empty():
        return pl.DataFrame()

    rows: list[dict[str, Any]] = []
    groups = feature_frame.partition_by(["ticker", "model", "horizon_bars"], as_dict=True)
    for key, group in groups.items():
        ticker, model, horizon = key
        metric = _score_group(
            group,
            ticker=str(ticker),
            model=str(model),
            horizon_bars=int(horizon),
            min_rows=min_rows,
            min_directional_coverage=min_directional_coverage,
            min_hit_rate=min_hit_rate,
            min_avg_signed_return_bps=min_avg_signed_return_bps,
            min_top_quartile_signed_return_bps=min_top_quartile_signed_return_bps,
        )
        rows.append(metric.as_dict())

    return pl.DataFrame(rows).sort(
        ["verdict", "top_quartile_avg_signed_return_bps", "direction_hit_rate"],
        descending=[False, True, True],
    )


def write_diagnostic_artifacts(
    *,
    metrics: pl.DataFrame,
    output_dir: Path,
    feature_rows: pl.DataFrame | None = None,
    notes: list[str] | None = None,
) -> Path:
    """Write summary CSVs and a concise Markdown verdict."""
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "forecast_metric_summary.csv"
    metrics.write_csv(metrics_path)
    if feature_rows is not None:
        feature_rows.write_csv(output_dir / "forecast_feature_rows.csv")

    verdict_path = output_dir / "VERDICT.md"
    verdict_path.write_text(_render_verdict(metrics, notes=notes or []), encoding="utf-8")
    return verdict_path


def run_diagnostic(
    *,
    tickers: list[str],
    start: date,
    end: date,
    horizons: list[int],
    models: list[str],
    sample_every: int,
    output_dir: Path,
    write_rows: bool = False,
) -> Path:
    storage = LocalStorage()
    feature_frames: list[pl.DataFrame] = []
    notes: list[str] = []
    for ticker in tickers:
        df = storage.load_bars(ticker, start, end)
        if df.is_empty():
            notes.append(f"No cached bars found for {ticker}.")
            continue
        feature_frames.append(
            build_forecast_feature_frame(
                df,
                ticker=ticker,
                horizons=horizons,
                models=models,
                sample_every=sample_every,
            )
        )
    if not feature_frames:
        raise SystemExit("No cached bars were available for the requested diagnostic.")

    feature_rows = pl.concat(feature_frames, how="vertical")
    metrics = summarize_forecast_edges(feature_rows)
    return write_diagnostic_artifacts(
        metrics=metrics,
        output_dir=output_dir,
        feature_rows=feature_rows if write_rows else None,
        notes=notes,
    )


def _forecast_expr(model: str, horizon: int) -> pl.Expr:
    if model == "persistence":
        return pl.lit(0.0)
    if model == "lag_return":
        return (pl.col("close") / pl.col("close").shift(horizon)) - 1.0
    if model == "momentum_30":
        return (pl.col("close") / pl.col("close").shift(30)) - 1.0
    if model == "mean_reversion_30":
        return -((pl.col("close") / pl.col("close").shift(30)) - 1.0)
    if model == "momentum_60":
        return (pl.col("close") / pl.col("close").shift(60)) - 1.0
    if model == "mean_reversion_60":
        return -((pl.col("close") / pl.col("close").shift(60)) - 1.0)
    raise ValueError(f"Unsupported diagnostic model: {model}")


def _score_group(
    group: pl.DataFrame,
    *,
    ticker: str,
    model: str,
    horizon_bars: int,
    min_rows: int,
    min_directional_coverage: float,
    min_hit_rate: float,
    min_avg_signed_return_bps: float,
    min_top_quartile_signed_return_bps: float,
) -> ForecastMetric:
    clean = group.drop_nulls(["actual_return", "forecast_return"])
    rows = clean.height
    if rows == 0:
        return ForecastMetric(
            ticker=ticker,
            model=model,
            horizon_bars=horizon_bars,
            rows=0,
            directional_rows=0,
            directional_coverage=0.0,
            direction_hit_rate=None,
            avg_signed_return_bps=None,
            top_quartile_rows=0,
            top_quartile_hit_rate=None,
            top_quartile_avg_signed_return_bps=None,
            pearson_corr=None,
            mae_bps=None,
            rmse_bps=None,
            verdict="fail",
        )

    directional = clean.filter(pl.col("forecast_return") != 0)
    directional_rows = directional.height
    directional_coverage = directional_rows / rows if rows else 0.0
    scored = directional.with_columns(
        [
            (pl.col("actual_return") * pl.col("forecast_return").sign()).alias(
                "_signed_return"
            ),
            (
                pl.col("actual_return").sign() == pl.col("forecast_return").sign()
            ).alias("_direction_hit"),
            pl.col("forecast_return").abs().alias("_strength"),
            (pl.col("actual_return") - pl.col("forecast_return")).alias("_error"),
        ]
    )

    direction_hit_rate = _mean_or_none(scored, "_direction_hit")
    avg_signed_return_bps = _mean_bps_or_none(scored, "_signed_return")
    top_quartile = _top_strength_slice(scored)
    top_quartile_rows = top_quartile.height
    top_quartile_hit_rate = _mean_or_none(top_quartile, "_direction_hit")
    top_quartile_avg_signed_return_bps = _mean_bps_or_none(
        top_quartile,
        "_signed_return",
    )
    pearson_corr = _pearson_or_none(scored)
    mae_bps = _mean_bps_or_none(scored.with_columns(pl.col("_error").abs()), "_error")
    rmse_bps = _rmse_bps_or_none(scored)

    verdict = "pass" if (
        rows >= min_rows
        and directional_coverage >= min_directional_coverage
        and (direction_hit_rate or 0.0) >= min_hit_rate
        and (avg_signed_return_bps or 0.0) > min_avg_signed_return_bps
        and (top_quartile_avg_signed_return_bps or 0.0)
        >= min_top_quartile_signed_return_bps
    ) else "fail"

    return ForecastMetric(
        ticker=ticker,
        model=model,
        horizon_bars=horizon_bars,
        rows=rows,
        directional_rows=directional_rows,
        directional_coverage=round(directional_coverage, 4),
        direction_hit_rate=_round_optional(direction_hit_rate),
        avg_signed_return_bps=_round_optional(avg_signed_return_bps),
        top_quartile_rows=top_quartile_rows,
        top_quartile_hit_rate=_round_optional(top_quartile_hit_rate),
        top_quartile_avg_signed_return_bps=_round_optional(
            top_quartile_avg_signed_return_bps
        ),
        pearson_corr=_round_optional(pearson_corr),
        mae_bps=_round_optional(mae_bps),
        rmse_bps=_round_optional(rmse_bps),
        verdict=verdict,
    )


def _top_strength_slice(scored: pl.DataFrame) -> pl.DataFrame:
    if scored.is_empty():
        return scored
    threshold = scored.select(pl.col("_strength").quantile(0.75)).item()
    if threshold is None:
        return scored.head(0)
    return scored.filter(pl.col("_strength") >= threshold)


def _mean_or_none(df: pl.DataFrame, column: str) -> float | None:
    if df.is_empty() or column not in df.columns:
        return None
    value = df.select(pl.col(column).mean()).item()
    return float(value) if value is not None else None


def _mean_bps_or_none(df: pl.DataFrame, column: str) -> float | None:
    value = _mean_or_none(df, column)
    return value * 10_000.0 if value is not None else None


def _pearson_or_none(df: pl.DataFrame) -> float | None:
    if df.height < 2:
        return None
    value = df.select(pl.corr("forecast_return", "actual_return")).item()
    return float(value) if value is not None else None


def _rmse_bps_or_none(df: pl.DataFrame) -> float | None:
    if df.is_empty():
        return None
    value = df.select((pl.col("_error").pow(2).mean().sqrt() * 10_000.0)).item()
    return float(value) if value is not None else None


def _round_optional(value: float | None, digits: int = 4) -> float | None:
    return round(float(value), digits) if value is not None else None


def _render_verdict(metrics: pl.DataFrame, *, notes: list[str]) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    if metrics.is_empty():
        body = "No metrics were generated."
    else:
        passes = metrics.filter(pl.col("verdict") == "pass")
        best = metrics.sort(
            ["top_quartile_avg_signed_return_bps", "direction_hit_rate"],
            descending=[True, True],
        ).head(10)
        body = (
            f"- rows_scored: `{metrics.height}`\n"
            f"- diagnostic_passes: `{passes.height}`\n\n"
            "## Top Forecast Diagnostics\n\n"
            f"{_markdown_table(best)}\n\n"
            "## Interpretation\n\n"
            f"{_interpret(metrics)}\n"
        )
    note_block = ""
    if notes:
        note_block = "\n## Notes\n\n" + "\n".join(f"- {note}" for note in notes) + "\n"
    return f"# Forecast Diagnostic Verdict\n\n- generated_at: `{generated_at}`\n\n{body}{note_block}"


def _interpret(metrics: pl.DataFrame) -> str:
    passes = metrics.filter(pl.col("verdict") == "pass")
    if passes.is_empty():
        return (
            "No baseline forecast passed the diagnostic hurdle. A foundation model "
            "must beat these baselines before it should be wired into strategy gates."
        )
    return (
        "At least one forecast model passed the diagnostic hurdle. The next step "
        "is to compare it against lag-safe baselines on the same timestamps before "
        "any strategy-class integration."
    )


def _markdown_table(df: pl.DataFrame) -> str:
    columns = [
        "ticker",
        "model",
        "horizon_bars",
        "direction_hit_rate",
        "top_quartile_avg_signed_return_bps",
        "pearson_corr",
        "verdict",
    ]
    present = [column for column in columns if column in df.columns]
    if not present or df.is_empty():
        return "_No rows._"
    lines = [
        "| " + " | ".join(present) + " |",
        "| " + " | ".join("---" for _ in present) + " |",
    ]
    for row in df.select(present).to_dicts():
        lines.append("| " + " | ".join(str(row.get(column, "")) for column in present) + " |")
    return "\n".join(lines)


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_int_csv(value: str) -> list[int]:
    return [int(item) for item in _parse_csv(value)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", default="SPY,QQQ,AMD,TSLA")
    parser.add_argument("--start", default="2024-01-02")
    parser.add_argument("--end", default="2026-02-28")
    parser.add_argument("--horizons", default=",".join(str(v) for v in DEFAULT_HORIZONS))
    parser.add_argument("--models", default=",".join(DEFAULT_MODELS))
    parser.add_argument("--sample-every", type=int, default=5)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--write-rows", action="store_true")
    args = parser.parse_args(argv)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else DEFAULT_OUTPUT_ROOT / timestamp
    )
    verdict_path = run_diagnostic(
        tickers=[ticker.upper() for ticker in _parse_csv(args.tickers)],
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        horizons=_parse_int_csv(args.horizons),
        models=_parse_csv(args.models),
        sample_every=args.sample_every,
        output_dir=output_dir,
        write_rows=args.write_rows,
    )
    print(verdict_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
