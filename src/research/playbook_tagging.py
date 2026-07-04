"""Playbook tagging for the operator's personal round-trips (P1 of the
Playbook Discovery Program, docs/PLAYBOOK_DISCOVERY_PROGRAM.md).

Tags each timestamped long-premium EPISODE (same-symbol / same-direction fills
clustered within ~10 minutes — spec X2) with one of the four playbooks
(FLASH_REVERSAL, EXHAUSTION_REVERSAL, TREND_CONTINUATION, RANGE_EXPANSION), a
confidence tier (HIGH / MEDIUM), and a reason string that cites the spec.
UNCLASSIFIED is a legal output — ambiguous episodes are never force-fit.

Spec source of truth: docs/EXIT_PROFILE_PLAYBOOKS.md § "P0 Spec-Lock
(2026-07-04)". Rule IDs in reasons (F*, E*, T*, R*) refer to that section via
docs/PLAYBOOK_SPEC_LOCK_QUESTIONNAIRE.md numbering.

Hard rule (P1 no-leakage): PnL and exit prices are NEVER inputs to feature
extraction or tagging. They may appear only in downstream fingerprint
reporting.
"""

from __future__ import annotations

import csv
import math
from bisect import bisect_left, insort
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

ET = ZoneInfo("America/New_York")

SESSION_START = time(9, 30)
SESSION_END = time(16, 0)

# X2: same-symbol/same-direction entries within ~10 min are one episode.
EPISODE_GAP_MINUTES = 10.0

# ── Trades & episodes ────────────────────────────────────────────────────────


@dataclass
class Trade:
    underlying: str
    entry_dt: datetime  # tz-aware ET
    thesis_dir: int  # +1 long call (bullish), -1 long put (bearish)
    dte: int | None
    holding_minutes: float | None
    pnl: float | None  # carried for FINGERPRINT ONLY — never read by tagging
    opening_fill_id: str


@dataclass
class Episode:
    episode_id: str
    underlying: str
    thesis_dir: int
    entry_dt: datetime  # first fill's entry (ET)
    n_fills: int
    dte: int | None
    holding_minutes: float | None  # max across fills
    pnl: float | None  # summed — FINGERPRINT ONLY
    features: dict = field(default_factory=dict)
    tag: str = "UNCLASSIFIED"
    confidence: str = "NONE"
    reason: str = ""


def load_timestamped_trades(csv_path: Path) -> list[Trade]:
    """Long-premium trades with intraday entry timestamps (spec X1: short
    premium is parked; X4: call=bullish, put=bearish)."""
    trades: list[Trade] = []
    with open(csv_path, newline="") as fh:
        for row in csv.DictReader(fh):
            if row["has_intraday_time"] != "True":
                continue
            if row["opening_side"] != "LONG":
                continue
            right = row["option_right"].strip().upper()
            if right not in ("C", "P"):
                continue
            entry_dt = datetime.fromisoformat(row["opened_at_et"]).astimezone(ET)
            trades.append(
                Trade(
                    underlying=row["underlying"],
                    entry_dt=entry_dt,
                    thesis_dir=1 if right == "C" else -1,
                    dte=int(row["dte_at_entry"]) if row["dte_at_entry"] else None,
                    holding_minutes=(
                        float(row["holding_minutes"]) if row["holding_minutes"] else None
                    ),
                    pnl=float(row["pnl"]) if row["pnl"] else None,
                    opening_fill_id=row["opening_fill_id"],
                )
            )
    trades.sort(key=lambda t: (t.underlying, t.thesis_dir, t.entry_dt))
    return trades


def cluster_episodes(trades: list[Trade]) -> list[Episode]:
    episodes: list[Episode] = []
    current: list[Trade] = []

    def flush() -> None:
        if not current:
            return
        first = current[0]
        pnls = [t.pnl for t in current if t.pnl is not None]
        holds = [t.holding_minutes for t in current if t.holding_minutes is not None]
        episodes.append(
            Episode(
                episode_id=(
                    f"{first.underlying}_{'L' if first.thesis_dir > 0 else 'S'}_"
                    f"{first.entry_dt:%Y%m%d_%H%M%S}"
                ),
                underlying=first.underlying,
                thesis_dir=first.thesis_dir,
                entry_dt=first.entry_dt,
                n_fills=len(current),
                dte=first.dte,
                holding_minutes=max(holds) if holds else None,
                pnl=sum(pnls) if pnls else None,
            )
        )

    for trade in trades:
        if current and (
            trade.underlying != current[0].underlying
            or trade.thesis_dir != current[0].thesis_dir
            or (trade.entry_dt - current[-1].entry_dt).total_seconds() / 60.0
            > EPISODE_GAP_MINUTES
        ):
            flush()
            current = []
        current.append(trade)
    flush()
    episodes.sort(key=lambda e: e.entry_dt)
    return episodes


# ── Symbol bar context ───────────────────────────────────────────────────────


class SymbolBars:
    """All cached 1-min session bars for one symbol, plus the pooled baselines
    the spec needs (stretch percentile CDF, typical 15-min move, volume norms,
    14-day ATR)."""

    def __init__(self, symbol: str, data_dir: Path) -> None:
        self.symbol = symbol
        sym_dir = data_dir / symbol
        files = sorted(sym_dir.glob("*.parquet")) if sym_dir.is_dir() else []
        if not files:
            raise FileNotFoundError(f"no bar cache for {symbol} under {data_dir}")
        frames = []
        for f in files:
            part = pl.read_parquet(f)
            if part.schema["timestamp"].time_zone is None:  # legacy naive-UTC files
                part = part.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
            frames.append(part)
        df = pl.concat(frames, how="vertical_relaxed")
        df = (
            df.with_columns(pl.col("timestamp").dt.convert_time_zone("America/New_York"))
            .sort("timestamp")
            .with_columns(
                pl.col("timestamp").dt.date().alias("d"),
                pl.col("timestamp").dt.time().alias("t"),
            )
        )
        session = df.filter(
            (pl.col("t") >= SESSION_START) & (pl.col("t") < SESSION_END)
        )
        self.days: dict[date, dict[str, np.ndarray]] = {}
        for (day,), grp in session.group_by("d", maintain_order=True):
            # dt.hour() is Int8 in polars — cast BEFORE arithmetic or hour*60
            # wraps mod 256 and the minute axis silently corrupts.
            ts = (
                grp["timestamp"].dt.hour().cast(pl.Int64) * 60
                + grp["timestamp"].dt.minute().cast(pl.Int64)
            )
            close = grp["close"].to_numpy()
            vol = grp["volume"].to_numpy()
            vwap_px = grp["vwap"].to_numpy()
            cum_vol = np.cumsum(vol)
            cum_pv = np.cumsum(vwap_px * vol)
            with np.errstate(invalid="ignore", divide="ignore"):
                sess_vwap = np.where(cum_vol > 0, cum_pv / cum_vol, close)
            self.days[day] = {
                "min_of_day": ts.to_numpy().astype(np.int64),
                "open": grp["open"].to_numpy(),
                "high": grp["high"].to_numpy(),
                "low": grp["low"].to_numpy(),
                "close": close,
                "volume": vol,
                "vwap": sess_vwap,
            }
        self.day_list = sorted(self.days)
        self._day_index = {d: i for i, d in enumerate(self.day_list)}
        self._build_baselines()

    # 14-session trailing ATR as a fraction of close (day granularity).
    def atr14_pct(self, day: date) -> float:
        idx = self._day_index.get(day)
        if idx is None:
            return float("nan")
        lo = max(0, idx - 14)
        vals = []
        for d in self.day_list[lo:idx] or self.day_list[max(0, idx - 1) : idx + 1]:
            bars = self.days[d]
            rng = float(bars["high"].max() - bars["low"].min())
            c = float(bars["close"][-1])
            if c > 0:
                vals.append(rng / c)
        return float(np.mean(vals)) if vals else float("nan")

    def prior_close(self, day: date) -> float | None:
        idx = self._day_index.get(day)
        if not idx:
            return None
        return float(self.days[self.day_list[idx - 1]]["close"][-1])

    def _build_baselines(self) -> None:
        """Pooled, per-symbol baselines sampled every 5 minutes across all
        cached sessions. stretch = (close - session_vwap) / (atr14_pct*close);
        the CDF of |stretch| powers the E-spec percentile rule."""
        stretch_samples: list[float] = []
        ret15_samples: list[float] = []
        vol15_by_bucket: dict[int, list[float]] = {}
        width90_samples: list[float] = []
        for day in self.day_list:
            bars = self.days[day]
            atr = self.atr14_pct(day)
            close, vwap, vol = bars["close"], bars["vwap"], bars["volume"]
            mins = bars["min_of_day"]
            n = len(close)
            if n < 30 or not math.isfinite(atr) or atr <= 0:
                continue
            for i in range(15, n, 5):
                px = close[i]
                if px <= 0:
                    continue
                stretch_samples.append(abs(px - vwap[i]) / (atr * px))
                ret15_samples.append(abs(px / close[i - 15] - 1.0))
                bucket = int(mins[i] // 30)
                vol15_by_bucket.setdefault(bucket, []).append(float(vol[i - 14 : i + 1].sum()))
                if i >= 90:
                    win = close[i - 90 : i + 1]
                    width90_samples.append((win.max() - win.min()) / px)
        self.stretch_cdf = np.sort(np.array(stretch_samples)) if stretch_samples else np.array([])
        self.typical_ret15 = float(np.median(ret15_samples)) if ret15_samples else float("nan")
        self.vol15_norm = {
            b: float(np.median(v)) for b, v in vol15_by_bucket.items() if v
        }
        self.width90_cdf = np.sort(np.array(width90_samples)) if width90_samples else np.array([])

    def stretch_percentile(self, value: float) -> float:
        if not len(self.stretch_cdf):
            return float("nan")
        return 100.0 * bisect_left(self.stretch_cdf, abs(value)) / len(self.stretch_cdf)

    def width90_percentile(self, value: float) -> float:
        if not len(self.width90_cdf):
            return float("nan")
        return 100.0 * bisect_left(self.width90_cdf, value) / len(self.width90_cdf)


def _vwma(close: np.ndarray, vol: np.ndarray, idx: int, periods: int, step: int) -> float:
    """Volume-weighted MA of `periods` buckets of `step` minutes ending at idx."""
    lo = idx + 1 - periods * step
    if lo < 0:
        return float("nan")
    c, v = close[lo : idx + 1], vol[lo : idx + 1]
    tv = v.sum()
    return float((c * v).sum() / tv) if tv > 0 else float(np.mean(c))


def extract_features(bars: SymbolBars, entry_dt: datetime, thesis_dir: int) -> dict:
    """Entry-context features at the bar BEFORE the entry minute. Returns {}
    when the day is missing from the cache or the entry is outside the
    session."""
    day = entry_dt.date()
    if day not in bars.days:
        return {}
    b = bars.days[day]
    entry_min = entry_dt.hour * 60 + entry_dt.minute
    idx = int(np.searchsorted(b["min_of_day"], entry_min)) - 1
    if idx < 5:
        return {}
    close, vwap, vol, hi, lo = b["close"], b["vwap"], b["volume"], b["high"], b["low"]
    mins = b["min_of_day"]
    px = float(close[idx])
    atr = bars.atr14_pct(day)
    f: dict = {"px": px, "minutes_since_open": int(mins[idx] - (9 * 60 + 30))}

    def ret(lookback: int) -> float:
        j = int(np.searchsorted(mins, mins[idx] - lookback))
        return px / float(close[max(0, min(j, idx))]) - 1.0 if idx > 0 else 0.0

    f["ret_5"], f["ret_15"], f["ret_30"], f["ret_60"] = (
        ret(5), ret(15), ret(30), ret(60),
    )
    f["vwap_dist_pct"] = px / float(vwap[idx]) - 1.0
    f["vwap_dist_atr"] = (
        f["vwap_dist_pct"] / atr if math.isfinite(atr) and atr > 0 else float("nan")
    )
    f["stretch_pctile"] = bars.stretch_percentile(f["vwap_dist_atr"])
    # Day extreme in the direction of the preceding move (the move being faded).
    move_dir = 1 if f["ret_15"] >= 0 else -1
    if move_dir > 0:
        ext_idx = int(np.argmax(hi[: idx + 1]))
        f["extreme_px"] = float(hi[ext_idx])
    else:
        ext_idx = int(np.argmin(lo[: idx + 1]))
        f["extreme_px"] = float(lo[ext_idx])
    f["move_dir"] = move_dir
    f["fresh_extreme_min"] = int(mins[idx] - mins[ext_idx])
    # Failed retest (E-spec entry): within the last 30 min price approached the
    # prior extreme (within 0.15 * day ATR) without taking it out.
    retest_lo = max(0, idx - 30)
    tol = 0.15 * atr * px if math.isfinite(atr) else 0.0015 * px
    if move_dir > 0:
        recent_hi = float(hi[retest_lo : idx + 1].max())
        f["failed_retest"] = bool(
            ext_idx < retest_lo and recent_hi < f["extreme_px"] and
            recent_hi > f["extreme_px"] - tol
        )
    else:
        recent_lo = float(lo[retest_lo : idx + 1].min())
        f["failed_retest"] = bool(
            ext_idx < retest_lo and recent_lo > f["extreme_px"] and
            recent_lo < f["extreme_px"] + tol
        )
    # Run length of consecutive same-direction 1-min closes before entry.
    run = 0
    for j in range(idx, 0, -1):
        step_dir = 1 if close[j] >= close[j - 1] else -1
        if step_dir != move_dir:
            break
        run += 1
    f["run_len"] = run
    # Violence: |15m ret| vs symbol's typical |15m ret|; volume vs clock norm.
    f["flash_mag"] = (
        abs(f["ret_15"]) / bars.typical_ret15
        if math.isfinite(bars.typical_ret15) and bars.typical_ret15 > 0
        else float("nan")
    )
    norm = bars.vol15_norm.get(int(mins[idx] // 30))
    f["vol_ratio_15"] = (
        float(vol[max(0, idx - 14) : idx + 1].sum()) / norm if norm else float("nan")
    )
    # Trend state (T-spec): side-of-VWAP persistence over the last 60 min and
    # the 10-period VWMA on 5-min buckets (the operator's anchor).
    t_lo = max(0, idx - 59)
    above = float((close[t_lo : idx + 1] > vwap[t_lo : idx + 1]).mean())
    f["trend_side_frac_60"] = max(above, 1.0 - above)
    f["trend_dir"] = 1 if above >= 0.5 else -1
    f["vma10_5m"] = _vwma(close, vol, idx, periods=10, step=5)
    f["vma10_dist_pct"] = (
        px / f["vma10_5m"] - 1.0 if math.isfinite(f["vma10_5m"]) else float("nan")
    )
    f["touched_vma10"] = bool(
        math.isfinite(f["vma10_5m"]) and abs(f["vma10_dist_pct"]) <= 0.25 * (atr or 0.01)
    )
    # Pullback depth vs the last 90-min leg in the trend direction.
    leg_lo_i = max(0, idx - 90)
    leg_hi = float(hi[leg_lo_i : idx + 1].max())
    leg_lo = float(lo[leg_lo_i : idx + 1].min())
    if leg_hi > leg_lo:
        f["pullback_depth_frac"] = (
            (leg_hi - px) / (leg_hi - leg_lo)
            if f["trend_dir"] > 0
            else (px - leg_lo) / (leg_hi - leg_lo)
        )
    else:
        f["pullback_depth_frac"] = float("nan")
    # Compression (R-spec a): rolling 90-min width percentile vs symbol norm.
    if idx >= 90:
        width = (float(close[idx - 90 : idx + 1].max()) - float(close[idx - 90 : idx + 1].min())) / px
        f["range_width_90m_pctile"] = bars.width90_percentile(width)
    else:
        f["range_width_90m_pctile"] = float("nan")
    # Gap (R-spec b): today's open vs prior session close.
    pc = bars.prior_close(day)
    f["gap_pct"] = (float(b["open"][0]) / pc - 1.0) if pc else float("nan")
    return f


# ── The tagger ───────────────────────────────────────────────────────────────

PLAYBOOKS = (
    "FLASH_REVERSAL",
    "EXHAUSTION_REVERSAL",
    "TREND_CONTINUATION",
    "RANGE_EXPANSION",
)


def tag_episode(f: dict, thesis_dir: int) -> tuple[str, str, str]:
    """Deterministic spec-citing tagger. Returns (tag, confidence, reason).

    Precedence when several playbooks match: FLASH beats EXHAUSTION only when
    the flush is fresh AND violent (P0 boundary rule); reversal tags beat
    TREND/RANGE (a counter-move thesis cannot be continuation); TREND beats
    RANGE when a same-session trend is established (T5 default)."""
    if not f:
        return "UNCLASSIFIED", "NONE", "no bar context (missing day or pre-open entry)"

    counter_move = f["move_dir"] == -thesis_dir and abs(f["ret_15"]) > 0
    against_stretch = (
        math.isfinite(f["vwap_dist_atr"]) and np.sign(f["vwap_dist_atr"]) == -thesis_dir
    )
    fresh = f["fresh_extreme_min"] <= 15
    violent = (f.get("flash_mag") or 0) >= 2.5 or (
        f["run_len"] >= 4 and (f.get("vol_ratio_15") or 0) >= 2.5
    )
    stretch_p = f.get("stretch_pctile", float("nan"))

    # FLASH_REVERSAL — F1/F5 + operator: flash is AGAINST the recent trend,
    # faded fresh (extreme ≤ ~15 min old, ≥ ~2.5× normal 15-min range).
    if counter_move and fresh and violent:
        return (
            "FLASH_REVERSAL",
            "HIGH",
            f"F1/F5: fades a fresh violent flush ({f['flash_mag']:.1f}x typical 15m move, "
            f"extreme {f['fresh_extreme_min']}m ago, run {f['run_len']})",
        )
    # EXHAUSTION_REVERSAL — operator reframe: stretch at the ~85–90th pctile
    # (any symbol), entry = inability to cross the prior extreme.
    if against_stretch and math.isfinite(stretch_p) and stretch_p >= 88 and (
        f["failed_retest"] or not fresh
    ):
        return (
            "EXHAUSTION_REVERSAL",
            "HIGH",
            f"E1': stretch at p{stretch_p:.0f} vs own history"
            + (", failed retest of extreme" if f["failed_retest"] else
               f", extreme {f['fresh_extreme_min']}m old (mature)"),
        )
    if counter_move and fresh and (f.get("flash_mag") or 0) >= 1.8:
        return (
            "FLASH_REVERSAL",
            "MEDIUM",
            f"F1 partial: fresh counter-move fade at {f['flash_mag']:.1f}x typical "
            f"(below the 2.5x bar)",
        )
    if against_stretch and math.isfinite(stretch_p) and stretch_p >= 80:
        return (
            "EXHAUSTION_REVERSAL",
            "MEDIUM",
            f"E1' partial: stretch p{stretch_p:.0f} (below the p88 bar)",
        )
    # TREND_CONTINUATION — T1/T2 + operator anchor: with-trend thesis, 10 VMA
    # touch (or shallow pullback) in an established same-session trend.
    with_trend = f["trend_dir"] == thesis_dir and f["trend_side_frac_60"] >= 0.75
    pull = f.get("pullback_depth_frac", float("nan"))
    if with_trend and (f["touched_vma10"] or (math.isfinite(pull) and 0.1 <= pull <= 0.6)):
        anchor = "10-VMA touch" if f["touched_vma10"] else f"pullback {pull:.0%} of leg"
        return (
            "TREND_CONTINUATION",
            "HIGH",
            f"T1/T2': with-trend ({f['trend_side_frac_60']:.0%} of last 60m one side of "
            f"VWAP), {anchor}",
        )
    # RANGE_EXPANSION — R1/R2 compression breakout, or operator's earnings-gap
    # continuation sub-case.
    width_p = f.get("range_width_90m_pctile", float("nan"))
    if (
        math.isfinite(width_p)
        and width_p <= 30
        and f["minutes_since_open"] >= 90
    ):
        return (
            "RANGE_EXPANSION",
            "HIGH",
            f"R1: 90m range width at p{width_p:.0f} (compressed base), positioned for expansion",
        )
    gap = f.get("gap_pct", float("nan"))
    if (
        math.isfinite(gap)
        and abs(gap) >= 0.015
        and np.sign(gap) == thesis_dir
        and f["minutes_since_open"] <= 120
    ):
        return (
            "RANGE_EXPANSION",
            "MEDIUM",
            f"R-gap (operator): {gap:+.1%} gap continuation within first 2h",
        )
    if with_trend:
        return (
            "TREND_CONTINUATION",
            "MEDIUM",
            f"T1 partial: with-trend ({f['trend_side_frac_60']:.0%}) but no clear "
            f"10-VMA touch / pullback",
        )
    return (
        "UNCLASSIFIED",
        "NONE",
        f"no rule met: move_dir={f['move_dir']:+d} vs thesis {thesis_dir:+d}, "
        f"stretch p{stretch_p:.0f}, trend {f['trend_side_frac_60']:.0%}, "
        f"width p{width_p:.0f}" if math.isfinite(width_p) else
        f"no rule met: stretch p{stretch_p:.0f}, trend {f['trend_side_frac_60']:.0%}",
    )
