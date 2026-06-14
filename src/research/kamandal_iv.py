"""Reader for kamandal's captured option IV (the real-IV source of truth).

The kamandal app (`/Users/suman/code/kamandal_v2`) captures real ATM implied
vol per symbol into `kamandal_v2.db:iv_snapshots` (metric `atm_30_45_mean_iv`,
and `atm_2_10_mean_iv` once short-dated capture ships). This module reads it so
the S4 option-translation scorer can calibrate its modeled IV against real
captured IV per symbol — rather than mala_v2 duplicating the collection.

`iv` is stored in percent in the db; we return decimals. Returns None whenever
no captured IV (or no cached bars) is available, so callers fall back to the
modeled `iv_premium_factor`.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from src.chronos.storage import LocalStorage
from src.research.option_translation import annualized_realized_vol


def _resolve_kamandal_db() -> Path:
    """Find the kamandal IV db across machines. mala runs on oldmac (where IV is
    captured) and on this dev Mac; honor KAMANDAL_DB_PATH, else the first
    existing known location, else the dev path."""
    candidates = [
        os.environ.get("KAMANDAL_DB_PATH"),
        "/Users/sunny/Documents/kamandal_v2/data/kamandal_v2.db",   # oldmac (prod capture)
        "/Users/suman/code/kamandal_v2/data/kamandal_v2.db",        # this Mac (dev)
        str(Path.home() / "Documents/kamandal_v2/data/kamandal_v2.db"),
        str(Path.home() / "code/kamandal_v2/data/kamandal_v2.db"),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return Path(c)
    return Path("/Users/suman/code/kamandal_v2/data/kamandal_v2.db")


DEFAULT_KAMANDAL_DB = _resolve_kamandal_db()
# Prefer short-dated IV (matches the 2-7 DTE profiles); fall back to 30-45 DTE.
PREFERRED_METRICS = ("atm_2_10_mean_iv", "atm_30_45_mean_iv")


def latest_atm_iv(symbol: str, *, db_path: Path | str = DEFAULT_KAMANDAL_DB,
                  metrics: tuple[str, ...] = PREFERRED_METRICS) -> tuple[float, date, str] | None:
    """Most recent captured ATM IV (decimal) for a symbol, trying the shortest
    available tenor first. Returns (iv, snapshot_date, metric) or None."""
    p = Path(db_path)
    if not p.exists():
        return None
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        for metric in metrics:
            row = con.execute(
                "SELECT iv, snapshot_date FROM iv_snapshots "
                "WHERE symbol = ? AND metric = ? ORDER BY snapshot_date DESC LIMIT 1",
                (symbol.upper(), metric),
            ).fetchone()
            if row and row[0]:
                return float(row[0]) / 100.0, date.fromisoformat(str(row[1])), metric
        return None
    except sqlite3.Error:
        return None
    finally:
        con.close()


def real_iv_factor(symbol: str, *, db_path: Path | str = DEFAULT_KAMANDAL_DB,
                   lookback_days: int = 60) -> float | None:
    """Empirical iv_premium_factor = captured ATM IV / annualized realized vol.

    Realized vol is taken from the local bar cache over the window ending at the
    IV snapshot date. None when captured IV or cached bars are unavailable.
    """
    res = latest_atm_iv(symbol, db_path=db_path)
    if res is None:
        return None
    iv, snap_date, _metric = res
    try:
        bars = LocalStorage().load_bars(symbol.upper(), snap_date - timedelta(days=lookback_days), snap_date)
    except Exception:
        return None
    if bars.is_empty():
        return None
    rv = annualized_realized_vol(bars)
    return iv / rv if rv > 0 else None
