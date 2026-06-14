#!/usr/bin/env python3
"""Calibrate the S4 IV model against real Public option IV + collect daily snapshots.

For each symbol it computes realized vol (from the local cache) and the real ATM
implied vol (live, Public), then reports the empirical
``iv_premium_factor = real_IV / realized_vol`` and flags "live surprises" where
it diverges from the modeled default (1.2x in option_translation). Every run
appends a row to ``data/iv_snapshots/`` so a real historical IV series accrues
for future, real-IV-driven backtests (the historical chain we don't have yet).

LIVE NOTE: the real-IV side needs PUBLIC_ACCESS_TOKEN / PUBLIC_SECRET_TOKEN and
**market hours** — run it Monday. The realized-vol + collector side works any
time; the live side degrades gracefully (reports "unavailable") off-hours or
without creds, so a Saturday run still records realized vol.

    python scripts/calibrate_iv.py --symbols IWM,SPY,TSLA,NVDA,AMD,META
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import polars as pl
from loguru import logger

from src.chronos.storage import LocalStorage
from src.research.option_translation import annualized_realized_vol

logger.remove()
logger.add(sys.stderr, level="ERROR")

MODELED_FACTOR = 1.2          # option_translation default; what we're calibrating
SURPRISE_BAND = (0.9, 1.6)    # real factor outside this vs MODELED -> flag


# ── Pure logic (tested) ─────────────────────────────────────────────────────────

def compute_factor(real_iv: float | None, realized_vol: float | None) -> float | None:
    if not real_iv or not realized_vol or realized_vol <= 0:
        return None
    return real_iv / realized_vol


def extract_atm_iv(chain: dict, spot: float) -> float | None:
    """Tolerant ATM-IV reader for a Public option-chain payload.

    Scans calls+puts for the contract whose strike is nearest spot and returns
    its implied vol, looking under a few likely field names.
    """
    contracts = []
    for side in ("calls", "puts", "call", "put"):
        v = chain.get(side)
        if isinstance(v, list):
            contracts.extend(v)
    if not contracts and isinstance(chain.get("options"), list):
        contracts = chain["options"]

    def _iv(c: dict):
        for k in ("impliedVolatility", "implied_volatility", "iv"):
            if k in c and c[k] not in (None, ""):
                return float(c[k])
        g = c.get("greeks")
        if isinstance(g, dict):
            for k in ("impliedVolatility", "iv"):
                if k in g and g[k] not in (None, ""):
                    return float(g[k])
        return None

    def _strike(c: dict):
        for k in ("strike", "strikePrice", "strike_price"):
            if k in c and c[k] not in (None, ""):
                return float(c[k])
        return None

    best, best_dist = None, float("inf")
    for c in contracts:
        s, iv = _strike(c), _iv(c)
        if s is None or iv is None:
            continue
        d = abs(s - spot)
        if d < best_dist:
            best, best_dist = iv, d
    return best


def realized_vol_for(symbol: str, lookback_days: int = 45) -> float | None:
    end = datetime.now().date()
    start = end - timedelta(days=lookback_days)
    try:
        bars = LocalStorage().load_bars(symbol, start, end)
    except Exception:
        return None
    if bars.is_empty():
        return None
    return annualized_realized_vol(bars)


# ── Live Public IV (faithful to public_api_trading_v3; validate Monday) ──────────

def fetch_live_atm_iv(symbol: str) -> float | None:
    """Best-effort live ATM IV via Public. Returns None (never raises) when the
    market is closed, creds are absent, or the response shape differs."""
    try:
        import requests
        from src.chronos.client import PublicMarketDataClient

        client = PublicMarketDataClient()
        token = client._get_access_token()
        if not token:
            return None
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        base = client.base_url

        acct = requests.get(f"{base}/userapigateway/trading/account", headers=headers, timeout=15)
        account_id = None
        if acct.ok:
            data = acct.json()
            account_id = data.get("accountId") or (data.get("accounts") or [{}])[0].get("accountId")
        if not account_id:
            return None

        spot_bars = LocalStorage().load_bars(symbol, datetime.now().date() - timedelta(days=5),
                                             datetime.now().date())
        spot = float(spot_bars["close"].to_list()[-1]) if not spot_bars.is_empty() else None
        if spot is None:
            return None

        expiry = (datetime.now().date() + timedelta(days=7)).isoformat()
        payload = {"instrument": {"symbol": symbol, "type": "EQUITY"}, "expirationDate": expiry}
        resp = requests.post(f"{base}/userapigateway/marketdata/{account_id}/option-chain",
                             headers=headers, json=payload, timeout=20)
        if not resp.ok:
            return None
        return extract_atm_iv(resp.json() or {}, spot)
    except Exception:
        return None


# ── Collector ────────────────────────────────────────────────────────────────────

def append_snapshot(rows: list[dict], out_dir: Path, day: date) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"iv_snapshot_{day.isoformat()}.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default="IWM,SPY,TSLA,NVDA,AMD,META")
    p.add_argument("--out-dir", default=str(REPO_ROOT / "data" / "iv_snapshots"))
    args = p.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    today = datetime.now().date()

    print(f"IV calibration {today}  (modeled factor = {MODELED_FACTOR})\n")
    print(f"  {'sym':>5s} {'realized_vol':>12s} {'real_IV':>8s} {'factor':>7s} {'note':>22s}")
    rows = []
    for sym in symbols:
        rv = realized_vol_for(sym)
        iv = fetch_live_atm_iv(sym)
        factor = compute_factor(iv, rv)
        note = ""
        if iv is None:
            note = "live IV unavailable"
        elif factor and not (SURPRISE_BAND[0] <= factor / MODELED_FACTOR <= SURPRISE_BAND[1]):
            note = "** SURPRISE vs modeled"
        print(f"  {sym:>5s} {rv if rv else float('nan'):>12.3f} "
              f"{iv if iv else float('nan'):>8.3f} {factor if factor else float('nan'):>7.2f} {note:>22s}")
        rows.append({"date": today.isoformat(), "symbol": sym, "realized_vol": rv,
                     "real_iv": iv, "iv_premium_factor": factor})

    path = append_snapshot(rows, Path(args.out_dir), today)
    have_iv = sum(1 for r in rows if r["real_iv"])
    print(f"\n  collected {len(rows)} rows ({have_iv} with live IV) -> {path}")
    if have_iv == 0:
        print("  (no live IV — off-hours or creds absent. Re-run Monday in market hours.)")


if __name__ == "__main__":
    main()
