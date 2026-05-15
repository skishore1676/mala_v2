"""
Central configuration for mala_v2.

Uses pydantic-settings to pull from .env and provide typed, validated config.
Only the settings needed for local research are included — no Sheets, no Bhiksha.
"""

from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_env_file() -> str | None:
    env_path = PROJECT_ROOT / ".env"
    try:
        if env_path.is_file():
            return str(env_path)
    except (PermissionError, OSError):
        pass
    return None


_ENV_FILE = _resolve_env_file()


class Settings(BaseSettings):
    """Application-wide settings, loaded from .env at project root."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── API Keys ─────────────────────────────────────────────────────────
    market_data_provider: str = Field(
        default="polygon",
        description="Market-data provider for new Chronos fetches: public or polygon.",
    )
    public_secret_token: str = Field(
        default="",
        description="Public API secret token used to mint short-lived access tokens.",
    )
    public_access_token: str = Field(
        default="",
        description="Optional pre-minted Public API access token for market data.",
    )
    public_api_base_url: str = Field(
        default="https://api.public.com",
        description="Public API base URL.",
    )
    public_auth_endpoint: str = Field(
        default="https://api.public.com/userapiauthservice/personal/access-tokens",
        description="Public API personal access-token endpoint.",
    )
    public_bars_period: str = Field(
        default="FIVE_YEARS",
        description="Public historical bars period to request before local date filtering.",
    )
    public_bars_aggregation: str = Field(
        default="ONE_MINUTE",
        description="Public historical bars aggregation.",
    )
    public_instrument_type: str = Field(
        default="EQUITY",
        description="Public instrument type for default Chronos historical bars.",
    )
    polygon_api_key: str = Field(
        default="",
        description="Optional Polygon.io API key for fallback market data.",
    )

    # ── Data Pipeline Defaults ───────────────────────────────────────────
    default_tickers: List[str] = Field(
        default=["SPY", "QQQ", "IWM"],
        description="Tickers to download by default.",
    )
    lookback_years: int = Field(
        default=2,
        description="Number of years of historical data to fetch.",
    )

    # ── Physics Engine Defaults ──────────────────────────────────────────
    vpoc_lookback_bars: int = Field(
        default=240,
        description="Rolling window size for VPOC (4 hrs × 60 min).",
    )

    # ── Strategy Defaults ────────────────────────────────────────────────
    ema_periods: List[int] = Field(
        default=[4, 8, 12],
        description="EMA periods for the momentum strategy.",
    )
    volume_ma_period: int = Field(
        default=20,
        description="Moving-average window for volume filter.",
    )

    # ── Market Impulse Defaults ──────────────────────────────────────────
    vma_length: int = Field(
        default=10,
        description="Lookback for the VMA directional strength.",
    )
    vwma_periods: List[int] = Field(
        default=[8, 21, 34],
        description="VWMA periods for the impulse regime stack.",
    )
    impulse_entry_buffer_minutes: int = Field(
        default=3,
        description="Minutes after open before entries are allowed.",
    )
    impulse_entry_window_minutes: int = Field(
        default=60,
        description="Maximum minutes after open for entries.",
    )
    market_open_hour: int = Field(
        default=9,
        description="Market open hour (ET).",
    )
    market_open_minute: int = Field(
        default=30,
        description="Market open minute (ET).",
    )

    # ── Oracle Defaults ──────────────────────────────────────────────────
    forward_window_bars: int = Field(
        default=15,
        description="Bars to look ahead for MFE / MAE (15 × 1-min = 15 min).",
    )

    # ── Google Sheets (Mala evidence; legacy env names retained) ──────────────
    google_api_credentials_path: str = Field(
        default="",
        description="Path to service-account JSON for Google Sheets access.",
    )
    strategy_catalog_sheet_id: str = Field(
        default="",
        description="Spreadsheet ID for the canonical Mala evidence workbook (Mala_Evidence_v1; legacy STRATEGY_CATALOG_SHEET_ID env name retained for compatibility).",
    )
    strategy_catalog_sheet_name: str = Field(
        default="Mala_Evidence_v1",
        description="Canonical Mala evidence tab name; legacy Strategy_Catalog is read-only/deprecated.",
    )


# Singleton – importable everywhere as `from src.config import settings`
settings = Settings()
