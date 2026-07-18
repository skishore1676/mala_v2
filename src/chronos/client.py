"""
Market-data clients for the Chronos data pipeline.

Handles:
  - Authentication via API key
  - Fetching 1-minute OHLCV aggregates
  - Rate-limit-aware retries
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from typing import List, Optional, Protocol

import requests
from loguru import logger
from tqdm import tqdm

from src.config import settings

# ── Constants ────────────────────────────────────────────────────────────────
BASE_URL = "https://api.polygon.io"
AGGS_ENDPOINT = "/v2/aggs/ticker/{ticker}/range/1/minute/{start}/{end}"
MAX_RESULTS_PER_PAGE = 50_000  # Polygon max limit per request
REQUEST_DELAY_S = 0.25  # polite pause between paginated calls

PUBLIC_BARS_ENDPOINT = "/userapigateway/historicdata/{type}/{symbol}/{period}/{aggregation}"
PUBLIC_INSTRUMENTS_ENDPOINT = "/userapigateway/trading/instruments"
PUBLIC_SESSIONS = ("preMarket", "regularMarket", "afterMarket")


class ChronosMarketDataClient(Protocol):
    """Provider interface consumed by the Chronos pipeline."""

    def fetch_aggs(
        self,
        ticker: str,
        start: date,
        end: date,
        adjusted: bool = True,
    ) -> List[dict]:
        """Fetch provider bars normalized to Polygon-style aggregate dicts."""


class PolygonClient:
    """Thin wrapper around the Polygon.io Aggregates API."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or settings.polygon_api_key
        if not self.api_key:
            raise ValueError(
                "POLYGON_API_KEY is not set. "
                "Provide it in .env or pass it explicitly."
            )
        self._session = requests.Session()
        self._session.params = {"apiKey": self.api_key}  # type: ignore[assignment]

    # ── Public API ───────────────────────────────────────────────────────
    def fetch_aggs(
        self,
        ticker: str,
        start: date,
        end: date,
        adjusted: bool = True,
    ) -> List[dict]:
        """
        Download 1-minute aggregates for *ticker* between *start* and *end*
        (inclusive). Automatically paginates if the window exceeds one page.

        Returns a list of bar dicts with keys:
            t, o, h, l, c, v, vw, n
        """
        all_results: List[dict] = []
        url = BASE_URL + AGGS_ENDPOINT.format(
            ticker=ticker,
            start=start.isoformat(),
            end=end.isoformat(),
        )
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": "asc",
            "limit": MAX_RESULTS_PER_PAGE,
        }

        page = 0
        while url:
            page += 1
            logger.debug("Fetching page {} for {} ({} → {})", page, ticker, start, end)
            resp = self._get_with_retry(url, params=params if page == 1 else None)
            data = resp.json()

            results = data.get("results", [])
            all_results.extend(results)

            # Polygon returns "next_url" if there are more pages
            url = data.get("next_url")
            if url:
                time.sleep(REQUEST_DELAY_S)

        logger.info(
            "Downloaded {} bars for {} ({} → {})",
            len(all_results),
            ticker,
            start,
            end,
        )
        return all_results

    def fetch_aggs_chunked(
        self,
        ticker: str,
        start: date,
        end: date,
        chunk_days: int = 30,
        adjusted: bool = True,
    ) -> List[dict]:
        """
        Break a large date range into *chunk_days*-sized windows and fetch
        each one sequentially. This avoids hitting Polygon timeouts on
        multi-year queries.
        """
        all_results: List[dict] = []
        current = start
        total_days = (end - start).days
        pbar = tqdm(total=total_days, desc=f"Downloading {ticker}", unit="day")

        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            bars = self.fetch_aggs(ticker, current, chunk_end, adjusted=adjusted)
            all_results.extend(bars)
            advance = (chunk_end - current).days + 1
            pbar.update(advance)
            current = chunk_end + timedelta(days=1)

        pbar.close()
        return all_results

    # ── Private Helpers ──────────────────────────────────────────────────
    def _get_with_retry(
        self,
        url: str,
        params: Optional[dict] = None,
        max_retries: int = 3,
        backoff: float = 2.0,
    ) -> requests.Response:
        """GET with exponential back-off on transient errors."""
        for attempt in range(1, max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as exc:
                if attempt == max_retries:
                    logger.error("Request failed after {} retries: {}", max_retries, exc)
                    raise
                wait = backoff ** attempt
                logger.warning(
                    "Attempt {}/{} failed ({}). Retrying in {:.1f}s …",
                    attempt,
                    max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
        # unreachable, but keeps mypy happy
        raise RuntimeError("Unreachable")


class PublicMarketDataClient:
    """Thin wrapper around Public historical bars, normalized for Chronos."""

    def __init__(
        self,
        *,
        access_token: Optional[str] = None,
        secret_token: Optional[str] = None,
        base_url: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
        period: Optional[str] = None,
        aggregation: Optional[str] = None,
        instrument_type: Optional[str] = None,
    ) -> None:
        self.access_token = access_token or settings.public_access_token
        self.secret_token = secret_token or settings.public_secret_token
        self.base_url = (base_url or settings.public_api_base_url).rstrip("/")
        self.auth_endpoint = auth_endpoint or settings.public_auth_endpoint
        self.period = (period or settings.public_bars_period).upper()
        self.aggregation = (aggregation or settings.public_bars_aggregation).upper()
        self.instrument_type = (instrument_type or settings.public_instrument_type).upper()
        self._session = requests.Session()

    def fetch_aggs(
        self,
        ticker: str,
        start: date,
        end: date,
        adjusted: bool = True,
    ) -> List[dict]:
        """
        Download Public historical bars and return Polygon-style aggregate dicts.

        Public's bars endpoint is period-based rather than arbitrary date-range
        based, so Chronos requests a configured period and filters locally.
        """
        del adjusted  # Public's endpoint does not expose an adjusted toggle.
        payload = self.fetch_bars_payload(ticker)
        bars = [
            bar
            for bar in self._normalize_public_bars(ticker, payload)
            if start <= _bar_date(bar) <= end
        ]
        bars.sort(key=lambda bar: bar["t"])
        logger.info(
            "Downloaded {} Public bars for {} ({} → {})",
            len(bars),
            ticker,
            start,
            end,
        )
        return bars

    def fetch_bars_payload(self, ticker: str) -> dict:
        """Return one raw Public bars payload without persisting credentials."""

        url = self.base_url + PUBLIC_BARS_ENDPOINT.format(
            type=self.instrument_type,
            symbol=ticker.upper(),
            period=self.period,
            aggregation=self.aggregation,
        )
        logger.debug(
            "Fetching Public bars for {} period={} aggregation={}",
            ticker,
            self.period,
            self.aggregation,
        )
        resp = self._get_with_retry(url, headers=self._headers())
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("Public bars response must be a JSON object.")
        return payload

    def fetch_instruments(self, instrument_type: str = "EQUITY") -> dict:
        """Return the current Public instrument catalogue for one type."""

        url = self.base_url + PUBLIC_INSTRUMENTS_ENDPOINT
        resp = self._get_with_retry(
            url,
            headers=self._headers(),
            params={"typeFilter": instrument_type.upper()},
        )
        payload = resp.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("instruments"), list):
            raise ValueError("Public instruments response has an invalid shape.")
        return payload

    def fetch_aggs_chunked(
        self,
        ticker: str,
        start: date,
        end: date,
        chunk_days: int = 30,
        adjusted: bool = True,
    ) -> List[dict]:
        """Compatibility shim; Public fetches by period and filters locally."""
        del chunk_days
        return self.fetch_aggs(ticker, start, end, adjusted=adjusted)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _get_access_token(self) -> str:
        if self.access_token:
            return self.access_token
        if not self.secret_token:
            raise ValueError(
                "PUBLIC_SECRET_TOKEN or PUBLIC_ACCESS_TOKEN is not set. "
                "Provide Public API credentials in .env or pass them explicitly."
            )
        resp = self._post_with_retry(
            self.auth_endpoint,
            json={"secret": self.secret_token, "validityInMinutes": 60},
        )
        data = resp.json()
        token = data.get("accessToken")
        if not token:
            raise ValueError("Public auth response did not include accessToken")
        self.access_token = str(token)
        return self.access_token

    @staticmethod
    def _normalize_public_bars(ticker: str, payload: dict) -> List[dict]:
        bars: List[dict] = []
        for session_name in PUBLIC_SESSIONS:
            session_payload = payload.get(session_name) or {}
            for raw in session_payload.get("bars") or []:
                normalized = _public_bar_to_polygon_shape(raw)
                if normalized is not None:
                    normalized["ticker"] = ticker.upper()
                    normalized["session"] = session_name
                    bars.append(normalized)
        return bars

    def _get_with_retry(
        self,
        url: str,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
        max_retries: int = 3,
        backoff: float = 2.0,
    ) -> requests.Response:
        for attempt in range(1, max_retries + 1):
            try:
                resp = self._session.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as exc:
                if attempt == max_retries:
                    logger.error("Public request failed after {} retries: {}", max_retries, exc)
                    raise
                wait = backoff ** attempt
                logger.warning(
                    "Public attempt {}/{} failed ({}). Retrying in {:.1f}s",
                    attempt,
                    max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
        raise RuntimeError("Unreachable")

    def _post_with_retry(
        self,
        url: str,
        json: Optional[dict] = None,
        max_retries: int = 3,
        backoff: float = 2.0,
    ) -> requests.Response:
        for attempt in range(1, max_retries + 1):
            try:
                resp = self._session.post(url, json=json, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as exc:
                if attempt == max_retries:
                    logger.error("Public auth failed after {} retries: {}", max_retries, exc)
                    raise
                wait = backoff ** attempt
                logger.warning(
                    "Public auth attempt {}/{} failed ({}). Retrying in {:.1f}s",
                    attempt,
                    max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
        raise RuntimeError("Unreachable")


def build_market_data_client(provider: Optional[str] = None) -> ChronosMarketDataClient:
    """Build the configured Chronos market-data client."""
    selected = (provider or settings.market_data_provider or "public").strip().lower()
    if selected == "public":
        return PublicMarketDataClient()
    if selected == "polygon":
        return PolygonClient(api_key=settings.polygon_api_key)
    raise ValueError(f"Unsupported market data provider: {provider}")


def _public_bar_to_polygon_shape(raw: dict) -> dict | None:
    try:
        timestamp_ms = _public_timestamp_ms(raw["timestamp"])
        open_ = float(raw["open"])
        high = float(raw["high"])
        low = float(raw["low"])
        close = float(raw["close"])
        volume = float(raw.get("volume") or 0)
    except (KeyError, TypeError, ValueError):
        return None
    return {
        "t": timestamp_ms,
        "o": open_,
        "h": high,
        "l": low,
        "c": close,
        "v": volume,
    }


def _public_timestamp_ms(value: str) -> int:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def _bar_date(bar: dict) -> date:
    return datetime.fromtimestamp(bar["t"] / 1000, tz=UTC).date()
