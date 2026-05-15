from __future__ import annotations

from datetime import date

import pytest

from src.chronos.client import PublicMarketDataClient, build_market_data_client


class StubResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class StubSession:
    def __init__(self, *, bars_payload: dict, auth_payload: dict | None = None) -> None:
        self.bars_payload = bars_payload
        self.auth_payload = auth_payload or {"accessToken": "minted-token"}
        self.get_calls: list[dict] = []
        self.post_calls: list[dict] = []

    def get(self, url: str, *, headers: dict | None = None, timeout: int = 30) -> StubResponse:
        self.get_calls.append({"url": url, "headers": headers, "timeout": timeout})
        return StubResponse(self.bars_payload)

    def post(self, url: str, *, json: dict | None = None, timeout: int = 30) -> StubResponse:
        self.post_calls.append({"url": url, "json": json, "timeout": timeout})
        return StubResponse(self.auth_payload)


def test_public_client_normalizes_and_filters_bars() -> None:
    payload = {
        "preMarket": {
            "bars": [
                {
                    "timestamp": "2026-05-10T13:29:00Z",
                    "open": "100.0",
                    "high": "101.0",
                    "low": "99.5",
                    "close": "100.5",
                    "volume": 50,
                }
            ]
        },
        "regularMarket": {
            "bars": [
                {
                    "timestamp": "2026-05-11T13:30:00Z",
                    "open": "101.0",
                    "high": "102.0",
                    "low": "100.5",
                    "close": "101.5",
                    "volume": 123,
                }
            ]
        },
        "afterMarket": {"bars": []},
    }
    session = StubSession(bars_payload=payload)
    client = PublicMarketDataClient(
        access_token="token",
        base_url="https://example.test",
        period="FIVE_YEARS",
        aggregation="ONE_MINUTE",
    )
    client._session = session

    bars = client.fetch_aggs("qqq", date(2026, 5, 11), date(2026, 5, 11))

    assert len(bars) == 1
    assert bars[0]["o"] == 101.0
    assert bars[0]["h"] == 102.0
    assert bars[0]["l"] == 100.5
    assert bars[0]["c"] == 101.5
    assert bars[0]["v"] == 123.0
    assert bars[0]["ticker"] == "QQQ"
    assert bars[0]["session"] == "regularMarket"
    assert session.get_calls[0]["url"] == (
        "https://example.test/userapigateway/historicdata/EQUITY/QQQ/FIVE_YEARS/ONE_MINUTE"
    )
    assert session.get_calls[0]["headers"]["Authorization"] == "Bearer token"


def test_public_client_mints_access_token_from_secret() -> None:
    session = StubSession(bars_payload={"regularMarket": {"bars": []}})
    client = PublicMarketDataClient(
        secret_token="secret-token",
        base_url="https://example.test",
        auth_endpoint="https://auth.example.test/access-tokens",
    )
    client._session = session

    assert client.fetch_aggs("SPY", date(2026, 5, 11), date(2026, 5, 11)) == []

    assert session.post_calls == [
        {
            "url": "https://auth.example.test/access-tokens",
            "json": {"secret": "secret-token", "validityInMinutes": 60},
            "timeout": 30,
        }
    ]
    assert session.get_calls[0]["headers"]["Authorization"] == "Bearer minted-token"


def test_build_market_data_client_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unsupported market data provider"):
        build_market_data_client("not-real")
