from datetime import UTC, date, datetime

from src.chronos.storage import LocalStorage
from src.trading_calendar import is_trading_day, trading_dates, trading_days_ago, trading_window_start


def test_is_trading_day_rejects_nyse_holidays_and_special_closure() -> None:
    assert is_trading_day(date(2021, 6, 18)) is True  # Juneteenth was not yet a NYSE holiday.
    assert is_trading_day(date(2022, 6, 20)) is False
    assert is_trading_day(date(2025, 1, 9)) is False
    assert is_trading_day(date(2026, 4, 3)) is False
    assert is_trading_day(date(2026, 4, 6)) is True


def test_trading_dates_skips_weekends_and_holidays() -> None:
    assert trading_dates(date(2025, 1, 8), date(2025, 1, 13)) == [
        date(2025, 1, 8),
        date(2025, 1, 10),
        date(2025, 1, 13),
    ]


def test_trading_days_ago_and_window_start_use_holidays() -> None:
    assert trading_days_ago(date(2025, 1, 13), 2) == date(2025, 1, 10)
    assert trading_window_start(datetime(2025, 1, 13, 14, 0, tzinfo=UTC), 2) == datetime(
        2025, 1, 10, 0, 0, tzinfo=UTC
    )


def test_local_storage_missing_dates_uses_nyse_calendar(tmp_path) -> None:
    storage = LocalStorage(base_dir=tmp_path)
    assert storage.missing_dates("SPY", date(2025, 1, 8), date(2025, 1, 13)) == [
        date(2025, 1, 8),
        date(2025, 1, 10),
        date(2025, 1, 13),
    ]
