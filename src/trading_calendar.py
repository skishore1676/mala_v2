"""NYSE trading-calendar helpers for cache and replay lookbacks."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache


SPECIAL_FULL_DAY_CLOSURES: frozenset[date] = frozenset(
    {
        date(2025, 1, 9),  # National Day of Mourning for President Jimmy Carter.
    }
)

# Explicit NYSE 13:00 ET closes used by retained research data. Keep this
# allowlist auditable; an unknown short session must fail closed rather than be
# inferred from missing bars.
SPECIAL_EARLY_CLOSES: frozenset[date] = frozenset(
    {
        date(2021, 11, 26),
        date(2022, 11, 25),
        date(2023, 7, 3),
        date(2023, 11, 24),
        date(2024, 7, 3),
        date(2024, 11, 29),
        date(2024, 12, 24),
        date(2025, 7, 3),
        date(2025, 11, 28),
        date(2025, 12, 24),
        date(2026, 11, 27),
        date(2026, 12, 24),
        date(2027, 11, 26),
        date(2028, 7, 3),
        date(2028, 11, 24),
    }
)


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    return first_day + timedelta(days=offset, weeks=n - 1)


def _observe(value: date) -> date:
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _easter(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    first_next_month = date(year + int(month == 12), 1 if month == 12 else month + 1, 1)
    candidate = first_next_month - timedelta(days=1)
    while candidate.weekday() != weekday:
        candidate -= timedelta(days=1)
    return candidate


@lru_cache(maxsize=32)
def nyse_holidays(year: int) -> frozenset[date]:
    holidays = {
        _observe(date(year, 1, 1)),
        _nth_weekday_of_month(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday_of_month(year, 2, 0, 3),  # Washington's Birthday
        _easter(year) - timedelta(days=2),  # Good Friday
        _last_weekday_of_month(year, 5, 0),  # Memorial Day
        _observe(date(year, 7, 4)),
        _nth_weekday_of_month(year, 9, 0, 1),  # Labor Day
        _nth_weekday_of_month(year, 11, 3, 4),  # Thanksgiving Day
        _observe(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observe(date(year, 6, 19)))
    holidays.update(d for d in SPECIAL_FULL_DAY_CLOSURES if d.year == year)
    return frozenset(holidays)


def is_trading_day(value: date) -> bool:
    return value.weekday() < 5 and value not in nyse_holidays(value.year)


def nyse_market_close(value: date) -> time:
    """Return the declared regular-session close for a supported NYSE date."""

    return time(13, 0) if value in SPECIAL_EARLY_CLOSES else time(16, 0)


def expected_rth_minutes(value: date) -> int:
    return 210 if value in SPECIAL_EARLY_CLOSES else 390


def trading_dates(start: date, end: date) -> list[date]:
    if end < start:
        return []
    return [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if is_trading_day(start + timedelta(days=i))
    ]


def previous_trading_day(value: date) -> date:
    candidate = value - timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def trading_days_ago(anchor: date, trading_days: int) -> date:
    candidate = anchor
    if not is_trading_day(candidate):
        candidate = previous_trading_day(candidate)
    for _ in range(max(trading_days - 1, 0)):
        candidate = previous_trading_day(candidate)
    return candidate


def trading_window_start(now: datetime, trading_days: int) -> datetime:
    start_day = trading_days_ago(now.astimezone(UTC).date(), trading_days)
    return datetime.combine(start_day, datetime.min.time(), tzinfo=UTC)
