"""Normalize broker history exports into research-ready trade rows."""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")
CENTRAL = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

PUBLIC_OCC_RE = re.compile(r"^(?P<underlying>[A-Z]+)(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{8})$")
SCHWAB_OPTION_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)\s+(?P<expiry>\d{2}/\d{2}/\d{4})\s+(?P<strike>\d+(?:\.\d+)?)\s+(?P<right>[CP])$"
)


@dataclass(slots=True)
class NormalizedFill:
    source: str
    source_file: str
    account_alias: str
    broker: str
    timestamp_utc: str
    timestamp_et: str
    trade_date: str
    time_et: str
    has_intraday_time: bool
    action: str
    side: str
    position_effect: str
    symbol: str
    description: str
    security_type: str
    underlying: str
    expiration: str
    option_right: str
    strike: str
    dte: int | None
    quantity: float
    signed_quantity: float
    price: float | None
    fees: float
    net_amount: float | None
    trade_value: float | None
    raw_id: str


@dataclass(slots=True)
class RoundTrip:
    account_alias: str
    source: str
    symbol: str
    underlying: str
    expiration: str
    option_right: str
    strike: str
    opened_at_et: str
    closed_at_et: str
    open_date: str
    close_date: str
    has_intraday_time: bool
    entry_time_et: str
    holding_minutes: float | None
    dte_at_entry: int | None
    opening_side: str
    quantity: float
    entry_price: float | None
    exit_price: float | None
    entry_notional: float
    exit_notional: float
    fees: float
    pnl: float
    return_on_entry_cost: float | None
    opening_fill_id: str
    closing_fill_id: str


def load_all_fills(raw_dir: Path) -> list[NormalizedFill]:
    fills: list[NormalizedFill] = []
    fills.extend(load_public_exports(raw_dir))
    fills.extend(load_thinkorswim_exports(raw_dir))
    fills.extend(load_schwab_exports(raw_dir))
    return sorted(fills, key=_fill_sort_key)


def load_public_exports(raw_dir: Path) -> list[NormalizedFill]:
    fills: list[NormalizedFill] = []
    for path in sorted(raw_dir.glob("public_*/*.csv")):
        source = path.parent.name
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("type") != "TRADE":
                    continue
                fill = _public_row_to_fill(row, path, source)
                if fill:
                    fills.append(fill)
    return fills


def load_schwab_exports(raw_dir: Path) -> list[NormalizedFill]:
    fills: list[NormalizedFill] = []
    for path in sorted(raw_dir.glob("schwab_*_transactions_*.csv")):
        source = path.stem
        with path.open(newline="") as handle:
            for idx, row in enumerate(csv.DictReader(handle)):
                fill = _schwab_row_to_fill(row, path, source, idx)
                if fill:
                    fills.append(fill)
    return fills


def load_thinkorswim_exports(raw_dir: Path) -> list[NormalizedFill]:
    fills: list[NormalizedFill] = []
    for path in sorted(raw_dir.glob("*AccountStatement*.csv")):
        source = path.stem
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.reader(handle))
        if not rows:
            continue
        account_label = rows[0][0] if rows[0] else source
        for idx, row in enumerate(_thinkorswim_trade_history_rows(rows)):
            fill = _thinkorswim_row_to_fill(row, path, source, account_label, idx)
            if fill:
                fills.append(fill)
    return fills


def build_round_trips(fills: list[NormalizedFill]) -> list[RoundTrip]:
    lots: dict[tuple[str, str], list[dict[str, object]]] = {}
    trips: list[RoundTrip] = []

    for fill in fills:
        if fill.security_type != "OPTION" or fill.signed_quantity == 0 or fill.net_amount is None:
            continue
        key = (fill.account_alias, fill.symbol)
        open_lots = lots.setdefault(key, [])
        remaining_qty = fill.signed_quantity

        while remaining_qty and open_lots and _opposite_sign(float(open_lots[0]["remaining_qty"]), remaining_qty):
            lot = open_lots[0]
            lot_remaining = float(lot["remaining_qty"])
            close_abs = min(abs(remaining_qty), abs(lot_remaining))
            close_signed = close_abs if remaining_qty > 0 else -close_abs
            open_signed = close_abs if lot_remaining > 0 else -close_abs

            trips.append(_make_round_trip(lot, fill, open_signed, close_signed))

            lot["remaining_qty"] = lot_remaining - open_signed
            remaining_qty -= close_signed
            if abs(float(lot["remaining_qty"])) < 1e-9:
                open_lots.pop(0)

        if abs(remaining_qty) > 1e-9:
            open_lots.append({
                "fill": fill,
                "remaining_qty": remaining_qty,
                "original_qty": fill.signed_quantity,
            })

    return trips


def fills_to_dicts(fills: list[NormalizedFill]) -> list[dict[str, object]]:
    return [_dataclass_to_dict(fill) for fill in fills]


def round_trips_to_dicts(round_trips: list[RoundTrip]) -> list[dict[str, object]]:
    return [_dataclass_to_dict(trip) for trip in round_trips]


def write_dict_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _public_row_to_fill(row: dict[str, str], path: Path, source: str) -> NormalizedFill | None:
    symbol = row.get("symbol") or ""
    quantity = _to_float(row.get("quantity"))
    net_amount = _to_float_or_none(row.get("netAmount"))
    if not symbol or quantity == 0 or net_amount is None:
        return None

    timestamp_utc, timestamp_et, trade_date, time_et = _parse_public_timestamp(row["timestamp"])
    underlying, expiration, right, strike = _parse_option_symbol(symbol)
    trade_dt = date.fromisoformat(trade_date)
    dte = (date.fromisoformat(expiration) - trade_dt).days if expiration else None
    side = (row.get("side") or "").upper()
    price = _price_from_description(row.get("description", "")) or _unit_price(net_amount, quantity)

    return NormalizedFill(
        source=source,
        source_file=str(path),
        account_alias=_account_alias("public", row.get("accountNumber") or row.get("accountId") or source),
        broker="public",
        timestamp_utc=timestamp_utc,
        timestamp_et=timestamp_et,
        trade_date=trade_date,
        time_et=time_et,
        has_intraday_time=True,
        action=row.get("subType") or row.get("type") or "",
        side=side,
        position_effect="UNKNOWN",
        symbol=symbol,
        description=row.get("description") or "",
        security_type=row.get("securityType") or "",
        underlying=underlying,
        expiration=expiration,
        option_right=right,
        strike=strike,
        dte=dte,
        quantity=abs(quantity),
        signed_quantity=quantity,
        price=price,
        fees=_to_float(row.get("fees")),
        net_amount=net_amount,
        trade_value=_to_float_or_none(row.get("principalAmount")),
        raw_id=row.get("id") or "",
    )


def _schwab_row_to_fill(row: dict[str, str], path: Path, source: str, idx: int) -> NormalizedFill | None:
    action = row.get("Action") or ""
    symbol = row.get("Symbol") or ""
    quantity = _to_float(row.get("Quantity"))
    if not action or not symbol or quantity == 0:
        return None

    position_effect = _schwab_position_effect(action)
    signed_quantity = quantity
    side = ""
    if action.startswith("Buy"):
        side = "BUY"
        signed_quantity = abs(quantity)
    elif action.startswith("Sell"):
        side = "SELL"
        signed_quantity = -abs(quantity)
    elif action == "Expired":
        side = "EXPIRE"
    else:
        return None

    trade_date = _parse_schwab_date(row.get("Date") or "")
    if not trade_date:
        return None
    timestamp_et = datetime.combine(trade_date, time(0, 0), tzinfo=NY)
    underlying, expiration, right, strike = _parse_option_symbol(symbol)
    dte = (date.fromisoformat(expiration) - trade_date).days if expiration else None

    return NormalizedFill(
        source=source,
        source_file=str(path),
        account_alias=_account_alias("schwab", source),
        broker="schwab",
        timestamp_utc=timestamp_et.astimezone(UTC).isoformat(),
        timestamp_et=timestamp_et.isoformat(),
        trade_date=trade_date.isoformat(),
        time_et="",
        has_intraday_time=False,
        action=action,
        side=side,
        position_effect=position_effect,
        symbol=symbol,
        description=row.get("Description") or "",
        security_type="OPTION" if right else "",
        underlying=underlying,
        expiration=expiration,
        option_right=right,
        strike=strike,
        dte=dte,
        quantity=abs(quantity),
        signed_quantity=signed_quantity,
        price=_to_float_or_none(row.get("Price")),
        fees=_to_float(row.get("Fees & Comm")),
        net_amount=_to_float_or_none(row.get("Amount")),
        trade_value=None,
        raw_id=f"{path.name}:{idx}",
    )


def _thinkorswim_trade_history_rows(rows: list[list[str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    in_trade_history = False
    header_seen = False
    current_timestamp = ""
    current_spread = ""
    current_order_type = ""

    for row in rows:
        padded = row + [""] * max(0, 13 - len(row))
        first_cell = padded[0].strip()
        if len(row) == 1 and first_cell == "Account Trade History":
            in_trade_history = True
            continue
        if not in_trade_history:
            continue
        if len(row) == 1 and first_cell and first_cell != "Account Trade History":
            break
        if padded[1].strip() == "Exec Time":
            header_seen = True
            continue
        if not header_seen:
            continue

        exec_time = padded[1].strip()
        if exec_time:
            current_timestamp = exec_time
            current_spread = padded[2].strip()
            current_order_type = padded[12].strip()

        side = padded[3].strip()
        symbol = padded[6].strip()
        if not current_timestamp or not side or not symbol:
            continue

        output.append(
            {
                "exec_time": current_timestamp,
                "spread": padded[2].strip() or current_spread,
                "side": side,
                "quantity": padded[4].strip(),
                "position_effect": padded[5].strip(),
                "underlying": symbol,
                "expiration": padded[7].strip(),
                "strike": padded[8].strip(),
                "option_type": padded[9].strip(),
                "price": padded[10].strip(),
                "net_price": padded[11].strip(),
                "order_type": padded[12].strip() or current_order_type,
            }
        )
    return output


def _thinkorswim_row_to_fill(
    row: dict[str, str],
    path: Path,
    source: str,
    account_label: str,
    idx: int,
) -> NormalizedFill | None:
    side = row["side"].upper()
    if side not in {"BUY", "SELL"}:
        return None

    timestamp_utc, timestamp_et, trade_date, time_et = _parse_thinkorswim_timestamp(row["exec_time"])
    underlying = row["underlying"].upper()
    expiration_date = _parse_thinkorswim_expiration(row["expiration"])
    right = _thinkorswim_option_right(row["option_type"])
    strike_decimal = _to_decimal_or_none(row["strike"])
    quantity = abs(_to_float(row["quantity"]))
    price = _to_float_or_none(row["price"])
    if not underlying or not expiration_date or not right or strike_decimal is None or quantity == 0 or price is None:
        return None

    signed_quantity = quantity if side == "BUY" else -quantity
    net_amount = -signed_quantity * price * 100
    trade_dt = date.fromisoformat(trade_date)
    symbol = _occ_option_symbol(underlying, expiration_date, right, strike_decimal)
    strike = _decimal_str(strike_decimal)

    return NormalizedFill(
        source=source,
        source_file=str(path),
        account_alias=_account_alias("tos", account_label),
        broker="thinkorswim",
        timestamp_utc=timestamp_utc,
        timestamp_et=timestamp_et,
        trade_date=trade_date,
        time_et=time_et,
        has_intraday_time=True,
        action=row["spread"] or "SINGLE",
        side=side,
        position_effect=row["position_effect"].replace("TO ", ""),
        symbol=symbol,
        description=(
            f"{row['spread'] or 'SINGLE'} {side} {row['quantity']} {underlying} "
            f"{row['expiration']} {row['strike']} {row['option_type']} @{row['price']}"
        ),
        security_type="OPTION",
        underlying=underlying,
        expiration=expiration_date.isoformat(),
        option_right=right,
        strike=strike,
        dte=(expiration_date - trade_dt).days,
        quantity=quantity,
        signed_quantity=signed_quantity,
        price=price,
        fees=0.0,
        net_amount=net_amount,
        trade_value=net_amount,
        raw_id=f"{path.name}:trade_history:{idx}",
    )


def _make_round_trip(lot: dict[str, object], close_fill: NormalizedFill, open_qty: float, close_qty: float) -> RoundTrip:
    open_fill = lot["fill"]
    assert isinstance(open_fill, NormalizedFill)
    original_qty = abs(float(lot["original_qty"]))
    matched_qty = abs(open_qty)
    open_ratio = matched_qty / original_qty if original_qty else 0.0
    close_ratio = matched_qty / abs(close_fill.signed_quantity) if close_fill.signed_quantity else 0.0

    open_cash = (open_fill.net_amount or 0.0) * open_ratio
    close_cash = (close_fill.net_amount or 0.0) * close_ratio
    pnl = open_cash + close_cash
    entry_cost = abs(open_cash) if open_cash < 0 else None
    opened_at = _dt_from_iso(open_fill.timestamp_et)
    closed_at = _dt_from_iso(close_fill.timestamp_et)
    holding_minutes = (
        (closed_at - opened_at).total_seconds() / 60.0
        if open_fill.has_intraday_time and close_fill.has_intraday_time
        else None
    )

    return RoundTrip(
        account_alias=open_fill.account_alias,
        source=open_fill.source,
        symbol=open_fill.symbol,
        underlying=open_fill.underlying,
        expiration=open_fill.expiration,
        option_right=open_fill.option_right,
        strike=open_fill.strike,
        opened_at_et=open_fill.timestamp_et,
        closed_at_et=close_fill.timestamp_et,
        open_date=open_fill.trade_date,
        close_date=close_fill.trade_date,
        has_intraday_time=open_fill.has_intraday_time and close_fill.has_intraday_time,
        entry_time_et=open_fill.time_et,
        holding_minutes=holding_minutes,
        dte_at_entry=open_fill.dte,
        opening_side="LONG" if open_qty > 0 else "SHORT",
        quantity=matched_qty,
        entry_price=open_fill.price,
        exit_price=close_fill.price,
        entry_notional=open_cash,
        exit_notional=close_cash,
        fees=(open_fill.fees * open_ratio) + (close_fill.fees * close_ratio),
        pnl=pnl,
        return_on_entry_cost=(pnl / entry_cost) if entry_cost else None,
        opening_fill_id=open_fill.raw_id,
        closing_fill_id=close_fill.raw_id,
    )


def _parse_public_timestamp(value: str) -> tuple[str, str, str, str]:
    dt_utc = datetime.fromisoformat(value.replace("Z", "+00:00"))
    dt_et = dt_utc.astimezone(NY)
    return (
        dt_utc.isoformat(),
        dt_et.isoformat(),
        dt_et.date().isoformat(),
        dt_et.strftime("%H:%M:%S"),
    )


def _parse_schwab_date(value: str) -> date | None:
    raw = value.split(" as of ", 1)[0].strip()
    try:
        return datetime.strptime(raw, "%m/%d/%Y").date()
    except ValueError:
        return None


def _parse_thinkorswim_timestamp(value: str) -> tuple[str, str, str, str]:
    dt_central = datetime.strptime(value, "%m/%d/%y %H:%M:%S").replace(tzinfo=CENTRAL)
    dt_et = dt_central.astimezone(NY)
    return (
        dt_central.astimezone(UTC).isoformat(),
        dt_et.isoformat(),
        dt_et.date().isoformat(),
        dt_et.strftime("%H:%M:%S"),
    )


def _parse_thinkorswim_expiration(value: str) -> date:
    return datetime.strptime(value.strip(), "%d %b %y").date()


def _parse_option_symbol(symbol: str) -> tuple[str, str, str, str]:
    public_match = PUBLIC_OCC_RE.match(symbol)
    if public_match:
        expiry = datetime.strptime(public_match.group("expiry"), "%y%m%d").date().isoformat()
        strike = Decimal(public_match.group("strike")) / Decimal("1000")
        return (
            public_match.group("underlying"),
            expiry,
            public_match.group("right"),
            _decimal_str(strike),
        )

    schwab_match = SCHWAB_OPTION_RE.match(symbol)
    if schwab_match:
        expiry = datetime.strptime(schwab_match.group("expiry"), "%m/%d/%Y").date().isoformat()
        return (
            schwab_match.group("underlying"),
            expiry,
            schwab_match.group("right"),
            _decimal_str(Decimal(schwab_match.group("strike"))),
        )

    return "", "", "", ""


def _thinkorswim_option_right(value: str) -> str:
    normalized = value.strip().upper()
    if normalized == "CALL":
        return "C"
    if normalized == "PUT":
        return "P"
    return ""


def _schwab_position_effect(action: str) -> str:
    if "Open" in action:
        return "OPEN"
    if "Close" in action:
        return "CLOSE"
    if action == "Expired":
        return "CLOSE"
    return "UNKNOWN"


def _to_float(value: str | None) -> float:
    parsed = _to_float_or_none(value)
    return parsed if parsed is not None else 0.0


def _to_float_or_none(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().replace("$", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_decimal_or_none(value: str | None) -> Decimal | None:
    if value is None:
        return None
    cleaned = value.strip().replace("$", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _price_from_description(description: str) -> float | None:
    match = re.search(r"\bat\s+(-?\d+(?:\.\d+)?)\b", description)
    return float(match.group(1)) if match else None


def _unit_price(net_amount: float, quantity: float) -> float | None:
    if quantity == 0:
        return None
    return round(abs(net_amount) / (abs(quantity) * 100), 4)


def _decimal_str(value: Decimal) -> str:
    try:
        normalized = value.normalize()
    except InvalidOperation:
        return str(value)
    return format(normalized, "f")


def _occ_option_symbol(underlying: str, expiration: date, right: str, strike: Decimal) -> str:
    strike_int = int((strike * Decimal("1000")).to_integral_value())
    return f"{underlying}{expiration:%y%m%d}{right}{strike_int:08d}"


def _account_alias(prefix: str, raw: str) -> str:
    digest = hashlib.sha256(raw.encode()).hexdigest()[:8]
    suffix = raw[-4:] if len(raw) >= 4 else digest[:4]
    return f"{prefix}_{suffix}_{digest}"


def _fill_sort_key(fill: NormalizedFill) -> tuple[str, str, str]:
    return (fill.timestamp_utc, fill.account_alias, fill.raw_id)


def _opposite_sign(left: float, right: float) -> bool:
    return (left > 0 and right < 0) or (left < 0 and right > 0)


def _dt_from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _dataclass_to_dict(item: object) -> dict[str, object]:
    return {field: getattr(item, field) for field in item.__dataclass_fields__}  # type: ignore[attr-defined]
