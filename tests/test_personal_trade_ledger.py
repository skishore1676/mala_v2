from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from src.personal.replay_evaluator import ReplayAnchor, evaluate_anchor
from src.personal.trade_ledger import build_round_trips, load_thinkorswim_exports
from src.strategy.base import BaseStrategy


def test_load_thinkorswim_trade_history_expands_timestamped_legs(tmp_path: Path) -> None:
    statement = tmp_path / "tos_AccountStatement.csv"
    statement.write_text(
        "\n".join(
            [
                "Account Statement for 1234SCHW since 5/1/25 through 5/2/25",
                "",
                "Account Trade History",
                ",Exec Time,Spread,Side,Qty,Pos Effect,Symbol,Exp,Strike,Type,Price,Net Price,Order Type",
                ",5/1/25 08:35:00,SINGLE,BUY,+2,TO OPEN,IWM,1 MAY 25,210,CALL,.50,.50,LMT",
                ",5/1/25 08:45:00,SINGLE,SELL,-2,TO CLOSE,IWM,1 MAY 25,210,CALL,.75,.75,LMT",
                ",5/1/25 09:00:00,VERTICAL,BUY,+1,TO OPEN,SPY,2 MAY 25,580,PUT,1.20,.40,LMT",
                ",,,SELL,-1,TO OPEN,SPY,2 MAY 25,575,PUT,.80,CREDIT,",
                "",
                "Forex Account Summary",
            ]
        ),
        encoding="utf-8",
    )

    fills = load_thinkorswim_exports(tmp_path)

    assert len(fills) == 4
    assert fills[0].time_et == "09:35:00"
    assert fills[0].symbol == "IWM250501C00210000"
    assert fills[0].position_effect == "OPEN"
    assert fills[0].net_amount == -100.0
    assert fills[1].time_et == "09:45:00"
    assert fills[1].net_amount == 150.0
    assert fills[3].symbol == "SPY250502P00575000"
    assert fills[3].signed_quantity == -1

    trips = build_round_trips(fills)

    assert len(trips) == 1
    assert trips[0].underlying == "IWM"
    assert trips[0].pnl == 50.0
    assert trips[0].holding_minutes == 10.0


class _SignalAtThirdBarStrategy(BaseStrategy):
    @property
    def name(self) -> str:
        return "Toy Strategy"

    def generate_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.Series("signal", [False, False, True, False, False]),
            pl.Series("signal_direction", [None, None, "long", None, None]),
        )


def test_evaluate_anchor_marks_near_strategy_and_oracle_direction() -> None:
    anchor = ReplayAnchor(
        entry_timestamp_et=datetime(2025, 5, 1, 9, 32, tzinfo=ZoneInfo("America/New_York")),
        exit_timestamp_et=None,
        source="test",
        account_alias="acct",
        underlying="IWM",
        symbol="IWM250501C00210000",
        option_right="C",
        strike="210",
        expiration="2025-05-01",
        dte_at_entry=0,
        opening_side="LONG",
        quantity=1,
        entry_price=0.5,
        exit_price=0.75,
        holding_minutes=10,
        pnl=25,
        return_on_entry_cost=0.5,
        label="TRADED",
    )
    frame = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                pl.datetime(2025, 5, 1, 9, 30, time_zone="America/New_York"),
                pl.datetime(2025, 5, 1, 9, 34, time_zone="America/New_York"),
                interval="1m",
                eager=True,
            ),
            "ticker": ["IWM"] * 5,
            "open": [100, 101, 102, 103, 104],
            "high": [101, 102, 104, 106, 105],
            "low": [99, 100, 101, 102, 103],
            "close": [100, 101, 102, 103, 104],
            "volume": [1000] * 5,
        }
    )

    row = evaluate_anchor(
        anchor,
        frame,
        strategies=[_SignalAtThirdBarStrategy()],
        signal_tolerance_minutes=1,
        oracle_windows=(2,),
    )

    assert row["status"] == "evaluated"
    assert row["underlying_direction"] == "long"
    assert row["oracle_mfe_2m"] == 4
    assert row["toy_strategy_near_signal"] is True
    assert row["toy_strategy_direction_match"] is True
