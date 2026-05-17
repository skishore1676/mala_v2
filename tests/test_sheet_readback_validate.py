from __future__ import annotations

from src.research.sheet_readback_validate import _range_suffix_for_table


def test_readback_validator_derives_csv_sized_range() -> None:
    table = [
        [f"col_{index}" for index in range(38)],
        [str(index) for index in range(38)],
        [str(index * 2) for index in range(38)],
    ]

    assert _range_suffix_for_table(table) == "A1:AL3"
