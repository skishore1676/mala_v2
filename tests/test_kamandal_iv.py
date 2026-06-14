"""Tests for the kamandal IV reader."""

import sqlite3
from datetime import date

from src.research.kamandal_iv import latest_atm_iv


def _make_db(tmp_path, rows):
    db = tmp_path / "k.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE iv_snapshots (symbol TEXT, snapshot_date TEXT, metric TEXT, "
        "iv REAL, source TEXT, quote_count INT, raw_json TEXT)"
    )
    con.executemany(
        "INSERT INTO iv_snapshots(symbol,snapshot_date,metric,iv) VALUES (?,?,?,?)", rows
    )
    con.commit()
    con.close()
    return db


def test_latest_atm_iv_prefers_short_dated(tmp_path):
    db = _make_db(tmp_path, [
        ("NVDA", "2026-04-26", "atm_30_45_mean_iv", 41.38),
        ("NVDA", "2026-04-26", "atm_2_10_mean_iv", 55.0),
        ("NVDA", "2026-04-20", "atm_2_10_mean_iv", 50.0),
    ])
    iv, d, metric = latest_atm_iv("nvda", db_path=db)
    assert metric == "atm_2_10_mean_iv"      # short-dated preferred
    assert abs(iv - 0.55) < 1e-9             # latest row, percent -> decimal
    assert d == date(2026, 4, 26)


def test_latest_atm_iv_falls_back_to_30_45(tmp_path):
    db = _make_db(tmp_path, [("IWM", "2026-04-26", "atm_30_45_mean_iv", 22.38)])
    iv, d, metric = latest_atm_iv("IWM", db_path=db)
    assert metric == "atm_30_45_mean_iv"
    assert abs(iv - 0.2238) < 1e-9


def test_latest_atm_iv_missing(tmp_path):
    db = _make_db(tmp_path, [("IWM", "2026-04-26", "atm_30_45_mean_iv", 22.0)])
    assert latest_atm_iv("ZZZZ", db_path=db) is None
    assert latest_atm_iv("IWM", db_path=tmp_path / "nope.db") is None
