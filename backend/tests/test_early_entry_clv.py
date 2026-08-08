"""Early-entry CLV recovery — join semantics and the arithmetic identity."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.backtest.early_entry_clv import (
    COMMENCE_TOL,
    build_index,
    describe,
    recover,
    report,
    series_for,
)

COMMENCE = datetime(2026, 7, 15, 23, 40, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _snapshot(dirpath, fetched: datetime, quotes: list[dict]) -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    name = fetched.strftime("%Y-%m-%d-%H%M") + ".json"
    (dirpath / name).write_text(json.dumps({
        "fetched_at": fetched.isoformat(),
        "quotes": quotes,
    }))


def _quote(batter: str, price: int, commence: datetime, book: str = "draftkings") -> dict:
    return {
        "batter_name": batter,
        "book": book,
        "bet_over_american": price,
        "main_over_american": None,
        "main_under_american": None,
        "commence_time": commence.isoformat(),
    }


def _archive(dirpath, date: str, *, batter="Aaron Judge", batter_id=592450,
             game_pk=900, commence=COMMENCE, entry_fair=0.20, dk_odds=400,
             ev_pct=30.0, outcome="L", ev_pct_p3=30.0) -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    pick = {
        "batter": batter, "batter_id": batter_id, "game_pk": game_pk,
        "game_datetime": commence.isoformat(),
        "market_prob_devig": entry_fair, "model_prob": 0.30,
        "dk_odds": dk_odds, "fd_odds": dk_odds, "best_book": "draftkings",
        "ev_pct": ev_pct, "tier": "primary", "top_3_features": [],
    }
    if ev_pct_p3 is not None:
        pick["ev_pct_p3"] = ev_pct_p3
    (dirpath / f"{date}.json").write_text(json.dumps({
        "date": date,
        "primary_picks": [pick],
        "settlement": {"primary_results": [
            {"batter_id": batter_id, "game_pk": game_pk,
             "outcome": outcome, "profit_units": -1.0, "actual_hr": 0},
        ]},
    }))


# ---------------------------------------------------------------------------
# Game matching — landmine 1
# ---------------------------------------------------------------------------

def test_series_for_tolerates_one_minute_api_skew(tmp_path):
    """MLB says 23:40, the Odds API says 23:41. An exact join drops the pick."""
    odds = tmp_path / "odds"
    quote_commence = COMMENCE + timedelta(minutes=1)
    _snapshot(odds, COMMENCE - timedelta(hours=10), [_quote("Aaron Judge", 400, quote_commence)])

    idx = build_index(odds)
    assert series_for(idx, "Aaron Judge", COMMENCE) is not None


def test_series_for_rejects_a_different_game(tmp_path):
    """Back-to-back days must not share a price series."""
    odds = tmp_path / "odds"
    yesterday = COMMENCE - timedelta(days=1)
    _snapshot(odds, yesterday - timedelta(hours=5), [_quote("Aaron Judge", 400, yesterday)])

    idx = build_index(odds)
    assert series_for(idx, "Aaron Judge", COMMENCE) is None
    assert series_for(idx, "Aaron Judge", yesterday) is not None


def test_series_for_picks_the_nearest_game(tmp_path):
    odds = tmp_path / "odds"
    other = COMMENCE + timedelta(days=1)
    _snapshot(odds, COMMENCE - timedelta(hours=8), [
        _quote("Aaron Judge", 400, COMMENCE + timedelta(minutes=1)),
        _quote("Aaron Judge", 500, other),
    ])
    idx = build_index(odds)
    series = series_for(idx, "Aaron Judge", COMMENCE)
    assert series[0][1][0]["bet_over_american"] == 400


def test_unknown_batter_returns_none(tmp_path):
    idx = build_index(tmp_path / "odds")
    assert series_for(idx, "Nobody", COMMENCE) is None


# ---------------------------------------------------------------------------
# Filter recomputation — landmine 2
# ---------------------------------------------------------------------------

def test_pre_v2_picks_are_evaluated_not_discarded(tmp_path):
    """Picks predating the passes_triple_v2 field must still be judged.

    Reading the stored filter_status treats all 927 pre-7/01 picks as rejected,
    which is what produced a 72-pick sample instead of 226.
    """
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=12), [_quote("Aaron Judge", 450, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=40), [_quote("Aaron Judge", 400, COMMENCE)])
    # No ev_pct_p3 and no filter_status at all — a pre-rebuild pick.
    _archive(arch, "2026-06-15", ev_pct_p3=None)

    rows = recover(archive_dir=arch, odds_dir=odds)
    assert len(rows) == 1
    # EV 30 clears the tier floor, no stacking, no pitcher-factor entry ->
    # passes_triple, and v2 degrades to triple when ev_pct_p3 is absent.
    assert rows[0]["passes_triple_v2"] is True


# ---------------------------------------------------------------------------
# Arithmetic
# ---------------------------------------------------------------------------

def test_clv_decomposition_identity(tmp_path):
    """clv_early must equal clv_actual + drift, exactly."""
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=14), [_quote("Aaron Judge", 600, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=30), [_quote("Aaron Judge", 350, COMMENCE)])
    _archive(arch, "2026-07-15")

    rows = recover(archive_dir=arch, odds_dir=odds)
    assert len(rows) == 1
    r = rows[0]
    assert r["clv_early_pp"] == pytest.approx(r["clv_actual_pp"] + r["drift_pp"], abs=1e-3)


def test_shortening_line_gives_positive_early_clv(tmp_path):
    """Price drops 600 -> 350: the market moved toward the Over, so early wins."""
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=14), [_quote("Aaron Judge", 600, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=30), [_quote("Aaron Judge", 350, COMMENCE)])
    _archive(arch, "2026-07-15")

    r = recover(archive_dir=arch, odds_dir=odds)[0]
    assert r["clv_early_pp"] > 0
    assert r["early_price"] == 600


# ---------------------------------------------------------------------------
# Eligibility rules
# ---------------------------------------------------------------------------

def test_requires_a_real_head_start(tmp_path):
    """Two snapshots an hour apart are not an 'early' price."""
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(minutes=90), [_quote("Aaron Judge", 420, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=20), [_quote("Aaron Judge", 400, COMMENCE)])
    _archive(arch, "2026-07-15")

    assert recover(archive_dir=arch, odds_dir=odds) == []


def test_single_snapshot_is_skipped(tmp_path):
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=10), [_quote("Aaron Judge", 400, COMMENCE)])
    _archive(arch, "2026-07-15")
    assert recover(archive_dir=arch, odds_dir=odds) == []


def test_post_first_pitch_snapshots_are_ignored(tmp_path):
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=10), [_quote("Aaron Judge", 600, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=30), [_quote("Aaron Judge", 400, COMMENCE)])
    _snapshot(odds, COMMENCE + timedelta(hours=1), [_quote("Aaron Judge", 100, COMMENCE)])

    _archive(arch, "2026-07-15")
    r = recover(archive_dir=arch, odds_dir=odds)[0]
    assert r["early_price"] == 600          # close is the 400, not the in-game 100
    assert r["clv_early_pp"] == pytest.approx(
        r["clv_actual_pp"] + r["drift_pp"], abs=1e-3)


def test_void_results_are_excluded(tmp_path):
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=10), [_quote("Aaron Judge", 600, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=30), [_quote("Aaron Judge", 400, COMMENCE)])
    _archive(arch, "2026-07-15", outcome="VOID")
    assert recover(archive_dir=arch, odds_dir=odds) == []


def test_date_range_filters(tmp_path):
    odds, arch = tmp_path / "odds", tmp_path / "arch"
    _snapshot(odds, COMMENCE - timedelta(hours=10), [_quote("Aaron Judge", 600, COMMENCE)])
    _snapshot(odds, COMMENCE - timedelta(minutes=30), [_quote("Aaron Judge", 400, COMMENCE)])
    _archive(arch, "2026-07-15")

    assert len(recover("2026-07-01", "2026-07-31", archive_dir=arch, odds_dir=odds)) == 1
    assert recover("2026-08-01", archive_dir=arch, odds_dir=odds) == []


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def test_describe_needs_a_minimum_sample():
    assert describe([{"clv_early_pp": 1.0}] * 24) is None
    assert describe([{"clv_early_pp": 1.0}] * 25) is not None


def test_describe_flags_significance():
    tight = describe([{"clv_early_pp": 1.0 + (i % 2) * 0.01} for i in range(60)])
    assert tight["significant"] is True

    noisy = describe([{"clv_early_pp": 10.0 if i % 2 else -10.0} for i in range(60)])
    assert noisy["significant"] is False


def test_report_handles_no_matches():
    assert "no matched picks" in report([])
