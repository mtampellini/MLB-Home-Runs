"""Settlement against mocked MLB Stats box scores."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.pipeline.slate import MlbStatsClient
from src.results.settle import settle_date, settle_pick


def _box_with_hr(batter_id: int, hr: int) -> dict:
    return {
        "teams": {
            "home": {"players": {
                f"ID_{batter_id}": {
                    "person": {"id": batter_id, "fullName": "X"},
                    "stats": {"batting": {"homeRuns": hr, "atBats": 4}},
                }
            }},
            "away": {"players": {}},
        }
    }


def _pick(batter_id: int, *, best_book="draftkings",
          dk_odds=310, fd_odds=290, game_pk=1, model_prob=0.30) -> dict:
    return {
        "batter": "Test Player",
        "batter_id": batter_id,
        "team": "NYY",
        "game_pk": game_pk,
        "game_datetime": "2026-05-06T23:05:00+00:00",
        "best_book": best_book,
        "fd_odds": fd_odds,
        "dk_odds": dk_odds,
        "model_prob": model_prob,
        "market_prob_devig": 0.234,
        "ev_pct": 25.0,
    }


def test_settle_pick_marks_win_when_batter_homers():
    p = _pick(592450)
    box = _box_with_hr(592450, hr=1)
    s = settle_pick(p, box)
    assert s.outcome == "W"
    assert s.actual_hr == 1
    assert s.profit_units == pytest.approx(3.10)   # +310 → payout 3.10


def test_settle_pick_marks_loss_when_no_hr():
    p = _pick(592450)
    box = _box_with_hr(592450, hr=0)
    s = settle_pick(p, box)
    assert s.outcome == "L"
    assert s.actual_hr == 0
    assert s.profit_units == -1.0


def test_settle_pick_marks_void_when_batter_did_not_appear():
    """Batter id not in boxscore → batter_did_not_bat void."""
    p = _pick(592450)
    box = _box_with_hr(999999, hr=1)
    s = settle_pick(p, box)
    assert s.outcome == "VOID"
    assert s.profit_units == 0.0
    assert s.void_reason == "batter_did_not_bat"


def test_settle_pick_marks_void_when_game_pk_missing():
    p = _pick(592450, game_pk=None)
    s = settle_pick(p, boxscore=None)
    assert s.outcome == "VOID"
    assert s.void_reason == "no_game_pk"


def test_settle_pick_marks_void_when_boxscore_unavailable():
    p = _pick(592450)
    s = settle_pick(p, boxscore=None)
    assert s.outcome == "VOID"
    assert s.void_reason == "boxscore_unavailable"


def test_settle_pick_marks_void_when_game_not_final():
    """Empty teams.players → no batting stats → not final → VOID."""
    p = _pick(592450)
    box = {"teams": {"home": {"players": {}}, "away": {"players": {}}}}
    s = settle_pick(p, box)
    assert s.outcome == "VOID"
    assert s.void_reason == "game_not_final"


def test_settle_pick_uses_taken_book_price_for_payout():
    """If best_book is fanduel, payout uses fd_odds, not dk_odds."""
    p = _pick(592450, best_book="fanduel", fd_odds=200, dk_odds=310)
    box = _box_with_hr(592450, hr=1)
    s = settle_pick(p, box)
    # +200 → decimal 3.0 → payout 2.0
    assert s.profit_units == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# settle_date: full file flow
# ---------------------------------------------------------------------------

def _write_picks_file(tmp_dir: Path, day: date, picks: list[dict]) -> None:
    f = tmp_dir / f"picks_{day.isoformat()}.json"
    f.write_text(json.dumps({
        "as_of_date": day.isoformat(),
        "model_version": "v7-baseline-0.1.0",
        "league_hr_per_pa": 0.032,
        "ev_threshold_pct": 25.0,
        "picks": picks,
        "skipped_count": 0,
        "skipped_reference": "x",
    }))


def test_settle_date_writes_results_file_with_aggregate(tmp_path):
    day = date(2026, 5, 6)
    picks = [
        _pick(1, game_pk=10),
        _pick(2, game_pk=10),    # same game → boxscore cached
        _pick(3, game_pk=20),
    ]
    _write_picks_file(tmp_path, day, picks)

    client = MlbStatsClient()
    # Mock boxscore returns: id=1 hits HR, id=2 doesn't, id=3 hits HR.
    boxes = {
        10: {"teams": {
            "home": {"players": {
                "ID_1": {"person": {"id": 1}, "stats": {"batting": {"homeRuns": 1, "atBats": 4}}},
                "ID_2": {"person": {"id": 2}, "stats": {"batting": {"homeRuns": 0, "atBats": 4}}},
            }},
            "away": {"players": {}},
        }},
        20: {"teams": {
            "home": {"players": {
                "ID_3": {"person": {"id": 3}, "stats": {"batting": {"homeRuns": 2, "atBats": 4}}},
            }},
            "away": {"players": {}},
        }},
    }
    def _get(path, params=None):
        # Path looks like "/game/{pk}/boxscore"
        pk = int(path.split("/")[2])
        return boxes[pk]
    client._get = MagicMock(side_effect=_get)

    report = settle_date(day, client=client, processed_dir=tmp_path)
    assert report.n_picks == 3
    assert report.n_wins == 2
    assert report.n_losses == 1
    assert report.n_voids == 0
    assert report.units_staked == 3.0
    # Two wins at +310 (payout 3.10 each) - one loss at -1
    assert report.units_profit == pytest.approx(3.10 + 3.10 - 1.0)
    # Boxscore was fetched twice (once per game_pk), not three times.
    assert client._get.call_count == 2

    # Output file exists with correct shape.
    out = json.loads(report.output_path.read_text())
    assert out["n_wins"] == 2
    assert len(out["results"]) == 3


def test_settle_date_raises_when_picks_file_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        settle_date(date(2026, 5, 6), processed_dir=tmp_path)
