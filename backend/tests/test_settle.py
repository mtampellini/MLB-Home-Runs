"""Settlement against mocked MLB Stats box scores."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.pipeline.slate import MlbStatsClient
from src.results.settle import settle_all_tiers, settle_date, settle_pick


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
# settle_date + settle_all_tiers: read picks from daily archive
# ---------------------------------------------------------------------------

def _write_archive(archives_dir: Path, day: date, *,
                   primary_picks: list[dict] = (),
                   secondary_picks: list[dict] = (),
                   shadow_picks: list[dict] = ()) -> Path:
    archives_dir.mkdir(parents=True, exist_ok=True)
    path = archives_dir / f"{day.isoformat()}.json"
    path.write_text(json.dumps({
        "date": day.isoformat(),
        "generated_at": f"{day.isoformat()}T15:00:00+00:00",
        "model_version": "v7-baseline-0.1.0",
        "league_hr_per_pa": 0.032,
        "funnel": {},
        "primary_picks": list(primary_picks),
        "secondary_picks": list(secondary_picks),
        "shadow_picks": list(shadow_picks),
        "settlement": None,
    }))
    return path


def _mock_client_for_boxes(boxes: dict) -> MlbStatsClient:
    client = MlbStatsClient()
    def _get(path, params=None):
        # Path looks like "/game/{pk}/boxscore"
        pk = int(path.split("/")[2])
        return boxes[pk]
    client._get = MagicMock(side_effect=_get)
    return client


def test_settle_date_reads_picks_from_archive_and_aggregates(tmp_path):
    day = date(2026, 5, 6)
    picks = [
        _pick(1, game_pk=10),
        _pick(2, game_pk=10),    # same game → boxscore cached
        _pick(3, game_pk=20),
    ]
    _write_archive(tmp_path, day, primary_picks=picks)

    # id=1 hits HR, id=2 doesn't, id=3 hits HR.
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
    client = _mock_client_for_boxes(boxes)

    report = settle_date(day, client=client, archives_dir=tmp_path, tier="primary")
    assert report.n_picks == 3
    assert report.n_wins == 2
    assert report.n_losses == 1
    assert report.n_voids == 0
    assert report.units_staked == 3.0
    assert report.units_profit == pytest.approx(3.10 + 3.10 - 1.0)
    # Boxscore was fetched twice (once per game_pk), not three times.
    assert client._get.call_count == 2


def test_settle_date_raises_when_archive_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        settle_date(date(2026, 5, 6), archives_dir=tmp_path)


def test_settle_all_tiers_appends_settlement_block_to_archive(tmp_path):
    """End-to-end: archive in → settlement block appended to same archive."""
    day = date(2026, 5, 6)
    _write_archive(
        tmp_path, day,
        primary_picks=[_pick(1, game_pk=10)],
        secondary_picks=[_pick(2, game_pk=20)],
        shadow_picks=[_pick(3, game_pk=30)],
    )
    boxes = {
        10: _box_with_hr(1, hr=1),
        20: _box_with_hr(2, hr=0),
        30: _box_with_hr(3, hr=2),
    }
    client = _mock_client_for_boxes(boxes)

    reports = settle_all_tiers(day, client=client, archives_dir=tmp_path)
    assert set(reports.keys()) == {"primary", "secondary", "shadow"}
    assert reports["primary"].n_wins == 1
    assert reports["secondary"].n_losses == 1
    assert reports["shadow"].n_wins == 1

    archive = json.loads((tmp_path / f"{day.isoformat()}.json").read_text())
    settle = archive["settlement"]
    assert settle is not None
    assert "settled_at" in settle
    assert settle["primary_summary"]["n_wins"] == 1
    assert settle["secondary_summary"]["n_losses"] == 1
    assert settle["shadow_summary"]["n_wins"] == 1
    assert len(settle["primary_results"]) == 1
    assert len(settle["secondary_results"]) == 1
    assert len(settle["shadow_results"]) == 1


def test_settle_all_tiers_skips_tiers_with_no_picks(tmp_path):
    """Tier with empty picks list is skipped (not in the settlement block)."""
    day = date(2026, 5, 6)
    _write_archive(
        tmp_path, day,
        primary_picks=[_pick(1, game_pk=10)],
        secondary_picks=[],
        shadow_picks=[],
    )
    client = _mock_client_for_boxes({10: _box_with_hr(1, hr=1)})
    reports = settle_all_tiers(day, client=client, archives_dir=tmp_path)
    assert set(reports.keys()) == {"primary"}

    settle = json.loads((tmp_path / f"{day.isoformat()}.json").read_text())["settlement"]
    assert "primary_summary" in settle
    assert "secondary_summary" not in settle
    assert "shadow_summary" not in settle


def test_settle_all_tiers_returns_empty_when_archive_missing(tmp_path):
    """Missing archive → empty result, no exception (workflow can run idempotently)."""
    reports = settle_all_tiers(date(2026, 5, 6), archives_dir=tmp_path)
    assert reports == {}
