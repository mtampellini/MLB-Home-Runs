"""End-to-end test of predict_slate with a stub FeatureProvider.

Exercises every branch:
- Skip path (no track record)
- Low-confidence path (no current season data, prior year only)
- Full prediction path (vet with full data + breakout)
- Top-3 feature ranking
"""

from datetime import date, datetime

import pytest

from src.backtest.as_of_context import AsOfContext
from src.model.baseline import BaselineConfig
from src.model.predict import (
    FeatureProvider,
    SlateEntry,
    predict_slate,
    top_n_features,
)


# ---------------------------------------------------------------------------
# Stub fixture
# ---------------------------------------------------------------------------

VET_SEASON = {
    "pa": 200, "ab": 175, "hr": 12, "bbe": 130,
    "hr_per_pa": 0.060, "iso": 0.250,
    "barrel_pct": 0.14, "xwobacon": 0.46, "hardhit_pct": 0.50,
    "sweetspot_pct": 0.38, "pull_pct": 0.42, "avg_ev": 92.0,
    "scope": "season",
}
VET_RECENT = {**VET_SEASON, "pa": 60, "hr": 5, "hr_per_pa": 0.083, "scope": "last_30d"}
VET_PRIOR = {**VET_SEASON, "pa": 600, "hr": 30, "hr_per_pa": 0.050,
             "barrel_pct": 0.10, "xwobacon": 0.42, "hardhit_pct": 0.45, "avg_ev": 90.5}

ROOKIE_SEASON_LOW_PA = {**VET_SEASON, "pa": 30, "hr": 1, "hr_per_pa": 0.033}
ROOKIE_PRIOR_EMPTY = {"pa": 0}

EARLY_SEASON_NO_GAMES = {**VET_SEASON, "pa": 0, "hr": 0, "hr_per_pa": float("nan")}

PITCHER_FEATURES = {
    "season": {
        "scope": "season", "player_id": 999, "season_year": 2026,
        "window_start": "2026-03-01", "window_end": "2026-05-05",
        "overall": {"pa": 200, "bbe": 130, "hr": 7, "ip_estimate": 50.0,
                    "hr_per_9": 1.26, "barrel_pct_allowed": 0.10,
                    "xwobacon_allowed": 0.40, "hardhit_pct_allowed": 0.42},
        "vs_R":    {"pa": 110, "bbe": 75, "hr": 5, "ip_estimate": 28.0,
                    "hr_per_9": 1.61, "barrel_pct_allowed": 0.12,
                    "xwobacon_allowed": 0.43, "hardhit_pct_allowed": 0.45},
        "vs_L":    {"pa": 90, "bbe": 55, "hr": 2, "ip_estimate": 22.0,
                    "hr_per_9": 0.82, "barrel_pct_allowed": 0.07,
                    "xwobacon_allowed": 0.36, "hardhit_pct_allowed": 0.38},
    },
    "recent": {
        "scope": "last_30d", "player_id": 999, "days": 30,
        "window_start": "2026-04-06", "window_end": "2026-05-05",
        "overall": {"pa": 100, "bbe": 65, "hr": 4, "ip_estimate": 25.0,
                    "hr_per_9": 1.44, "barrel_pct_allowed": 0.11,
                    "xwobacon_allowed": 0.41, "hardhit_pct_allowed": 0.43},
        "vs_R":    {"pa": 55, "bbe": 38, "hr": 3, "ip_estimate": 14.0,
                    "hr_per_9": 1.93, "barrel_pct_allowed": 0.13,
                    "xwobacon_allowed": 0.44, "hardhit_pct_allowed": 0.46},
        "vs_L":    {"pa": 45, "bbe": 27, "hr": 1, "ip_estimate": 11.0,
                    "hr_per_9": 0.82, "barrel_pct_allowed": 0.07,
                    "xwobacon_allowed": 0.36, "hardhit_pct_allowed": 0.38},
    },
}

NEUTRAL_PARK_WEATHER = {
    "park": "NYY", "park_name": "Yankee Stadium", "bat_side": "R",
    "park_hr_factor": 1.10,
    "is_indoor": False,
    "temperature_f": 78.0,
    "wind_speed_mph": 8.0,
    "wind_direction_deg": 270.0,   # from W → blowing east; CF at 75 → component out to CF
    "wind_out_to_cf_mph": 7.7,
    "precipitation_in": 0.0,
    "cf_bearing_deg": 75.0,
    "game_datetime": "2026-05-06T19:05:00",
    "as_of": "2026-05-06",
}


def _make_provider(*,
                   season_for: dict,
                   recent_for: dict,
                   prior_for: dict,
                   pitcher: dict = PITCHER_FEATURES,
                   park_wx: dict = NEUTRAL_PARK_WEATHER):
    """Build a FeatureProvider that maps batter_id → canned dicts.

    season_for/recent_for/prior_for are batter_id → dict mappings.
    """
    def _bs(player_id, ctx, *, batter_hand, season_year=None):
        if season_year is not None and season_year < ctx.cutoff_date.year:
            return prior_for.get(player_id, {"pa": 0})
        return season_for.get(player_id, {"pa": 0})

    def _br(player_id, ctx, *, batter_hand, days=30):
        return recent_for.get(player_id, {"pa": 0})

    def _pf(player_id, ctx, *, days=30, season_year=None):
        return pitcher

    def _pw(park, batter_hand, game_datetime, ctx):
        return park_wx

    return FeatureProvider(
        batter_season=_bs,
        batter_recent=_br,
        pitcher_features=_pf,
        park_weather=_pw,
    )


def _entry(batter_id, batter_hand="R", lineup_spot=3) -> SlateEntry:
    return SlateEntry(
        batter_id=batter_id,
        batter_name=f"Player {batter_id}",
        batter_hand=batter_hand,
        team="NYY",
        pitcher_id=999,
        pitcher_name="Some Lefty",
        pitcher_hand="L",
        park="NYY",
        game_datetime=datetime(2026, 5, 6, 19, 5),
        lineup_spot=lineup_spot,
    )


CTX = AsOfContext(cutoff_date=date(2026, 5, 6))


# ---------------------------------------------------------------------------
# Skip path
# ---------------------------------------------------------------------------

def test_skip_no_track_record():
    provider = _make_provider(
        season_for={1: ROOKIE_SEASON_LOW_PA},
        recent_for={1: ROOKIE_SEASON_LOW_PA},
        prior_for={1: ROOKIE_PRIOR_EMPTY},
    )
    rows = predict_slate([_entry(1)], CTX, provider=provider)
    assert len(rows) == 1
    r = rows[0]
    assert r.skipped is True
    assert r.skip_code == "LOW_DATA"
    assert r.prediction is None
    assert "current_season_PA=30" in r.skip_reason


def test_keep_when_prior_year_carries_low_current():
    provider = _make_provider(
        season_for={1: ROOKIE_SEASON_LOW_PA},
        recent_for={1: ROOKIE_SEASON_LOW_PA},
        prior_for={1: VET_PRIOR},
    )
    rows = predict_slate([_entry(1)], CTX, provider=provider)
    assert rows[0].skipped is False
    assert rows[0].prediction is not None


# ---------------------------------------------------------------------------
# Low-confidence path
# ---------------------------------------------------------------------------

def test_low_confidence_when_no_current_season_pa():
    provider = _make_provider(
        season_for={1: EARLY_SEASON_NO_GAMES},
        recent_for={1: EARLY_SEASON_NO_GAMES},
        prior_for={1: VET_PRIOR},
    )
    rows = predict_slate([_entry(1)], CTX, provider=provider)
    r = rows[0]
    assert r.skipped is False
    assert r.low_confidence is True
    assert r.prediction is not None
    # Blend should resolve from prior year alone.
    assert r.batter_blend.used_prior_year is True
    # Breakout should be 0 — current data missing means no signal.
    assert r.breakout.score == 0.0


# ---------------------------------------------------------------------------
# Full prediction path
# ---------------------------------------------------------------------------

def test_full_prediction_returns_p_hr_in_zero_to_one():
    provider = _make_provider(
        season_for={1: VET_SEASON},
        recent_for={1: VET_RECENT},
        prior_for={1: VET_PRIOR},
    )
    rows = predict_slate([_entry(1)], CTX, provider=provider)
    r = rows[0]
    assert r.skipped is False
    assert r.low_confidence is False
    p = r.prediction
    assert 0.0 < p.p_hr < 1.0
    # p_per_pa lives in the configured clip range; hitting the boundary is valid
    # (the vet fixture intentionally stacks every positive factor).
    assert 0.001 <= p.p_per_pa <= 0.25
    # Breakout should be positive — current Statcast > prior across the board.
    assert r.breakout.score > 0.0
    # Blend should NOT use prior year (season_pa=200 → dynamic weight = 0).
    assert r.batter_blend.used_prior_year is False


def test_pitcher_split_picked_by_batter_hand():
    """Right-handed batter must read pitcher's vs_R split, not overall."""
    provider = _make_provider(
        season_for={1: VET_SEASON, 2: VET_SEASON},
        recent_for={1: VET_RECENT, 2: VET_RECENT},
        prior_for={1: VET_PRIOR, 2: VET_PRIOR},
    )
    r_rhb = predict_slate([_entry(1, batter_hand="R")], CTX, provider=provider)[0]
    r_lhb = predict_slate([_entry(2, batter_hand="L")], CTX, provider=provider)[0]
    # Pitcher in fixture is much worse vs RHB (1.61 HR/9) than vs LHB (0.82).
    # Same batter inputs → RHB must end up with higher p_hr.
    assert r_rhb.prediction.p_hr > r_lhb.prediction.p_hr


def test_lineup_spot_drives_pa_per_game():
    provider = _make_provider(
        season_for={1: VET_SEASON, 2: VET_SEASON},
        recent_for={1: VET_RECENT, 2: VET_RECENT},
        prior_for={1: VET_PRIOR, 2: VET_PRIOR},
    )
    leadoff = predict_slate([_entry(1, lineup_spot=1)], CTX, provider=provider)[0]
    nine = predict_slate([_entry(2, lineup_spot=9)], CTX, provider=provider)[0]
    # More PAs in the leadoff spot → higher P(at least 1 HR).
    assert leadoff.prediction.pa_per_game > nine.prediction.pa_per_game
    assert leadoff.prediction.p_hr > nine.prediction.p_hr


def test_components_dict_has_all_factors():
    provider = _make_provider(
        season_for={1: VET_SEASON},
        recent_for={1: VET_RECENT},
        prior_for={1: VET_PRIOR},
    )
    rows = predict_slate([_entry(1)], CTX, provider=provider)
    keys = set(rows[0].prediction.components)
    assert {"batter_skill", "breakout_signal", "pitcher", "park",
            "temperature", "wind"} <= keys


# ---------------------------------------------------------------------------
# Top-3 feature ranking
# ---------------------------------------------------------------------------

def test_top_3_returns_three_items_ranked_by_deviation():
    provider = _make_provider(
        season_for={1: VET_SEASON},
        recent_for={1: VET_RECENT},
        prior_for={1: VET_PRIOR},
    )
    rows = predict_slate([_entry(1)], CTX, provider=provider)
    top3 = top_n_features(rows[0].prediction, n=3)
    assert len(top3) == 3
    # Sorted descending by deviation.
    devs = [item["deviation"] for item in top3]
    assert devs == sorted(devs, reverse=True)
    # Each item has the picks.json shape.
    for item in top3:
        assert set(item.keys()) == {"name", "value", "deviation"}
        assert item["deviation"] >= 0


def test_top_3_handles_empty_components():
    from src.model.baseline import BaselinePrediction
    empty_pred = BaselinePrediction(
        p_hr=0.0, p_per_pa=0.0, pa_per_game=4.2,
        blended_hr_per_pa=0.0, breakout_score=0.0,
        adjusted_per_pa=0.0, components={},
    )
    assert top_n_features(empty_pred) == []
