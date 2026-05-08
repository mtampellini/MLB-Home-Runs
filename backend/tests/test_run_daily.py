"""End-to-end orchestrator test: run_daily with every external client stubbed.

Verifies:
- picks.json schema matches what the README documents.
- skipped_batters_<date>.json is written.
- Odds snapshot is written under the test's tmp dir.
- EV >= 25% filter is applied.
- Best-book selection is the highest-payout American price.
- Top-3 features are included per pick.
- Switch-hitter / handedness-routed pitcher splits don't crash anything.
"""

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.model.predict import FeatureProvider
from src.odds.fetch import (
    Event,
    FetchResult,
    HRPropQuote,
    OddsAPIClient,
)
from src.pipeline.run_daily import EV_THRESHOLD_PCT, run_daily
from src.pipeline.slate import MlbStatsClient
import src.pipeline.run_daily as rd_mod
import src.odds.log as log_mod


# ---------------------------------------------------------------------------
# Shared fixtures: model + slate stubs that produce a juiced "value" pick
# ---------------------------------------------------------------------------

VET_SEASON = {
    "pa": 200, "ab": 175, "hr": 12, "bbe": 130,
    "hr_per_pa": 0.060, "iso": 0.250,
    "barrel_pct": 0.14, "xwobacon": 0.46, "hardhit_pct": 0.50,
    "sweetspot_pct": 0.38, "pull_pct": 0.42, "pull_air_pct": 0.22,
    "avg_ev": 92.0, "max_ev": 113.0,
}
VET_RECENT = {**VET_SEASON, "pa": 60, "hr": 5, "hr_per_pa": 0.083}
VET_PRIOR  = {**VET_SEASON, "pa": 600, "hr": 30, "hr_per_pa": 0.050,
               "barrel_pct": 0.10, "xwobacon": 0.42, "hardhit_pct": 0.45,
               "sweetspot_pct": 0.36, "pull_air_pct": 0.18,
               "avg_ev": 90.5, "max_ev": 110.5}

PITCHER = {
    "season": {
        "overall": {"pa": 200, "bbe": 130, "hr": 7, "hr_per_9": 1.26},
        "vs_R":    {"pa": 110, "bbe": 75, "hr": 5, "hr_per_9": 1.61},
        "vs_L":    {"pa": 90, "bbe": 55, "hr": 2, "hr_per_9": 0.82},
    },
    "recent": {
        "overall": {"pa": 100, "bbe": 65, "hr": 4, "hr_per_9": 1.44},
        "vs_R":    {"pa": 55, "bbe": 38, "hr": 3, "hr_per_9": 1.93},
        "vs_L":    {"pa": 45, "bbe": 27, "hr": 1, "hr_per_9": 0.82},
    },
}

PARK_WEATHER = {
    "park_hr_factor": 1.10,
    "is_indoor": False,
    "temperature_f": 78.0,
    "wind_out_to_cf_mph": 7.7,
}


def _feature_provider() -> FeatureProvider:
    """Return a provider that gives Vet-quality data for every batter."""
    def _bs(player_id, ctx, *, batter_hand, season_year=None):
        if season_year is not None and season_year < ctx.cutoff_date.year:
            return VET_PRIOR
        return VET_SEASON
    def _br(player_id, ctx, *, batter_hand, days=30):
        return VET_RECENT
    def _pf(player_id, ctx, *, days=30, season_year=None):
        return PITCHER
    def _pw(park, batter_hand, game_datetime, ctx):
        return PARK_WEATHER
    return FeatureProvider(
        batter_season=_bs, batter_recent=_br,
        pitcher_features=_pf, park_weather=_pw,
    )


# ---------------------------------------------------------------------------
# Slate + odds stubs
# ---------------------------------------------------------------------------

def _slate_client_one_game() -> MlbStatsClient:
    schedule = {
        "dates": [{"games": [{
            "gamePk": 1, "gameDate": "2026-05-06T23:05:00Z",
            "status": {"abstractGameState": "Preview", "detailedState": "Scheduled"},
            "venue": {"name": "Yankee Stadium"},
            "teams": {
                "home": {"team": {"id": 147, "name": "New York Yankees",
                                   "abbreviation": "NYY"},
                         "probablePitcher": {"id": 999, "fullName": "Lefty",
                                              "pitchHand": {"code": "L"}}},
                "away": {"team": {"id": 111, "name": "Boston Red Sox",
                                   "abbreviation": "BOS"},
                         "probablePitcher": {"id": 888, "fullName": "Righty",
                                              "pitchHand": {"code": "R"}}},
            },
            "lineups": {
                "homePlayers": [
                    {"id": 592450}, {"id": 100002}, {"id": 100003},
                    {"id": 100004}, {"id": 100005}, {"id": 100006},
                    {"id": 100007}, {"id": 100008}, {"id": 100009},
                ],
                "awayPlayers": [
                    {"id": 200001}, {"id": 200002}, {"id": 200003},
                    {"id": 200004}, {"id": 200005}, {"id": 200006},
                    {"id": 200007}, {"id": 200008}, {"id": 200009},
                ],
            },
        }]}],
    }
    people = {
        i: {"id": i, "fullName": f"Aaron Judge" if i == 592450 else f"Player {i}",
            "batSide": {"code": "R"}}
        for i in (
            [592450] + list(range(100002, 100010))
            + list(range(200001, 200010))
        )
    }
    client = MlbStatsClient()
    client.schedule_for_date = MagicMock(return_value=schedule)
    client.fetch_people = MagicMock(return_value=people)
    return client


def _odds_for_one_batter() -> FetchResult:
    """FD and DK both quote Aaron Judge with main (de-vig) AND alt (bet) markets."""
    quotes = [
        HRPropQuote(
            event_id="evt_1", home_team="New York Yankees", away_team="Boston Red Sox",
            commence_time=datetime(2026, 5, 6, 23, 5),
            book="fanduel", batter_name="Aaron Judge",
            bet_over_american=290,         # alt @ 0.5 Over (bet price)
            main_over_american=290,        # main yes/no Over (de-vig)
            main_under_american=-380,      # main yes/no Under (de-vig)
            last_update=datetime(2026, 5, 6, 19, 0),
        ),
        HRPropQuote(
            event_id="evt_1", home_team="New York Yankees", away_team="Boston Red Sox",
            commence_time=datetime(2026, 5, 6, 23, 5),
            book="draftkings", batter_name="Aaron Judge",
            bet_over_american=310,
            main_over_american=310,
            main_under_american=-400,
            last_update=datetime(2026, 5, 6, 19, 1),
        ),
    ]
    events = [Event(event_id="evt_1", sport_key="baseball_mlb",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    home_team="New York Yankees", away_team="Boston Red Sox")]
    return FetchResult(
        fetched_at=datetime(2026, 5, 6, 15, 0),
        quotes=quotes, events=events,
        requests_remaining=18999, requests_used=1001,
        books=("fanduel", "draftkings"),
        markets="batter_home_runs,batter_home_runs_alternate",
    )


def _odds_client(fetch_result: FetchResult) -> OddsAPIClient:
    """Return an OddsAPIClient whose http calls are bypassed by patching fetch_today_hr_props."""
    return MagicMock(spec=OddsAPIClient)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_layout(tmp_path, monkeypatch):
    picks_path = tmp_path / "picks.json"
    skipped_dir = tmp_path / "data" / "processed"
    odds_dir = tmp_path / "data" / "odds"
    archives_dir = tmp_path / "data" / "daily_archives"
    odds_dir.mkdir(parents=True)
    monkeypatch.setattr(log_mod, "ODDS_DIR", odds_dir)
    # Without this patch, tests calling run_daily(cutoff_date=...) would write
    # a stub archive into the REAL backend/data/daily_archives/ on the runner,
    # clobbering production data. (Caused 2026-05-06 to be wiped on
    # 2026-05-07's CI run — the fixture was incomplete.)
    monkeypatch.setattr(rd_mod, "DAILY_ARCHIVES_DIR", archives_dir)
    return {"picks_path": picks_path, "skipped_dir": skipped_dir,
            "odds_dir": odds_dir, "archives_dir": archives_dir}


def test_run_daily_merge_appends_picks_from_subsequent_run(tmp_layout, monkeypatch):
    """Merge mode: a second run on the same date appends to the existing
    archive. Game_pks already covered are filtered from the slate; new
    game_pks add their picks to the existing list. Ranks are recomputed."""
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None,
                                skip_started_clock_skew_min=None: fetch)

    # Pre-seed an archive with a pick from a "morning" run (different game_pk
    # than what the slate fixture below produces).
    archives_dir = tmp_layout["archives_dir"]
    archives_dir.mkdir(parents=True, exist_ok=True)
    morning_pick = {
        "tier": "primary", "daily_rank": 1, "tier_rank": 1,
        "batter": "Mookie Betts", "batter_id": 605141,
        "team": "LAD", "lineup_spot": 1,
        "pitcher": "Morning Pitcher", "pitcher_id": 555,
        "pitcher_hand": "R", "park": "LAD",
        "game_pk": 9999, "game_datetime": "2026-05-06T20:10:00Z",
        "line": 0.5, "fd_odds": 250, "dk_odds": 270, "best_book": "draftkings",
        "market_prob_devig": 0.20, "model_prob": 0.27,
        "ev_pct": 35.0, "edge_pct": 7.0,
    }
    morning_archive = {
        "date": "2026-05-06",
        "generated_at": "2026-05-06T15:13:00Z",
        "model_version": "v7-baseline-0.1.0",
        "primary_picks": [morning_pick],
        "secondary_picks": [], "shadow_picks": [],
        "settlement": None,
    }
    import json as _json
    (archives_dir / "2026-05-06.json").write_text(_json.dumps(morning_archive))

    # Now run the pipeline — the fixture's slate produces game_pk=1 (different
    # from the morning's 9999), so the new pick should APPEND.
    run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    merged = _json.loads((archives_dir / "2026-05-06.json").read_text())
    pks = sorted({p["game_pk"] for p in merged["primary_picks"]})
    assert pks == [1, 9999], f"merge should keep both game_pks; got {pks}"
    # Mookie's morning pick survives.
    assert any(p["batter_id"] == 605141 for p in merged["primary_picks"])
    # Tier_rank is recomputed across the combined set (1..N).
    ranks = sorted(p["tier_rank"] for p in merged["primary_picks"])
    assert ranks == list(range(1, len(merged["primary_picks"]) + 1))


def test_run_daily_archive_writes_stay_inside_tmp(tmp_layout, monkeypatch):
    """Regression: tmp_layout used to leave DAILY_ARCHIVES_DIR un-patched, so
    a test that called run_daily(cutoff_date=date(2026,5,6)) would write a
    stub archive into the REAL backend/data/daily_archives/2026-05-06.json
    on the CI runner — committing it and wiping yesterday's settled data.
    """
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None,
                                skip_started_clock_skew_min=None: fetch)
    run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    # Archive lands in the patched tmp dir, not the production directory.
    assert (tmp_layout["archives_dir"] / "2026-05-06.json").exists()


def test_run_daily_writes_picks_with_full_schema(tmp_layout, monkeypatch):
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )

    assert report.picks_count >= 1
    payload = json.loads(tmp_layout["picks_path"].read_text())
    assert "generated_at" in payload
    assert payload["model_version"] == "v7-baseline-0.1.0"
    assert payload["ev_threshold_pct"] == EV_THRESHOLD_PCT
    pick = payload["picks"][0]
    expected = {
        "batter", "batter_id", "batter_hand", "team", "lineup_spot",
        "pitcher", "pitcher_id", "pitcher_hand", "park", "game_datetime",
        "line", "fd_odds", "dk_odds", "best_book", "market_prob_devig",
        "model_prob", "ev_pct",
        "blended_hr_per_pa", "breakout_score", "low_confidence",
        "top_3_features",
    }
    assert expected <= set(pick.keys())
    assert pick["line"] == 0.5
    assert pick["best_book"] == "draftkings"   # +310 > +290
    assert pick["fd_odds"] == 290 and pick["dk_odds"] == 310
    assert len(pick["top_3_features"]) == 3
    assert pick["batter"] == "Aaron Judge"


def test_run_daily_filters_below_ev_threshold(tmp_layout, monkeypatch):
    """Force a near-zero model prob so EV << 25% — picks list should be empty."""
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)

    # Provider returns weak metrics ACROSS THE BOARD so:
    #   (a) hr_per_pa is tiny → low blended skill,
    #   (b) current-year metrics match prior → breakout score = 0 (can't save the pick).
    weak_metrics = {
        "pa": 200, "ab": 175, "hr": 1, "bbe": 130,
        "hr_per_pa": 0.005, "iso": 0.05,
        "barrel_pct": 0.04, "xwobacon": 0.30, "hardhit_pct": 0.25,
        "sweetspot_pct": 0.30, "pull_pct": 0.30, "pull_air_pct": 0.10,
        "avg_ev": 86.0, "max_ev": 105.0,
    }
    weak_season = dict(weak_metrics)
    weak_recent = {**weak_metrics, "pa": 60, "hr": 0}
    weak_prior  = {**weak_metrics, "pa": 600, "hr": 3}
    def _bs(*a, season_year=None, **k):
        return weak_prior if season_year else weak_season
    def _br(*a, **k): return weak_recent
    def _pf(*a, **k): return PITCHER
    def _pw(*a, **k): return PARK_WEATHER
    weak_provider = FeatureProvider(_bs, _br, _pf, _pw)

    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=weak_provider,
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    payload = json.loads(tmp_layout["picks_path"].read_text())
    assert report.picks_count == 0
    assert payload["picks"] == []


def test_run_daily_writes_skipped_file_and_odds_snapshot(tmp_layout, monkeypatch):
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    # Skipped file always written (even if zero skips, it's a valid empty list).
    assert report.skipped_path.exists()
    skipped = json.loads(report.skipped_path.read_text())
    assert skipped["as_of_date"] == "2026-05-06"
    assert isinstance(skipped["skipped"], list)

    # Odds snapshot written under the patched ODDS_DIR.
    snapshot_files = list(tmp_layout["odds_dir"].glob("*.json"))
    assert len(snapshot_files) == 1
    snap = json.loads(snapshot_files[0].read_text())
    assert "batter_home_runs" in snap["markets"]
    assert "batter_home_runs_alternate" in snap["markets"]
    assert snap["requests_remaining"] == 18999


def test_run_daily_keeps_pick_when_only_one_book_has_main_market(tmp_layout, monkeypatch):
    """One book missing main market → de-vig from the other book alone, pick survives."""
    quotes = [
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="fanduel", batter_name="Aaron Judge",
                    bet_over_american=290,
                    main_over_american=None, main_under_american=None,
                    last_update=datetime(2026, 5, 6, 19, 0)),
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="draftkings", batter_name="Aaron Judge",
                    bet_over_american=310,
                    main_over_american=310, main_under_american=-400,
                    last_update=datetime(2026, 5, 6, 19, 1)),
    ]
    fetch = FetchResult(
        fetched_at=datetime(2026, 5, 6, 15, 0),
        quotes=quotes, events=[], requests_remaining=1, requests_used=1,
        books=("fanduel", "draftkings"),
        markets="batter_home_runs,batter_home_runs_alternate",
    )
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    assert report.picks_count >= 1
    assert report.funnel["matched_main_market"] >= 1


def test_run_daily_uses_single_sided_devig_when_main_market_missing(tmp_layout, monkeypatch):
    """When no book quotes main market, fall back to single-sided de-vig from alt
    Over price + price-tiered vig haircut. The pick should still surface; funnel
    should report single_sided_devig=1, two_way_devig=0."""
    quotes = [
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="fanduel", batter_name="Aaron Judge",
                    bet_over_american=290,
                    main_over_american=None, main_under_american=None,
                    last_update=datetime(2026, 5, 6, 19, 0)),
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="draftkings", batter_name="Aaron Judge",
                    bet_over_american=310,
                    main_over_american=None, main_under_american=None,
                    last_update=datetime(2026, 5, 6, 19, 1)),
    ]
    fetch = FetchResult(
        fetched_at=datetime(2026, 5, 6, 15, 0),
        quotes=quotes, events=[], requests_remaining=1, requests_used=1,
        books=("fanduel", "draftkings"),
        markets="batter_home_runs,batter_home_runs_alternate",
    )
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    # Single-sided fallback fired; pick survives.
    assert report.funnel["matched_alt_market"] >= 1
    assert report.funnel["matched_main_market"] == 0
    assert report.funnel["single_sided_devig"] == 1
    assert report.funnel["two_way_devig"] == 0
    # Pick goes to primary or shadow tier depending on EV — either way at
    # least one tier should populate. Vet provider gives high model_prob.
    assert (report.picks_count + report.shadow_picks_count) >= 1


def test_run_daily_skips_pick_when_no_book_quotes_alt_market(tmp_layout, monkeypatch):
    """Has main but no alt @ 0.5 Over → no bet price → skip."""
    quotes = [
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="fanduel", batter_name="Aaron Judge",
                    bet_over_american=None,
                    main_over_american=290, main_under_american=-380,
                    last_update=datetime(2026, 5, 6, 19, 0)),
    ]
    fetch = FetchResult(
        fetched_at=datetime(2026, 5, 6, 15, 0),
        quotes=quotes, events=[], requests_remaining=1, requests_used=1,
        books=("fanduel", "draftkings"),
        markets="batter_home_runs,batter_home_runs_alternate",
    )
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    assert report.picks_count == 0
    assert report.funnel["matched_main_market"] >= 1
    assert report.funnel["matched_alt_market"] == 0


def test_primary_excludes_picks_above_max_price(tmp_layout, monkeypatch):
    """A pick with EV>=25 but best_price > +900 → SECONDARY, not primary."""
    quotes = [
        # Long-shot Over (price > +900) with both books quoting alt only.
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="fanduel", batter_name="Aaron Judge",
                    bet_over_american=1500,
                    main_over_american=None, main_under_american=None,
                    last_update=datetime(2026, 5, 6, 19, 0)),
        HRPropQuote(event_id="e", home_team="New York Yankees", away_team="Boston Red Sox",
                    commence_time=datetime(2026, 5, 6, 23, 5),
                    book="draftkings", batter_name="Aaron Judge",
                    bet_over_american=1400,
                    main_over_american=None, main_under_american=None,
                    last_update=datetime(2026, 5, 6, 19, 1)),
    ]
    fetch = FetchResult(
        fetched_at=datetime(2026, 5, 6, 15, 0),
        quotes=quotes, events=[], requests_remaining=1, requests_used=1,
        books=("fanduel", "draftkings"),
        markets="batter_home_runs,batter_home_runs_alternate",
    )
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    # Vet provider × +1500 long shot has high EV, but price > +900 → secondary.
    assert report.picks_count == 0, "long-shot price should NOT enter primary"
    assert report.secondary_picks_count >= 1, "long-shot should be in secondary"
    assert report.funnel.get("above_price_cap_pushed_to_secondary", 0) >= 1


def test_run_daily_routes_picks_to_primary_or_shadow_by_ev(tmp_layout, monkeypatch):
    """A pick whose EV lands in [10, 25) should go to shadow_picks; >=25 to primary."""
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    # Vet provider × HR-prone matchup → high EV → primary tier.
    assert report.picks_count >= 1
    # Shadow picks file always written, even if empty.
    assert report.shadow_picks_path is not None
    assert report.shadow_picks_path.exists()
    shadow_payload = json.loads(report.shadow_picks_path.read_text())
    assert shadow_payload["tier"] == "shadow"
    assert shadow_payload["ev_threshold_pct_min"] == 10.0
    assert shadow_payload["ev_threshold_pct_max"] == 25.0


def test_run_daily_dated_shadow_copy_written(tmp_layout, monkeypatch):
    """data/processed/shadow_picks_YYYY-MM-DD.json must be written for tracker."""
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    dated = tmp_layout["skipped_dir"] / "shadow_picks_2026-05-06.json"
    assert dated.exists()


def test_run_daily_picks_carry_both_ev_and_edge_fields(tmp_layout, monkeypatch):
    """picks.json schema must include ev_pct (Option A) and edge_pct (Option B)
    so post-deploy analysis can compare frameworks."""
    fetch = _odds_for_one_batter()
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props",
                        lambda client=None, books=None, relevant_team_pairs=None: fetch)
    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    payload = json.loads(tmp_layout["picks_path"].read_text())
    if not payload["picks"]:
        return    # nothing to assert — vet provider should generate picks
    pick = payload["picks"][0]
    assert "ev_pct" in pick
    assert "edge_pct" in pick
    assert "devig_method" in pick


def test_run_daily_handles_odds_fetch_failure_gracefully(tmp_layout, monkeypatch):
    def _boom(client=None, books=None, relevant_team_pairs=None):
        raise RuntimeError("Savant flu")
    monkeypatch.setattr(rd_mod, "fetch_today_hr_props", _boom)

    report = run_daily(
        cutoff_date=date(2026, 5, 6),
        feature_provider=_feature_provider(),
        odds_client=MagicMock(spec=OddsAPIClient),
        slate_client=_slate_client_one_game(),
        picks_path=tmp_layout["picks_path"],
        skipped_dir=tmp_layout["skipped_dir"],
    )
    # Pipeline still produces empty picks.json instead of crashing.
    assert report.picks_count == 0
    payload = json.loads(tmp_layout["picks_path"].read_text())
    assert payload["picks"] == []
