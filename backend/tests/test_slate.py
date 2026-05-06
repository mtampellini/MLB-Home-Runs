"""Slate parser + builder tests with mocked MLB Stats API."""

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest

from src.pipeline.slate import (
    MlbStatsClient,
    build_slate,
    normalize_name,
    parse_schedule,
    _effective_bat_side,
)


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

def test_normalize_name_strips_accents():
    assert normalize_name("José Ramírez") == "jose ramirez"


def test_normalize_name_strips_suffixes():
    assert normalize_name("Cedric Mullins II") == "cedric mullins"
    assert normalize_name("Ronald Acuña Jr.") == "ronald acuna"


def test_normalize_name_strips_punctuation_and_compresses_whitespace():
    assert normalize_name("L.J.  Cron") == "lj cron"


def test_normalize_name_handles_empty():
    assert normalize_name("") == ""


# ---------------------------------------------------------------------------
# Switch-hitter handedness logic
# ---------------------------------------------------------------------------

def test_switch_hitter_vs_lhp_bats_R():
    assert _effective_bat_side("S", "L") == "R"


def test_switch_hitter_vs_rhp_bats_L():
    assert _effective_bat_side("S", "R") == "L"


def test_non_switch_hitter_keeps_native_side():
    assert _effective_bat_side("L", "R") == "L"
    assert _effective_bat_side("R", "L") == "R"


def test_unknown_side_defaults_to_R():
    assert _effective_bat_side("", "R") == "R"


# ---------------------------------------------------------------------------
# Schedule parsing
# ---------------------------------------------------------------------------

SCHEDULE_PAYLOAD = {
    "dates": [{
        "date": "2026-05-06",
        "games": [
            {
                "gamePk": 12345,
                "gameDate": "2026-05-06T23:05:00Z",
                "venue": {"id": 3313, "name": "Yankee Stadium"},
                "teams": {
                    "home": {
                        "team": {"id": 147, "name": "New York Yankees", "abbreviation": "NYY"},
                        "probablePitcher": {
                            "id": 999, "fullName": "Some Lefty",
                            "pitchHand": {"code": "L"},
                        },
                    },
                    "away": {
                        "team": {"id": 111, "name": "Boston Red Sox", "abbreviation": "BOS"},
                        "probablePitcher": {
                            "id": 888, "fullName": "Some Righty",
                            "pitchHand": {"code": "R"},
                        },
                    },
                },
                "lineups": {
                    "homePlayers": [
                        {"id": 592450}, {"id": 624413}, {"id": 519317},
                        {"id": 543063}, {"id": 502675}, {"id": 656941},
                        {"id": 596142}, {"id": 666156}, {"id": 595879},
                    ],
                    "awayPlayers": [
                        {"id": 605141}, {"id": 645277}, {"id": 622491},
                        {"id": 656555}, {"id": 593428}, {"id": 663538},
                        {"id": 666182}, {"id": 595453}, {"id": 656976},
                    ],
                },
            },
            {
                # Game with NO lineup posted yet — must be skipped.
                "gamePk": 67890,
                "gameDate": "2026-05-06T19:35:00Z",
                "venue": {"id": 5, "name": "Citi Field"},
                "teams": {
                    "home": {"team": {"id": 121, "abbreviation": "NYM"},
                             "probablePitcher": {"id": 1, "fullName": "X",
                                                 "pitchHand": {"code": "R"}}},
                    "away": {"team": {"id": 144, "abbreviation": "ATL"},
                             "probablePitcher": {"id": 2, "fullName": "Y",
                                                 "pitchHand": {"code": "R"}}},
                },
                "lineups": {"homePlayers": [], "awayPlayers": []},
            },
        ],
    }]
}


def test_parse_schedule_extracts_games_and_probables():
    games = parse_schedule(SCHEDULE_PAYLOAD)
    assert len(games) == 2
    g0 = games[0]
    assert g0.game_pk == 12345
    assert g0.home_team_code == "NYY"
    assert g0.away_team_code == "BOS"
    assert g0.home_starter_id == 999
    assert g0.home_starter_hand == "L"
    assert g0.away_starter_hand == "R"
    assert len(g0.home_lineup_ids) == 9
    assert g0.park_code == "NYY"


def test_parse_schedule_handles_missing_lineups():
    games = parse_schedule(SCHEDULE_PAYLOAD)
    g1 = games[1]
    assert g1.home_lineup_ids == []
    assert g1.away_lineup_ids == []


# ---------------------------------------------------------------------------
# build_slate end-to-end with mocked client
# ---------------------------------------------------------------------------

def _people_payload(ids):
    """Synthesize a /people response. Even ids → 'L', odd → 'R', ones ending in 5 → 'S'."""
    out = []
    for i in ids:
        if str(i).endswith("5"):
            side = "S"
        elif int(i) % 2 == 0:
            side = "L"
        else:
            side = "R"
        out.append({"id": int(i), "fullName": f"Player {i}",
                    "batSide": {"code": side}})
    return {"people": out}


def test_build_slate_creates_18_entries_for_one_game_with_lineups():
    """One game with two posted 9-player lineups → 18 slate entries."""
    schedule_one_game = {"dates": [SCHEDULE_PAYLOAD["dates"][0]["games"][0]],
                          "totalGames": 1}
    schedule_one_game = {"dates": [{"games": [SCHEDULE_PAYLOAD["dates"][0]["games"][0]]}]}

    client = MlbStatsClient()
    client.schedule_for_date = MagicMock(return_value=schedule_one_game)

    all_ids = (SCHEDULE_PAYLOAD["dates"][0]["games"][0]["lineups"]["homePlayers"]
               + SCHEDULE_PAYLOAD["dates"][0]["games"][0]["lineups"]["awayPlayers"])
    all_ids = [p["id"] for p in all_ids]
    client.fetch_people = MagicMock(return_value={
        p["id"]: p for p in _people_payload(all_ids)["people"]
    })

    slate, meta = build_slate(date(2026, 5, 6), client=client)
    assert len(slate) == 18
    assert meta["games_with_lineups"] == 1
    assert meta["games_no_lineup_skipped"] == 0


def test_build_slate_skips_game_without_lineup():
    client = MlbStatsClient()
    client.schedule_for_date = MagicMock(return_value=SCHEDULE_PAYLOAD)
    # Resolve handedness only for posted-lineup IDs (game 12345).
    posted_ids = [p["id"] for p in
                   SCHEDULE_PAYLOAD["dates"][0]["games"][0]["lineups"]["homePlayers"]
                   + SCHEDULE_PAYLOAD["dates"][0]["games"][0]["lineups"]["awayPlayers"]]
    client.fetch_people = MagicMock(return_value={
        p["id"]: p for p in _people_payload(posted_ids)["people"]
    })

    slate, meta = build_slate(date(2026, 5, 6), client=client)
    assert meta["games_total"] == 2
    assert meta["games_with_lineups"] == 1
    assert meta["games_no_lineup_skipped"] == 1


def test_build_slate_resolves_switch_hitter_against_pitcher_hand():
    """Player id ending in 5 → 'S'. vs LHP they bat R; vs RHP they bat L."""
    client = MlbStatsClient()
    client.schedule_for_date = MagicMock(return_value={
        "dates": [{"games": [SCHEDULE_PAYLOAD["dates"][0]["games"][0]]}],
    })
    all_ids = [p["id"] for p in
               SCHEDULE_PAYLOAD["dates"][0]["games"][0]["lineups"]["homePlayers"]
               + SCHEDULE_PAYLOAD["dates"][0]["games"][0]["lineups"]["awayPlayers"]]
    client.fetch_people = MagicMock(return_value={
        p["id"]: p for p in _people_payload(all_ids)["people"]
    })

    slate, _ = build_slate(date(2026, 5, 6), client=client)
    # Home batters face the AWAY starter — Some Righty (RHP) here. Switch
    # hitter (id ends in 5) facing RHP bats LEFT.
    home_switch = [e for e in slate
                   if e.team == "NYY" and str(e.batter_id).endswith("5")]
    assert len(home_switch) > 0
    assert all(e.batter_hand == "L" for e in home_switch)
    # Away batters face the HOME starter — Some Lefty (LHP). Switch hitter
    # facing LHP bats RIGHT.
    away_switch = [e for e in slate
                   if e.team == "BOS" and str(e.batter_id).endswith("5")]
    assert len(away_switch) > 0
    assert all(e.batter_hand == "R" for e in away_switch)
