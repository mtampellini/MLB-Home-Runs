"""Odds API parser + client tests with stubbed HTTP — no network calls.

Mocks requests.Session so we exercise the parsing logic against canned
Odds API responses with BOTH markets:
- batter_home_runs (main yes/no, both sides) → for de-vig
- batter_home_runs_alternate (point=0.5 Over) → for the bet price
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.odds.fetch import (
    DEFAULT_BOOKS,
    HRPropQuote,
    MARKETS_REQUEST,
    OddsAPIClient,
    OddsAPIError,
    RateLimitedError,
    fetch_today_hr_props,
    parse_event_response,
)


# ---------------------------------------------------------------------------
# Sample Odds API payload — both markets per book
# ---------------------------------------------------------------------------

SAMPLE_EVENT_PROPS = {
    "id": "evt_abc",
    "sport_key": "baseball_mlb",
    "commence_time": "2026-05-06T23:05:00Z",
    "home_team": "New York Yankees",
    "away_team": "Boston Red Sox",
    "bookmakers": [
        {
            "key": "fanduel",
            "last_update": "2026-05-06T19:00:00Z",
            "markets": [
                {
                    "key": "batter_home_runs",
                    "outcomes": [
                        # Main yes/no — both sides quoted, used for de-vig.
                        {"name": "Over",  "description": "Aaron Judge", "price": 290},
                        {"name": "Under", "description": "Aaron Judge", "price": -380},
                        {"name": "Over",  "description": "Anthony Volpe", "price": 600},
                        {"name": "Under", "description": "Anthony Volpe", "price": -1200},
                    ],
                },
                {
                    "key": "batter_home_runs_alternate",
                    "outcomes": [
                        # Alt @ 0.5 Over → bet price.
                        {"name": "Over", "description": "Aaron Judge", "point": 0.5, "price": 290},
                        # Higher alternate lines we ignore.
                        {"name": "Over", "description": "Aaron Judge", "point": 1.5, "price": 1100},
                        {"name": "Over", "description": "Anthony Volpe", "point": 0.5, "price": 600},
                    ],
                },
            ],
        },
        {
            "key": "draftkings",
            "last_update": "2026-05-06T19:01:00Z",
            "markets": [
                {
                    "key": "batter_home_runs",
                    "outcomes": [
                        {"name": "Over",  "description": "Aaron Judge", "price": 310},
                        {"name": "Under", "description": "Aaron Judge", "price": -400},
                        {"name": "Over",  "description": "Anthony Volpe", "price": 580},
                        {"name": "Under", "description": "Anthony Volpe", "price": -1100},
                    ],
                },
                {
                    "key": "batter_home_runs_alternate",
                    "outcomes": [
                        {"name": "Over", "description": "Aaron Judge", "point": 0.5, "price": 310},
                        {"name": "Over", "description": "Anthony Volpe", "point": 0.5, "price": 580},
                    ],
                },
            ],
        },
    ],
}

SAMPLE_EVENTS_LIST = [
    {
        "id": "evt_abc", "sport_key": "baseball_mlb",
        "commence_time": "2026-05-06T23:05:00Z",
        "home_team": "New York Yankees", "away_team": "Boston Red Sox",
    },
]


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

def test_parse_event_returns_one_quote_per_book_per_batter():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    # 2 batters × 2 books = 4 quotes (one per (book, batter) regardless of market count)
    assert len(quotes) == 4
    books = sorted({q.book for q in quotes})
    assert books == ["draftkings", "fanduel"]
    batters = sorted({q.batter_name for q in quotes})
    assert batters == ["Aaron Judge", "Anthony Volpe"]


def test_parse_collects_main_market_for_devig():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    judge_dk = next(q for q in quotes
                    if q.batter_name == "Aaron Judge" and q.book == "draftkings")
    assert judge_dk.main_over_american == 310
    assert judge_dk.main_under_american == -400


def test_parse_collects_alt_market_for_bet_price():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    judge_dk = next(q for q in quotes
                    if q.batter_name == "Aaron Judge" and q.book == "draftkings")
    assert judge_dk.bet_over_american == 310           # alt @ 0.5 Over


def test_parse_filters_alt_market_to_point_0_5():
    """Higher alternate lines (1.5, 2.5...) shouldn't pollute bet_over_american."""
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    judge_fd = next(q for q in quotes
                    if q.batter_name == "Aaron Judge" and q.book == "fanduel")
    assert judge_fd.bet_over_american == 290           # 0.5 line, NOT the 1.5's +1100


def test_parse_handles_batter_in_only_one_market():
    """Add a batter to the alt market only — main fields should be None."""
    payload = {**SAMPLE_EVENT_PROPS}
    # Inject a batter that exists in alt but not in main on FanDuel.
    fd = payload["bookmakers"][0]
    fd_markets = [dict(m) for m in fd["markets"]]
    fd_markets[1] = dict(fd_markets[1])
    fd_markets[1]["outcomes"] = list(fd_markets[1]["outcomes"]) + [
        {"name": "Over", "description": "Cedric Mullins", "point": 0.5, "price": 800},
    ]
    payload["bookmakers"] = [{**fd, "markets": fd_markets}, payload["bookmakers"][1]]
    quotes = parse_event_response(payload)
    mullins = next((q for q in quotes
                     if q.batter_name == "Cedric Mullins" and q.book == "fanduel"), None)
    assert mullins is not None
    assert mullins.bet_over_american == 800
    assert mullins.main_over_american is None
    assert mullins.main_under_american is None


def test_parse_attaches_event_metadata():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    q = quotes[0]
    assert q.event_id == "evt_abc"
    assert q.home_team == "New York Yankees"
    assert q.away_team == "Boston Red Sox"
    assert q.commence_time == datetime.fromisoformat("2026-05-06T23:05:00+00:00")


def test_parse_empty_event_returns_empty_list():
    assert parse_event_response({"id": "x", "commence_time": "2026-05-06T23:05:00Z",
                                 "bookmakers": []}) == []


# ---------------------------------------------------------------------------
# Client tests with mocked Session
# ---------------------------------------------------------------------------

def _mock_response(status: int, json_body, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.ok = 200 <= status < 300
    resp.headers = headers or {}
    resp.json.return_value = json_body
    resp.text = str(json_body)
    return resp


def test_client_requests_both_markets_in_one_call():
    """Verify the markets= param sends the comma-joined two-market value."""
    session = MagicMock()
    session.get.return_value = _mock_response(200, SAMPLE_EVENT_PROPS,
                                              headers={"x-requests-remaining": "498"})
    client = OddsAPIClient(api_key="test", session=session)
    client.fetch_event_props("evt_abc")
    call_kwargs = session.get.call_args
    params = call_kwargs.kwargs.get("params") or call_kwargs.args[1]
    assert params["markets"] == MARKETS_REQUEST
    assert "batter_home_runs" in params["markets"]
    assert "batter_home_runs_alternate" in params["markets"]


def test_client_tracks_requests_remaining_header():
    session = MagicMock()
    session.get.return_value = _mock_response(
        200, SAMPLE_EVENTS_LIST,
        headers={"x-requests-remaining": "19483", "x-requests-used": "517"},
    )
    client = OddsAPIClient(api_key="test", session=session)
    client.list_events()
    assert client.last_requests_remaining == 19483
    assert client.last_requests_used == 517


def test_client_raises_rate_limit_on_429():
    session = MagicMock()
    session.get.return_value = _mock_response(429, {"message": "Too many"})
    client = OddsAPIClient(api_key="test", session=session)
    with pytest.raises(RateLimitedError):
        client.list_events()


def test_client_raises_generic_error_on_500():
    session = MagicMock()
    session.get.return_value = _mock_response(500, {"message": "boom"})
    client = OddsAPIClient(api_key="test", session=session)
    with pytest.raises(OddsAPIError):
        client.list_events()


def test_client_requires_api_key(monkeypatch):
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    with pytest.raises(OddsAPIError):
        OddsAPIClient()


def test_fetch_today_hr_props_full_flow():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(
            200, SAMPLE_EVENTS_LIST,
            headers={"x-requests-remaining": "19000", "x-requests-used": "1000"},
        ),
        _mock_response(
            200, SAMPLE_EVENT_PROPS,
            headers={"x-requests-remaining": "18998", "x-requests-used": "1002"},
        ),
    ]
    client = OddsAPIClient(api_key="test", session=session)
    # skip_started_clock_skew_min=None disables the past-event filter so the
    # static 2026-05-06 fixture doesn't stop being "today" tomorrow.
    result = fetch_today_hr_props(client=client, skip_started_clock_skew_min=None)

    assert len(result.events) == 1
    assert len(result.quotes) == 4
    assert result.requests_remaining == 18998
    assert result.requests_used == 1002
    assert result.markets == MARKETS_REQUEST
    assert result.books == DEFAULT_BOOKS


def test_fetch_today_hr_props_filters_events_by_team_pairs():
    """Only events whose (home, away) match the slate should be queried."""
    session = MagicMock()
    # Two events; only one is in the slate.
    events_payload = [
        SAMPLE_EVENTS_LIST[0],
        {"id": "evt_xyz", "sport_key": "baseball_mlb",
         "commence_time": "2026-05-06T20:00:00Z",
         "home_team": "Cincinnati Reds", "away_team": "Pittsburgh Pirates"},
    ]
    session.get.side_effect = [
        _mock_response(200, events_payload, headers={"x-requests-remaining": "100"}),
        _mock_response(200, SAMPLE_EVENT_PROPS, headers={"x-requests-remaining": "98"}),
    ]
    client = OddsAPIClient(api_key="test", session=session)
    relevant = {("New York Yankees", "Boston Red Sox")}
    result = fetch_today_hr_props(client=client, relevant_team_pairs=relevant,
                                   skip_started_clock_skew_min=None)
    # Only one detail call made (for the matching event).
    assert session.get.call_count == 2     # 1 list + 1 detail (NOT 2 details)
    assert len(result.quotes) == 4         # only from the matching event


def test_fetch_records_per_event_errors_without_failing():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(
            200, SAMPLE_EVENTS_LIST,
            headers={"x-requests-remaining": "19000"},
        ),
        _mock_response(429, {"message": "Too many"}),  # fail one event
    ]
    client = OddsAPIClient(api_key="test", session=session)
    result = fetch_today_hr_props(client=client, skip_started_clock_skew_min=None)

    assert len(result.events) == 1
    assert len(result.quotes) == 0
    assert len(result.errors) == 1
    assert result.errors[0]["event_id"] == "evt_abc"


def test_fetch_today_hr_props_pregame_filter_drops_past_events():
    """Pre-game filter test (covers the behavior the three tests above bypass).
    Builds events relative to NOW so the assertion stays valid every day."""
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    now = _dt.now(_tz.utc)
    past_iso   = (now - _td(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    future_iso = (now + _td(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ")

    events_payload = [
        {"id": "evt_past",   "sport_key": "baseball_mlb",
         "commence_time": past_iso,
         "home_team": "Houston Astros", "away_team": "Texas Rangers"},
        {"id": "evt_future", "sport_key": "baseball_mlb",
         "commence_time": future_iso,
         "home_team": "New York Yankees", "away_team": "Boston Red Sox"},
    ]
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(200, events_payload, headers={"x-requests-remaining": "100"}),
        # Only the future event should trigger a detail call.
        _mock_response(200, SAMPLE_EVENT_PROPS, headers={"x-requests-remaining": "98"}),
    ]
    client = OddsAPIClient(api_key="test", session=session)
    result = fetch_today_hr_props(client=client)   # default skip_started=5min
    assert session.get.call_count == 2             # 1 list + 1 detail (past dropped)
    assert len(result.quotes) == 4
