"""Odds API parser + client tests with stubbed HTTP — no network calls.

Mocks requests.Session so we exercise the parsing logic against canned
Odds API responses, including:
- multiple books (FD + DK)
- multiple alternate lines per batter (we only keep 0.5)
- missing Under price
- rate-limit response
- x-requests-remaining tracking
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.odds.fetch import (
    DEFAULT_BOOKS,
    HRPropQuote,
    OddsAPIClient,
    OddsAPIError,
    RateLimitedError,
    fetch_today_hr_props,
    parse_event_response,
)


# ---------------------------------------------------------------------------
# Sample Odds API payloads
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
            "markets": [{
                "key": "batter_home_runs_alternate",
                "outcomes": [
                    {"name": "Over",  "description": "Aaron Judge", "point": 0.5, "price": 290},
                    {"name": "Under", "description": "Aaron Judge", "point": 0.5, "price": -380},
                    {"name": "Over",  "description": "Aaron Judge", "point": 1.5, "price": 1100},
                    {"name": "Over",  "description": "Anthony Volpe", "point": 0.5, "price": 600},
                    # Volpe's Under intentionally missing — we should still record him.
                ],
            }],
        },
        {
            "key": "draftkings",
            "last_update": "2026-05-06T19:01:00Z",
            "markets": [{
                "key": "batter_home_runs_alternate",
                "outcomes": [
                    {"name": "Over",  "description": "Aaron Judge", "point": 0.5, "price": 310},
                    {"name": "Under", "description": "Aaron Judge", "point": 0.5, "price": -400},
                    {"name": "Over",  "description": "Anthony Volpe", "point": 0.5, "price": 580},
                    {"name": "Under", "description": "Anthony Volpe", "point": 0.5, "price": -800},
                ],
            }],
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
    # 2 batters × 2 books = 4 quotes (only point=0.5 Overs are kept)
    assert len(quotes) == 4
    books = sorted({q.book for q in quotes})
    assert books == ["draftkings", "fanduel"]
    batters = sorted({q.batter_name for q in quotes})
    assert batters == ["Aaron Judge", "Anthony Volpe"]


def test_parse_filters_alternate_lines_other_than_0_5():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    assert all(q.point == 0.5 for q in quotes)


def test_parse_keeps_under_price_when_present():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    judge_dk = next(q for q in quotes if q.batter_name == "Aaron Judge" and q.book == "draftkings")
    assert judge_dk.over_american == 310
    assert judge_dk.under_american == -400


def test_parse_handles_missing_under_price():
    quotes = parse_event_response(SAMPLE_EVENT_PROPS)
    volpe_fd = next(q for q in quotes if q.batter_name == "Anthony Volpe" and q.book == "fanduel")
    assert volpe_fd.over_american == 600
    assert volpe_fd.under_american is None  # FD didn't quote Under for Volpe


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
            headers={"x-requests-remaining": "18999", "x-requests-used": "1001"},
        ),
    ]
    client = OddsAPIClient(api_key="test", session=session)
    result = fetch_today_hr_props(client=client)

    assert len(result.events) == 1
    assert len(result.quotes) == 4
    assert result.requests_remaining == 18999
    assert result.requests_used == 1001
    assert result.market == "batter_home_runs_alternate"
    assert result.books == DEFAULT_BOOKS


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
    result = fetch_today_hr_props(client=client)

    # One event listed, but the props call failed → no quotes, error logged.
    assert len(result.events) == 1
    assert len(result.quotes) == 0
    assert len(result.errors) == 1
    assert result.errors[0]["event_id"] == "evt_abc"
