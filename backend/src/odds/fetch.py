"""The Odds API client for batter HR alternate lines.

Workflow:
    1. List today's MLB events (cheap: one request).
    2. For each event, fetch player-prop odds filtered to:
       - market = batter_home_runs_alternate
       - bookmakers = fanduel,draftkings
       - regions = us
    3. Parse to HRPropQuote rows: one row per (event, batter, book), keeping
       both Over and Under prices for the 0.5 line so EV de-vig works.

Rate budget: each event costs ~1 request for player props (plus 1 for the
list). x-requests-remaining and x-requests-used response headers are tracked
on every call and surfaced in the snapshot metadata.

The HTTP session is injectable so tests can stub the network without touching
The Odds API.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "baseball_mlb"
MARKET_KEY = "batter_home_runs_alternate"
DEFAULT_BOOKS = ("fanduel", "draftkings")
DEFAULT_REGIONS = "us"
TARGET_POINT = 0.5      # we only bet the 0.5 alternate line
TARGET_OVER = "Over"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class OddsAPIError(RuntimeError):
    """Raised on any non-2xx response from The Odds API."""


class RateLimitedError(OddsAPIError):
    """422 / 429 / quota-exhausted variant."""


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Event:
    event_id: str
    sport_key: str
    commence_time: datetime
    home_team: str
    away_team: str


@dataclass(frozen=True)
class HRPropQuote:
    """One book's Over+Under quote on a single batter's 0.5 HR line.

    `under_american=None` means the book didn't quote the Under (rare but
    possible — downstream EV code falls back to single-side handling).
    """
    event_id: str
    home_team: str
    away_team: str
    commence_time: datetime
    book: str
    batter_name: str
    point: float
    over_american: int
    under_american: Optional[int]
    last_update: datetime


@dataclass
class FetchResult:
    """One pull's worth of quotes plus API budget metadata for the snapshot log."""
    fetched_at: datetime
    quotes: list[HRPropQuote]
    events: list[Event]
    requests_remaining: Optional[int]
    requests_used: Optional[int]
    books: tuple[str, ...]
    market: str
    raw_event_responses: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class OddsAPIClient:
    """Thin wrapper. Caller injects requests.Session for testability."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        base_url: str = ODDS_API_BASE,
        timeout: int = 15,
    ) -> None:
        self.api_key = api_key or os.environ.get("ODDS_API_KEY")
        if not self.api_key:
            raise OddsAPIError(
                "ODDS_API_KEY not provided (env var or constructor arg)."
            )
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.last_requests_remaining: Optional[int] = None
        self.last_requests_used: Optional[int] = None

    # -------- HTTP plumbing --------------------------------------------------

    def _get(self, path: str, params: dict) -> requests.Response:
        params = {**params, "apiKey": self.api_key}
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        self._track_budget(resp)
        if resp.status_code == 429 or resp.status_code == 422:
            raise RateLimitedError(
                f"{resp.status_code} from {path}: {resp.text[:200]}"
            )
        if not resp.ok:
            raise OddsAPIError(
                f"{resp.status_code} from {path}: {resp.text[:200]}"
            )
        return resp

    def _track_budget(self, resp: requests.Response) -> None:
        rem = resp.headers.get("x-requests-remaining")
        used = resp.headers.get("x-requests-used")
        if rem is not None:
            try:
                self.last_requests_remaining = int(rem)
            except ValueError:
                pass
        if used is not None:
            try:
                self.last_requests_used = int(used)
            except ValueError:
                pass
        if rem is not None:
            logger.info(
                "Odds API budget: remaining=%s used=%s",
                self.last_requests_remaining, self.last_requests_used,
            )

    # -------- High-level reads ----------------------------------------------

    def list_events(self) -> list[Event]:
        resp = self._get(f"/sports/{SPORT_KEY}/events", params={})
        return [_parse_event(e) for e in resp.json()]

    def fetch_event_props(
        self,
        event_id: str,
        books: Iterable[str] = DEFAULT_BOOKS,
        regions: str = DEFAULT_REGIONS,
    ) -> dict:
        params = {
            "markets": MARKET_KEY,
            "bookmakers": ",".join(books),
            "regions": regions,
            "oddsFormat": "american",
        }
        resp = self._get(f"/sports/{SPORT_KEY}/events/{event_id}/odds", params=params)
        return resp.json()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_event(payload: dict) -> Event:
    return Event(
        event_id=payload["id"],
        sport_key=payload["sport_key"],
        commence_time=_parse_iso(payload["commence_time"]),
        home_team=payload["home_team"],
        away_team=payload["away_team"],
    )


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def parse_event_response(payload: dict) -> list[HRPropQuote]:
    """Extract Over+Under @ point=0.5 quotes for each (book, batter)."""
    event_id = payload.get("id", "")
    home = payload.get("home_team", "")
    away = payload.get("away_team", "")
    commence = _parse_iso(payload["commence_time"]) if "commence_time" in payload else datetime.now().astimezone()

    out: list[HRPropQuote] = []
    for bk in payload.get("bookmakers", []) or []:
        book_key = bk.get("key", "")
        last_update = _parse_iso(bk["last_update"]) if "last_update" in bk else commence
        # Collect Over and Under prices keyed by batter name.
        over_by_name: dict[str, int] = {}
        under_by_name: dict[str, int] = {}
        for market in bk.get("markets", []) or []:
            if market.get("key") != MARKET_KEY:
                continue
            for o in market.get("outcomes", []) or []:
                if o.get("point") != TARGET_POINT:
                    continue
                name = o.get("description") or o.get("name_player") or o.get("participant")
                if not name:
                    continue
                side = o.get("name")
                price = o.get("price")
                if price is None:
                    continue
                if side == TARGET_OVER:
                    over_by_name[name] = int(price)
                elif side == "Under":
                    under_by_name[name] = int(price)

        for batter, over_price in over_by_name.items():
            out.append(HRPropQuote(
                event_id=event_id, home_team=home, away_team=away,
                commence_time=commence, book=book_key, batter_name=batter,
                point=TARGET_POINT, over_american=over_price,
                under_american=under_by_name.get(batter),
                last_update=last_update,
            ))
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def fetch_today_hr_props(
    client: Optional[OddsAPIClient] = None,
    books: Iterable[str] = DEFAULT_BOOKS,
) -> FetchResult:
    """List today's events, fetch HR alternates for each, return assembled result."""
    if client is None:
        client = OddsAPIClient()

    fetched_at = datetime.now().astimezone()
    events = client.list_events()
    quotes: list[HRPropQuote] = []
    raw: list[dict] = []
    errors: list[dict] = []

    for ev in events:
        try:
            payload = client.fetch_event_props(ev.event_id, books=books)
        except OddsAPIError as e:
            errors.append({"event_id": ev.event_id, "error": str(e)})
            continue
        raw.append(payload)
        quotes.extend(parse_event_response(payload))

    return FetchResult(
        fetched_at=fetched_at,
        quotes=quotes,
        events=events,
        requests_remaining=client.last_requests_remaining,
        requests_used=client.last_requests_used,
        books=tuple(books),
        market=MARKET_KEY,
        raw_event_responses=raw,
        errors=errors,
    )
