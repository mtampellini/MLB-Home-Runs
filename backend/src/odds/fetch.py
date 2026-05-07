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

# Two markets, fetched together in one API call (saves request quota):
#   - MAIN: yes/no (over/under at the implicit 0.5 line). Both sides quoted →
#     used for de-vigging fair probability.
#   - ALT:  alternate lines (0.5, 1.5, 2.5+). Over-only at point=0.5 →
#     used as the bet price.
MARKET_MAIN = "batter_home_runs"
MARKET_ALT = "batter_home_runs_alternate"
MARKETS_REQUEST = f"{MARKET_MAIN},{MARKET_ALT}"

DEFAULT_BOOKS = ("fanduel", "draftkings")
DEFAULT_REGIONS = "us"
TARGET_POINT = 0.5      # the alt-market line we bet
TARGET_OVER = "Over"
TARGET_UNDER = "Under"


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
    """All HR-related odds for one (event, book, batter) combination.

    Fields fall into two groups:
      - **bet_over_american**: the price we'd take if we bet — the alt
        market's point=0.5 Over price.
      - **main_over_american / main_under_american**: the main yes/no HR
        market's both sides — used ONLY for de-vigging to a fair
        probability. We never bet the main market; the alt has the line
        we want.

    Any of these can be None if the book didn't quote that side. Caller
    skips picks where required fields are missing rather than
    single-sided estimating.
    """
    event_id: str
    home_team: str
    away_team: str
    commence_time: datetime
    book: str
    batter_name: str
    bet_over_american: Optional[int]      # alt @ 0.5 Over → bet price
    main_over_american: Optional[int]     # main HR market Over → for de-vig
    main_under_american: Optional[int]    # main HR market Under → for de-vig
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
    markets: str
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
        markets: str = MARKETS_REQUEST,
    ) -> dict:
        """Pull both HR markets in one API call.

        NOTE on credit budget: The Odds API charges 1 credit *per market per
        event call*. Two markets means 2 credits per event. Caller should
        filter events to those actually in today's slate (see
        fetch_today_hr_props) to keep usage under the free-tier 500/month cap.
        """
        params = {
            "markets": markets,
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


def _batter_name(outcome: dict) -> Optional[str]:
    return outcome.get("description") or outcome.get("name_player") or outcome.get("participant")


def parse_event_response(payload: dict) -> list[HRPropQuote]:
    """Extract two markets per (book, batter):
       - main HR market (yes/no — both Over+Under) → de-vig source
       - alt HR market @ point=0.5 Over → bet price

    Returns one HRPropQuote per (book, batter) with all three fields populated
    where the book quoted them. Caller decides how to handle missing pieces.
    """
    event_id = payload.get("id", "")
    home = payload.get("home_team", "")
    away = payload.get("away_team", "")
    commence = _parse_iso(payload["commence_time"]) if "commence_time" in payload else datetime.now().astimezone()

    out: list[HRPropQuote] = []
    for bk in payload.get("bookmakers", []) or []:
        book_key = bk.get("key", "")
        last_update = _parse_iso(bk["last_update"]) if "last_update" in bk else commence

        # Per-batter slots populated below from the two markets.
        bet_over: dict[str, int] = {}        # alt @ 0.5 Over
        main_over: dict[str, int] = {}       # main HR Over
        main_under: dict[str, int] = {}      # main HR Under
        all_batters: set[str] = set()

        for market in bk.get("markets", []) or []:
            mkey = market.get("key")
            for o in market.get("outcomes", []) or []:
                name = _batter_name(o)
                if not name:
                    continue
                side = o.get("name")
                price = o.get("price")
                if price is None:
                    continue

                if mkey == MARKET_MAIN:
                    # Main yes/no market. Some API responses include point=0.5,
                    # others omit it. Accept any (or no) point — this is the
                    # canonical "did the batter homer" line.
                    if side == TARGET_OVER:
                        main_over[name] = int(price)
                        all_batters.add(name)
                    elif side == TARGET_UNDER:
                        main_under[name] = int(price)
                        all_batters.add(name)

                elif mkey == MARKET_ALT:
                    # Alternate market — only the point=0.5 Over interests us.
                    if o.get("point") != TARGET_POINT:
                        continue
                    if side == TARGET_OVER:
                        bet_over[name] = int(price)
                        all_batters.add(name)

        for batter in sorted(all_batters):
            out.append(HRPropQuote(
                event_id=event_id, home_team=home, away_team=away,
                commence_time=commence, book=book_key, batter_name=batter,
                bet_over_american=bet_over.get(batter),
                main_over_american=main_over.get(batter),
                main_under_american=main_under.get(batter),
                last_update=last_update,
            ))
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def fetch_today_hr_props(
    client: Optional[OddsAPIClient] = None,
    books: Iterable[str] = DEFAULT_BOOKS,
    relevant_team_pairs: Optional[set[tuple[str, str]]] = None,
    skip_started_clock_skew_min: Optional[int] = 5,
) -> FetchResult:
    """List today's events, pull both HR markets per event, return assembled result.

    `relevant_team_pairs`: optional set of (home_team_name, away_team_name) tuples
    from the slate. When provided, events whose teams aren't in the set are
    skipped — saves API credits (each event call costs 2 credits with 2 markets).

    `skip_started_clock_skew_min`: events whose `commence_time` is more than
    this many minutes in the past are skipped. Defense in depth — the slate
    filter already excludes Live games via MLB Stats, but the Odds API may
    still list events for which we shouldn't pull props. ``None`` disables the
    filter entirely (used by tests with static fixtures whose dates would
    otherwise go stale every day).
    """
    from datetime import timedelta as _td
    if client is None:
        client = OddsAPIClient()

    fetched_at = datetime.now().astimezone()
    events = client.list_events()

    # Defense-in-depth pre-game filter on commence_time. The slate already
    # filtered by MLB Stats abstractGameState; this catches any drift.
    if skip_started_clock_skew_min is None:
        events_pregame = list(events)
    else:
        started_cutoff = fetched_at - _td(minutes=skip_started_clock_skew_min)
        before_time = len(events)
        events_pregame = [e for e in events if e.commence_time > started_cutoff]
        if before_time != len(events_pregame):
            logger.info(
                "Odds: dropped %d events whose commence_time was in the past",
                before_time - len(events_pregame),
            )

    if relevant_team_pairs is not None:
        before = len(events_pregame)
        events_filtered = [
            e for e in events_pregame
            if (e.home_team, e.away_team) in relevant_team_pairs
        ]
        logger.info(
            "Odds: filtered events by slate teams: %d → %d (saves %d × 2 credits)",
            before, len(events_filtered), before - len(events_filtered),
        )
        events_to_query = events_filtered
    else:
        events_to_query = events_pregame

    quotes: list[HRPropQuote] = []
    raw: list[dict] = []
    errors: list[dict] = []

    for ev in events_to_query:
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
        markets=MARKETS_REQUEST,
        raw_event_responses=raw,
        errors=errors,
    )
