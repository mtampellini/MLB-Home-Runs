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


class QuotaExhaustedError(RateLimitedError):
    """401 + error_code=OUT_OF_USAGE_CREDITS — the monthly request budget
    on the active API key is gone. Distinct from a transient 429 so the
    client can rotate to a fallback key instead of retrying the same one."""


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
# Key resolution + quota-error detection helpers
# ---------------------------------------------------------------------------

# Scan up to this many indexed env vars: ODDS_API_KEY, ODDS_API_KEY_2, _3, ...
# Five is enough headroom for hand-managed free-tier rotation; raise if needed.
_MAX_ENV_KEYS = 5


def _resolve_api_keys(api_key: Optional[str | list[str]]) -> list[str]:
    """Build the ordered key list for OddsAPIClient.

    Inputs accepted, in priority order:
      1. ``api_key`` constructor arg: ``str`` (comma-separated OK) or ``list[str]``.
      2. Env vars ``ODDS_API_KEY``, ``ODDS_API_KEY_2``, ..., ``ODDS_API_KEY_{N}``.
         A comma-separated ``ODDS_API_KEY`` is also accepted (split here).

    Empties/duplicates removed while preserving order — secrets that aren't
    configured in CI come through as empty strings."""
    if api_key is not None:
        raw = api_key if isinstance(api_key, list) else [api_key]
    else:
        raw = [os.environ.get("ODDS_API_KEY", "")]
        for i in range(2, _MAX_ENV_KEYS + 1):
            raw.append(os.environ.get(f"ODDS_API_KEY_{i}", ""))

    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not item:
            continue
        for tok in item.split(","):
            tok = tok.strip()
            if tok and tok not in seen:
                out.append(tok)
                seen.add(tok)
    return out


def _is_out_of_credits(resp: requests.Response) -> bool:
    """Return True iff the body indicates monthly quota exhaustion.

    The Odds API returns 401 with JSON body containing
    ``"error_code": "OUT_OF_USAGE_CREDITS"`` when the key's monthly budget
    is gone. Distinguishing this from other 401s (e.g. invalid key) matters —
    we only rotate on quota exhaustion, not on auth failures that would
    affect every key equally."""
    try:
        body = resp.json()
        if isinstance(body, dict) and body.get("error_code") == "OUT_OF_USAGE_CREDITS":
            return True
    except (ValueError, AttributeError):
        pass
    # Fallback: text match in case the JSON parse fails for any reason.
    return "OUT_OF_USAGE_CREDITS" in (resp.text or "")


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class OddsAPIClient:
    """Thin wrapper. Caller injects requests.Session for testability."""

    def __init__(
        self,
        api_key: Optional[str | list[str]] = None,
        session: Optional[requests.Session] = None,
        base_url: str = ODDS_API_BASE,
        timeout: int = 15,
    ) -> None:
        self._api_keys: list[str] = _resolve_api_keys(api_key)
        if not self._api_keys:
            raise OddsAPIError(
                "ODDS_API_KEY not provided (env var or constructor arg)."
            )
        self._key_idx: int = 0
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.last_requests_remaining: Optional[int] = None
        self.last_requests_used: Optional[int] = None

    @property
    def api_key(self) -> str:
        """Currently active API key. Rotates on QuotaExhaustedError."""
        return self._api_keys[self._key_idx]

    @property
    def active_key_index(self) -> int:
        return self._key_idx

    @property
    def num_keys(self) -> int:
        return len(self._api_keys)

    # -------- HTTP plumbing --------------------------------------------------

    def _get(self, path: str, params: dict) -> requests.Response:
        """GET with automatic key rotation on quota exhaustion.

        Each call tries the active key first. If it returns
        401 OUT_OF_USAGE_CREDITS, advance to the next key and retry the same
        request. Stops when a non-quota response arrives or all keys are
        exhausted. 429/422 (transient rate limit) does NOT trigger rotation —
        it propagates as before."""
        last_err: Optional[QuotaExhaustedError] = None
        while True:
            try:
                return self._get_once(path, params)
            except QuotaExhaustedError as e:
                last_err = e
                if self._key_idx + 1 >= len(self._api_keys):
                    logger.error(
                        "Odds API: all %d key(s) exhausted at %s",
                        len(self._api_keys), path,
                    )
                    raise
                logger.warning(
                    "Odds API: key index %d exhausted (remaining=%s used=%s); "
                    "rotating to key index %d",
                    self._key_idx,
                    self.last_requests_remaining, self.last_requests_used,
                    self._key_idx + 1,
                )
                self._key_idx += 1
                continue

    def _get_once(self, path: str, params: dict) -> requests.Response:
        params = {**params, "apiKey": self.api_key}
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        self._track_budget(resp)
        if resp.status_code == 401 and _is_out_of_credits(resp):
            raise QuotaExhaustedError(
                f"401 OUT_OF_USAGE_CREDITS from {path} on key index "
                f"{self._key_idx}: {resp.text[:200]}"
            )
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
                    # Don't compare `o.get("point") != 0.5` directly: a missing
                    # `point` field returns None and `None != 0.5` is True, so
                    # an API schema change that drops the field would silently
                    # drop every alt outcome. Log loudly instead.
                    point = o.get("point")
                    if point is None:
                        logger.warning(
                            "alt-market outcome missing 'point' field "
                            "(book=%s batter=%s side=%s) — dropping",
                            book_key, name, side,
                        )
                        continue
                    if point != TARGET_POINT:
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
