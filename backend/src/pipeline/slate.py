"""Today's slate from the MLB Stats API.

Workflow:
  1. Fetch /v1/schedule for the cutoff date with hydrates for probable pitcher,
     posted lineups, team, and venue.
  2. For each game with a posted home + away lineup, build SlateEntry rows
     (one per batter facing the opposing probable starter).
  3. Hydrate batter handedness via /v1/people/{ids} (batched).
  4. Resolve switch-hitters' effective bat side based on the projected starter.

If a game has no posted lineup yet, it is skipped with a clear log message.
The pipeline can be re-run later to pick up late-posting lineups.

The HTTP session is injectable so tests can stub the network without touching
the live MLB Stats API.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Optional

import requests

from src.pipeline._teams import TEAM_CODE_BY_MLBAM_ID
from src.model.predict import SlateEntry

logger = logging.getLogger(__name__)


MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"


# ---------------------------------------------------------------------------
# Name normalization (used to match odds-API batter names back to slate IDs)
# ---------------------------------------------------------------------------

_SUFFIXES = re.compile(r"\b(jr\.?|sr\.?|ii|iii|iv)\b", flags=re.IGNORECASE)


def normalize_name(name: str) -> str:
    """Lowercase + strip accents/suffixes/punctuation. For odds-name matching.

    "José Ramírez Jr." → "jose ramirez"
    "L.J. Cron"         → "lj cron"
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = _SUFFIXES.sub("", s)
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return " ".join(s.split())


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class MlbStatsError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class MlbStatsClient:
    def __init__(
        self,
        session: Optional[requests.Session] = None,
        base_url: str = MLB_STATS_BASE,
        timeout: int = 15,
    ) -> None:
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        resp = self.session.get(
            f"{self.base_url}{path}", params=params or {}, timeout=self.timeout
        )
        if not resp.ok:
            raise MlbStatsError(
                f"{resp.status_code} from {path}: {resp.text[:200]}"
            )
        return resp.json()

    def schedule_for_date(self, d: date) -> dict:
        """Schedule for one date with probable pitchers + posted lineups."""
        return self._get(
            "/schedule",
            params={
                "sportId": 1,
                "date": d.isoformat(),
                # `person` (top-level, NOT nested as probablePitcher(person))
                # is what populates each probable pitcher's pitchHand.code.
                # Without it the pitchHand field is missing for every pitcher
                # in the response — which used to silently coerce them to "R"
                # at line ~340 below, breaking platoon-split logic for every
                # left-handed pitcher in the slate.
                "hydrate": "probablePitcher,lineups,team,venue,person",
            },
        )

    def fetch_people(self, person_ids: Iterable[int]) -> dict[int, dict]:
        """Batch-fetch /v1/people for handedness, etc. Returns {id: payload}."""
        ids = list(dict.fromkeys(person_ids))   # de-dup, preserve order
        if not ids:
            return {}
        out: dict[int, dict] = {}
        # MLB Stats supports comma-joined ids on /people; chunk to avoid URL bloat.
        chunk_size = 50
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i + chunk_size]
            payload = self._get(
                "/people",
                params={"personIds": ",".join(str(x) for x in chunk)},
            )
            for p in payload.get("people", []) or []:
                out[int(p["id"])] = p
        return out


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GameInfo:
    game_pk: int
    game_datetime: datetime
    home_team_id: int
    home_team_name: str
    home_team_code: str
    away_team_id: int
    away_team_name: str
    away_team_code: str
    venue_name: str
    park_code: Optional[str]
    home_starter_id: Optional[int]
    home_starter_name: Optional[str]
    home_starter_hand: Optional[str]    # 'R' / 'L'
    away_starter_id: Optional[int]
    away_starter_name: Optional[str]
    away_starter_hand: Optional[str]
    home_lineup_ids: list[int]          # ordered 1..9
    away_lineup_ids: list[int]
    abstract_game_state: str = ""       # 'Preview' / 'Live' / 'Final'
    detailed_state: str = ""            # 'Scheduled' / 'In Progress' / 'Final' / 'Postponed' / etc


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _team_code_from_payload(team: dict) -> Optional[str]:
    """Resolve our internal park/team code from the API team payload."""
    team_id = team.get("id")
    if team_id and team_id in TEAM_CODE_BY_MLBAM_ID:
        return TEAM_CODE_BY_MLBAM_ID[team_id]
    abbr = team.get("abbreviation") or ""
    return abbr.upper() if abbr else None


def parse_schedule(schedule_payload: dict) -> list[GameInfo]:
    games: list[GameInfo] = []
    for date_block in schedule_payload.get("dates", []) or []:
        for g in date_block.get("games", []) or []:
            teams = g.get("teams", {}) or {}
            home = teams.get("home", {}) or {}
            away = teams.get("away", {}) or {}
            home_team = home.get("team", {}) or {}
            away_team = away.get("team", {}) or {}
            home_pp = home.get("probablePitcher") or {}
            away_pp = away.get("probablePitcher") or {}

            lineups = g.get("lineups", {}) or {}
            home_lineup = [int(p["id"]) for p in (lineups.get("homePlayers") or [])]
            away_lineup = [int(p["id"]) for p in (lineups.get("awayPlayers") or [])]

            venue = g.get("venue", {}) or {}
            home_code = _team_code_from_payload(home_team)
            away_code = _team_code_from_payload(away_team)

            status = g.get("status", {}) or {}

            games.append(GameInfo(
                game_pk=int(g["gamePk"]),
                game_datetime=_parse_iso(g["gameDate"]),
                home_team_id=int(home_team.get("id", 0)),
                home_team_name=home_team.get("name", ""),
                home_team_code=home_code or "",
                away_team_id=int(away_team.get("id", 0)),
                away_team_name=away_team.get("name", ""),
                away_team_code=away_code or "",
                venue_name=venue.get("name", ""),
                park_code=home_code,    # park = home team's park code
                home_starter_id=int(home_pp["id"]) if home_pp else None,
                home_starter_name=home_pp.get("fullName") if home_pp else None,
                home_starter_hand=(home_pp.get("pitchHand") or {}).get("code") if home_pp else None,
                away_starter_id=int(away_pp["id"]) if away_pp else None,
                away_starter_name=away_pp.get("fullName") if away_pp else None,
                away_starter_hand=(away_pp.get("pitchHand") or {}).get("code") if away_pp else None,
                home_lineup_ids=home_lineup,
                away_lineup_ids=away_lineup,
                abstract_game_state=str(status.get("abstractGameState", "")),
                detailed_state=str(status.get("detailedState", "")),
            ))
    return games


# Set of MLB Stats API detailed_state values that disqualify a game from
# being part of today's pre-game slate. abstractGameState=Preview catches
# the live/final cases; this catches the pre-game-but-postponed edge cases.
EXCLUDE_DETAILED_STATES = frozenset({
    "Postponed", "Suspended", "Cancelled", "Canceled", "Forfeit",
})


def is_pregame(g: GameInfo) -> bool:
    """True iff the game is in a state we'd safely bet on.

    abstractGameState='Preview' covers Scheduled / Pre-Game / Warmup / Delayed
    Start. Anything else (Live, Final) is excluded. Postponed/Suspended games
    can be in 'Preview' too (rescheduled), so we explicitly reject those by
    detailed_state.
    """
    if g.abstract_game_state != "Preview":
        return False
    if g.detailed_state in EXCLUDE_DETAILED_STATES:
        return False
    return True


# ---------------------------------------------------------------------------
# Build slate
# ---------------------------------------------------------------------------

def _effective_bat_side(bat_side: str, pitcher_hand: Optional[str]) -> str:
    """Switch hitters bat opposite the pitcher. Default to 'R' if anything is unknown."""
    if bat_side == "S":
        if pitcher_hand == "L":
            return "R"
        if pitcher_hand == "R":
            return "L"
        return "R"
    return bat_side or "R"


def build_slate(
    cutoff_date: date,
    client: Optional[MlbStatsClient] = None,
) -> tuple[list[SlateEntry], dict]:
    """Top-level: schedule → games → batter rows. Returns (slate, metadata).

    metadata includes:
      - games_total: total scheduled
      - games_with_lineups: games that contributed slate entries
      - games_no_lineup_skipped: games skipped (lineups not posted yet)
      - missing_handedness: list of player IDs we couldn't resolve handedness for
    """
    client = client or MlbStatsClient()

    schedule = client.schedule_for_date(cutoff_date)
    games = parse_schedule(schedule)

    # Collect every player id we'll need handedness for.
    needed_ids: list[int] = []
    for g in games:
        needed_ids.extend(g.home_lineup_ids)
        needed_ids.extend(g.away_lineup_ids)
    people = client.fetch_people(needed_ids)

    slate: list[SlateEntry] = []
    games_with_lineups = 0
    games_skipped: list[int] = []
    games_excluded_non_pregame: list[dict] = []  # for funnel transparency
    missing_hand: list[int] = []
    # Track (home_team_name, away_team_name) for the games we DO use, so the
    # Odds API client can filter events to just our slate (saves credits).
    team_pairs_in_slate: list[tuple[str, str]] = []

    for g in games:
        # PRE-GAME FILTER (root-cause fix for live-game pricing artifacts).
        # Books reprice alt HR props as PAs are consumed; if we ran during a
        # live game, batters with 1 PA remaining would look like long-shot
        # values when the model still assumes 4+ PAs ahead.
        if not is_pregame(g):
            games_excluded_non_pregame.append({
                "game_pk": g.game_pk,
                "home": g.home_team_name,
                "away": g.away_team_name,
                "abstract_state": g.abstract_game_state,
                "detailed_state": g.detailed_state,
            })
            logger.info(
                "game %s (%s @ %s): non-pregame (state=%s/%s); excluded",
                g.game_pk, g.away_team_name, g.home_team_name,
                g.abstract_game_state, g.detailed_state,
            )
            continue

        if not g.home_lineup_ids or not g.away_lineup_ids:
            games_skipped.append(g.game_pk)
            logger.info(
                "game %s (%s @ %s): lineups not posted; skipped — re-run later",
                g.game_pk, g.away_team_name, g.home_team_name,
            )
            continue
        if g.home_starter_id is None or g.away_starter_id is None:
            games_skipped.append(g.game_pk)
            logger.info(
                "game %s: probable pitchers missing for one side; skipped", g.game_pk,
            )
            continue
        games_with_lineups += 1
        team_pairs_in_slate.append((g.home_team_name, g.away_team_name))

        # Home batters face away starter, away batters face home starter.
        for ids, team_code, starter_id, starter_name, starter_hand in (
            (g.home_lineup_ids, g.home_team_code,
             g.away_starter_id, g.away_starter_name, g.away_starter_hand),
            (g.away_lineup_ids, g.away_team_code,
             g.home_starter_id, g.home_starter_name, g.home_starter_hand),
        ):
            # If the schedule hydrate is missing pitchHand we don't want to
            # silently coerce every pitcher to R (used to be `starter_hand or "R"`
            # — broke platoon-split logic for every LHP in the slate). Log the
            # gap loudly. Default of "R" is kept only as last-resort defense.
            if starter_id is not None and not starter_hand:
                logger.warning(
                    "slate: pitchHand missing for starter %s (id=%s) — "
                    "platoon split will default to RHP. Check the /schedule "
                    "hydrate string includes `person`.",
                    starter_name or "?", starter_id,
                )
            for spot, batter_id in enumerate(ids[:9], start=1):
                person = people.get(batter_id, {})
                bat_side_raw = (person.get("batSide") or {}).get("code") or ""
                if not bat_side_raw:
                    missing_hand.append(batter_id)
                eff_bat = _effective_bat_side(bat_side_raw, starter_hand)
                slate.append(SlateEntry(
                    batter_id=batter_id,
                    batter_name=person.get("fullName", f"id_{batter_id}"),
                    batter_hand=eff_bat,
                    team=team_code,
                    pitcher_id=starter_id,
                    pitcher_name=starter_name or "",
                    pitcher_hand=starter_hand or "R",
                    park=g.park_code or g.home_team_code,
                    game_datetime=g.game_datetime,
                    lineup_spot=spot,
                    game_pk=g.game_pk,
                ))

    metadata = {
        "games_total": len(games),
        "games_pregame": games_with_lineups + len(games_skipped),
        "games_with_lineups": games_with_lineups,
        "games_no_lineup_skipped": len(games_skipped),
        "games_excluded_live_or_complete": len(games_excluded_non_pregame),
        "excluded_non_pregame_games": games_excluded_non_pregame,
        "skipped_game_pks": games_skipped,
        "missing_handedness": list(dict.fromkeys(missing_hand)),
        "team_pairs": team_pairs_in_slate,
    }
    return slate, metadata
