"""Settle yesterday's picks against MLB Stats API box scores.

Workflow:
    1. Read the day's daily archive at data/daily_archives/YYYY-MM-DD.json
       (the committed source of truth — primary_picks/secondary_picks/shadow_picks).
    2. For each pick, GET /v1/game/{game_pk}/boxscore to find the batter's
       hits + HR for that game.
    3. Mark W (HR >= 1) / L / VOID (game postponed, batter didn't appear, or
       pick is missing game_pk).
    4. Compute realized profit at $1/pick: payout if W, -1 if L, 0 if VOID.
    5. Append a `settlement` block back into the same daily archive file.

The MlbStatsClient is reused (and injectable for tests).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from src.odds.ev import american_payout
from src.pipeline.slate import MlbStatsClient

# Slate dates are in ET (see run_daily.today_et). Default "yesterday" for
# settle must match what run_daily wrote, so derive it in ET too.
_ET = ZoneInfo("America/New_York")

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
DAILY_ARCHIVES_DIR = _DATA_DIR / "daily_archives"

TIERS = ("primary", "secondary", "shadow")


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------

@dataclass
class SettledPick:
    batter: str
    batter_id: int
    team: str
    game_pk: Optional[int]
    over_american: int
    model_prob: float
    market_prob_devig: float
    ev_pct: float
    actual_hr: int
    outcome: str             # 'W' | 'L' | 'VOID'
    profit_units: float      # net profit at $1 stake (positive = profit, -1 = loss)
    void_reason: Optional[str] = None


@dataclass
class SettlementReport:
    as_of_date: _date
    n_picks: int
    n_wins: int
    n_losses: int
    n_voids: int
    units_staked: float
    units_profit: float
    roi_pct: float
    settled: list[SettledPick] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Box-score parsing
# ---------------------------------------------------------------------------

def _hr_count_for_batter(boxscore: dict, batter_id: int) -> Optional[int]:
    """Find the batter in the boxscore and return HR count.

    Returns None if the batter didn't appear (so the pick voids).
    """
    teams = boxscore.get("teams", {}) or {}
    for side in ("home", "away"):
        players = (teams.get(side, {}) or {}).get("players", {}) or {}
        # Keys look like "ID_592450"; we scan by the inner id.
        for _, player in players.items():
            if int(player.get("person", {}).get("id", -1)) == batter_id:
                stats = (player.get("stats", {}) or {}).get("batting", {}) or {}
                if not stats:
                    return None      # batter on roster but didn't bat (DNP)
                return int(stats.get("homeRuns", 0) or 0)
    return None


def _is_game_final(boxscore: dict) -> bool:
    """True iff the game is finished (status code F/Final)."""
    teams = boxscore.get("teams", {}) or {}
    for side in ("home", "away"):
        players = (teams.get(side, {}) or {}).get("players", {}) or {}
        for _, player in players.items():
            if (player.get("stats", {}) or {}).get("batting"):
                return True
    return False


# ---------------------------------------------------------------------------
# Archive I/O
# ---------------------------------------------------------------------------

def _archive_path_for(cutoff_date: _date, archives_dir: Path) -> Path:
    return archives_dir / f"{cutoff_date.isoformat()}.json"


def _read_archive(cutoff_date: _date, archives_dir: Path) -> dict:
    path = _archive_path_for(cutoff_date, archives_dir)
    if not path.exists():
        raise FileNotFoundError(
            f"no daily archive at {path}; "
            "run_daily writes it as part of every run."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_picks_for_date(
    cutoff_date: _date,
    *,
    archives_dir: Path = DAILY_ARCHIVES_DIR,
    tier: str = "primary",
) -> list[dict]:
    """Load one tier's picks from the day's daily archive."""
    archive = _read_archive(cutoff_date, archives_dir)
    return archive.get(f"{tier}_picks", []) or []


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

def settle_pick(pick: dict, boxscore: Optional[dict]) -> SettledPick:
    """Mark one pick as W / L / VOID given its game's boxscore."""
    if pick.get("best_book") == "fanduel" and pick.get("fd_odds") is not None:
        over = int(pick["fd_odds"])
    elif pick.get("best_book") == "draftkings" and pick.get("dk_odds") is not None:
        over = int(pick["dk_odds"])
    else:
        over = int(pick.get("dk_odds") or pick.get("fd_odds") or 0)

    base = dict(
        batter=pick.get("batter", ""),
        batter_id=int(pick.get("batter_id", 0)),
        team=pick.get("team", ""),
        game_pk=pick.get("game_pk"),
        over_american=over,
        model_prob=float(pick.get("model_prob", 0.0)),
        market_prob_devig=float(pick.get("market_prob_devig", 0.0)),
        ev_pct=float(pick.get("ev_pct", 0.0)),
    )

    if pick.get("game_pk") is None:
        return SettledPick(**base, actual_hr=0, outcome="VOID",
                           profit_units=0.0, void_reason="no_game_pk")
    if boxscore is None:
        return SettledPick(**base, actual_hr=0, outcome="VOID",
                           profit_units=0.0, void_reason="boxscore_unavailable")
    if not _is_game_final(boxscore):
        return SettledPick(**base, actual_hr=0, outcome="VOID",
                           profit_units=0.0, void_reason="game_not_final")

    hr = _hr_count_for_batter(boxscore, base["batter_id"])
    if hr is None:
        return SettledPick(**base, actual_hr=0, outcome="VOID",
                           profit_units=0.0, void_reason="batter_did_not_bat")

    if hr >= 1:
        return SettledPick(**base, actual_hr=hr, outcome="W",
                           profit_units=american_payout(over))
    return SettledPick(**base, actual_hr=hr, outcome="L",
                       profit_units=-1.0)


def _settle_picks(
    picks: list[dict],
    *,
    client: MlbStatsClient,
    box_cache: dict[int, Optional[dict]],
) -> SettlementReport:
    settled: list[SettledPick] = []
    for pick in picks:
        gpk = pick.get("game_pk")
        if gpk is None:
            settled.append(settle_pick(pick, None))
            continue
        if gpk not in box_cache:
            try:
                box_cache[gpk] = client._get(f"/game/{gpk}/boxscore")
            except Exception as e:    # noqa: BLE001
                logger.warning("boxscore fetch failed for game_pk=%s: %s: %s",
                               gpk, type(e).__name__, e)
                box_cache[gpk] = None
        settled.append(settle_pick(pick, box_cache[gpk]))

    n_w = sum(1 for s in settled if s.outcome == "W")
    n_l = sum(1 for s in settled if s.outcome == "L")
    n_v = sum(1 for s in settled if s.outcome == "VOID")
    units_staked = float(n_w + n_l)
    units_profit = float(sum(s.profit_units for s in settled))
    roi_pct = (units_profit / units_staked * 100.0) if units_staked > 0 else 0.0
    return SettlementReport(
        as_of_date=_date.today(),  # caller fixes this up if it cares
        n_picks=len(settled),
        n_wins=n_w, n_losses=n_l, n_voids=n_v,
        units_staked=units_staked, units_profit=units_profit, roi_pct=roi_pct,
        settled=settled,
    )


def settle_date(
    cutoff_date: _date,
    *,
    client: Optional[MlbStatsClient] = None,
    archives_dir: Path = DAILY_ARCHIVES_DIR,
    tier: str = "primary",
    box_cache: Optional[dict[int, Optional[dict]]] = None,
) -> SettlementReport:
    """Settle one tier for one date by reading the day's daily archive.

    `box_cache` is shared across tiers so primary + secondary + shadow on the
    same day only hit the MLB Stats API once per game.
    """
    client = client or MlbStatsClient()
    picks = load_picks_for_date(cutoff_date, archives_dir=archives_dir, tier=tier)
    if box_cache is None:
        box_cache = {}
    report = _settle_picks(picks, client=client, box_cache=box_cache)
    report.as_of_date = cutoff_date
    return report


def settle_all_tiers(
    cutoff_date: _date,
    *,
    client: Optional[MlbStatsClient] = None,
    archives_dir: Path = DAILY_ARCHIVES_DIR,
) -> dict[str, SettlementReport]:
    """Settle all tiers for one date, sharing the boxscore cache.

    Reads picks from `data/daily_archives/YYYY-MM-DD.json` and appends a
    `settlement` block back into the same file. The archive is the committed
    source of truth for the tracker dashboard.
    """
    client = client or MlbStatsClient()
    archive_path = _archive_path_for(cutoff_date, archives_dir)
    if not archive_path.exists():
        logger.warning("no archive at %s — nothing to settle", archive_path)
        return {}
    with open(archive_path, "r", encoding="utf-8") as f:
        archive = json.load(f)

    box_cache: dict[int, Optional[dict]] = {}
    out: dict[str, SettlementReport] = {}
    for tier in TIERS:
        picks = archive.get(f"{tier}_picks") or []
        if not picks:
            logger.info("%s tier has no picks for %s; skipping", tier, cutoff_date)
            continue
        report = _settle_picks(picks, client=client, box_cache=box_cache)
        report.as_of_date = cutoff_date
        out[tier] = report

    if not out:
        return out

    settlement_block = {
        "settled_at": datetime.now().astimezone().isoformat(),
    }
    for tier, report in out.items():
        settlement_block[f"{tier}_results"] = [s.__dict__ for s in report.settled]
        settlement_block[f"{tier}_summary"] = {
            "n_picks": report.n_picks,
            "n_wins": report.n_wins,
            "n_losses": report.n_losses,
            "n_voids": report.n_voids,
            "units_staked": report.units_staked,
            "units_profit": round(report.units_profit, 4),
            "roi_pct": round(report.roi_pct, 2),
        }
    archive["settlement"] = settlement_block
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(archive, f, indent=2)
    logger.info("appended settlement to %s", archive_path)

    return out


def main() -> int:
    import argparse
    from datetime import timedelta

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Settle picks against MLB box scores.")
    parser.add_argument(
        "--date", type=str, default=None,
        help="ISO date to settle (default: yesterday).",
    )
    args = parser.parse_args()
    cutoff = (_date.fromisoformat(args.date) if args.date
              else datetime.now(_ET).date() - timedelta(days=1))
    reports = settle_all_tiers(cutoff)
    summary = {tier: {
        "n_picks": r.n_picks,
        "n_wins": r.n_wins, "n_losses": r.n_losses, "n_voids": r.n_voids,
        "roi_pct": r.roi_pct,
        "units_profit": r.units_profit,
    } for tier, r in reports.items()}
    print(json.dumps({"as_of_date": cutoff.isoformat(), "tiers": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
