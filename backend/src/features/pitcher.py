"""Pitcher features (season + 30d), with vs-LHB / vs-RHB platoon splits.

Each call returns a nested dict keyed by split:
    {
        'overall': {...},
        'vs_R':    {...},   # vs right-handed batters
        'vs_L':    {...},   # vs left-handed batters
    }

Splits matter for HR projection: a pitcher's HR/9 vs RHB can differ wildly
from his number vs LHB, and the batter we're projecting will sit on one side
of that split.
"""

from __future__ import annotations

from datetime import date as _date
from typing import Optional

from src.backtest.as_of_context import AsOfContext
from src.features._statcast import compute_pitcher_metrics, fetch_pitcher_pitches


def _pull_metrics(pitches, scope: str, window_start: _date, window_end: _date) -> dict:
    overall = compute_pitcher_metrics(pitches, vs_hand=None)
    vs_r = compute_pitcher_metrics(pitches, vs_hand="R")
    vs_l = compute_pitcher_metrics(pitches, vs_hand="L")
    return {
        "scope": scope,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "overall": overall,
        "vs_R": vs_r,
        "vs_L": vs_l,
    }


def pitcher_season_features(
    player_id: int,
    ctx: AsOfContext,
    season_year: Optional[int] = None,
) -> dict:
    season_start = ctx.season_start(season_year)
    end = ctx.last_allowed_date
    if season_year is not None and season_year < ctx.cutoff_date.year:
        end = _date(season_year, 12, 31)
    pitches = fetch_pitcher_pitches(player_id, season_start, end, ctx)
    out = _pull_metrics(pitches, scope="season", window_start=season_start, window_end=end)
    out.update({
        "player_id": player_id,
        "season_year": season_year if season_year is not None else ctx.cutoff_date.year,
    })
    return out


def pitcher_recent_features(
    player_id: int,
    ctx: AsOfContext,
    days: int = 30,
) -> dict:
    start = ctx.window_start(days)
    end = ctx.last_allowed_date
    pitches = fetch_pitcher_pitches(player_id, start, end, ctx)
    out = _pull_metrics(pitches, scope=f"last_{days}d", window_start=start, window_end=end)
    out.update({"player_id": player_id, "days": days})
    return out


def pitcher_features(
    player_id: int,
    ctx: AsOfContext,
    days: int = 30,
    season_year: Optional[int] = None,
) -> dict:
    """Convenience: season + 30d in one call."""
    return {
        "season": pitcher_season_features(player_id, ctx, season_year=season_year),
        "recent": pitcher_recent_features(player_id, ctx, days=days),
    }
