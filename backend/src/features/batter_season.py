"""Year-to-date batter Statcast features, as-of cutoff_date.

Pulls regular-season pitch data from March 1 of the cutoff year through
cutoff_date - 1, then aggregates per `compute_batter_metrics`.

Returns NaN for any metric whose denominator is zero. Skip-the-batter logic
lives upstream — feature modules never median-fill.
"""

from __future__ import annotations

import logging
from typing import Optional

from src.backtest.as_of_context import AsOfContext
from src.features._statcast import compute_batter_metrics, fetch_batter_pitches

logger = logging.getLogger(__name__)


def batter_season_features(
    player_id: int,
    ctx: AsOfContext,
    batter_hand: Optional[str] = None,
    season_year: Optional[int] = None,
) -> dict:
    """YTD features for one batter.

    Args:
        player_id: MLBAM ID.
        ctx: AsOfContext — data on/after ctx.cutoff_date is forbidden.
        batter_hand: 'R' or 'L'. Required for pull% (pull side depends on stance).
        season_year: override season-start year (default: ctx.cutoff_date.year).
            Used by early-season fallback to pull the prior year as a deeper prior.

    Returns:
        dict with keys: pa, ab, hr, bbe, hr_per_pa, iso, barrel_pct, xwobacon,
                        hardhit_pct, sweetspot_pct, pull_pct, plus
                        scope='season' and player_id.
    """
    season_start = ctx.season_start(season_year)
    end = ctx.last_allowed_date

    # If season_year is in the past, end is December 31 of that year.
    if season_year is not None and season_year < ctx.cutoff_date.year:
        from datetime import date as _date
        end = _date(season_year, 12, 31)

    pitches = fetch_batter_pitches(player_id, season_start, end, ctx)
    metrics = compute_batter_metrics(pitches, batter_hand=batter_hand)
    metrics.update({
        "player_id": player_id,
        "scope": "season",
        "season_year": season_year if season_year is not None else ctx.cutoff_date.year,
        "window_start": season_start.isoformat(),
        "window_end": end.isoformat(),
    })
    return metrics
