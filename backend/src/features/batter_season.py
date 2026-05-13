"""Pre-30d batter Statcast features, as-of cutoff_date.

For the CURRENT season this pulls regular-season pitch data from March 1
of the cutoff year through `cutoff_date - 31` (i.e. the season window EXCLUDING
the most recent 30 days). The last 30 days are owned by `batter_recent_features`;
keeping the two windows disjoint is what makes the Bayesian blend in
`src/features/blend.py` mathematically coherent — otherwise the same PAs are
counted twice (once in season, once again at 1.5× weight in recent) and a
hot 30 days dominates the blended rate.

For PRIOR YEARS (season_year < cutoff year) the full year is returned —
prior-year is the per-player anchor in the blend, so we want all of it.

Returns NaN for any metric whose denominator is zero. Skip-the-batter logic
lives upstream — feature modules never median-fill.
"""

from __future__ import annotations

import logging
from datetime import date as _date
from datetime import timedelta
from typing import Optional

from src.backtest.as_of_context import AsOfContext
from src.features._statcast import compute_batter_metrics, fetch_batter_pitches

logger = logging.getLogger(__name__)


RECENT_WINDOW_DAYS = 30  # must match batter_recent_features default


def batter_season_features(
    player_id: int,
    ctx: AsOfContext,
    batter_hand: Optional[str] = None,
    season_year: Optional[int] = None,
) -> dict:
    """Pre-30d features for one batter (current year) OR full-season (prior year).

    Args:
        player_id: MLBAM ID.
        ctx: AsOfContext — data on/after ctx.cutoff_date is forbidden.
        batter_hand: 'R' or 'L'. Required for pull% (pull side depends on stance).
        season_year: override season-start year (default: ctx.cutoff_date.year).
            When < ctx.cutoff_date.year, returns the FULL prior-year window
            (used as the per-player prior anchor in the blend).

    Returns:
        dict with keys: pa, ab, hr, bbe, hr_per_pa, iso, barrel_pct, xwobacon,
                        hardhit_pct, sweetspot_pct, pull_pct, plus
                        scope='season' and player_id.
    """
    season_start = ctx.season_start(season_year)
    is_prior_year = season_year is not None and season_year < ctx.cutoff_date.year

    if is_prior_year:
        end = _date(season_year, 12, 31)
    else:
        # Disjoint with batter_recent_features (cutoff-30 → cutoff-1).
        end = ctx.last_allowed_date - timedelta(days=RECENT_WINDOW_DAYS)

    # Early-season edge case: pre-30d window is empty (e.g. cutoff in early
    # April). Return zero-PA metrics so the blend falls back to prior + recent.
    if end < season_start:
        metrics = compute_batter_metrics(None, batter_hand=batter_hand)
        metrics.update({
            "player_id": player_id,
            "scope": "season",
            "season_year": season_year if season_year is not None else ctx.cutoff_date.year,
            "window_start": season_start.isoformat(),
            "window_end": end.isoformat(),
            "empty_window": True,
        })
        return metrics

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
