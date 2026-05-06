"""Rolling 30-day batter features, as-of cutoff_date.

Same metric definitions as batter_season — only the window differs. The
Bayesian blend in src/features/blend.py combines this (recent form) with the
season prior.
"""

from __future__ import annotations

from typing import Optional

from src.backtest.as_of_context import AsOfContext
from src.features._statcast import compute_batter_metrics, fetch_batter_pitches


def batter_recent_features(
    player_id: int,
    ctx: AsOfContext,
    batter_hand: Optional[str] = None,
    days: int = 30,
) -> dict:
    """Last-N-days features for one batter (default 30)."""
    start = ctx.window_start(days)
    end = ctx.last_allowed_date

    pitches = fetch_batter_pitches(player_id, start, end, ctx)
    metrics = compute_batter_metrics(pitches, batter_hand=batter_hand)
    metrics.update({
        "player_id": player_id,
        "scope": f"last_{days}d",
        "days": days,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
    })
    return metrics
