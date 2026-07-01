"""Preflight checks + cache warming for the daily run.

This is intentionally light. The heavy-lifting feature pulls (Statcast per
batter / per pitcher) are cached on demand by `src/features/_statcast.py` —
re-pulling tomorrow only fetches yesterday's incremental data.

What this module owns:
  - Verify required artifacts exist (park metadata, park factors).
  - Warn (don't auto-build) if park factors aren't present — building from
    Statcast is a multi-hour operation and shouldn't fire from the daily cron.
  - Provide a `warm_caches_for_slate()` helper the cron can call to pre-fetch
    feature data for today's slate, shifting that latency out of the
    EV-computation hot path.
"""

from __future__ import annotations

import logging
from datetime import date as _date
from pathlib import Path

from src.backtest.as_of_context import AsOfContext
from src.features.batter_recent import batter_recent_features
from src.features.batter_season import batter_season_features
from src.features.park_weather import (
    PARK_FACTORS_PATH,
    PARK_METADATA_PATH,
    validate_park_factor_coverage,
)
from src.features.pitcher import pitcher_features
from src.model.predict import SlateEntry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

def preflight() -> dict:
    """Return a structured summary of artifact health. Never raises."""
    out = {
        "park_metadata_present": PARK_METADATA_PATH.exists(),
        "park_factors_present": PARK_FACTORS_PATH.exists(),
        "warnings": [],
        "errors": [],
    }
    if not out["park_metadata_present"]:
        out["errors"].append(
            f"missing park metadata at {PARK_METADATA_PATH} — run cannot proceed"
        )
    if not out["park_factors_present"]:
        out["warnings"].append(
            f"missing park factors at {PARK_FACTORS_PATH} — neutral 1.0 will be used. "
            "Build with `from src.features.park_weather import compute_park_factors_from_statcast; "
            "compute_park_factors_from_statcast()` (slow, multi-hour)."
        )
    else:
        # Guard the 2026-05 code-space bug: factors keyed on the wrong code
        # (AZ vs ARI) look present but resolve to neutral 1.0 at lookup time.
        missing = validate_park_factor_coverage()
        out["park_factor_coverage_missing"] = missing
        if missing:
            out["warnings"].append(
                f"park factors present but {len(missing)} internal (code/hand) "
                f"pairs have no matching row — these silently get neutral 1.0: "
                f"{', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}. "
                "Likely a code-space mismatch; rebuild via scripts/build_park_factors.py."
            )
    return out


# ---------------------------------------------------------------------------
# Cache warming
# ---------------------------------------------------------------------------

def warm_caches_for_slate(
    slate: list[SlateEntry],
    ctx: AsOfContext,
    *,
    batters: bool = True,
    pitchers: bool = True,
) -> dict:
    """Pre-fetch Statcast features for every batter and pitcher in the slate.

    Caches land in data/raw/statcast_batter/ and data/raw/statcast_pitcher/.
    Subsequent calls inside run_daily.py will hit the cache instead of pybaseball.

    Returns counts; never raises (individual fetch failures are logged and skipped).
    """
    seen_batters: set[int] = set()
    seen_pitchers: set[int] = set()
    cur_year = ctx.cutoff_date.year

    if batters:
        for entry in slate:
            if entry.batter_id in seen_batters:
                continue
            seen_batters.add(entry.batter_id)
            try:
                batter_season_features(entry.batter_id, ctx, batter_hand=entry.batter_hand)
                batter_recent_features(entry.batter_id, ctx, batter_hand=entry.batter_hand)
                # Prior year — used by blend's dynamic prior + breakout
                batter_season_features(
                    entry.batter_id, ctx,
                    batter_hand=entry.batter_hand, season_year=cur_year - 1,
                )
            except Exception as e:    # noqa: BLE001 — we want to soldier on
                logger.warning(
                    "warm-cache batter %s failed: %s: %s",
                    entry.batter_id, type(e).__name__, e,
                )

    if pitchers:
        for entry in slate:
            if entry.pitcher_id in seen_pitchers:
                continue
            seen_pitchers.add(entry.pitcher_id)
            try:
                pitcher_features(entry.pitcher_id, ctx)
            except Exception as e:    # noqa: BLE001
                logger.warning(
                    "warm-cache pitcher %s failed: %s: %s",
                    entry.pitcher_id, type(e).__name__, e,
                )

    return {
        "batters_warmed": len(seen_batters),
        "pitchers_warmed": len(seen_pitchers),
    }
