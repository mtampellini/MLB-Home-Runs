"""Skip rules for the daily pipeline.

Project rule: do not bet on hitters with no meaningful track record. Skipping
is preferred to median-fill (which broke V4). Caller should log skipped batters
to data/processed/skipped_batters_YYYY-MM-DD.json with the returned reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


MIN_CURRENT_SEASON_PA = 50    # below this we require prior-year data to bet
MIN_PRIOR_YEAR_PA = 50        # what counts as "has prior-year data"


@dataclass(frozen=True)
class SkipDecision:
    skip: bool
    reason: Optional[str]      # None when not skipped
    code: Optional[str]        # short tag for log aggregation, None when not skipped


def should_skip_batter(
    season_pa: int,
    prior_year_pa: int,
    min_current_pa: int = MIN_CURRENT_SEASON_PA,
    min_prior_year_pa: int = MIN_PRIOR_YEAR_PA,
) -> SkipDecision:
    """Skip iff current_season_PA < threshold AND no prior-year track record.

    Examples (with defaults):
      - season=30, prior=0    → SKIP (true rookie, not enough current data)
      - season=30, prior=600  → KEEP (early-season vet — prior carries us)
      - season=60, prior=0    → KEEP (current sample sufficient on its own)
      - season=10, prior=600  → KEEP (vet hasn't played much yet — prior dominates)
    """
    season_pa = int(season_pa or 0)
    prior_year_pa = int(prior_year_pa or 0)

    if season_pa < min_current_pa and prior_year_pa < min_prior_year_pa:
        return SkipDecision(
            skip=True,
            reason=(
                f"insufficient track record: current_season_PA={season_pa} "
                f"(< {min_current_pa}) AND prior_year_PA={prior_year_pa} "
                f"(< {min_prior_year_pa})"
            ),
            code="LOW_DATA",
        )
    return SkipDecision(skip=False, reason=None, code=None)
