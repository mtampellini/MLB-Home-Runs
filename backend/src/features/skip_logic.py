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
CAREER_PA_MIN = 200           # season+prior_year PA total floor.
                              # Statcast metrics (barrel%, xwOBAcon, etc) stabilize
                              # around 100-200 PAs; 200 ensures the features driving
                              # both batter rate AND breakout score are reliable.
                              # Catches the Felix-Reyes pattern (55 + 5 = 60 career
                              # PA, slipped through the original 50-PA gate).


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
    career_pa_min: int = CAREER_PA_MIN,
) -> SkipDecision:
    """Skip iff:
      - current_season_PA < min AND prior_year_PA < min  (no track record), OR
      - season_PA + prior_year_PA < career_pa_min        (career too small to trust).

    Two-part gate. The first catches true rookies; the second catches the
    longer-tail problem of recent call-ups with one MLB year split between
    seasons (where each year alone might pass the 50-PA gate but the combined
    sample is still too noisy for reliable Statcast metrics).

    Examples (defaults min_current=50, min_prior=50, career_min=200):
      - season=55, prior=5    → SKIP (career=60 < 200; 'Felix Reyes' pattern)
      - season=30, prior=0    → SKIP (no track record)
      - season=30, prior=600  → KEEP (career=630)
      - season=60, prior=0    → SKIP (career=60 < 200)
      - season=100, prior=110 → KEEP (career=210)
    """
    season_pa = int(season_pa or 0)
    prior_year_pa = int(prior_year_pa or 0)
    career_pa = season_pa + prior_year_pa

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
    if career_pa < career_pa_min:
        return SkipDecision(
            skip=True,
            reason=(
                f"career sample too small: season_PA={season_pa} + "
                f"prior_year_PA={prior_year_pa} = {career_pa} (< {career_pa_min})"
            ),
            code="LOW_CAREER_PA",
        )
    return SkipDecision(skip=False, reason=None, code=None)
