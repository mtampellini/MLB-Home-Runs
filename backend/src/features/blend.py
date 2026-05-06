"""Bayesian blend of season-to-date and last-30-day rates.

Formula:

    blended = (season_PA * season_rate + recent_PA * recent_rate * shrinkage)
              / (season_PA + recent_PA * shrinkage)

`shrinkage` weights the recent window (default 1.5 → recent form upweighted).

Prior-year fallback (early-season): when the season window is sparse, prior-year
season is folded in as another sample. The PRIOR-YEAR WEIGHT IS DYNAMIC:

    prior_year_weight = max(0, 1 - current_season_PA / 200)

So:
- April (~20 current-season PAs)  → prior year ≈ 90% weight
- May   (~100 current-season PAs) → prior year ≈ 50%
- June+ (≥200 current-season PAs) → prior year clamped to 0%

Edge cases:
- No prior-year data → prior_year_weight is irrelevant; only current-season is used.
  If current_season_PA < 50 the BATTER should be SKIPPED upstream (see skip_logic).
- Prior-year data but no current data (early April, hasn't played yet) → prior
  year is used at full weight (1.0). Caller should flag low-confidence.
- Player who changed teams → prior-year still applies; skill is player-level.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


DEFAULT_SHRINKAGE = 1.5
DYNAMIC_PRIOR_YEAR_PA_DENOMINATOR = 200  # decay scale for prior-year weight
EARLY_SEASON_PA_THRESHOLD = 100          # below this we even bother computing prior-year fold-in


def dynamic_prior_year_weight(
    current_season_pa: int,
    pa_denominator: int = DYNAMIC_PRIOR_YEAR_PA_DENOMINATOR,
) -> float:
    """Weight assigned to prior-year season as a function of current-season PA.

    weight = max(0, 1 - current_season_PA / 200)
    """
    if current_season_pa <= 0:
        return 1.0
    w = 1.0 - (current_season_pa / pa_denominator)
    return max(0.0, w)


@dataclass(frozen=True)
class BlendResult:
    rate: float
    season_pa: int
    recent_pa: int
    prior_year_pa: int
    used_prior_year: bool
    prior_year_weight: float

    def is_valid(self) -> bool:
        return not (math.isnan(self.rate) or math.isinf(self.rate))


def bayesian_blend(
    season_pa: int,
    season_rate: float,
    recent_pa: int,
    recent_rate: float,
    shrinkage: float = DEFAULT_SHRINKAGE,
    prior_year_pa: int = 0,
    prior_year_rate: float = float("nan"),
    prior_year_weight: Optional[float] = None,
    early_season_pa_threshold: int = EARLY_SEASON_PA_THRESHOLD,
) -> BlendResult:
    """Combine season + recent (and optionally prior-year) into one blended rate.

    `prior_year_weight=None` (default) → computed dynamically from season_pa via
    `dynamic_prior_year_weight()`. Pass an explicit float to override (useful in
    tests and for non-PA-counted metrics).

    Skip semantics:
      - If everything is empty/NaN → returns NaN. Caller decides skip-vs-impute
        (project rule: skip, never median-fill).
      - If a rate is NaN, its component is dropped (PA effectively zeroed).
    """
    use_prior_year = (
        season_pa < early_season_pa_threshold
        and prior_year_pa > 0
        and not _is_nan(prior_year_rate)
    )

    if prior_year_weight is None:
        py_weight_resolved = dynamic_prior_year_weight(season_pa)
    else:
        py_weight_resolved = float(prior_year_weight)

    season_w, season_term = _term(season_pa, season_rate, weight=1.0)
    recent_w, recent_term = _term(recent_pa, recent_rate, weight=shrinkage)

    if use_prior_year and py_weight_resolved > 0:
        py_w, py_term = _term(prior_year_pa, prior_year_rate, weight=py_weight_resolved)
    else:
        py_w, py_term = 0.0, 0.0
        if not use_prior_year:
            py_weight_resolved = 0.0

    denom = season_w + recent_w + py_w
    if denom <= 0:
        return BlendResult(
            rate=float("nan"),
            season_pa=season_pa,
            recent_pa=recent_pa,
            prior_year_pa=prior_year_pa if use_prior_year else 0,
            used_prior_year=False,
            prior_year_weight=py_weight_resolved,
        )

    rate = (season_term + recent_term + py_term) / denom
    return BlendResult(
        rate=rate,
        season_pa=season_pa,
        recent_pa=recent_pa,
        prior_year_pa=prior_year_pa if use_prior_year else 0,
        used_prior_year=use_prior_year and py_w > 0,
        prior_year_weight=py_weight_resolved,
    )


def blend_features(
    season: dict,
    recent: dict,
    prior_year: Optional[dict] = None,
    metric_key: str = "hr_per_pa",
    pa_key: str = "pa",
    shrinkage: float = DEFAULT_SHRINKAGE,
    prior_year_weight: Optional[float] = None,
) -> BlendResult:
    """Higher-level wrapper: blend a feature key across season/recent/prior-year dicts.

    Each dict must have `metric_key` (the rate) and `pa_key` (the denominator).
    Default `prior_year_weight=None` → dynamic decay based on current-season PA.
    """
    season_pa = int(season.get(pa_key, 0) or 0)
    recent_pa = int(recent.get(pa_key, 0) or 0)
    season_rate = float(season.get(metric_key, float("nan")))
    recent_rate = float(recent.get(metric_key, float("nan")))

    py_pa = 0
    py_rate = float("nan")
    if prior_year is not None:
        py_pa = int(prior_year.get(pa_key, 0) or 0)
        py_rate = float(prior_year.get(metric_key, float("nan")))

    return bayesian_blend(
        season_pa=season_pa,
        season_rate=season_rate,
        recent_pa=recent_pa,
        recent_rate=recent_rate,
        shrinkage=shrinkage,
        prior_year_pa=py_pa,
        prior_year_rate=py_rate,
        prior_year_weight=prior_year_weight,
    )


def _term(pa: int, rate: float, weight: float) -> tuple[float, float]:
    if pa <= 0 or _is_nan(rate):
        return 0.0, 0.0
    w = pa * weight
    return w, w * rate


def _is_nan(x: float) -> bool:
    try:
        return math.isnan(x)
    except (TypeError, ValueError):
        return True
