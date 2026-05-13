"""Empirical-Bayes blend of pre-30d + recent + per-player prior.

Formula:

    blended = (prior_PA × prior_rate
               + season_PA × season_rate
               + recent_PA × recent_rate × shrinkage)
            / (prior_PA + season_PA + recent_PA × shrinkage)

Three terms, each defensible on its own:

- `prior_PA × prior_rate` — the anchor. Caller passes either the batter's
  prior-year HR/PA (preferred) or the league mean (~0.032) when no prior
  year exists. `PRIOR_PA_EQUIVALENT=100` gives this anchor a fixed strength
  in PA-equivalent units, so its relative weight naturally diminishes as the
  in-season sample grows. (Replaces the older `dynamic_prior_year_weight`
  decay, which made the prior vanish at exactly the point where the
  in-season sample was big enough to over-fit.)

- `season_PA × season_rate` — the pre-30d in-season sample. The caller
  (`batter_season_features`) computes this excluding the last 30 days, so
  this term and the recent term are DISJOINT. Without that de-overlap the
  last 30 days were counted twice and the blend over-weighted hot streaks.

- `recent_PA × recent_rate × shrinkage` — last 30 days. `DEFAULT_SHRINKAGE=1.0`
  gives recent its natural inverse-variance weight (proportional to sample
  size). Pass `shrinkage > 1.0` if you want a recent-form premium; the
  previous default of 1.5 was unjustified by the literature on intra-season
  HR-rate predictiveness and compounded with the overlap bug to inflate
  estimates for hot players.

NaN policy: any rate that's NaN drops its term (PA effectively zeroed) so
NaN can't pollute the blended rate. Skip-the-batter decisions live upstream
(see src/features/skip_logic.py) — this module never median-fills.

Why this is "empirical-Bayes" not full Bayesian:
The prior is point-valued (no variance assumption), the sample weights
ignore over-dispersion, and there's no MCMC. But the shrinkage behavior is
the same: small samples get pulled toward the prior, large samples
overwhelm it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


DEFAULT_SHRINKAGE = 1.0          # recent gets weight by sample size, not multiplier
PRIOR_PA_EQUIVALENT = 100        # fixed anchor weight in PA-equivalent units


@dataclass(frozen=True)
class BlendResult:
    rate: float
    season_pa: int               # pre-30d in-season PA (current year)
    recent_pa: int
    prior_pa: int                # how much weight the anchor carried
    prior_rate: float            # the anchor value used (for diagnostics)
    used_prior: bool             # True iff the anchor contributed > 0

    def is_valid(self) -> bool:
        return not (math.isnan(self.rate) or math.isinf(self.rate))


def bayesian_blend(
    season_pa: int,
    season_rate: float,
    recent_pa: int,
    recent_rate: float,
    *,
    prior_pa: int = 0,
    prior_rate: float = float("nan"),
    shrinkage: float = DEFAULT_SHRINKAGE,
) -> BlendResult:
    """Combine pre-30d + recent + (optional) prior anchor into one blended rate.

    Arguments:
      season_pa / season_rate: PRE-30d in-season sample (March 1 → cutoff-31).
        Disjoint with recent. See `batter_season_features`.
      recent_pa / recent_rate: last-30-day sample.
      prior_pa / prior_rate: anchor in PA-equivalent units. Pass 0 (default)
        for no anchor — used by the pitcher HR/9 blend, which doesn't have a
        per-pitcher prior available yet. For batters, pass either prior-year
        HR/PA or the league rate, with prior_pa = PRIOR_PA_EQUIVALENT.
      shrinkage: weight multiplier on the recent term. Default 1.0 = inverse
        -variance weighting by sample size; >1.0 = recent-form premium.

    Returns a BlendResult. `rate` is NaN iff every term has zero effective weight.
    """
    season_w, season_term = _term(season_pa, season_rate, weight=1.0)
    recent_w, recent_term = _term(recent_pa, recent_rate, weight=shrinkage)
    prior_w, prior_term = _term(prior_pa, prior_rate, weight=1.0)

    denom = season_w + recent_w + prior_w
    if denom <= 0:
        return BlendResult(
            rate=float("nan"),
            season_pa=season_pa,
            recent_pa=recent_pa,
            prior_pa=0,
            prior_rate=prior_rate,
            used_prior=False,
        )

    return BlendResult(
        rate=(season_term + recent_term + prior_term) / denom,
        season_pa=season_pa,
        recent_pa=recent_pa,
        prior_pa=int(prior_pa) if prior_w > 0 else 0,
        prior_rate=prior_rate,
        used_prior=prior_w > 0,
    )


def blend_features(
    season: dict,
    recent: dict,
    *,
    prior_rate: Optional[float] = None,
    prior_pa: int = PRIOR_PA_EQUIVALENT,
    metric_key: str = "hr_per_pa",
    pa_key: str = "pa",
    shrinkage: float = DEFAULT_SHRINKAGE,
) -> BlendResult:
    """Higher-level wrapper: blend a feature key across season + recent + anchor.

    `prior_rate` is the per-player anchor for this metric — for HR/PA pass
    either prior-year HR/PA (when the batter has a prior year) OR the league
    mean as a fallback. Pass `None` to skip the anchor (no prior shrinkage).
    """
    season_pa = int(season.get(pa_key, 0) or 0)
    recent_pa = int(recent.get(pa_key, 0) or 0)
    season_rate = float(season.get(metric_key, float("nan")))
    recent_rate = float(recent.get(metric_key, float("nan")))

    if prior_rate is None or _is_nan(prior_rate):
        prior_pa_resolved = 0
        prior_rate_resolved = float("nan")
    else:
        prior_pa_resolved = int(prior_pa)
        prior_rate_resolved = float(prior_rate)

    return bayesian_blend(
        season_pa=season_pa,
        season_rate=season_rate,
        recent_pa=recent_pa,
        recent_rate=recent_rate,
        prior_pa=prior_pa_resolved,
        prior_rate=prior_rate_resolved,
        shrinkage=shrinkage,
    )


def _term(pa: int, rate: float, weight: float) -> tuple[float, float]:
    if pa <= 0 or _is_nan(rate):
        return 0.0, 0.0
    w = pa * weight
    return w, w * rate


def _is_nan(x) -> bool:
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return True
