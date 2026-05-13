"""Breakout detection + recent-form flags.

Two related signals consumed by the daily pipeline:

1. **Breakout score** — how much current-season Statcast diverges from prior
   year. Targets the model's primary edge: hitters whose underlying metrics
   have meaningfully improved year-over-year, where books are still pricing
   them on last year's reputation.

   Formula:
        raw = sum(w_metric * (current[metric] - prior_year[metric]))
        reliable_breakout = clip(raw * min(1, current_PA / 100), -CAP, +CAP)

2. **Recent-form flags** — `trend_signal` and `unstable_recent`. NOT used to
   filter or score; surfaced in picks.json so the human reviewer can see when
   a pick is being driven by hot/cold short-term samples vs settled-in skill.

DEFAULT WEIGHTS — REBALANCED 2026-05-06, SCALE-CORRECTED 2026-05-13.

The 2022-2025 holdout feature-importance research showed `barrel_pct_season`
ranked #1 by both gain AND mean-|SHAP| in LightGBM. The 2026-05-06 rebalance
got the metric *ratios* right (barrel >> others) but set absolute magnitudes
~5x too high: barrel_pct=15.0 meant a 1pp YoY barrel improvement alone
(15 x 0.01) produced raw = 0.15 — exactly saturating the cap by itself.

Empirical audit on 2026-05-13 (5 days of paper-traded picks, n=306) found
87.3% of picks pegged at ±0.15, with 93% of top-30-by-p_game picks pegged
at +0.15. The signal had degenerated into a three-state flag (improved /
unchanged / declined) instead of a continuous score that differentiates
"modestly improved" from "dramatically improved."

Scale-corrected weights (÷5) so typical YoY deltas produce modest scores
and only EXTREME year-over-year changes saturate the cap:

    barrel_pct       3.0   primary; 1pp YoY delta -> raw +0.03 (~20% of cap)
    sweetspot_pct    0.6   secondary
    pull_air_pct     1.0   secondary; pulled fly-ball/line-drive contact
    max_ev (mph)     0.02  secondary; max exit velo

Replaces the prior set {xwobacon, barrel_pct, hardhit_pct, avg_ev}, which
mixed mid-tier and low-tier features. Weights are configurable per call.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


DEFAULT_BREAKOUT_WEIGHTS: dict[str, float] = {
    "barrel_pct":      3.0,
    "sweetspot_pct":   0.6,
    "pull_air_pct":    1.0,
    "max_ev":          0.02,
}

DEFAULT_RELIABILITY_PA = 100        # full reliability at this PA count
DEFAULT_BREAKOUT_CAP = 0.15         # symmetric clip on the final reliable score

# Recent-form flag thresholds. unstable_recent fires when 30d barrel rate is
# 1.5x or more, OR 0.5x or less, of season barrel rate. Source: review-gate
# spec; intentionally wide so the flag highlights only the genuinely-volatile
# short windows, not normal mid-season noise.
UNSTABLE_HIGH_RATIO = 1.5
UNSTABLE_LOW_RATIO  = 0.5


# ---------------------------------------------------------------------------
# Breakout score (current vs prior year)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BreakoutScore:
    raw: float
    reliability: float           # min(1, current_pa / RELIABILITY_PA)
    score: float                 # raw * reliability, clipped to ±CAP
    has_prior_year: bool
    components: dict             # per-metric contributions to raw (pre-reliability)
    weights: dict = field(default_factory=dict)

    def is_zero(self) -> bool:
        return self.score == 0.0


def compute_breakout_score(
    current: Optional[dict],
    prior_year: Optional[dict],
    *,
    current_pa_key: str = "pa",
    weights: Optional[dict[str, float]] = None,
    reliability_pa: int = DEFAULT_RELIABILITY_PA,
    cap: float = DEFAULT_BREAKOUT_CAP,
) -> BreakoutScore:
    """Reliability-scaled, capped breakout score.

    Args:
        current: dict from compute_batter_metrics() for the current season.
        prior_year: dict from compute_batter_metrics() for the prior season.
            None or empty → score = 0 (no baseline to compare to).
        weights: per-metric weights. Defaults to DEFAULT_BREAKOUT_WEIGHTS.
        reliability_pa: PA at which we trust the current sample fully.
        cap: symmetric clip applied AFTER reliability scaling.
    """
    w = dict(DEFAULT_BREAKOUT_WEIGHTS)
    if weights:
        w.update(weights)

    if not prior_year or current is None:
        return BreakoutScore(
            raw=0.0, reliability=0.0, score=0.0,
            has_prior_year=False, components={}, weights=w,
        )

    components: dict[str, float] = {}
    raw = 0.0
    for metric, weight in w.items():
        cur_v = current.get(metric, float("nan"))
        prior_v = prior_year.get(metric, float("nan"))
        if _is_nan(cur_v) or _is_nan(prior_v):
            components[metric] = 0.0
            continue
        delta = float(cur_v) - float(prior_v)
        contribution = weight * delta
        components[metric] = contribution
        raw += contribution

    current_pa = int(current.get(current_pa_key, 0) or 0)
    reliability = min(1.0, current_pa / reliability_pa) if reliability_pa > 0 else 0.0
    if current_pa <= 0:
        reliability = 0.0

    reliable = raw * reliability
    score = max(-cap, min(cap, reliable))

    return BreakoutScore(
        raw=raw,
        reliability=reliability,
        score=score,
        has_prior_year=True,
        components=components,
        weights=w,
    )


def apply_reliability_and_cap(
    raw: float,
    current_pa: int,
    reliability_pa: int = DEFAULT_RELIABILITY_PA,
    cap: float = DEFAULT_BREAKOUT_CAP,
) -> float:
    """Lower-level helper: take a raw breakout value, scale & cap. Exposed for testing."""
    if current_pa <= 0 or reliability_pa <= 0:
        return 0.0
    reliability = min(1.0, current_pa / reliability_pa)
    return max(-cap, min(cap, raw * reliability))


# ---------------------------------------------------------------------------
# Recent-form flags (season vs 30d barrel) — surfaced, not scored
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RecentFormFlags:
    """Diagnostic flags for season-vs-30d divergence. Surfaced only.

    Attributes:
      trend_signal:    (recent_barrel - season_barrel) / season_barrel.
                       Positive → batter barreling more recently than usual.
                       None when season_barrel is missing/zero.
      unstable_recent: True when recent/season ratio is >= 1.5 or <= 0.5.
                       False when either rate is missing or zero.
    """
    trend_signal: Optional[float]
    unstable_recent: bool
    season_barrel_pct: Optional[float]
    recent_barrel_pct: Optional[float]


def compute_recent_form_flags(
    season: Optional[dict],
    recent: Optional[dict],
    *,
    metric_key: str = "barrel_pct",
    high_ratio: float = UNSTABLE_HIGH_RATIO,
    low_ratio: float = UNSTABLE_LOW_RATIO,
) -> RecentFormFlags:
    """Compute trend_signal + unstable_recent from season vs 30d barrel rates.

    Per the 2026-05-06 research observation: `barrel_pct_30d` shows ~zero
    importance in LightGBM when `barrel_pct_season` is present, because the
    30d level alone is noisy. The CHANGE between the two carries signal —
    that's what trend_signal captures. Surfaced for human review; not used
    to score or filter.
    """
    s_val = (season or {}).get(metric_key)
    r_val = (recent or {}).get(metric_key)

    s_clean = float(s_val) if (s_val is not None and not _is_nan(s_val)) else None
    r_clean = float(r_val) if (r_val is not None and not _is_nan(r_val)) else None

    if s_clean is None or r_clean is None or s_clean <= 0:
        return RecentFormFlags(
            trend_signal=None, unstable_recent=False,
            season_barrel_pct=s_clean, recent_barrel_pct=r_clean,
        )

    trend = (r_clean - s_clean) / s_clean
    ratio = r_clean / s_clean
    unstable = ratio >= high_ratio or ratio <= low_ratio

    return RecentFormFlags(
        trend_signal=trend,
        unstable_recent=unstable,
        season_barrel_pct=s_clean,
        recent_barrel_pct=r_clean,
    )


def _is_nan(x) -> bool:
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return True
