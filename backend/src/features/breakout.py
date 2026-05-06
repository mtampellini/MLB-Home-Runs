"""Breakout detection: how much current-season Statcast diverges from prior-year.

Targets the model's primary edge: hitters whose underlying metrics have
meaningfully improved year-over-year, where the books are still pricing them
on last year's reputation.

Formula (per spec):

    raw_breakout = w_xwobacon * (curr_xwobacon - prior_xwobacon)
                 + w_barrel   * (curr_barrel  - prior_barrel)
                 + w_hardhit  * (curr_hardhit - prior_hardhit)
                 + w_avg_ev   * (curr_avg_ev  - prior_avg_ev)

    reliable_breakout = clip(
        raw_breakout * min(1, current_season_PA / 100),
        -CAP, +CAP,
    )

CAP = 0.15.

Default weights (normalized so each metric contributes ~0.15 to raw_breakout
for a typical elite YoY delta — i.e. a single dominant metric improvement is
enough to hit the cap, and a fully-broken-out hitter across all four metrics
sits comfortably above the cap):

    xwobacon     5.0   → typical YoY delta ≈ 0.03 → contribution ≈ 0.15
    barrel_pct   7.5   → typical YoY delta ≈ 0.02 → contribution ≈ 0.15
    hardhit_pct  5.0   → typical YoY delta ≈ 0.03 → contribution ≈ 0.15
    avg_ev (mph) 0.15  → typical YoY delta ≈ 1.0  → contribution ≈ 0.15

Weights are configurable per call.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


DEFAULT_BREAKOUT_WEIGHTS: dict[str, float] = {
    "xwobacon":    5.0,
    "barrel_pct":  7.5,
    "hardhit_pct": 5.0,
    "avg_ev":      0.15,
}

DEFAULT_RELIABILITY_PA = 100   # full reliability at this PA count
DEFAULT_BREAKOUT_CAP = 0.15    # symmetric clip on the final reliable score


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
    """Compute reliability-scaled, capped breakout score.

    Args:
        current: dict from compute_batter_metrics() for the current season.
        prior_year: dict from compute_batter_metrics() for the prior season.
            If None or empty → score = 0 (no baseline to compare to).
        weights: per-metric weights. Defaults to equal 0.25 each.
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


def _is_nan(x) -> bool:
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return True
