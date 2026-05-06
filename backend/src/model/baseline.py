"""Empirical-Bayes baseline P(HR) model.

This is what we BET WITH for the first 60+ days while we accumulate logged
odds. Simple, inspectable, every factor named. The full ML model is dormant
(Phase 8) until we have a real backtest dataset.

Prediction flow (per batter, per game):

    1. blended_hr_per_pa  ← Bayesian blend (season + 30d + dynamic prior-year)
    2. adjusted_per_pa    ← blended_hr_per_pa  +  breakout_coefficient * reliable_breakout
                            ─────────────────────────────────────────
                            (additive bump — this is where breakout enters)
    3. pitcher_factor     ← pitcher's HR/PA on the matched platoon split / league HR/PA
    4. matchup_per_pa     ← adjusted_per_pa * pitcher_factor
    5. park_adjusted      ← matchup_per_pa * park_hr_factor   (handedness-specific)
    6. env_factor         ← temp_factor * wind_factor
    7. final_per_pa       ← clip(park_adjusted * env_factor, [p_min, p_max])
    8. p_game             ← 1 - (1 - final_per_pa) ** pa_per_game

`components` is a dict of every multiplicative factor (and breakout's
multiplicative-equivalent), so run_daily.py can pick the top-3 contributors
to surface in picks.json under top_3_features.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


# League constants — tunable; values are typical modern-MLB.
LEAGUE_HR_PER_PA_DEFAULT = 0.032
DEFAULT_PA_PER_GAME = 4.2

# PA per game by lineup spot (approximate, from public lineup-position data).
PA_BY_LINEUP_SPOT: dict[int, float] = {
    1: 4.6, 2: 4.5, 3: 4.4, 4: 4.3, 5: 4.2, 6: 4.0, 7: 3.9, 8: 3.7, 9: 3.6,
}


@dataclass(frozen=True)
class BaselineConfig:
    league_hr_per_pa: float = LEAGUE_HR_PER_PA_DEFAULT
    breakout_coefficient: float = 1.0  # additive bump multiplier — tunable
    # Environment scaling — light, conservative defaults.
    temp_baseline_f: float = 70.0
    temp_per_degree: float = 0.01      # +1% per °F above baseline
    wind_per_mph: float = 0.01         # +1% per mph blowing out to CF
    temp_factor_clip: tuple[float, float] = (0.85, 1.20)
    wind_factor_clip: tuple[float, float] = (0.70, 1.40)
    # Final per-PA HR probability sanity bounds.
    p_per_pa_clip: tuple[float, float] = (0.001, 0.25)
    # Conversion: pitcher HR/9 → HR/PA. ~9 IP × ~4.3 PA/IP ≈ 38 PA per 9 IP.
    pa_per_9_innings: float = 38.0


@dataclass(frozen=True)
class BaselinePrediction:
    p_hr: float                      # P(at least one HR in the game)
    p_per_pa: float                  # final per-PA HR probability
    pa_per_game: float
    blended_hr_per_pa: float         # batter's blended rate before breakout
    breakout_score: float            # the reliability-scaled breakout (Step 2 input)
    adjusted_per_pa: float           # blended + coefficient * breakout
    components: dict[str, float] = field(default_factory=dict)
    skipped: bool = False
    skip_reason: Optional[str] = None

    def is_valid(self) -> bool:
        return (not self.skipped) and not (math.isnan(self.p_hr) or math.isinf(self.p_hr))


def predict(
    *,
    blended_hr_per_pa: float,
    reliable_breakout: float,
    pitcher_hr_per_9: float,
    pitcher_hand_split_pa: int,
    park_hr_factor: float,
    temperature_f: float,
    wind_out_to_cf_mph: float,
    is_indoor: bool,
    pa_per_game: Optional[float] = None,
    lineup_spot: Optional[int] = None,
    config: BaselineConfig = BaselineConfig(),
) -> BaselinePrediction:
    """Empirical-Bayes P(HR ≥ 1) for one batter–pitcher–park–weather combo."""
    # 1. Start from the Bayesian-blended batter rate.
    if _is_nan(blended_hr_per_pa):
        return BaselinePrediction(
            p_hr=float("nan"), p_per_pa=float("nan"), pa_per_game=0.0,
            blended_hr_per_pa=float("nan"), breakout_score=0.0,
            adjusted_per_pa=float("nan"), skipped=True,
            skip_reason="blended_hr_per_pa is NaN (insufficient batter data)",
        )

    # 2. Additive breakout bump.
    breakout_bump = config.breakout_coefficient * (reliable_breakout if not _is_nan(reliable_breakout) else 0.0)
    adjusted_per_pa = blended_hr_per_pa + breakout_bump

    # 3. Pitcher factor — convert HR/9 to HR/PA on the matched platoon split.
    if _is_nan(pitcher_hr_per_9) or pitcher_hand_split_pa < 50:
        # Sparse split: fall back to neutral pitcher (factor=1.0).
        pitcher_factor = 1.0
    else:
        pitcher_hr_per_pa = pitcher_hr_per_9 / config.pa_per_9_innings
        pitcher_factor = pitcher_hr_per_pa / config.league_hr_per_pa

    # 4. Park.
    pf = 1.0 if _is_nan(park_hr_factor) else float(park_hr_factor)

    # 5. Environment.
    if is_indoor:
        temp_factor = 1.0
        wind_factor = 1.0
    else:
        temp_factor = _clip(
            1.0 + config.temp_per_degree * (temperature_f - config.temp_baseline_f),
            config.temp_factor_clip,
        )
        wind_factor = _clip(
            1.0 + config.wind_per_mph * wind_out_to_cf_mph,
            config.wind_factor_clip,
        )

    # 6. Combine.
    matchup_per_pa = adjusted_per_pa * pitcher_factor
    park_adjusted = matchup_per_pa * pf
    final_per_pa = _clip(park_adjusted * temp_factor * wind_factor, config.p_per_pa_clip)

    # 7. Per-game P(HR ≥ 1).
    if pa_per_game is None:
        pa_per_game = PA_BY_LINEUP_SPOT.get(lineup_spot, DEFAULT_PA_PER_GAME)
    p_game = 1.0 - (1.0 - final_per_pa) ** pa_per_game

    # Components for top-3 feature surfacing. Each value is a multiplicative
    # equivalent vs neutral=1.0; "deviation" = abs(value-1) ranks them.
    breakout_mult_equiv = (
        1.0 + (breakout_bump / blended_hr_per_pa) if blended_hr_per_pa > 0 else 1.0
    )
    components = {
        "batter_skill":     blended_hr_per_pa / config.league_hr_per_pa,
        "breakout_signal":  breakout_mult_equiv,
        "pitcher":          pitcher_factor,
        "park":             pf,
        "temperature":      temp_factor,
        "wind":             wind_factor,
    }

    return BaselinePrediction(
        p_hr=p_game,
        p_per_pa=final_per_pa,
        pa_per_game=pa_per_game,
        blended_hr_per_pa=blended_hr_per_pa,
        breakout_score=reliable_breakout if not _is_nan(reliable_breakout) else 0.0,
        adjusted_per_pa=adjusted_per_pa,
        components=components,
        skipped=False,
        skip_reason=None,
    )


def _clip(x: float, bounds: tuple[float, float]) -> float:
    lo, hi = bounds
    return max(lo, min(hi, x))


def _is_nan(x) -> bool:
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return True
