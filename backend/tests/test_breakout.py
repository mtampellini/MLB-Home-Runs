"""Pre-Phase-3 review-gate tests.

Locks in the four behaviours requested:
1. Dynamic prior-year weight: 1 - PA/200, clamped at 0.
2. Breakout reliability scaling: raw * min(1, PA/100), clipped to ±0.15.
3. No-prior-year edge case → breakout = 0.
4. Skip logic: season_PA<50 AND no prior-year → SKIP, otherwise KEEP.

Plus integration sanity checks for blend + breakout interplay.
"""

import math

import pytest

from src.features.blend import (
    DYNAMIC_PRIOR_YEAR_PA_DENOMINATOR,
    bayesian_blend,
    blend_features,
    dynamic_prior_year_weight,
)
from src.features.breakout import (
    BreakoutScore,
    DEFAULT_BREAKOUT_CAP,
    apply_reliability_and_cap,
    compute_breakout_score,
)
from src.features.skip_logic import should_skip_batter
from src.model.baseline import BaselineConfig, predict


# ---------------------------------------------------------------------------
# 1. Dynamic prior-year weight
# ---------------------------------------------------------------------------

def test_dynamic_weight_april_20_pa_returns_about_90_percent():
    assert dynamic_prior_year_weight(20) == pytest.approx(0.90)


def test_dynamic_weight_may_100_pa_returns_50_percent():
    assert dynamic_prior_year_weight(100) == pytest.approx(0.50)


def test_dynamic_weight_june_200_pa_returns_zero():
    assert dynamic_prior_year_weight(200) == pytest.approx(0.00)


def test_dynamic_weight_300_pa_clamped_to_zero():
    assert dynamic_prior_year_weight(300) == 0.0


def test_dynamic_weight_zero_pa_returns_one():
    # Vet hasn't played yet — give prior year full weight.
    assert dynamic_prior_year_weight(0) == 1.0


def test_blend_features_uses_dynamic_weight_by_default():
    # With season_pa=20 and prior-year present, dynamic weight should be ~0.90.
    season = {"pa": 20, "hr_per_pa": 0.04}
    recent = {"pa": 20, "hr_per_pa": 0.04}
    py = {"pa": 600, "hr_per_pa": 0.05}
    r = blend_features(season, recent, prior_year=py, metric_key="hr_per_pa", pa_key="pa")
    assert r.used_prior_year is True
    assert r.prior_year_weight == pytest.approx(0.90)


def test_blend_features_dynamic_zero_weight_post_june():
    # season_pa=250 → dynamic weight clamps to 0 → prior year not used.
    season = {"pa": 250, "hr_per_pa": 0.04}
    recent = {"pa": 60, "hr_per_pa": 0.05}
    py = {"pa": 600, "hr_per_pa": 0.07}
    r = blend_features(season, recent, prior_year=py, metric_key="hr_per_pa", pa_key="pa")
    # Even though use_prior_year is technically gated by EARLY_SEASON_PA_THRESHOLD,
    # at season_pa=250 we're past the threshold AND the weight would be 0.
    assert r.used_prior_year is False


# ---------------------------------------------------------------------------
# 2. Breakout reliability scaling
# ---------------------------------------------------------------------------

def test_breakout_reliability_50_pa_halves_raw():
    # raw=0.10, PA=50 → reliability=0.5 → 0.05 (under cap).
    assert apply_reliability_and_cap(raw=0.10, current_pa=50) == pytest.approx(0.05)


def test_breakout_reliability_100_pa_full():
    # raw=0.10, PA=100 → reliability=1.0 → 0.10 (under cap).
    assert apply_reliability_and_cap(raw=0.10, current_pa=100) == pytest.approx(0.10)


def test_breakout_reliability_200_pa_still_clamped_at_one():
    # Reliability never exceeds 1.0.
    assert apply_reliability_and_cap(raw=0.10, current_pa=200) == pytest.approx(0.10)


def test_breakout_cap_applied_above_threshold():
    # raw=0.30, PA=200 → reliability=1.0 → 0.30, then capped to +0.15.
    assert apply_reliability_and_cap(raw=0.30, current_pa=200) == pytest.approx(DEFAULT_BREAKOUT_CAP)


def test_breakout_cap_applied_negative():
    assert apply_reliability_and_cap(raw=-0.50, current_pa=200) == pytest.approx(-DEFAULT_BREAKOUT_CAP)


def test_breakout_zero_pa_returns_zero():
    assert apply_reliability_and_cap(raw=0.10, current_pa=0) == 0.0


def test_compute_breakout_score_single_metric_via_barrel_pct():
    """Single-metric breakout. Default barrel_pct weight = 7.5; delta = 0.02
    → raw = 7.5 * 0.02 = 0.15 (exactly hits the cap at full reliability)."""
    current = {"pa": 50, "xwobacon": 0.40, "barrel_pct": 0.10,
               "hardhit_pct": 0.40, "avg_ev": 90.0}
    prior = {"pa": 600, "xwobacon": 0.40, "barrel_pct": 0.08,
             "hardhit_pct": 0.40, "avg_ev": 90.0}
    r = compute_breakout_score(current, prior)
    assert r.raw == pytest.approx(0.15)
    assert r.reliability == pytest.approx(0.5)
    assert r.score == pytest.approx(0.075)


def test_compute_breakout_score_default_weights_balance_contributions():
    """Sanity: with default weights, a typical elite YoY delta in EACH metric
    contributes ~0.15 to raw_breakout — confirming the weights are normalized
    across the four metrics' very different scales."""
    # xwobacon delta 0.03 × w=5.0 → 0.15
    only_xw = compute_breakout_score(
        current={"pa": 200, "xwobacon": 0.43, "barrel_pct": 0.10,
                 "hardhit_pct": 0.40, "avg_ev": 90.0},
        prior_year={"pa": 600, "xwobacon": 0.40, "barrel_pct": 0.10,
                    "hardhit_pct": 0.40, "avg_ev": 90.0},
    )
    # barrel delta 0.02 × w=7.5 → 0.15
    only_br = compute_breakout_score(
        current={"pa": 200, "xwobacon": 0.40, "barrel_pct": 0.12,
                 "hardhit_pct": 0.40, "avg_ev": 90.0},
        prior_year={"pa": 600, "xwobacon": 0.40, "barrel_pct": 0.10,
                    "hardhit_pct": 0.40, "avg_ev": 90.0},
    )
    # avg_ev delta 1.0 × w=0.15 → 0.15
    only_ev = compute_breakout_score(
        current={"pa": 200, "xwobacon": 0.40, "barrel_pct": 0.10,
                 "hardhit_pct": 0.40, "avg_ev": 91.0},
        prior_year={"pa": 600, "xwobacon": 0.40, "barrel_pct": 0.10,
                    "hardhit_pct": 0.40, "avg_ev": 90.0},
    )
    assert only_xw.raw == pytest.approx(0.15)
    assert only_br.raw == pytest.approx(0.15)
    assert only_ev.raw == pytest.approx(0.15)


# ---------------------------------------------------------------------------
# 3. No-prior-year edge case
# ---------------------------------------------------------------------------

def test_breakout_score_returns_zero_when_no_prior_year():
    current = {"pa": 200, "xwobacon": 0.45, "barrel_pct": 0.15,
               "hardhit_pct": 0.50, "avg_ev": 92.0}
    r = compute_breakout_score(current, prior_year=None)
    assert r.score == 0.0
    assert r.raw == 0.0
    assert r.has_prior_year is False
    assert r.is_zero() is True


def test_breakout_score_returns_zero_when_prior_year_empty_dict():
    current = {"pa": 200, "xwobacon": 0.45, "barrel_pct": 0.15,
               "hardhit_pct": 0.50, "avg_ev": 92.0}
    r = compute_breakout_score(current, prior_year={})
    assert r.score == 0.0
    assert r.has_prior_year is False


def test_breakout_drops_component_with_nan_in_either_side():
    """If prior xwOBAcon is NaN but other metrics are clean, only those contribute."""
    current = {"pa": 200, "xwobacon": 0.45, "barrel_pct": 0.10,
               "hardhit_pct": 0.40, "avg_ev": 90.0}
    prior = {"pa": 600, "xwobacon": float("nan"), "barrel_pct": 0.10,
             "hardhit_pct": 0.40, "avg_ev": 90.0}
    r = compute_breakout_score(current, prior)
    assert r.components["xwobacon"] == 0.0
    assert r.raw == 0.0
    assert r.score == 0.0


# ---------------------------------------------------------------------------
# 4. Skip logic
# ---------------------------------------------------------------------------

def test_skip_30_pa_no_prior_year_is_skipped():
    d = should_skip_batter(season_pa=30, prior_year_pa=0)
    assert d.skip is True
    assert d.code == "LOW_DATA"
    assert "current_season_PA=30" in d.reason


def test_keep_30_pa_with_full_prior_year():
    d = should_skip_batter(season_pa=30, prior_year_pa=600)
    assert d.skip is False
    assert d.reason is None


def test_keep_60_pa_no_prior_year():
    # Current sample is sufficient on its own.
    d = should_skip_batter(season_pa=60, prior_year_pa=0)
    assert d.skip is False


def test_keep_10_pa_with_prior_year():
    # Vet hasn't played but has a track record.
    d = should_skip_batter(season_pa=10, prior_year_pa=600)
    assert d.skip is False


def test_skip_when_both_below_threshold():
    # Tiny prior-year sample (e.g., 10 PA cup of coffee) should NOT save them.
    d = should_skip_batter(season_pa=20, prior_year_pa=10)
    assert d.skip is True


# ---------------------------------------------------------------------------
# Integration: breakout enters baseline as additive bump, scaled by coefficient
# ---------------------------------------------------------------------------

def _common_pred_args() -> dict:
    """Minimal neutral context: park=1.0, pitcher=neutral, indoor (no env adjust)."""
    return dict(
        pitcher_hr_per_9=float("nan"),     # forces neutral pitcher_factor=1.0
        pitcher_hand_split_pa=0,
        park_hr_factor=1.0,
        temperature_f=70.0,
        wind_out_to_cf_mph=0.0,
        is_indoor=True,
        pa_per_game=4.2,
    )


def test_breakout_enters_as_additive_bump_with_coefficient_one():
    base = predict(blended_hr_per_pa=0.040, reliable_breakout=0.0, **_common_pred_args())
    bump = predict(blended_hr_per_pa=0.040, reliable_breakout=0.05, **_common_pred_args())
    # adjusted_per_pa should be 0.040 (no breakout) and 0.090 (with breakout).
    assert base.adjusted_per_pa == pytest.approx(0.040)
    assert bump.adjusted_per_pa == pytest.approx(0.090)
    # And p_hr should rise.
    assert bump.p_hr > base.p_hr


def test_breakout_coefficient_scales_bump():
    cfg_half = BaselineConfig(breakout_coefficient=0.5)
    p = predict(blended_hr_per_pa=0.040, reliable_breakout=0.05,
                **_common_pred_args(), config=cfg_half)
    # adjusted = 0.040 + 0.5 * 0.05 = 0.065
    assert p.adjusted_per_pa == pytest.approx(0.065)


def test_baseline_skips_when_blended_is_nan():
    p = predict(blended_hr_per_pa=float("nan"), reliable_breakout=0.0,
                **_common_pred_args())
    assert p.skipped is True
    assert "insufficient batter data" in p.skip_reason


def test_baseline_components_include_breakout_signal():
    p = predict(blended_hr_per_pa=0.040, reliable_breakout=0.05, **_common_pred_args())
    assert "breakout_signal" in p.components
    # multiplicative equivalent: 1 + (0.05 / 0.040) = 2.25
    assert p.components["breakout_signal"] == pytest.approx(1 + 0.05 / 0.040)
