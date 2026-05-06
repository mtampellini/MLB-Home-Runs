"""Lock in the Bayesian blend formula.

Formula:
    blended = (season_PA * season_rate + recent_PA * recent_rate * shrinkage)
              / (season_PA + recent_PA * shrinkage)

Plus early-season prior-year fallback when season_PA < threshold.
"""

import math

import pytest

from src.features.blend import (
    DEFAULT_SHRINKAGE,
    bayesian_blend,
    blend_features,
)


def test_pure_season_when_no_recent():
    r = bayesian_blend(season_pa=400, season_rate=0.05, recent_pa=0, recent_rate=float("nan"))
    assert r.rate == pytest.approx(0.05)
    assert r.used_prior_year is False


def test_pure_recent_when_no_season():
    # Mid-season rookie call-up: season_pa=0, recent has data, no prior year.
    r = bayesian_blend(season_pa=0, season_rate=float("nan"), recent_pa=80, recent_rate=0.07)
    assert r.rate == pytest.approx(0.07)


def test_shrinkage_upweights_recent():
    # Equal PA, different rates. With shrinkage=1.5, recent should pull harder.
    r = bayesian_blend(
        season_pa=100, season_rate=0.04,
        recent_pa=100, recent_rate=0.08,
        shrinkage=1.5,
    )
    # Manual: (100*0.04 + 100*0.08*1.5) / (100 + 100*1.5)
    #       = (4 + 12) / 250 = 0.064
    assert r.rate == pytest.approx(0.064)


def test_shrinkage_one_equals_simple_weighted_average():
    r = bayesian_blend(
        season_pa=300, season_rate=0.03,
        recent_pa=100, recent_rate=0.06,
        shrinkage=1.0,
    )
    # (300*0.03 + 100*0.06) / 400 = (9 + 6) / 400 = 0.0375
    assert r.rate == pytest.approx(0.0375)


def test_default_shrinkage_is_one_point_five():
    assert DEFAULT_SHRINKAGE == 1.5


def test_zero_pa_everywhere_returns_nan():
    r = bayesian_blend(0, float("nan"), 0, float("nan"))
    assert math.isnan(r.rate)
    assert r.is_valid() is False


def test_nan_rate_drops_that_component():
    # Recent rate is NaN even though PA > 0 → recent term zeroed out.
    r = bayesian_blend(season_pa=400, season_rate=0.05, recent_pa=50, recent_rate=float("nan"))
    assert r.rate == pytest.approx(0.05)


def test_early_season_uses_prior_year():
    # April game: season_pa=40 (below threshold=100), recent has some data,
    # prior year has a full season → prior year should be folded in.
    r = bayesian_blend(
        season_pa=40, season_rate=0.06,
        recent_pa=40, recent_rate=0.06,
        prior_year_pa=600, prior_year_rate=0.04,
        prior_year_weight=0.5,
        early_season_pa_threshold=100,
    )
    assert r.used_prior_year is True
    # Manual: numerator = 40*0.06 + 40*0.06*1.5 + 600*0.04*0.5
    #                   = 2.4 + 3.6 + 12 = 18
    #         denom     = 40 + 40*1.5 + 600*0.5 = 40 + 60 + 300 = 400
    #         rate = 18 / 400 = 0.045
    assert r.rate == pytest.approx(0.045)


def test_mid_season_does_not_use_prior_year():
    r = bayesian_blend(
        season_pa=400, season_rate=0.05,
        recent_pa=80, recent_rate=0.07,
        prior_year_pa=600, prior_year_rate=0.04,
        prior_year_weight=0.5,
        early_season_pa_threshold=100,
    )
    assert r.used_prior_year is False
    # Manual: (400*0.05 + 80*0.07*1.5) / (400 + 80*1.5)
    #       = (20 + 8.4) / 520 = 0.054615...
    assert r.rate == pytest.approx(28.4 / 520)


def test_blend_features_pulls_from_dicts():
    season = {"pa": 200, "hr_per_pa": 0.03}
    recent = {"pa": 60, "hr_per_pa": 0.05}
    r = blend_features(season, recent, metric_key="hr_per_pa", pa_key="pa", shrinkage=1.5)
    # (200*0.03 + 60*0.05*1.5) / (200 + 60*1.5)
    # = (6 + 4.5) / 290 = 0.036206...
    assert r.rate == pytest.approx(10.5 / 290)


def test_blend_features_with_prior_year():
    season = {"pa": 50, "hr_per_pa": 0.06}
    recent = {"pa": 50, "hr_per_pa": 0.06}
    py = {"pa": 600, "hr_per_pa": 0.04}
    r = blend_features(season, recent, prior_year=py, metric_key="hr_per_pa", pa_key="pa")
    assert r.used_prior_year is True


def test_blended_rate_bounded_by_inputs():
    # Sanity: blend can't escape [min(rate), max(rate)] across components.
    r = bayesian_blend(season_pa=300, season_rate=0.02, recent_pa=100, recent_rate=0.10)
    assert 0.02 <= r.rate <= 0.10
