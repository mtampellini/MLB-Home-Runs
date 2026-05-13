"""Lock in the Bayesian blend formula.

Formula:
    blended = (prior_PA * prior_rate
               + season_PA * season_rate
               + recent_PA * recent_rate * shrinkage)
            / (prior_PA + season_PA + recent_PA * shrinkage)

Where:
  - season_PA / season_rate: PRE-30d in-season sample (disjoint with recent)
  - recent_PA / recent_rate: last-30-day sample
  - prior_PA / prior_rate:   per-player anchor; pass prior_pa=0 for none
  - shrinkage: weight multiplier on recent. Default 1.0.
"""

import math

import pytest

from src.features.blend import (
    DEFAULT_SHRINKAGE,
    PRIOR_PA_EQUIVALENT,
    bayesian_blend,
    blend_features,
)


def test_default_shrinkage_is_one():
    assert DEFAULT_SHRINKAGE == 1.0


def test_default_prior_pa_equivalent_is_one_hundred():
    assert PRIOR_PA_EQUIVALENT == 100


def test_pure_season_when_no_recent_and_no_prior():
    r = bayesian_blend(season_pa=400, season_rate=0.05, recent_pa=0, recent_rate=float("nan"))
    assert r.rate == pytest.approx(0.05)
    assert r.used_prior is False


def test_pure_recent_when_no_season_and_no_prior():
    # Mid-season rookie call-up: no pre-30d, recent has data, no anchor passed.
    r = bayesian_blend(season_pa=0, season_rate=float("nan"), recent_pa=80, recent_rate=0.07)
    assert r.rate == pytest.approx(0.07)


def test_shrinkage_default_one_is_simple_weighted_average():
    # With shrinkage=1.0 (default), the blend is the sample-size-weighted mean
    # of the two terms (no anchor).
    r = bayesian_blend(
        season_pa=100, season_rate=0.04,
        recent_pa=100, recent_rate=0.08,
    )
    # (100*0.04 + 100*0.08) / 200 = 0.06
    assert r.rate == pytest.approx(0.06)


def test_shrinkage_one_point_five_upweights_recent():
    # Caller can still ask for a recent-form premium.
    r = bayesian_blend(
        season_pa=100, season_rate=0.04,
        recent_pa=100, recent_rate=0.08,
        shrinkage=1.5,
    )
    # (100*0.04 + 100*0.08*1.5) / (100 + 100*1.5) = (4 + 12) / 250 = 0.064
    assert r.rate == pytest.approx(0.064)


def test_prior_anchor_shrinks_small_samples_toward_prior():
    # Tiny in-season sample + strong anchor → anchor wins.
    r = bayesian_blend(
        season_pa=20, season_rate=0.10,
        recent_pa=20, recent_rate=0.10,
        prior_pa=100, prior_rate=0.032,
    )
    # (100*0.032 + 20*0.10 + 20*0.10) / (100 + 20 + 20)
    # = (3.2 + 2 + 2) / 140 = 0.0514
    assert r.rate == pytest.approx(7.2 / 140)
    assert r.used_prior is True
    assert r.prior_pa == 100


def test_prior_anchor_overwhelmed_by_large_sample():
    # Plenty of in-season data → anchor's relative weight is small.
    r = bayesian_blend(
        season_pa=400, season_rate=0.08,
        recent_pa=100, recent_rate=0.08,
        prior_pa=100, prior_rate=0.032,
    )
    # (100*0.032 + 400*0.08 + 100*0.08) / (100 + 400 + 100)
    # = (3.2 + 32 + 8) / 600 = 0.0720
    assert r.rate == pytest.approx(43.2 / 600)
    # Still pulled slightly below the 0.08 sample rate.
    assert r.rate < 0.08


def test_zero_pa_everywhere_returns_nan():
    r = bayesian_blend(0, float("nan"), 0, float("nan"))
    assert math.isnan(r.rate)
    assert r.is_valid() is False


def test_nan_rate_drops_that_component():
    # Recent rate is NaN even though PA > 0 → recent term zeroed out.
    r = bayesian_blend(season_pa=400, season_rate=0.05, recent_pa=50, recent_rate=float("nan"))
    assert r.rate == pytest.approx(0.05)


def test_anchor_with_nan_rate_is_skipped():
    # If caller passes prior_rate=NaN (no per-player prior, no league fallback),
    # the anchor term contributes nothing.
    r = bayesian_blend(
        season_pa=100, season_rate=0.05,
        recent_pa=100, recent_rate=0.05,
        prior_pa=100, prior_rate=float("nan"),
    )
    assert r.rate == pytest.approx(0.05)
    assert r.used_prior is False


def test_blend_features_pulls_from_dicts_no_anchor():
    season = {"pa": 200, "hr_per_pa": 0.03}
    recent = {"pa": 60, "hr_per_pa": 0.05}
    r = blend_features(season, recent, metric_key="hr_per_pa", pa_key="pa")
    # Default shrinkage=1.0, no prior_rate: (200*0.03 + 60*0.05) / 260
    assert r.rate == pytest.approx((6 + 3) / 260)
    assert r.used_prior is False


def test_blend_features_with_prior_rate_passes_anchor():
    season = {"pa": 50, "hr_per_pa": 0.06}
    recent = {"pa": 50, "hr_per_pa": 0.06}
    r = blend_features(
        season, recent,
        prior_rate=0.04,
        metric_key="hr_per_pa", pa_key="pa",
    )
    assert r.used_prior is True
    # (100*0.04 + 50*0.06 + 50*0.06) / (100 + 50 + 50)
    # = (4 + 3 + 3) / 200 = 0.05
    assert r.rate == pytest.approx(10 / 200)


def test_blend_features_prior_rate_none_skips_anchor():
    season = {"pa": 100, "hr_per_pa": 0.05}
    recent = {"pa": 50, "hr_per_pa": 0.07}
    r = blend_features(season, recent, prior_rate=None, metric_key="hr_per_pa", pa_key="pa")
    # (100*0.05 + 50*0.07) / 150 = (5 + 3.5)/150
    assert r.rate == pytest.approx(8.5 / 150)
    assert r.used_prior is False


def test_blended_rate_bounded_by_inputs_no_anchor():
    # Sanity: blend can't escape [min(rate), max(rate)] across components.
    r = bayesian_blend(season_pa=300, season_rate=0.02, recent_pa=100, recent_rate=0.10)
    assert 0.02 <= r.rate <= 0.10


def test_blended_rate_bounded_when_anchor_present():
    r = bayesian_blend(
        season_pa=300, season_rate=0.02,
        recent_pa=100, recent_rate=0.10,
        prior_pa=100, prior_rate=0.04,
    )
    assert min(0.02, 0.04, 0.10) <= r.rate <= max(0.02, 0.04, 0.10)
