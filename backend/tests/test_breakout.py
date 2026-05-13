"""Pre-Phase-3 review-gate tests.

Locks in the behaviours requested:
1. Breakout reliability scaling: raw * min(1, PA/100), clipped to ±0.15.
2. No-prior-year edge case → breakout = 0.
3. Skip logic: season_PA<50 AND no prior-year → SKIP, otherwise KEEP.

Plus integration sanity checks for blend + breakout interplay. The
dynamic prior-year weight (1 - PA/200) was replaced by a fixed-strength
per-player anchor in the blend; see test_blend.py.
"""

import math

import pytest

from src.features.blend import (
    bayesian_blend,
    blend_features,
)
from src.features.breakout import (
    BreakoutScore,
    DEFAULT_BREAKOUT_CAP,
    DEFAULT_BREAKOUT_WEIGHTS,
    RecentFormFlags,
    apply_reliability_and_cap,
    compute_breakout_score,
    compute_recent_form_flags,
)
from src.features.skip_logic import should_skip_batter
from src.model.baseline import BaselineConfig, predict


# ---------------------------------------------------------------------------
# 1. Breakout reliability scaling
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
    """Single-metric breakout. Default barrel_pct weight = 3.0; delta = 0.01
    → raw = 3.0 * 0.01 = 0.03. At PA=50 (reliability 0.5) → score = 0.015."""
    current = {"pa": 50, "barrel_pct": 0.11, "sweetspot_pct": 0.36,
               "pull_air_pct": 0.18, "max_ev": 110.0}
    prior = {"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
             "pull_air_pct": 0.18, "max_ev": 110.0}
    r = compute_breakout_score(current, prior)
    assert r.raw == pytest.approx(0.03)
    assert r.reliability == pytest.approx(0.5)
    assert r.score == pytest.approx(0.015)


def test_compute_breakout_score_default_weights_post_rebalance():
    """Sanity check on the scale-corrected weights:
    - barrel_pct (w=3.0) is the primary signal — biggest contribution per unit delta.
    - sweetspot, pull_air, max_ev contribute on smaller scales.
    Each test isolates ONE metric so we can read its contribution directly.
    Under the corrected weights, NONE of these realistic deltas saturate
    the cap — they're well inside the differentiating zone."""
    # barrel delta 0.02 × w=3.0 = 0.06 raw.
    only_barrel = compute_breakout_score(
        current={"pa": 200, "barrel_pct": 0.12, "sweetspot_pct": 0.36,
                 "pull_air_pct": 0.18, "max_ev": 110.0},
        prior_year={"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                    "pull_air_pct": 0.18, "max_ev": 110.0},
    )
    # sweetspot delta 0.05 × w=0.6 = 0.03 raw.
    only_ss = compute_breakout_score(
        current={"pa": 200, "barrel_pct": 0.10, "sweetspot_pct": 0.41,
                 "pull_air_pct": 0.18, "max_ev": 110.0},
        prior_year={"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                    "pull_air_pct": 0.18, "max_ev": 110.0},
    )
    # pull_air delta 0.03 × w=1.0 = 0.03 raw.
    only_pa = compute_breakout_score(
        current={"pa": 200, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                 "pull_air_pct": 0.21, "max_ev": 110.0},
        prior_year={"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                    "pull_air_pct": 0.18, "max_ev": 110.0},
    )
    # max_ev delta 1.5 × w=0.02 = 0.03 raw.
    only_ev = compute_breakout_score(
        current={"pa": 200, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                 "pull_air_pct": 0.18, "max_ev": 111.5},
        prior_year={"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                    "pull_air_pct": 0.18, "max_ev": 110.0},
    )
    assert only_barrel.raw == pytest.approx(0.06)   # barrel dominates per unit delta
    assert only_ss.raw == pytest.approx(0.03)
    assert only_pa.raw == pytest.approx(0.03)
    assert only_ev.raw == pytest.approx(0.03)
    # None saturate the cap — they're inside the differentiating zone.
    assert only_barrel.score == pytest.approx(0.06)
    assert only_ss.score == pytest.approx(0.03)
    # Sanity: an EXTREME barrel delta still hits the cap.
    extreme_barrel = compute_breakout_score(
        current={"pa": 200, "barrel_pct": 0.16, "sweetspot_pct": 0.36,
                 "pull_air_pct": 0.18, "max_ev": 110.0},
        prior_year={"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
                    "pull_air_pct": 0.18, "max_ev": 110.0},
    )
    # 0.06 delta × 3.0 = 0.18 raw → score clipped to +0.15.
    assert extreme_barrel.raw == pytest.approx(0.18)
    assert extreme_barrel.score == pytest.approx(0.15)


def test_compute_breakout_score_old_metrics_now_ignored():
    """The pre-rebalance metric set {xwobacon, hardhit_pct, avg_ev} is no longer
    in the default weights. A delta on those should produce zero breakout."""
    current = {"pa": 200, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
               "pull_air_pct": 0.18, "max_ev": 110.0,
               "xwobacon": 0.50, "hardhit_pct": 0.55, "avg_ev": 92.0}
    prior = {"pa": 600, "barrel_pct": 0.10, "sweetspot_pct": 0.36,
             "pull_air_pct": 0.18, "max_ev": 110.0,
             "xwobacon": 0.40, "hardhit_pct": 0.40, "avg_ev": 89.0}
    r = compute_breakout_score(current, prior)
    assert r.raw == pytest.approx(0.0)
    assert r.score == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 3. No-prior-year edge case
# ---------------------------------------------------------------------------

def test_breakout_score_returns_zero_when_no_prior_year():
    current = {"pa": 200, "barrel_pct": 0.15, "sweetspot_pct": 0.40,
               "pull_air_pct": 0.20, "max_ev": 112.0}
    r = compute_breakout_score(current, prior_year=None)
    assert r.score == 0.0
    assert r.raw == 0.0
    assert r.has_prior_year is False
    assert r.is_zero() is True


def test_breakout_score_returns_zero_when_prior_year_empty_dict():
    current = {"pa": 200, "barrel_pct": 0.15, "sweetspot_pct": 0.40,
               "pull_air_pct": 0.20, "max_ev": 112.0}
    r = compute_breakout_score(current, prior_year={})
    assert r.score == 0.0
    assert r.has_prior_year is False


def test_breakout_drops_component_with_nan_in_either_side():
    """If prior barrel_pct is NaN, that component contributes 0; others still count."""
    current = {"pa": 200, "barrel_pct": 0.12, "sweetspot_pct": 0.36,
               "pull_air_pct": 0.18, "max_ev": 110.0}
    prior = {"pa": 600, "barrel_pct": float("nan"), "sweetspot_pct": 0.36,
             "pull_air_pct": 0.18, "max_ev": 110.0}
    r = compute_breakout_score(current, prior)
    assert r.components["barrel_pct"] == 0.0   # NaN delta → no contribution
    # Other components also unchanged → total raw == 0
    assert r.raw == pytest.approx(0.0)
    assert r.score == pytest.approx(0.0)


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


def test_skip_60_pa_no_prior_year_career_too_small():
    """Post-Felix-Reyes: season=60 + prior=0 = career 60 < 200 → SKIP via
    LOW_CAREER_PA, even though season ≥ 50 clears the original gate.
    Statcast metrics aren't trustworthy at 60 career PAs."""
    d = should_skip_batter(season_pa=60, prior_year_pa=0)
    assert d.skip is True
    assert d.code == "LOW_CAREER_PA"


def test_keep_when_career_pa_clears_the_threshold():
    """season=100 + prior=110 = 210 → just over 200 → KEEP."""
    d = should_skip_batter(season_pa=100, prior_year_pa=110)
    assert d.skip is False


def test_skip_when_career_pa_just_under_threshold():
    """season=100 + prior=90 = 190 → SKIP via LOW_CAREER_PA."""
    d = should_skip_batter(season_pa=100, prior_year_pa=90)
    assert d.skip is True
    assert d.code == "LOW_CAREER_PA"


def test_career_pa_threshold_configurable():
    """Override career_pa_min to keep a 60-PA batter (e.g. for a relaxed mode)."""
    d = should_skip_batter(season_pa=60, prior_year_pa=0, career_pa_min=50)
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
# 5. Recent-form flags (trend_signal + unstable_recent)
# ---------------------------------------------------------------------------

def test_recent_form_default_weights_match_spec():
    """Confirm the post-rebalance default weights are exactly what the
    2026-05-06 review gate approved."""
    assert DEFAULT_BREAKOUT_WEIGHTS == {
        "barrel_pct":      3.0,
        "sweetspot_pct":   0.6,
        "pull_air_pct":    1.0,
        "max_ev":          0.02,
    }


def test_trend_signal_positive_when_recent_higher_than_season():
    season = {"barrel_pct": 0.10}
    recent = {"barrel_pct": 0.12}
    f = compute_recent_form_flags(season, recent)
    assert f.trend_signal == pytest.approx(0.20)         # +20% vs season
    assert f.unstable_recent is False                    # 1.2x is below the 1.5x threshold


def test_trend_signal_negative_when_cooling_off():
    season = {"barrel_pct": 0.10}
    recent = {"barrel_pct": 0.08}
    f = compute_recent_form_flags(season, recent)
    assert f.trend_signal == pytest.approx(-0.20)        # -20% vs season
    assert f.unstable_recent is False


def test_unstable_recent_fires_on_high_ratio():
    """recent / season >= 1.5 → unstable_recent=True."""
    season = {"barrel_pct": 0.10}
    recent = {"barrel_pct": 0.16}
    f = compute_recent_form_flags(season, recent)
    assert f.trend_signal == pytest.approx(0.60)
    assert f.unstable_recent is True


def test_unstable_recent_fires_on_low_ratio():
    """recent / season <= 0.5 → unstable_recent=True (cold spell)."""
    season = {"barrel_pct": 0.10}
    recent = {"barrel_pct": 0.04}
    f = compute_recent_form_flags(season, recent)
    assert f.trend_signal == pytest.approx(-0.60)
    assert f.unstable_recent is True


def test_unstable_recent_off_at_exactly_threshold_just_inside():
    """1.49x and 0.51x should NOT trigger; the threshold is inclusive at 1.5/0.5."""
    just_under_high = compute_recent_form_flags(
        {"barrel_pct": 0.10}, {"barrel_pct": 0.149},
    )
    just_above_low = compute_recent_form_flags(
        {"barrel_pct": 0.10}, {"barrel_pct": 0.051},
    )
    assert just_under_high.unstable_recent is False
    assert just_above_low.unstable_recent is False


def test_recent_form_returns_none_trend_when_season_missing():
    f = compute_recent_form_flags(None, {"barrel_pct": 0.12})
    assert f.trend_signal is None
    assert f.unstable_recent is False


def test_recent_form_returns_none_trend_when_season_zero():
    """Avoid division by zero. Cold-storage batter with 0 season barrel rate
    can't have a meaningful trend signal — return None, don't flag."""
    f = compute_recent_form_flags({"barrel_pct": 0.0}, {"barrel_pct": 0.10})
    assert f.trend_signal is None
    assert f.unstable_recent is False


def test_recent_form_returns_none_trend_when_recent_missing():
    f = compute_recent_form_flags({"barrel_pct": 0.10}, None)
    assert f.trend_signal is None
    assert f.unstable_recent is False


def test_recent_form_returns_none_trend_when_recent_nan():
    f = compute_recent_form_flags(
        {"barrel_pct": 0.10}, {"barrel_pct": float("nan")},
    )
    assert f.trend_signal is None
    assert f.unstable_recent is False


# ---------------------------------------------------------------------------
# Integration: breakout enters baseline as additive bump, scaled by coefficient
# ---------------------------------------------------------------------------

def _common_pred_args() -> dict:
    """Minimal neutral context: park=1.0, pitcher=neutral, indoor (no env adjust)."""
    return dict(
        pitcher_hr_per_9=float("nan"),     # forces neutral pitcher_factor=1.0
        pitcher_hand_split_pa=0,
        pitcher_split_pa=400.0,            # past shrinkage threshold → no pull
        park_hr_factor=1.0,
        temperature_f=70.0,
        wind_out_to_cf_mph=0.0,
        is_indoor=True,
        pa_per_game=4.2,
    )


def _shrinkage_pred_args(*, split_pa: float, hr_per_9: float, hand_split_pa: int) -> dict:
    """Args for testing the shrinkage formula end-to-end. ``split_pa`` is the
    PA on the matched platoon split that drives shrinkage; ``hand_split_pa``
    is the PA gating the platoon-split lookup itself (must be >=50 to read
    a non-neutral pitcher_factor)."""
    return dict(
        blended_hr_per_pa=0.040, reliable_breakout=0.0,
        pitcher_hr_per_9=hr_per_9, pitcher_hand_split_pa=hand_split_pa,
        pitcher_split_pa=split_pa,
        park_hr_factor=1.0, temperature_f=70.0, wind_out_to_cf_mph=0.0,
        is_indoor=True, pa_per_game=4.2,
    )


def test_breakout_enters_as_multiplicative_lift_with_coefficient_one():
    """Post-2026-05-06: breakout is a MULTIPLICATIVE lift, not an additive bump.
    blended × (1 + coefficient × score). Critical: low-skill batter does NOT
    get a giant bump from a maxed breakout score."""
    base = predict(blended_hr_per_pa=0.040, reliable_breakout=0.0, **_common_pred_args())
    bump = predict(blended_hr_per_pa=0.040, reliable_breakout=0.05, **_common_pred_args())
    # 0.040 × (1 + 0.05) = 0.042 — a +5% lift, NOT a +0.05 absolute bump.
    assert base.adjusted_per_pa == pytest.approx(0.040)
    assert bump.adjusted_per_pa == pytest.approx(0.042)
    assert bump.p_hr > base.p_hr


def test_breakout_lift_does_not_explode_low_skill_batters():
    """The whole point of multiplicative form: a maxed +0.15 breakout on a
    0.02 batter stays at 0.023, not 0.17."""
    p = predict(blended_hr_per_pa=0.020, reliable_breakout=0.15, **_common_pred_args())
    assert p.adjusted_per_pa == pytest.approx(0.020 * 1.15)
    assert p.adjusted_per_pa < 0.025                     # well clear of the 0.25 ceiling


def test_breakout_coefficient_scales_lift():
    cfg_half = BaselineConfig(breakout_coefficient=0.5)
    p = predict(blended_hr_per_pa=0.040, reliable_breakout=0.05,
                **_common_pred_args(), config=cfg_half)
    # 0.040 × (1 + 0.5 × 0.05) = 0.040 × 1.025 = 0.041
    assert p.adjusted_per_pa == pytest.approx(0.041)


def test_baseline_skips_when_blended_is_nan():
    p = predict(blended_hr_per_pa=float("nan"), reliable_breakout=0.0,
                **_common_pred_args())
    assert p.skipped is True
    assert "insufficient batter data" in p.skip_reason


def test_baseline_components_include_breakout_signal():
    p = predict(blended_hr_per_pa=0.040, reliable_breakout=0.05, **_common_pred_args())
    assert "breakout_signal" in p.components
    # Multiplicative form: 1 + lift = 1 + (1.0 × 0.05) = 1.05
    assert p.components["breakout_signal"] == pytest.approx(1.05)


# ---------------------------------------------------------------------------
# Pitcher-factor shrinkage by split-PA (was IP — see baseline.py history)
# ---------------------------------------------------------------------------

def test_pitcher_factor_unchanged_at_full_threshold():
    """At pitcher_shrinkage_split_pa (default 200 PA), no pull toward neutral."""
    # Disable the post-shrinkage clip so this test isolates shrinkage behaviour.
    cfg = BaselineConfig(pitcher_factor_clip=(0.0, 10.0))
    p = predict(**_shrinkage_pred_args(split_pa=200.0, hr_per_9=2.0, hand_split_pa=80),
                config=cfg)
    # Raw factor = (2.0 / 38) / 0.032 = 1.645. Weight = 1.0 → unchanged.
    assert p.components["pitcher"] == pytest.approx(1.6447, abs=1e-3)


def test_pitcher_factor_pulls_60pct_toward_neutral_at_40pct_threshold():
    """At 80 PA (40% of 200), shrinkage_weight=0.4 → factor = raw*0.4 + 1.0*0.6."""
    p = predict(**_shrinkage_pred_args(split_pa=80.0, hr_per_9=2.0, hand_split_pa=80))
    # raw = 1.645; shrunken = 1.645 × 0.4 + 1.0 × 0.6 = 0.658 + 0.6 = 1.258
    assert p.components["pitcher"] == pytest.approx(1.2579, abs=1e-3)


def test_pitcher_factor_clamps_at_full_above_threshold():
    """At 400 PA (past 200 threshold), shrinkage_weight clamps at 1.0 → factor unchanged."""
    p_at = predict(**_shrinkage_pred_args(split_pa=200.0, hr_per_9=2.0, hand_split_pa=80))
    p_above = predict(**_shrinkage_pred_args(split_pa=400.0, hr_per_9=2.0, hand_split_pa=80))
    assert p_at.components["pitcher"] == pytest.approx(p_above.components["pitcher"])


def test_pitcher_factor_pulled_to_one_at_zero_split_pa():
    """0 split-PA → weight=0 → factor = 1.0 regardless of raw."""
    p = predict(**_shrinkage_pred_args(split_pa=0.0, hr_per_9=2.0, hand_split_pa=80))
    assert p.components["pitcher"] == pytest.approx(1.0)


def test_shrinkage_disabled_when_threshold_zero():
    """Setting pitcher_shrinkage_split_pa=0 in config disables the dampener."""
    # Also disable the post-shrinkage clip so this test isolates the shrinkage path.
    cfg = BaselineConfig(pitcher_shrinkage_split_pa=0.0,
                         pitcher_factor_clip=(0.0, 10.0))
    p = predict(**_shrinkage_pred_args(split_pa=20.0, hr_per_9=2.0, hand_split_pa=80),
                config=cfg)
    # Raw factor full strength: (2.0 / 38) / 0.032 = 1.645
    assert p.components["pitcher"] == pytest.approx(1.6447, abs=1e-3)


def test_pitcher_factor_capped_by_clip():
    """Raw factor above config.pitcher_factor_clip[1] is capped post-shrinkage.

    Empirical: 5 days of paper-trade picks showed shrunk factors reaching 3.5
    on tiny-split-PA blowups. The clip is the safety rail for what shrinkage
    fails to tame.
    """
    # hr_per_9=3.0 → raw factor = 3.0/38/0.032 = 2.467. At 400 split_PA the
    # shrinkage weight clamps at 1.0 (no pull), so the unshrunk 2.467 would
    # be the output — except the clip should bring it back down to 1.6.
    p = predict(**_shrinkage_pred_args(split_pa=400.0, hr_per_9=3.0, hand_split_pa=80))
    assert p.components["pitcher"] == pytest.approx(1.6)
    # Sanity: a within-bounds factor passes through unclipped.
    p_low = predict(**_shrinkage_pred_args(split_pa=400.0, hr_per_9=1.5, hand_split_pa=80))
    assert p_low.components["pitcher"] == pytest.approx(1.5 / 38 / 0.032, abs=1e-3)


def test_compute_slate_league_hr_per_pa_aggregates_when_sample_sufficient():
    from src.model.baseline import compute_slate_league_hr_per_pa
    # 8 batters × 200 PA = 1600 PA; 50 HR total → 0.03125 HR/PA.
    season_recs = [
        {"hr": 6, "pa": 200}, {"hr": 8, "pa": 200},
        {"hr": 4, "pa": 200}, {"hr": 7, "pa": 200},
        {"hr": 5, "pa": 200}, {"hr": 9, "pa": 200},
        {"hr": 6, "pa": 200}, {"hr": 5, "pa": 200},
    ]
    rate = compute_slate_league_hr_per_pa(season_recs)
    assert rate == pytest.approx(50.0 / 1600.0, abs=1e-6)


def test_compute_slate_league_hr_per_pa_falls_back_when_sample_thin():
    """Opening-day case: not enough cumulative PA → use the LEAGUE_HR_PER_PA_DEFAULT."""
    from src.model.baseline import (
        LEAGUE_HR_PER_PA_DEFAULT, compute_slate_league_hr_per_pa,
    )
    # 5 batters × 50 PA = 250 PA, well below the 1000 floor.
    season_recs = [{"hr": 1, "pa": 50}] * 5
    rate = compute_slate_league_hr_per_pa(season_recs)
    assert rate == LEAGUE_HR_PER_PA_DEFAULT


def test_compute_slate_league_hr_per_pa_skips_invalid_records():
    from src.model.baseline import compute_slate_league_hr_per_pa
    # Mix of valid + None / zero-PA / missing records — only the valid ones count.
    season_recs = [
        {"hr": 50, "pa": 1500},   # 0.0333
        None,
        {"hr": 0, "pa": 0},
        {},                       # missing keys
    ]
    rate = compute_slate_league_hr_per_pa(season_recs)
    assert rate == pytest.approx(50.0 / 1500.0, abs=1e-6)


def test_shrinkage_neutral_factor_unchanged():
    """A pitcher who's already league-average (factor=1.0) is unchanged by shrinkage."""
    # HR/9 such that raw factor = 1.0:  (1.0 × 38) / 38 = 1.0 means hr_per_pa=0.032,
    # so hr_per_9 = 0.032 × 38 = 1.216
    p = predict(**_shrinkage_pred_args(split_pa=40.0, hr_per_9=1.216, hand_split_pa=80))
    assert p.components["pitcher"] == pytest.approx(1.0, abs=1e-3)
