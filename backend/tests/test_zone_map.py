"""Zone search — metrics, multiple-comparison control, and split hygiene."""

import math

import pytest

from src.backtest.zone_map import (
    MIN_EVENTS,
    ZONES,
    benjamini_hochberg,
    render_comparison,
    search,
    time_split,
    zone_stat,
)


def _row(model_prob, market, label, date="2026-07-01", **extra):
    r = {
        "model_prob": model_prob,
        "market_prob_devig": market,
        "label": label,
        "slate_date": date,
    }
    r.update(extra)
    return r


# ---------------------------------------------------------------------------
# Skill metric
# ---------------------------------------------------------------------------

def test_better_forecast_scores_positive_skill():
    """Model says 90% on events that all happen; market says 10%."""
    rows = [_row(0.9, 0.1, 1) for _ in range(60)]
    s = zone_stat("t", rows)
    assert s.skill > 0
    assert s.p_value < 0.05


def test_worse_forecast_scores_negative_skill():
    rows = [_row(0.1, 0.9, 1) for _ in range(60)]
    assert zone_stat("t", rows).skill < 0


def test_identical_forecasts_score_zero_skill():
    rows = [_row(0.3, 0.3, i % 2) for i in range(60)]
    s = zone_stat("t", rows)
    assert s.skill == pytest.approx(0.0, abs=1e-9)


def test_confident_miss_does_not_produce_infinity():
    """Without clipping, log(0) makes the whole zone unusable."""
    s = zone_stat("t", [_row(1.0, 0.0, 0) for _ in range(30)])
    assert s.skill is not None
    assert math.isfinite(s.skill)


def test_calibration_drift():
    rows = [_row(0.5, 0.5, 1) for _ in range(25)] + [_row(0.5, 0.5, 0) for _ in range(75)]
    s = zone_stat("t", rows)
    assert s.actual == pytest.approx(0.25)
    assert s.predicted == pytest.approx(0.50)
    assert s.drift_pp == pytest.approx(25.0)     # over-predicts by 25pp


# ---------------------------------------------------------------------------
# Power gating
# ---------------------------------------------------------------------------

def test_thin_zone_is_underpowered_not_null():
    """'No signal' and 'no data' must not look alike."""
    s = zone_stat("t", [_row(0.3, 0.3, 1) for _ in range(MIN_EVENTS - 1)])
    assert s.underpowered is True
    assert s.beats_market is False


def test_powered_zone_is_evaluated():
    s = zone_stat("t", [_row(0.3, 0.3, 1) for _ in range(MIN_EVENTS)])
    assert s.underpowered is False


# ---------------------------------------------------------------------------
# Multiple comparisons
# ---------------------------------------------------------------------------

def test_bh_inflates_q_above_p():
    stats = [zone_stat(f"z{i}", [_row(0.9, 0.1, 1) for _ in range(60)]) for i in range(10)]
    for s in stats:
        s.p_value = 0.04
    benjamini_hochberg(stats)
    assert all(s.q_value >= 0.04 for s in stats)
    assert max(s.q_value for s in stats) > 0.04


def test_bh_ignores_underpowered_zones():
    """Underpowered zones must not pad the denominator and soften the correction."""
    powered = [zone_stat(f"p{i}", [_row(0.9, 0.1, 1) for _ in range(60)]) for i in range(2)]
    thin = [zone_stat(f"t{i}", [_row(0.9, 0.1, 1) for _ in range(3)]) for i in range(20)]
    for s in powered:
        s.p_value = 0.01
    benjamini_hochberg(powered + thin)
    assert all(s.q_value is None for s in thin)
    assert max(s.q_value for s in powered) <= 0.02      # m=2, not m=22


def test_bh_is_monotonic():
    stats = [zone_stat(f"z{i}", [_row(0.9, 0.1, 1) for _ in range(60)]) for i in range(5)]
    for s, p in zip(stats, [0.001, 0.01, 0.02, 0.03, 0.04]):
        s.p_value = p
    benjamini_hochberg(stats)
    q = [s.q_value for s in sorted(stats, key=lambda x: x.p_value)]
    assert q == sorted(q)


def test_beats_market_requires_significance_and_direction():
    s = zone_stat("t", [_row(0.9, 0.1, 1) for _ in range(60)])
    s.q_value = 0.01
    assert s.beats_market is True
    s.q_value = 0.20
    assert s.beats_market is False
    s.q_value, s.skill = 0.01, -0.5
    assert s.beats_market is False


# ---------------------------------------------------------------------------
# Time split
# ---------------------------------------------------------------------------

def test_time_split_never_straddles_a_day():
    """Same-day rows share weather, parks and pitchers — they must not leak."""
    rows = ([_row(0.2, 0.2, 0, "2026-07-01") for _ in range(50)]
            + [_row(0.2, 0.2, 0, "2026-07-02") for _ in range(50)]
            + [_row(0.2, 0.2, 0, "2026-07-03") for _ in range(50)])
    a, b = time_split(rows, 0.6)
    assert set(r["slate_date"] for r in a).isdisjoint(
        set(r["slate_date"] for r in b))


def test_time_split_is_chronological():
    rows = [_row(0.2, 0.2, 0, f"2026-07-{d:02d}") for d in range(1, 11)]
    a, b = time_split(rows, 0.6)
    assert max(r["slate_date"] for r in a) < min(r["slate_date"] for r in b)


def test_time_split_single_day_yields_empty_holdout():
    rows = [_row(0.2, 0.2, 0, "2026-07-01") for _ in range(10)]
    a, b = time_split(rows)
    assert len(a) == 10 and b == []


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def test_search_bins_by_declared_dimensions():
    rows = ([_row(0.05, 0.05, 0) for _ in range(40)]
            + [_row(0.30, 0.30, 1) for _ in range(40)])
    rep = search(rows)
    labels = [z.label for z in rep.zones]
    assert any("model_prob 0..0.08" in x for x in labels)
    assert any("model_prob 0.22..1.01" in x for x in labels)


def test_search_handles_missing_features_without_crashing():
    """Most rows lack `components`; those dimensions should just be skipped."""
    rows = [_row(0.2, 0.2, i % 3 == 0) for i in range(90)]
    rep = search(rows)
    assert rep.overall.n == 90
    assert any("model_prob" in z.label for z in rep.zones)


def test_search_reads_component_dimensions():
    rows = [_row(0.2, 0.2, i % 4 == 0, components={"pitcher": 1.2, "park": 1.0})
            for i in range(120)]
    rep = search(rows)
    assert any("pitcher factor 1.1..1.45" in z.label for z in rep.zones)


def test_search_on_empty_input():
    rep = search([])
    assert rep.overall.n == 0
    assert rep.zones == []


def test_rows_without_labels_are_excluded():
    rows = [_row(0.2, 0.2, 1) for _ in range(30)]
    rows.append({"model_prob": 0.2, "market_prob_devig": 0.2, "slate_date": "2026-07-01"})
    assert search(rows).overall.n == 30


# ---------------------------------------------------------------------------
# Holdout verdict
# ---------------------------------------------------------------------------

def test_no_explore_winners_reads_as_a_clean_null():
    rows = [_row(0.3, 0.3, i % 3 == 0) for i in range(120)]
    e, h = search(rows, "explore"), search(rows, "holdout")
    assert "clean null" in render_comparison(e, h)


def test_winner_that_fails_holdout_is_reported_as_such():
    good = [_row(0.9, 0.1, 1, "2026-07-01") for _ in range(80)]
    bad = [_row(0.9, 0.1, 0, "2026-08-01") for _ in range(80)]
    e, h = search(good, "explore"), search(bad, "holdout")
    out = render_comparison(e, h)
    assert "REPLICATED" not in out
    assert "did NOT replicate" in out or "underpowered" in out


def test_winner_that_survives_is_reported_as_replicated():
    good = [_row(0.9, 0.1, 1, "2026-07-01") for _ in range(80)]
    also = [_row(0.9, 0.1, 1, "2026-08-01") for _ in range(80)]
    out = render_comparison(search(good, "explore"), search(also, "holdout"))
    assert "REPLICATED" in out


def test_zone_definitions_are_frozen_tuples():
    """Bin edges must not be mutable — post-hoc edits are the whole risk."""
    assert isinstance(ZONES, tuple)
    for d in ZONES:
        assert isinstance(d.edges, tuple)
        assert list(d.edges) == sorted(d.edges)
