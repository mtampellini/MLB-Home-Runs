"""Filter tests — pure functions, no I/O.

Locks in the P3 drop-only filter (passes_triple_v2, calibration-v2, shipped
2026-06-23): it must be a strict SUBSET of passes_triple (drop-only — never
readmits), cut picks that only cleared the tier floor on the breakout boost,
and fall back to triple behaviour when the neutralized EV is unavailable.
"""

from src.pipeline.filters import (
    EV_CEILING_PCT,
    TIER_EV_MIN,
    annotate_filter_status,
    passes_triple,
    passes_triple_v2,
)


def _pick(**kw):
    """A primary-tier pick that passes the live triple by default."""
    base = {
        "tier": "primary",
        "ev_pct": 40.0,            # below the 50 ceiling, above the 25 floor
        "ev_pct_p3": 40.0,         # boost-off EV == real EV by default (no change)
        "model_prob": 0.18,
        "market_prob_devig": 0.12,
        "stacked": False,
        "top_3_features": [{"name": "pitcher", "value": 1.0}],  # outside the band
    }
    base.update(kw)
    return base


def test_passes_triple_baseline_pick():
    assert passes_triple(_pick()) is True
    assert passes_triple_v2(_pick()) is True


def test_v2_drops_pick_whose_floor_clearance_was_only_the_boost():
    # Real EV clears the 25 floor, but with the boost removed it falls below.
    p = _pick(ev_pct=30.0, ev_pct_p3=20.0)
    assert passes_triple(p) is True       # still passes the live triple
    assert passes_triple_v2(p) is False   # ...but v2 cuts it (boost-inflated)


def test_v2_is_strict_subset_never_readmits_ceiling_drop():
    # A pick the EV>=50 ceiling drops must STAY dropped even if removing the
    # boost pulls its neutralized EV down under 50.
    p = _pick(ev_pct=55.0, ev_pct_p3=30.0)
    assert passes_triple(p) is False
    assert passes_triple_v2(p) is False


def test_v2_does_not_drop_when_boost_off_ev_still_clears_floor():
    p = _pick(ev_pct=45.0, ev_pct_p3=28.0)  # both above the 25 floor
    assert passes_triple_v2(p) is True


def test_v2_falls_back_to_triple_when_no_neutralized_ev():
    p = _pick(ev_pct_p3=None)
    assert passes_triple_v2(p) is True
    # and never stricter than triple in the fallback: a ceiling-dropped pick
    # is still dropped because passes_triple already fails.
    assert passes_triple_v2(_pick(ev_pct=60.0, ev_pct_p3=None)) is False


def test_v2_stacked_shade_applies_to_neutralized_ev():
    # Stacked: shaded boost-off EV (ev_pct_p3 * 0.7) must clear the floor.
    tier_min = TIER_EV_MIN["primary"]
    # ev_pct_p3=30 -> shaded 21 < 25 floor -> drop
    assert passes_triple_v2(_pick(stacked=True, ev_pct_p3=30.0)) is False
    # ev_pct_p3=40 -> shaded 28 >= 25 floor -> keep (real ev also passes shade)
    assert passes_triple_v2(_pick(stacked=True, ev_pct=40.0, ev_pct_p3=40.0)) is True


def test_v2_shadow_tier_uses_lower_floor():
    # Shadow floor is 10; a boost-off EV of 12 clears it.
    p = _pick(tier="shadow", ev_pct=12.0, ev_pct_p3=12.0)
    assert passes_triple_v2(p) is True
    assert passes_triple_v2(_pick(tier="shadow", ev_pct=12.0, ev_pct_p3=8.0)) is False


def test_annotate_adds_v2_flag_and_subset_property():
    picks = [
        _pick(),                              # keeps both
        _pick(ev_pct=30.0, ev_pct_p3=20.0),   # triple yes, v2 no
        _pick(ev_pct=60.0, ev_pct_p3=60.0),   # ceiling drop, both no
    ]
    annotate_filter_status(picks)
    for p in picks:
        fs = p["filter_status"]
        assert "passes_triple_v2" in fs
        # subset invariant: v2 => triple
        if fs["passes_triple_v2"]:
            assert fs["passes_triple"]
    assert [p["filter_status"]["passes_triple_v2"] for p in picks] == [True, False, False]
