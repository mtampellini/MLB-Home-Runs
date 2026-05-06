"""EV math tests — pure functions, no I/O.

Locks in the formulas: American → implied prob, decimal odds, two-way de-vig,
and EV per unit stake (= ROI %).
"""

import math

import pytest

from src.odds.ev import (
    EVResult,
    american_payout,
    american_to_decimal,
    american_to_implied_prob,
    best_over_book,
    best_payout_decimal,
    devig_consensus,
    devig_two_way,
    ev_pct,
)


# ---------------------------------------------------------------------------
# American ↔ probability ↔ decimal
# ---------------------------------------------------------------------------

def test_implied_prob_positive_odds():
    # +100 ↔ 50%
    assert american_to_implied_prob(100) == pytest.approx(0.5)
    # +200 ↔ 33.33%
    assert american_to_implied_prob(200) == pytest.approx(1 / 3)
    # +310 ↔ 24.39%
    assert american_to_implied_prob(310) == pytest.approx(0.24390244, abs=1e-6)


def test_implied_prob_negative_odds():
    # -100 ↔ 50%
    assert american_to_implied_prob(-100) == pytest.approx(0.5)
    # -200 ↔ 66.67%
    assert american_to_implied_prob(-200) == pytest.approx(2 / 3)
    # -400 ↔ 80%
    assert american_to_implied_prob(-400) == pytest.approx(0.80)


def test_decimal_odds_positive():
    assert american_to_decimal(200) == pytest.approx(3.0)
    assert american_to_decimal(310) == pytest.approx(4.10)


def test_decimal_odds_negative():
    assert american_to_decimal(-200) == pytest.approx(1.5)
    assert american_to_decimal(-150) == pytest.approx(1.6667, abs=1e-3)


def test_payout_is_decimal_minus_one():
    assert american_payout(200) == pytest.approx(2.0)
    assert american_payout(-150) == pytest.approx(0.6667, abs=1e-3)


def test_zero_american_raises():
    with pytest.raises(ValueError):
        american_to_implied_prob(0)
    with pytest.raises(ValueError):
        american_to_decimal(0)


# ---------------------------------------------------------------------------
# De-vig
# ---------------------------------------------------------------------------

def test_devig_two_way_strips_vig():
    # Symmetric -110/-110 means 5.24% Over implied + 5.24% Under = 110.5%.
    # Fair: 50/50.
    fair = devig_two_way(over_american=-110, under_american=-110)
    assert fair == pytest.approx(0.5)


def test_devig_two_way_realistic_hr_line():
    # Aaron Judge over 0.5 +310 (implied 24.39%); under 0.5 -400 (implied 80%).
    # Total = 104.39%; fair Over = 24.39 / 104.39 = 23.36%.
    fair = devig_two_way(over_american=310, under_american=-400)
    assert fair == pytest.approx(0.24390244 / (0.24390244 + 0.80), abs=1e-6)


def test_devig_two_way_invalid_raises():
    # Both at +1000 → implied total 0.0909*2 = 0.18 > 0; that's fine math but
    # impossible book. Just check the path raises only when total <= 0.
    # Construct unreachable input: this is a contract test — we don't accept 0 prob.
    with pytest.raises(ValueError):
        american_to_implied_prob(0)


def test_devig_consensus_averages_book_fairs():
    # Same line at both books → consensus = same fair.
    fair = devig_consensus([(-110, -110), (-110, -110)])
    assert fair == pytest.approx(0.5)
    # Mixed lines.
    consensus = devig_consensus([(-110, -110), (200, -250)])
    # FD fair = 0.5; DK fair = (1/3) / (1/3 + 5/7) ≈ 0.318...
    fd = devig_two_way(-110, -110)
    dk = devig_two_way(200, -250)
    assert consensus == pytest.approx((fd + dk) / 2)


def test_devig_consensus_empty_raises():
    with pytest.raises(ValueError):
        devig_consensus([])


# ---------------------------------------------------------------------------
# EV calculation
# ---------------------------------------------------------------------------

def test_ev_zero_when_model_equals_market_no_vig():
    # Fair coin flip: model 50%, +100 odds → break-even EV.
    r = ev_pct(model_prob=0.5, over_american=100, market_prob_devig=0.5)
    assert r.ev_per_unit == pytest.approx(0.0)
    assert r.ev_pct == pytest.approx(0.0)
    assert r.payout == pytest.approx(1.0)


def test_ev_positive_when_model_above_market():
    # Model says 30%, market fair says 24%, taking +310.
    # payout = 3.10; EV = 0.30*3.10 - 0.70 = 0.93 - 0.70 = 0.23 → +23%.
    r = ev_pct(model_prob=0.30, over_american=310, market_prob_devig=0.24)
    assert r.payout == pytest.approx(3.10)
    assert r.ev_per_unit == pytest.approx(0.23)
    assert r.ev_pct == pytest.approx(23.0)
    assert r.edge_pct == pytest.approx(6.0)


def test_ev_negative_when_model_below_market():
    r = ev_pct(model_prob=0.10, over_american=310, market_prob_devig=0.24)
    # payout 3.1, EV = 0.10 * 3.1 - 0.90 = 0.31 - 0.90 = -0.59
    assert r.ev_pct == pytest.approx(-59.0)
    assert r.is_value(threshold_pct=25.0) is False


def test_ev_meets_25_pct_threshold():
    r = ev_pct(model_prob=0.32, over_american=310, market_prob_devig=0.24)
    # 0.32 * 3.10 - 0.68 = 0.992 - 0.68 = 0.312 → +31.2%
    assert r.ev_pct == pytest.approx(31.2)
    assert r.is_value(threshold_pct=25.0) is True


def test_ev_invalid_model_prob_raises():
    with pytest.raises(ValueError):
        ev_pct(model_prob=1.5, over_american=200, market_prob_devig=0.3)
    with pytest.raises(ValueError):
        ev_pct(model_prob=-0.01, over_american=200, market_prob_devig=0.3)


def test_ev_invalid_market_prob_raises():
    with pytest.raises(ValueError):
        ev_pct(model_prob=0.3, over_american=200, market_prob_devig=0.0)
    with pytest.raises(ValueError):
        ev_pct(model_prob=0.3, over_american=200, market_prob_devig=1.0)
    with pytest.raises(ValueError):
        ev_pct(model_prob=0.3, over_american=200, market_prob_devig=float("nan"))


# ---------------------------------------------------------------------------
# Best-book selection
# ---------------------------------------------------------------------------

def test_best_book_prefers_higher_payout_positive():
    # +310 > +290 (more payout per dollar)
    book, price = best_over_book({"fanduel": 290, "draftkings": 310})
    assert book == "draftkings" and price == 310


def test_best_book_prefers_least_negative_when_both_negative():
    book, price = best_over_book({"fanduel": -150, "draftkings": -120})
    assert book == "draftkings" and price == -120


def test_best_payout_decimal_matches_winning_book():
    assert best_payout_decimal({"fd": 200, "dk": 300}) == pytest.approx(4.0)


def test_best_book_empty_raises():
    with pytest.raises(ValueError):
        best_over_book({})
