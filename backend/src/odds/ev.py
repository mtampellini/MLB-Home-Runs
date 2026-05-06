"""Expected-value math for HR Over 0.5 props.

Pure functions, no I/O. Three responsibilities:

1. American ↔ implied probability conversions.
2. Two-way de-vig (Over + Under) → fair market probability.
3. EV calculation given model probability and best available odds.

Convention: EV is expressed as **ROI percentage** — `+10.5` means a $100 wager
returns $10.50 profit on average, given the model is correctly calibrated.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# American odds <-> probability / decimal
# ---------------------------------------------------------------------------

def american_to_implied_prob(american: int) -> float:
    """American odds → implied probability (with vig)."""
    if american == 0:
        raise ValueError("american odds must be non-zero")
    if american > 0:
        return 100.0 / (american + 100.0)
    return -american / (-american + 100.0)


def american_to_decimal(american: int) -> float:
    """American odds → European decimal odds (e.g. +200 → 3.0, -150 → 1.667)."""
    if american == 0:
        raise ValueError("american odds must be non-zero")
    if american > 0:
        return american / 100.0 + 1.0
    return 100.0 / abs(american) + 1.0


def american_payout(american: int) -> float:
    """Net profit per $1 stake on a winning bet (decimal_odds - 1)."""
    return american_to_decimal(american) - 1.0


# ---------------------------------------------------------------------------
# De-vig
# ---------------------------------------------------------------------------

def devig_two_way(over_american: int, under_american: int) -> float:
    """Return the de-vigged fair probability of OVER, given both sides' prices.

    Formula: divide implied-prob(Over) by (implied-prob(Over) + implied-prob(Under)).
    Assumes the book's vig is symmetrically split across the two sides.
    """
    p_over = american_to_implied_prob(over_american)
    p_under = american_to_implied_prob(under_american)
    total = p_over + p_under
    if total <= 0:
        raise ValueError(f"non-positive implied total for ({over_american}, {under_american})")
    return p_over / total


def devig_consensus(quotes: list[tuple[int, int]]) -> float:
    """Average de-vigged Over probabilities across multiple (over, under) pairs.

    Useful when both FD and DK quote both sides — average gives a smoother
    'consensus fair line' less affected by one book's positioning.
    """
    if not quotes:
        raise ValueError("no quotes provided")
    fairs = [devig_two_way(o, u) for o, u in quotes]
    return sum(fairs) / len(fairs)


# ---------------------------------------------------------------------------
# EV
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EVResult:
    model_prob: float
    market_prob_devig: float
    over_american: int
    payout: float            # decimal_odds - 1
    ev_per_unit: float       # expected profit per $1 stake (signed)
    ev_pct: float            # ROI % — ev_per_unit * 100
    edge_pct: float          # model_prob - market_prob_devig, in pp

    def is_value(self, threshold_pct: float = 25.0) -> bool:
        return self.ev_pct >= threshold_pct


def ev_pct(model_prob: float, over_american: int, market_prob_devig: float) -> EVResult:
    """Compute EV given model P(HR), the OVER price we'd take, and the de-vigged market prob.

    EV per unit stake on a coin flip with P(win)=p and net payout=b:
        EV = p * b - (1 - p) * 1

    Returns the structured result so the caller can also surface edge / payout.
    """
    if not (0.0 <= model_prob <= 1.0):
        raise ValueError(f"model_prob must be in [0, 1], got {model_prob}")
    if math.isnan(market_prob_devig) or not (0.0 < market_prob_devig < 1.0):
        raise ValueError(f"market_prob_devig must be in (0, 1), got {market_prob_devig}")
    payout = american_payout(over_american)
    ev_unit = model_prob * payout - (1.0 - model_prob)
    return EVResult(
        model_prob=model_prob,
        market_prob_devig=market_prob_devig,
        over_american=over_american,
        payout=payout,
        ev_per_unit=ev_unit,
        ev_pct=ev_unit * 100.0,
        edge_pct=(model_prob - market_prob_devig) * 100.0,
    )


# ---------------------------------------------------------------------------
# Best-book selection
# ---------------------------------------------------------------------------

def best_over_book(prices: dict[str, int]) -> tuple[str, int]:
    """From {book_name: over_american}, return (best_book, best_price).

    'Best' = highest payout = highest American number for positive odds, OR the
    *least negative* for negative odds. Equivalent to max() since +310 > +290 > -150.
    """
    if not prices:
        raise ValueError("no prices to compare")
    best = max(prices.items(), key=lambda kv: kv[1])
    return best[0], best[1]


def best_payout_decimal(prices: dict[str, int]) -> float:
    """Return the decimal odds of the best (highest-payout) book in the input."""
    _, american = best_over_book(prices)
    return american_to_decimal(american)
