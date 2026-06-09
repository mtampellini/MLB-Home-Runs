"""
Post-build empirical filters derived from the 7-day 5/13-5/19 backtest.

Three filters are stacked into the "triple" combo (the production filter as of
2026-05-20). A "quad" combo adds a fourth filter and is tracked in parallel for
later validation but does NOT affect site output.

All picks pass through and are tagged with `filter_status`; the site picks.json
files include only `passes_triple=True` picks, while the daily archive retains
every pick for settlement and the day-30 stat-sig evaluation.

Filter rationale and parameters: see option_simulator.py backtest output.
Pre-registered hypotheses for the 2026-06-18 evaluation:
  H1: ROI(triple)  > ROI(baseline) by >= 10pp
  H2: ROI(quad)    > ROI(triple)   by >= 8pp
  H3: ROI(dropped) < ROI(kept)     by >= 10pp
"""

from __future__ import annotations

from typing import Iterable


STACKED_SHADE_FACTOR = 0.7
EV_CEILING_PCT = 50.0
PITCHER_FACTOR_BAND = (1.10, 1.45)
MODEL_PROB_BAND = (0.15, 0.25)

TIER_EV_MIN = {"primary": 25.0, "secondary": 25.0, "shadow": 10.0}

# ANCHOR view (added 2026-06-09). Encodes the over-prediction audit findings
# as a pick-level overlay, tracked in parallel exactly like quad was:
#   1. model/market ratio window — calibration gap scales with disagreement
#      (ratio <1.3 was -1.6pp / +7% ROI; >2.0 was -21pp / -52% ROI). The cap
#      kills the winner's-curse zone; the floor keeps a real model edge.
#   2. blended_hr_per_pa < 0.060 — the inflated hot-hitter tail (-16pp).
#   3. breakout_score < 0.10 — hot-streak chasing was net harmful (-7.9pp).
#   4. pitcher-factor band drop — shared with triple (validated OOS).
# Replaces QUAD in the tracker UI only. passes_quad tagging continues below —
# it is part of the pre-registered H2 evaluation on 2026-06-18 and must keep
# being recorded until then. ANCHOR is exploratory (parameters chosen on
# 5/13-6/08 data, robust across both halves) — a candidate for pre-registration
# at the 6/18 review, NOT a settled production filter.
ANCHOR_RATIO_BAND = (1.15, 1.60)
ANCHOR_BLENDED_MAX = 0.060
ANCHOR_BREAKOUT_MAX = 0.10


def _pitcher_factor(pick: dict) -> float:
    for f in pick.get("top_3_features") or []:
        if f.get("name") == "pitcher":
            return float(f.get("value", 1.0))
    return 1.0


def passes_triple(pick: dict) -> bool:
    if float(pick.get("ev_pct", 0.0)) >= EV_CEILING_PCT:
        return False
    pf = _pitcher_factor(pick)
    if PITCHER_FACTOR_BAND[0] <= pf < PITCHER_FACTOR_BAND[1]:
        return False
    if pick.get("stacked"):
        shaded = float(pick["ev_pct"]) * STACKED_SHADE_FACTOR
        tier_min = TIER_EV_MIN.get(pick.get("tier", "primary"), 25.0)
        if shaded < tier_min:
            return False
    return True


def passes_quad(pick: dict) -> bool:
    if not passes_triple(pick):
        return False
    mp = float(pick.get("model_prob", 0.0))
    if MODEL_PROB_BAND[0] <= mp < MODEL_PROB_BAND[1]:
        return False
    return True


def passes_anchor(pick: dict) -> bool:
    market = float(pick.get("market_prob_devig") or 0.0)
    if market <= 0:
        return False
    ratio = float(pick.get("model_prob", 0.0)) / market
    if not (ANCHOR_RATIO_BAND[0] <= ratio < ANCHOR_RATIO_BAND[1]):
        return False
    if float(pick.get("blended_hr_per_pa") or 0.0) >= ANCHOR_BLENDED_MAX:
        return False
    if float(pick.get("breakout_score") or 0.0) >= ANCHOR_BREAKOUT_MAX:
        return False
    pf = _pitcher_factor(pick)
    if PITCHER_FACTOR_BAND[0] <= pf < PITCHER_FACTOR_BAND[1]:
        return False
    return True


def annotate_filter_status(picks: Iterable[dict]) -> None:
    """Mutate each pick in place to add a filter_status dict."""
    for p in picks:
        p["filter_status"] = {
            "passes_baseline": True,
            "passes_triple": passes_triple(p),
            "passes_quad": passes_quad(p),
            "passes_anchor": passes_anchor(p),
        }


def kept_by(filter_name: str, picks: Iterable[dict]) -> list[dict]:
    """Return picks where filter_status[filter_name] is True. Tolerates missing tags."""
    out = []
    for p in picks:
        fs = p.get("filter_status") or {}
        if fs.get(filter_name, filter_name == "passes_baseline"):
            out.append(p)
    return out
