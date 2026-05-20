"""
Pre-registered stat-sig evaluator. Run on day 30 (2026-06-18) against the
accumulated daily archives.

Hypotheses (pre-registered 2026-05-20 before any post-build data was collected
under the triple-filter regime):
  H1: ROI(triple)  > ROI(baseline) by >= 10pp
  H2: ROI(quad)    > ROI(triple)   by >= 8pp
  H3: ROI(dropped) < ROI(kept)     by >= 10pp     (validates triple filter logic)

Method: paired day-block bootstrap (resample DAYS with replacement, since picks
within a day are correlated through stacking). 10,000 resamples. Bonferroni
correction for the 3 tests: alpha = 0.05/3 = 0.0167.

A hypothesis is "supported" if the bootstrap distribution of (lhs - rhs - delta)
has its 1.67th percentile > 0.

Usage:
    python -m src.backtest.stat_sig_eval                    # full window
    python -m src.backtest.stat_sig_eval 2026-05-13         # explicit start
    python -m src.backtest.stat_sig_eval 2026-05-13 2026-06-18  # bounded window

If filter_status is missing on archived picks (data collected before the
2026-05-20 pipeline change), the evaluator computes it on the fly using the
current filters module.
"""

from __future__ import annotations

import json
import random
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

# Make this script runnable directly via -m without hard-coding sys.path
_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from src.pipeline.filters import (  # noqa: E402
    annotate_filter_status,
    passes_quad,
    passes_triple,
)


ARCHIVE_DIR = _BACKEND / "data" / "daily_archives"
POST_BUILD_START = "2026-05-13"
EVAL_DATE = "2026-06-18"

N_BOOTSTRAP = 10_000
ALPHA = 0.05
N_TESTS = 3
ALPHA_CORRECTED = ALPHA / N_TESTS  # Bonferroni


@dataclass
class SettledPick:
    date: str
    tier: str
    batter: str
    game_pk: int
    profit_units: float
    hit: bool
    passes_triple: bool
    passes_quad: bool


def _load_archive(path: Path) -> list[SettledPick]:
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    settlement = d.get("settlement") or {}
    if not settlement:
        return []
    picks: list[SettledPick] = []
    for tier in ("primary", "secondary", "shadow"):
        tier_picks = d.get(f"{tier}_picks", []) or []
        results = settlement.get(f"{tier}_results", []) or []
        results_by_key = {(r["batter_id"], r["game_pk"]): r for r in results}
        # Backfill filter_status for archives written before the 2026-05-20 change
        needs_backfill = any("filter_status" not in p for p in tier_picks)
        if needs_backfill:
            # `tier` field may not be on pick dicts in older archives — set it.
            for p in tier_picks:
                p.setdefault("tier", tier)
            annotate_filter_status(tier_picks)
        for p in tier_picks:
            r = results_by_key.get((p["batter_id"], p["game_pk"]))
            if r is None or r.get("void_reason"):
                continue
            fs = p.get("filter_status") or {}
            picks.append(SettledPick(
                date=d["date"],
                tier=tier,
                batter=p["batter"],
                game_pk=int(p["game_pk"]),
                profit_units=float(r["profit_units"]),
                hit=bool(r.get("actual_hr")),
                passes_triple=bool(fs.get("passes_triple", passes_triple(p))),
                passes_quad=bool(fs.get("passes_quad", passes_quad(p))),
            ))
    return picks


def load_picks(start: str, end: Optional[str] = None) -> list[SettledPick]:
    out: list[SettledPick] = []
    for path in sorted(ARCHIVE_DIR.glob("*.json")):
        date = path.stem
        if date < start:
            continue
        if end is not None and date > end:
            continue
        out.extend(_load_archive(path))
    return out


def _roi(picks: list[SettledPick]) -> float:
    if not picks:
        return 0.0
    return 100.0 * sum(p.profit_units for p in picks) / len(picks)


def _hit_rate(picks: list[SettledPick]) -> float:
    if not picks:
        return 0.0
    return 100.0 * sum(p.hit for p in picks) / len(picks)


def _bootstrap_paired_diff(
    by_date: dict[str, list[SettledPick]],
    lhs_filter: Callable[[SettledPick], bool],
    rhs_filter: Callable[[SettledPick], bool],
    delta_pp: float,
    n_iter: int = N_BOOTSTRAP,
    seed: int = 42,
) -> tuple[float, float, float, float]:
    """
    Returns (point_diff, ci_low, ci_high, p_supported).
    Bootstraps days with replacement and computes (ROI(lhs) - ROI(rhs) - delta) per resample.
    p_supported = fraction of resamples where the difference exceeds 0 (i.e., H is supported).
    The hypothesis is supported at corrected alpha if 1-p_supported < ALPHA_CORRECTED,
    equivalently the lower bound of the (1 - ALPHA_CORRECTED) CI exceeds 0.
    """
    rng = random.Random(seed)
    dates = sorted(by_date.keys())
    diffs = []
    for _ in range(n_iter):
        sample_dates = [rng.choice(dates) for _ in range(len(dates))]
        lhs_picks: list[SettledPick] = []
        rhs_picks: list[SettledPick] = []
        for d in sample_dates:
            day = by_date[d]
            lhs_picks.extend(p for p in day if lhs_filter(p))
            rhs_picks.extend(p for p in day if rhs_filter(p))
        diffs.append(_roi(lhs_picks) - _roi(rhs_picks) - delta_pp)
    diffs.sort()
    point = sum(diffs) / len(diffs) + delta_pp  # un-subtract delta for reporting
    # corrected one-sided lower bound at ALPHA_CORRECTED
    lo_idx = int(ALPHA_CORRECTED * n_iter)
    hi_idx = int((1 - ALPHA_CORRECTED) * n_iter)
    ci_low = diffs[lo_idx] + delta_pp
    ci_high = diffs[hi_idx] + delta_pp
    p_supported = sum(1 for x in diffs if x > 0) / n_iter
    return point, ci_low, ci_high, p_supported


def _summarize(picks: list[SettledPick], label: str) -> None:
    n = len(picks)
    if n == 0:
        print(f"  {label:24s}  n=0")
        return
    pnl = sum(p.profit_units for p in picks)
    hits = sum(p.hit for p in picks)
    print(
        f"  {label:24s}  n={n:4d}  hits={hits:3d}  hit%={100*hits/n:5.1f}  "
        f"P/L={pnl:+8.2f}u  ROI={100*pnl/n:+6.2f}%"
    )


def main(start: str = POST_BUILD_START, end: Optional[str] = None) -> None:
    picks = load_picks(start, end)
    if not picks:
        print(f"No settled picks in window {start}..{end or 'today'}")
        return

    dates = sorted({p.date for p in picks})
    by_date: dict[str, list[SettledPick]] = defaultdict(list)
    for p in picks:
        by_date[p.date].append(p)

    days_n = len(dates)
    print(f"Window: {dates[0]}..{dates[-1]}  ({days_n} days, {len(picks)} settled picks)")
    print(f"Bootstrap: {N_BOOTSTRAP} resamples, Bonferroni alpha = {ALPHA_CORRECTED:.4f} ({N_TESTS} tests)")
    print()

    baseline_picks = picks
    triple_picks = [p for p in picks if p.passes_triple]
    quad_picks = [p for p in picks if p.passes_quad]
    dropped_picks = [p for p in picks if not p.passes_triple]
    kept_picks = triple_picks

    print("POINT ESTIMATES")
    _summarize(baseline_picks, "baseline")
    _summarize(triple_picks, "triple (live)")
    _summarize(quad_picks, "quad (counterfactual)")
    _summarize(kept_picks, "kept by triple")
    _summarize(dropped_picks, "dropped by triple")
    print()

    if days_n < 14:
        print(f"WARNING: only {days_n} days. Bootstrap CIs will be wide. "
              f"Target N >= 30 days for confident decisions.")
        print()

    print("PRE-REGISTERED HYPOTHESIS TESTS")
    print("-" * 80)

    tests = [
        ("H1: ROI(triple)  > ROI(baseline) by >= 10pp",
         lambda p: p.passes_triple,
         lambda p: True,
         10.0),
        ("H2: ROI(quad)    > ROI(triple)   by >=  8pp",
         lambda p: p.passes_quad,
         lambda p: p.passes_triple,
         8.0),
        ("H3: ROI(kept)    > ROI(dropped)  by >= 10pp",
         lambda p: p.passes_triple,
         lambda p: not p.passes_triple,
         10.0),
    ]

    results = []
    for label, lhs, rhs, delta in tests:
        point, lo, hi, p_sup = _bootstrap_paired_diff(by_date, lhs, rhs, delta)
        supported = p_sup > (1 - ALPHA_CORRECTED)
        verdict = "SUPPORTED" if supported else "not yet supported"
        results.append((label, point, lo, hi, p_sup, supported))
        print(f"{label}")
        print(f"  point diff:                 {point:+6.2f}pp")
        print(f"  CI ({100*(1-ALPHA_CORRECTED):.2f}% one-sided): [{lo:+6.2f}pp, inf)")
        print(f"  P(supported in bootstrap):  {p_sup:.1%}")
        print(f"  Verdict: {verdict}")
        print()

    print("DECISION RULES")
    print("-" * 80)
    print("If H1 supported -> keep triple filter in production.")
    print("If H1 NOT supported AND H3 NOT supported -> revert to baseline (no filter).")
    print("If H2 supported -> upgrade to quad filter.")
    print("If H3 NOT supported -> investigate filter mechanics; the dropped picks")
    print("  aren't underperforming, so the filter is removing value.")
    print()
    print("RESULT SUMMARY")
    print("-" * 80)
    for label, point, lo, hi, p_sup, supported in results:
        flag = "[x]" if supported else "[ ]"
        print(f"  {flag} {label}  (p={p_sup:.1%})")


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else POST_BUILD_START
    end = sys.argv[2] if len(sys.argv) > 2 else None
    main(start, end)
