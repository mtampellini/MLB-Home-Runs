"""Where does the model actually know something the market doesn't?

WHAT THIS IS
    A read-only zone search over a LABELED frame. For each pre-declared region
    of feature space it answers two questions:

      1. Is the model calibrated there?   predicted HR rate vs actual
      2. Does it beat the market there?   log-loss(market) - log-loss(model)

    Metric 2 is the point. It compares two forecasts of the same events on the
    same rows, so it needs no assumption that the closing line is efficient and
    no bet to be placed. It also converges far faster than ROI, which at +400
    needs thousands of settled picks per zone to separate +10% from -10%.

WHY IT EXISTS
    The production stack (25% EV floor, top-10 by edge, EV ceiling, pitcher-
    factor band, stacked shade, P3) accreted one patch at a time, and every
    layer was fit on PICKS — the sample the 2026-06-11 P1 backtest found carries
    zero incremental signal conditional on selection. This module starts from
    the unbiased full-slate frame instead and lets the data say where the edge
    is, rather than defending inherited thresholds.

    Feed it `results.full_slate_outcomes.build_labeled_dataset()`.

THE DISCIPLINE (this is the whole design, not decoration)
    Searching ~10k rows across many slices WILL surface green zones. Most will
    be noise. A +0.64pp CLV result on 390 picks looked real, significant and
    mechanistically plausible in July 2026, and evaporated the moment it was
    checked against another period. So:

    * Zones are PRE-DECLARED in ZONES below. Bin edges are frozen in code, not
      chosen after seeing outcomes. Post-hoc bin boundaries are where most false
      discoveries live.
    * Every run is TIME-SPLIT. Explore on the earlier window, hold out the
      later one. A zone that only works in-sample is reported as failed.
    * Multiple comparisons are CORRECTED (Benjamini-Hochberg FDR) and the
      number of zones tested is always printed. Twenty zones at p<0.05 buys you
      one false positive for free.
    * Zones below MIN_EVENTS are reported as underpowered, never as null
      results. "No signal" and "no data" are different answers.

POWER
    ~10k rows at a ~12% base rate is ~1,200 home runs. That supports a handful
    of coarse zones with real events in each — not fifty fine ones. If a zone
    comes back with 30 events, treat it as a hypothesis, not a finding.

USAGE
    python -m src.backtest.zone_map                  # default 60/40 time split
    python -m src.backtest.zone_map --split 0.5
    python -m src.backtest.zone_map --require-odds   # rows with a real price only
"""

from __future__ import annotations

import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# Below this many HR events a zone is underpowered, not null.
MIN_EVENTS = 25

# Probabilities are clipped before log-loss so a confident miss can't return inf.
_EPS = 1e-6


# ---------------------------------------------------------------------------
# Pre-declared zones — EDIT DELIBERATELY, NOT AFTER SEEING RESULTS
# ---------------------------------------------------------------------------

def _component(name: str) -> Callable[[dict], Optional[float]]:
    def get(r: dict) -> Optional[float]:
        c = r.get("components") or {}
        v = c.get(name)
        return float(v) if v is not None else None
    return get


def _field(name: str) -> Callable[[dict], Optional[float]]:
    def get(r: dict) -> Optional[float]:
        v = r.get(name)
        return float(v) if v is not None else None
    return get


def _ratio(r: dict) -> Optional[float]:
    m, k = r.get("model_prob"), r.get("market_prob_devig")
    if m is None or k is None or float(k) <= 0:
        return None
    return float(m) / float(k)


@dataclass(frozen=True)
class Dimension:
    name: str
    getter: Callable[[dict], Optional[float]]
    edges: tuple[float, ...]


ZONES: tuple[Dimension, ...] = (
    Dimension("model_prob", _field("model_prob"),
              (0.0, 0.08, 0.12, 0.16, 0.22, 1.01)),
    Dimension("model/market ratio", _ratio,
              (0.0, 0.9, 1.15, 1.4, 1.8, 99.0)),
    Dimension("batter skill", _component("batter_skill"),
              (0.0, 0.8, 1.1, 1.5, 99.0)),
    Dimension("breakout signal", _component("breakout_signal"),
              (0.0, 0.95, 1.0, 1.05, 99.0)),
    Dimension("pitcher factor", _component("pitcher"),
              (0.0, 0.9, 1.1, 1.45, 99.0)),
    Dimension("park factor", _component("park"),
              (0.0, 0.95, 1.05, 99.0)),
    Dimension("price", _field("best_price"),
              (-1e9, 300, 500, 700, 900, 1e9)),
    Dimension("lineup spot", _field("lineup_spot"),
              (0.5, 3.5, 6.5, 9.5)),
)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _logloss(p: float, y: int) -> float:
    p = min(max(p, _EPS), 1 - _EPS)
    return -(math.log(p) if y else math.log(1 - p))


@dataclass
class ZoneStat:
    label: str
    n: int
    events: int
    actual: float
    predicted: float
    drift_pp: float                  # predicted - actual, in percentage points
    skill: Optional[float]           # mean logloss(market) - logloss(model)
    skill_ci: Optional[tuple[float, float]]
    p_value: Optional[float]
    underpowered: bool
    q_value: Optional[float] = None  # BH-adjusted

    @property
    def beats_market(self) -> bool:
        return (not self.underpowered and self.skill is not None
                and self.q_value is not None and self.q_value < 0.05
                and self.skill > 0)


def _normal_sf(z: float) -> float:
    """Two-sided p-value from a z-score (erfc, no scipy dependency)."""
    return math.erfc(abs(z) / math.sqrt(2))


def zone_stat(label: str, rows: list[dict]) -> ZoneStat:
    """Calibration + market-relative skill for one zone.

    Skill is a PAIRED comparison: both forecasts score the same events on the
    same rows, so the per-row log-loss difference is the unit of analysis and a
    paired t-statistic is the right test.
    """
    n = len(rows)
    events = sum(int(r["label"]) for r in rows)
    actual = events / n if n else 0.0
    preds = [float(r["model_prob"]) for r in rows if r.get("model_prob") is not None]
    predicted = statistics.mean(preds) if preds else 0.0

    # `is not None`, NOT truthiness: a probability of 0.0 is a real forecast
    # (and a maximally wrong one), so testing truthiness would silently drop
    # exactly the rows that matter most to the comparison.
    diffs = [
        _logloss(float(r["market_prob_devig"]), int(r["label"]))
        - _logloss(float(r["model_prob"]), int(r["label"]))
        for r in rows
        if r.get("market_prob_devig") is not None and r.get("model_prob") is not None
    ]

    skill = ci = p = None
    if len(diffs) >= 2:
        skill = statistics.mean(diffs)
        sd = statistics.stdev(diffs)
        if sd > 0:
            se = sd / math.sqrt(len(diffs))
            ci = (skill - 1.96 * se, skill + 1.96 * se)
            p = _normal_sf(skill / se)
        else:
            ci, p = (skill, skill), 0.0

    return ZoneStat(
        label=label, n=n, events=events, actual=actual, predicted=predicted,
        drift_pp=(predicted - actual) * 100,
        skill=skill, skill_ci=ci, p_value=p,
        underpowered=events < MIN_EVENTS,
    )


def benjamini_hochberg(stats: list[ZoneStat], alpha: float = 0.05) -> None:
    """Assign q-values in place. Only powered zones enter the correction —
    including underpowered ones would inflate the denominator and make the
    correction look kinder than it is."""
    testable = [s for s in stats if not s.underpowered and s.p_value is not None]
    m = len(testable)
    if not m:
        return
    for rank, s in enumerate(sorted(testable, key=lambda x: x.p_value), start=1):
        s.q_value = min(1.0, s.p_value * m / rank)
    # Enforce monotonicity (a q-value may not exceed a higher-ranked one).
    running = 1.0
    for s in sorted(testable, key=lambda x: x.p_value, reverse=True):
        running = min(running, s.q_value)
        s.q_value = running


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@dataclass
class ZoneReport:
    window: str
    date_min: Optional[str]
    date_max: Optional[str]
    overall: ZoneStat
    zones: list[ZoneStat] = field(default_factory=list)
    n_tested: int = 0


def _bin_label(dim: Dimension, lo: float, hi: float) -> str:
    def fmt(v: float) -> str:
        if abs(v) >= 1e8:
            return "inf"
        return f"{v:g}"
    return f"{dim.name} {fmt(lo)}..{fmt(hi)}"


def search(rows: list[dict], window: str = "all") -> ZoneReport:
    """Run every pre-declared zone over `rows`."""
    usable = [r for r in rows
              if r.get("label") is not None and r.get("model_prob") is not None]
    dates = [r.get("slate_date") for r in usable if r.get("slate_date")]
    rep = ZoneReport(
        window=window,
        date_min=min(dates) if dates else None,
        date_max=max(dates) if dates else None,
        overall=zone_stat("ALL ROWS", usable) if usable else zone_stat("ALL ROWS", []),
    )
    if not usable:
        return rep

    for dim in ZONES:
        for lo, hi in zip(dim.edges, dim.edges[1:]):
            sub = []
            for r in usable:
                v = dim.getter(r)
                if v is not None and lo <= v < hi:
                    sub.append(r)
            if sub:
                rep.zones.append(zone_stat(_bin_label(dim, lo, hi), sub))

    benjamini_hochberg(rep.zones)
    rep.n_tested = sum(1 for z in rep.zones if not z.underpowered)
    return rep


def time_split(rows: list[dict], fraction: float = 0.6) -> tuple[list[dict], list[dict]]:
    """Split chronologically by slate_date. Earlier -> explore, later -> holdout.

    Splitting on the date boundary (not the row index) keeps a single day whole,
    so same-day picks — whose outcomes share weather, parks and pitchers — can
    never straddle the split and leak.
    """
    dated = [r for r in rows if r.get("slate_date")]
    days = sorted({r["slate_date"] for r in dated})
    if len(days) < 2:
        return dated, []
    cut = days[max(1, min(len(days) - 1, int(len(days) * fraction))) - 1]
    return ([r for r in dated if r["slate_date"] <= cut],
            [r for r in dated if r["slate_date"] > cut])


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _fmt(z: ZoneStat) -> str:
    if z.underpowered:
        return (f"  {z.label:<30} n={z.n:<6} ev={z.events:<5} "
                f"UNDERPOWERED (need {MIN_EVENTS} events)")
    skill = f"{z.skill:+.4f}" if z.skill is not None else "  --  "
    ci = (f"[{z.skill_ci[0]:+.4f},{z.skill_ci[1]:+.4f}]"
          if z.skill_ci else "")
    q = f"q={z.q_value:.3f}" if z.q_value is not None else ""
    mark = "  <== BEATS MARKET" if z.beats_market else ""
    return (f"  {z.label:<30} n={z.n:<6} ev={z.events:<5} "
            f"pred {z.predicted*100:5.1f}% act {z.actual*100:5.1f}% "
            f"drift {z.drift_pp:+5.1f}pp  skill {skill} {ci} {q}{mark}")


def render(rep: ZoneReport) -> str:
    out = [f"=== {rep.window.upper()}  ({rep.date_min} .. {rep.date_max}) ==="]
    out.append(_fmt(rep.overall))
    out.append("")
    for z in rep.zones:
        out.append(_fmt(z))
    out.append("")
    out.append(f"  zones tested (powered): {rep.n_tested}   "
               f"underpowered: {sum(1 for z in rep.zones if z.underpowered)}")
    out.append("  skill = mean[logloss(market) - logloss(model)]; positive = model better.")
    out.append("  q = Benjamini-Hochberg FDR across powered zones in this window.")
    return "\n".join(out)


def render_comparison(explore: ZoneReport, holdout: ZoneReport) -> str:
    """The only output that matters: what survived the holdout."""
    won = {z.label for z in explore.zones if z.beats_market}
    out = ["", "=== HOLDOUT VERDICT ===", ""]
    if not won:
        out.append("  No zone beat the market in the explore window.")
        out.append("  Nothing to validate — this is a clean null, not a failure to look.")
        return "\n".join(out)
    by_label = {z.label: z for z in holdout.zones}
    out.append(f"  {len(won)} zone(s) beat the market in explore. Holdout:")
    out.append("")
    for label in sorted(won):
        h = by_label.get(label)
        if h is None:
            out.append(f"  {label:<30} ABSENT from holdout window")
        elif h.underpowered:
            out.append(f"  {label:<30} holdout underpowered "
                       f"(ev={h.events}) — unproven")
        elif h.skill is not None and h.skill > 0 and h.p_value is not None and h.p_value < 0.05:
            out.append(f"  {label:<30} REPLICATED  skill {h.skill:+.4f} "
                       f"p={h.p_value:.3f}")
        else:
            out.append(f"  {label:<30} did NOT replicate  "
                       f"skill {h.skill:+.4f}" if h.skill is not None
                       else f"  {label:<30} did NOT replicate")
    return "\n".join(out)


def main() -> int:
    import argparse

    from src.results.full_slate_outcomes import build_labeled_dataset

    p = argparse.ArgumentParser(description="Zone search over the labeled frame.")
    p.add_argument("--split", type=float, default=0.6,
                   help="Fraction of DAYS in the explore window (default 0.6).")
    p.add_argument("--require-odds", action="store_true",
                   help="Keep only rows with a real market price attached.")
    p.add_argument("--model-version", default=None,
                   help="Restrict to one model generation; model_prob is not "
                        "comparable across the 2026-07-01 rebuild.")
    p.add_argument("--allow-partial", action="store_true",
                   help="Include games whose box score has not been read. Their "
                        "only labels are picks, so this reintroduces selection "
                        "bias. Off by default, and for good reason.")
    args = p.parse_args()

    rows = build_labeled_dataset(require_odds=args.require_odds,
                                 model_version=args.model_version,
                                 complete_games_only=not args.allow_partial)
    if not rows:
        print("no labeled rows — run src.results.full_slate_outcomes first")
        return 1

    if args.allow_partial:
        print("!" * 72)
        print("WARNING: --allow-partial includes games labeled from PICKS ONLY.")
        print("That is the selected sample this module exists to avoid.")
        print("!" * 72)
        print()

    explore, holdout = time_split(rows, args.split)
    rep_e = search(explore, "explore")
    print(render(rep_e))
    print()
    rep_h = search(holdout, "holdout")
    print(render(rep_h))
    print(render_comparison(rep_e, rep_h))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
