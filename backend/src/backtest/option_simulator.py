"""
Simulate ROI impact of four candidate pipeline changes against settled archives.

Options:
  1. per_game_cap(N)           — keep top N picks per game (ranked by ev_pct desc)
  2. stacked_ev_shade(F)       — shade ev_pct of stacked picks by factor F, drop if below tier threshold
  3. drop_both_pitcher_bad(T)  — drop picks in games where BOTH starters' top_3 features include pitcher factor >= T
  4. volume_governor(frac)     — cap any single game's pick share to `frac` of slate

Runs against backend/data/daily_archives/*.json. Uses settlement.{tier}_results.profit_units.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable


ARCHIVE_DIR = Path(__file__).resolve().parents[2] / "data" / "daily_archives"
POST_BUILD_START = "2026-05-13"

TIER_EV_MIN = {"primary": 25.0, "secondary": 25.0, "shadow": 10.0}
TIER_EV_MAX = {"primary": None, "secondary": None, "shadow": 25.0}


@dataclass
class Pick:
    date: str
    tier: str
    batter: str
    game_pk: int
    pitcher: str
    pitcher_id: int
    ev_pct: float
    model_prob: float
    stacked: bool
    top_3_features: list
    profit_units: float
    hit: bool

    @property
    def pitcher_factor(self) -> float:
        for feat in self.top_3_features or []:
            if feat.get("name") == "pitcher":
                return float(feat.get("value", 1.0))
        return 1.0


def _load_archive(path: Path) -> list[Pick]:
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    date = d["date"]
    settlement = d.get("settlement") or {}
    if not settlement:
        return []
    picks: list[Pick] = []
    for tier in ("primary", "secondary", "shadow"):
        picks_list = d.get(f"{tier}_picks", []) or []
        results = settlement.get(f"{tier}_results", []) or []
        results_by_batter = {(r["batter_id"], r["game_pk"]): r for r in results}
        for p in picks_list:
            key = (p["batter_id"], p["game_pk"])
            r = results_by_batter.get(key)
            if r is None or r.get("void_reason"):
                continue
            picks.append(
                Pick(
                    date=date,
                    tier=tier,
                    batter=p["batter"],
                    game_pk=int(p["game_pk"]),
                    pitcher=p.get("pitcher", ""),
                    pitcher_id=int(p.get("pitcher_id") or 0),
                    ev_pct=float(p["ev_pct"]),
                    model_prob=float(p["model_prob"]),
                    stacked=bool(p.get("stacked", False)),
                    top_3_features=p.get("top_3_features") or [],
                    profit_units=float(r["profit_units"]),
                    hit=bool(r.get("actual_hr")),
                )
            )
    return picks


def load_all_picks(start_date: str = POST_BUILD_START) -> list[Pick]:
    picks: list[Pick] = []
    for f in sorted(ARCHIVE_DIR.glob("*.json")):
        date = f.stem
        if date < start_date:
            continue
        picks.extend(_load_archive(f))
    return picks


# ---- option implementations ----

def baseline(picks: list[Pick]) -> list[Pick]:
    return list(picks)


def per_game_cap(picks: list[Pick], cap: int) -> list[Pick]:
    by_date_game: dict[tuple, list[Pick]] = defaultdict(list)
    for p in picks:
        by_date_game[(p.date, p.game_pk)].append(p)
    kept: list[Pick] = []
    for group in by_date_game.values():
        group_sorted = sorted(group, key=lambda x: -x.ev_pct)
        kept.extend(group_sorted[:cap])
    return kept


def stacked_ev_shade(picks: list[Pick], factor: float) -> list[Pick]:
    kept: list[Pick] = []
    for p in picks:
        if not p.stacked:
            kept.append(p)
            continue
        shaded = p.ev_pct * factor
        ev_min = TIER_EV_MIN[p.tier]
        ev_max = TIER_EV_MAX[p.tier]
        if shaded < ev_min:
            continue
        if ev_max is not None and shaded >= ev_max:
            continue
        kept.append(p)
    return kept


def drop_both_pitcher_bad(picks: list[Pick], threshold: float) -> list[Pick]:
    pitchers_per_game: dict[tuple, set[tuple[int, float]]] = defaultdict(set)
    for p in picks:
        pitchers_per_game[(p.date, p.game_pk)].add((p.pitcher_id, p.pitcher_factor))
    bad_games: set[tuple] = set()
    for key, entries in pitchers_per_game.items():
        # picks store the pitcher the batter is FACING — different teams' batters cite different pitchers
        distinct_pitchers = {(pid, pf) for pid, pf in entries if pid > 0}
        if len(distinct_pitchers) >= 2:
            if all(pf >= threshold for _, pf in distinct_pitchers):
                bad_games.add(key)
    return [p for p in picks if (p.date, p.game_pk) not in bad_games]


def model_prob_band(picks: list[Pick], lo: float, hi: float) -> list[Pick]:
    """Drop picks with model_prob in [lo, hi)."""
    return [p for p in picks if not (lo <= p.model_prob < hi)]


def ev_ceiling(picks: list[Pick], max_ev: float) -> list[Pick]:
    """Drop picks with ev_pct >= max_ev (over-confident outliers)."""
    return [p for p in picks if p.ev_pct < max_ev]


def pitcher_factor_band(picks: list[Pick], lo: float, hi: float) -> list[Pick]:
    """Drop picks where pitcher_factor is in [lo, hi)."""
    return [p for p in picks if not (lo <= p.pitcher_factor < hi)]


def gold_zone_only(picks: list[Pick]) -> list[Pick]:
    """Keep only picks in observed winning zones (model_prob 25-30% OR EV 35-50%)."""
    return [
        p for p in picks
        if (0.25 <= p.model_prob < 0.30) or (35.0 <= p.ev_pct < 50.0)
    ]


def volume_governor(picks: list[Pick], frac: float) -> list[Pick]:
    by_date: dict[str, list[Pick]] = defaultdict(list)
    for p in picks:
        by_date[p.date].append(p)
    kept: list[Pick] = []
    for date, day_picks in by_date.items():
        total = len(day_picks)
        if total == 0:
            continue
        cap = max(1, math.floor(frac * total))
        by_game: dict[int, list[Pick]] = defaultdict(list)
        for p in day_picks:
            by_game[p.game_pk].append(p)
        for game_picks in by_game.values():
            if len(game_picks) <= cap:
                kept.extend(game_picks)
            else:
                kept.extend(sorted(game_picks, key=lambda x: -x.ev_pct)[:cap])
    return kept


# ---- evaluation ----

@dataclass
class Result:
    name: str
    n_picks: int = 0
    n_wins: int = 0
    profit: float = 0.0
    by_tier: dict[str, dict[str, float]] = field(default_factory=dict)

    @property
    def roi(self) -> float:
        return 100.0 * self.profit / self.n_picks if self.n_picks else 0.0

    @property
    def hit_rate(self) -> float:
        return 100.0 * self.n_wins / self.n_picks if self.n_picks else 0.0


def evaluate(name: str, picks: list[Pick]) -> Result:
    r = Result(name=name)
    by_tier: dict[str, dict[str, float]] = defaultdict(lambda: {"n": 0, "wins": 0, "profit": 0.0})
    for p in picks:
        r.n_picks += 1
        r.n_wins += int(p.hit)
        r.profit += p.profit_units
        bt = by_tier[p.tier]
        bt["n"] += 1
        bt["wins"] += int(p.hit)
        bt["profit"] += p.profit_units
    r.by_tier = dict(by_tier)
    return r


def fmt(r: Result) -> str:
    line = f"{r.name:35s}  n={r.n_picks:4d}  W={r.n_wins:3d}  hit={r.hit_rate:5.1f}%  P/L={r.profit:+7.2f}u  ROI={r.roi:+6.2f}%"
    return line


def main(start_date: str = POST_BUILD_START) -> None:
    picks = load_all_picks(start_date)
    if not picks:
        print(f"No settled picks found for window starting {start_date}.")
        return

    dates = sorted({p.date for p in picks})
    print(f"Window: {dates[0]} to {dates[-1]}  ({len(dates)} days, {len(picks)} settled picks)")
    print()

    strategies: list[tuple[str, Callable[[list[Pick]], list[Pick]]]] = [
        ("BASELINE", lambda ps: baseline(ps)),
    ]
    for n in (1, 2, 3, 4, 5):
        strategies.append((f"per_game_cap(N={n})", lambda ps, n=n: per_game_cap(ps, n)))
    for f in (0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9):
        strategies.append((f"stacked_ev_shade(F={f})", lambda ps, f=f: stacked_ev_shade(ps, f)))
    for t in (1.05, 1.10, 1.20):
        strategies.append((f"drop_both_pitcher_bad(T={t})", lambda ps, t=t: drop_both_pitcher_bad(ps, t)))
    for fr in (0.20, 0.25, 0.33):
        strategies.append((f"volume_governor(frac={fr})", lambda ps, fr=fr: volume_governor(ps, fr)))

    # Combinations on top of best single strategy
    strategies.append((
        "combo: shade(0.7)+per_game_cap(3)",
        lambda ps: per_game_cap(stacked_ev_shade(ps, 0.7), 3),
    ))
    strategies.append((
        "combo: shade(0.7)+volume_gov(0.25)",
        lambda ps: volume_governor(stacked_ev_shade(ps, 0.7), 0.25),
    ))
    strategies.append((
        "combo: shade(0.7)+drop_both_pitcher(1.1)",
        lambda ps: drop_both_pitcher_bad(stacked_ev_shade(ps, 0.7), 1.10),
    ))
    strategies.append((
        "combo: shade(0.6)+per_game_cap(3)",
        lambda ps: per_game_cap(stacked_ev_shade(ps, 0.6), 3),
    ))
    print(f"{'Strategy':35s}  {'picks':>5s} {'wins':>5s} {'hit%':>6s} {'P/L':>9s} {'ROI':>8s}")
    print("-" * 95)
    results = []
    for name, fn in strategies:
        kept = fn(picks)
        r = evaluate(name, kept)
        results.append(r)
        print(fmt(r))

    # Best ROI strategies summary
    print()
    print("Top 5 by ROI (min 50 picks):")
    top = sorted([r for r in results if r.n_picks >= 50], key=lambda r: -r.roi)[:5]
    for r in top:
        print(f"  {fmt(r)}")

    # Per-tier breakdown for baseline + top single-option strategies
    print()
    print("Per-tier breakdown (baseline + top non-baseline):")
    for r in [results[0]] + [x for x in top if x.name != "BASELINE"][:3]:
        print(f"\n  {r.name}:")
        for tier in ("primary", "secondary", "shadow"):
            bt = r.by_tier.get(tier, {"n": 0, "wins": 0, "profit": 0.0})
            n = bt["n"]
            roi = 100.0 * bt["profit"] / n if n else 0.0
            hit = 100.0 * bt["wins"] / n if n else 0.0
            print(f"    {tier:10s}: n={n:3d}  hit={hit:5.1f}%  P/L={bt['profit']:+7.2f}u  ROI={roi:+6.2f}%")


if __name__ == "__main__":
    import sys
    start = sys.argv[1] if len(sys.argv) > 1 else POST_BUILD_START
    main(start)
