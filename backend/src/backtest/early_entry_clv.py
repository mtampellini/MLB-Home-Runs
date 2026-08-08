"""Would betting EARLIER have produced better closing-line value?

WHAT THIS IS
    A read-only companion to clv_recover.py. That tool measures the CLV we
    actually got; this one measures the CLV we would have got had we taken the
    EARLIEST price the books posted, using only data already on disk. No new
    bets, no API calls, nothing touched in the pick pipeline.

WHY IT EXISTS
    The model can't run until lineups are confirmed, so entry sits a median
    ~52 min before first pitch — but the books post these lines the night
    before. Measured over the full life of a line, the price moves ~64 cents
    and moves at all ~86% of the time; measured over our window it moves 1.6
    cents and is static 95% of the time. Effectively all price discovery
    happens before we arrive, which means recovered CLV is measured across a
    frozen window and cannot detect edge in either direction.

    That raises an obvious question — is money being left there? — and this
    module answers it without building an early-projection pipeline first.

HOW IT WORKS
    For each settled pick, find its price series across all odds snapshots,
    take the earliest and latest observations before first pitch, and de-vig
    both with the SAME primitives clv_recover uses. Then:

        clv_early  = (close_fair - early_fair) * 100
        clv_actual = (close_fair - entry_fair) * 100
        drift      = (entry_fair - early_fair) * 100

    These satisfy clv_early == clv_actual + drift exactly. `drift` is the part
    we forfeit by entering late, and is the quantity of interest.

TWO LANDMINES THIS ENCODES
    1. The MLB Stats API and the Odds API report first pitch about a minute
       apart ("00:40" vs "00:41"). Joining games on an exact timestamp drops
       ~90% of matches and leaves a badly underpowered sample that looks like
       real data. Games are matched on a tolerance instead (COMMENCE_TOL),
       which is far narrower than the 24h between back-to-back games — the
       thing that actually needs disambiguating.
    2. `passes_triple_v2` only exists on picks written from 2026-07-01. Reading
       the stored `filter_status` therefore discards all 927 earlier picks as
       "rejected" when they were simply never evaluated. This module recomputes
       the filter via src.pipeline.filters, which degrades to `passes_triple`
       when `ev_pct_p3` is absent — exactly how the tracker treats pre-7/01
       picks. Recomputing is what turns the pre-rebuild era into an
       out-of-sample test rather than a silent hole.

FINDINGS AS OF 2026-08-08 (n=226 primary production picks)
    Aggregate early-entry CLV is +0.17pp, CI [-0.05, +0.38] — not significant.
    The +0.64pp that showed up post-rebuild did NOT replicate pre-rebuild
    (-0.05pp), and picks the filter REJECTS moved the same way in both eras, so
    the split tracks the calendar rather than the filter. Month by month the
    sign flips (May +0.04, Jun -0.33, Jul +0.42, Aug -0.46) at near-identical
    ~19h lead times, so it is not a lead-time artifact. Read: the line moves a
    lot before we bet, but not reliably toward our picks. Re-run as n grows.

USAGE
    python -m src.backtest.early_entry_clv                  # all archived dates
    python -m src.backtest.early_entry_clv 2026-07-01       # explicit start
    python -m src.backtest.early_entry_clv 2026-05-20 2026-06-18
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from src.backtest.clv_recover import (  # noqa: E402
    ARCHIVE_DIR,
    ODDS_DIR,
    _dt,
    best_over,
    market_prob_devig,
)
from src.pipeline.filters import passes_triple_v2  # noqa: E402

OUT_CSV = _BACKEND / "data" / "processed" / "early_entry_clv.csv"

TIERS = ("primary", "secondary", "shadow")

# How far back a snapshot may sit and still count as this game's price. Lines
# post the night before, so this must clear 24h — clv_recover's 16h window is
# built for finding the CLOSE and would truncate the early end.
LOOKBACK = timedelta(hours=30)

# The earliest observation must beat our real entry by a real margin, or
# "early" means nothing. Our median entry is ~52 min out.
MIN_EARLY_LEAD = timedelta(hours=3)

# Tolerance for the MLB-vs-Odds API first-pitch skew (~1 min). Wide enough for
# the skew, far narrower than the 24h separating back-to-back games.
COMMENCE_TOL = timedelta(minutes=15)


# ---------------------------------------------------------------------------
# Price series index
# ---------------------------------------------------------------------------

def build_index(odds_dir: Path = ODDS_DIR) -> dict:
    """{batter: {commence_dt: [(fetched_at, [quote, ...]), ...]}}, time-sorted.

    Keyed by game, not by batter alone: with a 30h lookback a batter playing
    on consecutive days would otherwise inherit his previous game's prices.
    """
    idx: dict = defaultdict(lambda: defaultdict(list))
    for f in sorted(Path(odds_dir).glob("*.json")):
        try:
            snap = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        fetched = snap.get("fetched_at") or snap.get("_snapshot_written_at")
        if not fetched:
            continue
        t = _dt(fetched)
        per: dict = defaultdict(list)
        for q in snap.get("quotes") or []:
            ct = q.get("commence_time")
            if not ct or not q.get("batter_name"):
                continue
            per[(q["batter_name"], _dt(ct))].append(q)
        for (name, ct), quotes in per.items():
            idx[name][ct].append((t, quotes))
    for name in idx:
        for ct in idx[name]:
            idx[name][ct].sort(key=lambda x: x[0])
    return idx


def series_for(idx: dict, batter: str, commence: datetime) -> Optional[list]:
    """Price series for the game closest to `commence` within COMMENCE_TOL."""
    games = idx.get(batter)
    if not games:
        return None
    best, best_gap = None, None
    for ct, series in games.items():
        gap = abs(ct - commence)
        if gap <= COMMENCE_TOL and (best_gap is None or gap < best_gap):
            best, best_gap = series, gap
    return best


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def recover(
    start: Optional[str] = None,
    end: Optional[str] = None,
    *,
    archive_dir: Path = ARCHIVE_DIR,
    odds_dir: Path = ODDS_DIR,
) -> list[dict]:
    """One row per settled pick that has a usable early price."""
    idx = build_index(odds_dir)
    rows: list[dict] = []
    for f in sorted(Path(archive_dir).glob("*.json")):
        date = f.stem
        if (start and date < start) or (end and date > end):
            continue
        try:
            archive = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        for tier in TIERS:
            picks = {(p["batter_id"], p.get("game_pk")): p
                     for p in archive.get(f"{tier}_picks") or []}
            for res in (archive.get("settlement") or {}).get(f"{tier}_results") or []:
                pick = picks.get((res["batter_id"], res.get("game_pk")))
                if not pick or res.get("outcome") not in ("W", "L"):
                    continue
                entry_fair = pick.get("market_prob_devig")
                if entry_fair is None:
                    continue
                commence = _dt(pick["game_datetime"])
                series = series_for(idx, pick["batter"], commence)
                if not series:
                    continue
                pre = [(t, q) for t, q in series if commence - LOOKBACK <= t <= commence]
                if len(pre) < 2:
                    continue
                early_t, early_q = pre[0]
                close_t, close_q = pre[-1]
                if commence - early_t < MIN_EARLY_LEAD:
                    continue
                early_fair, early_method = market_prob_devig(early_q)
                close_fair, _ = market_prob_devig(close_q)
                early_price = best_over(early_q)
                if early_fair is None or close_fair is None or early_price is None:
                    continue

                rows.append({
                    "date": date,
                    "tier": tier,
                    "batter": pick["batter"],
                    "outcome": res["outcome"],
                    "profit_units": res.get("profit_units", 0.0),
                    "model_prob": pick.get("model_prob"),
                    "ev_pct": pick.get("ev_pct"),
                    # Recomputed, NOT read from filter_status — see landmine 2.
                    "passes_triple_v2": passes_triple_v2(pick),
                    "entry_price": (pick.get("fd_odds")
                                    if pick.get("best_book") == "fanduel"
                                    else pick.get("dk_odds")),
                    "early_price": early_price,
                    "early_method": early_method,
                    "early_lead_h": round((commence - early_t).total_seconds() / 3600, 2),
                    "entry_fair": entry_fair,
                    "early_fair": round(early_fair, 6),
                    "close_fair": round(close_fair, 6),
                    "clv_actual_pp": round((close_fair - entry_fair) * 100, 4),
                    "clv_early_pp": round((close_fair - early_fair) * 100, 4),
                    "drift_pp": round((entry_fair - early_fair) * 100, 4),
                })
    return rows


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def describe(rows: list[dict], key: str = "clv_early_pp") -> Optional[dict]:
    """Mean / median / 5%-trimmed mean / 95% CI. None below 25 rows."""
    if len(rows) < 25:
        return None
    v = sorted(r[key] for r in rows)
    mean = statistics.mean(v)
    se = statistics.stdev(v) / (len(v) ** 0.5)
    cut = len(v) // 20
    trimmed = statistics.mean(v[cut: -cut or None])
    return {
        "n": len(v),
        "mean": mean,
        "median": statistics.median(v),
        "trimmed": trimmed,
        "ci_lo": mean - 1.96 * se,
        "ci_hi": mean + 1.96 * se,
        "significant": abs(mean) - 1.96 * se > 0,
    }


def _line(label: str, d: Optional[dict], n_raw: int) -> str:
    if d is None:
        return f"  {label:<38} n={n_raw:<5} (too few)"
    return (f"  {label:<38} n={d['n']:<5} {d['mean']:+.3f}pp  "
            f"median {d['median']:+.3f}  trim {d['trimmed']:+.3f}  "
            f"CI [{d['ci_lo']:+.3f},{d['ci_hi']:+.3f}]"
            f"{'  *' if d['significant'] else ''}")


def report(rows: list[dict]) -> str:
    if not rows:
        return "no matched picks — check that data/odds and daily_archives overlap"
    out: list[str] = []
    prim = [r for r in rows if r["tier"] == "primary"]
    lead = statistics.median([r["early_lead_h"] for r in rows])
    out.append(f"matched settled picks: {len(rows)}   primary: {len(prim)}")
    out.append(f"median early-price lead over first pitch: {lead:.1f}h")
    out.append("")

    prod = [r for r in prim if r["passes_triple_v2"]]
    rej = [r for r in prim if not r["passes_triple_v2"]]
    out.append("EARLY-ENTRY CLV (primary)")
    out.append(_line("production filter", describe(prod), len(prod)))
    out.append(_line("filter rejects (control)", describe(rej), len(rej)))
    out.append("")

    out.append("SAME CUT ON ACTUAL-ENTRY CLV")
    out.append(_line("production filter", describe(prod, "clv_actual_pp"), len(prod)))
    out.append(_line("filter rejects (control)", describe(rej, "clv_actual_pp"), len(rej)))
    out.append("")

    out.append("BY MONTH (primary, all picks) — watch for sign flips")
    by_month: dict = defaultdict(list)
    for r in prim:
        by_month[r["date"][:7]].append(r)
    for m in sorted(by_month):
        sub = by_month[m]
        lead_m = statistics.median([r["early_lead_h"] for r in sub])
        out.append(_line(f"{m}  (lead {lead_m:.1f}h)", describe(sub), len(sub)))
    out.append("")
    out.append("  * = 95% CI excludes zero.  drift = clv_early - clv_actual is the")
    out.append("  movement we forfeit by entering late; it is the quantity of interest.")
    return "\n".join(out)


def write_csv(rows: list[dict], path: Path = OUT_CSV) -> Optional[Path]:
    if not rows:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return path


def main() -> int:
    start = sys.argv[1] if len(sys.argv) > 1 else None
    end = sys.argv[2] if len(sys.argv) > 2 else None
    rows = recover(start, end)
    print(report(rows))
    path = write_csv(rows)
    if path:
        print(f"\nwrote {len(rows)} rows -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
