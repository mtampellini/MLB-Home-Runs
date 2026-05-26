"""
CLV (closing-line value) recovery from our own captured odds snapshots.

WHAT THIS IS
    A read-only MEASUREMENT tool. It does not touch the model, the filters,
    feature engineering, or MODEL_VERSION, so it is safe to run during the
    pre-registered filter experiment (lock window 2026-05-20 .. 2026-06-18).
    It only reads daily_archives (settled picks) and data/odds (snapshots) and
    writes a CSV of recovered CLV.

HOW IT WORKS
    Every daily-picks fire writes a timestamped odds snapshot to data/odds/
    (<date>-<HHMM>.json) containing per-book, per-batter HR prices plus each
    game's commence_time. For each settled pick we find the LATEST snapshot
    taken before that game's first pitch and recompute the de-vigged fair
    market probability there, using the SAME logic the pipeline used at entry
    (src.pipeline.run_daily._market_prob_devig, replicated below against
    src.odds.ev primitives). CLV is the change in fair probability between
    entry and that closing snapshot.

    CLV_pp = (closing_fair_prob - entry_fair_prob) * 100
        > 0  : market revised the event MORE likely after we bet  -> we beat the line
        < 0  : market drifted against us

IMPORTANT CAVEAT — THIS IS "T-MINUS" CLV, NOT TRUE CLOSE
    Daily-picks fires ~9x/day and stops well before late first pitches, so the
    last pre-game snapshot sits a median ~60 min before first pitch for the
    5/20+ window. Lines still move in that final hour. Treat recovered numbers
    as "CLV vs the line ~1h out", a directionally useful but conservative proxy
    for true closing-line value. True CLV requires the going-forward
    closing-capture job.

USAGE
    python -m src.backtest.clv_recover                      # 2026-05-20 .. today
    python -m src.backtest.clv_recover 2026-05-20           # explicit start
    python -m src.backtest.clv_recover 2026-05-20 2026-06-18
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Make this script runnable directly via -m without hard-coding sys.path
_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from src.odds.ev import (  # noqa: E402
    devig_consensus,
    devig_two_way,
    single_sided_consensus,
)

ARCHIVE_DIR = _BACKEND / "data" / "daily_archives"
ODDS_DIR = _BACKEND / "data" / "odds"
OUT_CSV = _BACKEND / "data" / "processed" / "clv_recovered.csv"

DEFAULT_START = "2026-05-20"          # triple/quad filters shipped this date
TIERS = ("primary", "secondary", "shadow")
# A snapshot is only a candidate "close" if it predates first pitch but isn't
# stale from a prior slate. 16h back-window comfortably covers same-day fires.
MAX_LOOKBACK = timedelta(hours=16)


def _dt(s: str) -> datetime:
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d


def market_prob_devig(quotes: list[dict]) -> tuple[Optional[float], str]:
    """Replica of src.pipeline.run_daily._market_prob_devig for a batter's quotes.

    Kept in lock-step with production: two-way (consensus) from the main
    over/under when present, else single-sided consensus from alt over prices.
    """
    pairs = [
        (q["main_over_american"], q["main_under_american"])
        for q in quotes
        if q.get("main_over_american") is not None
        and q.get("main_under_american") is not None
    ]
    if pairs:
        if len(pairs) == 1:
            return devig_two_way(*pairs[0]), "two_way"
        return devig_consensus(pairs), "two_way_consensus"
    bet = [q["bet_over_american"] for q in quotes if q.get("bet_over_american") is not None]
    if not bet:
        return None, "none"
    return single_sided_consensus(bet), "single_sided"


def load_snapshots(start: str, end: str) -> list[tuple[datetime, dict[str, list[dict]]]]:
    """Return [(fetched_at, {batter_name: [quote, ...]})] sorted by time.

    Includes the day after `end` so late games whose snapshots roll past
    midnight UTC are covered.
    """
    end_plus = (datetime.fromisoformat(end).date() + timedelta(days=1)).isoformat()
    snaps: list[tuple[datetime, dict[str, list[dict]]]] = []
    for f in sorted(ODDS_DIR.glob("*.json")):
        day = f.stem[:10]
        if not (start <= day <= end_plus):
            continue
        try:
            s = json.loads(f.read_text())
        except Exception:
            continue
        fa = s.get("fetched_at") or s.get("_snapshot_written_at")
        if not fa:
            continue
        by_batter: dict[str, list[dict]] = {}
        for q in s.get("quotes", []):
            by_batter.setdefault(q["batter_name"], []).append(q)
        snaps.append((_dt(fa), by_batter))
    snaps.sort(key=lambda x: x[0])
    return snaps


def closing_for(
    batter: str, commence: datetime, snaps: list[tuple[datetime, dict[str, list[dict]]]]
) -> tuple[Optional[list[dict]], Optional[datetime]]:
    """Latest snapshot before first pitch (within MAX_LOOKBACK) holding the batter."""
    best_q: Optional[list[dict]] = None
    best_t: Optional[datetime] = None
    lo = commence - MAX_LOOKBACK
    for t, by_batter in snaps:
        if lo <= t <= commence and batter in by_batter:
            if best_t is None or t > best_t:
                best_t, best_q = t, by_batter[batter]
    return best_q, best_t


def best_over(quotes: list[dict]) -> Optional[int]:
    prices = [q["bet_over_american"] for q in quotes if q.get("bet_over_american") is not None]
    return max(prices) if prices else None


def recover(start: str, end: str) -> list[dict]:
    snaps = load_snapshots(start, end)
    rows: list[dict] = []
    for f in sorted(ARCHIVE_DIR.glob("*.json")):
        date = f.stem
        if not (start <= date <= end):
            continue
        d = json.loads(f.read_text())
        for tier in TIERS:
            picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks", [])}
            for r in d.get("settlement", {}).get(f"{tier}_results", []) or []:
                p = picks.get((r["batter_id"], r["game_pk"]))
                if not p:
                    continue
                commence = _dt(p["game_datetime"])
                cl_quotes, cl_t = closing_for(p["batter"], commence, snaps)
                entry_fair = p.get("market_prob_devig")
                row = {
                    "date": date,
                    "tier": tier,
                    "batter": p["batter"],
                    "best_book": p.get("best_book"),
                    "entry_price": p.get("fd_odds") if p.get("best_book") == "fanduel" else p.get("dk_odds"),
                    "entry_fair": entry_fair,
                    "entry_method": p.get("devig_method"),
                    "outcome": r["outcome"],
                    "passes_baseline": p.get("filter_status", {}).get("passes_baseline", True),
                    "passes_triple": p.get("filter_status", {}).get("passes_triple"),
                    "passes_quad": p.get("filter_status", {}).get("passes_quad"),
                    "close_price": None,
                    "close_fair": None,
                    "close_method": None,
                    "gap_min": None,
                    "clv_pp": None,
                }
                if cl_quotes is not None:
                    close_fair, close_method = market_prob_devig(cl_quotes)
                    row["close_price"] = best_over(cl_quotes)
                    row["close_fair"] = close_fair
                    row["close_method"] = close_method
                    row["gap_min"] = round((commence - cl_t).total_seconds() / 60, 1)
                    if close_fair is not None and entry_fair is not None:
                        row["clv_pp"] = round((close_fair - entry_fair) * 100, 3)
                rows.append(row)
    return rows


def _summ(label: str, xs: list[dict]) -> None:
    have = [x for x in xs if x["clv_pp"] is not None]
    if not have:
        print(f"{label:28s} n={len(xs):4d}  (no CLV)")
        return
    clv = [x["clv_pp"] for x in have]
    beat = sum(1 for c in clv if c > 0) / len(clv) * 100
    print(
        f"{label:28s} n={len(xs):4d}  w/CLV={len(have):4d}  "
        f"mean={statistics.mean(clv):+.3f}pp  median={statistics.median(clv):+.3f}pp  "
        f"beat-close={beat:4.1f}%"
    )


def main(argv: list[str]) -> None:
    start = argv[1] if len(argv) > 1 else DEFAULT_START
    end = argv[2] if len(argv) > 2 else datetime.now(timezone.utc).date().isoformat()
    rows = recover(start, end)
    matched = [r for r in rows if r["clv_pp"] is not None]
    n = len(rows)
    print(f"CLV recovery  window={start}..{end}  picks={n}")
    if not n:
        return
    print(f"coverage: {len(matched)}/{n} picks matched to a pre-game close ({len(matched)/n*100:.1f}%)")
    gaps = sorted(r["gap_min"] for r in matched)
    if gaps:
        print(
            f"snapshot-to-first-pitch gap (min): median={statistics.median(gaps):.0f}  "
            f"p25={gaps[len(gaps)//4]:.0f}  p75={gaps[3*len(gaps)//4]:.0f}  max={max(gaps):.0f}"
            "   <-- 'T-minus' CLV, not true close"
        )
    print("\n-- CLV by cohort (fair-prob de-vig, same math as entry) --")
    _summ("baseline (all)", [r for r in rows if r["passes_baseline"]])
    _summ("triple (shown)", [r for r in rows if r["passes_triple"]])
    _summ("quad", [r for r in rows if r["passes_quad"]])
    _summ("dropped by triple", [r for r in rows if r["passes_baseline"] and not r["passes_triple"]])
    print("\n-- triple cohort by tier --")
    for tier in TIERS:
        _summ(f"triple {tier}", [r for r in rows if r["passes_triple"] and r["tier"] == tier])

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote per-pick CSV: {OUT_CSV.relative_to(_BACKEND)}  ({n} rows)")


if __name__ == "__main__":
    main(sys.argv)
