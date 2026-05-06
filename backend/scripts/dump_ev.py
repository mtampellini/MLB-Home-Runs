"""Diagnostic: dump per-batter EV regardless of threshold.

Shows where the model agrees vs disagrees with the market. If 0 picks cleared
the 25% EV gate, this tells us whether the top EVs are 24% (one calibration
nudge from picks) or 5% (model genuinely matches market — no edges today).
"""

from __future__ import annotations

import json
import sys
from datetime import date as _date
from pathlib import Path

from dotenv import load_dotenv

from src.backtest.as_of_context import AsOfContext
from src.model.baseline import BaselineConfig
from src.model.predict import predict_slate, top_n_features
from src.odds.ev import devig_consensus, devig_two_way, ev_pct
from src.pipeline.run_daily import (
    _index_quotes_by_norm_name,
    _market_prob_devig,
    _best_over_prices_by_book,
    PROCESSED_DIR,
)
from src.pipeline.slate import build_slate, normalize_name


def main() -> int:
    load_dotenv()
    cutoff = _date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else _date.today()
    ctx = AsOfContext(cutoff_date=cutoff)

    # Load the freshest odds snapshot for that date.
    odds_dir = Path(__file__).resolve().parents[1] / "data" / "odds"
    snaps = sorted(odds_dir.glob(f"{cutoff.isoformat()}-*.json"))
    if not snaps:
        print(f"no odds snapshots for {cutoff}")
        return 1
    snap = json.loads(snaps[-1].read_text())
    print(f"# Snapshot: {snaps[-1].name}  ({len(snap.get('quotes', []))} quotes)")

    # Reconstruct quote objects from the snapshot for the matcher.
    from src.odds.fetch import HRPropQuote
    from datetime import datetime
    quotes = []
    for q in snap.get("quotes", []):
        quotes.append(HRPropQuote(
            event_id=q["event_id"], home_team=q["home_team"], away_team=q["away_team"],
            commence_time=datetime.fromisoformat(q["commence_time"]),
            book=q["book"], batter_name=q["batter_name"], point=float(q["point"]),
            over_american=int(q["over_american"]),
            under_american=int(q["under_american"]) if q.get("under_american") is not None else None,
            last_update=datetime.fromisoformat(q["last_update"]),
        ))
    quotes_idx = _index_quotes_by_norm_name(quotes)

    # Re-run predictions (cache hot)
    slate, _ = build_slate(cutoff)
    rows = predict_slate(slate, ctx, config=BaselineConfig())

    # Compute EV per batter
    ev_rows = []
    for row in rows:
        if row.skipped or row.prediction is None:
            continue
        qs = quotes_idx.get(normalize_name(row.entry.batter_name)) or []
        if not qs:
            continue
        mkt = _market_prob_devig(qs)
        if mkt is None:
            continue
        book_prices = _best_over_prices_by_book(qs)
        if not book_prices:
            continue
        best_book = max(book_prices, key=book_prices.get)
        best_price = book_prices[best_book]
        ev = ev_pct(
            model_prob=row.prediction.p_hr,
            over_american=best_price,
            market_prob_devig=mkt,
        )
        ev_rows.append({
            "batter": row.entry.batter_name,
            "team": row.entry.team,
            "spot": row.entry.lineup_spot,
            "model_prob": row.prediction.p_hr,
            "market_prob": mkt,
            "best_book": best_book,
            "best_price": best_price,
            "fd_odds": book_prices.get("fanduel"),
            "dk_odds": book_prices.get("draftkings"),
            "ev_pct": ev.ev_pct,
            "edge_pp": ev.edge_pct,
            "top3": top_n_features(row.prediction, n=3),
            "row": row,
        })

    ev_rows.sort(key=lambda x: x["ev_pct"], reverse=True)

    print(f"\n# Top 25 by EV  (cleared 25%: {sum(1 for r in ev_rows if r['ev_pct'] >= 25)})")
    print(f"# Total matched: {len(ev_rows)}")
    print()
    cols = (
        f"{'Batter':<22}{'Team':<5}{'#':<3}{'Model%':>7}{'Mkt%':>7}{'Edge':>7}"
        f"{'EV%':>8}{'Best':>5}{'Price':>7}{'FD':>7}{'DK':>7}  "
    )
    print(cols + "Top features")
    print("-" * 130)
    for r in ev_rows[:25]:
        top = "  ".join(f"{t['name'][:14]}={t['value']:.2f}" for t in r["top3"])
        fd = f"{r['fd_odds']:+d}" if r['fd_odds'] is not None else "  --"
        dk = f"{r['dk_odds']:+d}" if r['dk_odds'] is not None else "  --"
        print(
            f"{r['batter'][:22]:<22}{r['team']:<5}{(r['spot'] or 0):<3}"
            f"{r['model_prob']*100:>6.1f}%{r['market_prob']*100:>6.1f}%"
            f"{r['edge_pp']:>+6.1f}%{r['ev_pct']:>+7.1f}%"
            f"{r['best_book'][:4]:>5}{r['best_price']:>+7d}{fd:>7}{dk:>7}  {top}"
        )

    print(f"\n# EV distribution across {len(ev_rows)} matched batters:")
    if ev_rows:
        evs = [r["ev_pct"] for r in ev_rows]
        evs_sorted = sorted(evs)
        print(f"  min={min(evs):+.1f}%  p10={evs_sorted[len(evs)//10]:+.1f}%  "
              f"median={evs_sorted[len(evs)//2]:+.1f}%  "
              f"p90={evs_sorted[int(len(evs)*0.9)]:+.1f}%  max={max(evs):+.1f}%")
        for thresh in (-25, 0, 10, 15, 20, 25, 30):
            n = sum(1 for e in evs if e >= thresh)
            print(f"  picks above {thresh:+d}% EV: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
