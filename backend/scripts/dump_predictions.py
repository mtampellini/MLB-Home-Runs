"""Diagnostic: dump all non-skipped model predictions for a given date.

Useful for eyeballing the model output BEFORE we have real odds. Run after a
daily-pipeline pass; cache will hit everywhere so this is seconds.

    python -m scripts.dump_predictions [YYYY-MM-DD]
"""

from __future__ import annotations

import sys
from datetime import date as _date

from src.backtest.as_of_context import AsOfContext
from src.model.baseline import BaselineConfig
from src.model.predict import predict_slate, top_n_features
from src.pipeline.slate import build_slate


def main() -> int:
    cutoff = _date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else _date.today()
    ctx = AsOfContext(cutoff_date=cutoff)
    print(f"# Predictions for {cutoff}")
    print()

    slate, meta = build_slate(cutoff)
    print(f"Slate: {len(slate)} entries from {meta['games_with_lineups']} games "
          f"(skipped {meta['games_no_lineup_skipped']} games without lineups)")
    print()

    rows = predict_slate(slate, ctx, config=BaselineConfig())
    kept = [r for r in rows if not r.skipped and r.prediction is not None]
    skipped = [r for r in rows if r.skipped]

    # Sort descending by model P(HR) — the most interesting batters at top.
    kept.sort(key=lambda r: r.prediction.p_hr, reverse=True)

    cols = (
        f"{'Batter':<22}{'Team':<5}{'Pitcher':<22}{'P-Hand':<7}{'Park':<6}"
        f"{'Lineup':<8}{'Model%':>8}{'PerPA%':>8}{'Blend':>8}{'Brkout':>8}"
        f"{'Trend':>8}{'UR':>4}{'LC':>4}  Top-3 features"
    )
    print(cols)
    print("-" * len(cols))
    for r in kept:
        e = r.entry
        p = r.prediction
        bk = r.breakout.score if r.breakout else 0.0
        rf = r.recent_form
        trend = (f"{rf.trend_signal*100:+.0f}%" if rf and rf.trend_signal is not None else "  -- ")
        ur = "Y" if (rf and rf.unstable_recent) else "-"
        lc = "Y" if r.low_confidence else "-"
        top3 = top_n_features(p, n=3)
        top_str = "  ".join(f"{t['name'][:14]}={t['value']:.2f}" for t in top3)
        print(
            f"{e.batter_name[:22]:<22}{e.team:<5}{(e.pitcher_name or '?')[:22]:<22}"
            f"{e.pitcher_hand:<7}{e.park:<6}#{e.lineup_spot or 0:<7}"
            f"{p.p_hr*100:>7.1f}%{p.p_per_pa*100:>7.2f}%{p.blended_hr_per_pa:>8.4f}"
            f"{bk:>+8.3f}{trend:>8}{ur:>4}{lc:>4}  {top_str}"
        )

    print()
    print(f"Kept: {len(kept)} / Skipped: {len(skipped)}")
    if kept:
        probs = [r.prediction.p_hr for r in kept]
        print(f"Model P(HR) distribution:  min={min(probs)*100:.1f}%  "
              f"median={sorted(probs)[len(probs)//2]*100:.1f}%  "
              f"p90={sorted(probs)[int(len(probs)*0.9)]*100:.1f}%  "
              f"max={max(probs)*100:.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
