"""Diagnostic: run the daily pipeline with a permissive EV threshold so we
see all matched batters sorted by EV. Also prints pitcher-feature drilldowns
for the top 3 picks (raw season + 30d HR/9, barrel% allowed, etc) so we can
sanity-check against Baseball Savant.
"""

from __future__ import annotations

import sys
from datetime import date as _date

from dotenv import load_dotenv

from src.backtest.as_of_context import AsOfContext
from src.features.blend import blend_features
from src.model.baseline import BaselineConfig
from src.pipeline.run_daily import run_daily, PROCESSED_DIR


def main() -> int:
    load_dotenv()
    cutoff = _date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else _date.today()

    print(f"# Running with EV threshold = -100% (show every matched batter)")
    report = run_daily(cutoff_date=cutoff, ev_threshold_pct=-100.0)

    import json
    picks_data = json.loads(report.picks_path.read_text())
    picks = picks_data.get("picks", [])
    print(f"\n# Matched batters (any EV): {len(picks)}")
    print(f"# Real picks at >=25% EV: {sum(1 for p in picks if p['ev_pct'] >= 25)}")
    print()

    # EV distribution
    if picks:
        evs = sorted([p["ev_pct"] for p in picks])
        print(f"EV distribution: min={evs[0]:+.1f}%  p10={evs[len(evs)//10]:+.1f}%  "
              f"median={evs[len(evs)//2]:+.1f}%  "
              f"p90={evs[int(len(evs)*0.9)]:+.1f}%  max={evs[-1]:+.1f}%")
        print()

    # Top 25 by EV
    cols = (
        f"{'Batter':<22}{'Team':<5}{'#':<3}{'Mdl%':>6}{'Mkt%':>6}{'EV%':>7}"
        f"{'Best':>5}{'Price':>7}{'FD':>7}{'DK':>7}  Top features"
    )
    print(cols)
    print("-" * 130)
    for p in picks[:25]:
        top = "  ".join(f"{t['name'][:13]}={t['value']:.2f}" for t in p.get("top_3_features", []))
        fd = f"{p['fd_odds']:+d}" if p.get('fd_odds') is not None else "  --"
        dk = f"{p['dk_odds']:+d}" if p.get('dk_odds') is not None else "  --"
        spot = p.get("lineup_spot") or 0
        # ev_pct already in percent. model_prob and market_prob are 0-1 scaled.
        print(
            f"{p['batter'][:22]:<22}{p['team']:<5}{spot:<3}"
            f"{p['model_prob']*100:>5.1f}%{p['market_prob_devig']*100:>5.1f}%"
            f"{p['ev_pct']:>+6.1f}%"
            f"{p['best_book'][:4]:>5}{p.get('dk_odds') if p['best_book']=='draftkings' else p.get('fd_odds'):>+7d}"
            f"{fd:>7}{dk:>7}  {top}"
        )

    # Pitcher feature dump for top 3
    print()
    print("=" * 80)
    print("# Pitcher feature drill-down for top 3 picks (raw values)")
    print("=" * 80)

    from src.features.pitcher import pitcher_features
    ctx = AsOfContext(cutoff_date=cutoff)
    seen_pitchers = set()
    for p in picks[:3]:
        pid = p["pitcher_id"]
        if pid in seen_pitchers:
            continue
        seen_pitchers.add(pid)
        print(f"\n{p['pitcher']} (id={pid}, {p['pitcher_hand']}HP) facing "
              f"{p['batter']} ({p['batter_hand']}HB):")
        try:
            pf = pitcher_features(pid, ctx)
        except Exception as e:    # noqa: BLE001
            print(f"  pitcher_features failed: {type(e).__name__}: {e}")
            continue

        for scope_name in ("season", "recent"):
            scope = pf[scope_name]
            split = scope[f"vs_{p['batter_hand']}"]
            overall = scope["overall"]
            print(f"  {scope_name:<7} window {scope['window_start']} .. {scope['window_end']}")
            print(f"    overall:   pa={overall['pa']:>4}  ip={overall['ip_estimate']:>5.1f}  "
                  f"HR/9={overall['hr_per_9']:>5.2f}  bbe={overall['bbe']:>3}  "
                  f"barrel%={overall.get('barrel_pct_allowed') or 0:.3f}  "
                  f"xwobacon={overall.get('xwobacon_allowed') or 0:.3f}")
            print(f"    vs_{p['batter_hand']}:      pa={split['pa']:>4}  ip={split['ip_estimate']:>5.1f}  "
                  f"HR/9={split['hr_per_9']:>5.2f}  bbe={split['bbe']:>3}  "
                  f"barrel%={split.get('barrel_pct_allowed') or 0:.3f}  "
                  f"xwobacon={split.get('xwobacon_allowed') or 0:.3f}")

        # Show the blended HR/9 the model actually used.
        season_split = pf["season"][f"vs_{p['batter_hand']}"]
        recent_split = pf["recent"][f"vs_{p['batter_hand']}"]
        blend = blend_features(season_split, recent_split,
                                metric_key="hr_per_9", pa_key="pa")
        print(f"  --> blended HR/9 used by model (vs_{p['batter_hand']}): {blend.rate:.3f}")
        print(f"      season_pa={blend.season_pa}  recent_pa={blend.recent_pa}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
