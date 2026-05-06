"""Deep diagnostic on the current primary picks.

Surfaces: breakout saturation, multiplier stacking, sample size, and
Harper-vs-Brooks-Lee contrast (the ones the user flagged).

Cache is hot from the smoke run, so re-running predict_slate is fast.
"""

from __future__ import annotations

import json
from datetime import date

from dotenv import load_dotenv

from src.backtest.as_of_context import AsOfContext
from src.model.baseline import BaselineConfig
from src.model.predict import predict_slate
from src.pipeline.slate import build_slate, normalize_name


def _fmt(v, w=10, fmt=".4f"):
    if v is None or v != v:
        return f"{'nan':>{w}}"
    return f"{v:>{w}{fmt}}"


def main() -> int:
    load_dotenv()
    cutoff = date.today()
    ctx = AsOfContext(cutoff_date=cutoff)

    with open(r"C:\Users\mtamp\Documents\Fantasy Baseball\HR Picks\backend\picks.json") as f:
        picks_data = json.load(f)
    primary_picks = picks_data["picks"]
    primary_norm = [normalize_name(p["batter"]) for p in primary_picks]

    print(f"Re-predicting slate (cache hot)...")
    slate, _ = build_slate(cutoff)
    rows = predict_slate(slate, ctx, config=BaselineConfig())

    by_norm = {normalize_name(r.entry.batter_name): r
                for r in rows if not r.skipped and r.prediction is not None}

    target_norms = list(primary_norm)
    if "bryce harper" not in target_norms:
        target_norms.append("bryce harper")     # for contrast

    targets = [by_norm[n] for n in target_norms if n in by_norm]
    targets.sort(key=lambda r: -r.prediction.p_hr)

    # =====================================================================
    print()
    print("# DIAGNOSTIC 1: Breakout saturation across primary picks")
    print()
    print(f"{'Batter':<22}{'Spot':>5}{'Breakout.score':>16}{'Breakout.raw':>14}"
          f"{'Reliability':>13}{'Capped?':>9}")
    print("-" * 80)
    capped_count = 0
    for r in targets:
        b = r.breakout
        if b is None:
            continue
        score = b.score
        raw = b.raw
        rel = b.reliability
        capped = abs(score) >= 0.149
        if capped:
            capped_count += 1
        marker = " *** CAP" if capped else ""
        print(f"{r.entry.batter_name[:22]:<22}{(r.entry.lineup_spot or 0):>5}"
              f"{score:>+16.4f}{raw:>+14.4f}{rel:>13.2f}{marker:>9}")
    print(f"\nPicks at or near +0.150 cap: {capped_count} / {len(targets)}")

    # =====================================================================
    print()
    print()
    print("# DIAGNOSTIC 2: Multiplier decomposition (Brooks Lee + a few)")
    print()
    print(f"{'Batter':<22}{'Blend':>9}{'+Brkout':>9}{'AdjPA':>9}"
          f"{'Pitch×':>8}{'Park×':>8}{'Temp×':>7}{'Wind×':>7}"
          f"{'FinalPA':>9}{'Cum×Blend':>10}{'pHR%':>7}{'PA/G':>5}")
    print("-" * 130)
    for r in targets:
        p = r.prediction
        c = p.components or {}
        cum = (p.p_per_pa / p.blended_hr_per_pa) if p.blended_hr_per_pa > 0 else 0.0
        print(f"{r.entry.batter_name[:22]:<22}"
              f"{p.blended_hr_per_pa:>9.4f}"
              f"{p.adjusted_per_pa - p.blended_hr_per_pa:>+9.4f}"
              f"{p.adjusted_per_pa:>9.4f}"
              f"{c.get('pitcher', 1):>8.2f}{c.get('park', 1):>8.2f}"
              f"{c.get('temperature', 1):>7.2f}{c.get('wind', 1):>7.2f}"
              f"{p.p_per_pa:>9.4f}{cum:>9.2f}x"
              f"{p.p_hr*100:>6.1f}%{p.pa_per_game:>5.1f}")

    # =====================================================================
    print()
    print()
    print("# DIAGNOSTIC 3: Sample size + career rates")
    print()
    print(f"{'Batter':<22}{'SeasPA':>7}{'SeasHR':>7}{'SeasHR/PA':>10}"
          f"{'PriorPA':>9}{'PriorHR':>9}{'PriorHR/PA':>12}"
          f"{'CareerHR/PA':>13}")
    print("-" * 95)
    for r in targets:
        s = r.season or {}
        py = r.prior_year or {}
        spa = int(s.get("pa", 0) or 0)
        shr = int(s.get("hr", 0) or 0)
        srate = s.get("hr_per_pa")
        ppa = int(py.get("pa", 0) or 0)
        phr = int(py.get("hr", 0) or 0)
        prate = py.get("hr_per_pa")
        total_pa = spa + ppa
        total_hr = shr + phr
        career = total_hr / total_pa if total_pa else 0
        print(f"{r.entry.batter_name[:22]:<22}{spa:>7}{shr:>7}"
              f"{_fmt(srate, 10)}{ppa:>9}{phr:>9}{_fmt(prate, 12)}"
              f"{career:>13.4f}")

    # =====================================================================
    print()
    print()
    print("# DIAGNOSTIC 4: Harper vs Brooks Lee (rank #5 vs rank #1)")
    print()
    h = by_norm.get("bryce harper")
    l = by_norm.get("brooks lee")
    if h is None or l is None:
        print(f"  Harper found: {h is not None};  Brooks Lee found: {l is not None}")
        return 1

    def _row(label, r):
        p = r.prediction
        c = p.components or {}
        b = r.breakout
        s = r.season or {}
        py = r.prior_year or {}
        return [
            ("blended_hr_per_pa", f"{p.blended_hr_per_pa:.4f}"),
            ("breakout.score", f"{b.score if b else 0:+.3f}"),
            ("breakout.raw", f"{b.raw if b else 0:+.3f}"),
            ("breakout.reliability", f"{b.reliability if b else 0:.2f}"),
            ("adjusted_per_pa", f"{p.adjusted_per_pa:.4f}"),
            ("pitcher_factor", f"{c.get('pitcher', 1):.3f}"),
            ("park_factor", f"{c.get('park', 1):.3f}"),
            ("env (temp×wind)", f"{c.get('temperature', 1) * c.get('wind', 1):.3f}"),
            ("final_per_pa", f"{p.p_per_pa:.4f}"),
            ("PA/game", f"{p.pa_per_game:.1f}"),
            ("p_hr", f"{p.p_hr*100:.1f}%"),
            ("season_pa", f"{int(s.get('pa', 0) or 0)}"),
            ("season_hr_per_pa", f"{s.get('hr_per_pa') or 0:.4f}"),
            ("prior_year_pa", f"{int(py.get('pa', 0) or 0)}"),
            ("prior_year_hr_per_pa", f"{py.get('hr_per_pa') or 0:.4f}"),
        ]

    h_data = _row("Harper", h)
    l_data = _row("Brooks Lee", l)

    print(f"{'Field':<25}{'Harper':>15}{'Brooks Lee':>15}{'Lee/Harper':>15}")
    print("-" * 70)
    for (label, hv), (_, lv) in zip(h_data, l_data):
        try:
            hf = float(hv.rstrip("%").rstrip("x"))
            lf = float(lv.rstrip("%").rstrip("x"))
            ratio = f"{lf/hf:.2f}" if hf else "n/a"
        except ValueError:
            ratio = ""
        print(f"{label:<25}{hv:>15}{lv:>15}{ratio:>15}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
