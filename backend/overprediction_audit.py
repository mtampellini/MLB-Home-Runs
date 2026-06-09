"""Localize the over-prediction: calibration (expected vs actual) by model component.

All settled picks 5/13-6/08, all tiers, non-void. Expected = mean model_prob.
"""
import json, glob, os, math
from collections import defaultdict

TIERS = ["primary", "secondary", "shadow"]
PA_BY_SPOT = {1: 4.6, 2: 4.5, 3: 4.4, 4: 4.3, 5: 4.2, 6: 4.0, 7: 3.9, 8: 3.7, 9: 3.6}

rows = []
for path in sorted(glob.glob("data/daily_archives/*.json")):
    date = os.path.basename(path)[:10]
    if date < "2026-05-13":
        continue
    d = json.load(open(path))
    s = d.get("settlement") or {}
    for tier in TIERS:
        picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks") or []}
        for r in s.get(f"{tier}_results") or []:
            if r.get("void_reason") is not None or r["outcome"] == "V":
                continue
            p = picks.get((r["batter_id"], r["game_pk"]))
            if p is None:
                continue
            feats = {f.get("name"): float(f.get("value", 1.0)) for f in p.get("top_3_features") or []}
            spot = p.get("lineup_spot")
            pa = PA_BY_SPOT.get(spot, 4.2)
            model = float(p["model_prob"])
            blended = float(p.get("blended_hr_per_pa") or float("nan"))
            per_pa = 1.0 - (1.0 - model) ** (1.0 / pa)
            tot_mult = per_pa / blended if blended and not math.isnan(blended) and blended > 0 else float("nan")
            rows.append({
                "date": date, "tier": tier, "win": r["outcome"] == "W",
                "profit": r["profit_units"], "model": model,
                "market": float(p["market_prob_devig"]),
                "blended": blended, "tot_mult": tot_mult,
                "feats": feats, "spot": spot,
                "home": p.get("team") == p.get("park"),
                "park_code": p.get("park"),
                "low_conf": bool(p.get("low_confidence")),
                "unstable": bool(p.get("unstable_recent")),
                "stacked": bool(p.get("stacked")),
                "breakout": float(p.get("breakout_score") or 0.0),
            })

print(f"rows={len(rows)}  home-batter share={sum(1 for r in rows if r['home'])/len(rows)*100:.0f}%")

def cal(sel, label):
    n = len(sel)
    if n < 20:
        print(f"  {label:44s} n={n:4d}  (too small)")
        return
    exp = sum(r["model"] for r in sel) / n
    mkt = sum(r["market"] for r in sel) / n
    act = sum(1 for r in sel if r["win"]) / n
    pl = sum(r["profit"] for r in sel)
    print(f"  {label:44s} n={n:4d}  exp={exp*100:5.1f}%  act={act*100:5.1f}%  gap={(act-exp)*100:+6.1f}pp  mkt={mkt*100:5.1f}%  ROI={pl/n*100:+6.1f}%")

print("\n== total implied multiplier (model per-PA / blended per-PA) ==")
for lo, hi in [(0.0, 1.0), (1.0, 1.25), (1.25, 1.5), (1.5, 2.0), (2.0, 99.0)]:
    cal([r for r in rows if not math.isnan(r["tot_mult"]) and lo <= r["tot_mult"] < hi], f"multiplier {lo:.2f}-{hi:.2f}")

print("\n== batter skill (blended HR/PA) ==")
for lo, hi in [(0.0, 0.03), (0.03, 0.04), (0.04, 0.05), (0.05, 0.06), (0.06, 1.0)]:
    cal([r for r in rows if not math.isnan(r["blended"]) and lo <= r["blended"] < hi], f"blended {lo:.3f}-{hi:.3f}")

print("\n== park factor (when in top-3) x home/away ==")
cal([r for r in rows if "park" not in r["feats"]], "park not in top3 (~neutral)")
for lo, hi in [(1.0, 1.1), (1.1, 1.3), (1.3, 9.9)]:
    cal([r for r in rows if lo <= r["feats"].get("park", -1) < hi], f"park factor {lo:.1f}-{hi:.1f}")
cal([r for r in rows if r["feats"].get("park", -1) >= 1.1 and r["home"]], "park>=1.1 HOME batter (double-count?)")
cal([r for r in rows if r["feats"].get("park", -1) >= 1.1 and not r["home"]], "park>=1.1 AWAY batter")
cal([r for r in rows if r["feats"].get("park", 9) < 1.0], "park factor < 1.0")

print("\n== pitcher factor (when in top-3) ==")
cal([r for r in rows if "pitcher" not in r["feats"]], "pitcher not in top3")
for lo, hi in [(0.0, 1.0), (1.0, 1.1), (1.1, 1.45), (1.45, 1.55), (1.55, 1.65)]:
    cal([r for r in rows if lo <= r["feats"].get("pitcher", -1) < hi], f"pitcher factor {lo:.2f}-{hi:.2f}")

print("\n== environment (when in top-3) ==")
cal([r for r in rows if r["feats"].get("temperature", 0) >= 1.08], "temp factor >= 1.08")
cal([r for r in rows if 1.0 < r["feats"].get("temperature", 0) < 1.08], "temp factor 1.00-1.08")
cal([r for r in rows if r["feats"].get("wind", 0) >= 1.08], "wind factor >= 1.08")
cal([r for r in rows if r["feats"].get("wind", 9) <= 0.95], "wind factor <= 0.95")
cal([r for r in rows if "temperature" not in r["feats"] and "wind" not in r["feats"]], "no env factor in top3")

print("\n== breakout ==")
cal([r for r in rows if r["breakout"] >= 0.10], "breakout score >= +0.10")
cal([r for r in rows if 0.0 < r["breakout"] < 0.10], "breakout 0-0.10")
cal([r for r in rows if r["breakout"] <= 0.0], "breakout <= 0")

print("\n== flags ==")
cal([r for r in rows if r["low_conf"]], "low_confidence=True")
cal([r for r in rows if r["unstable"]], "unstable_recent=True")
cal([r for r in rows if r["stacked"]], "stacked=True")

print("\n== month (temp proxy) ==")
cal([r for r in rows if r["date"] <= "2026-05-31"], "May (5/13-5/31)")
cal([r for r in rows if r["date"] >= "2026-06-01"], "June (6/01-6/08)")

print("\n== lineup spot ==")
for spots, lab in [((1, 2, 3), "spots 1-3"), ((4, 5, 6), "spots 4-6"), ((7, 8, 9), "spots 7-9")]:
    cal([r for r in rows if r["spot"] in spots], lab)
cal([r for r in rows if r["spot"] is None], "no lineup spot (pre-lineup default)")

print("\n== model-vs-market gap (selection severity) ==")
for lo, hi in [(0.0, 1.3), (1.3, 1.6), (1.6, 2.0), (2.0, 99)]:
    cal([r for r in rows if lo <= r["model"] / r["market"] < hi], f"model/market ratio {lo:.1f}-{hi:.1f}")
