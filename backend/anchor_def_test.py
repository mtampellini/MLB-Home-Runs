"""Pick the ANCHOR view definition: audit recommendations as a pick-level overlay.

Components under test (from the 2026-06-09 over-prediction audit):
  A. model/market ratio cap (winner's curse)      — ratio < 1.6 (and floor variants)
  B. blended_hr_per_pa < 0.060 (inflated tail)
  C. breakout_score < 0.10 (hot-streak chase)
  D. pitcher_factor not in [1.10, 1.45) (validated triple component)
Report full window + first/second half for robustness.
"""
import json, glob, os
from collections import defaultdict

TIERS = ["primary", "secondary", "shadow"]

def pf_of(p):
    for f in p.get("top_3_features") or []:
        if f.get("name") == "pitcher":
            return float(f.get("value", 1.0))
    return 1.0

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
            rows.append({
                "date": date, "tier": tier, "win": r["outcome"] == "W", "profit": r["profit_units"],
                "ratio": float(p["model_prob"]) / float(p["market_prob_devig"]),
                "blended": float(p.get("blended_hr_per_pa") or 0),
                "breakout": float(p.get("breakout_score") or 0),
                "pf": pf_of(p),
                "triple": bool((p.get("filter_status") or {}).get("passes_triple")),
            })

H1_END = "2026-05-26"

def ev(sel, label):
    for win, wsel in [("full", sel),
                      ("H1", [r for r in sel if r["date"] <= H1_END]),
                      ("H2", [r for r in sel if r["date"] > H1_END])]:
        n = len(wsel)
        if n == 0:
            print(f"    {win}: n=0")
            continue
        w = sum(1 for r in wsel if r["win"])
        pl = sum(r["profit"] for r in wsel)
        days = len(set(r["date"] for r in wsel))
        print(f"    {win}: n={n:4d} ({n/days:4.1f}/day) hit={w/n*100:5.1f}% P/L={pl:+8.2f}u ROI={pl/n*100:+7.2f}%")

def anchor(r, lo, hi=1.6, use_b=True, use_c=True, use_d=True):
    if not (lo <= r["ratio"] < hi):
        return False
    if use_b and r["blended"] >= 0.060:
        return False
    if use_c and r["breakout"] >= 0.10:
        return False
    if use_d and 1.10 <= r["pf"] < 1.45:
        return False
    return True

print("=== ratio window variants (B+C+D all on) ===")
for lo in (1.10, 1.15, 1.20, 1.25):
    print(f"  ANCHOR ratio [{lo:.2f},1.60):")
    ev([r for r in rows if anchor(r, lo)], "")

print("\n=== component ablations at ratio [1.15,1.60) ===")
print("  no B (blended cap off):"); ev([r for r in rows if anchor(r, 1.15, use_b=False)], "")
print("  no C (breakout cap off):"); ev([r for r in rows if anchor(r, 1.15, use_c=False)], "")
print("  no D (pitcher band off):"); ev([r for r in rows if anchor(r, 1.15, use_d=False)], "")
print("  ratio-only [1.15,1.60):"); ev([r for r in rows if anchor(r, 1.15, use_b=False, use_c=False, use_d=False)], "")

print("\n=== reference cohorts ===")
print("  TRIPLE:"); ev([r for r in rows if r["triple"]], "")
print("  baseline:"); ev(rows, "")

print("\n=== ANCHOR [1.15,1.6) by tier ===")
for t in TIERS:
    print(f"  {t}:"); ev([r for r in rows if r["tier"] == t and anchor(r, 1.15)], "")

print("\n=== overlap with TRIPLE, full window (5/20+ only where tags exist) ===")
a_set = [r for r in rows if r["date"] >= "2026-05-20" and anchor(r, 1.15)]
both = [r for r in a_set if r["triple"]]
print(f"  ANCHOR n={len(a_set)}, also-TRIPLE n={len(both)} ({len(both)/max(1,len(a_set))*100:.0f}%)")
