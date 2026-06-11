"""H6 readout: mid-band (model_prob 10-20%) vs rest-of-TRIPLE, primary+shadow.

Pre-registered 2026-06-10 (see docs/filter_experiment.md). The band was found
by mining 5/20-6/10, so only dates from 2026-06-11 on are out-of-sample.

Usage: python band_vs_triple.py [start_date]   (default 2026-06-11 = OOS only)
"""
import json, glob, os, random, sys
from collections import defaultdict
from src.pipeline.filters import passes_triple

BAND_LO, BAND_HI = 0.10, 0.20
TIERS = ["primary", "shadow"]

START = sys.argv[1] if len(sys.argv) > 1 else "2026-06-11"
random.seed(42)
rows = []
for path in sorted(glob.glob("data/daily_archives/*.json")):
    date = os.path.basename(path)[:10]
    if date < START:
        continue
    d = json.load(open(path))
    s = d.get("settlement") or {}
    for tier in TIERS:
        picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks") or []}
        for r in s.get(f"{tier}_results") or []:
            if r.get("void_reason") is not None or r["outcome"] not in ("W", "L"):
                continue
            p = picks.get((r["batter_id"], r["game_pk"]))
            if p is None or not passes_triple({**p, "tier": tier}):
                continue
            mp = p.get("model_prob")
            if mp is None:
                continue
            rows.append({"date": date, "profit": r["profit_units"], "win": r["outcome"] == "W",
                         "mp": mp, "band": BAND_LO <= mp < BAND_HI})

def st(sel, label):
    n = len(sel)
    if n == 0:
        print(f"{label:26s} n=0")
        return
    w = sum(1 for r in sel if r["win"])
    pl = sum(r["profit"] for r in sel)
    exp = sum(r["mp"] for r in sel) / n
    print(f"{label:26s} n={n:4d}  hit={w/n*100:5.1f}%  exp={exp*100:5.1f}%  "
          f"P/L={pl:+8.2f}u  ROI={pl/n*100:+7.2f}%")

print(f"TRIPLE primary+shadow, settled picks since {START}")
st(rows, "all TRIPLE P+S")
st([r for r in rows if r["band"]], "BAND 10-20%")
st([r for r in rows if not r["band"]], "outside band")

# Day-block bootstrap: H6a P(ROI(band) > 0) and H6b ROI(band) - ROI(outside).
by = defaultdict(lambda: {"bp": 0.0, "bn": 0, "op": 0.0, "on": 0})
for r in rows:
    b = by[r["date"]]
    if r["band"]:
        b["bp"] += r["profit"]; b["bn"] += 1
    else:
        b["op"] += r["profit"]; b["on"] += 1
days = sorted(by)
if not days or not any(by[d]["bn"] for d in days):
    print("\nno band picks settled yet — nothing to bootstrap")
    sys.exit(0)
rois, diffs = [], []
for _ in range(20000):
    ds = [days[random.randrange(len(days))] for _ in days]
    bp = sum(by[d]["bp"] for d in ds); bn = sum(by[d]["bn"] for d in ds)
    op = sum(by[d]["op"] for d in ds); on = sum(by[d]["on"] for d in ds)
    if bn:
        rois.append(bp / bn * 100)
        if on:
            diffs.append((bp / bn - op / on) * 100)
rois.sort(); diffs.sort()
point_b = sum(by[d]["bp"] for d in days) / sum(by[d]["bn"] for d in days) * 100
print(f"\nH6a ROI(band): point {point_b:+.2f}%  "
      f"95% CI [{rois[int(0.025*len(rois))]:+.2f}%, {rois[int(0.975*len(rois))]:+.2f}%]  "
      f"P(>0)={sum(1 for x in rois if x > 0)/len(rois)*100:.1f}%")
if diffs:
    on_tot = sum(by[d]["on"] for d in days)
    point_d = point_b - sum(by[d]["op"] for d in days) / on_tot * 100
    print(f"H6b ROI(band) - ROI(outside): point {point_d:+.2f}pp  "
          f"95% CI [{diffs[int(0.025*len(diffs))]:+.2f}pp, {diffs[int(0.975*len(diffs))]:+.2f}pp]  "
          f"P(band > outside)={sum(1 for x in diffs if x > 0)/len(diffs)*100:.1f}%")
