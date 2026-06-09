"""Paired comparison: ANCHOR vs TRIPLE, 5/13-6/08 settled picks.

Where do the cohorts differ, and is the ROI difference bigger than day-block noise?
"""
import json, glob, os, random, sys
from collections import defaultdict
from src.pipeline.filters import passes_anchor, passes_triple

START = sys.argv[1] if len(sys.argv) > 1 else "2026-05-13"
random.seed(42)
rows = []
for path in sorted(glob.glob("data/daily_archives/*.json")):
    date = os.path.basename(path)[:10]
    if date < START:
        continue
    d = json.load(open(path))
    s = d.get("settlement") or {}
    for tier in ["primary", "secondary", "shadow"]:
        picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks") or []}
        for r in s.get(f"{tier}_results") or []:
            if r.get("void_reason") is not None or r["outcome"] == "V":
                continue
            p = picks.get((r["batter_id"], r["game_pk"]))
            if p is None:
                continue
            pp = {**p, "tier": tier}
            rows.append({"date": date, "profit": r["profit_units"], "win": r["outcome"] == "W",
                         "anchor": passes_anchor(pp), "triple": passes_triple(pp)})

def st(sel, label):
    n = len(sel)
    if n == 0:
        print(f"{label:34s} n=0")
        return
    w = sum(1 for r in sel if r["win"])
    pl = sum(r["profit"] for r in sel)
    print(f"{label:34s} n={n:4d}  hit={w/n*100:5.1f}%  P/L={pl:+8.2f}u  ROI={pl/n*100:+7.2f}%")

st([r for r in rows if r["anchor"]], "ANCHOR")
st([r for r in rows if r["triple"]], "TRIPLE")
st([r for r in rows if r["anchor"] and r["triple"]], "both")
st([r for r in rows if r["anchor"] and not r["triple"]], "ANCHOR only")
st([r for r in rows if r["triple"] and not r["anchor"]], "TRIPLE only")

# paired day-block bootstrap on ROI(anchor) - ROI(triple)
by = defaultdict(lambda: {"ap": 0.0, "an": 0, "tp": 0.0, "tn": 0})
for r in rows:
    b = by[r["date"]]
    if r["anchor"]:
        b["ap"] += r["profit"]; b["an"] += 1
    if r["triple"]:
        b["tp"] += r["profit"]; b["tn"] += 1
days = sorted(by)
point = (sum(by[d]["ap"] for d in days) / sum(by[d]["an"] for d in days)
         - sum(by[d]["tp"] for d in days) / sum(by[d]["tn"] for d in days)) * 100
diffs = []
for _ in range(20000):
    ds = [days[random.randrange(len(days))] for _ in days]
    ap = sum(by[d]["ap"] for d in ds); an = sum(by[d]["an"] for d in ds)
    tp = sum(by[d]["tp"] for d in ds); tn = sum(by[d]["tn"] for d in ds)
    if an and tn:
        diffs.append((ap / an - tp / tn) * 100)
diffs.sort()
p_gt = sum(1 for x in diffs if x > 0) / len(diffs)
print(f"\nROI(anchor) - ROI(triple): point {point:+.2f}pp")
print(f"95% CI [{diffs[int(0.025*len(diffs))]:+.2f}pp, {diffs[int(0.975*len(diffs))]:+.2f}pp]")
print(f"P(anchor > triple in bootstrap): {p_gt*100:.1f}%")
