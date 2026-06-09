import json, glob, os
from collections import defaultdict

TIERS = ["primary", "secondary", "shadow"]
REBUILD = "2026-05-13"
TRIPLE = "2026-05-20"

rows = []  # one per settled pick
days_missing_settlement = []
for path in sorted(glob.glob("data/daily_archives/*.json")):
    date = os.path.basename(path)[:10]
    if date < REBUILD:
        continue
    d = json.load(open(path))
    s = d.get("settlement") or {}
    if not s:
        days_missing_settlement.append(date)
        continue
    for tier in TIERS:
        picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks") or []}
        for r in s.get(f"{tier}_results") or []:
            p = picks.get((r["batter_id"], r["game_pk"]))
            fs = (p or {}).get("filter_status") or {}
            rows.append({
                "date": date, "tier": tier,
                "outcome": r["outcome"], "profit": r["profit_units"],
                "model_prob": r.get("model_prob"), "ev_pct": r.get("ev_pct"),
                "void": r.get("void_reason") is not None or r["outcome"] == "V",
                "triple": fs.get("passes_triple"), "quad": fs.get("passes_quad"),
                "baseline": fs.get("passes_baseline"),
                "matched": p is not None,
            })

unmatched = sum(1 for r in rows if not r["matched"])
print(f"settled rows: {len(rows)}, unmatched to picks: {unmatched}")
if days_missing_settlement:
    print("days without settlement (excluded):", days_missing_settlement)

def summarize(sel, label):
    sel = [r for r in sel if not r["void"]]
    n = len(sel)
    if n == 0:
        print(f"{label:42s} n=0")
        return
    w = sum(1 for r in sel if r["outcome"] == "W")
    pl = sum(r["profit"] for r in sel)
    exp = sum(r["model_prob"] for r in sel if r["model_prob"] is not None) / n
    print(f"{label:42s} n={n:4d}  W={w:3d}  hit={w/n*100:5.1f}%  exp={exp*100:5.1f}%  P/L={pl:+8.2f}u  ROI={pl/n*100:+6.1f}%")

for window, start in [("POST-REBUILD (5/13+)", REBUILD), ("POST-TRIPLE-SHIP (5/20+)", TRIPLE)]:
    wr = [r for r in rows if r["date"] >= start]
    print(f"\n=== {window} ===  dates {min(r['date'] for r in wr)}..{max(r['date'] for r in wr)}, {len(set(r['date'] for r in wr))} slates")
    for tier in TIERS:
        summarize([r for r in wr if r["tier"] == tier], f"{tier} (all settled picks)")
    summarize(wr, "ALL TIERS (all settled picks)")
    print("-- filter cohorts (all tiers) --")
    summarize([r for r in wr if r["triple"]], "TRIPLE (passes_triple)")
    summarize([r for r in wr if r["baseline"] and not r["triple"]], "dropped by triple (baseline & !triple)")
    summarize([r for r in wr if r["quad"]], "QUAD (passes_quad)")
    print("-- triple by tier --")
    for tier in TIERS:
        summarize([r for r in wr if r["tier"] == tier and r["triple"]], f"{tier} TRIPLE")

# day-by-day, post-triple window, triple-filtered all tiers + primary-all for texture
print("\n=== day-by-day (post-rebuild): primary-all P/L | TRIPLE all-tiers P/L ===")
bydate = defaultdict(lambda: {"prim": 0.0, "prim_n": 0, "tri": 0.0, "tri_n": 0})
for r in rows:
    if r["void"]:
        continue
    b = bydate[r["date"]]
    if r["tier"] == "primary":
        b["prim"] += r["profit"]; b["prim_n"] += 1
    if r["triple"]:
        b["tri"] += r["profit"]; b["tri_n"] += 1
cum_p = cum_t = 0.0
for date in sorted(bydate):
    b = bydate[date]
    cum_p += b["prim"]; cum_t += b["tri"]
    print(f"{date}  primary n={b['prim_n']:3d} {b['prim']:+7.2f}u (cum {cum_p:+8.2f})   TRIPLE n={b['tri_n']:3d} {b['tri']:+7.2f}u (cum {cum_t:+8.2f})")

# calibration, post-rebuild, all tiers, non-void
print("\n=== calibration (post-rebuild, all tiers, non-void) ===")
bins = [(0.05,0.10),(0.10,0.15),(0.15,0.20),(0.20,0.25),(0.25,0.30),(0.30,0.40),(0.40,1.0)]
nv = [r for r in rows if not r["void"] and r["model_prob"] is not None]
for lo, hi in bins:
    sel = [r for r in nv if lo <= r["model_prob"] < hi]
    if not sel:
        continue
    n = len(sel); w = sum(1 for r in sel if r["outcome"] == "W")
    exp = sum(r["model_prob"] for r in sel)/n
    print(f"{lo*100:3.0f}-{hi*100:3.0f}%: n={n:4d}  exp={exp*100:5.1f}%  act={w/n*100:5.1f}%  diff={(w/n-exp)*100:+5.1f}pp")
