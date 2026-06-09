import json, glob, os, random
from collections import defaultdict

random.seed(42)
START = "2026-05-20"
TIERS = ["primary", "secondary", "shadow"]

rows = []
for path in sorted(glob.glob("data/daily_archives/*.json")):
    date = os.path.basename(path)[:10]
    if date < START:
        continue
    d = json.load(open(path))
    s = d.get("settlement") or {}
    if not s:
        continue
    for tier in TIERS:
        picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks") or []}
        for r in s.get(f"{tier}_results") or []:
            if r.get("void_reason") is not None or r["outcome"] == "V":
                continue
            fs = (picks.get((r["batter_id"], r["game_pk"])) or {}).get("filter_status") or {}
            rows.append({"date": date, "tier": tier, "profit": r["profit_units"],
                         "win": r["outcome"] == "W", "triple": bool(fs.get("passes_triple"))})

def daily(sel):
    by = defaultdict(lambda: [0.0, 0])
    for r in sel:
        by[r["date"]][0] += r["profit"]
        by[r["date"]][1] += 1
    return by

def boot_test(sel, label, resamples=20000):
    by = daily(sel)
    days = sorted(by)
    n = sum(by[d][1] for d in days)
    pl = sum(by[d][0] for d in days)
    roi = pl / n * 100
    wins = sum(1 for r in sel if r["win"])
    # day-block bootstrap of ROI
    rois = []
    for _ in range(resamples):
        ds = [days[random.randrange(len(days))] for _ in days]
        p = sum(by[d][0] for d in ds)
        c = sum(by[d][1] for d in ds)
        rois.append(p / c * 100 if c else 0.0)
    rois.sort()
    p_le0 = sum(1 for x in rois if x <= 0) / resamples
    lo95 = rois[int(0.025 * resamples)]
    hi95 = rois[int(0.975 * resamples)]
    lo90 = rois[int(0.05 * resamples)]
    print(f"{label:38s} n={n:4d} W={wins:3d} ({wins/n*100:4.1f}%) P/L={pl:+8.2f}u ROI={roi:+6.2f}%")
    print(f"{'':38s} 95% CI [{lo95:+6.2f}%, {hi95:+6.2f}%]  one-sided p(ROI<=0)={p_le0:.4f}")
    # daily P/L spread for context
    worst = min(by[d][0] for d in days); best = max(by[d][0] for d in days)
    neg_days = sum(1 for d in days if by[d][0] < 0)
    print(f"{'':38s} {len(days)} days, {neg_days} red; best day {best:+.2f}u, worst {worst:+.2f}u; 90% lower bound {lo90:+.2f}%")
    print()

print(f"Window {START}..{max(r['date'] for r in rows)}, day-block bootstrap 20k resamples\n")
boot_test([r for r in rows if r["triple"] and r["tier"] in ("primary", "shadow")], "TRIPLE primary+shadow")
boot_test([r for r in rows if r["triple"] and r["tier"] == "primary"], "TRIPLE primary only")
boot_test([r for r in rows if r["triple"] and r["tier"] == "shadow"], "TRIPLE shadow only")
boot_test([r for r in rows if r["triple"]], "TRIPLE all tiers (incl secondary)")
boot_test([r for r in rows if r["tier"] in ("primary", "shadow")], "primary+shadow UNFILTERED (reference)")
