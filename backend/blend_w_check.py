"""Quick check: optimal model-vs-market blend weight on different windows/subsets."""
import json, glob, os, math

TIERS = ["primary", "secondary", "shadow"]
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
            fs = p.get("filter_status") or {}
            rows.append({"date": date, "tier": tier, "win": r["outcome"] == "W",
                         "model": float(p["model_prob"]), "market": float(p["market_prob_devig"]),
                         "triple": bool(fs.get("passes_triple"))})

def fit_w(sel):
    best_w, best_ll = 0.0, 1e18
    for i in range(101):
        w = i / 100
        tot = 0.0
        for r in sel:
            p = min(max(w * r["model"] + (1 - w) * r["market"], 1e-6), 1 - 1e-6)
            tot += -(math.log(p) if r["win"] else math.log(1 - p))
        ll = tot / len(sel)
        if ll < best_ll:
            best_ll, best_w = ll, w
    return best_w, best_ll

def ll_of(sel, fn):
    tot = 0.0
    for r in sel:
        p = min(max(fn(r), 1e-6), 1 - 1e-6)
        tot += -(math.log(p) if r["win"] else math.log(1 - p))
    return tot / len(sel)

for label, sel in [
    ("full 5/13-6/08", rows),
    ("train 5/13-5/26", [r for r in rows if r["date"] <= "2026-05-26"]),
    ("valid 5/27-6/08", [r for r in rows if r["date"] > "2026-05-26"]),
    ("triple picks only (5/20+)", [r for r in rows if r["date"] >= "2026-05-20" and r["triple"]]),
    ("primary only", [r for r in rows if r["tier"] == "primary"]),
    ("shadow only", [r for r in rows if r["tier"] == "shadow"]),
]:
    w, ll = fit_w(sel)
    print(f"{label:28s} n={len(sel):4d}  best w={w:.2f}  logloss(blend)={ll:.4f}  model-only={ll_of(sel, lambda r: r['model']):.4f}  market-only={ll_of(sel, lambda r: r['market']):.4f}")
