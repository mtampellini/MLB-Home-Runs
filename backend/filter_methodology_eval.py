"""Exploratory re-evaluation of the TRIPLE filter methodology (NOT pre-registered).

Part 1: component attribution of the triple filter, OOS window 5/20+.
Part 2: candidate alternative methodologies, train 5/13-5/26 -> validate 5/27-6/08.
"""
import json, glob, os, math, random
from collections import defaultdict

random.seed(42)
TIERS = ["primary", "secondary", "shadow"]
TIER_EV_MIN = {"primary": 25.0, "secondary": 25.0, "shadow": 10.0}
TRAIN_END = "2026-05-26"   # inclusive
SHIP = "2026-05-20"

def pitcher_factor(pick):
    for f in pick.get("top_3_features") or []:
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
    if not s:
        continue
    for tier in TIERS:
        picks = {(p["batter_id"], p["game_pk"]): p for p in d.get(f"{tier}_picks") or []}
        for r in s.get(f"{tier}_results") or []:
            if r.get("void_reason") is not None or r["outcome"] == "V":
                continue
            p = picks.get((r["batter_id"], r["game_pk"]))
            if p is None:
                continue
            fs = p.get("filter_status") or {}
            ev = float(p.get("ev_pct", 0.0))
            pf = pitcher_factor(p)
            stacked = bool(p.get("stacked"))
            fail_ev50 = ev >= 50.0
            fail_pf = 1.10 <= pf < 1.45
            fail_stack = stacked and ev * 0.7 < TIER_EV_MIN.get(tier, 25.0)
            recomputed = not (fail_ev50 or fail_pf or fail_stack)
            rows.append({
                "date": date, "tier": tier, "profit": r["profit_units"],
                "win": r["outcome"] == "W",
                "model": float(p["model_prob"]), "market": float(p["market_prob_devig"]),
                "ev": ev, "pf": pf, "stacked": stacked,
                "low_conf": bool(p.get("low_confidence")),
                "fail_ev50": fail_ev50, "fail_pf": fail_pf, "fail_stack": fail_stack,
                "triple_tag": bool(fs.get("passes_triple")), "triple_rc": recomputed,
            })

mismatch = sum(1 for r in rows if r["date"] >= SHIP and r["triple_tag"] != r["triple_rc"])
print(f"rows={len(rows)}; recomputed-vs-tag mismatches (5/20+): {mismatch}")

def stats(sel):
    n = len(sel)
    if n == 0:
        return 0, 0, 0.0, 0.0
    w = sum(1 for r in sel if r["win"])
    pl = sum(r["profit"] for r in sel)
    return n, w, pl, pl / n * 100

def show(sel, label):
    n, w, pl, roi = stats(sel)
    hr = (w / n * 100) if n else 0
    print(f"  {label:46s} n={n:4d} hit={hr:5.1f}% P/L={pl:+8.2f}u ROI={roi:+7.2f}%")

def boot(sel, resamples=20000):
    by = defaultdict(lambda: [0.0, 0])
    for r in sel:
        by[r["date"]][0] += r["profit"]; by[r["date"]][1] += 1
    days = sorted(by)
    rois = []
    for _ in range(resamples):
        ds = [days[random.randrange(len(days))] for _ in days]
        pl = sum(by[d][0] for d in ds); c = sum(by[d][1] for d in ds)
        rois.append(pl / c * 100 if c else 0.0)
    rois.sort()
    p = sum(1 for x in rois if x <= 0) / resamples
    return p, rois[int(0.025 * resamples)], rois[int(0.975 * resamples)]

# ---------------- Part 1: component attribution, OOS 5/20+ ----------------
oos = [r for r in rows if r["date"] >= SHIP]
print(f"\n=== PART 1: triple component attribution (OOS {SHIP}+) ===")
print("Picks dropped by each component (overlaps possible):")
show([r for r in oos if r["fail_ev50"]], "dropped by EV>=50 only-rule")
show([r for r in oos if r["fail_pf"]], "dropped by pitcher_factor [1.10,1.45)")
show([r for r in oos if r["fail_stack"]], "dropped by stacked-shade")
print("Relax one component at a time (what triple would be without it):")
show([r for r in oos if r["triple_rc"]], "TRIPLE (all three)")
show([r for r in oos if not (r["fail_pf"] or r["fail_stack"])], "without EV>=50 rule")
show([r for r in oos if not (r["fail_ev50"] or r["fail_stack"])], "without pitcher-factor rule")
show([r for r in oos if not (r["fail_ev50"] or r["fail_pf"])], "without stacked-shade rule")
print("Single-component-only filters:")
show([r for r in oos if not r["fail_ev50"]], "EV<50 only")
show([r for r in oos if not r["fail_pf"]], "pitcher-factor rule only")
show([r for r in oos if not r["fail_stack"]], "stacked-shade rule only")

# ---------------- Part 2: candidate methodologies ----------------
train = [r for r in rows if r["date"] <= TRAIN_END]
valid = [r for r in rows if r["date"] > TRAIN_END]
print(f"\n=== PART 2: candidates — train 5/13..{TRAIN_END} (n={len(train)}), validate {min(r['date'] for r in valid)}..{max(r['date'] for r in valid)} (n={len(valid)}) ===")

def logloss(sel, prob_fn):
    tot = 0.0
    for r in sel:
        p = min(max(prob_fn(r), 1e-6), 1 - 1e-6)
        tot += -(math.log(p) if r["win"] else math.log(1 - p))
    return tot / len(sel)

# w*model + (1-w)*market, fit on train
best_w, best_ll = 0.0, 1e9
for i in range(51):
    w = i / 50
    ll = logloss(train, lambda r, w=w: w * r["model"] + (1 - w) * r["market"])
    if ll < best_ll:
        best_ll, best_w = ll, w
print(f"market-blend weight fit on train: w={best_w:.2f} (model weight), logloss={best_ll:.4f}")
print(f"  reference logloss: model-only={logloss(train, lambda r: r['model']):.4f}, market-only={logloss(train, lambda r: r['market']):.4f}")

# Platt-style recalibration p = sigmoid(a + b*logit(model)), grid fit on train
def logit(p):
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))
best_ab, best_ll2 = (0.0, 1.0), 1e9
for ai in range(-30, 11):
    a = ai / 10
    for bi in range(4, 31):
        b = bi / 20
        ll = logloss(train, lambda r, a=a, b=b: 1 / (1 + math.exp(-(a + b * logit(r["model"])))))
        if ll < best_ll2:
            best_ll2, best_ab = ll, (a, b)
a, b = best_ab
print(f"platt recalibration fit on train: a={a:.2f} b={b:.2f}, logloss={best_ll2:.4f}")

def p_blend(r):
    return best_w * r["model"] + (1 - best_w) * r["market"]
def p_platt(r):
    return 1 / (1 + math.exp(-(a + b * logit(r["model"]))))
def ev_of(r, fn):
    return (fn(r) / r["market"] - 1) * 100

# choose thresholds on train to match triple's train volume
triple_train_n = sum(1 for r in train if r["triple_rc"])
def matched_threshold(score_fn):
    scores = sorted((score_fn(r) for r in train), reverse=True)
    return scores[min(triple_train_n, len(scores)) - 1]
t_blend = matched_threshold(lambda r: ev_of(r, p_blend))
t_platt = matched_threshold(lambda r: ev_of(r, p_platt))
print(f"volume-matched thresholds (triple kept {triple_train_n}/{len(train)} on train): blend-EV>={t_blend:.1f}%, platt-EV>={t_platt:.1f}%")

candidates = {
    "TRIPLE (current)": lambda r: r["triple_rc"],
    "blend-EV (shrink to market, w=%.2f)" % best_w: lambda r: ev_of(r, p_blend) >= t_blend,
    "platt-EV (recalibrated model)": lambda r: ev_of(r, p_platt) >= t_platt,
    "TRIPLE + blend-EV>=5%": lambda r: r["triple_rc"] and ev_of(r, p_blend) >= 5.0,
    "EV<50 rule only": lambda r: not r["fail_ev50"],
    "unfiltered baseline": lambda r: True,
}

for window, sel0 in [("TRAIN (in-sample for candidates)", train), ("VALIDATION (honest comparison)", valid)]:
    print(f"\n-- {window} --")
    for name, fn in candidates.items():
        show([r for r in sel0 if fn(r)], name)

print("\nBootstrap on VALIDATION window (day-block, 20k):")
for name, fn in candidates.items():
    sel = [r for r in valid if fn(r)]
    if not sel:
        continue
    n, w_, pl, roi = stats(sel)
    p, lo, hi = boot(sel)
    print(f"  {name:46s} ROI={roi:+7.2f}%  p(ROI<=0)={p:.3f}  95% CI [{lo:+6.1f}%, {hi:+6.1f}%]")

# calibration of blend vs raw on validation
print("\nValidation calibration (expected vs actual hit), raw model vs blend:")
for label, fn in [("raw model", lambda r: r["model"]), ("blend", p_blend), ("platt", p_platt)]:
    exp = sum(fn(r) for r in valid) / len(valid)
    act = sum(1 for r in valid if r["win"]) / len(valid)
    print(f"  {label:10s} exp={exp*100:5.1f}%  act={act*100:5.1f}%  gap={(act-exp)*100:+5.1f}pp")
