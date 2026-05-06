# Known issues — HR-Picks-V7

Items flagged by the 2026-05-06 feature-importance research that are NOT
addressed in production yet. We don't tune on hunches; revisit each after
30 days of CLV data accumulates.

## 1. `pitcher_barrel_pct_allowed` — gain rank 9, SHAP rank 14

LightGBM gain says it's mid-tier useful (rank 9 of 27 features). Mean-|SHAP|
on the holdout puts it 5 ranks lower (rank 14). That gap is the canonical
signature of a feature whose train-set patterns don't fully generalize:
the model exploits something during training that doesn't pay off on holdout.

**Status:** kept in the model (variant C). Not weighted explicitly anywhere.
**Revisit when:** we have 30+ days of logged picks; check whether picks
where this feature was load-bearing have positive or negative CLV. If
negative, drop the feature from any future ML training input.

## 2. `pull_air_pct_season` — gain rank 7, split rank 2, SHAP rank 6

LightGBM uses this feature in many splits (rank 2 by split count) but each
split contributes only modestly (rank 7 by gain, rank 6 by SHAP). That
pattern is "model is hunting for marginal signal" — borderline keep/drop.

**Status:** kept in the V7 baseline as part of breakout-weight-rebalance.
We weight `pull_air_pct` at 5.0 (per the 2026-05-06 review-gate decision)
to capture the season-level signal without over-relying on it.
**Revisit when:** we have 30+ days of logged picks. If picks driven by
high `pull_air_pct` show negative CLV, reduce its weight or drop.

## 3. `barrel_pct_30d` — 0.0 importance across all three views

The 30-day version of barrel rate has zero LightGBM importance when the
season version is also a feature. The model has so much signal from
`barrel_pct_season` that it doesn't bother splitting on the recent window.

**Already addressed:** `breakout.py` no longer treats the 30d/season ratio
as a primary signal. Instead, the *change* between the two is surfaced
as `trend_signal` in picks.json (informational only, not scored). See
`backend/src/features/breakout.py::compute_recent_form_flags`.

## 4. Leadoff hitters may be inflated by PA-count effect

Per-game P(HR ≥ 1) is computed as `1 - (1 - per_PA_rate)^pa_per_game`. The
`pa_per_game` table favors leadoff (4.6) over the bottom of the order (3.6),
which compounds positively with the per-PA rate.

In the 2026-05-06 smoke run, Drake Baldwin showed up at #5 (32.4%) despite a
moderate blended rate (0.044) — significantly amplified by his #1 lineup spot
(4.6 PA per game). A solid #6-#9 hitter with a higher per-PA rate may rank
below him simply because they get fewer at-bats.

That's not necessarily wrong — leadoff hitters DO get more chances per game.
But it's a known compounding effect worth tracking.

**Status:** kept as-is in V7 baseline. The `pa_per_game` table is empirically
defensible.
**Revisit when:** after 30 days of paper-trade results, segment picks by
`lineup_spot`. If leadoff picks underperform their model probability vs the
rest of the slate, add a position-based adjustment (e.g., dampen leadoff
lift, or use a more conservative `pa_per_game` value for spots 1-2).

## 4b. Primary picks capped at top 10 by EV/day (paper-trade safety)

Pre-game smoke runs surfaced 21 picks/day above 25% EV. Most look like real
elite-hitter spots; some are likely mild over-confidence on small-sample
batters. Without 60 days of CLV data we can't tell which is which.

**Decision:** primary tier (the only one displayed on the front-end and
counted against your bankroll) is capped at the top 10 by EV. Picks at
ev_pct >= 25% but ranked 11+ go to a SECONDARY tier (`secondary_picks.json`)
which is settled and tracked but never displayed.

Why 10:
- Empirical research (public-domain HR-prop modeling) suggests sustainable
  +EV pick volume is in the 5-15/day range. 10 sits in the middle.
- Bigger than the original 8-pick gate to capture more calibration data.
- Negligible additional risk vs 8 — the 9th and 10th pick each add ~10%
  to bankroll exposure.

**Revisit when:** 60 days of paper-trade CLV data exists. Compare per-day
ROI / hit rate of (top 10) vs (rank 11-20). If they're indistinguishable,
the cap is arbitrary noise and we can lift it. If primary outperforms
secondary, the EV ranking is meaningful and the cap stays.

## 5. Bullpen exposure not modeled — `pitcher_factor` applied to all PAs

The matchup multiplier uses the **starting pitcher's** vs-RHB or vs-LHB
HR/9, blended (season + 30d). It's applied to every PA the batter is
projected for in the game (4.6 PAs at #1 down to 3.6 at #9).

In reality, modern starters average 5–6 IP per start (~62% of a 9-inning
game). The remaining ~38% of PAs come against the bullpen. With the
current model the starter's HR-prone-ness is over-applied, and bullpen
quality is ignored entirely.

This was flagged in the early Phase-3 review when the user pushed back on
my pitcher-factor divisor. We deferred a proper fix until we have logged
data to validate the cleaner formulation:

```
p_game = 1 - (1 - p_per_PA_starter)^pa_vs_starter
            × (1 - p_per_PA_bullpen)^pa_vs_bullpen
```

That requires a per-team bullpen quality estimate that V7 doesn't compute.

**Status:** known bias, not currently corrected. Effect direction depends
on whether the starter is more or less HR-prone than his team's bullpen.
For most slates, pitcher-factor extremes (e.g. our 1.58× for Bryan Woo)
would be diluted toward 1.0 in a properly bullpen-weighted version.
**Revisit when:** after 30 days of CLV data. Segment picks by starter's
pitcher_factor magnitude (say >1.4 vs <1.2). If high-pitcher-factor
picks underperform their model probability, that's the bullpen-dilution
signal — implement the split formula and a bullpen-quality feature.

## 6. Variant B vs production breakout — name mismatch (resolved 2026-05-06)

The original spec called the four breakout metrics
`{barrel, sweet_spot, pull_air, max_ev}`. The pre-research implementation
used `{xwobacon, barrel, hardhit, avg_ev}` — only `barrel` overlapped.

**Status:** resolved. After the research showed barrel-pct dominates and
the spec set's other three (sweet_spot, pull_air, max_ev) all rank in the
top half of LightGBM importance, the production weights now match the
spec set. See commit on `v7-rewrite` branch.

---

## How to dispose of items here

After 30 days of paper-trade data:
1. Pull `backend/data/processed/results_*.json` and `picks_*.json`.
2. For each item above, segment picks by whether the flagged feature was
   in `top_3_features`. Compute CLV % on each segment.
3. If the flagged-feature segment has CLV ≥ 0, the feature is justified —
   delete the item from this file.
4. If the segment has CLV materially below the rest, drop or down-weight
   the feature, retrain, and delete.

Negative CLV in a segment AFTER 30+ days is the only acceptable evidence
for tuning. Don't move on hunches — they're how V4 broke.
