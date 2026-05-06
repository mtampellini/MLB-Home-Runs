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

## 4. Variant B vs production breakout — name mismatch (resolved 2026-05-06)

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
