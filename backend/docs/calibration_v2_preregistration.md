# Calibration-v2 (P3 drop-only) — shipped + pre-registered 2026-06-23

Committed AT SHIP TIME, before any live calibration-v2 picks have settled.
This is the standard the change must clear to stay in production. Decided
with no live results in hand; do not renegotiate after results are visible.

## What shipped (MODEL_VERSION v7-baseline-0.2.0 -> v7-cal2-0.3.0)

The production site filter switched from `passes_triple` to
`passes_triple_v2` (P3 DROP-ONLY) in `src/pipeline/run_daily.py`. The new
filter (`src/pipeline/filters.py`):

1. Requires a pick to FIRST pass the live triple on its real EV (so the
   EV>=50 over-confidence ceiling is judged on the original prob -> removing
   the breakout boost can never READMIT a ceiling-dropped pick), then
2. ALSO requires it to clear tier-min (and the stacked shade) on a
   **breakout-neutralized EV** (`ev_pct_p3`, computed in
   `_assemble_pick`: divide the multiplicative breakout factor back out of
   the final per-PA rate, re-price at the same book).

Net: a strict SUBSET of `passes_triple`. It only removes picks that cleared
the floor solely on the hot-streak boost; it never adds. `model_prob` /
`ev_pct` are UNCHANGED (Option B), so CLV recovery, full-slate logging, and
tracker calibration stay on one scale across the version bump. The old
`passes_triple`, `passes_quad`, `passes_anchor` tags keep being recorded.

## Why (the diagnosis, 2026-06-23)

The triple filter's edge died around 2026-06-09. By BOTH lenses:

- ROI: triple-kept ran -22.7% over 6/09-6/22 (vs +11.5% OOS at the 6/09
  checkpoint); the cohort it dropped ran +4.7% — the filter INVERTED.
- CLV: triple-kept beat-close fell from 54% (through 6/08, CI excluding 0,
  real edge) to ~48% (6/09+, CI includes 0) — edge no longer demonstrable.

The model over-predicts in every calibration bin; the breakout boost is a
material contributor. Backtest (train 5/13-5/26, validation extended through
6/22) showed P3 drop-only is the conservative survivor: applying the SHIPPED
filter to stored picks, kept ROI ran +2.9% (extended) / -4.0% (decay) vs the
archived triple's -10.3% / -22.7% on the same slates.

## Honest framing: this is DEFENSE, not a restored edge

The CLV cross-check (decay window) is explicit: the picks P3 drops were NOT
mispriced — deselected-cohort CLV (+0.038pp) was no worse than kept-cohort
CLV (+0.001pp). So a large part of the ROI recovery is the dropped picks'
bad luck, not a pricing edge P3 harvests. Expected forward value of
calibration-v2 = lower variance + roughly half the exposure (cuts ~40-50%
of picks) + avoided worst-priced bets, moving P/L from bleeding toward
break-even. It is NOT expected to reproduce the backtest's headline ROI.
The real edge fix is the P5 ML rebuild (see p5_preregistration.md).

## Pre-registered live evaluation (readout ~2026-07-23, ~30 days)

Cohorts, all on slates dated 2026-06-23+ (calibration-v2 live):
- **KEPT** = picks with `passes_triple_v2` (what the site now bets).
- **CUT** = picks with `passes_triple` AND NOT `passes_triple_v2` (the
  picks v2 newly drops vs the old filter). This is the decision cohort.

Measured over the first ~30 days or ~150 KEPT picks, whichever later, with
CLV exactly as `clv_recover.py` measures it (de-vigged entry vs latest
pre-game snapshot), ROI from settlement `profit_units`, 95% CIs by
day-block bootstrap.

### Primary gate — did dropping help (or at least not hurt)?

1. **CUT-cohort CLV does NOT exceed KEPT-cohort CLV by a significant
   margin** — i.e. the picks we removed were not the better-priced ones.
   Formally: mean CLV(CUT) - mean CLV(KEPT) < +0.10pp, OR its 95% CI
   includes 0. If CUT clearly beats KEPT on CLV (lower CI > +0.10pp), v2 is
   cutting value -> **REVERT to triple**.
2. **KEPT-cohort ROI >= CUT-cohort ROI** (point estimate). Confirmatory,
   not decisive (ROI is noisy at n~150).

### Secondary gates (sanity)

- KEPT volume >= ~6 picks/day (if v2 starves the slate, loosen tier-min
  rather than silently shipping near-zero picks).
- Full-slate logloss (`data/full_slate/`) no worse than at registration.
- No guard-cohort damage vs the archived-triple baseline on the same window.

## Outcomes

- **KEPT >= CUT on CLV (or indistinguishable)** -> calibration-v2 stays in
  production; revisit alongside the P5 readout (~7/19+).
- **CUT clearly beats KEPT on CLV (CI excludes +0.10pp)** -> REVERT to
  `passes_triple`; the drop-only filter was removing fairly-priced value.
- **Both cohorts deeply negative on ROI with flat/negative CLV** -> the
  model has no live edge regardless of filter; cut stakes / pause new bets
  and prioritize the P5 rebuild. A filter cannot manufacture an edge.

## Anti-goalpost clause

The decision cohort (CUT vs KEPT on CLV) and the +0.10pp threshold are
fixed here, before any 6/23+ pick settles. Do not switch the primary metric
to whichever lens looks best at readout. If the window is underpowered
(wide CIs spanning the threshold), the outcome is "extend, do not act,"
not "pick the favorable read."
