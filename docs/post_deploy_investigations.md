# Post-deploy investigations

Things to look at once we have logged data. Each item includes the question
to ask, the data needed, and the trigger condition. None of these are
blocking deploy — they're calibration / model-improvement work for after
paper-trade phase produces enough signal.

---

## Pitcher early-season pattern detection

**Question:** Do some pitchers consistently start hot or cold relative to
their full-season performance? If so, the pitcher-factor shrinkage we apply
in `baseline.py` is treating a real signal as noise for those specific
pitchers.

**Data needed:**
- Multi-year pitcher data with at least two seasons.
- For each pitcher, compute:
  - `early_factor_year_n` = HR/9 over first 8 starts of season N
  - `full_factor_year_n`  = HR/9 over the full season N
  - delta_n = early - full
- Look at correlation of `delta_n` across years for the same pitcher (does
  a pitcher who runs hot in their first 8 starts of 2024 also run hot in
  their first 8 starts of 2025?).

**Hypothesis:** Most pitchers won't show consistent patterns and noise will
dominate. A few might (mechanical issues that resolve with reps,
weather-sensitivity, ramping fastball velocity, etc.).

**Trigger to invest more:** Worth the complexity only if the pattern is
**both meaningful and stable**:
- Meaningful: |delta| > 0.3 HR/9 averaged across years for a non-trivial
  set of pitchers (say, 20+).
- Stable: year-over-year correlation of delta_n is significantly positive
  (Pearson r > 0.3 with p < 0.05).

If both: replace the uniform shrinkage in `BaselineConfig.pitcher_shrinkage_innings`
with a per-pitcher prior that uses their historical early-vs-full pattern.

If neither: noise dominates, the uniform shrinkage stays as is.

**Revisit after:** 1-2 months of logged data showing whether early-season
pitcher predictions systematically miss in one direction (CLV negative on
high-pitcher-factor early-season picks would be the warning).

---

## Template for new investigations

Add a new section above this template when something comes up. Each item
should include:
- The question
- Data needed
- Hypothesis (or "no prior — exploratory")
- Trigger condition for investing more time
- "Revisit after" date or condition
