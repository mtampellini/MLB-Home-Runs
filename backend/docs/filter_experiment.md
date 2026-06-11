# Triple-filter experiment (2026-05-20 -> 2026-06-18)

## Setup

Shipped 2026-05-20 in commit on `main`. The pipeline (`src/pipeline/run_daily.py`)
tags every pick with `filter_status = {passes_baseline, passes_triple, passes_quad}`.
Site `picks.json` files include only `passes_triple` picks; the daily archive
retains every pick so the day-30 evaluation can compare strategies on the same
underlying slate.

Filter definitions: `src/pipeline/filters.py`.

## Pre-registered hypotheses (locked 2026-05-20)

Backtest window: 5/13-5/19, n=364 settled picks. All three filters and their
thresholds were derived from this window. The hypotheses below test whether the
backtest signal survives out-of-sample.

- **H1**: ROI(triple) > ROI(baseline) by >= 10pp
- **H2**: ROI(quad) > ROI(triple) by >= 8pp
- **H3**: ROI(dropped picks) < ROI(kept picks) by >= 10pp

Method: paired day-block bootstrap, 10k resamples, Bonferroni-corrected
one-sided alpha = 0.05 / 3 = 0.0167. A hypothesis is supported if the lower
bound of the (1 - alpha_corrected) CI on the point difference excludes 0 in the
predicted direction.

## Decision rules

| Outcome on 2026-06-18 | Action |
|---|---|
| H1 supported | Keep triple filter in production |
| H1 NOT supported AND H3 NOT supported | Revert to baseline (no filter) |
| H2 supported | Upgrade production filter to quad |
| H3 NOT supported | Investigate filter mechanics - dropped picks aren't underperforming, filter is removing value |

## How to run

```
cd backend && python -m src.backtest.stat_sig_eval
```

Optional args: `start_date [end_date]`. Default window starts 2026-05-13.

## Snapshot at experiment kickoff (2026-05-20)

The same evaluator on backtest data (7 days, 364 picks) already shows:
- H1: p=97.6%, point diff +39.96pp - just below 98.33% threshold
- H2: p=67.9% - cannot yet discriminate quad vs triple
- H3: p=98.8%, point diff +77.88pp - SUPPORTED (kept picks +35.85% vs dropped -42.01%)

H3 is the most direct validation: the picks the filter drops are clearly
losing money in-sample. Out-of-sample data over the next 30 days will tell us
whether the filter's discrimination generalizes.

## DO NOT during the experiment window

- Do not change `src/pipeline/filters.py` thresholds.
- Do not change `MODEL_VERSION` or any feature-engineering code that would shift
  pick distributions.
- Do not selectively settle picks - settle every pick the pipeline generates.

Any of these break the pre-registration and forfeit the stat-sig claim.

## Pre-registered 2026-06-10: H6 mid-band concentration

Registered mid-experiment; does not touch filters, MODEL_VERSION, or the
H1-H3 lock. Tracking only.

**Disclosure**: this band was found by mining the 5/20-6/10 settled window
(triple primary+shadow: band n=173, 30W, ROI +37.0%, hit 17.3% vs 15.0%
expected; outside band n=162, ROI -8.2%). Everything before 2026-06-11 is
therefore in-sample for H6 and proves nothing. Day-block bootstrap on the
discovery window itself: P(band ROI > 0) = 95.6%, P(band > outside) = 96.6% -
suggestive, not significant, and tainted by selection anyway.

**Frozen definition**: `passes_triple` AND tier in {primary, shadow} AND
`0.10 <= model_prob < 0.20`.

**Hypotheses** (OOS window starts 2026-06-11):

- **H6a**: OOS ROI(band) > 0
- **H6b**: OOS ROI(band) > ROI(triple primary+shadow outside band)

Method: day-block bootstrap, 20k resamples, one-sided alpha = 0.05 / 2 = 0.025.

**Timeline**: 2026-06-18 review gets a directional readout only (~50-70 band
picks expected - underpowered, no decision). Formal eval 2026-07-10 (30 OOS
days). If H6b holds, consider band-weighted staking on top of whatever filter
wins the H1-H3 decision; never band-exclusivity (outside-band picks still
carry the volume). If OOS band ROI <= 0, the band is dead - it was bucket
luck.

```
cd backend && python band_vs_triple.py            # OOS only (default 2026-06-11)
cd backend && python band_vs_triple.py 2026-05-20 # include the tainted discovery window
```
