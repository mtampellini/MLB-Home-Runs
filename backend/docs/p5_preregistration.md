# P5 ship gate — pre-registered 2026-06-11

Committed BEFORE any P5 code or results exist. This is the standard the P5
ML rebuild (market-prob-as-feature, target ship ~2026-07-19) must clear to
replace the baseline pipeline. Decided with no skin in the game; do not
renegotiate after results are visible.

## Primary gate (CLV)

Over P5's first ~150 selected picks (~2-3 weeks at current volume):

1. **Mean CLV >= +0.20pp** (fair-prob percentage points) with the **95%
   confidence interval excluding zero**, and
2. **Beat-close rate >= 55%**.

Measured exactly as `src/backtest/clv_recover.py` measures it today:
de-vigged fair probability at entry vs the latest pre-game snapshot, same
devig math as production entry. The T-minus-close caveat applies equally to
the baseline comparison, so it does not bias the gate.

Context at registration (5/20-6/11, n=922 w/CLV): current triple cohort
mean CLV = +0.065pp, beat-close 54.1%. The gate asks P5 for ~3x the
current edge — justified because P5 gets the market price as an input,
which the 2026-06-11 P1 backtest proved is where all the predictive signal
lives (logit-blend weight fit = 0.00 on model_prob).

## Secondary gates (sanity, not ship/no-ship)

- Logloss on the full-slate log (`data/full_slate/`, odds-matched rows,
  all predictions not just picks) <= the market baseline on the same rows.
  This is checkable because full-slate logging (commit 8963345) captures
  predictions WITHOUT selection bias.
- No guard-cohort damage: breakout<=0 and 10-20% band calibration gaps no
  worse than baseline's on the same window.

## Outcomes

- **Pass** -> P5 becomes the production model; baseline retires to shadow.
- **Fail but positive** (CLV > 0, CI includes 0) -> extend 60d in shadow
  tier, do not ship, do not kill.
- **Fail flat/negative** -> baseline stays; P5 back to research.

## Anti-goalpost clause

If we want to change ANY number above after P5 results exist, the change
must be written down with its reason BEFORE looking at how it affects the
verdict, and the original verdict must be reported alongside.
