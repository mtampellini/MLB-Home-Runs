# HR-Picks-V7

MLB home run prop betting model. Predicts P(HR) per batter per game by Bayesian-blending
season-to-date skill with last-30-day form, then compares to FanDuel / DraftKings
alt HR lines (point = 0.5, "Over") via The Odds API. Surfaces picks above 25% EV.

Replaces the V4 production / V6 trained-but-unshipped models. Front-end at
[mlb-home-runs.vercel.app](https://mlb-home-runs.vercel.app) is reused; only the
`picks.json` schema it consumes changes.

---

## The constraint that shapes everything: no historical odds

We do **not** have stored historical odds data. Consequences:

- **No real backtest is possible on day one.** Anyone running `walk_forward.py`
  before we have a real odds dataset gets unreliable numbers and a loud warning.
- **We log odds from day one.** Every fetch in `src/odds/fetch.py` writes a
  timestamped snapshot to `data/odds/YYYY-MM-DD-HHMM.json`. Snapshots are
  never overwritten. **These snapshots are committed to git** — they ARE our dataset.
- **No synthetic odds, ever.** Reverse-engineering odds from historical HR rates
  to fake a backtest is forbidden. If you're tempted, stop and re-read this section.
- **Ship the pipeline, then wait.** ML training infrastructure is built but
  dormant until we have ~60 days of logged odds.

---

## Build phases

The project is built in strict phases. Do not skip ahead.

| Phase | What ships | Status |
|-------|------------|--------|
| 1 | Scaffolding (this commit) | ✅ |
| 2 | Data layer: `AsOfContext` + batter/pitcher/park-weather features + Bayesian blend | ⬜ |
| 3 | Baseline empirical-Bayes model (`src/model/baseline.py`) — what we actually bet with for the first 60+ days | ⬜ |
| 4 | Odds client + EV calculator + daily snapshot logger | ⬜ |
| 5 | Daily pipeline (`run_daily.py`) → `picks.json` | ⬜ |
| 6 | Settlement + ROI / CLV / hit-rate tracker | ⬜ |
| 7 | GitHub Actions: 11am ET daily picks cron + next-morning settlement cron | ⬜ |
| 8 | Dormant ML infra: `train.py`, `walk_forward.py`, `calibration.py` | ⬜ |

---

## Architecture

```
HR-Picks-V7/
├── data/
│   ├── raw/         # Statcast / MLB Stats API caches  (gitignored)
│   ├── processed/   # Daily feature tables             (gitignored)
│   └── odds/        # Daily odds snapshots             (committed — our dataset)
├── src/
│   ├── features/   batter_season, batter_recent, pitcher, park_weather, blend
│   ├── model/      baseline (Phase 3) · predict · train (dormant) · calibration (dormant)
│   ├── odds/       fetch · log · ev
│   ├── pipeline/   refresh_data · run_daily
│   ├── backtest/   as_of_context · walk_forward (dormant) · metrics
│   └── results/    settle · tracker
├── web/            # consumes picks.json — Vercel front-end stays
├── .github/workflows/  daily_picks.yml · settle_results.yml
└── picks.json      # today's picks, served to web
```

---

## Validation gates

These gates govern when the model can move from paper to real money. **All must pass.**

1. **Calibration.** Predicted vs actual HR rate within 2pp across deciles.
2. **Pick volume sanity.** Expect 2–8 picks/day above 25% EV. 40+ picks/day means
   something is broken (model overconfident, devig wrong, etc.) — investigate before
   trusting any output.
3. **Closing line value.** Positive CLV is the cleanest edge proof. Prioritize CLV
   over realized ROI when judging the model — ROI is noisy over short windows.
4. **Paper-trade minimum: 60 days.** No real money before 60 days of logged picks
   with positive CLV and good calibration.

---

## Data hygiene rules (non-negotiable)

- **No median-fill for missing Statcast features.** If a batter is missing required
  features, skip them and log the skip. Median-fill inflated rookie probabilities
  in V4 — that mistake doesn't repeat.
- **No end-of-season aggregates in training.** Every feature is computed
  as-of the game date via `AsOfContext`. Tests enforce this.
- **No synthetic odds.** See the constraint section above.
- **Commit `data/odds/`. Do NOT commit `data/raw/` or `data/processed/`** — those
  are regeneratable caches.

---

## `picks.json` schema

Written by `src/pipeline/run_daily.py` (Phase 5), consumed by the Vercel front-end.

```json
{
  "generated_at": "2026-05-06T15:00:00Z",
  "as_of_date": "2026-05-06",
  "model_version": "v7-baseline-0.1.0",
  "league_hr_per_pa": 0.032,
  "picks": [
    {
      "batter": "Aaron Judge",
      "batter_id": 592450,
      "batter_hand": "R",
      "team": "NYY",
      "lineup_spot": 2,
      "pitcher": "Tarik Skubal",
      "pitcher_id": 669373,
      "pitcher_hand": "L",
      "park": "NYY",
      "game_datetime": "2026-05-06T19:05:00-04:00",

      "line": 0.5,
      "fd_odds": 310,
      "dk_odds": 295,
      "best_book": "DK",
      "market_prob_devig": 0.241,

      "model_prob": 0.302,
      "ev_pct": 19.7,

      "blended_hr_per_pa": 0.058,
      "breakout_score": 0.082,
      "low_confidence": false,
      "trend_signal": 0.18,
      "unstable_recent": false,

      "top_3_features": [
        {"name": "batter_skill",     "value": 1.81, "deviation": 0.81},
        {"name": "park",             "value": 1.18, "deviation": 0.18},
        {"name": "breakout_signal",  "value": 1.14, "deviation": 0.14}
      ]
    }
  ],
  "skipped_count": 12,
  "skipped_reference": "data/processed/skipped_batters_2026-05-06.json"
}
```

Field notes:
- `breakout_score` — reliability-scaled, clipped value from `src/features/breakout.py`.
  Range [-0.15, +0.15]; this is the raw additive bump applied (× `breakout_coefficient`)
  inside the baseline model.
- `top_3_features` — multiplicative factors from `BaselinePrediction.components`,
  ranked by `abs(value - 1)`. All values are direct multipliers vs neutral=1.0:
  - `batter_skill`, `pitcher`, `park`, `temperature`, `wind` → e.g. `park: 1.18`
    = park inflates HR rate by 18%.
  - **`breakout_signal`** is now also natively multiplicative (post-2026-05-06
    change from additive). Encoded as `1 + (breakout_coefficient × reliable_breakout)`.
    With default coefficient = 1.0 and cap = 0.15, range is `[0.85, 1.15]` —
    i.e. max ±15% lift on the underlying skill rate. So `1.14` = +14% lift,
    `0.92` = −8% drag.
- `low_confidence: true` when blended rate relies entirely on prior year (e.g., player
  hasn't appeared in the current season yet but has a prior-year track record).
- `trend_signal` — `(barrel_30d - barrel_season) / barrel_season`. Positive = batter
  barreling more recently than usual baseline; negative = cooling off. **Surfaced for
  human review only — NOT used to score or filter picks.** `null` when season barrel
  rate is missing or zero.
- `unstable_recent: true` when `barrel_30d / barrel_season` ≥ 1.5 OR ≤ 0.5 — i.e. the
  recent window has diverged wildly from the season baseline. Visibility-only flag;
  picks still surface, just look at them with extra skepticism. Goes false when either
  rate is missing or zero.
- `skipped_count` — batters dropped by skip_logic; full list in the referenced file.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate              # Windows
# source .venv/bin/activate         # macOS/Linux

pip install -e ".[dev]"
copy .env.example .env              # then fill in ODDS_API_KEY
```

## Running (once Phase 5 ships)

```bash
python -m src.pipeline.run_daily         # generate picks.json for today
python -m src.results.settle             # next morning, settle yesterday's picks
```

## Tests

```bash
pytest
```
