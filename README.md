# HR Picks — V7

MLB home run prop betting model + front-end. Single repo, single deploy.

```
/                       <- Next.js root, deployed to Vercel
├── src/pages/index.js  <- the picks page; imports ../../picks.json
├── picks.json          <- written by the daily cron, picked up at build time
├── backend/            <- V7 Python pipeline (model, odds, settle, tracker)
│   ├── src/            <- all production code
│   ├── tests/          <- pytest suite (163 tests)
│   ├── scripts/        <- one-off utilities (e.g. precompute_park_factors)
│   └── data/
│       ├── park_metadata.json    <- committed
│       ├── odds/                 <- daily snapshots, committed (this IS our dataset)
│       ├── processed/
│       │   └── park_factors.parquet  <- committed (precomputed once)
│       └── raw/                  <- gitignored Statcast caches
└── .github/workflows/
    ├── daily_picks.yml      <- 11am ET cron, runs run_daily.py, commits picks.json
    └── settle_results.yml   <- next-morning cron, settles + updates tracker
```

The full V7 architecture (8 build phases, validation gates, the no-historical-odds
constraint, data-hygiene rules) is documented in `backend/README.md`.

## Local development

```powershell
# Backend (Python 3.11+):
cd backend
pip install -e ".[dev]"
pytest tests/                    # 163 tests, all should pass
python -m src.pipeline.run_daily # generates picks.json + dated artifacts
cp picks.json ../picks.json      # what the cron does in CI

# Front-end (Node 18+):
cd ..
npm install
npm run dev                      # http://localhost:3000
```

## Cron schedule (UTC)

| Workflow | Schedule | What it does |
|---|---|---|
| `daily_picks.yml` | `0 15 * * *` (11am ET EDT, 10am ET EST) | Pulls slate + odds, runs baseline, writes `picks.json` to repo root, commits. Vercel auto-deploys. |
| `settle_results.yml` | `0 14 * * *` (10am ET EDT, 9am ET EST) | Pulls yesterday's box scores, marks each pick W/L, updates `backend/data/processed/tracker.json`. |

Both workflows commit on the cron account; `[ci skip]` in the message prevents
re-triggering each other.

## Validation status

V7 is built but **must paper-trade for 60+ days** before any real money — see
`backend/README.md` → "Validation gates". The ML training infrastructure is
dormant by design until ~60 days of logged odds accumulate.

## Secrets

Add to the repo's GitHub Actions secrets:
- `ODDS_API_KEY` — from https://the-odds-api.com
