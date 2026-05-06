# Feature importance research

One-shot empirical study of which Statcast features actually predict HRs on
historical data. Used to validate or refute theory-driven choices in
`src/features/breakout.py`. **Not part of the daily pipeline.**

## What it does

1. Pulls 2022–2025 Statcast data, chunked + cached per (year, start, end).
2. Builds a labeled batter-game dataset (label = HR in that game) using
   leakage-safe as-of features (`AsOfContext` discipline matched).
3. Filters to **starters** (PA ≥ 2 in the game) and **established hitters**
   (career_pa ≥ 100 as of game start).
4. Trains logistic regression and LightGBM for three feature ablations:
   - **A**: `blended_hr_per_pa` only
   - **B**: blended + spec breakout metrics (barrel, sweet_spot, pull_air, max_ev)
   - **C**: every feature (incl. handedness, platoon, park, pitcher splits)
5. Reports holdout AUC + log-loss, LightGBM gain/split/SHAP, logreg coefficients,
   univariate AUC per feature, full correlation matrix, and flagged redundancies.

Output: `src/research/feature_importance/research_report.md`

## Run command

```powershell
# From the project root, in your venv:
pip install -e ".[research]"            # adds shap; pyarrow for parquet
python -m src.research.feature_importance.run_research
```

That's it. The script prints timestamped progress to stdout — you can pipe to a
file if you want to walk away:

```powershell
python -m src.research.feature_importance.run_research *> research.log
```

## Resumability

Every expensive step is cached to disk. Re-running picks up exactly where it
stopped:

| Step | Cache location | Skip behavior |
|---|---|---|
| Statcast pull | `data/research/feature_importance/chunks/{year}_{start}_{end}.parquet` | Each chunk skipped if file exists. Empty chunks (off-days) get a `.empty` marker so they aren't retried. |
| Dataset build | `data/research/feature_importance/datasets/dataset_{train,test}.parquet` | Skipped if both parquets exist. Delete one to rebuild. |
| Models | `data/research/feature_importance/models/{logreg,lgbm}_{A,B,C}.pkl` | Skipped if pickle exists. Delete to retrain. |

If Savant rate-limits or times out, the script logs which chunk failed,
retries up to 3× with 30s sleeps, and on final failure exits with a clear
error. Re-launch — only the missing chunks get refetched.

To force a clean rebuild of a phase, delete the cache files for that phase.

## Progress output format

Every line carries an ISO timestamp and elapsed-since-start counter:

```
[2026-05-07 02:14:33] [+0:00:01] [====] PHASE START: Statcast pull 2022..2025
[2026-05-07 02:14:35] [+0:00:03] [INFO] year 2022: 47 chunks of 5 days each
[2026-05-07 02:14:35] [+0:00:03] [INFO] chunk 1/47 (  2.1%): fetching 2022-03-15..2022-03-19
[2026-05-07 02:14:48] [+0:00:16] [INFO]   → 18,234 rows → 2022_2022-03-15_2022-03-19.parquet
[2026-05-07 02:14:48] [+0:00:16] [INFO] chunk 2/47 (  4.3%): fetching 2022-03-20..2022-03-24  ETA 0:09:23
```

When you check on it in the morning:
- **Finished cleanly** → final line is `SCRIPT DONE — total wall time HH:MM:SS. report: …`
- **Still running** → look at the most recent `chunk i/N` and the ETA.
- **Crashed** → look for the last `ERROR` or Python traceback. Re-run; cached work is reused.

## Time expectations

First run, all phases:

| Phase | Expected wall time |
|---|---|
| Statcast pull (2022–2025) | 60–150 min (Savant load dependent) |
| Dataset build (annotate, aggregate, cumulative, rolling, joins) | 60–150 min |
| Train (3 variants × 2 models) | 5–15 min |
| Analyze (incl. SHAP on 30k holdout sample) | 15–45 min |
| Write report | < 1 min |
| **Total** | **2.5 – 6 hours** |

Re-runs with all caches present: under 5 minutes.

## What this script does NOT do

- Does **not** modify `src/features/breakout.py` or any production code.
- Does **not** run on partial 2026 data (sample too small).
- Does **not** integrate with `run_daily.py`.
- Does **not** auto-tune weights based on results — that's an explicit human
  decision after reading the report.
