# Weather calibration (2026-07-01)

Two-part overhaul of the weather variable after an audit found it was
materially wired (temperature ranked top-3 driver on ~37% of picks) but
systematically miscalibrated.

## Fix 1 — MLB Stats API weather (source)

Previously: Open-Meteo hourly forecast; the 8 retractable-roof parks were
hard-coded to `is_indoor` (72°F, no wind) **every game**, and wind came from a
10m-height reading projected onto the CF axis via `cf_bearing`.

Now: the MLB Stats API `weather` hydrate is the authoritative source, with
Open-Meteo as fallback (~15% of games lack an MLB block pre-game). It provides,
at Preview (pre-game, as-of safe):

- **Roof state** — `condition` is `Dome` / `Roof Closed` when closed, else a sky
  condition. So an *open* retractable roof now gets real weather instead of the
  forced indoor placeholder (verified live: Toronto, roof open, 80°F + wind).
- **Official game temperature**.
- **Field-relative wind** — `"13 mph, Out To CF"`. Mapped to an out-to-CF
  component directly (`Out To CF` ×1.0, `Out To RF/LF` ×0.7, `In From …`
  negative, crosswind/None/Varies ×0), which also sidesteps Open-Meteo's
  10m-height overstatement and the cf_bearing projection.

## Fix 2 — Re-fitted temp/wind coefficients

Fitted from **2025 per-game HR vs weather** (n=2238 outdoor games, 5219 HR).
Poisson GLM: `hr ~ offset(log pa) + C(park) + temp + wind_cf`. Park fixed
effects absorb each park's climate, so the slopes are the *within-park* physical
effect — no double-count with the park factor.

| Param | Old | New (fitted) | Note |
|---|---|---|---|
| `temp_baseline_f` | 70.0 | **74.0** | PA-weighted mean outdoor game temp (73.9). The bug. |
| `temp_per_degree` | 0.0100 | **0.0096** | +0.96%/°F, p=3e-11. Slope was already right. |
| `temp_factor_clip` | (0.85, 1.20) | **(0.75, 1.22)** | 0.5/99.5 pct; floor widened (cold games legitimately reach ~0.70). |
| `wind_per_mph` | 0.0100 | **0.0097** | +0.97%/mph out-to-CF, p=3e-4. |
| `wind_factor_clip` | (0.70, 1.40) | **(0.80, 1.25)** | Safety rail; binds ~0% of games. |

### What was actually wrong
The **slopes were fine** (fitted 0.96%/°F and 0.97%/mph are statistically
indistinguishable from the old 1%/1%, and robust to park FE → little
confounding). The **70°F baseline was the bug**: it sat below the operating
temperature range, so the mean temp_factor over outdoor games was **1.042** — a
systematic **+4% HR inflation on every outdoor game**, compounding onto blend ×
park × pitcher. Since the empirical-Bayes rates were calibrated on realized HR
data that already contained those temperatures, this mean-positive factor
double-inflated. Re-centering to 74°F drops the mean temp_factor to **1.006**
(≈neutral, as a proper adjustment should be) and net weather 1.053 → 1.012.

This is a likely contributor to the "model over-predicts every bin" finding
(2026-06-23), concentrated in warm months.

## Reproduce
Dataset builder + fit are one-off analysis scripts (2025 Statcast + MLB weather
hydrate joined on game_pk). Re-fit annually or when adding seasons; the slopes
are stable but the mean-temp baseline should track the game population.

## Not done here (future)
- Multi-year fit (2023-2025) for tighter slope CIs.
- Per-park-date temperature *normals* (fully removes the residual double-count;
  current re-centering removes the league-mean bias, not park-specific climate).
- Fold temp/wind into the P5 market-prob-as-feature ML rebuild.
