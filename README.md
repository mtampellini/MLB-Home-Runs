# HR Picks

MLB Home Run prop betting model. Walk-forward validated over 5 months (May-Sep 2025), profitable every month.

## Setup

1. **Clone and install:**
```bash
git clone <your-repo>
cd hr-picks-app
npm install
```

2. **Deploy to Vercel:**
   - Push to GitHub
   - Connect repo in Vercel dashboard
   - It auto-deploys on every push

3. **Get odds API key:**
   - Sign up at https://the-odds-api.com (free tier = 500 req/month)
   - Set env variable: `export ODDS_API_KEY=your_key`

4. **Model files** should be in `../f5_model/` relative to this app. The publish script references them there.

## Daily Workflow

### 1. Publish today's picks (after lineups post, ~3-5pm ET)
```bash
python scripts/publish_picks.py
```
This runs the model, pulls live odds, filters to picks with 10%+ projected ROI, and writes to `src/data/picks.json`.

### 2. Deploy
```bash
git add -A && git commit -m "picks $(date +%Y-%m-%d)" && git push
```
Vercel auto-deploys within ~30 seconds.

### 3. Grade yesterday's results (next morning)
```bash
python scripts/grade_results.py
```
This checks which batters hit HRs, updates the database, and recalculates cumulative ROI.

### 4. Deploy updated results
```bash
git add -A && git commit -m "results $(date -d yesterday +%Y-%m-%d)" && git push
```

## Model Details

- **Version:** v4 (pitch type matchup + TTO + lineup position)
- **Training:** 23K+ batter-games from 2025 season
- **Validation:** Walk-forward, zero data leakage
- **Backtest:** 5/5 months profitable, +48.4% ROI on top 3% at +500 avg odds
- **Pick criteria:** Projected ROI >= 10% (model probability vs sportsbook implied probability)
- **Features:** Barrel%, max EV, bat speed, pitcher HR/PA, pitcher FB rate, park HR factor, altitude, lineup position, times through order, pitch mix

## How Projected ROI Works

```
projected_roi = (model_prob * payout) - (1 - model_prob)

Example:
  Model says 22% chance of HR
  Book offers +400 (pays 4x)
  ROI = (0.22 * 4.0) - (0.78) = 0.88 - 0.78 = +0.10 = +10% ROI
```

Only bets where this is >= 10% make the cut.
