// Pure aggregation helpers for turning settled picks into the summary numbers
// shown on the tracker's metric cards (settled count, hit rate, units profit,
// ROI). Kept framework-free so the math is unit-tested directly (betStats.test.js).
//
// Staking convention mirrors the backend: 1u flat stake on every settled pick;
// voids don't stake. ROI is profit over units staked (i.e. over settled picks).

// Build the summary object from raw W/L/V counts + total units profit.
function summaryFromCounts(wins, losses, voids, unitsProfit) {
  const settled = wins + losses
  return {
    wins,
    losses,
    voids,
    total_picks: settled + voids,
    units_staked: settled,
    units_profit: unitsProfit,
    hit_rate: settled > 0 ? wins / settled : null,
    roi_pct: settled > 0 ? (unitsProfit / settled) * 100 : null,
  }
}

const { betKey, isBet } = require('./bets')

// Walk archives and pull the settlement result rows that should count toward
// the metrics, matching each result back to its pick. Optional scoping:
//   passes(pick, tier) -> bool   filter-view gate (triple/quad); omit for all
//   betsOnly + bets              keep only picks flagged in the bets map
// Each archive is { date, data }, mirroring the tracker's getStaticProps shape.
function collectResults(archives, tiers, { passes = null, bets = null, betsOnly = false } = {}) {
  const rows = []
  for (const { date, data } of archives || []) {
    const settle = data && data.settlement
    if (!settle) continue
    for (const t of tiers) {
      const picksList = data[`${t}_picks`] || []
      const idx = new Map()
      for (const p of picksList) idx.set(`${p.batter_id}|${p.game_pk || ''}`, p)
      for (const r of settle[`${t}_results`] || []) {
        const p = idx.get(`${r.batter_id}|${r.game_pk || ''}`)
        if (!p) continue
        if (passes && !passes(p, t)) continue
        if (betsOnly && !isBet(bets, betKey(date, r.batter_id, r.game_pk))) continue
        rows.push(r)
      }
    }
  }
  return rows
}

// Aggregate a list of settlement result rows ({ outcome, profit_units }) into a
// summary. Anything that isn't a 'W' or 'L' (e.g. 'VOID') counts as a void.
function aggregateResults(rows) {
  let wins = 0, losses = 0, voids = 0, profit = 0
  for (const r of rows || []) {
    if (r.outcome === 'W') wins++
    else if (r.outcome === 'L') losses++
    else voids++
    profit += r.profit_units || 0
  }
  return summaryFromCounts(wins, losses, voids, profit)
}

module.exports = { summaryFromCounts, aggregateResults, collectResults }
