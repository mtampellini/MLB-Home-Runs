// Unit tests for the tracker summary math. Run with: node --test
const { test } = require('node:test')
const assert = require('node:assert/strict')

const { summaryFromCounts, aggregateResults, collectResults } = require('./betStats')
const { betKey } = require('./bets')

// Two days of fake archives in the tracker's getStaticProps shape. Day 1 has a
// winning bet (Judge) and a losing non-bet (Soto); day 2 has a losing bet
// (Judge) and a winning non-bet (Ohtani, secondary tier).
const archives = [
  {
    date: '2026-05-30',
    data: {
      primary_picks: [
        { batter_id: 1, game_pk: 100, batter: 'Judge' },
        { batter_id: 2, game_pk: 100, batter: 'Soto' },
      ],
      settlement: {
        primary_results: [
          { batter_id: 1, game_pk: 100, outcome: 'W', profit_units: 2.0 },
          { batter_id: 2, game_pk: 100, outcome: 'L', profit_units: -1 },
        ],
      },
    },
  },
  {
    date: '2026-05-29',
    data: {
      primary_picks: [{ batter_id: 1, game_pk: 200, batter: 'Judge' }],
      secondary_picks: [{ batter_id: 3, game_pk: 201, batter: 'Ohtani' }],
      settlement: {
        primary_results: [{ batter_id: 1, game_pk: 200, outcome: 'L', profit_units: -1 }],
        secondary_results: [{ batter_id: 3, game_pk: 201, outcome: 'W', profit_units: 1.5 }],
      },
    },
  },
]

test('summaryFromCounts computes staked/profit/hit-rate/ROI (voids excluded from stake)', () => {
  // 3W, 1L, 1 void, +4.5u profit. Settled = 4, staked = 4.
  const s = summaryFromCounts(3, 1, 1, 4.5)
  assert.equal(s.wins, 3)
  assert.equal(s.losses, 1)
  assert.equal(s.voids, 1)
  assert.equal(s.total_picks, 5)      // 4 settled + 1 void
  assert.equal(s.units_staked, 4)     // voids don't stake
  assert.equal(s.units_profit, 4.5)
  assert.equal(s.hit_rate, 0.75)      // 3/4
  assert.equal(s.roi_pct, (4.5 / 4) * 100)
})

test('summaryFromCounts returns null rates when nothing settled', () => {
  const s = summaryFromCounts(0, 0, 2, 0)
  assert.equal(s.units_staked, 0)
  assert.equal(s.total_picks, 2)
  assert.equal(s.hit_rate, null)
  assert.equal(s.roi_pct, null)
})

test('aggregateResults tallies outcomes and profit from result rows', () => {
  const rows = [
    { outcome: 'W', profit_units: 1.85 },
    { outcome: 'W', profit_units: 2.10 },
    { outcome: 'L', profit_units: -1 },
    { outcome: 'VOID', profit_units: 0 },
  ]
  const s = aggregateResults(rows)
  assert.equal(s.wins, 2)
  assert.equal(s.losses, 1)
  assert.equal(s.voids, 1)
  assert.equal(s.units_staked, 3)
  assert.ok(Math.abs(s.units_profit - 2.95) < 1e-9)
  assert.ok(Math.abs(s.hit_rate - 2 / 3) < 1e-9)
  assert.ok(Math.abs(s.roi_pct - (2.95 / 3) * 100) < 1e-9)
})

test('aggregateResults handles an all-loss book (negative ROI) and empty input', () => {
  const allLoss = aggregateResults([
    { outcome: 'L', profit_units: -1 },
    { outcome: 'L', profit_units: -1 },
  ])
  assert.equal(allLoss.hit_rate, 0)
  assert.equal(allLoss.roi_pct, -100)
  assert.equal(allLoss.units_profit, -2)

  const empty = aggregateResults([])
  assert.equal(empty.total_picks, 0)
  assert.equal(empty.hit_rate, null)
  assert.equal(empty.roi_pct, null)
  assert.equal(aggregateResults(undefined).total_picks, 0)
})

test('collectResults without bet scoping returns every matched result', () => {
  const rows = collectResults(archives, ['primary'], {})
  // 2 primary results day 1 + 1 primary result day 2.
  assert.equal(rows.length, 3)
  const all = aggregateResults(rows)
  assert.equal(all.wins, 1)        // Judge day 1
  assert.equal(all.losses, 2)      // Soto day 1, Judge day 2
})

test('collectResults bets-only yields the actual ROI of just the flagged picks', () => {
  // Flag both Judge picks (day 1 win, day 2 loss) but NOT Soto or Ohtani.
  const bets = {
    [betKey('2026-05-30', 1, 100)]: true,
    [betKey('2026-05-29', 1, 200)]: true,
  }
  const rows = collectResults(archives, ['primary', 'secondary'], { bets, betsOnly: true })
  assert.equal(rows.length, 2, 'only the two flagged Judge picks')
  const s = aggregateResults(rows)
  assert.equal(s.wins, 1)
  assert.equal(s.losses, 1)
  assert.equal(s.units_staked, 2)
  assert.equal(s.units_profit, 1.0)        // +2.0 and -1
  assert.equal(s.hit_rate, 0.5)
  assert.equal(s.roi_pct, 50)              // +1u on 2u staked
})

test('collectResults bets-only ignores flags that point at non-existent results', () => {
  const bets = { [betKey('2026-05-30', 999, 100)]: true }  // nobody we have
  const rows = collectResults(archives, ['primary', 'secondary'], { bets, betsOnly: true })
  assert.equal(rows.length, 0)
  assert.equal(aggregateResults(rows).roi_pct, null)
})

test('collectResults bets-only excludes v2 un-flag tombstones', () => {
  // Judge day 1 flagged, Judge day 2 flagged-then-removed on another device.
  const bets = {
    [betKey('2026-05-30', 1, 100)]: { on: true, t: 100 },
    [betKey('2026-05-29', 1, 200)]: { on: false, t: 200 },
  }
  const rows = collectResults(archives, ['primary', 'secondary'], { bets, betsOnly: true })
  assert.equal(rows.length, 1, 'tombstone must not count as a bet')
  assert.equal(rows[0].outcome, 'W')
})

test('collectResults composes a filter-view gate with bet scoping', () => {
  const bets = {
    [betKey('2026-05-30', 1, 100)]: true,   // Judge, will pass the gate
    [betKey('2026-05-29', 1, 200)]: true,   // Judge day 2, gate will reject
  }
  // Gate that only admits day-1 Judge (game_pk 100).
  const passes = (p) => p.game_pk === 100
  const rows = collectResults(archives, ['primary', 'secondary'], { passes, bets, betsOnly: true })
  assert.equal(rows.length, 1)
  assert.equal(rows[0].outcome, 'W')
})
