// Unit tests for the bet-toggle persistence + cross-device merge logic.
// Run with: node --test
const { test } = require('node:test')
const assert = require('node:assert/strict')

const {
  BETS_STORAGE_KEY,
  LEGACY_BETS_STORAGE_KEY,
  TOMBSTONE_MAX_AGE_MS,
  betKey,
  normalizeBets,
  toggleBet,
  isBet,
  mergeBets,
  pruneBets,
  betsEqual,
  countBetsForDay,
  countBets,
  loadBets,
  saveBets,
} = require('./bets')

// Minimal in-memory stand-in for window.localStorage.
function fakeStorage(initial = {}) {
  const store = { ...initial }
  return {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => { store[k] = String(v) },
    removeItem: (k) => { delete store[k] },
    _dump: () => store,
  }
}

test('betKey is stable and disambiguates date / batter / double-headers', () => {
  assert.equal(betKey('2026-05-30', 12345, 778), '2026-05-30|12345|778')
  // Same batter, different day -> different key.
  assert.notEqual(betKey('2026-05-30', 12345, 778), betKey('2026-05-29', 12345, 778))
  // Same batter, same day, two games -> different key.
  assert.notEqual(betKey('2026-05-30', 12345, 778), betKey('2026-05-30', 12345, 779))
  // Missing game_pk is handled without producing "undefined".
  assert.equal(betKey('2026-05-30', 12345), '2026-05-30|12345|')
  assert.equal(betKey('2026-05-30', 12345, null), '2026-05-30|12345|')
})

test('toggleBet flips with a timestamp and never mutates the input', () => {
  const k = betKey('2026-05-30', 1, 10)
  const empty = {}
  const on = toggleBet(empty, k, 1000)
  assert.deepEqual(empty, {}, 'input must not be mutated')
  assert.equal(isBet(on, k), true)
  assert.deepEqual(on[k], { on: true, t: 1000 })

  // Un-flagging leaves a tombstone (so the removal can out-merge a stale
  // `on` from another device), not a deleted key.
  const off = toggleBet(on, k, 2000)
  assert.equal(isBet(off, k), false)
  assert.deepEqual(off[k], { on: false, t: 2000 })

  // Toggling one key leaves others untouched.
  const k2 = betKey('2026-05-30', 2, 10)
  const both = toggleBet(on, k2, 3000)
  assert.equal(isBet(both, k), true)
  assert.equal(isBet(both, k2), true)
})

test('countBetsForDay and countBets scope correctly and skip tombstones', () => {
  let bets = {}
  bets = toggleBet(bets, betKey('2026-05-30', 1, 10), 1)
  bets = toggleBet(bets, betKey('2026-05-30', 2, 10), 2)
  bets = toggleBet(bets, betKey('2026-05-29', 3, 11), 3)
  assert.equal(countBetsForDay(bets, '2026-05-30'), 2)
  assert.equal(countBetsForDay(bets, '2026-05-29'), 1)
  assert.equal(countBetsForDay(bets, '2026-05-28'), 0)
  assert.equal(countBets(bets), 3)
  assert.equal(countBetsForDay({}, '2026-05-30'), 0)
  // An un-flagged pick (tombstone) must not be counted anywhere.
  bets = toggleBet(bets, betKey('2026-05-30', 1, 10), 4)
  assert.equal(countBetsForDay(bets, '2026-05-30'), 1)
  assert.equal(countBets(bets), 2)
})

test('mergeBets is per-key last-write-wins; flagged wins timestamp ties', () => {
  const k1 = 'a', k2 = 'b', k3 = 'c'
  const phone = { [k1]: { on: true, t: 100 }, [k2]: { on: false, t: 500 } }
  const laptop = { [k1]: { on: false, t: 200 }, [k2]: { on: true, t: 400 }, [k3]: { on: true, t: 50 } }
  const merged = mergeBets(phone, laptop)
  assert.deepEqual(merged[k1], { on: false, t: 200 }, 'newer un-flag beats older flag')
  assert.deepEqual(merged[k2], { on: false, t: 500 }, 'newer un-flag survives even as base')
  assert.deepEqual(merged[k3], { on: true, t: 50 }, 'union keeps keys only one side has')
  // Symmetric inputs converge to the same result.
  assert.deepEqual(mergeBets(laptop, phone), merged)
  // Tie: the flagged side wins regardless of argument order.
  const tieA = { x: { on: true, t: 9 } }
  const tieB = { x: { on: false, t: 9 } }
  assert.equal(isBet(mergeBets(tieA, tieB), 'x'), true)
  assert.equal(isBet(mergeBets(tieB, tieA), 'x'), true)
})

test('migrated v1 entries (t=0) always lose to a real toggle', () => {
  const migrated = normalizeBets({ a: true })
  const unflaggedElsewhere = { a: { on: false, t: 1717000000000 } }
  assert.equal(isBet(mergeBets(migrated, unflaggedElsewhere), 'a'), false)
})

test('pruneBets drops only expired tombstones, never live flags', () => {
  const now = TOMBSTONE_MAX_AGE_MS * 10
  const bets = {
    live_old: { on: true, t: 0 },
    dead_old: { on: false, t: now - TOMBSTONE_MAX_AGE_MS - 1 },
    dead_recent: { on: false, t: now - 1000 },
  }
  const pruned = pruneBets(bets, now)
  assert.deepEqual(Object.keys(pruned).sort(), ['dead_recent', 'live_old'])
})

test('betsEqual compares maps independent of key order', () => {
  const a = { x: { on: true, t: 1 }, y: { on: false, t: 2 } }
  const b = { y: { on: false, t: 2 }, x: { on: true, t: 1 } }
  assert.equal(betsEqual(a, b), true)
  assert.equal(betsEqual(a, { x: { on: true, t: 1 } }), false, 'missing key')
  assert.equal(betsEqual(a, { ...a, y: { on: false, t: 3 } }), false, 'timestamp differs')
  assert.equal(betsEqual(a, { ...a, y: { on: true, t: 2 } }), false, 'state differs')
  assert.equal(betsEqual({}, {}), true)
})

test('save then load round-trips through storage and persists', () => {
  const storage = fakeStorage()
  let bets = toggleBet({}, betKey('2026-05-30', 99, 1), 1000)
  bets = toggleBet(bets, betKey('2026-05-30', 100, 1), 2000)
  saveBets(storage, bets)
  // Stored under the versioned key as JSON.
  assert.ok(storage.getItem(BETS_STORAGE_KEY))
  // A fresh load (simulating a page reload) recovers the same map.
  const reloaded = loadBets(storage)
  assert.deepEqual(reloaded, bets)
})

test('loadBets migrates legacy v1 data when no v2 data exists', () => {
  const storage = fakeStorage({
    [LEGACY_BETS_STORAGE_KEY]: JSON.stringify({ 'd|1|2': true, 'd|3|4': true }),
  })
  const loaded = loadBets(storage)
  assert.equal(isBet(loaded, 'd|1|2'), true)
  assert.equal(isBet(loaded, 'd|3|4'), true)
  assert.equal(countBets(loaded), 2)
  // Once v2 exists it takes precedence over v1 leftovers.
  saveBets(storage, toggleBet(loaded, 'd|1|2', 5000))
  assert.equal(isBet(loadBets(storage), 'd|1|2'), false)
})

test('loadBets is defensive against missing / corrupt / wrong-type data', () => {
  assert.deepEqual(loadBets(fakeStorage()), {}, 'no data -> empty')
  assert.deepEqual(loadBets(fakeStorage({ [BETS_STORAGE_KEY]: 'not json' })), {})
  assert.deepEqual(loadBets(fakeStorage({ [BETS_STORAGE_KEY]: '[1,2,3]' })), {}, 'array -> empty')
  assert.deepEqual(loadBets(fakeStorage({ [BETS_STORAGE_KEY]: 'null' })), {})
  // Corrupt entries inside an otherwise valid map are dropped, valid ones kept.
  const mixed = loadBets(fakeStorage({
    [BETS_STORAGE_KEY]: JSON.stringify({ good: { on: true, t: 1 }, bad: 'huh', worse: null }),
  }))
  assert.deepEqual(mixed, { good: { on: true, t: 1 } })
  // A storage that throws on access (private mode) must not crash.
  const throwing = { getItem() { throw new Error('blocked') } }
  assert.deepEqual(loadBets(throwing), {})
})

test('saveBets swallows write errors (quota / private mode)', () => {
  const throwing = { setItem() { throw new Error('quota') } }
  assert.doesNotThrow(() => saveBets(throwing, { a: { on: true, t: 1 } }))
})
