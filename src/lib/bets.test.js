// Unit tests for the bet-toggle persistence logic. Run with: node --test
const { test } = require('node:test')
const assert = require('node:assert/strict')

const {
  BETS_STORAGE_KEY,
  betKey,
  toggleBet,
  isBet,
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

test('toggleBet adds, removes, and never mutates the input', () => {
  const k = betKey('2026-05-30', 1, 10)
  const empty = {}
  const on = toggleBet(empty, k)
  assert.deepEqual(empty, {}, 'input must not be mutated')
  assert.equal(isBet(on, k), true)

  const off = toggleBet(on, k)
  assert.equal(isBet(off, k), false)
  assert.deepEqual(off, {})

  // Toggling one key leaves others untouched.
  const k2 = betKey('2026-05-30', 2, 10)
  const both = toggleBet(on, k2)
  assert.equal(isBet(both, k), true)
  assert.equal(isBet(both, k2), true)
})

test('countBetsForDay and countBets scope correctly', () => {
  let bets = {}
  bets = toggleBet(bets, betKey('2026-05-30', 1, 10))
  bets = toggleBet(bets, betKey('2026-05-30', 2, 10))
  bets = toggleBet(bets, betKey('2026-05-29', 3, 11))
  assert.equal(countBetsForDay(bets, '2026-05-30'), 2)
  assert.equal(countBetsForDay(bets, '2026-05-29'), 1)
  assert.equal(countBetsForDay(bets, '2026-05-28'), 0)
  assert.equal(countBets(bets), 3)
  // A date that is a prefix of another must not over-count.
  assert.equal(countBetsForDay({}, '2026-05-30'), 0)
})

test('save then load round-trips through storage and persists', () => {
  const storage = fakeStorage()
  let bets = toggleBet({}, betKey('2026-05-30', 99, 1))
  bets = toggleBet(bets, betKey('2026-05-30', 100, 1))
  saveBets(storage, bets)
  // Stored under the versioned key as JSON.
  assert.ok(storage.getItem(BETS_STORAGE_KEY))
  // A fresh load (simulating a page reload) recovers the same map.
  const reloaded = loadBets(storage)
  assert.deepEqual(reloaded, bets)
})

test('loadBets is defensive against missing / corrupt / wrong-type data', () => {
  assert.deepEqual(loadBets(fakeStorage()), {}, 'no data -> empty')
  assert.deepEqual(loadBets(fakeStorage({ [BETS_STORAGE_KEY]: 'not json' })), {})
  assert.deepEqual(loadBets(fakeStorage({ [BETS_STORAGE_KEY]: '[1,2,3]' })), {}, 'array -> empty')
  assert.deepEqual(loadBets(fakeStorage({ [BETS_STORAGE_KEY]: 'null' })), {})
  // A storage that throws on access (private mode) must not crash.
  const throwing = { getItem() { throw new Error('blocked') } }
  assert.deepEqual(loadBets(throwing), {})
})

test('saveBets swallows write errors (quota / private mode)', () => {
  const throwing = { setItem() { throw new Error('quota') } }
  assert.doesNotThrow(() => saveBets(throwing, { a: true }))
})
