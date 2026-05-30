// Pure, framework-free helpers for the "did I bet on this player?" toggle on
// the tracker page. Kept separate from the React component so the persistence
// logic can be unit-tested without a DOM (see bets.test.js).
//
// The tracker is a statically-built page with no per-user backend, so the set
// of bet-flagged picks lives entirely in the browser's localStorage. Each pick
// is identified by a stable key so a flag survives rebuilds and page reloads.

// Bump the suffix if the stored shape ever changes, so stale data is ignored.
const BETS_STORAGE_KEY = 'hr-tracker-bets-v1'

// Stable identifier for a single pick across page loads:
//   date    — disambiguates the same batter appearing on multiple days
//   batterId— the player
//   gamePk  — disambiguates a double-header (same batter_id, two games one day)
function betKey(date, batterId, gamePk) {
  return `${date}|${batterId}|${gamePk ?? ''}`
}

// Flip membership immutably: returns a NEW map with `key` added or removed.
function toggleBet(bets, key) {
  const next = { ...bets }
  if (next[key]) delete next[key]
  else next[key] = true
  return next
}

function isBet(bets, key) {
  return Boolean(bets && bets[key])
}

// How many flagged bets belong to a given archive date (per-day header badge).
function countBetsForDay(bets, date) {
  if (!bets) return 0
  const prefix = `${date}|`
  let n = 0
  for (const k of Object.keys(bets)) if (k.startsWith(prefix)) n++
  return n
}

// Total flagged bets across all days.
function countBets(bets) {
  return bets ? Object.keys(bets).length : 0
}

// Read the bet map from a Storage-like object, defensive against private-mode
// access errors and corrupt/legacy JSON. Always returns a plain object.
function loadBets(storage) {
  try {
    const raw = storage.getItem(BETS_STORAGE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

// Persist the bet map; swallow quota / private-mode write errors.
function saveBets(storage, bets) {
  try {
    storage.setItem(BETS_STORAGE_KEY, JSON.stringify(bets))
  } catch {
    /* ignore — flag just won't persist this session */
  }
}

module.exports = {
  BETS_STORAGE_KEY,
  betKey,
  toggleBet,
  isBet,
  countBetsForDay,
  countBets,
  loadBets,
  saveBets,
}
