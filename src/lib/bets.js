// Pure, framework-free helpers for the "did I bet on this player?" toggle on
// the tracker page. Kept separate from the React component so the persistence
// logic can be unit-tested without a DOM (see bets.test.js).
//
// v2 (cross-device sync): each flag is stored as { on, t } — current state
// plus the epoch-ms of the last toggle — instead of the bare `true` of v1.
// The timestamps let two devices merge with per-pick last-write-wins, and an
// un-flag survives as a tombstone ({ on: false }) so syncing a device that
// still has the old `true` doesn't resurrect it. Tombstones are pruned once
// they're old enough that every device must have seen them.
//
// localStorage stays the source of truth on each device; /api/bets merges
// maps across devices through a Vercel Blob copy when a sync key is set.

const BETS_STORAGE_KEY = 'hr-tracker-bets-v2'
const LEGACY_BETS_STORAGE_KEY = 'hr-tracker-bets-v1'
const SYNC_KEY_STORAGE_KEY = 'hr-tracker-sync-key-v1'

const TOMBSTONE_MAX_AGE_MS = 45 * 24 * 60 * 60 * 1000

// Stable identifier for a single pick across page loads:
//   date    — disambiguates the same batter appearing on multiple days
//   batterId— the player
//   gamePk  — disambiguates a double-header (same batter_id, two games one day)
function betKey(date, batterId, gamePk) {
  return `${date}|${batterId}|${gamePk ?? ''}`
}

// A v1 entry is the literal `true`; a v2 entry is { on, t }.
function entryOn(entry) {
  return entry === true || Boolean(entry && entry.on)
}

// Coerce anything parsed from storage / the network into a clean v2 map.
// v1 entries get t=0 so any real toggle (t > 0) wins a merge against them.
function normalizeBets(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {}
  const out = {}
  for (const [key, entry] of Object.entries(raw)) {
    if (entry === true) {
      out[key] = { on: true, t: 0 }
    } else if (entry && typeof entry === 'object' && typeof entry.on === 'boolean') {
      out[key] = { on: entry.on, t: Number.isFinite(entry.t) ? entry.t : 0 }
    }
    // anything else (false, strings, nulls) is corrupt — drop it
  }
  return out
}

// Flip membership immutably: returns a NEW map with `key` flipped at time `now`.
function toggleBet(bets, key, now = Date.now()) {
  const next = { ...bets }
  next[key] = { on: !entryOn(bets && bets[key]), t: now }
  return next
}

function isBet(bets, key) {
  return Boolean(bets) && entryOn(bets[key])
}

// Per-pick last-write-wins union of two v2 maps. On a timestamp tie, the
// flagged side wins — better to re-flag a bet than silently lose one.
function mergeBets(a, b) {
  const out = { ...a }
  for (const [key, entry] of Object.entries(b || {})) {
    const mine = out[key]
    if (!mine || entry.t > mine.t || (entry.t === mine.t && entry.on && !mine.on)) {
      out[key] = entry
    }
  }
  return out
}

// Drop un-flag tombstones old enough that every device has synced past them.
// Live flags are never pruned — they're the actual betting record.
function pruneBets(bets, now = Date.now()) {
  const out = {}
  for (const [key, entry] of Object.entries(bets || {})) {
    if (entry.on || now - entry.t <= TOMBSTONE_MAX_AGE_MS) out[key] = entry
  }
  return out
}

// Cheap deep-equality for two v2 maps (used to stop the sync loop once the
// server and client agree, without JSON.stringify key-order false negatives).
function betsEqual(a, b) {
  const ka = Object.keys(a || {})
  const kb = Object.keys(b || {})
  if (ka.length !== kb.length) return false
  for (const k of ka) {
    const ea = a[k]
    const eb = b && b[k]
    if (!eb || entryOn(ea) !== entryOn(eb) || (ea.t ?? 0) !== (eb.t ?? 0)) return false
  }
  return true
}

// How many flagged bets belong to a given archive date (per-day header badge).
function countBetsForDay(bets, date) {
  if (!bets) return 0
  const prefix = `${date}|`
  let n = 0
  for (const [k, entry] of Object.entries(bets)) {
    if (entryOn(entry) && k.startsWith(prefix)) n++
  }
  return n
}

// Total flagged bets across all days.
function countBets(bets) {
  if (!bets) return 0
  let n = 0
  for (const entry of Object.values(bets)) if (entryOn(entry)) n++
  return n
}

// Read the bet map from a Storage-like object, defensive against private-mode
// access errors and corrupt/legacy JSON. Falls back to the v1 key so flags
// from before the sync feature carry over. Always returns a plain v2 map.
function loadBets(storage) {
  try {
    const raw = storage.getItem(BETS_STORAGE_KEY) || storage.getItem(LEGACY_BETS_STORAGE_KEY)
    if (!raw) return {}
    return normalizeBets(JSON.parse(raw))
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
  LEGACY_BETS_STORAGE_KEY,
  SYNC_KEY_STORAGE_KEY,
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
}
