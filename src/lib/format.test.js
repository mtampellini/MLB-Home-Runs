// Unit tests for the tracker formatting helpers. Run with: node --test
const { test } = require('node:test')
const assert = require('node:assert/strict')

const { fmtRefreshed } = require('./format')

test('fmtRefreshed renders a stable UTC string regardless of host timezone', () => {
  // The whole point is timezone-independence (server build vs client browser),
  // so pin the process TZ to something far from UTC and assert UTC output.
  const prev = process.env.TZ
  process.env.TZ = 'America/Los_Angeles'
  try {
    assert.equal(fmtRefreshed('2026-05-30T19:22:00Z'), 'May 30, 2026, 7:22pm UTC')
    // Midnight and noon edge cases for the 12-hour clock.
    assert.equal(fmtRefreshed('2026-01-01T00:05:00Z'), 'Jan 1, 2026, 12:05am UTC')
    assert.equal(fmtRefreshed('2026-12-31T12:00:00Z'), 'Dec 31, 2026, 12:00pm UTC')
    // Single-digit minutes are zero-padded.
    assert.equal(fmtRefreshed('2026-07-04T23:09:00Z'), 'Jul 4, 2026, 11:09pm UTC')
  } finally {
    process.env.TZ = prev
  }
})

test('fmtRefreshed returns null for missing / unparseable input', () => {
  assert.equal(fmtRefreshed(null), null)
  assert.equal(fmtRefreshed(undefined), null)
  assert.equal(fmtRefreshed(''), null)
  assert.equal(fmtRefreshed('not a date'), null)
})
