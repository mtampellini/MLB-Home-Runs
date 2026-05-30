// Unit tests for the tracker formatting helpers. Run with: node --test
const { test } = require('node:test')
const assert = require('node:assert/strict')

const { fmtRefreshed } = require('./format')

test('fmtRefreshed renders Eastern time, DST-aware, regardless of host timezone', () => {
  // The result must depend only on the instant + America/New_York, not the
  // host's TZ (server build vs client browser), so pin the process TZ far away.
  const prev = process.env.TZ
  process.env.TZ = 'America/Los_Angeles'
  try {
    // Summer -> EDT (UTC-4): 19:22Z = 3:22pm ET.
    assert.equal(fmtRefreshed('2026-05-30T19:22:00Z'), 'May 30, 2026, 3:22pm ET')
    assert.equal(fmtRefreshed('2026-07-04T23:09:00Z'), 'Jul 4, 2026, 7:09pm ET')
    // Winter -> EST (UTC-5). 00:05Z rolls back across midnight to the prior day.
    assert.equal(fmtRefreshed('2026-01-01T00:05:00Z'), 'Dec 31, 2025, 7:05pm ET')
    // Noon UTC in winter is 7:00am ET.
    assert.equal(fmtRefreshed('2026-12-31T12:00:00Z'), 'Dec 31, 2026, 7:00am ET')
    // Midnight ET (05:00Z in winter) renders as 12:xx am, not 0:xx.
    assert.equal(fmtRefreshed('2026-12-31T05:07:00Z'), 'Dec 31, 2026, 12:07am ET')
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
