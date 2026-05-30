// Small pure formatting helpers for the tracker page, kept separate from the
// React component so they can be unit-tested without a DOM (see format.test.js).

const MONTHS_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// "Last refreshed" timestamp. Formatted entirely from UTC getters (no locale /
// timezone dependence) so the server-rendered HTML and the client hydration
// match byte-for-byte — otherwise React warns about a hydration mismatch when
// the build machine and the viewer's browser are in different timezones.
// Returns null for missing / unparseable input so callers can omit the line.
function fmtRefreshed(iso) {
  if (!iso) return null
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return null
  let h = d.getUTCHours()
  const ampm = h >= 12 ? 'pm' : 'am'
  h = h % 12 || 12
  const m = String(d.getUTCMinutes()).padStart(2, '0')
  return `${MONTHS_ABBR[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}, ${h}:${m}${ampm} UTC`
}

module.exports = { MONTHS_ABBR, fmtRefreshed }
