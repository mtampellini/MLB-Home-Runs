// Small pure formatting helpers for the tracker page, kept separate from the
// React component so they can be unit-tested without a DOM (see format.test.js).

const MONTHS_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// "Last refreshed" timestamp, shown in Eastern time (the crons run on ET and
// that's where the audience is). EDT vs EST is resolved automatically by the
// America/New_York zone for the given instant.
//
// We pull the wall-clock fields out of Intl with formatToParts and reassemble
// the string ourselves rather than using the formatted output directly: the
// numeric parts (and AM/PM) are stable across ICU versions, whereas the joined
// string's punctuation/whitespace is not (newer ICU inserts a narrow no-break
// space before AM/PM). Assembling it by hand keeps the build-time (server) and
// browser (client) output byte-for-byte identical, avoiding a React hydration
// mismatch. Returns null for missing / unparseable input so callers can omit
// the line.
function fmtRefreshed(iso) {
  if (!iso) return null
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return null
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', minute: '2-digit', hour12: true,
  }).formatToParts(d)
  const get = (type) => parts.find(p => p.type === type)?.value
  const month = MONTHS_ABBR[Number(get('month')) - 1]
  const ampm = (get('dayPeriod') || '').toLowerCase()
  return `${month} ${get('day')}, ${get('year')}, ${get('hour')}:${get('minute')}${ampm} ET`
}

module.exports = { MONTHS_ABBR, fmtRefreshed }
