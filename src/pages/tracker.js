import Head from 'next/head'
import Link from 'next/link'
import { useState, useMemo } from 'react'
import fs from 'fs'
import path from 'path'

const T = {
  bg: '#ffffff',
  border: '#e5e5e5',
  borderStrong: '#d4d4d4',
  text: '#0a0a0a',
  textMedium: '#525252',
  textLight: '#a3a3a3',
  bgSubtle: '#f5f5f5',
  accent: '#2563eb',
  positive: '#16a34a',
  negative: '#dc2626',
}
const FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, system-ui, sans-serif'
const TABULAR = { fontVariantNumeric: 'tabular-nums' }

const CALIBRATION_MIN_PICKS = 100   // calibration view shows numbers below this; visualizes above

// ─── data loading at build time ────────────────────────────────────────
export async function getStaticProps() {
  const archivesDir = path.join(process.cwd(), 'backend/data/daily_archives')
  let archives = []
  try {
    const files = fs.readdirSync(archivesDir).filter(f => /^\d{4}-\d{2}-\d{2}\.json$/.test(f))
    archives = files
      .map(f => {
        try {
          const data = JSON.parse(fs.readFileSync(path.join(archivesDir, f), 'utf8'))
          return { date: f.replace('.json', ''), data }
        } catch { return null }
      })
      .filter(Boolean)
      .sort((a, b) => b.date.localeCompare(a.date))
  } catch { /* directory not present yet — Day 1 case */ }

  let tracker = null
  try {
    tracker = JSON.parse(fs.readFileSync(
      path.join(process.cwd(), 'backend/data/processed/tracker.json'),
      'utf8'
    ))
  } catch { /* no tracker yet */ }

  return { props: { archives, tracker } }
}

// ─── helpers ──────────────────────────────────────────────────────────
function fmtPct(v, digits = 1) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—'
  return `${(v * 100).toFixed(digits)}%`
}
function fmtSigned(v, digits = 1) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(digits)}`
}
function fmtUnits(v) {
  if (v === null || v === undefined) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}u`
}
function fmtOdds(odds) {
  if (odds == null) return '—'
  return odds > 0 ? `+${odds}` : `${odds}`
}
function fmtGameTime(iso) {
  if (!iso) return ''
  try {
    return new Date(iso).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
      .replace(' AM', 'am').replace(' PM', 'pm')
  } catch { return '' }
}
function summarizeArchive(archive) {
  const funnel = archive.funnel || {}
  const settle = archive.settlement || null
  const primarySummary = settle?.primary_summary || null
  const wins = primarySummary?.n_wins ?? null
  const losses = primarySummary?.n_losses ?? null
  const voids = primarySummary?.n_voids ?? null
  const settled = (wins ?? 0) + (losses ?? 0)
  const hitRate = settled > 0 ? wins / settled : null
  const profit = primarySummary?.units_profit ?? null
  const roiPct = primarySummary?.roi_pct ?? null
  return {
    primary_count: funnel.primary_count ?? archive.primary_picks?.length ?? 0,
    secondary_count: funnel.secondary_count ?? archive.secondary_picks?.length ?? 0,
    shadow_count: funnel.shadow_count ?? archive.shadow_picks?.length ?? 0,
    wins, losses, voids, hitRate, profit, roiPct,
    isSettled: settle != null,
  }
}

// ─── calibration buckets ──────────────────────────────────────────────
const CALIBRATION_BUCKETS = [
  { lo: 0.05, hi: 0.10 },
  { lo: 0.10, hi: 0.15 },
  { lo: 0.15, hi: 0.20 },
  { lo: 0.20, hi: 0.25 },
  { lo: 0.25, hi: 0.30 },
  { lo: 0.30, hi: 0.40 },
  { lo: 0.40, hi: 0.60 },
]
function buildCalibration(archives) {
  const allSettled = []
  for (const { data } of archives) {
    const settle = data.settlement
    if (!settle) continue
    const tiers = ['primary_results', 'secondary_results', 'shadow_results']
    for (const key of tiers) {
      for (const r of settle[key] || []) {
        if (r.outcome === 'VOID') continue
        allSettled.push({ model_prob: r.model_prob, won: r.outcome === 'W' })
      }
    }
  }
  const buckets = CALIBRATION_BUCKETS.map(({ lo, hi }) => {
    const inRange = allSettled.filter(r => r.model_prob >= lo && r.model_prob < hi)
    const n = inRange.length
    const expected = n > 0 ? inRange.reduce((a, r) => a + r.model_prob, 0) / n : null
    const actual = n > 0 ? inRange.filter(r => r.won).length / n : null
    const drift = (expected != null && actual != null) ? Math.abs(expected - actual) : null
    return { lo, hi, n, expected, actual, drift }
  })
  return { buckets, total: allSettled.length }
}

// ─── components ────────────────────────────────────────────────────────
function StatCard({ label, value, sub, tone = 'default' }) {
  // tone: 'default' | 'positive' | 'negative' | 'muted'
  const valueColor =
    tone === 'positive' ? T.positive
    : tone === 'negative' ? T.negative
    : tone === 'muted' ? T.textLight
    : T.text
  return (
    <div style={{
      border: `1px solid ${T.border}`, borderRadius: 6,
      padding: '24px 26px', minWidth: 140, flex: '1 1 140px',
      background: T.bg,
    }}>
      <div style={{
        fontSize: 32, fontWeight: 600, color: valueColor,
        letterSpacing: -0.5, lineHeight: 1.1, ...TABULAR,
      }}>{value}</div>
      <div style={{ fontSize: 12, color: T.textMedium, marginTop: 10 }}>{label}</div>
      {sub && <div style={{ fontSize: 11, color: T.textLight, marginTop: 4 }}>{sub}</div>}
    </div>
  )
}

function PickRow({ pick, settledPick, isPersonal, onTogglePersonal }) {
  const bp = pick.best_book === 'draftkings' ? pick.dk_odds : pick.fd_odds
  const otherLabel = pick.best_book === 'draftkings' ? 'FD' : 'DK'
  const otherPrice = pick.best_book === 'draftkings' ? pick.fd_odds : pick.dk_odds
  const settledOutcome = settledPick?.outcome

  const evPositive = pick.ev_pct >= 0
  const edgePositive = pick.edge_pct >= 0

  // Quiet meta line: team · hand · #spot · tier · stacked · low conf · unstable
  const tierLabel =
    pick.tier === 'primary' ? 'primary'
    : pick.tier === 'secondary' ? 'secondary'
    : pick.tier === 'shadow' ? 'shadow'
    : null
  const metaParts = []
  metaParts.push(pick.team)
  if (pick.batter_hand) metaParts.push(`${pick.batter_hand}H`)
  if (pick.lineup_spot) metaParts.push(`#${pick.lineup_spot}`)
  if (tierLabel) metaParts.push(tierLabel)
  if (pick.stacked) metaParts.push('stacked')
  if (pick.low_confidence) metaParts.push('low conf')
  if (pick.unstable_recent) metaParts.push('unstable')

  const onShare = async (e) => {
    e.stopPropagation()
    const txt = (
      `${pick.batter} | ${pick.pitcher || '?'} | ` +
      `${pick.best_book === 'draftkings' ? 'DK' : 'FD'} ${fmtOdds(bp)} | ` +
      `Model ${(pick.model_prob * 100).toFixed(1)}% / ` +
      `Market ${(pick.market_prob_devig * 100).toFixed(1)}% / ` +
      `EV ${pick.ev_pct >= 0 ? '+' : ''}${pick.ev_pct.toFixed(1)}%`
    )
    try { await navigator.clipboard.writeText(txt) }
    catch { /* clipboard API failed */ }
  }

  // Result cell: W green, L red, VOID gray, pending = pitcher · time in light gray.
  const renderResult = () => {
    if (settledOutcome === 'W') {
      return (
        <span style={{ color: T.positive, fontWeight: 600 }}>
          W <span style={{ color: T.textLight, fontWeight: 500, marginLeft: 4 }}>{fmtSigned(settledPick.profit_units, 2)}u</span>
        </span>
      )
    }
    if (settledOutcome === 'L') return <span style={{ color: T.negative, fontWeight: 600 }}>L −1u</span>
    if (settledOutcome === 'VOID') return <span style={{ color: T.textLight, fontWeight: 500 }}>VOID</span>
    return (
      <span style={{ color: T.textLight, fontWeight: 400, whiteSpace: 'nowrap' }}>
        vs {pick.pitcher || '?'}
        {pick.game_datetime && <> · {fmtGameTime(pick.game_datetime)}</>}
      </span>
    )
  }

  return (
    <tr style={{ borderBottom: `1px solid ${T.border}` }}>
      <td style={{
        padding: '14px 8px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 11, color: T.textLight, ...TABULAR,
      }}>
        {pick.tier_rank ?? pick.daily_rank}
      </td>
      <td style={{ padding: '14px 8px', verticalAlign: 'top', whiteSpace: 'nowrap' }}>
        <div style={{ fontWeight: 600, color: T.text, fontSize: 13 }}>{pick.batter}</div>
        <div style={{ fontSize: 11, color: T.textLight, marginTop: 4 }}
             title={pick.stacked ? `stacked with ${(pick.stacked_with || []).join(', ')}` : undefined}>
          {metaParts.join(' · ')}
        </div>
      </td>
      <td style={{
        padding: '14px 8px', verticalAlign: 'top', whiteSpace: 'nowrap',
        fontSize: 12, color: T.textMedium,
      }}>{pick.pitcher || '—'}</td>

      <td style={{
        padding: '14px 8px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 13, fontWeight: 600, color: T.text, ...TABULAR,
      }}>{(pick.model_prob * 100).toFixed(1)}%</td>

      <td style={{
        padding: '14px 8px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 12, color: T.textMedium, ...TABULAR,
      }}>{(pick.market_prob_devig * 100).toFixed(1)}%</td>

      <td style={{
        padding: '14px 8px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 12, fontWeight: 600,
        color: edgePositive ? T.positive : T.text, ...TABULAR,
      }}>{edgePositive ? '+' : ''}{pick.edge_pct.toFixed(1)}pp</td>

      <td style={{
        padding: '14px 8px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 13, fontWeight: 700,
        color: evPositive ? T.positive : T.text, ...TABULAR,
      }}>{evPositive ? '+' : ''}{pick.ev_pct.toFixed(0)}%</td>

      <td style={{
        padding: '14px 8px', textAlign: 'right', verticalAlign: 'top', whiteSpace: 'nowrap',
        ...TABULAR,
      }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: T.text }}>
          {fmtOdds(bp)} <span style={{ fontSize: 10, color: T.textLight, fontWeight: 500 }}>{pick.best_book === 'draftkings' ? 'DK' : 'FD'}</span>
        </div>
        <div style={{ fontSize: 10, color: T.textLight, marginTop: 4 }}>
          {fmtOdds(otherPrice)} {otherLabel}
        </div>
      </td>

      <td style={{ padding: '14px 8px', verticalAlign: 'top', textAlign: 'left', fontSize: 12 }}>
        {renderResult()}
      </td>

      <td style={{ padding: '14px 8px', verticalAlign: 'top', textAlign: 'center' }}>
        <button onClick={(e) => { e.stopPropagation(); onTogglePersonal && onTogglePersonal() }}
          style={{
            background: isPersonal ? T.text : 'transparent',
            border: `1px solid ${isPersonal ? T.text : T.border}`,
            borderRadius: 999,
            color: isPersonal ? '#ffffff' : T.textMedium,
            fontSize: 11, fontFamily: 'inherit', fontWeight: 500,
            padding: '4px 12px', cursor: 'pointer', whiteSpace: 'nowrap',
            letterSpacing: 0,
          }}
          title={isPersonal ? 'You bet this' : 'Click if you bet this'}>
          {isPersonal ? '✓ bet' : 'log bet'}
        </button>
      </td>

      <td style={{ padding: '14px 8px', verticalAlign: 'top', textAlign: 'center' }}>
        <button onClick={onShare} style={{
          background: 'transparent', border: `1px solid ${T.border}`, borderRadius: 4,
          color: T.textMedium, fontSize: 11, padding: '4px 10px', cursor: 'pointer',
          fontFamily: 'inherit',
        }} title="Copy pick details to clipboard">share</button>
      </td>
    </tr>
  )
}

function DayBlock({ archive, expanded, onToggle, tierFilter, personalBets, togglePersonal }) {
  const summary = summarizeArchive(archive.data)
  const data = archive.data

  const picks = useMemo(() => {
    const all = []
    if (tierFilter === 'all' || tierFilter === 'primary') all.push(...(data.primary_picks || []))
    if (tierFilter === 'all') all.push(...(data.secondary_picks || []))
    if (tierFilter === 'all') all.push(...(data.shadow_picks || []))
    return all
  }, [data, tierFilter])

  const settledByKey = useMemo(() => {
    const map = {}
    const settle = data.settlement
    if (!settle) return map
    for (const key of ['primary_results', 'secondary_results', 'shadow_results']) {
      for (const r of settle[key] || []) {
        const k = `${r.batter_id}|${r.game_pk || ''}`
        map[k] = r
      }
    }
    return map
  }, [data])

  // Header: clean line of date | counts | W-L | ROI. No bg color.
  const profitTone =
    summary.profit == null ? T.textLight
    : summary.profit > 0 ? T.positive
    : summary.profit < 0 ? T.negative
    : T.textMedium

  return (
    <div style={{ borderTop: `1px solid ${T.border}` }}>
      <button onClick={onToggle} style={{
        width: '100%', padding: '18px 4px', background: 'none', border: 'none',
        color: T.text, cursor: 'pointer', fontFamily: 'inherit',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        flexWrap: 'wrap', gap: 12, textAlign: 'left',
      }}>
        <div style={{ display: 'flex', gap: 18, alignItems: 'baseline', flexWrap: 'wrap' }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: T.text }}>{archive.date}</span>
          <span style={{ fontSize: 12, color: T.textLight }}>
            {summary.primary_count} primary
            {summary.secondary_count > 0 && ` · ${summary.secondary_count} secondary`}
            {summary.shadow_count > 0 && ` · ${summary.shadow_count} shadow`}
          </span>
          {summary.isSettled ? (
            <>
              <span style={{ fontSize: 12, color: T.textMedium, ...TABULAR }}>
                {summary.wins}W–{summary.losses}L{summary.voids > 0 && `–${summary.voids}V`}
              </span>
              <span style={{ fontSize: 12, color: profitTone, fontWeight: 600, ...TABULAR }}>
                {fmtUnits(summary.profit)} ({summary.roiPct >= 0 ? '+' : ''}{summary.roiPct?.toFixed(1)}%)
              </span>
            </>
          ) : (
            <span style={{ fontSize: 12, color: T.textLight }}>unsettled</span>
          )}
        </div>
        <span style={{
          fontSize: 12, color: T.textLight,
          transform: expanded ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s',
        }}>▾</span>
      </button>
      {expanded && (
        <div style={{ padding: '0 0 24px', overflowX: 'auto', WebkitOverflowScrolling: 'touch' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, minWidth: 920 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${T.borderStrong}` }}>
                {[
                  ['#', 'right'], ['Batter', 'left'], ['Pitcher', 'left'],
                  ['Model', 'right'], ['Market', 'right'], ['Edge', 'right'],
                  ['EV', 'right'], ['Odds', 'right'],
                  ['Result', 'left'], ['Bet', 'center'], ['Share', 'center'],
                ].map(([h, align]) => (
                  <th key={h} style={{
                    padding: '12px 8px', fontSize: 11, fontWeight: 500,
                    color: T.textMedium, letterSpacing: 0.4, textAlign: align,
                  }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {picks.map(p => {
                const k = `${p.batter_id}|${p.game_pk || ''}`
                const personalKey = `${archive.date}|${k}`
                return (
                  <PickRow key={k} pick={p}
                            settledPick={settledByKey[k]}
                            isPersonal={personalBets[personalKey] === true}
                            onTogglePersonal={() => togglePersonal(personalKey)} />
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function CalibrationView({ archives }) {
  const cal = useMemo(() => buildCalibration(archives), [archives])
  if (cal.total < CALIBRATION_MIN_PICKS) {
    return (
      <div style={{
        border: `1px solid ${T.border}`, borderRadius: 6,
        padding: '24px 26px', color: T.textMedium, fontSize: 13, lineHeight: 1.6,
      }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: T.text, marginBottom: 10 }}>Calibration</div>
        Need {CALIBRATION_MIN_PICKS}+ settled picks across all tiers for meaningful
        calibration. Currently <strong style={{ color: T.text, fontWeight: 600 }}>{cal.total}</strong>.
        <div style={{ fontSize: 12, color: T.textLight, marginTop: 10 }}>
          The chart will appear here once we cross the threshold.
        </div>
      </div>
    )
  }
  const maxN = Math.max(...cal.buckets.map(b => b.n || 0), 1)
  return (
    <div style={{
      border: `1px solid ${T.border}`, borderRadius: 6,
      padding: '20px 26px',
    }}>
      <div style={{ fontSize: 14, fontWeight: 600, color: T.text, marginBottom: 4 }}>
        Calibration
      </div>
      <div style={{ fontSize: 12, color: T.textLight, marginBottom: 18 }}>
        {cal.total} settled picks · predicted vs actual hit rate per model-prob bucket
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${T.border}` }}>
            <th style={{ padding: '10px 8px 10px 0', textAlign: 'left', fontSize: 11, color: T.textMedium, fontWeight: 500 }}>Bucket</th>
            <th style={{ padding: '10px 8px', textAlign: 'right', fontSize: 11, color: T.textMedium, fontWeight: 500 }}>n</th>
            <th style={{ padding: '10px 8px', textAlign: 'right', fontSize: 11, color: T.textMedium, fontWeight: 500 }}>Expected</th>
            <th style={{ padding: '10px 8px', textAlign: 'right', fontSize: 11, color: T.textMedium, fontWeight: 500 }}>Actual</th>
            <th style={{ padding: '10px 8px', textAlign: 'right', fontSize: 11, color: T.textMedium, fontWeight: 500 }}>Drift</th>
            <th style={{ padding: '10px 0 10px 8px', textAlign: 'left', fontSize: 11, color: T.textMedium, fontWeight: 500, width: '30%' }}>Volume</th>
          </tr>
        </thead>
        <tbody>
          {cal.buckets.map(b => {
            const driftBad = b.drift != null && b.drift > 0.02
            return (
              <tr key={b.lo} style={{ borderBottom: `1px solid ${T.border}` }}>
                <td style={{ padding: '14px 8px 14px 0', color: T.text, fontSize: 12, ...TABULAR }}>
                  {Math.round(b.lo*100)}–{Math.round(b.hi*100)}%
                </td>
                <td style={{ padding: '14px 8px', textAlign: 'right', fontSize: 12, color: T.textMedium, ...TABULAR }}>{b.n || '—'}</td>
                <td style={{ padding: '14px 8px', textAlign: 'right', fontSize: 12, color: T.textMedium, ...TABULAR }}>
                  {b.expected != null ? `${(b.expected*100).toFixed(1)}%` : '—'}
                </td>
                <td style={{ padding: '14px 8px', textAlign: 'right', fontSize: 12, color: T.text, fontWeight: 600, ...TABULAR }}>
                  {b.actual != null ? `${(b.actual*100).toFixed(1)}%` : '—'}
                </td>
                <td style={{
                  padding: '14px 8px', textAlign: 'right', fontSize: 12,
                  color: driftBad ? T.negative : T.textMedium,
                  fontWeight: driftBad ? 600 : 400, ...TABULAR,
                }}>
                  {b.drift != null ? `±${(b.drift*100).toFixed(1)}pp` : '—'}
                </td>
                <td style={{ padding: '14px 0 14px 8px' }}>
                  {b.n > 0 && (
                    <div style={{
                      height: 6, width: `${(b.n / maxN) * 100}%`,
                      background: T.bgSubtle, borderRadius: 2,
                      borderRight: `2px solid ${T.borderStrong}`,
                    }} />
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// Filter button — text-only, underline + bold on active.
function FilterButton({ active, onClick, children }) {
  return (
    <button onClick={onClick} style={{
      background: 'transparent', border: 'none', padding: '4px 0',
      color: active ? T.text : T.textMedium,
      fontWeight: active ? 600 : 400,
      fontSize: 13, cursor: 'pointer', fontFamily: 'inherit',
      textDecoration: active ? 'underline' : 'none',
      textUnderlineOffset: 6, textDecorationThickness: 1.5,
    }}>{children}</button>
  )
}
function FilterRow({ label, options, value, onChange }) {
  return (
    <div style={{ display: 'flex', gap: 18, alignItems: 'center', flexWrap: 'wrap' }}>
      <span style={{ fontSize: 11, color: T.textLight, minWidth: 50,
                      textTransform: 'uppercase', letterSpacing: 0.6 }}>{label}</span>
      {options.map(([k, lbl], i) => (
        <span key={k} style={{ display: 'inline-flex', alignItems: 'center', gap: 18 }}>
          <FilterButton active={value === k} onClick={() => onChange(k)}>{lbl}</FilterButton>
        </span>
      ))}
    </div>
  )
}

// ─── page component ────────────────────────────────────────────────────
export default function Tracker({ archives, tracker }) {
  const [tierFilter, setTierFilter] = useState('primary')
  const [dateFilter, setDateFilter] = useState('all')
  const [sortBy, setSortBy] = useState('date_desc')
  const [expanded, setExpanded] = useState(() => {
    const newest = archives[0]?.date
    return newest ? { [newest]: true } : {}
  })
  const [personalBets, setPersonalBets] = useState({})

  // Hydrate localStorage personal bets on mount.
  useMemo(() => {
    if (typeof window === 'undefined') return
    try {
      const saved = window.localStorage.getItem('hr-picks-personal-bets')
      if (saved) setPersonalBets(JSON.parse(saved))
    } catch { /* ignore */ }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  const togglePersonal = (key) => {
    setPersonalBets(prev => {
      const next = { ...prev, [key]: !prev[key] }
      if (typeof window !== 'undefined') {
        try { window.localStorage.setItem('hr-picks-personal-bets', JSON.stringify(next)) } catch {}
      }
      return next
    })
  }

  // Filter + sort the archive list.
  const filteredArchives = useMemo(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    let list = archives
    if (dateFilter === '7d') {
      const cutoff = new Date(today); cutoff.setDate(cutoff.getDate() - 7)
      list = list.filter(a => new Date(a.date) >= cutoff)
    } else if (dateFilter === '30d') {
      const cutoff = new Date(today); cutoff.setDate(cutoff.getDate() - 30)
      list = list.filter(a => new Date(a.date) >= cutoff)
    }
    const sorted = [...list]
    if (sortBy === 'date_desc') sorted.sort((a, b) => b.date.localeCompare(a.date))
    else if (sortBy === 'date_asc') sorted.sort((a, b) => a.date.localeCompare(b.date))
    else if (sortBy === 'roi') sorted.sort((a, b) => (summarizeArchive(b.data).roiPct ?? -Infinity) - (summarizeArchive(a.data).roiPct ?? -Infinity))
    else if (sortBy === 'hit_rate') sorted.sort((a, b) => (summarizeArchive(b.data).hitRate ?? -Infinity) - (summarizeArchive(a.data).hitRate ?? -Infinity))
    else if (sortBy === 'count') sorted.sort((a, b) => summarizeArchive(b.data).primary_count - summarizeArchive(a.data).primary_count)
    return sorted
  }, [archives, dateFilter, sortBy])

  const sumP = tracker?.summary_primary || tracker?.summary || {}
  const totalSettled = (sumP.wins || 0) + (sumP.losses || 0)
  const isFirstWeek = totalSettled === 0

  const daysSinceDeploy = (() => {
    if (archives.length === 0) return 0
    const first = new Date(archives[archives.length - 1].date)
    const today = new Date()
    return Math.max(0, Math.floor((today - first) / (1000 * 60 * 60 * 24)) + 1)
  })()

  const personalPerf = useMemo(() => {
    let bets = 0, wins = 0, losses = 0, profit = 0
    for (const { date, data } of archives) {
      const settle = data.settlement
      if (!settle) continue
      const settled = {}
      for (const key of ['primary_results', 'secondary_results', 'shadow_results']) {
        for (const r of settle[key] || []) {
          settled[`${r.batter_id}|${r.game_pk || ''}`] = r
        }
      }
      for (const tierKey of ['primary_picks', 'secondary_picks', 'shadow_picks']) {
        for (const p of data[tierKey] || []) {
          const personalKey = `${date}|${p.batter_id}|${p.game_pk || ''}`
          if (!personalBets[personalKey]) continue
          const k = `${p.batter_id}|${p.game_pk || ''}`
          const r = settled[k]
          if (!r || r.outcome === 'VOID') continue
          bets++
          if (r.outcome === 'W') { wins++; profit += r.profit_units }
          else { losses++; profit -= 1 }
        }
      }
    }
    const roiPct = bets > 0 ? (profit / bets) * 100 : null
    return { bets, wins, losses, profit, roiPct }
  }, [archives, personalBets])

  const stackingMetric = useMemo(() => {
    let totalDays = archives.length
    let stackedDays = 0
    for (const { data } of archives) {
      if ((data.primary_picks || []).some(p => p.stacked)) stackedDays++
    }
    return { totalDays, stackedDays, pct: totalDays > 0 ? stackedDays / totalDays : 0 }
  }, [archives])

  return (
    <>
      <Head>
        <title>HR Picks — Tracker</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>

      <style jsx global>{`
        html, body { margin: 0; padding: 0; background: ${T.bg}; }
        a:hover { text-decoration: underline; }
      `}</style>

      <div style={{
        minHeight: '100vh', background: T.bg, color: T.text,
        fontFamily: FONT, padding: '40px 24px',
        maxWidth: 1080, margin: '0 auto',
      }}>
        {/* Header — minimal. Site title + Tracker label. */}
        <div style={{ marginBottom: 36 }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 24, flexWrap: 'wrap' }}>
            <Link href="/" style={{
              fontSize: 22, fontWeight: 700, color: T.text,
              letterSpacing: -0.4, textDecoration: 'none',
            }}>HR Picks</Link>
            <span style={{ fontSize: 14, color: T.text, fontWeight: 500 }}>Tracker</span>
          </div>
          <div style={{ fontSize: 12, color: T.textLight, marginTop: 8 }}>
            {archives.length} archived day{archives.length === 1 ? '' : 's'} · day {daysSinceDeploy} since deploy
          </div>
        </div>

        {/* Day-1 banner — minimal panel, no colored fill. */}
        {isFirstWeek && (
          <div style={{
            border: `1px solid ${T.border}`, borderRadius: 6,
            padding: '16px 20px', marginBottom: 28,
            fontSize: 13, color: T.textMedium, lineHeight: 1.5,
          }}>
            <strong style={{ color: T.text, fontWeight: 600 }}>Day {daysSinceDeploy} — no settled picks yet.</strong>{' '}
            Today's picks settle tomorrow at ~10am ET. ROI, hit rate, and CLV will populate after the first settlement run.
          </div>
        )}

        {/* Big metrics */}
        <div style={{ display: 'flex', gap: 14, marginBottom: 28, flexWrap: 'wrap' }}>
          <StatCard
            label="Picks settled"
            value={totalSettled}
            sub={`${sumP.wins || 0}W–${sumP.losses || 0}L · ${sumP.voids || 0} void`}
            tone={totalSettled > 0 ? 'default' : 'muted'}
          />
          <StatCard
            label="Hit rate"
            value={totalSettled > 0 ? fmtPct(sumP.hit_rate, 1) : '—'}
            sub={totalSettled > 0 ? null : 'awaiting settlement'}
            tone={totalSettled > 0 ? 'default' : 'muted'}
          />
          <StatCard
            label="ROI"
            value={totalSettled > 0 ? `${sumP.roi_pct >= 0 ? '+' : ''}${sumP.roi_pct?.toFixed(1)}%` : '—'}
            sub={totalSettled > 0 ? `${fmtUnits(sumP.units_profit)} on ${sumP.units_staked || 0}u` : null}
            tone={totalSettled > 0 ? (sumP.roi_pct >= 0 ? 'positive' : 'negative') : 'muted'}
          />
          <StatCard
            label="Avg CLV"
            value={sumP.avg_clv_pct != null ? `${sumP.avg_clv_pct >= 0 ? '+' : ''}${sumP.avg_clv_pct.toFixed(1)}%` : '—'}
            sub={sumP.n_picks_with_clv ? `${sumP.n_picks_with_clv} picks` : 'awaiting closing snaps'}
            tone={sumP.avg_clv_pct != null ? (sumP.avg_clv_pct >= 0 ? 'positive' : 'negative') : 'muted'}
          />
        </div>

        {/* Personal-bet card */}
        {personalPerf.bets > 0 && (
          <div style={{ display: 'flex', gap: 14, marginBottom: 28, flexWrap: 'wrap' }}>
            <StatCard
              label="Your bets"
              value={personalPerf.bets}
              sub={`${personalPerf.wins}W–${personalPerf.losses}L`}
            />
            <StatCard
              label="Your ROI"
              value={personalPerf.roiPct != null ? `${personalPerf.roiPct >= 0 ? '+' : ''}${personalPerf.roiPct.toFixed(1)}%` : '—'}
              sub={fmtUnits(personalPerf.profit)}
              tone={personalPerf.roiPct >= 0 ? 'positive' : 'negative'}
            />
            <StatCard
              label="Stacked days"
              value={`${(stackingMetric.pct * 100).toFixed(0)}%`}
              sub={`${stackingMetric.stackedDays}/${stackingMetric.totalDays} days`}
              tone="muted"
            />
          </div>
        )}

        {/* Filters — text-only, underline on active. */}
        <div style={{
          padding: '20px 0', marginBottom: 8,
          borderTop: `1px solid ${T.border}`, borderBottom: `1px solid ${T.border}`,
          display: 'flex', flexDirection: 'column', gap: 14,
        }}>
          <FilterRow label="Tier"  value={tierFilter} onChange={setTierFilter}
            options={[['primary', 'Primary only'], ['all', 'All tiers']]} />
          <FilterRow label="Range" value={dateFilter} onChange={setDateFilter}
            options={[['all', 'All time'], ['30d', '30 days'], ['7d', '7 days']]} />
          <FilterRow label="Sort"  value={sortBy} onChange={setSortBy}
            options={[
              ['date_desc', 'Date (newest)'],
              ['date_asc',  'Date (oldest)'],
              ['roi',       'ROI'],
              ['hit_rate',  'Hit rate'],
              ['count',     'Pick count'],
            ]} />
        </div>

        {/* Day blocks */}
        {filteredArchives.length === 0 ? (
          <div style={{
            padding: '48px 18px', textAlign: 'center', color: T.textLight, fontSize: 13,
            border: `1px solid ${T.border}`, borderRadius: 6, marginTop: 28,
          }}>
            No archived days yet. The first cron run will populate this list tomorrow at 11am ET.
          </div>
        ) : (
          <div style={{ marginTop: 8, marginBottom: 36 }}>
            {filteredArchives.map(a => (
              <DayBlock key={a.date} archive={a}
                        expanded={!!expanded[a.date]}
                        onToggle={() => setExpanded(p => ({ ...p, [a.date]: !p[a.date] }))}
                        tierFilter={tierFilter}
                        personalBets={personalBets}
                        togglePersonal={togglePersonal} />
            ))}
            {/* Bottom border to close the last day block */}
            <div style={{ borderTop: `1px solid ${T.border}` }} />
          </div>
        )}

        {/* Calibration view — placed below day blocks. */}
        <div style={{ marginTop: 36 }}>
          <CalibrationView archives={archives} />
        </div>

        {/* Footer */}
        <div style={{
          marginTop: 40, paddingTop: 20, borderTop: `1px solid ${T.border}`,
          fontSize: 11, color: T.textLight, lineHeight: 1.7,
        }}>
          Calibration computed across all settled tiers (primary + secondary + shadow).
          Personal-bet log stored in your browser only. "stacked" picks share a starting
          pitcher with another primary pick today — outcomes correlated.
        </div>
      </div>
    </>
  )
}
