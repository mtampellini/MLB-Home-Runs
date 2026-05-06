import Head from 'next/head'
import Link from 'next/link'
import { useState, useMemo } from 'react'
import fs from 'fs'
import path from 'path'

// Theme — match the home page so the two feel like one app.
const ACCENT = '#22c55e'
const ACCENT_RED = '#ef4444'
const YELLOW = '#facc15'
const ORANGE = '#fb923c'
const BLUE = '#3b82f6'
const PURPLE = '#a855f7'
const BG = '#06090f'
const CARD_BG = '#0c1220'
const BORDER = '#1a2332'
const MUTED = '#475569'
const TEXT = '#94a3b8'
const BRIGHT = '#e2e8f0'
const MONO = 'JetBrains Mono, monospace'
const SANS = 'DM Sans, sans-serif'

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

  return {
    props: { archives, tracker },
    // No revalidate — rebuilt on every commit which is when picks change anyway.
  }
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
function StatCard({ label, value, color, sub }) {
  return (
    <div style={{
      background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10,
      padding: '14px 18px', minWidth: 130, flex: '1 1 130px',
    }}>
      <div style={{ fontSize: 10, color: MUTED, textTransform: 'uppercase', letterSpacing: 1.2, fontFamily: MONO }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 800, color: color || BRIGHT, marginTop: 2, fontFamily: SANS }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: MUTED, marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

function PickRow({ pick, settledPick, isPersonal, onTogglePersonal }) {
  const bp = pick.best_book === 'draftkings' ? pick.dk_odds : pick.fd_odds
  const otherLabel = pick.best_book === 'draftkings' ? 'FD' : 'DK'
  const otherPrice = pick.best_book === 'draftkings' ? pick.fd_odds : pick.dk_odds
  const tierLabel = pick.tier?.[0]?.toUpperCase() + pick.tier?.slice(1) || '—'
  const tierColor = pick.tier === 'primary' ? ACCENT
                  : pick.tier === 'secondary' ? BLUE
                  : MUTED
  const settledOutcome = settledPick?.outcome
  const settledBg = settledOutcome === 'W' ? 'rgba(34,197,94,0.10)'
                   : settledOutcome === 'L' ? 'rgba(239,68,68,0.08)'
                   : settledOutcome === 'VOID' ? 'rgba(168,85,247,0.08)'
                   : null

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
    catch { /* clipboard API failed; mobile permission etc. */ }
  }

  return (
    <tr style={{ borderBottom: `1px solid ${BORDER}`, background: settledBg || 'transparent' }}>
      <td style={{ padding: '8px 6px', fontFamily: MONO, fontSize: 11, color: MUTED, textAlign: 'right' }}>
        #{pick.tier_rank ?? pick.daily_rank}
      </td>
      <td style={{ padding: '8px 8px', whiteSpace: 'nowrap' }}>
        <div style={{ fontWeight: 600, color: BRIGHT, fontSize: 13 }}>{pick.batter}</div>
        <div style={{ fontSize: 10, color: MUTED, fontFamily: MONO, marginTop: 1, display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap' }}>
          <span>{pick.team}</span>
          {pick.batter_hand && <span>· {pick.batter_hand}H</span>}
          {pick.lineup_spot && <span>· #{pick.lineup_spot}</span>}
          <span style={{ color: tierColor, fontWeight: 700 }}>· {tierLabel}</span>
          {pick.stacked && (
            <span style={{ color: YELLOW, background: 'rgba(250,204,21,0.10)', padding: '0 4px', borderRadius: 3 }}
                  title={`Correlated: ${(pick.stacked_with || []).join(', ')}`}>⛓</span>
          )}
          {pick.low_confidence && <span style={{ color: ORANGE }}>LC</span>}
          {pick.unstable_recent && <span style={{ color: ACCENT_RED }}>UR</span>}
        </div>
      </td>
      <td style={{ padding: '8px 6px', fontSize: 11, color: TEXT, fontFamily: MONO, whiteSpace: 'nowrap' }}>
        {pick.pitcher || '—'}
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12, fontWeight: 600, color: BRIGHT }}>
        {(pick.model_prob * 100).toFixed(1)}%
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12, color: TEXT }}>
        {(pick.market_prob_devig * 100).toFixed(1)}%
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12,
                  color: pick.edge_pct >= 15 ? ACCENT : (pick.edge_pct >= 10 ? YELLOW : TEXT) }}>
        {pick.edge_pct >= 0 ? '+' : ''}{pick.edge_pct.toFixed(1)}pp
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12,
                  color: pick.ev_pct >= 40 ? ACCENT : (pick.ev_pct >= 25 ? YELLOW : TEXT), fontWeight: 700 }}>
        {pick.ev_pct >= 0 ? '+' : ''}{pick.ev_pct.toFixed(0)}%
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12, fontWeight: 700, color: BRIGHT, whiteSpace: 'nowrap' }}>
        {fmtOdds(bp)} <span style={{ fontSize: 9, color: MUTED }}>{pick.best_book === 'draftkings' ? 'DK' : 'FD'}</span>
        <div style={{ fontSize: 9, color: MUTED, fontWeight: 500 }}>{fmtOdds(otherPrice)} {otherLabel}</div>
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'center', fontFamily: MONO, fontSize: 11, fontWeight: 700,
                  color: settledOutcome === 'W' ? ACCENT
                       : settledOutcome === 'L' ? ACCENT_RED
                       : settledOutcome === 'VOID' ? PURPLE : MUTED }}>
        {settledOutcome === 'W' ? `W +${fmtSigned(settledPick.profit_units, 2)}u`
         : settledOutcome === 'L' ? 'L −1u'
         : settledOutcome === 'VOID' ? 'VOID'
         : (
           <span style={{ color: MUTED, fontWeight: 500, whiteSpace: 'nowrap' }}>
             vs {pick.pitcher || '?'}
             {pick.game_datetime && <> · {fmtGameTime(pick.game_datetime)}</>}
           </span>
         )}
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'center' }}>
        <button onClick={(e) => { e.stopPropagation(); onTogglePersonal && onTogglePersonal() }}
          style={{
            background: isPersonal ? 'rgba(34,197,94,0.15)' : 'transparent',
            border: `1px solid ${isPersonal ? ACCENT : BORDER}`,
            borderRadius: 4, color: isPersonal ? ACCENT : MUTED,
            fontSize: 10, fontFamily: MONO, fontWeight: 600,
            padding: '3px 8px', cursor: 'pointer', whiteSpace: 'nowrap',
          }}
          title={isPersonal ? 'You bet this' : 'Click if you bet this'}>
          {isPersonal ? '✓ BET' : 'passed'}
        </button>
      </td>
      <td style={{ padding: '8px 6px', textAlign: 'center' }}>
        <button onClick={onShare} style={{
          background: 'transparent', border: `1px solid ${BORDER}`, borderRadius: 4,
          color: MUTED, fontSize: 10, padding: '3px 8px', cursor: 'pointer',
        }} title="Copy pick details to clipboard">📋</button>
      </td>
    </tr>
  )
}

function DayBlock({ archive, expanded, onToggle, tierFilter, personalBets, togglePersonal }) {
  const summary = summarizeArchive(archive.data)
  const data = archive.data

  // Build the picks list per filter.
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

  return (
    <div style={{
      background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10,
      marginBottom: 12, overflow: 'hidden',
    }}>
      <button onClick={onToggle} style={{
        width: '100%', padding: '12px 16px', background: 'none', border: 'none',
        color: BRIGHT, cursor: 'pointer', fontFamily: SANS,
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        flexWrap: 'wrap', gap: 8,
      }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'baseline', flexWrap: 'wrap' }}>
          <span style={{ fontSize: 16, fontWeight: 800 }}>{archive.date}</span>
          <span style={{ fontSize: 11, fontFamily: MONO, color: MUTED }}>
            {summary.primary_count}P · {summary.secondary_count}S · {summary.shadow_count}Sh
          </span>
          {summary.isSettled ? (
            <span style={{ fontSize: 11, fontFamily: MONO, color: summary.profit >= 0 ? ACCENT : ACCENT_RED, fontWeight: 700 }}>
              {summary.wins}W-{summary.losses}L
              {summary.voids > 0 && `-${summary.voids}V`}
              {' · '}
              {fmtUnits(summary.profit)} ({summary.roiPct >= 0 ? '+' : ''}{summary.roiPct?.toFixed(1)}% ROI)
            </span>
          ) : (
            <span style={{ fontSize: 11, fontFamily: MONO, color: BLUE,
                            padding: '2px 8px', background: 'rgba(59,130,246,0.10)', borderRadius: 4 }}>
              UNSETTLED
            </span>
          )}
        </div>
        <span style={{ fontSize: 14, color: MUTED, transform: expanded ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>▾</span>
      </button>
      {expanded && (
        <div style={{ padding: '0 8px 16px', overflowX: 'auto', WebkitOverflowScrolling: 'touch' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, minWidth: 900 }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${BORDER}`, color: MUTED }}>
                {['#', 'Batter', 'Pitcher', 'Mdl', 'Mkt', 'Edge', 'EV', 'Odds', 'Result', 'Bet?', 'Share'].map(h => (
                  <th key={h} style={{ padding: '8px 6px', fontSize: 9, fontFamily: MONO, fontWeight: 600, letterSpacing: 0.6, textAlign: 'center' }}>{h}</th>
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

function CalibrationView({ archives, total }) {
  const cal = useMemo(() => buildCalibration(archives), [archives])
  if (cal.total < CALIBRATION_MIN_PICKS) {
    return (
      <div style={{
        background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10,
        padding: 18, marginBottom: 24, color: TEXT, fontSize: 12, lineHeight: 1.6,
      }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 6 }}>Calibration</div>
        Need 100+ settled picks across all tiers for meaningful calibration. Currently <strong style={{ color: BRIGHT }}>{cal.total}</strong>.
        <div style={{ fontSize: 11, color: MUTED, marginTop: 8 }}>
          Settled-pick numbers will fill in over the paper-trade phase. The chart will appear here once we cross the 100-pick threshold.
        </div>
      </div>
    )
  }
  const maxN = Math.max(...cal.buckets.map(b => b.n || 0), 1)
  return (
    <div style={{
      background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10,
      padding: '14px 18px', marginBottom: 24,
    }}>
      <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 6 }}>
        Calibration ({cal.total} settled picks)
      </div>
      <div style={{ fontSize: 11, color: MUTED, marginBottom: 12 }}>
        Predicted vs actual hit rate per model-prob bucket. Drift &gt; 2pp marked red.
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${BORDER}`, color: MUTED }}>
            <th style={{ padding: '6px 8px', textAlign: 'left', fontSize: 10, fontFamily: MONO }}>Bucket</th>
            <th style={{ padding: '6px 8px', textAlign: 'right', fontSize: 10, fontFamily: MONO }}>n</th>
            <th style={{ padding: '6px 8px', textAlign: 'right', fontSize: 10, fontFamily: MONO }}>Expected</th>
            <th style={{ padding: '6px 8px', textAlign: 'right', fontSize: 10, fontFamily: MONO }}>Actual</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', fontSize: 10, fontFamily: MONO, width: '40%' }}>Drift</th>
          </tr>
        </thead>
        <tbody>
          {cal.buckets.map(b => {
            const driftBad = b.drift != null && b.drift > 0.02
            return (
              <tr key={b.lo} style={{ borderBottom: `1px solid #111827` }}>
                <td style={{ padding: '6px 8px', color: BRIGHT, fontFamily: MONO, fontSize: 11 }}>{Math.round(b.lo*100)}-{Math.round(b.hi*100)}%</td>
                <td style={{ padding: '6px 8px', textAlign: 'right', fontFamily: MONO, fontSize: 11, color: TEXT }}>{b.n || '—'}</td>
                <td style={{ padding: '6px 8px', textAlign: 'right', fontFamily: MONO, fontSize: 11, color: TEXT }}>{b.expected != null ? `${(b.expected*100).toFixed(1)}%` : '—'}</td>
                <td style={{ padding: '6px 8px', textAlign: 'right', fontFamily: MONO, fontSize: 11, color: BRIGHT, fontWeight: 600 }}>{b.actual != null ? `${(b.actual*100).toFixed(1)}%` : '—'}</td>
                <td style={{ padding: '6px 8px' }}>
                  {b.n > 0 ? (
                    <div style={{ position: 'relative', height: 14, background: '#0c1422', borderRadius: 3 }}>
                      <div style={{
                        position: 'absolute', left: 0, top: 0, bottom: 0,
                        width: `${(b.n / maxN) * 100}%`,
                        background: driftBad ? 'rgba(239,68,68,0.25)' : 'rgba(34,197,94,0.20)',
                        borderRadius: 3,
                      }} />
                      <span style={{ position: 'absolute', left: 6, top: 0, bottom: 0,
                                      display: 'flex', alignItems: 'center', fontSize: 9,
                                      fontFamily: MONO, color: driftBad ? ACCENT_RED : TEXT }}>
                        {b.drift != null ? `±${(b.drift*100).toFixed(1)}pp` : ''}
                      </span>
                    </div>
                  ) : null}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ─── page component ────────────────────────────────────────────────────
export default function Tracker({ archives, tracker }) {
  const [tierFilter, setTierFilter] = useState('primary')
  const [dateFilter, setDateFilter] = useState('all')
  const [sortBy, setSortBy] = useState('date_desc')
  const [expanded, setExpanded] = useState(() => {
    // Newest day expanded by default.
    const newest = archives[0]?.date
    return newest ? { [newest]: true } : {}
  })
  const [personalBets, setPersonalBets] = useState({})

  // Hydrate localStorage personal bets after mount.
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

  // Top-level metrics from tracker.json (primary tier).
  const sumP = tracker?.summary_primary || tracker?.summary || {}
  const sumS = tracker?.summary_secondary || {}
  const sumShadow = tracker?.summary_shadow || {}
  const totalSettled = (sumP.wins || 0) + (sumP.losses || 0)
  const isFirstWeek = totalSettled === 0

  // Days since deploy: first archive date → today.
  const daysSinceDeploy = (() => {
    if (archives.length === 0) return 0
    const first = new Date(archives[archives.length - 1].date)
    const today = new Date()
    return Math.max(0, Math.floor((today - first) / (1000 * 60 * 60 * 24)) + 1)
  })()

  // Personal-bet performance subset (computed across all archives × picks user toggled).
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

  // Stacked-pick days (any day with at least one pair of correlated primary picks).
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
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
      </Head>
      <div style={{
        minHeight: '100vh', background: BG, color: TEXT,
        fontFamily: SANS, padding: '24px 16px',
        maxWidth: 1100, margin: '0 auto',
      }}>
        {/* Header */}
        <div style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 4 }}>
            <Link href="/" style={{ fontSize: 28, fontWeight: 800, color: BRIGHT, letterSpacing: -1, textDecoration: 'none' }}>HR Picks</Link>
            <span style={{ fontSize: 16, color: MUTED }}>·</span>
            <span style={{ fontSize: 22, fontWeight: 700, color: BRIGHT }}>Tracker</span>
            <span style={{ fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 4,
                            background: 'rgba(168,85,247,0.12)', color: PURPLE, letterSpacing: 0.6, fontFamily: MONO }}>V7</span>
          </div>
          <div style={{ fontSize: 11, color: MUTED, fontFamily: MONO }}>
            {archives.length} archived day{archives.length === 1 ? '' : 's'} · day {daysSinceDeploy} since deploy
          </div>
        </div>

        {/* Empty state — Day 1 */}
        {isFirstWeek && (
          <div style={{
            background: 'rgba(59,130,246,0.06)', border: `1px solid rgba(59,130,246,0.2)`,
            borderRadius: 10, padding: 14, marginBottom: 20,
            fontSize: 13, color: TEXT, lineHeight: 1.5,
          }}>
            <strong style={{ color: BLUE }}>Day {daysSinceDeploy} — no settled picks yet.</strong>{' '}
            Today's picks settle tomorrow at ~10am ET. ROI / hit rate / CLV will populate after the first settlement run.
          </div>
        )}

        {/* Big metrics */}
        <div style={{ display: 'flex', gap: 10, marginBottom: 24, flexWrap: 'wrap' }}>
          <StatCard
            label="Picks Settled"
            value={totalSettled}
            sub={`${sumP.wins || 0}W-${sumP.losses || 0}L · ${sumP.voids || 0} void`}
            color={BLUE}
          />
          <StatCard
            label="Hit Rate"
            value={totalSettled > 0 ? fmtPct(sumP.hit_rate, 1) : '—'}
            sub={totalSettled > 0 ? null : 'awaiting settlement'}
            color={totalSettled > 0 ? BRIGHT : MUTED}
          />
          <StatCard
            label="ROI"
            value={totalSettled > 0 ? `${sumP.roi_pct >= 0 ? '+' : ''}${sumP.roi_pct?.toFixed(1)}%` : '—'}
            sub={totalSettled > 0 ? `${fmtUnits(sumP.units_profit)} on ${sumP.units_staked || 0}u` : null}
            color={totalSettled > 0 ? (sumP.roi_pct >= 0 ? ACCENT : ACCENT_RED) : MUTED}
          />
          <StatCard
            label="Avg CLV"
            value={sumP.avg_clv_pct != null ? `${sumP.avg_clv_pct >= 0 ? '+' : ''}${sumP.avg_clv_pct.toFixed(1)}%` : '—'}
            sub={sumP.n_picks_with_clv ? `${sumP.n_picks_with_clv} picks` : 'awaiting closing snaps'}
            color={sumP.avg_clv_pct != null ? (sumP.avg_clv_pct >= 0 ? ACCENT : ACCENT_RED) : MUTED}
          />
        </div>

        {/* Personal-bet card */}
        {personalPerf.bets > 0 && (
          <div style={{ display: 'flex', gap: 10, marginBottom: 24, flexWrap: 'wrap' }}>
            <StatCard
              label="Your Bets"
              value={personalPerf.bets}
              sub={`${personalPerf.wins}W-${personalPerf.losses}L`}
              color={YELLOW}
            />
            <StatCard
              label="Your ROI"
              value={personalPerf.roiPct != null ? `${personalPerf.roiPct >= 0 ? '+' : ''}${personalPerf.roiPct.toFixed(1)}%` : '—'}
              sub={fmtUnits(personalPerf.profit)}
              color={personalPerf.roiPct >= 0 ? ACCENT : ACCENT_RED}
            />
            <StatCard
              label="Stacked Days"
              value={`${(stackingMetric.pct * 100).toFixed(0)}%`}
              sub={`${stackingMetric.stackedDays}/${stackingMetric.totalDays} days`}
              color={MUTED}
            />
          </div>
        )}

        {/* Filters */}
        <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ fontSize: 10, fontFamily: MONO, color: MUTED, textTransform: 'uppercase', letterSpacing: 1 }}>Tier</span>
          {[
            ['primary', 'Primary only'],
            ['all', 'All tiers'],
          ].map(([k, label]) => (
            <button key={k} onClick={() => setTierFilter(k)} style={{
              background: tierFilter === k ? 'rgba(34,197,94,0.15)' : 'transparent',
              border: `1px solid ${tierFilter === k ? ACCENT : BORDER}`,
              borderRadius: 4, color: tierFilter === k ? ACCENT : TEXT,
              fontSize: 11, fontFamily: MONO, padding: '4px 10px', cursor: 'pointer',
            }}>{label}</button>
          ))}
          <span style={{ width: 12 }} />
          <span style={{ fontSize: 10, fontFamily: MONO, color: MUTED, textTransform: 'uppercase', letterSpacing: 1 }}>Range</span>
          {[
            ['all', 'All time'],
            ['30d', '30d'],
            ['7d', '7d'],
          ].map(([k, label]) => (
            <button key={k} onClick={() => setDateFilter(k)} style={{
              background: dateFilter === k ? 'rgba(59,130,246,0.15)' : 'transparent',
              border: `1px solid ${dateFilter === k ? BLUE : BORDER}`,
              borderRadius: 4, color: dateFilter === k ? BLUE : TEXT,
              fontSize: 11, fontFamily: MONO, padding: '4px 10px', cursor: 'pointer',
            }}>{label}</button>
          ))}
          <span style={{ width: 12 }} />
          <span style={{ fontSize: 10, fontFamily: MONO, color: MUTED, textTransform: 'uppercase', letterSpacing: 1 }}>Sort</span>
          {[
            ['date_desc', 'Date (newest)'],
            ['date_asc', 'Date (oldest)'],
            ['roi', 'ROI'],
            ['hit_rate', 'Hit rate'],
            ['count', 'Pick count'],
          ].map(([k, label]) => (
            <button key={k} onClick={() => setSortBy(k)} style={{
              background: sortBy === k ? 'rgba(168,85,247,0.15)' : 'transparent',
              border: `1px solid ${sortBy === k ? PURPLE : BORDER}`,
              borderRadius: 4, color: sortBy === k ? PURPLE : TEXT,
              fontSize: 11, fontFamily: MONO, padding: '4px 10px', cursor: 'pointer',
            }}>{label}</button>
          ))}
        </div>

        {/* Day blocks */}
        {filteredArchives.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: MUTED, fontSize: 13,
                          background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10 }}>
            No archived days yet. The first cron run will populate this list tomorrow at 11am ET.
          </div>
        ) : (
          filteredArchives.map(a => (
            <DayBlock key={a.date} archive={a}
                      expanded={!!expanded[a.date]}
                      onToggle={() => setExpanded(p => ({ ...p, [a.date]: !p[a.date] }))}
                      tierFilter={tierFilter}
                      personalBets={personalBets}
                      togglePersonal={togglePersonal} />
          ))
        )}

        {/* Calibration view — placed after day blocks so high-frequency info is up top. */}
        <div style={{ marginTop: 24 }}>
          <CalibrationView archives={archives} />
        </div>

        {/* Footer */}
        <div style={{
          marginTop: 28, padding: '14px 0', borderTop: `1px solid ${BORDER}`,
          fontSize: 10, color: MUTED, lineHeight: 1.7, fontFamily: MONO,
        }}>
          Calibration computed across all settled tiers (primary + secondary + shadow). Personal-bet log stored in your browser only.<br />
          Stacked picks (⛓) share a starting pitcher with another primary pick today — outcomes correlated.
        </div>
      </div>
    </>
  )
}
