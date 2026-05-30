import Head from 'next/head'
import Link from 'next/link'
import { useState, useMemo, useEffect, useCallback } from 'react'
import fs from 'fs'
import path from 'path'
import { betKey, toggleBet, countBetsForDay, countBets, loadBets, saveBets } from '../lib/bets'
import { fmtRefreshed } from '../lib/format'

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

// Model-rebuild boundary. Picks dated >= this are produced by the post-rebuild
// model (v7-baseline-0.2.0); picks dated < this are pre-rebuild (0.1.0) and
// shouldn't be aggregated with post-rebuild numbers because the two models
// over-predict at different rates. See the README of 2026-05-13.
const MODEL_REBUILD_DATE = '2026-05-13'

// Triple-filter ship date. The production triple filter (stacked-EV shade +
// EV ceiling + pitcher-factor band) went live 2026-05-20 and every pick since
// carries filter_status. The "Since triple" range scopes the view to this date
// onward — the live experiment window (pre-registered eval 2026-06-18). Combine
// with View = Triple to see exactly what's been shown/bet since the filter went on.
const TRIPLE_FILTER_DATE = '2026-05-20'

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
  } catch { /* directory not present yet */ }

  let tracker = null
  try {
    tracker = JSON.parse(fs.readFileSync(
      path.join(process.cwd(), 'backend/data/processed/tracker.json'),
      'utf8'
    ))
  } catch { /* no tracker yet */ }

  // Build timestamp — the page is statically generated, so "now" at build
  // time is when the data was last refreshed (each daily cron / Vercel deploy
  // regenerates it). Surfaced in the header as "Last refreshed".
  const generatedAt = new Date().toISOString()

  return { props: { archives, tracker, generatedAt } }
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
// 1u = $1 flat-stake assumption surfaced everywhere on this page.
function fmtDollars(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—'
  const sign = v > 0 ? '+' : v < 0 ? '−' : ''
  return `${sign}$${Math.abs(v).toFixed(digits)}`
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

// JS port of backend/src/pipeline/filters.py — backfills filter_status on
// archived picks that pre-date 2026-05-20. Must mirror the Python filter
// logic exactly or the per-day client-side compute drifts from tracker.json.
const STACKED_SHADE_FACTOR = 0.7
const EV_CEILING_PCT = 50.0
const PITCHER_FACTOR_BAND = [1.10, 1.45]
const MODEL_PROB_BAND = [0.15, 0.25]
const TIER_EV_MIN = { primary: 25.0, secondary: 25.0, shadow: 10.0 }

function pitcherFactor(pick) {
  const feats = pick.top_3_features || []
  for (const f of feats) if (f.name === 'pitcher') return Number(f.value) || 1.0
  return 1.0
}
function picksTriple(pick) {
  if (Number(pick.ev_pct || 0) >= EV_CEILING_PCT) return false
  const pf = pitcherFactor(pick)
  if (pf >= PITCHER_FACTOR_BAND[0] && pf < PITCHER_FACTOR_BAND[1]) return false
  if (pick.stacked) {
    const shaded = Number(pick.ev_pct) * STACKED_SHADE_FACTOR
    const tierMin = TIER_EV_MIN[pick.tier || 'primary'] ?? 25.0
    if (shaded < tierMin) return false
  }
  return true
}
function picksQuad(pick) {
  if (!picksTriple(pick)) return false
  const mp = Number(pick.model_prob || 0)
  if (mp >= MODEL_PROB_BAND[0] && mp < MODEL_PROB_BAND[1]) return false
  return true
}
function pickPassesFilter(pick, filterView, tier) {
  if (filterView === 'baseline') return true
  // Older archives may lack filter_status; compute from the pick fields directly.
  const fs = pick.filter_status
  const p = { ...pick, tier: pick.tier || tier }
  if (filterView === 'triple') return fs?.passes_triple ?? picksTriple(p)
  if (filterView === 'quad')   return fs?.passes_quad   ?? picksQuad(p)
  return true
}

// Combine tracker.json per-tier summaries when "All tiers" is selected.
function combineTrackerSummaries(...sums) {
  const valid = sums.filter(Boolean)
  if (valid.length === 0) return {}
  const wins = valid.reduce((a, s) => a + (s.wins || 0), 0)
  const losses = valid.reduce((a, s) => a + (s.losses || 0), 0)
  const voids = valid.reduce((a, s) => a + (s.voids || 0), 0)
  const total_picks = valid.reduce((a, s) => a + (s.total_picks || 0), 0)
  const units_staked = valid.reduce((a, s) => a + (s.units_staked || 0), 0)
  const units_profit = valid.reduce((a, s) => a + (s.units_profit || 0), 0)
  const settled = wins + losses
  const hit_rate = settled > 0 ? wins / settled : null
  const roi_pct = units_staked > 0 ? (units_profit / units_staked) * 100 : null
  const totalClvPicks = valid.reduce((a, s) => a + (s.n_picks_with_clv || 0), 0)
  const avg_clv_pct = totalClvPicks > 0
    ? valid.reduce((a, s) => a + ((s.avg_clv_pct || 0) * (s.n_picks_with_clv || 0)), 0) / totalClvPicks
    : null
  return {
    wins, losses, voids, total_picks, units_staked, units_profit,
    hit_rate, roi_pct, avg_clv_pct, n_picks_with_clv: totalClvPicks,
  }
}

// Sum settlement summaries across a list of archives, for the given tier(s).
// Mirrors backend tracker.py for the basic stats we surface in the top panels;
// CLV requires closing-line snapshots that aren't carried per-archive, so it's
// omitted here and the StatCard renders "—" when this path is used.
function computeSummaryFromArchives(archives, tier, filterView = 'baseline') {
  const tiers = tier === 'all' ? ['primary', 'secondary', 'shadow'] : [tier]
  let wins = 0, losses = 0, voids = 0, units_profit = 0
  // Baseline path uses the pre-computed per-day summaries (faster). Filter
  // views (triple/quad) iterate the raw picks + results so we can scope by
  // filter_status — that information isn't in the daily summary objects.
  if (filterView === 'baseline') {
    for (const { data } of archives) {
      const settle = data.settlement
      if (!settle) continue
      for (const t of tiers) {
        const s = settle[`${t}_summary`]
        if (!s) continue
        wins += s.n_wins || 0
        losses += s.n_losses || 0
        voids += s.n_voids || 0
        units_profit += s.units_profit || 0
      }
    }
  } else {
    for (const { data } of archives) {
      const settle = data.settlement
      if (!settle) continue
      for (const t of tiers) {
        const picksList = data[`${t}_picks`] || []
        const results = settle[`${t}_results`] || []
        const pickIdx = new Map()
        for (const p of picksList) pickIdx.set(`${p.batter_id}|${p.game_pk || ''}`, p)
        for (const r of results) {
          const p = pickIdx.get(`${r.batter_id}|${r.game_pk || ''}`)
          if (!p) continue
          if (!pickPassesFilter(p, filterView, t)) continue
          if (r.outcome === 'W') wins++
          else if (r.outcome === 'L') losses++
          else voids++
          units_profit += r.profit_units || 0
        }
      }
    }
  }
  const settled = wins + losses
  return {
    wins, losses, voids,
    total_picks: settled + voids,
    units_staked: settled,
    units_profit,
    hit_rate: settled > 0 ? wins / settled : null,
    roi_pct: settled > 0 ? (units_profit / settled) * 100 : null,
    avg_clv_pct: null,            // CLV not reconstructible from archives alone
    n_picks_with_clv: 0,
  }
}

// Combine per-archive settlement summaries (for day-block headers when
// tierFilter === 'all'). Each settled pick is 1u stake; voids don't stake.
function combineSettlementSummaries(...sums) {
  const valid = sums.filter(Boolean)
  if (valid.length === 0) return null
  const n_wins = valid.reduce((a, s) => a + (s.n_wins || 0), 0)
  const n_losses = valid.reduce((a, s) => a + (s.n_losses || 0), 0)
  const n_voids = valid.reduce((a, s) => a + (s.n_voids || 0), 0)
  const units_profit = valid.reduce((a, s) => a + (s.units_profit || 0), 0)
  const units_staked = n_wins + n_losses
  const roi_pct = units_staked > 0 ? (units_profit / units_staked) * 100 : null
  return { n_wins, n_losses, n_voids, units_profit, roi_pct }
}

function summarizeArchive(archive, tier = 'primary', filterView = 'baseline') {
  const funnel = archive.funnel || {}
  const settle = archive.settlement || null
  const tiers = tier === 'all' ? ['primary', 'secondary', 'shadow'] : [tier]

  // Baseline path uses pre-computed per-day summaries (cheaper).
  if (filterView === 'baseline') {
    let summary = null
    if (settle) {
      summary = tier === 'all'
        ? combineSettlementSummaries(
            settle.primary_summary, settle.secondary_summary, settle.shadow_summary
          )
        : (settle[`${tier}_summary`] || null)
    }
    const wins = summary?.n_wins ?? null
    const losses = summary?.n_losses ?? null
    const voids = summary?.n_voids ?? null
    const settled = (wins ?? 0) + (losses ?? 0)
    const hitRate = settled > 0 ? wins / settled : null
    const profit = summary?.units_profit ?? null
    const roiPct = summary?.roi_pct ?? null
    return {
      primary_count: funnel.primary_count ?? archive.primary_picks?.length ?? 0,
      secondary_count: funnel.secondary_count ?? archive.secondary_picks?.length ?? 0,
      shadow_count: funnel.shadow_count ?? archive.shadow_picks?.length ?? 0,
      wins, losses, voids, hitRate, profit, roiPct,
      isSettled: settle != null,
    }
  }

  // Filter view: compute counts + settlement from raw picks scoped by filter.
  const counts = { primary: 0, secondary: 0, shadow: 0 }
  let wins = 0, losses = 0, voids = 0, profit = 0
  for (const t of tiers) {
    const picksList = archive[`${t}_picks`] || []
    const filteredPicks = picksList.filter(p => pickPassesFilter(p, filterView, t))
    counts[t] = filteredPicks.length
    if (!settle) continue
    const results = settle[`${t}_results`] || []
    const keys = new Set(filteredPicks.map(p => `${p.batter_id}|${p.game_pk || ''}`))
    for (const r of results) {
      const k = `${r.batter_id}|${r.game_pk || ''}`
      if (!keys.has(k)) continue
      if (r.outcome === 'W') wins++
      else if (r.outcome === 'L') losses++
      else voids++
      profit += r.profit_units || 0
    }
  }
  const settled = wins + losses
  return {
    primary_count: counts.primary,
    secondary_count: counts.secondary,
    shadow_count: counts.shadow,
    wins, losses, voids,
    hitRate: settled > 0 ? wins / settled : null,
    profit: settle ? profit : null,
    roiPct: settled > 0 ? (profit / settled) * 100 : null,
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
function buildCalibration(archives, tier = 'all', filterView = 'baseline') {
  const tiers = tier === 'primary' ? ['primary']
              : tier === 'secondary' ? ['secondary']
              : tier === 'shadow' ? ['shadow']
              : ['primary', 'secondary', 'shadow']
  const allSettled = []
  for (const { data } of archives) {
    const settle = data.settlement
    if (!settle) continue
    for (const t of tiers) {
      const picksList = data[`${t}_picks`] || []
      const pickIdx = new Map()
      for (const p of picksList) pickIdx.set(`${p.batter_id}|${p.game_pk || ''}`, p)
      for (const r of settle[`${t}_results`] || []) {
        if (r.outcome === 'VOID') continue
        const p = pickIdx.get(`${r.batter_id}|${r.game_pk || ''}`)
        if (filterView !== 'baseline' && !(p && pickPassesFilter(p, filterView, t))) continue
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

function PickRow({ pick, settledPick, betted, onToggleBet }) {
  const bp = pick.best_book === 'draftkings' ? pick.dk_odds : pick.fd_odds
  const otherLabel = pick.best_book === 'draftkings' ? 'FD' : 'DK'
  const otherPrice = pick.best_book === 'draftkings' ? pick.fd_odds : pick.dk_odds
  const settledOutcome = settledPick?.outcome

  const evPositive = pick.ev_pct >= 0
  const edgePositive = pick.edge_pct >= 0

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
  if (pick.pitcher_factor_shrunk) metaParts.push('shrunk')

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

  const onBet = (e) => {
    e.stopPropagation()
    onToggleBet()
  }

  return (
    <tr style={{
      borderBottom: `1px solid ${T.border}`,
      background: betted ? '#f0fdf4' : 'transparent',
    }}>
      <td style={{ padding: '14px 8px', textAlign: 'center', verticalAlign: 'top' }}>
        <button
          onClick={onBet}
          aria-pressed={betted}
          title={betted ? 'You bet on this player — click to clear' : 'Mark that you bet on this player'}
          style={{
            width: 26, height: 26, borderRadius: 6, cursor: 'pointer',
            border: `1.5px solid ${betted ? T.positive : T.borderStrong}`,
            background: betted ? T.positive : 'transparent',
            color: '#ffffff', fontSize: 15, lineHeight: 1, fontFamily: 'inherit',
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            padding: 0, transition: 'background 0.1s, border-color 0.1s',
          }}
        >{betted ? '✓' : ''}</button>
      </td>
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
        <button onClick={onShare} style={{
          background: 'transparent', border: `1px solid ${T.border}`, borderRadius: 4,
          color: T.textMedium, fontSize: 11, padding: '4px 10px', cursor: 'pointer',
          fontFamily: 'inherit',
        }} title="Copy pick details to clipboard">share</button>
      </td>
    </tr>
  )
}

function DayBlock({ archive, expanded, onToggle, tierFilter, filterView = 'baseline', bets, onToggleBet }) {
  const summary = summarizeArchive(archive.data, tierFilter, filterView)
  const data = archive.data
  const date = archive.date
  const dayBetCount = countBetsForDay(bets, date)

  const picks = useMemo(() => {
    const all = []
    const pull = (t, list) => {
      for (const p of list || []) {
        if (pickPassesFilter(p, filterView, t)) all.push({ ...p, _tier: t })
      }
    }
    if (tierFilter === 'all' || tierFilter === 'primary')   pull('primary',   data.primary_picks)
    if (tierFilter === 'all' || tierFilter === 'secondary') pull('secondary', data.secondary_picks)
    if (tierFilter === 'all' || tierFilter === 'shadow')    pull('shadow',    data.shadow_picks)
    return all
  }, [data, tierFilter, filterView])

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
          {dayBetCount > 0 && (
            <span style={{
              fontSize: 11, color: T.positive, fontWeight: 600,
              border: `1px solid ${T.positive}`, borderRadius: 4,
              padding: '2px 8px', ...TABULAR,
            }}>✓ {dayBetCount} bet</span>
          )}
        </div>
        <span style={{
          fontSize: 12, color: T.textLight,
          transform: expanded ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s',
        }}>▾</span>
      </button>
      {expanded && (
        <div style={{ padding: '0 0 24px', overflowX: 'auto', WebkitOverflowScrolling: 'touch' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, minWidth: 880 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${T.borderStrong}` }}>
                {[
                  ['Bet', 'center'], ['#', 'right'], ['Batter', 'left'], ['Pitcher', 'left'],
                  ['Model', 'right'], ['Market', 'right'], ['Edge', 'right'],
                  ['EV', 'right'], ['Odds', 'right'],
                  ['Result', 'left'], ['Share', 'center'],
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
                const bKey = betKey(date, p.batter_id, p.game_pk)
                return (
                  <PickRow
                    key={k}
                    pick={p}
                    settledPick={settledByKey[k]}
                    betted={!!bets[bKey]}
                    onToggleBet={() => onToggleBet(bKey)}
                  />
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function CalibrationView({ archives, tierFilter, filterView = 'baseline' }) {
  const cal = useMemo(
    () => buildCalibration(archives, tierFilter, filterView),
    [archives, tierFilter, filterView],
  )
  const tierLabel = tierFilter === 'all' ? 'all tiers' : tierFilter
  if (cal.total < CALIBRATION_MIN_PICKS) {
    return (
      <div style={{
        border: `1px solid ${T.border}`, borderRadius: 6,
        padding: '24px 26px', color: T.textMedium, fontSize: 13, lineHeight: 1.6,
      }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: T.text, marginBottom: 10 }}>Calibration</div>
        Need {CALIBRATION_MIN_PICKS}+ settled picks ({tierLabel}) for meaningful
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
        {cal.total} settled picks ({tierLabel}) · predicted vs actual hit rate per model-prob bucket
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

// Filter button — text-only. Active state: dark + bold + thick underline.
// Inactive: light gray (high contrast against active so the click is unmistakable).
function FilterButton({ active, onClick, children }) {
  return (
    <button onClick={onClick} style={{
      background: 'transparent', border: 'none', padding: '4px 0',
      color: active ? T.text : T.textLight,
      fontWeight: active ? 700 : 400,
      fontSize: 13, cursor: 'pointer', fontFamily: 'inherit',
      textDecoration: active ? 'underline' : 'none',
      textUnderlineOffset: 6, textDecorationThickness: 2,
      transition: 'color 0.1s',
    }}>{children}</button>
  )
}
function FilterRow({ label, options, value, onChange }) {
  return (
    <div style={{ display: 'flex', gap: 18, alignItems: 'center', flexWrap: 'wrap' }}>
      <span style={{
        fontSize: 11, color: T.textLight, minWidth: 50,
        textTransform: 'uppercase', letterSpacing: 0.6,
      }}>{label}</span>
      {options.map(([k, lbl]) => (
        <FilterButton key={k} active={value === k} onClick={() => onChange(k)}>{lbl}</FilterButton>
      ))}
    </div>
  )
}

// ─── page component ────────────────────────────────────────────────────
export default function Tracker({ archives, tracker, generatedAt }) {
  const refreshed = fmtRefreshed(generatedAt)
  const [tierFilter, setTierFilter] = useState('primary')
  const [dateFilter, setDateFilter] = useState('all')
  // View filter scopes the top metrics by post-build empirical filter.
  // baseline = every settled pick (back-compat).
  // triple   = only picks that pass the production filter (passes_triple).
  // quad     = only picks that pass the experimental quad filter.
  // See backend/docs/filter_experiment.md.
  const [filterView, setFilterView] = useState('baseline')
  // Default to "post-rebuild" if any post-rebuild archive exists (current
  // model is the relevant one to show); otherwise default to "all" so the
  // page isn't empty when only pre-rebuild data exists yet.
  const [modelFilter, setModelFilter] = useState(() => {
    return archives.some(a => a.date >= MODEL_REBUILD_DATE) ? 'post' : 'all'
  })
  const [sortBy, setSortBy] = useState('date_desc')
  const [expanded, setExpanded] = useState(() => {
    const newest = archives[0]?.date
    return newest ? { [newest]: true } : {}
  })

  // "Did I bet on this player?" flags. This is a statically-built page with no
  // per-user backend, so the set lives in the browser's localStorage. Start
  // empty so the server-rendered HTML and first client render match (no
  // hydration mismatch); the saved flags load in an effect right after mount.
  const [bets, setBets] = useState({})
  const [betsLoaded, setBetsLoaded] = useState(false)
  useEffect(() => {
    setBets(loadBets(window.localStorage))
    setBetsLoaded(true)
  }, [])
  // Persist on every change — but only after the initial load, so we never
  // clobber stored flags with the empty starting state.
  useEffect(() => {
    if (!betsLoaded) return
    saveBets(window.localStorage, bets)
  }, [bets, betsLoaded])
  const handleToggleBet = useCallback((key) => {
    setBets(prev => toggleBet(prev, key))
  }, [])
  const totalBets = countBets(bets)

  // Two related conditions:
  //   hasPreRebuild — any pre-rebuild archive is in the dataset; drives the
  //     banner so users see the context whenever old-model data could be on
  //     the page.
  //   spansRebuild — the dataset has BOTH pre and post; drives the Model
  //     filter row (there's nothing to filter between if only one side exists).
  const { hasPreRebuild, spansRebuild } = useMemo(() => {
    const hasPre = archives.some(a => a.date < MODEL_REBUILD_DATE)
    const hasPost = archives.some(a => a.date >= MODEL_REBUILD_DATE)
    return { hasPreRebuild: hasPre, spansRebuild: hasPre && hasPost }
  }, [archives])

  // Filter + sort the archive list.
  const filteredArchives = useMemo(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    let list = archives
    // Model filter first — it's the most semantic cut.
    if (modelFilter === 'post') {
      list = list.filter(a => a.date >= MODEL_REBUILD_DATE)
    } else if (modelFilter === 'pre') {
      list = list.filter(a => a.date < MODEL_REBUILD_DATE)
    }
    if (dateFilter === 'yesterday') {
      const yest = new Date(today); yest.setDate(yest.getDate() - 1)
      const y = yest.getFullYear()
      const m = String(yest.getMonth() + 1).padStart(2, '0')
      const d = String(yest.getDate()).padStart(2, '0')
      const yestStr = `${y}-${m}-${d}`
      list = list.filter(a => a.date === yestStr)
    } else if (dateFilter === '7d') {
      const cutoff = new Date(today); cutoff.setDate(cutoff.getDate() - 7)
      list = list.filter(a => new Date(a.date) >= cutoff)
    } else if (dateFilter === '30d') {
      const cutoff = new Date(today); cutoff.setDate(cutoff.getDate() - 30)
      list = list.filter(a => new Date(a.date) >= cutoff)
    } else if (dateFilter === 'since_triple') {
      list = list.filter(a => a.date >= TRIPLE_FILTER_DATE)
    }
    const sorted = [...list]
    if (sortBy === 'date_desc') sorted.sort((a, b) => b.date.localeCompare(a.date))
    else if (sortBy === 'date_asc') sorted.sort((a, b) => a.date.localeCompare(b.date))
    else if (sortBy === 'roi') sorted.sort((a, b) => (summarizeArchive(b.data, tierFilter, filterView).roiPct ?? -Infinity) - (summarizeArchive(a.data, tierFilter, filterView).roiPct ?? -Infinity))
    else if (sortBy === 'hit_rate') sorted.sort((a, b) => (summarizeArchive(b.data, tierFilter, filterView).hitRate ?? -Infinity) - (summarizeArchive(a.data, tierFilter, filterView).hitRate ?? -Infinity))
    else if (sortBy === 'count') {
      sorted.sort((a, b) => {
        const sa = summarizeArchive(a.data, tierFilter, filterView)
        const sb = summarizeArchive(b.data, tierFilter, filterView)
        const ca = tierFilter === 'all'
          ? sa.primary_count + sa.secondary_count + sa.shadow_count
          : sa.primary_count
        const cb = tierFilter === 'all'
          ? sb.primary_count + sb.secondary_count + sb.shadow_count
          : sb.primary_count
        return cb - ca
      })
    }
    return sorted
  }, [archives, dateFilter, sortBy, tierFilter, modelFilter, filterView])

  // Top-level metrics — driven by tier, model, AND date filters. When any
  // filter is non-default, compute client-side from the filtered archives so
  // the panels match what the day blocks show. Only the all-time / all-model
  // path uses tracker.json (faster, and the only path that carries CLV since
  // closing snaps aren't reconstructible from per-day archives).
  const sum = useMemo(() => {
    if (modelFilter !== 'all' || dateFilter !== 'all') {
      return computeSummaryFromArchives(filteredArchives, tierFilter, filterView)
    }
    if (!tracker) return {}
    // Filter views read from the by_filter block. Falls back to baseline
    // (with a 0-count summary) if a pre-2026-05-20 tracker.json is loaded.
    const source = filterView === 'baseline'
      ? tracker
      : (tracker.by_filter?.[filterView] || tracker)
    if (tierFilter === 'all') {
      return combineTrackerSummaries(
        source.summary_primary, source.summary_secondary, source.summary_shadow
      )
    }
    return source[`summary_${tierFilter}`] || tracker.summary || {}
  }, [tracker, tierFilter, modelFilter, dateFilter, filterView, filteredArchives])

  const totalSettled = (sum.wins || 0) + (sum.losses || 0)
  const isFirstWeek = totalSettled === 0

  const daysSinceDeploy = (() => {
    if (archives.length === 0) return 0
    const first = new Date(archives[archives.length - 1].date)
    const today = new Date()
    return Math.max(0, Math.floor((today - first) / (1000 * 60 * 60 * 24)) + 1)
  })()

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
        {/* Header */}
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
            {refreshed && <> · last refreshed {refreshed}</>}
            {totalBets > 0 && (
              <span style={{ color: T.positive, fontWeight: 600 }}> · ✓ {totalBets} bet{totalBets === 1 ? '' : 's'} flagged</span>
            )}
          </div>
        </div>

        {/* Day-1 banner */}
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

        {/* Model-rebuild banner — shows whenever pre-rebuild data is in the
            dataset (so users get context while the old picks are still visible).
            Hides once pre-rebuild data ages out. */}
        {hasPreRebuild && (
          <div style={{
            border: `1px solid ${T.border}`, borderRadius: 6,
            padding: '16px 20px', marginBottom: 28,
            fontSize: 13, color: T.textMedium, lineHeight: 1.6,
            background: T.bgSubtle,
          }}>
            <strong style={{ color: T.text, fontWeight: 600 }}>Model rebuilt {MODEL_REBUILD_DATE}.</strong>{' '}
            Four calibration fixes shipped on this date: Bayesian-blend rewrite
            (de-overlap season/recent windows + per-player prior anchor),{' '}
            <code style={{ background: T.bg, padding: '0 4px', borderRadius: 3 }}>p_per_pa</code> ceiling
            tightened from 0.25 to 0.10,{' '}
            <code style={{ background: T.bg, padding: '0 4px', borderRadius: 3 }}>pitcher_factor</code> capped
            at 1.6 post-shrinkage, breakout-score weights rescaled ÷5 to unsaturate the cap. Pre-rebuild
            picks reflect a model that systematically over-predicted at the top of the distribution
            {spansRebuild ? (
              <> and shouldn't be aggregated with post-rebuild numbers — use the{' '}
              <strong style={{ color: T.text }}>Model</strong> filter below to scope the view.</>
            ) : (
              <>. Tomorrow's settled picks will be the first under the new model.</>
            )}
          </div>
        )}

        {/* Big metrics — driven by tier toggle, assumes 1u flat stake on every pick. */}
        <div style={{ display: 'flex', gap: 14, marginBottom: 28, flexWrap: 'wrap' }}>
          <StatCard
            label="Picks settled"
            value={totalSettled}
            sub={`${sum.wins || 0}W–${sum.losses || 0}L · ${sum.voids || 0} void`}
            tone={totalSettled > 0 ? 'default' : 'muted'}
          />
          <StatCard
            label="Hit rate"
            value={totalSettled > 0 ? fmtPct(sum.hit_rate, 1) : '—'}
            sub={totalSettled > 0 ? null : 'awaiting settlement'}
            tone={totalSettled > 0 ? 'default' : 'muted'}
          />
          <StatCard
            label="ROI"
            value={totalSettled > 0 ? `${sum.roi_pct >= 0 ? '+' : ''}${sum.roi_pct?.toFixed(1)}%` : '—'}
            sub={totalSettled > 0 ? `${fmtUnits(sum.units_profit)} on ${sum.units_staked || 0}u` : null}
            tone={totalSettled > 0 ? (sum.roi_pct >= 0 ? 'positive' : 'negative') : 'muted'}
          />
          <StatCard
            label="Net @ $1/bet"
            value={totalSettled > 0 ? fmtDollars(sum.units_profit) : '—'}
            sub={totalSettled > 0 ? `on ${sum.units_staked || 0} bets` : null}
            tone={totalSettled > 0 ? (sum.units_profit >= 0 ? 'positive' : 'negative') : 'muted'}
          />
          <StatCard
            label="Avg CLV"
            value={sum.avg_clv_pct != null ? `${sum.avg_clv_pct >= 0 ? '+' : ''}${sum.avg_clv_pct.toFixed(1)}%` : '—'}
            sub={sum.n_picks_with_clv ? `${sum.n_picks_with_clv} picks` : 'awaiting closing snaps'}
            tone={sum.avg_clv_pct != null ? (sum.avg_clv_pct >= 0 ? 'positive' : 'negative') : 'muted'}
          />
        </div>

        {/* Filters */}
        <div style={{
          padding: '20px 0', marginBottom: 8,
          borderTop: `1px solid ${T.border}`, borderBottom: `1px solid ${T.border}`,
          display: 'flex', flexDirection: 'column', gap: 14,
        }}>
          <FilterRow label="Tier"  value={tierFilter} onChange={setTierFilter}
            options={[
              ['primary',   'Primary'],
              ['secondary', 'Secondary'],
              ['shadow',    'Shadow'],
              ['all',       'All'],
            ]} />
          <FilterRow label="View"  value={filterView} onChange={setFilterView}
            options={[
              ['baseline', 'Baseline'],
              ['triple',   'Triple'],
              ['quad',     'Quad'],
            ]} />
          {spansRebuild && (
            <FilterRow label="Model" value={modelFilter} onChange={setModelFilter}
              options={[
                ['post', 'Post-rebuild'],
                ['pre',  'Pre-rebuild'],
                ['all',  'All'],
              ]} />
          )}
          <FilterRow label="Range" value={dateFilter} onChange={setDateFilter}
            options={[
              ['all',          'All time'],
              ['since_triple', 'Since triple (5/20)'],
              ['30d',          '30 days'],
              ['7d',           '7 days'],
              ['yesterday',    'Yesterday'],
            ]} />
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
            No archived days match the current filter.
          </div>
        ) : (
          <div style={{ marginTop: 8, marginBottom: 36 }}>
            {filteredArchives.map(a => (
              <DayBlock key={a.date} archive={a}
                        expanded={!!expanded[a.date]}
                        onToggle={() => setExpanded(p => ({ ...p, [a.date]: !p[a.date] }))}
                        tierFilter={tierFilter}
                        filterView={filterView}
                        bets={bets}
                        onToggleBet={handleToggleBet} />
            ))}
            <div style={{ borderTop: `1px solid ${T.border}` }} />
          </div>
        )}

        {/* Calibration view — scoped to whatever filtered archive set is active
            (so picking "Post-rebuild" shows only post-rebuild calibration, not
            the mixed-model calibration that would otherwise dominate the bins). */}
        <div style={{ marginTop: 36 }}>
          <CalibrationView archives={filteredArchives} tierFilter={tierFilter} filterView={filterView} />
        </div>

        {/* Footer */}
        <div style={{
          marginTop: 40, paddingTop: 20, borderTop: `1px solid ${T.border}`,
          fontSize: 11, color: T.textLight, lineHeight: 1.7,
        }}>
          All metrics assume a flat 1u stake on every pick of the selected tier.
          "stacked" picks share a starting pitcher with another primary pick that
          day — outcomes correlated. View filter scopes the top metrics:{' '}
          <strong style={{ color: T.textMedium }}>Baseline</strong> includes every settled pick,{' '}
          <strong style={{ color: T.textMedium }}>Triple</strong> is the production filter
          (stacked-EV shade + EV ceiling + pitcher-factor band),{' '}
          <strong style={{ color: T.textMedium }}>Quad</strong> adds a model-prob band drop.
          {' '}The <strong style={{ color: T.textMedium }}>Bet</strong> checkbox marks which
          players you actually bet on; flags are saved in this browser (localStorage) and
          persist across reloads — they're personal and not part of the model's record.
        </div>
      </div>
    </>
  )
}
