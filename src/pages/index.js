import Head from 'next/head'
import { useState } from 'react'
import picksData from '../data/picks.json'

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

function formatOdds(odds) {
  if (odds == null) return '---'
  return odds > 0 ? `+${odds}` : `${odds}`
}

function formatGameTime(iso) {
  if (!iso) return ''
  try {
    const d = new Date(iso)
    return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
  } catch { return '' }
}

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

// One small chip per top-3 feature contribution.
function FeatureChip({ name, value }) {
  // breakout_signal is encoded as 1 + bump/blended → display as +X% lift.
  // Multiplicative factors → display as +/-X% deviation from neutral 1.0.
  const pct = ((value - 1) * 100).toFixed(0)
  const sign = pct > 0 ? '+' : ''
  const color = pct > 0 ? ACCENT : (pct < 0 ? ACCENT_RED : MUTED)
  const isBreakout = name === 'breakout_signal'
  return (
    <span style={{
      display: 'inline-block',
      fontSize: 9, fontFamily: MONO, fontWeight: 600,
      padding: '2px 6px', borderRadius: 4,
      background: `${color}1a`, color,
      border: isBreakout ? `1px solid ${PURPLE}55` : `1px solid ${color}33`,
      marginRight: 4, marginBottom: 2,
      whiteSpace: 'nowrap',
    }} title={`${name}: value=${value}`}>
      {name.replace(/_/g, ' ')} {sign}{pct}%
    </span>
  )
}

function PickRow({ pick }) {
  const oddsByBook = {
    fanduel: pick.fd_odds,
    draftkings: pick.dk_odds,
  }
  const bestBookKey = pick.best_book
  const bestPrice = oddsByBook[bestBookKey]
  const otherPrice = bestBookKey === 'fanduel' ? pick.dk_odds : pick.fd_odds
  const otherBookLabel = bestBookKey === 'fanduel' ? 'DK' : 'FD'
  const bestBookLabel = bestBookKey === 'fanduel' ? 'FD' : 'DK'

  const evColor = pick.ev_pct >= 40 ? ACCENT : (pick.ev_pct >= 25 ? YELLOW : TEXT)
  const isHomeBatter = pick.team === pick.park

  return (
    <tr style={{ borderBottom: '1px solid #111827' }}>
      {/* Batter + team + flags */}
      <td style={{ padding: '10px 8px', whiteSpace: 'nowrap' }}>
        <div style={{ fontWeight: 600, color: BRIGHT }}>{pick.batter}</div>
        <div style={{ fontSize: 10, color: MUTED, fontFamily: MONO, marginTop: 1, display: 'flex', gap: 4, alignItems: 'center' }}>
          <span>{pick.team}{isHomeBatter ? ' (home)' : ''}</span>
          {pick.batter_hand && <span>· {pick.batter_hand}H</span>}
          {pick.lineup_spot && <span>· #{pick.lineup_spot}</span>}
          {pick.low_confidence && (
            <span style={{ color: ORANGE, background: 'rgba(251,146,60,0.12)', padding: '1px 5px', borderRadius: 3, fontSize: 9 }}>LC</span>
          )}
          {pick.breakout_score >= 0.10 && (
            <span style={{ color: PURPLE, background: 'rgba(168,85,247,0.12)', padding: '1px 5px', borderRadius: 3, fontSize: 9 }}
                  title={`breakout_score = ${pick.breakout_score}`}>BO</span>
          )}
        </div>
      </td>

      {/* Park / Game time */}
      <td style={{ padding: '10px 6px', color: TEXT, whiteSpace: 'nowrap', fontFamily: MONO, fontSize: 11 }}>
        <div>@ {pick.park}</div>
        <div style={{ color: MUTED, fontSize: 10 }}>{formatGameTime(pick.game_datetime)}</div>
      </td>

      {/* Pitcher */}
      <td style={{ padding: '10px 6px', color: TEXT, whiteSpace: 'nowrap' }}>
        <div>{pick.pitcher || '---'}</div>
        <div style={{ color: MUTED, fontSize: 10, fontFamily: MONO }}>{pick.pitcher_hand}HP</div>
      </td>

      {/* Line */}
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 11, color: TEXT }}>
        Over {pick.line}
      </td>

      {/* Model% */}
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12, fontWeight: 600, color: BRIGHT }}>
        {(pick.model_prob * 100).toFixed(1)}%
      </td>

      {/* Market% (de-vig) */}
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12, color: TEXT }}>
        {(pick.market_prob_devig * 100).toFixed(1)}%
      </td>

      {/* Odds — best price highlighted, other price subtle */}
      <td style={{ padding: '10px 6px', textAlign: 'right', whiteSpace: 'nowrap' }}>
        <div style={{ fontFamily: MONO, fontSize: 12, fontWeight: 700, color: BRIGHT }}>
          {formatOdds(bestPrice)} <span style={{ fontSize: 9, color: MUTED, fontWeight: 600 }}>{bestBookLabel}</span>
        </div>
        <div style={{ fontFamily: MONO, fontSize: 10, color: MUTED, marginTop: 1 }}>
          {formatOdds(otherPrice)} {otherBookLabel}
        </div>
      </td>

      {/* EV% */}
      <td style={{
        padding: '10px 6px', textAlign: 'right', fontWeight: 700, fontSize: 13,
        fontFamily: MONO, color: evColor,
      }}>+{pick.ev_pct.toFixed(1)}%</td>

      {/* Why — top_3_features chips */}
      <td style={{ padding: '10px 8px', minWidth: 220 }}>
        {(pick.top_3_features || []).map((f, i) => (
          <FeatureChip key={i} name={f.name} value={f.value} />
        ))}
      </td>
    </tr>
  )
}

function PicksTable({ picks }) {
  if (!picks || picks.length === 0) {
    return (
      <div style={{ padding: '32px 18px', textAlign: 'center', color: MUTED, fontSize: 13 }}>
        No picks above the EV threshold for this slate.
      </div>
    )
  }
  const cols = [
    { label: 'Batter', align: 'left' },
    { label: 'Park', align: 'left' },
    { label: 'vs Pitcher', align: 'left' },
    { label: 'Line', align: 'right' },
    { label: 'Model', align: 'right' },
    { label: 'Market', align: 'right' },
    { label: 'Odds', align: 'right' },
    { label: 'EV', align: 'right' },
    { label: 'Why', align: 'left' },
  ]
  return (
    <div style={{ overflowX: 'auto', WebkitOverflowScrolling: 'touch' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13, minWidth: 920 }}>
        <thead>
          <tr style={{ borderBottom: `2px solid ${BORDER}` }}>
            {cols.map(h => (
              <th key={h.label} style={{
                padding: '8px 6px', textAlign: h.align,
                color: MUTED, fontWeight: 600, fontSize: 9, letterSpacing: 0.8,
                fontFamily: MONO,
              }}>{h.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {picks.map((p, i) => <PickRow key={`${p.batter_id}-${p.game_pk || i}`} pick={p} />)}
        </tbody>
      </table>
    </div>
  )
}

function Methodology({ data }) {
  const [open, setOpen] = useState(false)

  return (
    <div style={{
      marginBottom: 28, background: CARD_BG, border: `1px solid ${BORDER}`,
      borderRadius: 10, overflow: 'hidden',
    }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          width: '100%', padding: '14px 18px', background: 'none', border: 'none',
          color: BRIGHT, cursor: 'pointer', display: 'flex', alignItems: 'center',
          justifyContent: 'space-between', fontFamily: SANS,
        }}
      >
        <span style={{ fontSize: 14, fontWeight: 700, letterSpacing: -0.3 }}>How this works (V7)</span>
        <span style={{ fontSize: 18, color: MUTED, transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>&#9662;</span>
      </button>

      {open && (
        <div style={{ padding: '0 18px 22px' }}>
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>What V7 is</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              An empirical-Bayes baseline that predicts P(HR ≥ 1) per batter per game.
              Bayesian-blends season-to-date HR/PA with last-30-day form (with a dynamic
              prior-year prior that decays as the current season accumulates plate
              appearances), then adjusts for matched platoon-split pitcher quality,
              handedness-specific park HR factor, and game-time temperature + wind out to CF.
            </div>
          </div>

          <div style={{ marginBottom: 20 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Column reference</div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <tbody>
                {[
                  ['Batter', 'Hitter being projected. LC = low confidence (no current-season PAs, projection driven by prior year). BO = breakout score ≥ 0.10 (current Statcast meaningfully better than prior year).'],
                  ['Park', "Home park. Game time shown below in your local timezone."],
                  ['vs Pitcher', "Opposing starter and throwing hand."],
                  ['Line', "Always 'Over 0.5' — the alternate HR market we bet."],
                  ['Model', "P(at least one HR) under the V7 baseline."],
                  ['Market', "De-vigged Over 0.5 probability across FD + DK (consensus when both books quote both sides)."],
                  ['Odds', "Best American price across FD/DK. Smaller line below shows the other book for comparison."],
                  ['EV', `Expected return per $1 stake. Filtered to ≥ ${data.ev_threshold_pct ?? 25}%. Yellow = ≥25%, green = ≥40%.`],
                  ['Why', "Top-3 contributors from the model components. Multiplicative factors show as +/-X% deviation from neutral. breakout_signal is encoded as +X% lift to the blended HR rate."],
                ].map(([term, desc], i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${BORDER}` }}>
                    <td style={{ padding: '10px 10px 10px 0', fontWeight: 700, color: BRIGHT, fontFamily: MONO, fontSize: 11, whiteSpace: 'nowrap', verticalAlign: 'top', width: 90 }}>{term}</td>
                    <td style={{ padding: '10px 0', color: TEXT, lineHeight: 1.6 }}>{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{ marginBottom: 20 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Validation status</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              We do <strong style={{ color: BRIGHT }}>not</strong> have stored historical
              odds, so a real backtest isn't possible on day one. Odds are logged from
              day one to build our own dataset; the ML training infrastructure is built
              but stays dormant until ~60 days of logged odds. <strong style={{ color: ORANGE }}>Paper-trade
              for at least 60 days before real money</strong>; gates require positive CLV
              (closing line value) and predicted-vs-actual HR rate within 2pp across deciles.
            </div>
          </div>

          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Data hygiene</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              No median-fill for missing Statcast features — batters with insufficient
              track record are <em>skipped</em> and logged separately, not imputed.
              No synthetic odds. No end-of-season aggregates leaking into training. Park
              factors are handedness-specific and computed from real Statcast data only.
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function RefreshButton() {
  const [status, setStatus] = useState(null)

  const trigger = async () => {
    setStatus('running')
    try {
      const r = await fetch('/api/trigger', { method: 'POST' })
      const data = await r.json()
      if (data.success) {
        setStatus('success')
        setTimeout(() => setStatus(null), 5000)
      } else {
        setStatus('error')
        setTimeout(() => setStatus(null), 5000)
      }
    } catch {
      setStatus('error')
      setTimeout(() => setStatus(null), 5000)
    }
  }

  const label = status === 'running' ? 'Running...' : status === 'success' ? 'Triggered' : status === 'error' ? 'Error' : 'Refresh Picks'
  const bg = status === 'success' ? 'rgba(34,197,94,0.15)' : status === 'error' ? 'rgba(239,68,68,0.15)' : 'rgba(59,130,246,0.1)'
  const color = status === 'success' ? ACCENT : status === 'error' ? ACCENT_RED : BLUE

  return (
    <button onClick={trigger} disabled={status === 'running'} style={{
      background: bg, border: `1px solid ${color}33`, borderRadius: 6,
      color, padding: '6px 14px', fontSize: 11, fontWeight: 600,
      fontFamily: MONO, cursor: status === 'running' ? 'wait' : 'pointer',
      letterSpacing: 0.3, opacity: status === 'running' ? 0.6 : 1,
    }}>{label}</button>
  )
}

export default function Home() {
  const picks = picksData?.picks ?? []
  const asOf = picksData?.as_of_date ?? '---'
  const generatedAt = picksData?.generated_at
  const modelVersion = picksData?.model_version ?? '---'
  const evThreshold = picksData?.ev_threshold_pct ?? 25
  const skippedCount = picksData?.skipped_count ?? 0
  const skippedRef = picksData?.skipped_reference

  const lowConfidence = picks.filter(p => p.low_confidence).length
  const breakouts = picks.filter(p => (p.breakout_score ?? 0) >= 0.10).length

  let generatedLocal = ''
  if (generatedAt) {
    try {
      generatedLocal = new Date(generatedAt).toLocaleString()
    } catch { /* swallow */ }
  }

  return (
    <>
      <Head>
        <title>HR Picks — V7</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
      </Head>

      <div style={{
        minHeight: '100vh', background: BG, color: TEXT,
        fontFamily: SANS, padding: '24px 16px',
        maxWidth: 1100, margin: '0 auto',
      }}>
        <div style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 28, fontWeight: 800, color: BRIGHT, letterSpacing: -1 }}>HR Picks</span>
            <span style={{
              fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 4,
              background: 'rgba(168,85,247,0.12)', color: PURPLE, letterSpacing: 0.6,
              fontFamily: MONO,
            }}>V7</span>
            <RefreshButton />
          </div>
          <div style={{ fontSize: 11, color: MUTED, fontFamily: MONO }}>
            {modelVersion} · As of {asOf} · EV threshold {evThreshold}%
            {generatedLocal && ` · Generated ${generatedLocal}`}
          </div>
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 28, flexWrap: 'wrap' }}>
          <StatCard
            label="Picks Today"
            value={`${picks.length}`}
            color={picks.length > 0 ? BLUE : MUTED}
            sub={picks.length === 0 ? 'no picks above EV threshold' : `≥ ${evThreshold}% EV`}
          />
          <StatCard
            label="Skipped Batters"
            value={`${skippedCount}`}
            color={skippedCount > 0 ? ORANGE : MUTED}
            sub={skippedRef ? `see ${skippedRef.split('/').pop()}` : 'no skips logged'}
          />
          <StatCard
            label="Low-Confidence"
            value={`${lowConfidence}`}
            color={lowConfidence > 0 ? ORANGE : MUTED}
            sub="prior-year only"
          />
          <StatCard
            label="Breakout Tagged"
            value={`${breakouts}`}
            color={breakouts > 0 ? PURPLE : MUTED}
            sub="score ≥ 0.10"
          />
        </div>

        <Methodology data={picksData ?? {}} />

        <PicksTable picks={picks} />

        <div style={{
          marginTop: 32, padding: '14px 0', borderTop: `1px solid ${BORDER}`,
          fontSize: 10, color: MUTED, lineHeight: 1.7, fontFamily: MONO,
        }}>
          Odds: FanDuel + DraftKings (best-of) via The Odds API. De-vigged when both
          books quote both sides.<br />
          V7 status: empirical-Bayes baseline. Paper-trade gate: 60+ days. ML model
          dormant until enough logged odds.<br />
          Always verify the current price on the listed book before placing a bet — lines move.
        </div>
      </div>
    </>
  )
}
