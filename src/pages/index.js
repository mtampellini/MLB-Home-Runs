import Head from 'next/head'
import picksData from '../data/picks.json'

const ACCENT = '#22c55e'
const ACCENT_RED = '#ef4444'
const BG = '#06090f'
const CARD_BG = '#0c1220'
const BORDER = '#1a2332'
const MUTED = '#475569'
const TEXT = '#94a3b8'
const BRIGHT = '#e2e8f0'

function formatOdds(odds) {
  return odds > 0 ? `+${odds}` : `${odds}`
}

function StatCard({ label, value, color, sub }) {
  return (
    <div style={{
      background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10,
      padding: '14px 18px', minWidth: 130, flex: '1 1 130px',
    }}>
      <div style={{ fontSize: 10, color: MUTED, textTransform: 'uppercase', letterSpacing: 1.2, fontFamily: 'JetBrains Mono, monospace' }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 800, color: color || BRIGHT, marginTop: 2, fontFamily: 'DM Sans, sans-serif' }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: MUTED, marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

function PickRow({ pick, i }) {
  const isSettled = pick.hit_hr !== null
  const hit = pick.hit_hr === true
  const payout = pick.book_odds / 100
  const pnl = isSettled ? (hit ? payout : -1) : null

  return (
    <tr style={{ borderBottom: `1px solid ${BORDER}` }}>
      <td style={{ padding: '10px 8px', fontWeight: 600, color: BRIGHT }}>{pick.batter}</td>
      <td style={{ padding: '10px 6px', color: TEXT }}>{pick.game}</td>
      <td style={{ padding: '10px 6px', color: TEXT }}>{pick.vs_pitcher}</td>
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: 'JetBrains Mono, monospace' }}>{(pick.model_prob * 100).toFixed(1)}%</td>
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: 'JetBrains Mono, monospace', fontWeight: 600 }}>{formatOdds(pick.book_odds)}</td>
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: 'JetBrains Mono, monospace' }}>{(pick.edge * 100).toFixed(1)}pp</td>
      <td style={{
        padding: '10px 6px', textAlign: 'right', fontWeight: 700,
        fontFamily: 'JetBrains Mono, monospace',
        color: pick.projected_roi >= 10 ? ACCENT : (pick.projected_roi > 0 ? '#facc15' : ACCENT_RED),
      }}>+{pick.projected_roi.toFixed(1)}%</td>
      <td style={{
        padding: '10px 10px', textAlign: 'center', fontWeight: 700, fontSize: 13,
      }}>
        {!isSettled ? (
          <span style={{ color: '#3b82f6' }}>LIVE</span>
        ) : hit ? (
          <span style={{ color: ACCENT }}>HIT +{payout.toFixed(1)}u</span>
        ) : (
          <span style={{ color: ACCENT_RED }}>MISS -1u</span>
        )}
      </td>
    </tr>
  )
}

function DaySection({ date, data }) {
  const picks = data.picks || []
  const settled = data.settled
  const settledPicks = picks.filter(p => p.hit_hr !== null)
  const wins = settledPicks.filter(p => p.hit_hr === true).length
  const losses = settledPicks.filter(p => p.hit_hr === false).length
  const dayPnl = settledPicks.reduce((sum, p) => sum + (p.hit_hr ? p.book_odds / 100 : -1), 0)

  return (
    <div style={{ marginBottom: 32 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 12, marginBottom: 10 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, color: BRIGHT, margin: 0 }}>{date}</h2>
        <span style={{ fontSize: 12, color: MUTED }}>{picks.length} picks</span>
        {settled && (
          <span style={{
            fontSize: 12, fontWeight: 700, fontFamily: 'JetBrains Mono, monospace',
            color: dayPnl >= 0 ? ACCENT : ACCENT_RED,
          }}>
            {wins}W-{losses}L | {dayPnl >= 0 ? '+' : ''}{dayPnl.toFixed(2)}u
          </span>
        )}
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${BORDER}` }}>
              {['Batter', 'Game', 'vs Pitcher', 'Model', 'Odds', 'Edge', 'Proj ROI', 'Result'].map((h, i) => (
                <th key={h} style={{
                  padding: '8px 6px', textAlign: i >= 3 ? 'right' : 'left',
                  color: MUTED, fontWeight: 600, fontSize: 10, letterSpacing: 0.8,
                  fontFamily: 'JetBrains Mono, monospace',
                  ...(i === 7 ? { textAlign: 'center' } : {}),
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {picks.map((p, i) => <PickRow key={i} pick={p} i={i} />)}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function Home() {
  const { dates, cumulative, config } = picksData
  const sortedDates = Object.keys(dates).sort().reverse()
  const today = sortedDates[0]
  const todayPicks = dates[today]?.picks || []

  return (
    <>
      <Head>
        <title>HR Picks</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
      </Head>

      <div style={{
        minHeight: '100vh', background: BG, color: TEXT,
        fontFamily: 'DM Sans, sans-serif', padding: '24px 16px',
        maxWidth: 960, margin: '0 auto',
      }}>
        {/* Header */}
        <div style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4 }}>
            <span style={{ fontSize: 28, fontWeight: 800, color: BRIGHT, letterSpacing: -1 }}>
              HR Picks
            </span>
            <span style={{
              fontSize: 10, fontWeight: 600, padding: '3px 8px', borderRadius: 4,
              background: 'rgba(34,197,94,0.12)', color: ACCENT, letterSpacing: 0.5,
              fontFamily: 'JetBrains Mono, monospace',
            }}>LIVE</span>
          </div>
          <div style={{ fontSize: 11, color: MUTED, fontFamily: 'JetBrains Mono, monospace' }}>
            Model v4 | Walk-forward validated | 5/5 months profitable | Min ROI: {config.min_roi_threshold}%
          </div>
        </div>

        {/* Stats Dashboard */}
        <div style={{ display: 'flex', gap: 10, marginBottom: 28, flexWrap: 'wrap' }}>
          <StatCard
            label="Cumulative P&L"
            value={`${cumulative.total_pnl >= 0 ? '+' : ''}${cumulative.total_pnl.toFixed(1)}u`}
            color={cumulative.total_pnl >= 0 ? ACCENT : ACCENT_RED}
          />
          <StatCard
            label="ROI"
            value={cumulative.total_bets > 0 ? `${(cumulative.roi).toFixed(1)}%` : '---'}
            color={cumulative.roi >= 0 ? ACCENT : ACCENT_RED}
            sub={`${cumulative.total_bets} bets`}
          />
          <StatCard
            label="Record"
            value={`${cumulative.total_wins}W-${cumulative.total_bets - cumulative.total_wins}L`}
            color={BRIGHT}
            sub={cumulative.total_bets > 0 ? `${(cumulative.total_wins / cumulative.total_bets * 100).toFixed(0)}% hit rate` : ''}
          />
          <StatCard
            label="Today"
            value={`${todayPicks.length} picks`}
            color="#3b82f6"
          />
        </div>

        {/* Picks by day */}
        {sortedDates.map(d => (
          <DaySection key={d} date={d} data={dates[d]} />
        ))}

        {/* Footer */}
        <div style={{
          marginTop: 32, padding: '14px 0', borderTop: `1px solid ${BORDER}`,
          fontSize: 10, color: MUTED, lineHeight: 1.7,
          fontFamily: 'JetBrains Mono, monospace',
        }}>
          Model trained on 23K+ batter-games | Walk-forward validated May-Sep 2025 (5/5 months profitable)<br />
          Picks qualified by projected ROI threshold, not arbitrary top N<br />
          Backtest: +48.4% ROI on top 3% at +500 avg odds | Breakeven at +305<br />
          Unit size: $1 flat per pick | Only bet when book odds exceed model breakeven + 10% buffer
        </div>
      </div>
    </>
  )
}
