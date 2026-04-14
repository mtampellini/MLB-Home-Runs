import Head from 'next/head'
import { useState } from 'react'
import picksData from '../data/picks.json'

const ACCENT = '#22c55e'
const ACCENT_RED = '#ef4444'
const YELLOW = '#facc15'
const BLUE = '#3b82f6'
const BG = '#06090f'
const CARD_BG = '#0c1220'
const BORDER = '#1a2332'
const MUTED = '#475569'
const TEXT = '#94a3b8'
const BRIGHT = '#e2e8f0'
const MONO = 'JetBrains Mono, monospace'
const SANS = 'DM Sans, sans-serif'

function formatOdds(odds) {
  return odds > 0 ? `+${odds}` : `${odds}`
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

function PickRow({ pick }) {
  const isSettled = pick.hit_hr !== null
  const hit = pick.hit_hr === true
  const payout = pick.book_odds / 100

  return (
    <tr style={{ borderBottom: '1px solid #111827' }}>
      <td style={{ padding: '10px 8px', fontWeight: 600, color: BRIGHT, whiteSpace: 'nowrap' }}>{pick.batter}</td>
      <td style={{ padding: '10px 6px', color: TEXT, whiteSpace: 'nowrap' }}>{pick.game}</td>
      <td style={{ padding: '10px 6px', color: TEXT, whiteSpace: 'nowrap' }}>{pick.vs_pitcher}</td>
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12 }}>{(pick.model_prob * 100).toFixed(1)}%</td>
      <td style={{ padding: '10px 6px', textAlign: 'right', fontFamily: MONO, fontSize: 12, fontWeight: 600 }}>{formatOdds(pick.book_odds)}</td>
      <td style={{
        padding: '10px 6px', textAlign: 'center', fontSize: 10, fontWeight: 600,
        fontFamily: MONO, color: MUTED,
      }}>{pick.book || '---'}</td>
      <td style={{
        padding: '10px 6px', textAlign: 'right', fontWeight: 700, fontSize: 12,
        fontFamily: MONO,
        color: pick.projected_roi >= 40 ? ACCENT : (pick.projected_roi >= 25 ? YELLOW : TEXT),
      }}>+{pick.projected_roi.toFixed(1)}%</td>
      <td style={{ padding: '10px 10px', textAlign: 'center', fontWeight: 700, fontSize: 12 }}>
        {!isSettled ? (
          <span style={{
            color: BLUE, background: 'rgba(59,130,246,0.1)',
            padding: '3px 10px', borderRadius: 4, fontSize: 11,
          }}>PENDING</span>
        ) : hit ? (
          <span style={{
            color: ACCENT, background: 'rgba(34,197,94,0.1)',
            padding: '3px 10px', borderRadius: 4, fontSize: 11,
          }}>YES +{payout.toFixed(0)}u</span>
        ) : (
          <span style={{
            color: ACCENT_RED, background: 'rgba(239,68,68,0.08)',
            padding: '3px 10px', borderRadius: 4, fontSize: 11,
          }}>NO -1u</span>
        )}
      </td>
    </tr>
  )
}

function DaySection({ date, data }) {
  const picks = data.picks || []
  const settled = data.settled
  const wins = picks.filter(p => p.hit_hr === true).length
  const losses = picks.filter(p => p.hit_hr === false).length
  const dayPnl = picks.reduce((sum, p) => sum + (p.pnl || 0), 0)

  return (
    <div style={{ marginBottom: 36 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, color: BRIGHT, margin: 0 }}>{date}</h2>
        <span style={{ fontSize: 12, color: MUTED }}>{picks.length} picks</span>
        {settled && (
          <span style={{
            fontSize: 12, fontWeight: 700, fontFamily: MONO,
            color: dayPnl >= 0 ? ACCENT : ACCENT_RED,
            background: dayPnl >= 0 ? 'rgba(34,197,94,0.1)' : 'rgba(239,68,68,0.08)',
            padding: '2px 10px', borderRadius: 4,
          }}>
            {wins}W-{losses}L &middot; {dayPnl >= 0 ? '+' : ''}{dayPnl.toFixed(1)}u
          </span>
        )}
        {!settled && (
          <span style={{
            fontSize: 11, fontWeight: 600, fontFamily: MONO,
            color: BLUE, background: 'rgba(59,130,246,0.1)',
            padding: '2px 10px', borderRadius: 4,
          }}>LIVE</span>
        )}
      </div>
      <div style={{ overflowX: 'auto', WebkitOverflowScrolling: 'touch' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13, minWidth: 720 }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${BORDER}` }}>
              {[
                { label: 'Batter', align: 'left' },
                { label: 'Game', align: 'left' },
                { label: 'vs Pitcher', align: 'left' },
                { label: 'Model', align: 'right' },
                { label: 'Odds', align: 'right' },
                { label: 'Book', align: 'center' },
                { label: 'Proj ROI', align: 'right' },
                { label: 'Hit HR?', align: 'center' },
              ].map(h => (
                <th key={h.label} style={{
                  padding: '8px 6px', textAlign: h.align,
                  color: MUTED, fontWeight: 600, fontSize: 9, letterSpacing: 0.8,
                  fontFamily: MONO,
                }}>{h.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {picks.map((p, i) => <PickRow key={i} pick={p} />)}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function HowItWorks({ config }) {
  const [open, setOpen] = useState(false)

  const definitions = [
    {
      term: 'Model %',
      desc: "The model's predicted probability that this batter hits a home run tonight. Calculated by feeding 39 features into an XGBoost model trained on 23,000+ real batter-game outcomes. Inputs include Statcast hitting metrics (barrel rate, exit velo, xSLG, bat speed), pitcher vulnerability data (HR/FB rate, pitch mix, xwOBA allowed), park factor, weather, platoon advantage, and lineup position.",
    },
    {
      term: 'Odds',
      desc: 'The best available American odds between FanDuel and DraftKings for that player to hit a HR. For example, +500 means a $1 bet returns $5 profit if it hits. The implied probability from +500 odds is 1 / (1 + 5.00) = 16.7%, which represents what the sportsbook thinks the HR chance is (with their margin built in).',
    },
    {
      term: 'Book',
      desc: 'Which sportsbook is offering the best price. The system compares FanDuel and DraftKings, then shows only the higher payout. Always verify the line on that book before placing the bet since odds can shift.',
    },
    {
      term: 'Proj ROI',
      desc: 'Projected return on investment if you bet this type of spot repeatedly. Formula: (Model% / Implied%) - 1, shown as a percentage. Example: if the model says 30% and the odds imply 17%, Proj ROI = (0.30 / 0.17 - 1) = +76%. Only picks above ' + config.min_roi_threshold + '% make the board.',
    },
    {
      term: 'Hit HR?',
      desc: 'Result tracking. Shows PENDING before the game, then YES (with payout in units) or NO (-1u) after. One unit = one flat bet. A hit at +500 odds returns +5.0u profit.',
    },
  ]

  const processSteps = [
    'Pull live HR prop odds from FanDuel and DraftKings via API',
    "Match each batter to their Statcast profile and the opposing pitcher's vulnerability metrics",
    'Run the matchup through the model to get a HR probability',
    "Compare model probability against the book's implied probability",
    'Flag picks where projected ROI exceeds ' + config.min_roi_threshold + '%',
    'Best price between FD and DK is selected automatically',
  ]

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
        <span style={{ fontSize: 14, fontWeight: 700, letterSpacing: -0.3 }}>How It Works</span>
        <span style={{ fontSize: 18, color: MUTED, transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>&#9662;</span>
      </button>

      {open && (
        <div style={{ padding: '0 18px 18px' }}>
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 1, fontFamily: MONO, marginBottom: 10 }}>
              Process
            </div>
            {processSteps.map((step, i) => (
              <div key={i} style={{ display: 'flex', gap: 10, marginBottom: 8, fontSize: 12, color: TEXT, lineHeight: 1.5 }}>
                <span style={{ color: MUTED, fontFamily: MONO, fontSize: 11, flexShrink: 0 }}>{i + 1}.</span>
                <span>{step}</span>
              </div>
            ))}
          </div>

          <div style={{ borderTop: `1px solid ${BORDER}`, paddingTop: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 1, fontFamily: MONO, marginBottom: 12 }}>
              Column Definitions
            </div>
            {definitions.map((d, i) => (
              <div key={i} style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: BRIGHT, fontFamily: MONO, marginBottom: 3 }}>{d.term}</div>
                <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.6 }}>{d.desc}</div>
              </div>
            ))}
          </div>

          <div style={{ borderTop: `1px solid ${BORDER}`, paddingTop: 14, marginTop: 6 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 1, fontFamily: MONO, marginBottom: 8 }}>
              Model Details
            </div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              XGBoost classifier trained on 23,728 batter-game samples. 39 features spanning Statcast hitting metrics,
              pitcher vulnerability profiles, pitch mix analysis, park factors, weather, and lineup context.
              Walk-forward validated across May through September 2025 with all 5 months profitable.
              Backtest ROI of +48.4% on the top tier at average odds of +500.
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default function Home() {
  const { dates, cumulative, config } = picksData
  const sortedDates = Object.keys(dates).sort().reverse()
  const todayPicks = dates[sortedDates[0]]?.picks || []
  const pendingCount = todayPicks.filter(p => p.hit_hr === null).length

  return (
    <>
      <Head>
        <title>HR Picks</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
      </Head>

      <div style={{
        minHeight: '100vh', background: BG, color: TEXT,
        fontFamily: SANS, padding: '24px 16px',
        maxWidth: 1000, margin: '0 auto',
      }}>
        <div style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4 }}>
            <span style={{ fontSize: 28, fontWeight: 800, color: BRIGHT, letterSpacing: -1 }}>HR Picks</span>
            {pendingCount > 0 && (
              <span style={{
                fontSize: 10, fontWeight: 600, padding: '3px 8px', borderRadius: 4,
                background: 'rgba(59,130,246,0.12)', color: BLUE, letterSpacing: 0.5,
                fontFamily: MONO,
              }}>{pendingCount} PENDING</span>
            )}
          </div>
          <div style={{ fontSize: 11, color: MUTED, fontFamily: MONO }}>
            Model v4 | Walk-forward validated | 5/5 months profitable | Min ROI: {config.min_roi_threshold}%
          </div>
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 28, flexWrap: 'wrap' }}>
          <StatCard
            label="Cumulative P&L"
            value={`${cumulative.total_pnl >= 0 ? '+' : ''}${cumulative.total_pnl.toFixed(1)}u`}
            color={cumulative.total_pnl >= 0 ? ACCENT : ACCENT_RED}
          />
          <StatCard
            label="ROI"
            value={cumulative.total_bets > 0 ? `${cumulative.roi.toFixed(1)}%` : '---'}
            color={cumulative.roi >= 0 ? ACCENT : ACCENT_RED}
            sub={`${cumulative.total_bets} bets`}
          />
          <StatCard
            label="Record"
            value={cumulative.total_bets > 0 ? `${cumulative.total_wins}W-${cumulative.total_bets - cumulative.total_wins}L` : '0W-0L'}
            color={BRIGHT}
            sub={cumulative.total_bets > 0 ? `${(cumulative.total_wins / cumulative.total_bets * 100).toFixed(0)}% hit rate` : ''}
          />
          <StatCard
            label="Today"
            value={`${todayPicks.length} picks`}
            color={BLUE}
          />
        </div>

        <HowItWorks config={config} />

        {sortedDates.map(d => (
          <DaySection key={d} date={d} data={dates[d]} />
        ))}

        <div style={{
          marginTop: 32, padding: '14px 0', borderTop: `1px solid ${BORDER}`,
          fontSize: 10, color: MUTED, lineHeight: 1.7, fontFamily: MONO,
        }}>
          Odds sourced exclusively from FanDuel and DraftKings. Best price selected automatically.<br />
          Backtest: +48.4% ROI over 5 months walk-forward (5/5 months profitable).<br />
          Always verify current odds before placing bets. Lines move.
        </div>
      </div>
    </>
  )
}
