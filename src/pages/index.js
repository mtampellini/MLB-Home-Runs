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

function Methodology({ config }) {
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
        <span style={{ fontSize: 14, fontWeight: 700, letterSpacing: -0.3 }}>Methodology &amp; Column Reference</span>
        <span style={{ fontSize: 18, color: MUTED, transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>&#9662;</span>
      </button>

      {open && (
        <div style={{ padding: '0 18px 22px' }}>

          {/* TABLE OF CONTENTS */}
          <div style={{
            display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 20,
            paddingBottom: 16, borderBottom: `1px solid ${BORDER}`,
          }}>
            {['Overview', 'Columns', 'Model', 'Odds Source', 'Updates', 'Betting Guide'].map(s => (
              <a key={s} href={`#section-${s.toLowerCase().replace(/ /g, '-')}`} style={{
                fontSize: 11, fontFamily: MONO, color: BLUE, textDecoration: 'none',
                padding: '4px 10px', borderRadius: 4, background: 'rgba(59,130,246,0.08)',
                border: `1px solid rgba(59,130,246,0.15)`,
              }}>{s}</a>
            ))}
          </div>

          {/* OVERVIEW */}
          <div id="section-overview" style={{ marginBottom: 24 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Overview</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              This app identifies MLB home run prop bets where the model believes a batter{"'"}s true HR probability
              is meaningfully higher than what the sportsbook odds imply. It compares a machine learning projection
              against FanDuel and DraftKings prices, then surfaces only the picks where the projected return on
              investment exceeds {config.min_roi_threshold}%. The goal is to find spots where the book is underpricing
              a batter{"'"}s HR likelihood based on matchup context the odds may not fully reflect.
            </div>
          </div>

          {/* COLUMNS */}
          <div id="section-columns" style={{ marginBottom: 24 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 12 }}>Column Definitions</div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <tbody>
                {[
                  ['Batter', 'The hitter being evaluated for a HR prop bet.'],
                  ['Game', 'The matchup, formatted as Away @ Home.'],
                  ['vs Pitcher', 'The opposing starting pitcher. Pitcher vulnerability is a major model input since HR rates vary significantly by who is on the mound.'],
                  ['Model', "The model's predicted probability that this batter hits a HR in this game. Ranges from roughly 5% for weak matchups to 30%+ for elite power hitters facing HR-prone pitchers in hitter-friendly parks. The league average HR rate per plate appearance is around 3%, but these are filtered to batters with real HR upside."],
                  ['Odds', 'The best available American odds between FanDuel and DraftKings. Example: +500 means a $1 bet pays $5 profit if it hits. The implied probability from +500 is 1 / (1 + 5.00) = 16.7%. That represents what the book thinks the HR chance is, with their margin built in.'],
                  ['Book', 'Which sportsbook is offering the better price. The system compares FanDuel and DraftKings for each batter and shows only the higher payout. Always verify the line on that book before betting since odds can move.'],
                  ['Proj ROI', 'Projected return on investment if you bet this type of spot repeatedly. Formula: (Model% / Implied%) - 1, shown as a percentage. Example: Model says 30%, odds imply 17%, so Proj ROI = (0.30 / 0.17 - 1) = +76%. Only picks above ' + config.min_roi_threshold + '% make the board. Green = 40%+, yellow = 25-39%.'],
                  ['Hit HR?', 'Result tracking. Shows PENDING before the game, then YES (with payout in units) or NO (-1u) after results are graded. One unit = one flat bet.'],
                ].map(([term, desc], i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${BORDER}` }}>
                    <td style={{ padding: '10px 10px 10px 0', fontWeight: 700, color: BRIGHT, fontFamily: MONO, fontSize: 11, whiteSpace: 'nowrap', verticalAlign: 'top', width: 90 }}>{term}</td>
                    <td style={{ padding: '10px 0', color: TEXT, lineHeight: 1.6 }}>{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* MODEL */}
          <div id="section-model" style={{ marginBottom: 24 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Model Details</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7, marginBottom: 12 }}>
              XGBoost binary classifier trained on 23,728 real batter-game outcomes from the 2022-2025 MLB seasons.
              The model uses 39 features across five categories:
            </div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.8, paddingLeft: 12 }}>
              <span style={{ color: BRIGHT, fontWeight: 600 }}>Batter Statcast (12 features):</span> xSLG, xwOBA, barrel rate, max exit velo, average exit velo, hard-hit rate, bat speed, squared-up rate, K%, BB%, chase rate, whiff rate.<br/>
              <span style={{ color: BRIGHT, fontWeight: 600 }}>Pitcher Vulnerability (12 features):</span> xwOBA allowed, xSLG allowed, xERA, HR/PA rate, HR/FB rate, fly ball rate, ground ball rate, average exit velo allowed, barrel rate allowed, hard-hit rate allowed, fastball velo, HR danger score.<br/>
              <span style={{ color: BRIGHT, fontWeight: 600 }}>Pitch Mix (4 features):</span> Fastball%, sinker%, breaking%, offspeed%. Used to identify pitchers whose arsenals are more susceptible to the long ball.<br/>
              <span style={{ color: BRIGHT, fontWeight: 600 }}>Park &amp; Weather (5 features):</span> Park HR factor, altitude, temperature, wind speed, humidity, dome indicator.<br/>
              <span style={{ color: BRIGHT, fontWeight: 600 }}>Context (6 features):</span> Platoon advantage (L/R matchup), plate appearances (sample size proxy), max times through the order, lineup position, PA count.
            </div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7, marginTop: 12 }}>
              Walk-forward validated across May through September 2025. All five months were individually profitable.
              Backtest ROI of +48.4% on the top tier at average odds of +500. CV AUC of 0.689, meaning the model
              separates HR hitters from non-HR hitters meaningfully better than chance.
            </div>
          </div>

          {/* ODDS SOURCE */}
          <div id="section-odds-source" style={{ marginBottom: 24 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Odds Source</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              HR prop odds are pulled exclusively from <span style={{ color: BRIGHT, fontWeight: 600 }}>FanDuel</span> and <span style={{ color: BRIGHT, fontWeight: 600 }}>DraftKings</span> via
              The Odds API. For each batter, the system compares the price on both books and selects the higher
              payout. The Book column tells you where the best price came from so you know which app to open.
              No other sportsbooks are used. Odds reflect the snapshot at the time the pipeline last ran.
            </div>
          </div>

          {/* UPDATES */}
          <div id="section-updates" style={{ marginBottom: 24 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>How It Updates</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              The pipeline runs on demand via a GitHub Actions workflow. When triggered, it pulls the latest FD/DK
              odds, runs the model against every available HR prop, filters to picks with {config.min_roi_threshold}%+ projected ROI,
              and pushes the updated data to this site. Vercel auto-deploys within 30 seconds of each push.
              FanDuel and DraftKings typically post HR props after lineups are confirmed, usually between 11am and 2pm ET.
              Running the pipeline after 1pm ET generally gets the fullest set of lines. Results are graded the
              following day.
            </div>
          </div>

          {/* BETTING GUIDE */}
          <div id="section-betting-guide">
            <div style={{ fontSize: 13, fontWeight: 700, color: BRIGHT, marginBottom: 8 }}>Betting Guide</div>
            <div style={{ fontSize: 12, color: TEXT, lineHeight: 1.7 }}>
              Flat betting is recommended: $1 (or whatever your unit is) on every pick the system surfaces. Do not
              size up on higher-conviction plays. The edge comes from volume and consistency, not from swinging big
              on individual bets. HR props are inherently high-variance since even the best matchups only hit roughly 25-30%
              of the time. Over a full month of betting, the model targets around 20 picks per day with a combined
              ROI of approximately 30%+. Losing days are expected and normal. The math works over hundreds of bets, not dozens.
              Always verify the current odds on FanDuel or DraftKings before placing a bet since lines move.
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

        <Methodology config={config} />

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
