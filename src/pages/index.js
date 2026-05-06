import Head from 'next/head'
import Link from 'next/link'
import { useState } from 'react'
// picks.json is written at repo root by the daily_picks GitHub Action.
import picksData from '../../picks.json'

const T = {
  bg: '#ffffff',
  border: '#e5e5e5',
  borderStrong: '#d4d4d4',
  text: '#0a0a0a',
  textMedium: '#525252',
  textLight: '#a3a3a3',
  accent: '#2563eb',
  positive: '#16a34a',
  negative: '#dc2626',
}
const FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, system-ui, sans-serif'

function formatOdds(odds) {
  if (odds == null) return '—'
  return odds > 0 ? `+${odds}` : `${odds}`
}

function formatGameTime(iso) {
  if (!iso) return ''
  try {
    return new Date(iso).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
      .replace(' AM', 'am').replace(' PM', 'pm')
  } catch { return '' }
}

function featureLabel(name, value) {
  const pct = (value - 1) * 100
  const sign = pct > 0 ? '+' : ''
  const cleanName = name
    .replace(/_signal$/, '')
    .replace(/_factor$/, '')
    .replace(/_/g, ' ')
  return `${sign}${pct.toFixed(0)}% ${cleanName}`
}

function StatCard({ label, value, sub }) {
  return (
    <div style={{
      border: `1px solid ${T.border}`, borderRadius: 6,
      padding: '24px 26px', minWidth: 140, flex: '1 1 140px',
      background: T.bg,
    }}>
      <div style={{
        fontSize: 32, fontWeight: 600, color: T.text,
        letterSpacing: -0.5, lineHeight: 1.1,
        fontVariantNumeric: 'tabular-nums',
      }}>{value}</div>
      <div style={{ fontSize: 12, color: T.textMedium, marginTop: 10 }}>{label}</div>
      {sub && <div style={{ fontSize: 11, color: T.textLight, marginTop: 4 }}>{sub}</div>}
    </div>
  )
}

function PickRow({ pick }) {
  const oddsByBook = { fanduel: pick.fd_odds, draftkings: pick.dk_odds }
  const bestBookKey = pick.best_book
  const bestPrice = oddsByBook[bestBookKey]
  const otherPrice = bestBookKey === 'fanduel' ? pick.dk_odds : pick.fd_odds
  const otherBookLabel = bestBookKey === 'fanduel' ? 'DK' : 'FD'
  const bestBookLabel = bestBookKey === 'fanduel' ? 'FD' : 'DK'
  const evPositive = pick.ev_pct >= 0
  const isHomeBatter = pick.team === pick.park

  // Compose the inline metadata line under the batter name. Each entry is
  // quiet gray text — no badges, no colored fills.
  const metaParts = []
  metaParts.push(`${pick.team}${isHomeBatter ? ' (home)' : ''}`)
  if (pick.batter_hand) metaParts.push(`${pick.batter_hand}H`)
  if (pick.lineup_spot) metaParts.push(`#${pick.lineup_spot}`)
  if (pick.low_confidence) metaParts.push('low conf')
  if (pick.breakout_score >= 0.10) metaParts.push('breakout')
  if (pick.unstable_recent) metaParts.push('unstable')
  if (pick.trend_signal != null && Math.abs(pick.trend_signal) >= 0.10) {
    const arrow = pick.trend_signal > 0 ? '↑' : '↓'
    metaParts.push(`${arrow}${Math.abs(Math.round(pick.trend_signal * 100))}%`)
  }
  if (pick.stacked) metaParts.push('stacked')

  return (
    <tr style={{ borderBottom: `1px solid ${T.border}` }}>
      {/* Batter */}
      <td style={{ padding: '18px 10px', whiteSpace: 'nowrap', verticalAlign: 'top' }}>
        <div style={{ fontWeight: 600, color: T.text, fontSize: 14 }}>{pick.batter}</div>
        <div style={{ fontSize: 11, color: T.textLight, marginTop: 4 }}
             title={pick.stacked ? `stacked with ${(pick.stacked_with || []).join(', ')}` : undefined}>
          {metaParts.join(' · ')}
        </div>
      </td>

      {/* Park / Game time */}
      <td style={{ padding: '18px 10px', verticalAlign: 'top', whiteSpace: 'nowrap', fontSize: 13, color: T.textMedium }}>
        <div>{pick.park}</div>
        <div style={{ color: T.textLight, fontSize: 11, marginTop: 4 }}>{formatGameTime(pick.game_datetime)}</div>
      </td>

      {/* Pitcher */}
      <td style={{ padding: '18px 10px', verticalAlign: 'top', whiteSpace: 'nowrap', fontSize: 13, color: T.textMedium }}>
        <div>{pick.pitcher || '—'}</div>
        <div style={{ color: T.textLight, fontSize: 11, marginTop: 4 }}>{pick.pitcher_hand}HP</div>
      </td>

      {/* Line */}
      <td style={{ padding: '18px 10px', textAlign: 'right', verticalAlign: 'top', fontSize: 13, color: T.textMedium }}>
        Over {pick.line}
      </td>

      {/* Model% */}
      <td style={{
        padding: '18px 10px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 14, fontWeight: 600, color: T.text,
        fontVariantNumeric: 'tabular-nums',
      }}>{(pick.model_prob * 100).toFixed(1)}%</td>

      {/* Market% (de-vigged) */}
      <td style={{
        padding: '18px 10px', textAlign: 'right', verticalAlign: 'top',
        fontSize: 13, color: T.textMedium,
        fontVariantNumeric: 'tabular-nums',
      }}>{(pick.market_prob_devig * 100).toFixed(1)}%</td>

      {/* Odds */}
      <td style={{
        padding: '18px 10px', textAlign: 'right', verticalAlign: 'top', whiteSpace: 'nowrap',
        fontVariantNumeric: 'tabular-nums',
      }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: T.text }}>
          {formatOdds(bestPrice)} <span style={{ fontSize: 10, color: T.textLight, fontWeight: 500 }}>{bestBookLabel}</span>
        </div>
        <div style={{ fontSize: 11, color: T.textLight, marginTop: 4 }}>
          {formatOdds(otherPrice)} {otherBookLabel}
        </div>
      </td>

      {/* EV — bold; green tone when positive, dark otherwise. No fills. */}
      <td style={{
        padding: '18px 10px', textAlign: 'right', verticalAlign: 'top',
        fontWeight: 700, fontSize: 14,
        color: evPositive ? T.positive : T.text,
        fontVariantNumeric: 'tabular-nums',
      }}>{evPositive ? '+' : ''}{pick.ev_pct.toFixed(1)}%</td>

      {/* Why — inline gray text, breakout in italic. */}
      <td style={{
        padding: '18px 10px', verticalAlign: 'top', minWidth: 240,
        fontSize: 12, color: T.textMedium, lineHeight: 1.7,
      }}>
        {(pick.top_3_features || []).map((f, i) => {
          const isBreakout = f.name === 'breakout_signal'
          return (
            <span key={i} style={{
              marginRight: 14, whiteSpace: 'nowrap',
              fontStyle: isBreakout ? 'italic' : 'normal',
            }} title={`${f.name}: ${f.value}`}>
              {featureLabel(f.name, f.value)}
            </span>
          )
        })}
      </td>
    </tr>
  )
}

function PicksTable({ picks }) {
  if (!picks || picks.length === 0) {
    return (
      <div style={{
        padding: '48px 18px', textAlign: 'center',
        color: T.textLight, fontSize: 14,
        border: `1px solid ${T.border}`, borderRadius: 6,
      }}>
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
          <tr style={{ borderBottom: `1px solid ${T.borderStrong}` }}>
            {cols.map(h => (
              <th key={h.label} style={{
                padding: '10px 10px', textAlign: h.align,
                color: T.textMedium, fontWeight: 500, fontSize: 11,
                letterSpacing: 0.4,
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
      marginBottom: 32, border: `1px solid ${T.border}`,
      borderRadius: 6, overflow: 'hidden',
    }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          width: '100%', padding: '14px 18px', background: 'none', border: 'none',
          color: T.text, cursor: 'pointer', display: 'flex', alignItems: 'center',
          justifyContent: 'space-between', fontFamily: 'inherit',
          fontSize: 13, fontWeight: 500,
        }}
      >
        <span>How this works</span>
        <span style={{
          fontSize: 12, color: T.textLight,
          transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s',
        }}>▾</span>
      </button>

      {open && (
        <div style={{ padding: '0 18px 22px', borderTop: `1px solid ${T.border}` }}>
          <div style={{ marginTop: 18, marginBottom: 22, fontSize: 13, color: T.textMedium, lineHeight: 1.7 }}>
            An empirical-Bayes baseline that predicts P(HR ≥ 1) per batter per game.
            We Bayesian-blend season HR/PA with last-30-day form (and a prior-year
            prior that decays as PAs accumulate), then adjust for matched-platoon
            pitcher quality, handedness-specific park HR factor, and game-time
            temperature + wind out to center.
          </div>

          <div style={{ marginBottom: 22 }}>
            <div style={{ fontSize: 12, fontWeight: 600, color: T.text, marginBottom: 10 }}>Columns</div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <tbody>
                {[
                  ['Batter', 'Hitter being projected. Inline tags: low conf (no current PAs), breakout (score ≥ 0.10), unstable (30d barrel rate diverged ≥1.5x or ≤0.5x vs season), ↑/↓ X% (recent barrel rate trend), stacked (shares starter with another primary pick — outcomes correlated).'],
                  ['Park', 'Home park. Game time below in your local timezone.'],
                  ['vs Pitcher', 'Opposing starter and throwing hand.'],
                  ['Line', "Always 'Over 0.5' — the alternate HR market we bet."],
                  ['Model', 'P(at least one HR) under the V7 baseline.'],
                  ['Market', 'De-vigged Over 0.5 probability across FD + DK.'],
                  ['Odds', 'Best American price across FD/DK. Other book shown below.'],
                  ['EV', `Expected return per $1 stake. Filtered to ≥ ${data.ev_threshold_pct ?? 25}%. Higher is better.`],
                  ['Why', 'Top-3 model components. Multiplicative factors show as +/-X% deviation from neutral; breakout shows as +X% lift to the blended HR rate.'],
                ].map(([term, desc], i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${T.border}` }}>
                    <td style={{
                      padding: '12px 14px 12px 0', fontWeight: 600, color: T.text,
                      fontSize: 12, whiteSpace: 'nowrap', verticalAlign: 'top', width: 90,
                    }}>{term}</td>
                    <td style={{ padding: '12px 0', color: T.textMedium, lineHeight: 1.6 }}>{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{ marginBottom: 22, fontSize: 12, color: T.textMedium, lineHeight: 1.7 }}>
            <div style={{ fontSize: 12, fontWeight: 600, color: T.text, marginBottom: 8 }}>Validation</div>
            We do not have stored historical odds, so a real backtest isn't possible
            on day one. Odds are logged from day one to build our own dataset; the ML
            training infrastructure stays dormant until ~60 days of logged odds.
            Paper-trade for at least 60 days before real money; gates require positive
            CLV and predicted-vs-actual HR rate within 2pp across deciles.
          </div>

          <div style={{ fontSize: 12, color: T.textMedium, lineHeight: 1.7 }}>
            <div style={{ fontSize: 12, fontWeight: 600, color: T.text, marginBottom: 8 }}>Data hygiene</div>
            No median-fill for missing Statcast features — batters with insufficient
            track record are skipped and logged separately, not imputed. No synthetic
            odds. No end-of-season aggregates leaking into training. Park factors are
            handedness-specific and computed from real Statcast data only.
          </div>
        </div>
      )}
    </div>
  )
}

export default function Home() {
  const picks = picksData?.picks ?? []
  const asOf = picksData?.as_of_date ?? '—'
  const generatedAt = picksData?.generated_at
  const modelVersion = picksData?.model_version ?? '—'
  const evThreshold = picksData?.ev_threshold_pct ?? 25
  const skippedCount = picksData?.skipped_count ?? 0

  const lowConfidence = picks.filter(p => p.low_confidence).length
  const breakouts = picks.filter(p => (p.breakout_score ?? 0) >= 0.10).length

  let generatedLocal = ''
  if (generatedAt) {
    try { generatedLocal = new Date(generatedAt).toLocaleString() } catch { /* swallow */ }
  }

  return (
    <>
      <Head>
        <title>HR Picks</title>
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
        {/* Header — site title + Tracker link, plain text. */}
        <div style={{ marginBottom: 36 }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 24, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 22, fontWeight: 700, color: T.text, letterSpacing: -0.4 }}>
              HR Picks
            </span>
            <Link href="/tracker" style={{
              fontSize: 14, color: T.textMedium, textDecoration: 'none',
            }}>Tracker</Link>
          </div>
          <div style={{ fontSize: 12, color: T.textLight, marginTop: 8 }}>
            {modelVersion} · As of {asOf} · EV threshold {evThreshold}%
            {generatedLocal && ` · Generated ${generatedLocal}`}
          </div>
        </div>

        {/* Stat row — bordered cards, white background, dark numbers. */}
        <div style={{ display: 'flex', gap: 14, marginBottom: 36, flexWrap: 'wrap' }}>
          <StatCard
            label="Picks today"
            value={picks.length}
            sub={picks.length === 0 ? 'no picks above EV threshold' : `≥ ${evThreshold}% EV`}
          />
          <StatCard
            label="Skipped batters"
            value={skippedCount}
            sub={skippedCount > 0 ? 'see skipped log' : 'no skips logged'}
          />
          <StatCard
            label="Low-confidence"
            value={lowConfidence}
            sub="prior-year only"
          />
          <StatCard
            label="Breakout tagged"
            value={breakouts}
            sub="score ≥ 0.10"
          />
        </div>

        <Methodology data={picksData ?? {}} />

        <PicksTable picks={picks} />

        {/* Footer */}
        <div style={{
          marginTop: 48, paddingTop: 20, borderTop: `1px solid ${T.border}`,
          fontSize: 11, color: T.textLight, lineHeight: 1.7,
        }}>
          Odds: FanDuel + DraftKings (best-of) via The Odds API. De-vigged when both
          books quote both sides. Paper-trade gate: 60+ days. Always verify the
          current price on the listed book before placing a bet — lines move.
        </div>
      </div>
    </>
  )
}
