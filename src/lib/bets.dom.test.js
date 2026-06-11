// DOM interaction test for the bet-toggle wiring used on the tracker page.
// Runs the real React hooks + the real bets lib + a real localStorage (jsdom),
// driving the same load-on-mount / save-on-change / click-to-toggle pattern as
// src/pages/tracker.js. Verifies a flag survives an unmount+remount ("reload").
//
// Run with: node --test src/lib/bets.dom.test.js

const { test } = require('node:test')
const assert = require('node:assert/strict')

// This test needs a DOM. jsdom is intentionally NOT a repo dependency (it's
// heavy and the front-end ships statically), so skip cleanly if it's absent.
// Install it on demand with `npm install --no-save jsdom` to run this locally.
// The zero-dependency unit tests in bets.test.js always cover the core logic.
let JSDOM
try { ({ JSDOM } = require('jsdom')) } catch { /* not installed */ }
if (!JSDOM) {
  test('DOM toggle persistence (skipped: jsdom not installed)', { skip: true }, () => {})
  return
}

// --- wire jsdom globals up BEFORE react-dom loads ---
const dom = new JSDOM('<!doctype html><html><body><div id="root"></div></body></html>', {
  url: 'https://example.test/',
})
global.window = dom.window
global.document = dom.window.document
global.navigator = dom.window.navigator
global.IS_REACT_ACT_ENVIRONMENT = true

const React = require('react')
const { createRoot } = require('react-dom/client')
const { act } = React

const { betKey, toggleBet, loadBets, saveBets, isBet } = require('./bets')

const h = React.createElement

// A faithful, JSX-free reproduction of the tracker's per-pick bet wiring:
// initial state empty (SSR-safe), load flags on mount, persist on change,
// toggle on click — backed by the real lib and window.localStorage.
function BetToggle({ pkey }) {
  const [bets, setBets] = React.useState({})
  const [loaded, setLoaded] = React.useState(false)
  React.useEffect(() => {
    setBets(loadBets(window.localStorage))
    setLoaded(true)
  }, [])
  React.useEffect(() => {
    if (!loaded) return
    saveBets(window.localStorage, bets)
  }, [bets, loaded])
  const betted = isBet(bets, pkey)
  return h('button', {
    id: 'toggle',
    'aria-pressed': betted ? 'true' : 'false',
    onClick: () => setBets(prev => toggleBet(prev, pkey)),
  }, betted ? '✓' : '')
}

test('clicking the bet toggle persists across an unmount/remount (reload)', () => {
  window.localStorage.clear()
  const pkey = betKey('2026-05-30', 660271, 778)
  const container = document.getElementById('root')

  // --- first mount ---
  let root = createRoot(container)
  act(() => { root.render(h(BetToggle, { pkey })) })
  let btn = document.getElementById('toggle')
  assert.equal(btn.getAttribute('aria-pressed'), 'false', 'starts unchecked')

  // Click to flag the bet.
  act(() => { btn.dispatchEvent(new window.MouseEvent('click', { bubbles: true })) })
  btn = document.getElementById('toggle')
  assert.equal(btn.getAttribute('aria-pressed'), 'true', 'checked after click')
  assert.equal(btn.textContent, '✓', 'shows checkmark')
  // It was written to storage.
  assert.equal(isBet(loadBets(window.localStorage), pkey), true, 'persisted to localStorage')

  // --- simulate a page reload: tear down and mount fresh ---
  act(() => { root.unmount() })
  root = createRoot(container)
  act(() => { root.render(h(BetToggle, { pkey })) })
  btn = document.getElementById('toggle')
  assert.equal(btn.getAttribute('aria-pressed'), 'true', 'flag restored after reload')

  // Click again clears it, and that also persists.
  act(() => { btn.dispatchEvent(new window.MouseEvent('click', { bubbles: true })) })
  btn = document.getElementById('toggle')
  assert.equal(btn.getAttribute('aria-pressed'), 'false', 'unchecked after second click')
  assert.equal(isBet(loadBets(window.localStorage), pkey), false, 'cleared in localStorage')

  act(() => { root.unmount() })
})
