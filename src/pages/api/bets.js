// Cross-device sync endpoint for the tracker's bet flags.
//
// POST { bets } with header x-sync-key → merges the client map with the copy
// in Vercel Blob (per-pick last-write-wins, see lib/bets.js) and returns the
// merged map. GET returns the server copy without writing (debugging aid).
//
// Auth is a single shared secret (BETS_SYNC_KEY env var) — this is a one-user
// site with no accounts. The repo is public, so the key must NEVER appear in
// source; devices receive it once via a /tracker?sync=<key> link.
//
// Requires in Vercel: a connected Blob store (BLOB_READ_WRITE_TOKEN) and
// BETS_SYNC_KEY. Without them the route answers 503 and the tracker quietly
// stays local-only.

import { list, put } from '@vercel/blob'
import { timingSafeEqual } from 'crypto'
import { mergeBets, normalizeBets, pruneBets } from '../../lib/bets'

const BLOB_PATH = 'tracker/bets.json'

function keyOk(req) {
  const expected = process.env.BETS_SYNC_KEY || ''
  const supplied = String(req.headers['x-sync-key'] || req.query.key || '')
  if (!expected || !supplied) return false
  const a = Buffer.from(supplied)
  const b = Buffer.from(expected)
  return a.length === b.length && timingSafeEqual(a, b)
}

async function readServerBets() {
  const { blobs } = await list({ prefix: BLOB_PATH, limit: 1 })
  if (!blobs.length) return {}
  // Blob URLs are CDN-cached; a unique query string forces a fresh read so a
  // device never merges against a stale copy and re-resurrects old flags.
  const res = await fetch(`${blobs[0].url}?ts=${Date.now()}`, { cache: 'no-store' })
  if (!res.ok) return {}
  try {
    return normalizeBets(await res.json())
  } catch {
    return {}
  }
}

export default async function handler(req, res) {
  if (!process.env.BLOB_READ_WRITE_TOKEN || !process.env.BETS_SYNC_KEY) {
    return res.status(503).json({ error: 'sync not configured' })
  }
  if (!keyOk(req)) {
    return res.status(401).json({ error: 'bad sync key' })
  }

  if (req.method === 'GET') {
    return res.status(200).json({ bets: await readServerBets() })
  }
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'GET, POST')
    return res.status(405).json({ error: 'method not allowed' })
  }

  const client = normalizeBets(req.body && req.body.bets)
  const merged = pruneBets(mergeBets(await readServerBets(), client))
  await put(BLOB_PATH, JSON.stringify(merged), {
    access: 'public',
    addRandomSuffix: false,
    allowOverwrite: true,
    contentType: 'application/json',
  })
  return res.status(200).json({ bets: merged })
}
