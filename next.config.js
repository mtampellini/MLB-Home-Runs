/** @type {import('next').NextConfig} */
const nextConfig = {
  // No `output: 'export'` — the tracker's /api/bets sync route needs a
  // serverless function, and static export silently drops API routes. The
  // pages themselves are all getStaticProps, so they still build to static
  // HTML exactly as before.
  images: { unoptimized: true },
  trailingSlash: true,
}
module.exports = nextConfig
