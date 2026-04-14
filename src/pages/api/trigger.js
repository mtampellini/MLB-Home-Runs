// POST /api/trigger - Triggers the GitHub Actions workflow
export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const token = process.env.GH_PAT;
  if (!token) {
    return res.status(500).json({ error: 'GH_PAT not configured' });
  }

  try {
    const response = await fetch(
      'https://api.github.com/repos/mtampellini/MLB-Home-Runs/actions/workflows/daily.yml/dispatches',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ ref: 'main' }),
      }
    );

    if (response.status === 204) {
      return res.status(200).json({ success: true, message: 'Pipeline triggered' });
    } else {
      const text = await response.text();
      return res.status(response.status).json({ error: text });
    }
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
