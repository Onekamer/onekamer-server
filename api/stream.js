import express from "express";

const router = express.Router();

// Use Node >=18 global fetch
const fetchImpl = globalThis.fetch;

function buildEmbedUrl(libraryId, guid) {
  const lib = String(libraryId || '').trim();
  const g = String(guid || '').trim();
  if (!lib || !g) return null;
  return `https://iframe.mediadelivery.net/embed/${lib}/${g}`;
}

router.post("/stream/import", async (req, res) => {
  try {
    const apiKey = process.env.BUNNY_STREAM_API_KEY;
    const libraryId = process.env.BUNNY_STREAM_LIBRARY_ID;
    const cdnBase = String(process.env.BUNNY_CDN_URL || '').replace(/\/$/, '');
    if (!apiKey || !libraryId) {
      return res.status(500).json({ error: "stream_config_missing" });
    }

    const sourceUrl = String(req.body?.sourceUrl || '').trim();
    const title = String(req.body?.title || '').trim() || 'OneKamer Video';

    if (!sourceUrl) return res.status(400).json({ error: 'sourceUrl_required' });
    if (!cdnBase || !sourceUrl.startsWith(cdnBase + '/')) {
      return res.status(400).json({ error: 'invalid_source_domain' });
    }

    // 1) Create a video object
    const createResp = await fetchImpl(`https://video.bunnycdn.com/library/${encodeURIComponent(libraryId)}/videos`, {
      method: 'POST',
      headers: {
        'AccessKey': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ title }),
    });
    const createData = await createResp.json().catch(() => ({}));
    if (!createResp.ok || !createData || !createData.guid) {
      return res.status(500).json({ error: 'stream_create_failed', details: createData || null });
    }

    const guid = String(createData.guid);

    // 2) Ask Bunny to fetch the source URL (asynchronous processing)
    const fetchResp = await fetchImpl(`https://video.bunnycdn.com/library/${encodeURIComponent(libraryId)}/videos/${encodeURIComponent(guid)}/fetch`, {
      method: 'POST',
      headers: {
        'AccessKey': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ url: sourceUrl }),
    });

    // We tolerate non-2xx here, but report info
    let fetchInfo = null;
    try { fetchInfo = await fetchResp.json(); } catch {}

    const embedUrl = buildEmbedUrl(libraryId, guid);
    return res.json({ ok: true, guid, embedUrl, fetchAccepted: fetchResp.ok, info: fetchInfo || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'internal_error' });
  }
});

export default router;
