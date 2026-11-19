async function req(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || res.statusText)
  }
  return res.json()
}

export const api = {
  health: () => req('/api/health'),
  overview: () => req('/api/overview'),
  visitors: () => req('/api/visitors'),
  sessions: (visitorId) =>
    req(visitorId ? `/api/sessions?visitor_id=${encodeURIComponent(visitorId)}` : '/api/sessions'),
  session: (id) => req(`/api/sessions/${encodeURIComponent(id)}`),
  quarantine: () => req('/api/quarantine'),
  ingest: () => req('/api/ingest/run', { method: 'POST' }),
  ingestStage: () => req('/api/ingest/stage', { method: 'POST' }),
  ingestCommit: (batchId) =>
    req('/api/ingest/commit', { method: 'POST', body: JSON.stringify({ batch_id: batchId }) }),
  ingestAbort: (batchId) =>
    req('/api/ingest/abort', { method: 'POST', body: JSON.stringify({ batch_id: batchId }) }),
  ingestCandidates: () => req('/api/ingest/candidates'),
  resplitRun: (body) => req('/api/resplit/run', { method: 'POST', body: JSON.stringify(body || {}) }),
  resplitBatches: () => req('/api/resplit/batches'),
  resplitVerify: () => req('/api/resplit/verify'),
  proofs: (sessionId) =>
    req(sessionId ? `/api/session-proofs?session_id=${encodeURIComponent(sessionId)}` : '/api/session-proofs'),
  settings: () => req('/api/settings'),
}
