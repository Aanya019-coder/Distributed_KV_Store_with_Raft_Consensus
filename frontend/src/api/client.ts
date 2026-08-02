// Gateway API base URL — checks VITE_GATEWAY_URL, VITE_API_URL, or defaults to Render live node
const BASE = (import.meta.env.VITE_GATEWAY_URL || import.meta.env.VITE_API_URL || 'https://raft-kv-node1.onrender.com').replace(/\/$/, '')

export interface NodeStatus {
  node_id: string
  role: 'leader' | 'follower' | 'candidate'
  current_term: number
  commit_index: number
  last_applied: number
  log_length: number
  voted_for: string
  cluster_members: string[]
  cluster_size: number
  joint_consensus: boolean
  pending_config_change: boolean
}

export interface ClusterOverview {
  nodes: NodeStatus[]
  leader_id: string
  leader_url: string
  updated_at: string
}

export interface ClusterEvent {
  type: 'state_update' | 'leader_change' | 'error'
  payload: ClusterOverview
  timestamp: string
}

function authHeaders(token?: string): Record<string, string> {
  if (!token) return {}
  return { Authorization: `Bearer ${token}` }
}

export async function getOverview(): Promise<ClusterOverview> {
  try {
    const res = await fetch(`${BASE}/cluster/overview`)
    if (res.ok) {
      return await res.json()
    }
  } catch {}

  // Fallback to direct node /status endpoint if gateway /cluster/overview is not present
  const res = await fetch(`${BASE}/status`)
  if (!res.ok) throw new Error(`status: ${res.status}`)
  const status: NodeStatus = await res.json()
  return {
    nodes: [status],
    leader_id: status.role === 'leader' ? status.node_id : (status.voted_for || status.node_id),
    leader_url: BASE,
    updated_at: new Date().toISOString()
  }
}

export async function kvGet(key: string, token?: string): Promise<{ key: string; value: string }> {
  const res = await fetch(`${BASE}/kv/${encodeURIComponent(key)}`, {
    headers: authHeaders(token),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text)
  }
  return res.json()
}

export async function kvPut(key: string, value: string, token?: string): Promise<{ status: string }> {
  const res = await fetch(`${BASE}/kv/${encodeURIComponent(key)}`, {
    method: 'PUT',
    body: value,
    headers: {
      'Content-Type': 'text/plain',
      'X-Client-ID': sessionId(),
      'X-Request-ID': String(nextRequestId()),
      ...authHeaders(token),
    },
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text)
  }
  return res.json()
}

export async function kvDelete(key: string, token?: string): Promise<{ status: string }> {
  const res = await fetch(`${BASE}/kv/${encodeURIComponent(key)}`, {
    method: 'DELETE',
    headers: authHeaders(token),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text)
  }
  return res.json()
}

export async function getAggregateMetrics(): Promise<string> {
  try {
    const res = await fetch(`${BASE}/metrics/aggregate`)
    if (res.ok) return await res.text()
  } catch {}

  const res = await fetch(`${BASE}/metrics`)
  if (!res.ok) throw new Error(`metrics: ${res.status}`)
  return await res.text()
}

export async function adminAddNode(
  id: string,
  grpcAddr: string,
  httpAddr: string,
  token?: string
): Promise<unknown> {
  const res = await fetch(`${BASE}/admin/addnode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders(token) },
    body: JSON.stringify({ id, grpc_addr: grpcAddr, http_addr: httpAddr }),
  })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function adminRemoveNode(id: string, token?: string): Promise<unknown> {
  const res = await fetch(`${BASE}/admin/removenode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders(token) },
    body: JSON.stringify({ id }),
  })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

// ---- Session helpers ----
let _sessionId: string | null = null
function sessionId(): string {
  if (!_sessionId) {
    _sessionId = `browser-${Math.random().toString(36).slice(2, 10)}`
  }
  return _sessionId
}

let _reqId = 0
function nextRequestId(): number {
  return ++_reqId
}
