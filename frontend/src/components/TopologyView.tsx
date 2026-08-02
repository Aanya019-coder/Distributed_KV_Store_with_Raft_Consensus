import type { ClusterOverview, NodeStatus } from '../api/client'

interface TopologyViewProps {
  overview: ClusterOverview | null
  connected: boolean
}

function getNodePositions(count: number): { x: number; y: number }[] {
  if (count <= 1) {
    return [{ x: 250, y: 200 }]
  }
  if (count === 2) {
    return [
      { x: 150, y: 200 },
      { x: 350, y: 200 },
    ]
  }
  if (count === 3) {
    return [
      { x: 250, y: 100 },
      { x: 100, y: 300 },
      { x: 400, y: 300 },
    ]
  }
  // Circle layout for > 3 nodes
  const positions: { x: number; y: number }[] = []
  const cx = 250
  const cy = 210
  const radius = 130
  for (let i = 0; i < count; i++) {
    const angle = (i * 2 * Math.PI) / count - Math.PI / 2
    positions.push({
      x: Math.round(cx + radius * Math.cos(angle)),
      y: Math.round(cy + radius * Math.sin(angle)),
    })
  }
  return positions
}

function roleColor(role: NodeStatus['role'] | undefined) {
  if (role === 'leader') return '#10b981'
  if (role === 'candidate') return '#f59e0b'
  return '#4facfe'
}

function roleBadge(role: NodeStatus['role'] | undefined, offline?: boolean) {
  if (offline) return { label: 'Offline', color: '#ef4444' }
  if (role === 'leader') return { label: '★ Leader', color: '#10b981' }
  if (role === 'candidate') return { label: '⚡ Candidate', color: '#f59e0b' }
  return { label: 'Follower', color: '#4facfe' }
}

export function TopologyView({ overview, connected }: TopologyViewProps) {
  const nodes = overview?.nodes ?? []
  const displayNodes = nodes.length > 0 ? nodes : [{ node_id: 'node1', role: 'follower' as const, current_term: 0, commit_index: 0, last_applied: 0, log_length: 0, voted_for: '', cluster_members: ['node1'], cluster_size: 1, joint_consensus: false, pending_config_change: false }]
  const positions = getNodePositions(displayNodes.length)

  return (
    <div className="topology-wrapper">
      <div className="topology-header">
        <h2>Cluster Topology</h2>
        <span className={`conn-badge ${connected ? 'conn-live' : 'conn-disconnected'}`}>
          {connected ? '● Live' : '○ Connecting...'}
        </span>
      </div>

      {overview && (
        <div className="topology-meta">
          <span>Leader: <strong>{overview.leader_id || 'Electing…'}</strong></span>
          {overview.nodes[0] && (
            <span>Term: <strong>{overview.nodes[0].current_term}</strong></span>
          )}
          <span>Active Nodes: <strong>{overview.nodes.length}</strong></span>
        </div>
      )}

      <svg viewBox="0 0 500 420" className="topology-svg">
        {/* Draw edges between nodes */}
        {displayNodes.length > 1 &&
          positions.map((a, i) =>
            positions.slice(i + 1).map((b, j) => {
              const targetIdx = i + j + 1
              const aNode = displayNodes[i]
              const bNode = displayNodes[targetIdx]
              const active = connected && aNode && bNode
              return (
                <line
                  key={`${i}-${targetIdx}`}
                  x1={a.x} y1={a.y}
                  x2={b.x} y2={b.y}
                  stroke={active ? '#4facfe55' : '#ffffff11'}
                  strokeWidth={active ? 2 : 1}
                  strokeDasharray={active ? undefined : '4 4'}
                />
              )
            })
          )}

        {/* Node circles */}
        {positions.map((pos, i) => {
          const node = displayNodes[i]
          const isOffline = !connected || !node
          const badge = roleBadge(node?.role, isOffline)
          const isLeader = connected && node?.role === 'leader'

          return (
            <g key={i} transform={`translate(${pos.x},${pos.y})`}>
              {/* Pulse ring for leader */}
              {isLeader && (
                <circle r={46} fill="none" stroke="#10b981" strokeWidth={2} opacity={0.3}>
                  <animate attributeName="r" from="40" to="56" dur="2s" repeatCount="indefinite" />
                  <animate attributeName="opacity" from="0.4" to="0" dur="2s" repeatCount="indefinite" />
                </circle>
              )}
              {/* Glow */}
              <circle
                r={40}
                fill={isOffline ? '#ef444415' : `${roleColor(node?.role)}22`}
                stroke={isOffline ? '#ef444488' : roleColor(node?.role)}
                strokeWidth={isLeader ? 3 : 1.5}
              />
              {/* Node ID */}
              <text
                textAnchor="middle"
                dominantBaseline="middle"
                y={-6}
                fill="#f3f4f6"
                fontSize={13}
                fontWeight={700}
                fontFamily="'JetBrains Mono', monospace"
              >
                {node?.node_id ?? `node${i + 1}`}
              </text>
              {/* Commit index */}
              {node && connected && (
                <text
                  textAnchor="middle"
                  dominantBaseline="middle"
                  y={10}
                  fill="#9ca3af"
                  fontSize={10}
                  fontFamily="'JetBrains Mono', monospace"
                >
                  ci:{node.commit_index}
                </text>
              )}
              {/* Role badge */}
              <text
                textAnchor="middle"
                dominantBaseline="middle"
                y={55}
                fill={badge.color}
                fontSize={11}
                fontWeight={600}
                fontFamily="'Outfit', sans-serif"
              >
                {badge.label}
              </text>
            </g>
          )
        })}
      </svg>

      {!connected && (
        <div className="topology-offline-notice">
          ⚡ Connecting to Raft backend (Render free tier may take ~30s on cold start)...
        </div>
      )}
    </div>
  )
}

