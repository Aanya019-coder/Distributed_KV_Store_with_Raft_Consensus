import type { ClusterOverview, NodeStatus } from '../api/client'

interface TopologyViewProps {
  overview: ClusterOverview | null
  connected: boolean
}

const NODE_POSITIONS = [
  { x: 250, y: 80 },
  { x: 80,  y: 320 },
  { x: 420, y: 320 },
]

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
  const nodeCount = overview?.nodes.length ?? 3
  const positions = NODE_POSITIONS.slice(0, Math.max(nodeCount, 3))

  return (
    <div className="topology-wrapper">
      <div className="topology-header">
        <h2>Cluster Topology</h2>
        <span className={`conn-badge ${connected ? 'conn-live' : 'conn-disconnected'}`}>
          {connected ? '● Live' : '○ Reconnecting...'}
        </span>
      </div>

      {overview && (
        <div className="topology-meta">
          <span>Leader: <strong>{overview.leader_id || 'Electing…'}</strong></span>
          {overview.nodes[0] && (
            <span>Term: <strong>{overview.nodes[0].current_term}</strong></span>
          )}
        </div>
      )}

      <svg viewBox="0 0 500 420" className="topology-svg">
        {/* Draw edges between all node pairs */}
        {positions.map((a, i) =>
          positions.slice(i + 1).map((b, j) => {
            const targetIdx = i + j + 1
            const aNode = overview?.nodes[i]
            const bNode = overview?.nodes[targetIdx]
            const active = aNode && bNode
            return (
              <line
                key={`${i}-${targetIdx}`}
                x1={a.x} y1={a.y}
                x2={b.x} y2={b.y}
                stroke={active ? '#4facfe33' : '#ffffff11'}
                strokeWidth={active ? 2 : 1}
                strokeDasharray={active ? undefined : '4 4'}
              />
            )
          })
        )}

        {/* Node circles */}
        {positions.map((pos, i) => {
          const node = overview?.nodes[i]
          const offline = !node || node.node_id.includes('offline')
          const badge = roleBadge(node?.role, offline)
          const isLeader = node?.role === 'leader'

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
                fill={`${roleColor(node?.role)}22`}
                stroke={roleColor(node?.role)}
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
              {node && (
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
          ⚠ SSE disconnected — attempting to reconnect to gateway…
        </div>
      )}
    </div>
  )
}
