import type { ClusterEvent } from '../api/client'

interface EventLogProps {
  events: ClusterEvent[]
}

export function EventLog({ events }: EventLogProps) {
  return (
    <div className="event-log-container">
      <h2>Cluster Event Stream</h2>
      <p className="section-desc">
        Real-time Server-Sent Events (SSE) received from the Gateway pushing live state transitions.
      </p>

      <div className="event-log-list">
        {events.length === 0 ? (
          <div className="log-placeholder">Waiting for gateway cluster events…</div>
        ) : (
          events.map((evt, idx) => (
            <div key={idx} className={`event-card event-type-${evt.type}`}>
              <div className="event-header">
                <span className="event-type-badge">{evt.type}</span>
                <span className="event-time">{new Date(evt.timestamp).toLocaleTimeString()}</span>
              </div>
              <div className="event-body">
                <span>Leader: <strong>{evt.payload.leader_id || 'None'}</strong></span>
                <span>Leader URL: <code>{evt.payload.leader_url || 'N/A'}</code></span>
                <span>Active Nodes: {evt.payload.nodes.length}</span>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
