import { useEffect, useRef, useState } from 'react'
import type { ClusterEvent, ClusterOverview } from './client'

const BASE = import.meta.env.VITE_GATEWAY_URL ?? ''

export function useSSE(): {
  overview: ClusterOverview | null
  events: ClusterEvent[]
  connected: boolean
} {
  const [overview, setOverview] = useState<ClusterOverview | null>(null)
  const [events, setEvents] = useState<ClusterEvent[]>([])
  const [connected, setConnected] = useState(false)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    function connect() {
      const es = new EventSource(`${BASE}/cluster/events`)
      esRef.current = es

      es.addEventListener('state_update', (e: MessageEvent) => {
        try {
          const event: ClusterEvent = JSON.parse(e.data)
          setOverview(event.payload)
          setConnected(true)
          setEvents(prev => [event, ...prev].slice(0, 200))
        } catch {}
      })

      es.addEventListener('leader_change', (e: MessageEvent) => {
        try {
          const event: ClusterEvent = JSON.parse(e.data)
          setOverview(event.payload)
          setEvents(prev => [event, ...prev].slice(0, 200))
        } catch {}
      })

      es.onerror = () => {
        setConnected(false)
        es.close()
        // Auto-reconnect after 2s
        setTimeout(connect, 2000)
      }

      es.onopen = () => setConnected(true)
    }

    connect()
    return () => {
      esRef.current?.close()
    }
  }, [])

  return { overview, events, connected }
}
