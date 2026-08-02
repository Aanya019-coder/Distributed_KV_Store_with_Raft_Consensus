import { useEffect, useRef, useState } from 'react'
import { getOverview, type ClusterEvent, type ClusterOverview } from './client'

const BASE = (import.meta.env.VITE_GATEWAY_URL || import.meta.env.VITE_API_URL || 'https://raft-kv-node1.onrender.com').replace(/\/$/, '')

export function useSSE(): {
  overview: ClusterOverview | null
  events: ClusterEvent[]
  connected: boolean
} {
  const [overview, setOverview] = useState<ClusterOverview | null>(null)
  const [events, setEvents] = useState<ClusterEvent[]>([])
  const [connected, setConnected] = useState(false)
  const esRef = useRef<EventSource | null>(null)
  const pollIntervalRef = useRef<number | null>(null)

  useEffect(() => {
    let active = true

    const startPolling = () => {
      if (pollIntervalRef.current) return
      const poll = async () => {
        try {
          const data = await getOverview()
          if (!active) return
          setOverview(data)
          setConnected(true)
        } catch {
          if (!active) return
          setConnected(false)
        }
      }
      poll()
      pollIntervalRef.current = window.setInterval(poll, 3000)
    }

    const stopPolling = () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current)
        pollIntervalRef.current = null
      }
    }

    function connect() {
      try {
        const es = new EventSource(`${BASE}/cluster/events`)
        esRef.current = es

        es.addEventListener('state_update', (e: MessageEvent) => {
          try {
            const event: ClusterEvent = JSON.parse(e.data)
            setOverview(event.payload)
            setConnected(true)
            stopPolling()
            setEvents(prev => [event, ...prev].slice(0, 200))
          } catch {}
        })

        es.addEventListener('leader_change', (e: MessageEvent) => {
          try {
            const event: ClusterEvent = JSON.parse(e.data)
            setOverview(event.payload)
            stopPolling()
            setEvents(prev => [event, ...prev].slice(0, 200))
          } catch {}
        })

        es.onerror = () => {
          es.close()
          startPolling()
        }

        es.onopen = () => {
          setConnected(true)
          stopPolling()
        }
      } catch {
        startPolling()
      }
    }

    connect()
    return () => {
      active = false
      esRef.current?.close()
      stopPolling()
    }
  }, [])

  return { overview, events, connected }
}

